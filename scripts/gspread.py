from .log import *
import gspread
import gspread_formatting as gfmt
import datetime
import time
import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def _disable_requests_tls_verification():
    original_request = requests.sessions.Session.request

    def patched_request(self, method, url, *args, **kwargs):
        kwargs.setdefault("verify", False)
        return original_request(self, method, url, *args, **kwargs)

    requests.sessions.Session.request = patched_request

TARGET_SHEET = "1gjYXr-aFrDLLXUfmsA-tN_Tc5rgovtIfeDqoJM-78jM"

# Retry policy — gspread silently dropping updates was reported as a recurring
# operational pain. Transient causes: rate limits, brief network blips, server
# 5xx. APIError covers all of those; we don't blanket-retry on auth or 4xx
# (caller has nothing to gain from re-trying a bad request).
_RETRYABLE_GSPREAD_HTTP = {429, 500, 502, 503, 504}
_MAX_RETRIES = 4
_BASE_BACKOFF_SECONDS = 2.0


def _is_retryable(exc: BaseException) -> bool:
    if isinstance(exc, requests.exceptions.RequestException):
        return True
    if hasattr(exc, "response") and getattr(exc.response, "status_code", None) in _RETRYABLE_GSPREAD_HTTP:
        return True
    try:
        from gspread.exceptions import APIError
        if isinstance(exc, APIError):
            code = None
            resp = getattr(exc, "response", None)
            if resp is not None:
                code = getattr(resp, "status_code", None)
            return code is None or code in _RETRYABLE_GSPREAD_HTTP
    except ImportError:
        pass
    return False


def _retry(call, label):
    """Call() with bounded exponential backoff for transient API errors.
    Returns the call's return value on success. Re-raises after final
    attempt so the caller's try/except still sees the failure."""
    for attempt in range(_MAX_RETRIES):
        try:
            return call()
        except Exception as e:
            if not _is_retryable(e) or attempt == _MAX_RETRIES - 1:
                LOG_ERROR(0, f"gspread {label} failed (attempt {attempt+1}/{_MAX_RETRIES}): {e}")
                raise
            backoff = _BASE_BACKOFF_SECONDS * (2 ** attempt)
            LOG_WARN(0, f"gspread {label} transient error (attempt {attempt+1}/{_MAX_RETRIES}), "
                        f"retrying in {backoff:.1f}s: {e}")
            time.sleep(backoff)


def _build_file_chip_cell(url, date_str):
    """Build a CellData with chipRuns for a Drive file chip + date text.

    Cell text: "@ (2026-03-22)"
    chipRuns maps the @ at index 0 to a Drive file rich link chip.
    Note: Only Google Drive file URIs can be written as chips.
    """
    cell_text = f"@ {date_str}"
    return {
        "userEnteredValue": {"stringValue": cell_text},
        "chipRuns": [
            {
                "startIndex": 0,
                "chip": {
                    "richLinkProperties": {"uri": url},
                },
            }
        ],
    }


def log(logs, new_file_urls=None):
    if new_file_urls is None:
        new_file_urls = []
    _disable_requests_tls_verification()
    account = _retry(lambda: gspread.service_account(r"api.json"), "service_account")
    SHEET = _retry(lambda: account.open_by_key(TARGET_SHEET), "open_by_key")
    worksheet = _retry(lambda: SHEET.worksheet("업데이트 로그"), "worksheet")
    sheet_id = worksheet.id
    _retry(lambda: worksheet.insert_cols([[]], 1), "insert_cols")

    now = datetime.datetime.now()
    date_str = now.strftime("(%Y-%m-%d)")
    title_text = str(now) + " 업데이트 기록"

    # Write title (A2) and log text (A3) via normal update
    _retry(lambda: worksheet.update([
        [title_text],
        [logs],
    ], 'A2:A3'), "update A2:A3")

    # Verify the title cell came back what we wrote — catches silent drops
    # where the API returned 200 but the row didn't actually update (rare
    # but observed during heavy contention).
    try:
        readback = _retry(lambda: worksheet.acell('A2').value, "verify acell A2")
        if readback != title_text:
            LOG_ERROR(0, f"gspread verify FAILED — A2 readback {readback!r} "
                         f"!= expected {title_text!r}. Sheet may be stale.")
        else:
            LOG_INFO(0, f"gspread verify ✓ A2 == expected title")
    except Exception as e:
        LOG_WARN(0, f"gspread verify skipped (read-back failed): {e}")

    # Format title (A2)
    worksheet.format("A2", {
        "backgroundColor": {
            "red": 0.945,
            "green": 0.760,
            "blue": 0.196
        },
        "horizontalAlignment": "CENTER",
        "textFormat": {
            "foregroundColor": {
                "red": 0.0,
                "green": 0.0,
                "blue": 0.0
            },
            "fontSize": 18,
            "bold": True
        }
    })
    # Format log text (A3)
    worksheet.format("A3", {
        "backgroundColor": {
            "red": 1.0,
            "green": 0.95,
            "blue": 0.8
        },
        "horizontalAlignment": "LEFT",
        "textFormat": {
            "foregroundColor": {
                "red": 0.0,
                "green": 0.0,
                "blue": 0.0
            },
            "fontSize": 12,
            "bold": False
        }
    })

    # Write file chip rows (A4+) via batchUpdate with chipRuns
    # Drive chip requests are limited to 10 per batchUpdate call
    if new_file_urls:
        chip_rows = []
        for url in new_file_urls:
            chip_rows.append({"values": [_build_file_chip_cell(url, date_str)]})

        CHIP_BATCH_SIZE = 10
        start_row = 3  # 0-indexed row 3 = A4
        for i in range(0, len(chip_rows), CHIP_BATCH_SIZE):
            batch = chip_rows[i:i + CHIP_BATCH_SIZE]
            batch_start = start_row + i
            SHEET.batch_update({
                "requests": [
                    {
                        "updateCells": {
                            "rows": batch,
                            "fields": "userEnteredValue,chipRuns",
                            "range": {
                                "sheetId": sheet_id,
                                "startRowIndex": batch_start,
                                "endRowIndex": batch_start + len(batch),
                                "startColumnIndex": 0,
                                "endColumnIndex": 1,
                            },
                        }
                    }
                ]
            })

        # Format chip rows
        last_row = 4 + len(new_file_urls) - 1
        worksheet.format(f"A4:A{last_row}", {
            "backgroundColor": {
                "red": 0.93,
                "green": 0.97,
                "blue": 1.0
            },
            "horizontalAlignment": "LEFT",
            "textFormat": {
                "foregroundColor": {
                    "red": 0.0,
                    "green": 0.0,
                    "blue": 0.0
                },
                "fontSize": 11,
                "bold": False
            }
        })

    gfmt.set_column_width(worksheet, 'A', 900)
