from .log import *
import gspread
import gspread_formatting as gfmt
import datetime

TARGET_SHEET = "1gjYXr-aFrDLLXUfmsA-tN_Tc5rgovtIfeDqoJM-78jM"

def log(logs, new_file_urls=None):
    if new_file_urls is None:
        new_file_urls = []
    account = gspread.service_account(r"api.json")
    SHEET = account.open_by_key(TARGET_SHEET)
    worksheet = SHEET.worksheet("업데이트 로그")
    worksheet.insert_cols([[]],1)

    # Build cell data: A2=title, A3=logs, A4+=file chip URLs
    cells = [
        [str(datetime.datetime.now()) + " 업데이트 기록"],
        [logs],
    ]
    for url in new_file_urls:
        cells.append([url])

    last_row = 2 + len(cells) - 1
    worksheet.update(cells, f'A2:A{last_row}')

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
    # Format file chip rows (A4+)
    if new_file_urls:
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
