"""ADV pipeline: file converters and folder-level orchestration.

Submodules:
- adv_encode: text encoding (_encode, _processEMtag)
- adv_record: DataFrame/record processing
- adv_merge: merge/diff/conversion logic
"""

import os, fnmatch, json
from io import StringIO
from pathlib import Path

from .helper import (
    Helper_GetFilesFromDir,
    Helper_FilterStaleByOutput,
)
from .log import LOG_DEBUG, LOG_INFO, LOG_WARN, LOG_ERROR, logger

# Re-export submodule functions for backward compatibility
from .adv_encode import _encode, _processEMtag, END_EM_LENGTH
from .adv_record import (
    _internalOverrideXlsxColumn,
    _internalReadXlsx,
    _internalXlsxDataFrameProcess,
    _internalXlsxRecordsProcess,
    _internalCsvWriter,
)
from .adv_merge import (
    _internalTxtToScv,
    _internalCsvToDataFrame,
    _internalUpdateDataFrame,
    _internalDataFrameToXlsx,
    _replace_at_offset,
    _internalCsvToTxt,
)
from .paths import (
    GIT_ADV_PATH, ADV_ORIGINAL_PATH, ADV_REMOTE_PATH, ADV_DRIVE_PATH,
    ADV_TEMP_PATH, ADV_OUTPUT_PATH,
)

ADV_MANIFEST_DIFF = Path("res/.manifest/adv.diff.json")
ADV_MANIFEST = Path("res/.manifest/adv.json")
# Per-file record of "the campus orig sha that Phase 2 last successfully
# pushed to Drive." Compared against the current campus manifest to detect:
#   * file addition          — present in campus manifest, missing from applied
#   * file modification      — applied.sha != current.sha
#   * (with --reconcile)     — xlsx text column drift even when applied.sha
#                              already matches (catches agent regressions)
# Without this record we would forever miss any file whose update happened
# while Phase 2 was offline / regressed: campus manifest diff between two
# consecutive syncs only shows the latest delta, never the historical truth.
ADV_APPLIED = Path("res/.manifest/adv.applied.json")


# ============================================================
# File Converters
# ============================================================


def XlsxToCsv(read_fp, write_fp, origin_path: str) -> None:
    """Convert translated XLSX → CSV for game engine import."""
    xlsx_dataframe = _internalReadXlsx(read_fp)
    _internalXlsxDataFrameProcess(xlsx_dataframe, origin_path)
    xlsx_records = xlsx_dataframe.to_dict(orient="records")
    xlsx_records = _internalXlsxRecordsProcess(xlsx_records)
    _internalCsvWriter(write_fp, xlsx_records)


def CsvToTxt(read_fp, write_path: str, original_path: str) -> None:
    """Merge CSV translations into original TXT game script."""
    csv_strings = "".join(read_fp.readlines())
    with open(original_path, "r", encoding='utf-8') as write_fp:
        txt_strings = "".join(write_fp.readlines())
    txt_strings = _internalCsvToTxt(csv_strings, txt_strings)
    with open(write_path, "w", encoding="utf-8") as write_fp:
        write_fp.write(txt_strings)


def XlsxToTxt(input_path: str, write_path: str, original_path: str) -> None:
    """Convert translated XLSX → TXT (XLSX→CSV→TXT pipeline)."""
    with open(input_path, "rb") as input_fp:
        csvIO = StringIO()
        XlsxToCsv(input_fp, csvIO, os.path.basename(write_path))
    csvIO.seek(0)
    CsvToTxt(csvIO, write_path, original_path)


def TxtToXlsx(input_path: str, output_path: str, file_name: str) -> list[str]:
    """Convert original TXT → XLSX for translation. Returns list of warnings."""
    with open(input_path, "r", encoding="utf-8") as input_fp:
        csv = _internalTxtToScv(input_fp, file_name)
    dataframe = _internalCsvToDataFrame(csv)
    warnings = []

    if os.path.exists(output_path):
        original_fp = open(output_path, "rb")
        LOG_DEBUG(4, "Try to update original file")
        dataframe, warnings = _internalUpdateDataFrame(dataframe, original_fp, file_name)
        original_fp.close()

    LOG_DEBUG(4, "Write result to file")
    dir_path = os.path.dirname(output_path)
    if dir_path and not os.path.exists(dir_path):
        os.makedirs(dir_path)
        
    write_fp = open(output_path, "wb")
    _internalDataFrameToXlsx(dataframe, write_fp)
    write_fp.close()
    return warnings


# ============================================================
# Parallel wrappers
# ============================================================


def XlsxToTxt_parallels(obj):
    """Parallel worker for XLSX→TXT conversion."""
    input_path, filename = obj
    output_path = os.path.join(ADV_OUTPUT_PATH, filename[:-5] + ".txt")
    original_path = os.path.join(ADV_ORIGINAL_PATH, filename[:-5] + ".txt")
    converted_file_list = []
    error_file_list = []
    try:
        XlsxToTxt(input_path, output_path, original_path)
        converted_file_list.append(filename)
    except Exception as e:
        LOG_ERROR(2, f"Error converting {filename}: {e}")
        error_file_list.append((e, filename))
    return error_file_list, converted_file_list


def TxtToXlsx_parallels(obj):
    """Parallel worker for TXT→XLSX conversion."""
    input_path, output_path, filename = obj
    try:
        warnings = TxtToXlsx(input_path, output_path, filename)
        return {filename: warnings} if warnings else {}
    except Exception as e:
        LOG_ERROR(2, f"Error: {e}")
        logger.exception(e)
        return {}


# ============================================================
# Folder processing helpers
# ============================================================


ADV_BLACKLIST_FILE = [
    "musics.txt",
    "adv_warmup.txt",
    "adv_produce_lesson_*",
    "adv_produce-lesson_*"
]

ADV_BLACKLIST_FOLDER = [
    "pstep",
    "pweek",
]


def _internalGetOutputPath(filename: str) -> str:
    """Extract output folder name from an ADV filename."""
    splitted_name = filename[4:-4].split("_")
    folder_name = splitted_name[0]
    if splitted_name[0] == "pstory":
        folder_name += "_" + splitted_name[1]
    folder_name = folder_name.split("-")[0]
    return folder_name


def _has_messages(abs_path: str) -> bool:
    """True if the orig txt has at least one [message text=...] tag.

    Pure-animation scripts (gasha CGs, lesson SP step setups, etc.) have only
    background / camera / titan / actor blocks and no dialogue — running
    TxtToXlsx on them raises "ValueError: No message" inside
    _internalCsvToDataFrame and produces no xlsx. Without this gate the Y+
    detection loop would queue them every run forever (xlsx never gets
    created → never marked applied → tries again next tick).
    """
    try:
        with open(abs_path, encoding="utf-8") as f:
            for line in f:
                if line.startswith("[message text="):
                    return True
    except OSError:
        return True   # let TxtToXlsx surface the real error
    return False


def _filter_adv_files(file_paths):
    """Apply blacklist + message-presence gate, map to
    (input_path, output_xlsx_path, filename) tuples."""
    file_list = []
    for abs_path, rel_path, filename in file_paths:
        if any(fnmatch.fnmatch(filename, rule) for rule in ADV_BLACKLIST_FILE):
            continue
        foldername = _internalGetOutputPath(filename)
        if foldername in ADV_BLACKLIST_FOLDER:
            continue
        if not _has_messages(abs_path):
            continue
        input_path = rel_path
        output_path = os.path.join(ADV_DRIVE_PATH, foldername, filename[:-4] + ".xlsx")
        file_list.append((input_path, output_path, filename))
    return file_list


def _convert_xlsx_to_txt_batch(drive_file_paths):
    """Run XlsxToTxt in parallel via multiprocessing Pool.

    Returns (error_file_list, converted_file_list).
    """
    from .parallel import run_parallel, collect_errors_and_successes

    results = run_parallel(
        XlsxToTxt_parallels,
        [(abs_path, filename) for abs_path, rel_path, filename in drive_file_paths],
        desc="XLSX→TXT",
    )
    return collect_errors_and_successes(results)


# ============================================================
# Folder Processors (public API)
# ============================================================


def _load_applied(path: Path = ADV_APPLIED) -> dict:
    """Load per-file 'last applied campus sha' record. Missing = empty dict."""
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except (json.JSONDecodeError, OSError) as e:
        LOG_ERROR(2, f"applied record unreadable, treating as empty: {e}")
        return {}


def _bootstrap_applied(campus_files: dict, xlsx_root: str) -> dict:
    """First-run baseline: for every campus file whose Drive xlsx exists,
    record {orig_sha: campus_sha, xlsx_text_sha: <hash of xlsx text col>}.

    The xlsx_text_sha is the future reconcile baseline — it's what the
    current Drive content looks like, so subsequent drift detection treats
    "Drive state at first run" as canonical until Phase 2 explicitly
    overwrites it (or reconcile detects regression).

    Files whose xlsx is missing stay unrecorded so the next detection cycle
    flags them as additions.
    """
    bootstrapped = {}
    for name, meta in campus_files.items():
        foldername = _internalGetOutputPath(name)
        xlsx_path = os.path.join(xlsx_root, foldername, name[:-4] + ".xlsx")
        if not os.path.exists(xlsx_path):
            continue
        sha = meta.get("sha256")
        if not sha:
            continue
        bootstrapped[name] = {
            "orig_sha": sha,
            "xlsx_text_sha": _xlsx_text_column_sha(xlsx_path),
        }
    return bootstrapped


def _save_applied(applied: dict, path: Path = ADV_APPLIED) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(applied, ensure_ascii=False, indent=2, sort_keys=True))


def _applied_entry(applied_value):
    """applied.json supports two shapes for back-compat:
        str       — legacy: just the campus orig sha
        dict      — current: {orig_sha, xlsx_text_sha}
    Normalize to dict-with-defaults at read time so detection / reconcile
    don't need to keep branching."""
    if isinstance(applied_value, str):
        return {"orig_sha": applied_value, "xlsx_text_sha": None}
    if isinstance(applied_value, dict):
        return {
            "orig_sha": applied_value.get("orig_sha"),
            "xlsx_text_sha": applied_value.get("xlsx_text_sha"),
        }
    return {"orig_sha": None, "xlsx_text_sha": None}


def _xlsx_text_column_sha(xlsx_path: str) -> str | None:
    """sha256 of the xlsx's `text` column, in row order, joined by NUL.
    Captures the source-of-truth content Phase 2 writes; invariant to
    `translated text` edits by agents (which is the whole point — we want
    to detect changes to the *original* column without false-flagging
    legitimate translation work)."""
    import openpyxl, hashlib
    h = hashlib.sha256()
    try:
        wb = openpyxl.load_workbook(xlsx_path, read_only=True, data_only=True)
    except Exception:
        return None
    try:
        ws = wb.active
        headers = [c.value for c in ws[1]]
        if "text" not in headers:
            return None
        ti = headers.index("text")
        for row in ws.iter_rows(min_row=2, values_only=True):
            v = row[ti] if ti < len(row) else None
            if v is not None:
                h.update(str(v).encode("utf-8"))
                h.update(b"\x00")
    finally:
        wb.close()
    return h.hexdigest()


def _detect_pending(campus_files: dict, applied: dict, xlsx_root: str) -> tuple[set, set, set]:
    """Compare campus manifest vs applied record.

    Returns:
        added    — filenames in campus manifest not yet applied (or whose
                   Drive xlsx is missing — covers Phase 2 having previously
                   succeeded but the xlsx getting deleted out-of-band)
        modified — filenames whose campus orig sha differs from applied orig sha
        obsolete — filenames recorded as applied but no longer in campus
                   (game removed the file). Caller may delete the xlsx, but
                   we default to leaving it for human review.
    """
    added, modified, obsolete = set(), set(), set()
    for name, meta in campus_files.items():
        entry = _applied_entry(applied.get(name))
        if entry["orig_sha"] is None:
            added.add(name)
            continue
        if entry["orig_sha"] != meta.get("sha256"):
            modified.add(name)
            continue
        # applied.orig_sha matches campus.sha — verify Drive xlsx exists.
        foldername = _internalGetOutputPath(name)
        xlsx_path = os.path.join(xlsx_root, foldername, name[:-4] + ".xlsx")
        if not os.path.exists(xlsx_path):
            added.add(name)
    for name in applied:
        if name not in campus_files:
            obsolete.add(name)
    return added, modified, obsolete


def _reconcile_drive_xlsx(campus_files: dict, applied: dict, xlsx_root: str) -> set:
    """For files whose applied.orig_sha already matches campus.sha (no change
    upstream), verify the xlsx text column hash matches what Phase 2 wrote
    last time. Catches agent / external regressions where the Drive xlsx was
    overwritten with stale content even though the orig didn't change.

    Files where applied.xlsx_text_sha is None (legacy entries that predate
    this tracking) are SKIPPED — we can't know what's drifted without a
    baseline. The first successful Phase 2 update establishes the baseline,
    and from then on this check is reliable.
    """
    drift = set()
    for name, meta in campus_files.items():
        entry = _applied_entry(applied.get(name))
        if entry["orig_sha"] != meta.get("sha256"):
            continue
        if entry["xlsx_text_sha"] is None:
            continue
        if any(fnmatch.fnmatch(name, rule) for rule in ADV_BLACKLIST_FILE):
            continue
        if _internalGetOutputPath(name) in ADV_BLACKLIST_FOLDER:
            continue
        foldername = _internalGetOutputPath(name)
        xlsx_path = os.path.join(xlsx_root, foldername, name[:-4] + ".xlsx")
        if not os.path.exists(xlsx_path):
            drift.add(name)
            continue
        current_sha = _xlsx_text_column_sha(xlsx_path)
        if current_sha != entry["xlsx_text_sha"]:
            drift.add(name)
    return drift


def UpdateOriginalToDrive(reconcile: bool = False):
    """Update: Campus-Adv-txts → Google Drive XLSX.

    Detection strategy (option Y+, supersedes the manifest-diff-only logic
    introduced in 7cd0203):

        1. Walk the campus manifest (sha256 per file — already computed by
           campus_sync; no extra hashing here).
        2. Compare each file's current sha against the per-file "applied"
           record at `res/.manifest/adv.applied.json`.
        3. needs_update = (campus.sha != applied.sha) ∪ (xlsx absent in Drive)
           — naturally covers additions, modifications, and previously-
           propagated files whose Drive xlsx vanished.
        4. (Optional, opt-in via `reconcile=True`): for files whose applied
           sha matches but the Drive xlsx exists, read the xlsx and verify
           its text column matches the orig. Catches agent regressions where
           the orig didn't change but the Drive xlsx got reverted.
        5. After TxtToXlsx succeeds for a file, record current sha in
           applied.json. The record is the source of truth — even if Phase
           2 missed a campus sync window, the next run sees the gap.

    Returns (file_list, all_warnings) — same shape as before.
    """
    if not ADV_MANIFEST.exists():
        LOG_INFO(2, "ADV manifest not found, skip")
        return [], {}

    manifest = json.loads(ADV_MANIFEST.read_text())
    campus_files = manifest.get("files", {})
    if not campus_files:
        LOG_INFO(2, "ADV manifest empty, skip")
        return [], {}

    applied = _load_applied()
    xlsx_root = ADV_DRIVE_PATH

    # First-run bootstrap: trust the current Drive state as already-applied
    # for any file whose xlsx exists. Missing xlsx still flow through as
    # additions (correct: those genuinely need Phase 2 to produce one).
    if not applied:
        applied = _bootstrap_applied(campus_files, xlsx_root)
        _save_applied(applied)
        LOG_INFO(2, f"ADV applied.json bootstrapped from current Drive state "
                    f"({len(applied)}/{len(campus_files)} files marked as baseline)")

    added, modified, obsolete = _detect_pending(campus_files, applied, xlsx_root)
    LOG_INFO(2,
        f"ADV pending: added={len(added)} modified={len(modified)} "
        f"obsolete={len(obsolete)} (applied-state vs campus-manifest)")

    drift = set()
    if reconcile:
        drift = _reconcile_drive_xlsx(campus_files, applied, xlsx_root)
        LOG_INFO(2, f"ADV reconcile detected drift in {len(drift)} file(s) "
                    f"(xlsx text-column sha vs last-applied baseline)")

    pending_names = added | modified | drift
    if not pending_names:
        LOG_INFO(2, "ADV is in sync with campus, skip")
        return [], {}

    # Resolve names → file tuples
    all_paths = Helper_GetFilesFromDir(GIT_ADV_PATH, ".txt", "adv_")
    path_by_name = {tpl[2]: tpl for tpl in all_paths}
    original_file_paths = [path_by_name[n] for n in pending_names if n in path_by_name]
    missing = pending_names - set(path_by_name)
    if missing:
        LOG_WARN(2, f"ADV pending but orig txt missing for: {sorted(missing)[:5]}"
                    + (f" (+{len(missing)-5} more)" if len(missing) > 5 else ""))

    file_list = _filter_adv_files(original_file_paths)
    # Files dropped by the blacklist / no-message gate still need to be
    # marked applied — otherwise they reappear in `added` every run and
    # we'd loop forever on files there's genuinely no work for.
    filtered_in = {tpl[2] for tpl in file_list}
    skipped_pending = []
    for tpl in original_file_paths:
        name = tpl[2]
        if name in filtered_in:
            continue
        sha = campus_files.get(name, {}).get("sha256")
        if sha:
            applied[name] = {"orig_sha": sha, "xlsx_text_sha": None}
            skipped_pending.append(name)
    if skipped_pending:
        LOG_DEBUG(2, f"ADV: {len(skipped_pending)} pending files were blacklist/"
                     f"no-message and recorded as applied (no work needed)")

    if not file_list:
        for name in obsolete:
            applied.pop(name, None)
        _save_applied(applied)
        LOG_INFO(2, f"ADV pending entirely under blacklist/no-message, skip "
                    f"(applied baseline updated: {len(applied)} entries)")
        return [], {}

    LOG_INFO(2, f"Updating {len(file_list)} adv files "
                f"(+{len(added)} ~{len(modified)} drift={len(drift)}, "
                f"baseline-skipped={len(skipped_pending)})")
    from .parallel import run_parallel, collect_dict_results

    results = run_parallel(TxtToXlsx_parallels, file_list, desc="TXT→XLSX")
    all_warnings = collect_dict_results(results)

    # Verify per-file success by checking that the xlsx now exists on disk.
    # TxtToXlsx_parallels swallows exceptions and returns {}, so a failed
    # attempt is indistinguishable from a clean run at the return-value
    # level. The filesystem check is the source of truth: if the xlsx isn't
    # there afterwards, the attempt failed and we must not mark it applied.
    # For successes, capture both orig_sha (from campus) and xlsx_text_sha
    # (from the freshly-written xlsx) so the next reconcile pass has a
    # solid baseline to compare against.
    succeeded = 0
    for input_path, output_path, filename in file_list:
        if not os.path.exists(output_path):
            continue
        sha = campus_files.get(filename, {}).get("sha256")
        if sha:
            applied[filename] = {
                "orig_sha": sha,
                "xlsx_text_sha": _xlsx_text_column_sha(output_path),
            }
            succeeded += 1

    for name in obsolete:
        applied.pop(name, None)

    _save_applied(applied)
    LOG_INFO(2, f"applied.json updated: {len(applied)} entries "
                 f"(this run: succeeded={succeeded}/{len(file_list)}, "
                 f"obsolete-cleared={len(obsolete)})")

    return file_list, all_warnings


def _adv_output_for(tpl):
    """xlsx tuple → expected output .txt path. Used by stale-detection."""
    _abs, _rel, filename = tpl
    if not filename.endswith(".xlsx"):
        return None
    return os.path.join(ADV_OUTPUT_PATH, filename[:-5] + ".txt")


def ConvertDriveToOutput(drive_file_paths=None, bFullUpdate=False):
    """Convert: Google Drive XLSX → GakumasTranslationDataKor TXT.

    Triggers (union):
      * Explicit `drive_file_paths` from Phase 0 (rclone diff).
      * Local mtime scan — any xlsx whose .txt output is missing or older.
    The mtime fallback catches xlsx mutations from any source (Phase 2,
    agent direct edit, external script) that Phase 0's diff would miss.
    """
    all_local = Helper_GetFilesFromDir(ADV_DRIVE_PATH, ".xlsx", "adv_")
    stale = Helper_FilterStaleByOutput(all_local, _adv_output_for)

    if drive_file_paths is None:
        LOG_DEBUG(2, "No file list provided, using local stale-mtime scan")
        to_convert = stale
    else:
        seen, to_convert = set(), []
        for tpl in list(drive_file_paths) + stale:
            if tpl[0] in seen:
                continue
            seen.add(tpl[0])
            to_convert.append(tpl)

    if not to_convert:
        LOG_INFO(2, "ADV is up-to-date, skip")
        return [], []
    LOG_INFO(
        2,
        f"Converting {len(to_convert)} adv files "
        f"(explicit={len(drive_file_paths or [])}, stale-by-mtime={len(stale)})",
    )

    return _convert_xlsx_to_txt_batch(to_convert)
