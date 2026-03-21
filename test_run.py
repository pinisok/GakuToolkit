#!/usr/bin/env python3
"""
GakuToolkit Test Mode Runner

Runs the translation pipeline in complete isolation:
- No rclone / Google Drive interaction
- No Google Sheets logging
- No git submodule updates or pushes
- DB.dat and cache files protected from modification

Usage:
    python3 test_run.py --convert --fullupdate --DEBUG
    python3 test_run.py --convert --fullupdate --DEBUG --adv
    python3 test_run.py --convert --fullupdate --DEBUG --isolate-output
    python3 test_run.py --fullupdate --DEBUG --isolate-drive
"""

import sys
import os
import types
import logging
import shelve
import shutil
import argparse
import builtins
from datetime import datetime

# ============================================================
# Section 1: sys.modules injection
#   MUST happen before any scripts.* import.
#   scripts/rclone.py calls init() at module load time (line 120),
#   so we replace the entire module before it can be imported.
# ============================================================

def _test_log(msg):
    """Print to stderr to survive stdout suppression in non-DEBUG mode."""
    print(f"[TEST MODE] {msg}", file=sys.stderr)

# Track suppressed calls for summary
_suppressed_calls = {
    "copy": 0, "sync": 0, "check": 0, "link": 0, "init": 0,
}

# --- 1a. Fake rclone_python package ---
_fake_rclone_py = types.ModuleType("rclone_python")
_fake_rclone_py_inner = types.ModuleType("rclone_python.rclone")
_fake_rclone_py_inner.is_installed = lambda: True
_fake_rclone_py_inner.version = lambda: "v1.69.0"
_fake_rclone_py_inner.get_remotes = lambda: ["gakumas:"]
_fake_rclone_py_inner.copy = lambda *a, **kw: None
_fake_rclone_py_inner.sync = lambda *a, **kw: None
_fake_rclone_py_inner.check = lambda *a, **kw: (0, [])
_fake_rclone_py_inner.link = lambda *a, **kw: ""
_fake_rclone_py.rclone = _fake_rclone_py_inner

_fake_remote_types = types.ModuleType("rclone_python.remote_types")
_fake_remote_types.RemoteTypes = type("RemoteTypes", (), {})

sys.modules["rclone_python"] = _fake_rclone_py
sys.modules["rclone_python.rclone"] = _fake_rclone_py_inner
sys.modules["rclone_python.remote_types"] = _fake_remote_types

# --- 1b. Fake scripts.rclone module ---
_fake_scripts_rclone = types.ModuleType("scripts.rclone")
_fake_scripts_rclone.__package__ = "scripts"


def _rclone_init():
    _suppressed_calls["init"] += 1
    _test_log("rclone.init() suppressed")


def _rclone_copy(source, destination):
    _suppressed_calls["copy"] += 1
    _test_log(f"rclone.copy('{source}', '{destination}') suppressed")
    return None


def _rclone_sync(source, destination):
    _suppressed_calls["sync"] += 1
    _test_log(f"rclone.sync('{source}', '{destination}') suppressed")
    return None


def _rclone_check_empty(source, destination):
    """Default stub: no changes detected."""
    _suppressed_calls["check"] += 1
    _test_log(f"rclone.check('{source}', '{destination}') -> []")
    return []


def _rclone_link(dest):
    _suppressed_calls["link"] += 1
    return "https://docs.google.com/spreadsheets/d/TEST_MODE_ID"


_fake_scripts_rclone.init = _rclone_init
_fake_scripts_rclone.copy = _rclone_copy
_fake_scripts_rclone.sync = _rclone_sync
_fake_scripts_rclone.check = _rclone_check_empty
_fake_scripts_rclone.link = _rclone_link
_fake_scripts_rclone.logger = logging.getLogger("rclone.test")

sys.modules["scripts.rclone"] = _fake_scripts_rclone

# --- 1c. Fake scripts.gspread module ---
_fake_scripts_gspread = types.ModuleType("scripts.gspread")
_fake_scripts_gspread.__package__ = "scripts"


def _gspread_log(logs):
    _test_log(f"gspread.log() suppressed ({len(logs)} chars)")


_fake_scripts_gspread.log = _gspread_log
sys.modules["scripts.gspread"] = _fake_scripts_gspread

# Also fake the third-party gspread libraries
_fake_gspread_lib = types.ModuleType("gspread")
_fake_gspread_lib.service_account = lambda *a, **kw: None
sys.modules["gspread"] = _fake_gspread_lib

_fake_gspread_fmt = types.ModuleType("gspread_formatting")
_fake_gspread_fmt.set_column_width = lambda *a, **kw: None
sys.modules["gspread_formatting"] = _fake_gspread_fmt

# --- 1d. Patch shelve.open for DB.dat isolation ---
_original_shelve_open = shelve.open


def _patched_shelve_open(filename, *args, **kwargs):
    basename = os.path.basename(filename)
    if basename.startswith("DB") and "test" not in basename.lower():
        redirected = filename.replace("DB", "DB_test")
        _test_log(f"shelve.open('{filename}') -> '{redirected}'")
        return _original_shelve_open(redirected, *args, **kwargs)
    return _original_shelve_open(filename, *args, **kwargs)


shelve.open = _patched_shelve_open

# Copy DB.dat files for test isolation
for _ext in ("", ".db", ".dir", ".bak", ".dat"):
    _src = f"DB.dat{_ext}" if _ext else "DB.dat"
    _dst = f"DB_test.dat{_ext}" if _ext else "DB_test.dat"
    if os.path.exists(_src):
        shutil.copy2(_src, _dst)

# --- 1e. Cache file backup / restore ---
_CACHE_FILES = [
    "./cache/adv_update_date.txt",
    "./cache/masterdb_update_date.txt",
]
_cache_backups = {}


def _backup_caches():
    for path in _CACHE_FILES:
        if os.path.exists(path):
            with open(path, "r") as f:
                _cache_backups[path] = f.read()
    _test_log(f"Backed up {len(_cache_backups)} cache file(s)")


def _restore_caches():
    for path, content in _cache_backups.items():
        with open(path, "w") as f:
            f.write(content)
    _test_log(f"Restored {len(_cache_backups)} cache file(s)")


def _cleanup_test_db():
    """Remove DB_test.dat* files."""
    removed = 0
    for ext in ("", ".db", ".dir", ".bak", ".dat"):
        path = f"DB_test.dat{ext}" if ext else "DB_test.dat"
        if os.path.exists(path):
            os.remove(path)
            removed += 1
    if removed:
        _test_log(f"Cleaned up {removed} DB_test.dat file(s)")


# ============================================================
# Section 2: Parse CLI arguments
# ============================================================

parser = argparse.ArgumentParser(
    description="GakuToolkit Test Mode - run pipeline without external dependencies"
)
# Same flags as main.py
parser.add_argument("--fullupdate", action="store_true", help="Force full refresh")
parser.add_argument("--DEBUG", action="store_true", help="Enable debug logging")
parser.add_argument("--convert", action="store_true", help="Only conversion (skip update)")
parser.add_argument("--update", action="store_true", help="Only update (skip conversion)")
parser.add_argument("--adv", action="store_true", help="Only ADV pipeline")
parser.add_argument("--masterdb", action="store_true", help="Only MasterDB pipeline")
parser.add_argument("--generic", action="store_true", help="Only Generic pipeline")
parser.add_argument("--localization", action="store_true", help="Only Localization pipeline")
parser.add_argument("--masterdb2", action="store_true", help="Use MasterDB v2 format")
# Test-mode specific flags
parser.add_argument(
    "--isolate-output",
    action="store_true",
    help="Redirect output to test_output/ instead of output/",
)
parser.add_argument(
    "--isolate-drive",
    action="store_true",
    help="Copy res/drive/ to test_drive/ and work there",
)
parser.add_argument(
    "--mock-check-all",
    action="store_true",
    help="Make rclone.check() return all local files as changed",
)
parser.add_argument(
    "--cleanup",
    action="store_true",
    help="Remove test artifacts (DB_test.dat*, test_output/, test_drive/) and exit",
)
args = parser.parse_args()

# --- Handle --cleanup ---
if args.cleanup:
    _cleanup_test_db()
    for dirname in ("test_output", "test_drive"):
        if os.path.exists(dirname):
            shutil.rmtree(dirname)
            _test_log(f"Removed {dirname}/")
    for logfile in sorted(f for f in os.listdir(".") if f.startswith("test_output_python_") and f.endswith(".log")):
        os.remove(logfile)
        _test_log(f"Removed {logfile}")
    _test_log("Cleanup complete")
    sys.exit(0)

# --- Handle --mock-check-all ---
if args.mock_check_all:

    def _rclone_check_all(source, destination):
        """Return all local files as changed (simulates remote diff)."""
        _suppressed_calls["check"] += 1
        # Import here because scripts.helper is not yet loaded
        from scripts.helper import Helper_GetFilesFromDir

        local_path = destination if os.path.isdir(destination) else source
        if os.path.isdir(local_path):
            files = Helper_GetFilesFromDir(local_path)
            result = [["*", os.path.relpath(f[0], local_path)] for f in files]
            _test_log(f"rclone.check() -> {len(result)} files (mock-check-all)")
            return result
        _test_log(f"rclone.check() -> [] (path not found: {local_path})")
        return []

    _fake_scripts_rclone.check = _rclone_check_all
    sys.modules["scripts.rclone"].check = _rclone_check_all


# ============================================================
# Section 3: Import main module (safe now)
# ============================================================

import main as main_module  # noqa: E402

# Ensure gspread fake is bound on the scripts package object.
# Python may skip parent-child binding when a module is pre-injected
# into sys.modules without going through the normal import machinery.
if "scripts" in sys.modules:
    sys.modules["scripts"].gspread = _fake_scripts_gspread
    sys.modules["scripts"].rclone = _fake_scripts_rclone


# ============================================================
# Section 4: Path isolation
# ============================================================

if args.isolate_output:
    TEST_OUTPUT_DIR = os.path.join(os.getcwd(), "test_output")
    for subdir in (
        "local-files/resource",
        "local-files/masterTrans",
        "local-files/genericTrans/lyrics",
    ):
        os.makedirs(os.path.join(TEST_OUTPUT_DIR, subdir), exist_ok=True)

    import scripts.helper as _helper

    _helper.OUTPUT_PATH = TEST_OUTPUT_DIR

    import scripts.adv as _adv_mod

    _adv_mod.ADV_OUTPUT_PATH = os.path.join(TEST_OUTPUT_DIR, "local-files", "resource")

    import scripts.masterdb2 as _mdb2_mod

    _mdb2_mod.MASTERDB_OUTPUT_PATH = os.path.join(
        TEST_OUTPUT_DIR, "local-files", "masterTrans"
    )

    import scripts.generic as _generic_mod

    _generic_mod.GENERIC_OUTPUT_PATH = os.path.join(
        TEST_OUTPUT_DIR, "local-files", "genericTrans"
    )
    _generic_mod.GENERIC_OUTPUT_LYRICS_PATH = os.path.join(
        TEST_OUTPUT_DIR, "local-files", "genericTrans", "lyrics"
    )

    import scripts.localization as _loc_mod

    _loc_mod.LOCALIZATION_OUTPUT_PATH = os.path.join(
        TEST_OUTPUT_DIR, "local-files", "localization.json"
    )

    # Patch builtins.open for version.txt (hardcoded in main.py:34)
    _original_open = builtins.open

    def _patched_open(file, *a, **kw):
        if isinstance(file, str) and "output/version.txt" in file:
            redirected = file.replace("output/", "test_output/")
            os.makedirs(os.path.dirname(os.path.abspath(redirected)), exist_ok=True)
            _test_log(f"open('{file}') -> '{redirected}'")
            return _original_open(redirected, *a, **kw)
        return _original_open(file, *a, **kw)

    builtins.open = _patched_open
    _test_log(f"Output isolated to {TEST_OUTPUT_DIR}/")

if args.isolate_drive:
    _DRIVE_PATH_ORIG = os.path.join(os.getcwd(), "res", "drive")
    TEST_DRIVE_DIR = os.path.join(os.getcwd(), "test_drive")
    if not os.path.exists(TEST_DRIVE_DIR):
        _test_log("Copying res/drive/ -> test_drive/ (this may take a moment)...")
        shutil.copytree(_DRIVE_PATH_ORIG, TEST_DRIVE_DIR)
    else:
        _test_log("Using existing test_drive/")

    import scripts.helper as _helper

    _helper.DRIVE_PATH = TEST_DRIVE_DIR

    import scripts.adv as _adv_mod

    _adv_mod.ADV_DRIVE_PATH = os.path.join(TEST_DRIVE_DIR, "text assets")

    import scripts.masterdb2 as _mdb2_mod

    _mdb2_mod.MASTERDB_DRIVE_PATH = os.path.join(TEST_DRIVE_DIR, "masterDB")
    _mdb2_mod.MASTERDB2_DRIVE_PATH = os.path.join(TEST_DRIVE_DIR, "masterDB2")

    import scripts.generic as _generic_mod

    _generic_mod.GENERIC_DRIVE_PATH = os.path.join(TEST_DRIVE_DIR, "GenericTrans")
    _generic_mod.GENERIC_DRIVE_LYRICS_PATH = os.path.join(
        TEST_DRIVE_DIR, "GenericTrans", "lyrics"
    )

    import scripts.localization as _loc_mod

    _loc_mod.LOCALIZATION_DRIVE_PATH = os.path.join(
        TEST_DRIVE_DIR, "localization.xlsx"
    )
    _test_log(f"Drive isolated to {TEST_DRIVE_DIR}/")

# Warn if update mode without drive isolation
if not args.isolate_drive:
    will_update = True
    if args.convert and not args.update:
        will_update = False
    if will_update:
        _test_log(
            "WARNING: Update mode may write xlsx to res/drive/. "
            "Use --isolate-drive for full isolation."
        )


# ============================================================
# Section 5: Configure and execute
# ============================================================

# Set main module variables
if args.fullupdate:
    main_module.full_update = True

if args.convert or args.update:
    main_module.CONVERT = False
    main_module.UPDATE = False
    if args.convert:
        main_module.CONVERT = True
    if args.update:
        main_module.UPDATE = True

if args.masterdb2:
    main_module.USE_MASTERDB2 = True

# Configure logging
from scripts.log import logger  # noqa: E402

if args.DEBUG:
    logger.setLevel("DEBUG")
else:
    logger.setLevel("INFO")

_log_handler = logging.FileHandler(
    f"test_output_python_{datetime.today().strftime('%Y%m%d_%H%M%S')}.log"
)
logger.addHandler(_log_handler)

# Determine module selection
ADV = True
MASTERDB = True
GENERIC = True
LOCALIZATION = True
if args.adv or args.masterdb or args.generic or args.localization:
    ADV = args.adv
    MASTERDB = args.masterdb
    GENERIC = args.generic
    LOCALIZATION = args.localization

# Print banner
_test_log("=" * 55)
_test_log("  GakuToolkit TEST MODE")
_test_log(f"  Time: {datetime.today().strftime('%Y-%m-%d %H:%M:%S')}")
_test_log(
    f"  Flags: convert={main_module.CONVERT}, update={main_module.UPDATE}, "
    f"fullupdate={main_module.full_update}"
)
_test_log(
    f"  Modules: ADV={ADV}, MASTERDB={MASTERDB}, "
    f"GENERIC={GENERIC}, LOCALIZATION={LOCALIZATION}"
)
_test_log(
    f"  Isolation: output={args.isolate_output}, drive={args.isolate_drive}"
)
_test_log("=" * 55)

# Execute with cache protection
_backup_caches()
try:
    main_module.main(ADV, MASTERDB, GENERIC, LOCALIZATION)
finally:
    _restore_caches()
    _cleanup_test_db()

    # Print summary
    total_suppressed = sum(_suppressed_calls.values())
    _test_log("=" * 55)
    _test_log(f"  Suppressed {total_suppressed} external call(s):")
    for call_type, count in _suppressed_calls.items():
        if count > 0:
            _test_log(f"    rclone.{call_type}(): {count}")
    _test_log("=" * 55)
    _test_log("Test run complete.")
