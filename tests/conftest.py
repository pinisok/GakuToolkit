"""
Shared test fixtures for GakuToolkit.

Injects fake rclone/gspread modules into sys.modules BEFORE any
scripts.* import, so tests run without external dependencies.
This file is loaded by pytest before any test module.
"""

import sys
import os
import types
import logging
import shelve
import shutil

import pytest

# ============================================================
# Module injection (same strategy as test_run.py)
# Must happen at module level, before any test imports scripts.*
# ============================================================

# --- Fake rclone_python package ---
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

# --- Fake scripts.rclone module ---
_fake_scripts_rclone = types.ModuleType("scripts.rclone")
_fake_scripts_rclone.__package__ = "scripts"
_fake_scripts_rclone.init = lambda: None
_fake_scripts_rclone.copy = lambda src, dst: None
_fake_scripts_rclone.sync = lambda src, dst: None
_fake_scripts_rclone.check = lambda src, dst: []
_fake_scripts_rclone.link = lambda dst: "https://test/TEST_ID"
_fake_scripts_rclone.logger = logging.getLogger("rclone.test")

sys.modules["scripts.rclone"] = _fake_scripts_rclone

# --- Fake scripts.gspread module ---
_fake_scripts_gspread = types.ModuleType("scripts.gspread")
_fake_scripts_gspread.__package__ = "scripts"
_fake_scripts_gspread.log = lambda logs: None
sys.modules["scripts.gspread"] = _fake_scripts_gspread

# --- Fake third-party gspread ---
_fake_gspread_lib = types.ModuleType("gspread")
_fake_gspread_lib.service_account = lambda *a, **kw: None
sys.modules["gspread"] = _fake_gspread_lib

_fake_gspread_fmt = types.ModuleType("gspread_formatting")
_fake_gspread_fmt.set_column_width = lambda *a, **kw: None
sys.modules["gspread_formatting"] = _fake_gspread_fmt

# --- Patch shelve.open to use test copy ---
_original_shelve_open = shelve.open


def _patched_shelve_open(filename, *args, **kwargs):
    basename = os.path.basename(filename)
    if basename.startswith("DB") and "test" not in basename.lower():
        redirected = filename.replace("DB", "DB_test")
        return _original_shelve_open(redirected, *args, **kwargs)
    return _original_shelve_open(filename, *args, **kwargs)


shelve.open = _patched_shelve_open

# ============================================================
# Now safe to import project modules
# ============================================================


def _ensure_bindings():
    if "scripts" in sys.modules:
        sys.modules["scripts"].rclone = _fake_scripts_rclone
        sys.modules["scripts"].gspread = _fake_scripts_gspread


# ============================================================
# Fixtures
# ============================================================

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


@pytest.fixture(autouse=True)
def _chdir_to_project():
    """Ensure working directory is project root for all tests."""
    original_cwd = os.getcwd()
    os.chdir(PROJECT_ROOT)
    _ensure_bindings()
    yield
    os.chdir(original_cwd)


@pytest.fixture
def tmp_output(tmp_path):
    """Provide a temporary output directory."""
    for subdir in (
        "local-files/resource",
        "local-files/masterTrans",
        "local-files/genericTrans/lyrics",
    ):
        (tmp_path / subdir).mkdir(parents=True, exist_ok=True)
    return tmp_path


@pytest.fixture
def shelve_test_cleanup():
    """Clean up DB_test.dat files after test."""
    yield
    for ext in ("", ".db", ".dir", ".bak", ".dat"):
        path = f"DB_test.dat{ext}" if ext else "DB_test.dat"
        if os.path.exists(path):
            os.remove(path)
