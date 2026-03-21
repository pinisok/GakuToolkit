"""Tests for scripts/helper.py utility functions."""

import os
import tempfile
from datetime import datetime

import pytest

from scripts.helper import (
    Helper_GetFilesFromDir,
    Helper_GetFilesFromDirByCheck,
    CHARACTER_REGEX_TRANS_MAP,
    REGEX_DOTS_3,
    REGEX_DOTS_4_TO_6,
    load_cache_date,
    save_cache_date,
)


class TestHelperGetFilesFromDir:
    """Tests for Helper_GetFilesFromDir."""

    def test_finds_files_with_suffix(self, tmp_path):
        (tmp_path / "file1.txt").write_text("a")
        (tmp_path / "file2.txt").write_text("b")
        (tmp_path / "file3.json").write_text("c")

        result = Helper_GetFilesFromDir(str(tmp_path), suffix=".txt")

        filenames = [r[2] for r in result]
        assert "file1.txt" in filenames
        assert "file2.txt" in filenames
        assert "file3.json" not in filenames

    def test_finds_files_with_prefix(self, tmp_path):
        (tmp_path / "adv_test.txt").write_text("a")
        (tmp_path / "other_test.txt").write_text("b")

        result = Helper_GetFilesFromDir(str(tmp_path), prefix="adv_")

        filenames = [r[2] for r in result]
        assert "adv_test.txt" in filenames
        assert "other_test.txt" not in filenames

    def test_finds_files_with_both_filters(self, tmp_path):
        (tmp_path / "adv_test.txt").write_text("a")
        (tmp_path / "adv_test.json").write_text("b")
        (tmp_path / "other.txt").write_text("c")

        result = Helper_GetFilesFromDir(str(tmp_path), suffix=".txt", prefix="adv_")

        filenames = [r[2] for r in result]
        assert filenames == ["adv_test.txt"]

    def test_recurses_subdirectories(self, tmp_path):
        sub = tmp_path / "subdir"
        sub.mkdir()
        (sub / "nested.txt").write_text("a")
        (tmp_path / "top.txt").write_text("b")

        result = Helper_GetFilesFromDir(str(tmp_path), suffix=".txt")

        filenames = [r[2] for r in result]
        assert "nested.txt" in filenames
        assert "top.txt" in filenames

    def test_returns_empty_for_no_matches(self, tmp_path):
        (tmp_path / "file.json").write_text("a")

        result = Helper_GetFilesFromDir(str(tmp_path), suffix=".txt")

        assert result == []

    def test_returns_tuple_of_three(self, tmp_path):
        (tmp_path / "test.txt").write_text("a")

        result = Helper_GetFilesFromDir(str(tmp_path), suffix=".txt")

        assert len(result) == 1
        abs_path, rel_path, filename = result[0]
        assert os.path.isabs(abs_path)
        assert filename == "test.txt"

    def test_no_filters_returns_all(self, tmp_path):
        (tmp_path / "a.txt").write_text("a")
        (tmp_path / "b.json").write_text("b")

        result = Helper_GetFilesFromDir(str(tmp_path))

        assert len(result) == 2


class TestHelperGetFilesFromDirByCheck:
    """Tests for Helper_GetFilesFromDirByCheck."""

    def test_filters_deleted_files(self, tmp_path):
        (tmp_path / "modified.xlsx").write_text("a")
        (tmp_path / "deleted.xlsx").write_text("b")

        check_result = [
            ["*", "modified.xlsx"],
            ["-", "deleted.xlsx"],
        ]

        result = Helper_GetFilesFromDirByCheck(check_result, str(tmp_path))

        filenames = [r[2] for r in result]
        assert "modified.xlsx" in filenames
        assert "deleted.xlsx" not in filenames

    def test_includes_new_and_modified(self, tmp_path):
        (tmp_path / "new.xlsx").write_text("a")
        (tmp_path / "mod.xlsx").write_text("b")

        check_result = [
            ["+", "new.xlsx"],
            ["*", "mod.xlsx"],
        ]

        result = Helper_GetFilesFromDirByCheck(check_result, str(tmp_path))

        filenames = [r[2] for r in result]
        assert "new.xlsx" in filenames
        assert "mod.xlsx" in filenames

    def test_applies_suffix_filter(self, tmp_path):
        (tmp_path / "file.xlsx").write_text("a")
        (tmp_path / "file.txt").write_text("b")

        check_result = [
            ["*", "file.xlsx"],
            ["*", "file.txt"],
        ]

        result = Helper_GetFilesFromDirByCheck(
            check_result, str(tmp_path), suffix=".xlsx"
        )

        filenames = [r[2] for r in result]
        assert "file.xlsx" in filenames
        assert "file.txt" not in filenames

    def test_applies_prefix_filter(self, tmp_path):
        (tmp_path / "adv_file.xlsx").write_text("a")
        (tmp_path / "other.xlsx").write_text("b")

        check_result = [
            ["*", "adv_file.xlsx"],
            ["*", "other.xlsx"],
        ]

        result = Helper_GetFilesFromDirByCheck(
            check_result, str(tmp_path), prefix="adv_"
        )

        filenames = [r[2] for r in result]
        assert "adv_file.xlsx" in filenames
        assert "other.xlsx" not in filenames

    def test_empty_check_result(self, tmp_path):
        result = Helper_GetFilesFromDirByCheck([], str(tmp_path))
        assert result == []


class TestCharacterTransMap:
    """Tests for CHARACTER_REGEX_TRANS_MAP."""

    def test_map_has_entries(self):
        assert len(CHARACTER_REGEX_TRANS_MAP) > 0

    def test_all_values_are_korean(self):
        for jp, kr in CHARACTER_REGEX_TRANS_MAP.items():
            assert len(kr) > 0, f"Empty translation for {jp}"

    def test_known_translations(self):
        assert CHARACTER_REGEX_TRANS_MAP["麻央"] == "마오"
        assert CHARACTER_REGEX_TRANS_MAP["ことね"] == "코토네"
        assert CHARACTER_REGEX_TRANS_MAP["手毬"] == "테마리"

    def test_fallback_for_unknown(self):
        unknown = "UnknownCharacter"
        result = CHARACTER_REGEX_TRANS_MAP.get(unknown, unknown)
        assert result == unknown


class TestRegexPatterns:
    """Tests for dot replacement regex patterns."""

    def test_4_to_6_dots_replaced(self):
        assert REGEX_DOTS_4_TO_6.sub("……", "test....end") == "test……end"
        assert REGEX_DOTS_4_TO_6.sub("……", "test.....end") == "test……end"
        assert REGEX_DOTS_4_TO_6.sub("……", "test......end") == "test……end"

    def test_3_dots_not_matched_by_4_to_6(self):
        assert REGEX_DOTS_4_TO_6.sub("……", "test...end") == "test...end"

    def test_2_to_3_dots_replaced(self):
        assert REGEX_DOTS_3.sub("…", "test..end") == "test…end"
        assert REGEX_DOTS_3.sub("…", "test...end") == "test…end"

    def test_single_dot_not_replaced(self):
        assert REGEX_DOTS_3.sub("…", "test.end") == "test.end"


class TestLoadCacheDate:
    """Tests for load_cache_date."""

    def test_reads_valid_date(self, tmp_path):
        cache_file = str(tmp_path / "cache.txt")
        with open(cache_file, "w") as f:
            f.write("2026-03-20 10:30:00")
        result = load_cache_date(cache_file)
        assert result == datetime(2026, 3, 20, 10, 30, 0)

    def test_returns_none_for_missing_file(self, tmp_path):
        result = load_cache_date(str(tmp_path / "nonexistent.txt"))
        assert result is None

    def test_returns_none_for_invalid_content(self, tmp_path):
        cache_file = str(tmp_path / "cache.txt")
        with open(cache_file, "w") as f:
            f.write("not a date")
        result = load_cache_date(cache_file)
        assert result is None

    def test_returns_none_for_empty_file(self, tmp_path):
        cache_file = str(tmp_path / "cache.txt")
        with open(cache_file, "w") as f:
            f.write("")
        result = load_cache_date(cache_file)
        assert result is None


class TestSaveCacheDate:
    """Tests for save_cache_date."""

    def test_writes_iso_date(self, tmp_path):
        cache_file = str(tmp_path / "cache.txt")
        save_cache_date(cache_file)
        with open(cache_file, "r") as f:
            content = f.read()
        # Should be parseable back
        parsed = datetime.fromisoformat(content)
        assert parsed.year >= 2026

    def test_creates_file(self, tmp_path):
        cache_file = str(tmp_path / "new_cache.txt")
        assert not os.path.exists(cache_file)
        save_cache_date(cache_file)
        assert os.path.exists(cache_file)

    def test_overwrites_existing(self, tmp_path):
        cache_file = str(tmp_path / "cache.txt")
        with open(cache_file, "w") as f:
            f.write("old content")
        save_cache_date(cache_file)
        with open(cache_file, "r") as f:
            content = f.read()
        assert content != "old content"


# ============================================================
# Conversion correctness tests (merged from test_conversion_correctness.py)
# ============================================================

class TestLoadCacheDateEdge:
    """Cache file edge cases."""

    def test_blank_first_line(self, tmp_path):
        """Blank first line should return None, not IndexError."""
        cache_file = str(tmp_path / "cache.txt")
        with open(cache_file, "w") as f:
            f.write("\n2026-03-22 10:00:00")
        result = load_cache_date(cache_file)
        assert result is None

    def test_multi_line_uses_first_only(self, tmp_path):
        """Only the first line should be parsed."""
        cache_file = str(tmp_path / "cache.txt")
        with open(cache_file, "w") as f:
            f.write("2026-03-22 10:00:00\n2026-01-01 00:00:00")
        result = load_cache_date(cache_file)
        assert result.year == 2026
        assert result.month == 3  # First line, not second


# ============================================================
# P3: _filter_adv_files tuple structure (P3-24)
# ============================================================


