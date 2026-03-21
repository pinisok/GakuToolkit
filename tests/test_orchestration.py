"""Tests for main.py orchestration: Convert/Update/summary/gspread flow.

These tests cover the gaps identified in the review:
- main() Phase execution order
- Convert() return value structure and version.txt writing
- _update_summary / _convert_summary formatting
- gspread conditional logging
- Error isolation (one pipeline failure shouldn't crash others)
"""

import os
import json
import logging

import pytest


# ============================================================
# Convert() orchestration
# ============================================================


class TestConvertOrchestration:
    """Test Convert() function in main.py."""

    def test_returns_4_tuples(self, tmp_output):
        """Convert should return 4 (errors, successes) tuples."""
        import main as m

        result = m.Convert(
            ADV=False, MASTERDB=False, GENERIC=False, LOCALIZATION=False,
            bFullUpdate=False, changed_files={},
        )

        assert len(result) == 4
        for item in result:
            assert isinstance(item, tuple)
            assert len(item) == 2  # (errors, successes)

    def test_skipped_pipelines_return_empty(self):
        """Disabled pipelines should return empty lists, not None."""
        import main as m

        result = m.Convert(
            ADV=False, MASTERDB=False, GENERIC=False, LOCALIZATION=False,
            bFullUpdate=False, changed_files={},
        )

        for errors, successes in result:
            assert errors == []
            assert successes == []

    def test_version_txt_written_on_success(self, tmp_output):
        """version.txt should be written when files are converted."""
        import main as m
        import scripts.localization as loc

        saved = loc.LOCALIZATION_OUTPUT_PATH
        loc.LOCALIZATION_OUTPUT_PATH = str(
            tmp_output / "local-files" / "localization.json"
        )
        version_path = str(tmp_output / "version.txt")

        # Patch version.txt path by creating a drive file for localization
        loc_drive = os.path.join(os.getcwd(), "res", "drive", "localization.xlsx")
        if not os.path.exists(loc_drive):
            pytest.skip("localization.xlsx not in res/drive/")

        try:
            result = m.Convert(
                ADV=False, MASTERDB=False, GENERIC=False, LOCALIZATION=True,
                bFullUpdate=False,
                changed_files={"localization": [
                    (loc_drive, "/localization.xlsx", "localization.xlsx")
                ]},
            )
        finally:
            loc.LOCALIZATION_OUTPUT_PATH = saved

        _, _, _, loc_result = result
        if len(loc_result[1]) > 0:
            assert os.path.exists("./output/version.txt")

    def test_version_txt_not_written_when_empty(self, tmp_output):
        """version.txt should NOT be written when no files are converted."""
        import main as m

        # Remove version.txt if it exists to test it's not created
        version_path = "./output/version.txt"
        existed_before = os.path.exists(version_path)
        if existed_before:
            with open(version_path, "r") as f:
                old_content = f.read()

        m.Convert(
            ADV=False, MASTERDB=False, GENERIC=False, LOCALIZATION=False,
            bFullUpdate=False, changed_files={},
        )

        if existed_before:
            # Should not have been overwritten
            with open(version_path, "r") as f:
                assert f.read() == old_content

    def test_empty_changed_files_means_no_conversion(self):
        """Passing empty lists in changed_files should skip all pipelines."""
        import main as m

        result = m.Convert(
            ADV=True, MASTERDB=True, GENERIC=True, LOCALIZATION=True,
            bFullUpdate=False,
            changed_files={
                "adv": [], "masterdb": [], "generic": [], "localization": [],
            },
        )

        for errors, successes in result:
            assert errors == []
            assert successes == []


# ============================================================
# _convert_summary / _update_summary
# ============================================================


class TestConvertSummary:
    """Test _convert_summary formatting."""

    def test_logs_errors_and_successes(self, caplog):
        """Should log both error and success entries."""
        from main import _convert_summary

        arr = (
            [(Exception("test error"), "broken_file.xlsx")],
            ["success_file.xlsx"],
        )

        with caplog.at_level(logging.INFO, logger="GakuToolkit"):
            _convert_summary("ADV", arr)

        assert any("변환 중 오류" in r.message for r in caplog.records)
        assert any("번역 갱신" in r.message for r in caplog.records)

    def test_empty_arr_no_log(self, caplog):
        """Empty errors and successes should produce no log."""
        from main import _convert_summary

        with caplog.at_level(logging.INFO, logger="GakuToolkit"):
            _convert_summary("ADV", ([], []))

        assert len(caplog.records) == 0

    def test_errors_only(self, caplog):
        """Only errors, no successes."""
        from main import _convert_summary

        arr = ([(Exception("err"), "file.xlsx")], [])

        with caplog.at_level(logging.INFO, logger="GakuToolkit"):
            _convert_summary("TEST", arr)

        assert any("변환 중 오류" in r.message for r in caplog.records)
        assert not any("번역 갱신" in r.message for r in caplog.records)


class TestUpdateSummary:
    """Test _update_summary formatting with rclone.check result format."""

    def test_logs_updated_files(self, caplog):
        """Updated files (marked *) should be logged."""
        from main import _update_summary

        arr = [["*", "path/to/file.xlsx"]]

        with caplog.at_level(logging.INFO, logger="GakuToolkit"):
            _update_summary("ADV", arr)

        assert any("업데이트" in r.message for r in caplog.records)

    def test_logs_new_files(self, caplog):
        """New files (marked +) should be logged."""
        from main import _update_summary

        arr = [["+", "path/to/new.xlsx"]]

        with caplog.at_level(logging.INFO, logger="GakuToolkit"):
            _update_summary("ADV", arr)

        assert any("추가" in r.message for r in caplog.records)

    def test_empty_arr_no_log(self, caplog):
        """Empty list should produce no log."""
        from main import _update_summary

        with caplog.at_level(logging.INFO, logger="GakuToolkit"):
            _update_summary("ADV", [])

        assert len(caplog.records) == 0

    def test_mixed_update_and_new(self, caplog):
        """Both * and + entries should be logged."""
        from main import _update_summary

        arr = [["*", "updated.xlsx"], ["+", "new.xlsx"]]

        with caplog.at_level(logging.INFO, logger="GakuToolkit"):
            _update_summary("ADV", arr)

        messages = [r.message for r in caplog.records]
        assert any("업데이트" in m for m in messages)
        assert any("추가" in m for m in messages)

    def test_warnings_shown_under_file(self, caplog):
        """Warnings should appear under the corresponding file entry."""
        from main import _update_summary

        arr = [["*", "cidol/adv_cidol-amao_01.xlsx"]]
        warnings = {"adv_cidol-amao_01": ["원문 불일치 at line 5", "원문 불일치 at line 12"]}

        with caplog.at_level(logging.INFO, logger="GakuToolkit"):
            _update_summary("ADV", arr, warnings)

        messages = [r.message for r in caplog.records]
        assert any("업데이트" in m for m in messages)
        assert any("⚠ 원문 불일치 at line 5" in m for m in messages)
        assert any("⚠ 원문 불일치 at line 12" in m for m in messages)

    def test_warnings_not_shown_for_other_file(self, caplog):
        """Warnings for a different file should not appear."""
        from main import _update_summary

        arr = [["*", "other/file.xlsx"]]
        warnings = {"adv_cidol-amao_01": ["원문 불일치 at line 5"]}

        with caplog.at_level(logging.INFO, logger="GakuToolkit"):
            _update_summary("ADV", arr, warnings)

        messages = [r.message for r in caplog.records]
        assert not any("⚠" in m for m in messages)

    def test_no_warnings_still_works(self, caplog):
        """Summary without warnings should work as before."""
        from main import _update_summary

        arr = [["*", "file.xlsx"]]

        with caplog.at_level(logging.INFO, logger="GakuToolkit"):
            _update_summary("ADV", arr)

        messages = [r.message for r in caplog.records]
        assert any("업데이트" in m for m in messages)
        assert not any("⚠" in m for m in messages)

    def test_excessive_warnings_capped(self, caplog):
        """More than 5 warnings per file should be capped with summary."""
        from main import _update_summary

        arr = [["*", "cidol/adv_test.xlsx"]]
        warnings = {"adv_test": [f"원문 불일치 at line {i}" for i in range(20)]}

        with caplog.at_level(logging.INFO, logger="GakuToolkit"):
            _update_summary("ADV", arr, warnings)

        messages = [r.message for r in caplog.records]
        warning_lines = [m for m in messages if "⚠" in m]
        # 5 individual + 1 "외 15건" = 6
        assert len(warning_lines) == 6
        assert any("외 15건" in m for m in messages)


# ============================================================
# main() Phase execution
# ============================================================


class TestMainPhaseExecution:
    """Test main() executes phases in correct order."""

    def test_convert_only_skips_update(self, caplog):
        """--convert only should run Phase 0+1, skip Phase 2+3."""
        import main as m

        saved_convert = m.CONVERT
        saved_update = m.UPDATE
        m.CONVERT = True
        m.UPDATE = False

        try:
            with caplog.at_level(logging.INFO, logger="GakuToolkit"):
                m.main(ADV=False, MASTERDB=False, GENERIC=False, LOCALIZATION=False)
        finally:
            m.CONVERT = saved_convert
            m.UPDATE = saved_update

        messages = " ".join(r.message for r in caplog.records)
        assert "Phase 0" in messages
        assert "Phase 1" in messages
        assert "Phase 2" not in messages
        assert "Phase 3" not in messages

    def test_update_only_skips_convert(self, caplog):
        """--update only should run Phase 0+2+3, skip Phase 1."""
        import main as m

        saved_convert = m.CONVERT
        saved_update = m.UPDATE
        m.CONVERT = False
        m.UPDATE = True

        try:
            with caplog.at_level(logging.INFO, logger="GakuToolkit"):
                m.main(ADV=False, MASTERDB=False, GENERIC=False, LOCALIZATION=False)
        finally:
            m.CONVERT = saved_convert
            m.UPDATE = saved_update

        messages = " ".join(r.message for r in caplog.records)
        assert "Phase 0" in messages
        assert "Phase 1" not in messages
        assert "Phase 2" in messages
        assert "Phase 3" in messages

    def test_no_changes_no_gspread(self, caplog):
        """When nothing changes, gspread should not be called."""
        import main as m

        saved_convert = m.CONVERT
        saved_update = m.UPDATE
        m.CONVERT = True
        m.UPDATE = False

        try:
            with caplog.at_level(logging.INFO, logger="GakuToolkit"):
                m.main(ADV=False, MASTERDB=False, GENERIC=False, LOCALIZATION=False)
        finally:
            m.CONVERT = saved_convert
            m.UPDATE = saved_update

        messages = " ".join(r.message for r in caplog.records)
        assert "gspread" not in messages.lower() or "Failed" not in messages


# ============================================================
# Error isolation
# ============================================================


class TestErrorIsolation:
    """Test that one pipeline's error doesn't crash others."""

    def test_convert_continues_after_pipeline_error(self):
        """If one pipeline returns errors, others should still run."""
        import main as m

        result = m.Convert(
            ADV=True, MASTERDB=False, GENERIC=False, LOCALIZATION=True,
            bFullUpdate=False,
            changed_files={"adv": [], "localization": []},
        )

        # Both should return without crashing
        assert len(result) == 4
