"""Tests for ADV (Text Assets) pipeline using synthetic fixtures."""

import os
from io import StringIO

import pytest
import pandas as pd

from tests.fixtures.create_fixtures import create_adv_xlsx, create_adv_txt
from scripts.adv import (
    _encode,
    _processEMtag,
    _internalGetOutputPath,
    _internalOverrideXlsxColumn,
    _internalXlsxRecordsProcess,
    XlsxToCsv,
    XlsxToTxt,
    ADV_BLACKLIST_FILE,
    ADV_BLACKLIST_FOLDER,
)


# ============================================================
# Pure unit tests (no fixtures needed)
# ============================================================


class TestEncode:
    def test_newline_escape(self):
        assert _encode("line1\nline2") == "line1\\nline2"

    def test_carriage_return_escape(self):
        assert _encode("line1\rline2") == "line1\\rline2"

    def test_tilde_to_fullwidth(self):
        assert _encode("hello~world") == "hello～world"

    def test_4_dots_to_ellipsis(self):
        assert _encode("wait....") == "wait……"

    def test_3_dots_to_ellipsis(self):
        assert _encode("wait...") == "wait…"

    def test_2_dots_to_ellipsis(self):
        assert _encode("wait..") == "wait…"

    def test_single_dot_unchanged(self):
        assert _encode("wait.") == "wait."

    def test_empty_string(self):
        assert _encode("") == ""

    def test_combined(self):
        result = _encode("hello~\nworld....")
        assert "～" in result
        assert "\\n" in result
        assert "……" in result


class TestProcessEMtag:
    def test_splits_words_in_em(self):
        assert _processEMtag("<em>hello world</em>") == "<em>hello</em> <em>world</em>"

    def test_no_em_unchanged(self):
        assert _processEMtag("no tags here") == "no tags here"

    def test_empty_string(self):
        assert _processEMtag("") == ""

    def test_single_word_em(self):
        assert _processEMtag("<em>hello</em>") == "<em>hello</em>"

    def test_multiple_em_tags(self):
        result = _processEMtag("<em>a b</em> text <em>c d</em>")
        assert "<em>a</em> <em>b</em>" in result
        assert "<em>c</em> <em>d</em>" in result

    def test_unclosed_em_unchanged(self):
        assert _processEMtag("<em>hello") == "<em>hello"

    def test_no_opening_em(self):
        assert _processEMtag("hello</em>") == "hello</em>"


class TestGetOutputPath:
    def test_cidol(self):
        assert _internalGetOutputPath("adv_cidol-amao-3-001_01.txt") == "cidol"

    def test_pstory_includes_number(self):
        assert _internalGetOutputPath("adv_pstory_001_hmsz_after.txt") == "pstory_001"

    def test_pevent(self):
        assert _internalGetOutputPath("adv_pevent_001_fktn.txt") == "pevent"

    def test_produce(self):
        assert _internalGetOutputPath("adv_produce-refresh_002.txt") == "produce"

    def test_live(self):
        assert _internalGetOutputPath("adv_live_amao_001.txt") == "live"

    def test_csprt(self):
        assert _internalGetOutputPath("adv_csprt-3-0090_03.txt") == "csprt"

    def test_unit(self):
        assert _internalGetOutputPath("adv_unit_01-01_01.txt") == "unit"


class TestBlacklist:
    def test_blacklist_files(self):
        assert "musics.txt" in ADV_BLACKLIST_FILE
        assert "adv_warmup.txt" in ADV_BLACKLIST_FILE

    def test_blacklist_folders(self):
        assert "pstep" in ADV_BLACKLIST_FOLDER
        assert "pweek" in ADV_BLACKLIST_FOLDER


class TestXlsxColumnOverride:
    def test_renames_columns(self):
        df = pd.DataFrame(
            {"c0": [1], "c1": ["n"], "c2": [""], "c3": ["t"], "c4": ["tr"]}
        )
        _internalOverrideXlsxColumn(df)
        assert list(df.columns) == [
            "id", "name", "translated name", "text", "translated text"
        ]


class TestXlsxRecordsProcess:
    def test_integer_id_to_string(self):
        records = [
            {"id": 123, "name": "t", "translated name": "",
             "text": "hello", "translated text": "안녕"}
        ]
        _internalXlsxRecordsProcess(records)
        assert records[0]["id"] == "123"

    def test_character_name_translation(self):
        records = [
            {"id": "1", "name": "麻央", "translated name": "",
             "text": "hello", "translated text": "안녕"}
        ]
        _internalXlsxRecordsProcess(records)
        assert records[0]["name"] == "마오"

    def test_translated_name_priority(self):
        records = [
            {"id": "1", "name": "麻央", "translated name": "커스텀",
             "text": "hello", "translated text": "안녕"}
        ]
        _internalXlsxRecordsProcess(records)
        assert records[0]["name"] == "커스텀"

    def test_empty_translation_raises_for_normal_row(self):
        records = [
            {"id": "1", "name": "t", "translated name": "",
             "text": "hello", "translated text": ""}
        ]
        with pytest.raises(Exception, match="빈 줄"):
            _internalXlsxRecordsProcess(records)

    def test_empty_translation_ok_for_info_row(self):
        records = [
            {"id": "info", "name": "f.txt", "translated name": "",
             "text": "", "translated text": ""}
        ]
        _internalXlsxRecordsProcess(records)  # Should not raise

    def test_empty_translation_ok_for_translator_row(self):
        records = [
            {"id": "译者", "name": "None", "translated name": "",
             "text": "", "translated text": ""}
        ]
        _internalXlsxRecordsProcess(records)  # Should not raise

    def test_encode_applied_to_text(self):
        records = [
            {"id": "1", "name": "t", "translated name": "",
             "text": "hello\nworld", "translated text": "안녕\n세계"}
        ]
        _internalXlsxRecordsProcess(records)
        assert "\\n" in records[0]["text"]
        assert "\\n" in records[0]["translated text"]


# ============================================================
# Integration tests with synthetic fixtures
# ============================================================


class TestXlsxToCsvSynthetic:
    """Test XlsxToCsv with synthetic xlsx data."""

    def test_produces_valid_csv(self, tmp_path):
        """Synthetic xlsx should produce valid CSV with expected headers."""
        xlsx_path = str(tmp_path / "adv_test.xlsx")
        create_adv_xlsx(xlsx_path, [
            {"id": "0", "name": "麻央", "translated name": "",
             "text": "テストメッセージ", "translated text": "테스트 메시지"},
        ])

        with open(xlsx_path, "rb") as fp:
            csv_io = StringIO()
            XlsxToCsv(fp, csv_io, "test.txt")

        csv_io.seek(0)
        lines = csv_io.readlines()
        assert len(lines) >= 2  # header + data + info + translator
        header = lines[0].strip()
        assert "id" in header
        assert "name" in header
        assert "text" in header
        assert "trans" in header

    def test_character_name_in_csv(self, tmp_path):
        """Character name should be translated in CSV output."""
        xlsx_path = str(tmp_path / "adv_test.xlsx")
        create_adv_xlsx(xlsx_path, [
            {"id": "0", "name": "麻央", "translated name": "",
             "text": "hello", "translated text": "안녕"},
        ])

        with open(xlsx_path, "rb") as fp:
            csv_io = StringIO()
            XlsxToCsv(fp, csv_io, "test.txt")

        csv_io.seek(0)
        content = csv_io.read()
        assert "마오" in content  # CHARACTER_REGEX_TRANS_MAP applied

    def test_multiple_rows(self, tmp_path):
        """Multiple rows should all appear in CSV."""
        xlsx_path = str(tmp_path / "adv_test.xlsx")
        create_adv_xlsx(xlsx_path, [
            {"id": "0", "name": "A", "translated name": "",
             "text": "第一行", "translated text": "첫줄"},
            {"id": "0", "name": "B", "translated name": "",
             "text": "第二行", "translated text": "둘째줄"},
        ])

        with open(xlsx_path, "rb") as fp:
            csv_io = StringIO()
            XlsxToCsv(fp, csv_io, "test.txt")

        csv_io.seek(0)
        lines = csv_io.readlines()
        # header + 2 data + info + translator = 5
        assert len(lines) == 5


class TestXlsxToTxtSynthetic:
    """Test full XlsxToTxt conversion with synthetic data."""

    def test_single_message(self, tmp_path):
        """Convert a 1-message synthetic ADV file end to end."""
        # Create original txt with one message
        txt_path = str(tmp_path / "adv_test.txt")
        create_adv_txt(txt_path, [
            {"tag": "message", "text": "テストメッセージ", "name": "麻央"},
        ])

        # Create matching xlsx with translation
        xlsx_path = str(tmp_path / "adv_test.xlsx")
        create_adv_xlsx(xlsx_path, [
            {"id": "0000000000000", "name": "麻央", "translated name": "마오",
             "text": "テストメッセージ", "translated text": "테스트 메시지"},
        ])

        output_path = str(tmp_path / "output.txt")
        XlsxToTxt(xlsx_path, output_path, txt_path)

        assert os.path.exists(output_path)
        with open(output_path, "r", encoding="utf-8") as f:
            content = f.read()
        # Translation should be applied
        assert "테스트 메시지" in content
        # Original name should be replaced
        assert "마오" in content

    def test_two_messages(self, tmp_path):
        """Convert 2 messages."""
        txt_path = str(tmp_path / "adv_test.txt")
        create_adv_txt(txt_path, [
            {"tag": "message", "text": "一番目", "name": "麻央"},
            {"tag": "message", "text": "二番目", "name": "燕"},
        ])

        xlsx_path = str(tmp_path / "adv_test.xlsx")
        create_adv_xlsx(xlsx_path, [
            {"id": "0000000000000", "name": "麻央", "translated name": "마오",
             "text": "一番目", "translated text": "첫번째"},
            {"id": "0000000000000", "name": "燕", "translated name": "츠바메",
             "text": "二番目", "translated text": "두번째"},
        ])

        output_path = str(tmp_path / "output.txt")
        XlsxToTxt(xlsx_path, output_path, txt_path)

        with open(output_path, "r", encoding="utf-8") as f:
            content = f.read()
        assert "첫번째" in content
        assert "두번째" in content
        assert "마오" in content
        assert "츠바메" in content
