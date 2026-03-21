"""Tests for Generic pipeline using synthetic fixtures."""

import os
import json

import pytest

from tests.fixtures.create_fixtures import create_generic_xlsx, create_lyrics_xlsx
from scripts.generic import (
    XlsxToJson as GenericXlsxToJson,
    GENERIC_FILE_LIST,
)
from scripts.helper import (
    Serialize as GenericSerialize,
    Deserialize as GenericDeserialize,
)


# ============================================================
# Serialize / Deserialize unit tests
# ============================================================


class TestSerializeDeserialize:
    def test_serialize_carriage_return(self):
        assert GenericSerialize("line1\rline2") == "line1\\rline2"

    def test_serialize_tab(self):
        assert GenericSerialize("col1\tcol2") == "col1\\tcol2"

    def test_deserialize_carriage_return(self):
        assert GenericDeserialize("line1\\rline2") == "line1\rline2"

    def test_deserialize_tab(self):
        assert GenericDeserialize("col1\\tcol2") == "col1\tcol2"

    def test_roundtrip(self):
        original = "hello\rworld\ttab"
        assert GenericDeserialize(GenericSerialize(original)) == original

    def test_empty_string(self):
        assert GenericSerialize("") == ""
        assert GenericDeserialize("") == ""

    def test_no_special_chars_unchanged(self):
        text = "normal text"
        assert GenericSerialize(text) == text


class TestGenericFileList:
    def test_has_expected_files(self):
        assert "/generic.xlsx" in GENERIC_FILE_LIST
        assert "/generic.fmt.xlsx" in GENERIC_FILE_LIST

    def test_only_two_files(self):
        assert len(GENERIC_FILE_LIST) == 2


# ============================================================
# Generic XlsxToJson — synthetic fixtures
# ============================================================


class TestGenericXlsxToJson:
    def test_basic_conversion(self, tmp_path):
        """Convert a synthetic generic xlsx and verify JSON output."""
        xlsx_path = str(tmp_path / "generic.xlsx")
        create_generic_xlsx(xlsx_path, [
            {"text": "こんにちは", "trans": "안녕하세요"},
            {"text": "さようなら", "trans": "안녕히가세요"},
        ])
        output_path = str(tmp_path / "generic.json")

        GenericXlsxToJson(xlsx_path, output_path)

        with open(output_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        assert data["こんにちは"] == "안녕하세요"
        assert data["さようなら"] == "안녕히가세요"

    def test_empty_trans_skipped(self, tmp_path):
        """Rows with empty translation should be skipped."""
        xlsx_path = str(tmp_path / "generic.xlsx")
        create_generic_xlsx(xlsx_path, [
            {"text": "has_trans", "trans": "번역있음"},
            {"text": "no_trans", "trans": ""},
        ])
        output_path = str(tmp_path / "output.json")

        GenericXlsxToJson(xlsx_path, output_path)

        with open(output_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        assert "has_trans" in data
        assert "no_trans" not in data

    def test_leading_apostrophe_stripped(self, tmp_path):
        """Leading single quote in trans should be removed."""
        xlsx_path = str(tmp_path / "generic.xlsx")
        create_generic_xlsx(xlsx_path, [
            {"text": "key1", "trans": "'value_with_quote"},
        ])
        output_path = str(tmp_path / "output.json")

        GenericXlsxToJson(xlsx_path, output_path)

        with open(output_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        assert data["key1"] == "value_with_quote"

    def test_non_string_text_skipped(self, tmp_path):
        """Rows where text is not a string should be skipped."""
        xlsx_path = str(tmp_path / "generic.xlsx")
        create_generic_xlsx(xlsx_path, [
            {"text": "valid", "trans": "유효"},
            {"text": 12345, "trans": "숫자"},
        ])
        output_path = str(tmp_path / "output.json")

        GenericXlsxToJson(xlsx_path, output_path)

        with open(output_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        assert "valid" in data
        assert len(data) == 1

    def test_output_creates_parent_dirs(self, tmp_path):
        """XlsxToJson should create parent directories."""
        xlsx_path = str(tmp_path / "generic.xlsx")
        create_generic_xlsx(xlsx_path, [
            {"text": "test", "trans": "테스트"},
        ])
        deep_path = str(tmp_path / "deep" / "nested" / "output.json")

        GenericXlsxToJson(xlsx_path, deep_path)

        assert os.path.exists(deep_path)


class TestLyricsXlsxToJson:
    def test_lyrics_column_format(self, tmp_path):
        """Lyrics use A/B columns instead of text/trans."""
        lyrics_dir = tmp_path / "lyrics"
        lyrics_dir.mkdir()
        xlsx_path = str(lyrics_dir / "song.xlsx")
        create_lyrics_xlsx(xlsx_path, [
            {"A": "歌詞一行目", "B": "가사 첫줄"},
            {"A": "歌詞二行目", "B": "가사 둘째줄"},
        ])
        output_path = str(lyrics_dir / "song.json")

        GenericXlsxToJson(xlsx_path, output_path)

        with open(output_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        assert data["歌詞一行目"] == "가사 첫줄"
        assert data["歌詞二行目"] == "가사 둘째줄"

    def test_lyrics_empty_b_skipped(self, tmp_path):
        """Lyrics with empty B column should be skipped."""
        lyrics_dir = tmp_path / "lyrics"
        lyrics_dir.mkdir()
        xlsx_path = str(lyrics_dir / "song.xlsx")
        create_lyrics_xlsx(xlsx_path, [
            {"A": "has_trans", "B": "번역"},
            {"A": "no_trans", "B": ""},
        ])
        output_path = str(lyrics_dir / "song.json")

        GenericXlsxToJson(xlsx_path, output_path)

        with open(output_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        assert "has_trans" in data
        assert "no_trans" not in data

    def test_lyrics_apostrophe_stripped(self, tmp_path):
        """Lyrics with leading apostrophe in B should strip it."""
        lyrics_dir = tmp_path / "lyrics"
        lyrics_dir.mkdir()
        xlsx_path = str(lyrics_dir / "song.xlsx")
        create_lyrics_xlsx(xlsx_path, [
            {"A": "key", "B": "'quoted_value"},
        ])
        output_path = str(lyrics_dir / "song.json")

        GenericXlsxToJson(xlsx_path, output_path)

        with open(output_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        assert data["key"] == "quoted_value"


# ============================================================
# ☢ Marker
# ============================================================


class TestGenericUpdateNotSupported:
    def test_returns_empty(self):
        from scripts.generic import UpdateOriginalToDrive
        assert UpdateOriginalToDrive() == []


class TestRadioactiveMarkerDeserialize:
    """Test ☢ marker → \\r conversion in Deserialize (Google Sheets workaround)."""

    def test_radioactive_to_cr(self):
        """☢ should be deserialized to carriage return."""
        assert GenericDeserialize("line1☢line2") == "line1\rline2"

    def test_backslash_r_to_cr(self):
        """\\r should also be deserialized to carriage return."""
        assert GenericDeserialize("line1\\rline2") == "line1\rline2"

    def test_radioactive_in_xlsx_to_json(self, tmp_path):
        """☢ in translation should become \\r in JSON output."""
        xlsx_path = str(tmp_path / "generic.xlsx")
        create_generic_xlsx(xlsx_path, [
            {"text": "원문", "trans": "줄1☢줄2"},
        ])
        output_path = str(tmp_path / "output.json")

        GenericXlsxToJson(xlsx_path, output_path)

        with open(output_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        assert data["원문"] == "줄1\r줄2"
