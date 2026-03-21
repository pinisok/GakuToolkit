"""Conversion correctness tests (P1/P2/P3).

Verifies actual output values, not just "doesn't crash".
Each test documents a specific scenario that was previously uncovered.
"""

import os
import json
from io import StringIO

import pytest
import pandas as pd

from tests.fixtures.create_fixtures import (
    create_adv_xlsx, create_adv_txt,
    create_generic_xlsx, create_lyrics_xlsx,
    create_localization_xlsx,
    create_masterdb_xlsx, create_masterdb_json,
)
from scripts.adv import (
    XlsxToTxt, TxtToXlsx, _filter_adv_files,
    _internalUpdateDataFrame, _internalReadXlsx,
    _internalDataFrameToXlsx,
)
from scripts.masterdb2 import (
    OverrideRecordToJson, CreateJSON, DataToRecord,
    ReadXlsx, ReadJson, WriteJson, WriteXlsx,
)
from scripts.helper import (
    Serialize, Deserialize, SERIALIZE_LIST_FULL,
    load_cache_date, save_cache_date,
)
from scripts.generic import XlsxToJson as GenericXlsxToJson
from scripts.localization import XlsxToJson as LocXlsxToJson


# ============================================================
# ADV: duplicate source text (P1-10)
# ============================================================


class TestDuplicateSourceText:
    """Two identical messages in the same file must each get their own translation."""

    def test_both_translations_applied(self, tmp_path):
        txt_path = str(tmp_path / "original.txt")
        create_adv_txt(txt_path, [
            {"tag": "message", "text": "同じテキスト", "name": "A"},
            {"tag": "message", "text": "同じテキスト", "name": "B"},
        ])

        xlsx_path = str(tmp_path / "test.xlsx")
        create_adv_xlsx(xlsx_path, [
            {"id": "0000000000000", "name": "A", "translated name": "에이",
             "text": "同じテキスト", "translated text": "첫번째 번역"},
            {"id": "0000000000000", "name": "B", "translated name": "비",
             "text": "同じテキスト", "translated text": "두번째 번역"},
        ])

        output_path = str(tmp_path / "output.txt")
        XlsxToTxt(xlsx_path, output_path, txt_path)

        with open(output_path, "r", encoding="utf-8") as f:
            content = f.read()

        assert "첫번째 번역" in content
        assert "두번째 번역" in content
        # Original should be fully replaced
        assert "同じテキスト" not in content


# ============================================================
# ADV: = escape in translated text (P1-9)
# ============================================================


class TestEqualsEscape:
    """Translated text containing = must be escaped to \\= in output TXT."""

    def test_equals_escaped(self, tmp_path):
        txt_path = str(tmp_path / "original.txt")
        create_adv_txt(txt_path, [
            {"tag": "message", "text": "テスト", "name": "A"},
        ])

        xlsx_path = str(tmp_path / "test.xlsx")
        create_adv_xlsx(xlsx_path, [
            {"id": "0000000000000", "name": "A", "translated name": "",
             "text": "テスト", "translated text": "점수=100점"},
        ])

        output_path = str(tmp_path / "output.txt")
        XlsxToTxt(xlsx_path, output_path, txt_path)

        with open(output_path, "r", encoding="utf-8") as f:
            content = f.read()

        assert "점수\\=100점" in content
        assert "text=점수=100점" not in content  # unescaped = must not appear

    def test_already_escaped_equals_not_double_escaped(self, tmp_path):
        """\\= should not become \\\\="""
        txt_path = str(tmp_path / "original.txt")
        create_adv_txt(txt_path, [
            {"tag": "message", "text": "テスト", "name": "A"},
        ])

        xlsx_path = str(tmp_path / "test.xlsx")
        create_adv_xlsx(xlsx_path, [
            {"id": "0000000000000", "name": "A", "translated name": "",
             "text": "テスト", "translated text": "이미\\=이스케이프됨"},
        ])

        output_path = str(tmp_path / "output.txt")
        XlsxToTxt(xlsx_path, output_path, txt_path)

        with open(output_path, "r", encoding="utf-8") as f:
            content = f.read()

        # Already escaped \= should stay as \=, not become \\=
        assert "이미\\=이스케이프됨" in content


# ============================================================
# ADV: row count shrink (P1-12)
# ============================================================


class TestUpdateDataFrameShrink:
    """When new TXT has fewer rows than existing XLSX, old rows must be dropped."""

    def test_shrink_returns_fewer_rows(self, tmp_path):
        xlsx_path = str(tmp_path / "existing.xlsx")
        create_adv_xlsx(xlsx_path, [
            {"id": "0", "name": "A", "translated name": "",
             "text": "行1", "translated text": "줄1"},
            {"id": "0", "name": "B", "translated name": "",
             "text": "行2", "translated text": "줄2"},
            {"id": "0", "name": "C", "translated name": "",
             "text": "行3", "translated text": "줄3"},
        ])

        # New source has only 2 rows (행3 removed)
        new_df = pd.DataFrame([
            {"id": "0", "name": "A", "translated name": "",
             "text": "行1", "translated text": ""},
            {"id": "0", "name": "B", "translated name": "",
             "text": "行2", "translated text": ""},
        ])

        with open(xlsx_path, "rb") as fp:
            result_df, warnings = _internalUpdateDataFrame(new_df, fp)

        records = result_df.to_dict(orient="records")
        assert len(records) == 2  # Must shrink to 2, not 3
        assert records[0]["translated text"] == "줄1"  # Preserved
        assert records[1]["translated text"] == "줄2"  # Preserved
        assert any("줄 수 변경" in w for w in warnings)

    def test_unchanged_rows_have_empty_comments(self, tmp_path):
        """In a multi-row update, unchanged rows must have comments == ''."""
        xlsx_path = str(tmp_path / "existing.xlsx")
        create_adv_xlsx(xlsx_path, [
            {"id": "0", "name": "A", "translated name": "",
             "text": "変わらない", "translated text": "안바뀜"},
            {"id": "0", "name": "B", "translated name": "",
             "text": "変わる前", "translated text": "바뀌기전"},
        ])

        new_df = pd.DataFrame([
            {"id": "0", "name": "A", "translated name": "",
             "text": "変わらない", "translated text": ""},
            {"id": "0", "name": "B", "translated name": "",
             "text": "変わった後", "translated text": ""},
        ])

        with open(xlsx_path, "rb") as fp:
            result_df, warnings = _internalUpdateDataFrame(new_df, fp)

        records = result_df.to_dict(orient="records")
        assert records[0]["comments"] == ""  # Unchanged row: empty comment
        assert "원본 문자열이 수정되었습니다" in records[1]["comments"]  # Changed row


# ============================================================
# MasterDB: OverrideRecordToJson assertion completeness (P1-13)
# ============================================================


class TestOverrideRecordToJsonCompleteness:
    """Verify actual output values, not just structure."""

    def test_traverse_exception_leaves_other_data_intact(self, shelve_test_cleanup):
        """A bad record path should not corrupt other records."""
        json_data = {
            "rules": {"primaryKeys": ["id"]},
            "data": [
                {"id": "good", "name": "良い名前"},
                {"id": "bad", "name": "悪い名前"},
            ],
        }
        records = [
            # Good record
            {"IMAGE": "", "KEY ID 0": "id", "KEY VALUE 0": "good",
             "ID": "name", "원문": "良い名前", "번역": "좋은이름", "설명": ""},
            # Bad record: path points to nonexistent field
            {"IMAGE": "", "KEY ID 0": "id", "KEY VALUE 0": "bad",
             "ID": "nonexistent.deep.path", "원문": "x", "번역": "y", "설명": ""},
        ]

        result = OverrideRecordToJson(json_data, records)

        # Good record should be translated
        assert result["data"][0]["name"] == "좋은이름"
        # Bad record should be unchanged (exception caught internally)
        assert result["data"][1]["name"] == "悪い名前"


# ============================================================
# MasterDB: CreateJSON round-trip completeness (P1-21)
# ============================================================


class TestCreateJSONRoundTrip:
    """Verify full round-trip preserves structure, not just one field."""

    def test_full_structure_preserved(self, tmp_path, shelve_test_cleanup):
        import scripts.masterdb2 as mdb2

        json_path = str(tmp_path / "json" / "RoundTrip.json")
        os.makedirs(os.path.dirname(json_path), exist_ok=True)
        create_masterdb_json(json_path, ["id"], [
            {"id": "rt-001", "name": "テスト", "description": "説明テスト", "score": 42},
        ])

        xlsx_path = str(tmp_path / "drive" / "RoundTrip.xlsx")
        os.makedirs(os.path.dirname(xlsx_path), exist_ok=True)
        create_masterdb_xlsx(xlsx_path, None, [
            {"IMAGE": "", "KEY ID 0": "id", "KEY VALUE 0": "rt-001",
             "ID": "name", "원문": "テスト", "번역": "테스트", "설명": ""},
            {"IMAGE": "", "KEY ID 0": "id", "KEY VALUE 0": "rt-001",
             "ID": "description", "원문": "説明テスト", "번역": "설명 테스트", "설명": ""},
        ])

        output_dir = str(tmp_path / "output")
        os.makedirs(output_dir, exist_ok=True)

        saved = {
            "json": mdb2.MASTERDB_JSON_PATH,
            "drive": mdb2.MASTERDB2_DRIVE_PATH,
            "output": mdb2.MASTERDB_OUTPUT_PATH,
        }
        mdb2.MASTERDB_JSON_PATH = str(tmp_path / "json")
        mdb2.MASTERDB2_DRIVE_PATH = str(tmp_path / "drive")
        mdb2.MASTERDB_OUTPUT_PATH = output_dir
        try:
            CreateJSON("RoundTrip")
        finally:
            mdb2.MASTERDB_JSON_PATH = saved["json"]
            mdb2.MASTERDB2_DRIVE_PATH = saved["drive"]
            mdb2.MASTERDB_OUTPUT_PATH = saved["output"]

        with open(os.path.join(output_dir, "RoundTrip.json"), "r", encoding="utf-8") as f:
            result = json.load(f)

        # Rules block preserved
        assert result["rules"]["primaryKeys"] == ["id"]
        # Data count preserved
        assert len(result["data"]) == 1
        # Primary key preserved
        assert result["data"][0]["id"] == "rt-001"
        # Both fields translated
        assert result["data"][0]["name"] == "테스트"
        assert result["data"][0]["description"] == "설명 테스트"
        # Non-translatable field preserved
        assert result["data"][0]["score"] == 42


# ============================================================
# ADV: output TXT structural integrity (P2-1)
# ============================================================


class TestOutputTxtStructure:
    """Verify game-engine syntax is preserved, not just translated string presence."""

    def test_text_field_syntax_preserved(self, tmp_path):
        """Output must have text=TRANSLATED, not bare translated string."""
        txt_path = str(tmp_path / "original.txt")
        create_adv_txt(txt_path, [
            {"tag": "message", "text": "テスト", "name": "麻央"},
        ])

        xlsx_path = str(tmp_path / "test.xlsx")
        create_adv_xlsx(xlsx_path, [
            {"id": "0000000000000", "name": "麻央", "translated name": "마오",
             "text": "テスト", "translated text": "테스트 번역"},
        ])

        output_path = str(tmp_path / "output.txt")
        XlsxToTxt(xlsx_path, output_path, txt_path)

        with open(output_path, "r", encoding="utf-8") as f:
            content = f.read()

        # Must have text=TRANSLATED (game engine format)
        assert "text=테스트 번역" in content
        # Must have name=TRANSLATED
        assert "name=마오" in content
        # Original Japanese must not remain
        assert "text=テスト" not in content
        assert "name=麻央" not in content


# ============================================================
# P2: CSV has more rows than TXT (P2-11)
# ============================================================


class TestCsvMoreRowsThanTxt:
    """Extra CSV rows beyond TXT messages should be silently ignored."""

    def test_extra_csv_rows_no_crash(self, tmp_path):
        txt_path = str(tmp_path / "original.txt")
        create_adv_txt(txt_path, [
            {"tag": "message", "text": "一つだけ", "name": "A"},
        ])

        # XLSX has 2 rows, but TXT has only 1 message
        xlsx_path = str(tmp_path / "test.xlsx")
        create_adv_xlsx(xlsx_path, [
            {"id": "0000000000000", "name": "A", "translated name": "",
             "text": "一つだけ", "translated text": "하나만"},
            {"id": "0000000000000", "name": "B", "translated name": "",
             "text": "余分な行", "translated text": "여분의 행"},
        ])

        output_path = str(tmp_path / "output.txt")
        XlsxToTxt(xlsx_path, output_path, txt_path)

        with open(output_path, "r", encoding="utf-8") as f:
            content = f.read()
        assert "하나만" in content
        # Extra row should not appear in output
        assert "여분의 행" not in content


# ============================================================
# P2: name in message text (P2-22)
# ============================================================


class TestNameInMessageText:
    """Character name appearing inside message text must not be replaced."""

    def test_name_replace_only_in_name_field(self, tmp_path):
        txt_path = str(tmp_path / "original.txt")
        create_adv_txt(txt_path, [
            # Message where the character name "麻央" also appears in text
            {"tag": "message", "text": "麻央が来た", "name": "麻央"},
        ])

        xlsx_path = str(tmp_path / "test.xlsx")
        create_adv_xlsx(xlsx_path, [
            {"id": "0000000000000", "name": "麻央", "translated name": "마오",
             "text": "麻央が来た", "translated text": "마오가 왔다"},
        ])

        output_path = str(tmp_path / "output.txt")
        XlsxToTxt(xlsx_path, output_path, txt_path)

        with open(output_path, "r", encoding="utf-8") as f:
            content = f.read()

        assert "text=마오가 왔다" in content
        assert "name=마오" in content
        # The text field should contain "마오가 왔다", not have "마오" double-replaced


# ============================================================
# P2: Serialize/Deserialize double substitution (P2-19)
# ============================================================


class TestSerializeDeserializeEdgeCases:
    """Test that \\r and ☢ don't cause double substitution."""

    def test_backslash_r_and_radioactive_coexist(self):
        """A string with both \\r and ☢ should deserialize each once."""
        input_str = "line1\\rline2☢line3"
        result = Deserialize(input_str)
        # \\r → \r, ☢ → \r
        assert result == "line1\rline2\rline3"
        # Must NOT have double \r\r
        assert "\r\r" not in result

    def test_serialize_then_deserialize_roundtrip(self):
        """Serialize → Deserialize should return original (minus ☢ ambiguity)."""
        original = "line1\rline2\tline3"
        serialized = Serialize(original)
        deserialized = Deserialize(serialized)
        assert deserialized == original


# ============================================================
# P2: OverrideRecordToJson exception branch (P2-14)
# ============================================================


class TestOverrideExceptionBranch:
    """traverse() exception should not corrupt other data items."""

    def test_key_error_in_traverse(self, shelve_test_cleanup):
        """Missing key in data should be caught, not crash."""
        json_data = {
            "rules": {"primaryKeys": ["id"]},
            "data": [
                {"id": "x", "name": "テスト"},
            ],
        }
        records = [
            # Path points to a field that doesn't exist
            {"IMAGE": "", "KEY ID 0": "id", "KEY VALUE 0": "x",
             "ID": "missing_field.sub", "원문": "x", "번역": "y", "설명": ""},
            # Valid record
            {"IMAGE": "", "KEY ID 0": "id", "KEY VALUE 0": "x",
             "ID": "name", "원문": "テスト", "번역": "테스트", "설명": ""},
        ]

        result = OverrideRecordToJson(json_data, records)

        # Valid translation should still be applied
        assert result["data"][0]["name"] == "테스트"


# ============================================================
# P3: DataFrame/WriteXlsx cell value verification (P3-5/6)
# ============================================================


class TestXlsxCellValues:
    """Verify actual cell values after write, not just row count."""

    def test_adv_dataframe_to_xlsx_values(self, tmp_path):
        df = pd.DataFrame([
            {"id": "0", "name": "テスト", "translated name": "테스트",
             "text": "本文", "translated text": "본문"},
        ])
        xlsx_path = str(tmp_path / "output.xlsx")
        with open(xlsx_path, "wb") as fp:
            _internalDataFrameToXlsx(df, fp)

        result = pd.read_excel(xlsx_path, engine="openpyxl")
        assert str(result.iloc[0, 0]) == "0"  # id (may be read as int)
        assert result.iloc[0, 1] == "テスト"  # name
        assert result.iloc[0, 3] == "本文"  # text

    def test_masterdb_write_xlsx_values(self, tmp_path):
        import scripts.masterdb2 as mdb2

        records = [
            {"IMAGE": "", "KEY ID 0": "id", "KEY VALUE 0": "test-001",
             "ID": "name", "원문": "テスト名前", "번역": "테스트이름", "설명": "메모"},
        ]
        saved = mdb2.MASTERDB2_DRIVE_PATH
        mdb2.MASTERDB2_DRIVE_PATH = str(tmp_path)
        try:
            WriteXlsx("CellTest", records)
        finally:
            mdb2.MASTERDB2_DRIVE_PATH = saved

        result = pd.read_excel(str(tmp_path / "CellTest.xlsx"), engine="openpyxl")
        assert result.iloc[0]["원문"] == "テスト名前"
        assert result.iloc[0]["번역"] == "테스트이름"
        assert result.iloc[0]["KEY VALUE 0"] == "test-001"


# ============================================================
# P3: TxtToXlsx cell value verification (P3-7/8)
# ============================================================


class TestTxtToXlsxValues:
    """Verify TxtToXlsx writes correct cell values."""

    def test_new_xlsx_has_source_text(self, tmp_path):
        txt_path = str(tmp_path / "adv_test.txt")
        create_adv_txt(txt_path, [
            {"tag": "message", "text": "検証テスト", "name": "テスター"},
        ])

        xlsx_path = str(tmp_path / "adv_test.xlsx")
        TxtToXlsx(txt_path, xlsx_path, "adv_test.txt")

        df = pd.read_excel(xlsx_path, engine="openpyxl")
        records = df.to_dict(orient="records")
        # First data row (before info/translator rows)
        assert any(r.get(df.columns[3]) == "検証テスト" for r in records)


# ============================================================
# P3: DataToRecord edge cases (P3-15/16)
# ============================================================


class TestDataToRecordEdgeCases:
    """DataToRecord with unusual data structures."""

    def test_empty_string_list_skipped(self, shelve_test_cleanup):
        """List of empty strings should not produce records (not exportable)."""
        data = {"id": "x", "tags": ["", "", ""]}
        records = DataToRecord("Achievement", data)
        # Empty strings are ASCII-only → check_need_export returns False
        tag_records = [r for r in records if r.get("ID", "").startswith("tags")]
        assert len(tag_records) == 0

    def test_integer_list_no_crash(self, shelve_test_cleanup):
        """List of integers should not crash (silently skipped)."""
        data = {"id": "x", "name": "テスト", "scores": [100, 200, 300]}
        records = DataToRecord("Achievement", data)
        # scores should not appear (integers are not translatable)
        score_records = [r for r in records if "scores" in r.get("ID", "")]
        assert len(score_records) == 0


# ============================================================
# P3: Generic lyrics \\r\\n expansion (P3-17)
# ============================================================


class TestLyricsEscapeExpansion:
    """Lyrics keys with \\r\\n should expand to actual \\r\\n in JSON."""

    def test_backslash_rn_expanded(self, tmp_path):
        lyrics_dir = tmp_path / "lyrics"
        lyrics_dir.mkdir()
        xlsx_path = str(lyrics_dir / "song.xlsx")
        create_lyrics_xlsx(xlsx_path, [
            {"A": "歌詞1行\\r\\n歌詞2行", "B": "가사1줄\\r\\n가사2줄"},
        ])
        output_path = str(lyrics_dir / "song.json")

        GenericXlsxToJson(xlsx_path, output_path)

        with open(output_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        # Keys and values with \\r\\n should be expanded to actual \r\n
        keys = list(data.keys())
        assert any("\r\n" in k for k in keys)


# ============================================================
# P3: Localization numeric translation column (P3-18)
# ============================================================


class TestLocalizationNumericTranslation:
    """Numeric value in 번역 column should be skipped."""

    def test_numeric_translation_skipped(self, tmp_path):
        xlsx_path = str(tmp_path / "localization.xlsx")
        create_localization_xlsx(xlsx_path, [
            {0: "ref1", "ID": "key.valid", "번역": "정상번역"},
            {0: "ref2", "ID": "key.numeric", "번역": 12345},
        ])
        output_path = str(tmp_path / "localization.json")

        LocXlsxToJson(xlsx_path, output_path)

        with open(output_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        assert "key.valid" in data
        assert "key.numeric" not in data  # Numeric translation skipped


# ============================================================
# P3: load_cache_date multi-line file (P3-20)
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


class TestFilterAdvFilesStructure:
    """Verify returned tuple slots are correct, not just filename."""

    def test_input_path_is_rel_path(self):
        file_paths = [
            ("/abs/path/adv_cidol-amao_01.txt", "rel/adv_cidol-amao_01.txt", "adv_cidol-amao_01.txt"),
        ]
        result = _filter_adv_files(file_paths)
        assert len(result) == 1
        input_path, output_path, filename = result[0]
        # input_path should be the rel_path from the input tuple
        assert input_path == "rel/adv_cidol-amao_01.txt"
        # filename should be unchanged
        assert filename == "adv_cidol-amao_01.txt"
        # output_path should be xlsx, not txt
        assert output_path.endswith(".xlsx")
