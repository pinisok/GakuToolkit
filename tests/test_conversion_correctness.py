"""P1 conversion correctness tests.

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
    create_masterdb_xlsx, create_masterdb_json,
)
from scripts.adv import (
    XlsxToTxt, TxtToXlsx,
    _internalUpdateDataFrame, _internalReadXlsx,
)
from scripts.masterdb2 import (
    OverrideRecordToJson, CreateJSON, ReadXlsx, ReadJson, WriteJson,
)


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
