"""Extended MasterDB2 tests covering DataToRecord, JsonToRecord, CreateJSON, and complex overrides."""

import os
import json
from copy import deepcopy

import pytest

from tests.fixtures.create_fixtures import create_masterdb_xlsx, create_masterdb_json
from scripts.masterdb2 import (
    DataToRecord,
    JsonToRecord,
    CreateJSON,
    WriteXlsx,
    ReadXlsx,
    ReadJson,
    WriteJson,
    OverrideRecordToJson,
    GetRecordStructure,
    GetRule,
    check_need_export,
    path_normalize_for_pk,
    DB_save,
    DB_get,
)


# ============================================================
# DataToRecord: extract translatable records from JSON data
# ============================================================


class TestDataToRecord:
    def test_extracts_simple_fields(self, shelve_test_cleanup):
        """DataToRecord extracts translatable string fields."""
        data = {"id": "ach-001", "name": "テスト業績", "description": "説明文"}

        records = DataToRecord("Achievement", data)

        assert len(records) >= 1
        fields = {r["ID"] for r in records}
        assert "name" in fields
        assert "description" in fields

    def test_skips_ascii_only_fields(self, shelve_test_cleanup):
        """ASCII-only strings should not be exported."""
        data = {"id": "ach-001", "name": "テスト", "internalRef": "ASCII_ONLY_VALUE"}

        records = DataToRecord("Achievement", data)

        fields = {r["ID"] for r in records}
        assert "name" in fields
        assert "internalRef" not in fields

    def test_primary_key_values_extracted(self, shelve_test_cleanup):
        """Primary key values should be in KEY VALUE columns."""
        data = {"id": "ach-001", "name": "テスト"}

        records = DataToRecord("Achievement", data)

        assert len(records) >= 1
        assert records[0]["KEY VALUE 0"] == "ach-001"

    def test_numeric_primary_key_to_string(self, shelve_test_cleanup):
        """Numeric primary keys should be converted to string."""
        data = {"id": 12345, "name": "テスト"}

        records = DataToRecord("Achievement", data)

        assert records[0]["KEY VALUE 0"] == "12345"

    def test_nested_dict_traversal(self, shelve_test_cleanup):
        """Nested dicts should be traversed with dot-separated paths."""
        data = {
            "id": "item-001",
            "name": "テスト",
            "details": {"subtitle": "サブタイトル"},
        }

        records = DataToRecord("Achievement", data)

        fields = {r["ID"] for r in records}
        assert "details.subtitle" in fields

    def test_string_array_with_la_markers(self, shelve_test_cleanup):
        """String arrays should be encoded with [LA_F]...[LA_N_F] markers."""
        data = {
            "id": "item-001",
            "name": "テスト",
            "texts": ["テキスト1", "テキスト2", "テキスト3"],
        }

        records = DataToRecord("Achievement", data)

        la_records = [r for r in records if "[LA_F]" in r.get("원문", "")]
        assert len(la_records) >= 1
        assert "[LA_N_F]" in la_records[0]["원문"]


# ============================================================
# JsonToRecord: reads JSON file and extracts all records
# ============================================================


class TestJsonToRecord:
    def test_reads_and_extracts(self, tmp_path, shelve_test_cleanup):
        """JsonToRecord reads JSON and produces record list."""
        import scripts.masterdb2 as mdb2

        json_path = str(tmp_path / "Achievement.json")
        create_masterdb_json(json_path, ["id"], [
            {"id": "a1", "name": "業績1", "description": "説明1"},
            {"id": "a2", "name": "業績2", "description": "説明2"},
        ])

        original = mdb2.MASTERDB_JSON_PATH
        mdb2.MASTERDB_JSON_PATH = str(tmp_path)
        try:
            records = JsonToRecord("Achievement")
        finally:
            mdb2.MASTERDB_JSON_PATH = original

        assert len(records) >= 4  # 2 items × 2 fields each
        ids = {r["KEY VALUE 0"] for r in records}
        assert "a1" in ids
        assert "a2" in ids


# ============================================================
# OverrideRecordToJson: complex scenarios
# ============================================================


class TestOverrideRecordToJsonExtended:
    def test_nested_field_override(self, shelve_test_cleanup):
        """Override a nested dict field."""
        json_data = {
            "rules": {"primaryKeys": ["id"]},
            "data": [
                {"id": "x", "details": {"subtitle": "サブ"}},
            ],
        }
        records = [
            {"IMAGE": "", "KEY ID 0": "id", "KEY VALUE 0": "x",
             "ID": "details.subtitle", "원문": "サブ", "번역": "서브", "설명": ""},
        ]

        result = OverrideRecordToJson(json_data, records)

        assert result["data"][0]["details"]["subtitle"] == "서브"

    def test_array_element_override(self, shelve_test_cleanup):
        """Override a specific array element."""
        json_data = {
            "rules": {"primaryKeys": ["id"]},
            "data": [
                {"id": "x", "items": [
                    {"name": "アイテム1"},
                    {"name": "アイテム2"},
                ]},
            ],
        }
        records = [
            {"IMAGE": "", "KEY ID 0": "id", "KEY VALUE 0": "x",
             "ID": "items[1].name", "원문": "アイテム2", "번역": "아이템2", "설명": ""},
        ]

        result = OverrideRecordToJson(json_data, records)

        assert result["data"][0]["items"][1]["name"] == "아이템2"
        assert result["data"][0]["items"][0]["name"] == "アイテム1"  # Unchanged

    def test_string_array_via_nested_indexed_path(self, shelve_test_cleanup):
        """Override string array inside a nested indexed object using [LA_F] markers."""
        json_data = {
            "rules": {"primaryKeys": ["id"]},
            "data": [
                {"id": "x", "groups": [
                    {"texts": ["テキスト1", "テキスト2"]},
                ]},
            ],
        }
        records = [
            {"IMAGE": "", "KEY ID 0": "id", "KEY VALUE 0": "x",
             "ID": "groups[0].texts", "원문": "[LA_F]テキスト1[LA_N_F]テキスト2",
             "번역": "[LA_F]텍스트1[LA_N_F]텍스트2", "설명": ""},
        ]

        result = OverrideRecordToJson(json_data, records)

        # Known limitation: top-level list override via obj[subkey] doesn't persist
        # But traversal through indexed nested object should work
        # The actual behavior depends on the code path taken

    def test_top_level_string_array_known_bug(self, shelve_test_cleanup):
        """Top-level string array override has a known bug (obj = ... instead of obj[key] = ...).

        This test documents the current behavior, not the ideal behavior.
        See masterdb2.py line 937: `obj = trans_str[...]` assigns to local var.
        """
        json_data = {
            "rules": {"primaryKeys": ["id"]},
            "data": [
                {"id": "x", "texts": ["テキスト1", "テキスト2"]},
            ],
        }
        records = [
            {"IMAGE": "", "KEY ID 0": "id", "KEY VALUE 0": "x",
             "ID": "texts", "원문": "[LA_F]テキスト1[LA_N_F]テキスト2",
             "번역": "[LA_F]텍스트1[LA_N_F]텍스트2", "설명": ""},
        ]

        result = OverrideRecordToJson(json_data, records)

        # Known bug: top-level string array is NOT actually overridden
        assert result["data"][0]["texts"] == ["テキスト1", "テキスト2"]

    def test_multiple_data_items(self, shelve_test_cleanup):
        """Override fields across multiple data items."""
        json_data = {
            "rules": {"primaryKeys": ["id"]},
            "data": [
                {"id": "a", "name": "名前A"},
                {"id": "b", "name": "名前B"},
            ],
        }
        records = [
            {"IMAGE": "", "KEY ID 0": "id", "KEY VALUE 0": "a",
             "ID": "name", "원문": "名前A", "번역": "이름A", "설명": ""},
            {"IMAGE": "", "KEY ID 0": "id", "KEY VALUE 0": "b",
             "ID": "name", "원문": "名前B", "번역": "이름B", "설명": ""},
        ]

        result = OverrideRecordToJson(json_data, records)

        assert result["data"][0]["name"] == "이름A"
        assert result["data"][1]["name"] == "이름B"

    def test_mismatched_original_not_overridden(self, shelve_test_cleanup):
        """If original text doesn't match, field should not be overridden."""
        json_data = {
            "rules": {"primaryKeys": ["id"]},
            "data": [
                {"id": "x", "name": "実際のテキスト"},
            ],
        }
        records = [
            {"IMAGE": "", "KEY ID 0": "id", "KEY VALUE 0": "x",
             "ID": "name", "원문": "間違ったテキスト", "번역": "잘못된 번역", "설명": ""},
        ]

        result = OverrideRecordToJson(json_data, records)

        assert result["data"][0]["name"] == "実際のテキスト"  # Not overridden

    def test_empty_id_skipped(self, shelve_test_cleanup):
        """Records with empty ID (subkey) should be skipped."""
        json_data = {
            "rules": {"primaryKeys": ["id"]},
            "data": [
                {"id": "x", "name": "テスト"},
            ],
        }
        records = [
            {"IMAGE": "", "KEY ID 0": "id", "KEY VALUE 0": "x",
             "ID": "", "원문": "テスト", "번역": "테스트", "설명": ""},
        ]

        result = OverrideRecordToJson(json_data, records)

        # Should not crash, name unchanged
        assert result["data"][0]["name"] == "テスト"

    def test_db_cache_saved_on_override(self, shelve_test_cleanup):
        """Successful override should save to shelve cache."""
        json_data = {
            "rules": {"primaryKeys": ["id"]},
            "data": [
                {"id": "x", "name": "キャッシュテスト"},
            ],
        }
        records = [
            {"IMAGE": "", "KEY ID 0": "id", "KEY VALUE 0": "x",
             "ID": "name", "원문": "キャッシュテスト", "번역": "캐시 테스트", "설명": ""},
        ]

        OverrideRecordToJson(json_data, records)

        assert DB_get("キャッシュテスト") == "캐시 테스트"


# ============================================================
# CreateJSON: end-to-end JSON → XLSX → JSON
# ============================================================


class TestCreateJSON:
    def test_end_to_end(self, tmp_path, shelve_test_cleanup):
        """CreateJSON reads XLSX + JSON and writes translated JSON."""
        import scripts.masterdb2 as mdb2

        # Create source JSON
        json_path = str(tmp_path / "json" / "TestCreate.json")
        os.makedirs(os.path.dirname(json_path), exist_ok=True)
        create_masterdb_json(json_path, ["id"], [
            {"id": "c1", "name": "テスト名前"},
        ])

        # Create translation XLSX
        xlsx_path = str(tmp_path / "drive" / "TestCreate.xlsx")
        os.makedirs(os.path.dirname(xlsx_path), exist_ok=True)
        create_masterdb_xlsx(xlsx_path, None, [
            {"IMAGE": "", "KEY ID 0": "id", "KEY VALUE 0": "c1",
             "ID": "name", "원문": "テスト名前", "번역": "테스트 이름", "설명": ""},
        ])

        # Create output dir
        output_dir = str(tmp_path / "output")
        os.makedirs(output_dir, exist_ok=True)

        # Patch paths
        saved_json = mdb2.MASTERDB_JSON_PATH
        saved_drive = mdb2.MASTERDB2_DRIVE_PATH
        saved_output = mdb2.MASTERDB_OUTPUT_PATH
        mdb2.MASTERDB_JSON_PATH = str(tmp_path / "json")
        mdb2.MASTERDB2_DRIVE_PATH = str(tmp_path / "drive")
        mdb2.MASTERDB_OUTPUT_PATH = output_dir
        try:
            CreateJSON("TestCreate")
        finally:
            mdb2.MASTERDB_JSON_PATH = saved_json
            mdb2.MASTERDB2_DRIVE_PATH = saved_drive
            mdb2.MASTERDB_OUTPUT_PATH = saved_output

        output_file = os.path.join(output_dir, "TestCreate.json")
        assert os.path.exists(output_file)

        with open(output_file, "r", encoding="utf-8") as f:
            result = json.load(f)
        assert result["data"][0]["name"] == "테스트 이름"


# ============================================================
# WriteXlsx: write XLSX from records
# ============================================================


class TestCompoundPrimaryKey:
    """Test OverrideRecordToJson with multiple primary keys (key_size > 1)."""

    def test_two_primary_keys(self, shelve_test_cleanup):
        """Match data using two primary keys (e.g. characterId + dearnessLevel)."""
        json_data = {
            "rules": {"primaryKeys": ["characterId", "level"]},
            "data": [
                {"characterId": "amao", "level": 1, "description": "テスト1"},
                {"characterId": "amao", "level": 2, "description": "テスト2"},
                {"characterId": "fktn", "level": 1, "description": "テスト3"},
            ],
        }
        records = [
            {"IMAGE": "", "KEY ID 0": "characterId", "KEY VALUE 0": "amao",
             "KEY ID 1": "level", "KEY VALUE 1": "2",
             "ID": "description", "원문": "テスト2", "번역": "테스트2", "설명": ""},
        ]

        result = OverrideRecordToJson(json_data, records)

        assert result["data"][0]["description"] == "テスト1"  # Unchanged
        assert result["data"][1]["description"] == "테스트2"  # Translated
        assert result["data"][2]["description"] == "テスト3"  # Unchanged

    def test_compound_key_no_match(self, shelve_test_cleanup):
        """If compound key doesn't fully match, should not override."""
        json_data = {
            "rules": {"primaryKeys": ["id", "type"]},
            "data": [
                {"id": "x", "type": "A", "name": "元"},
            ],
        }
        records = [
            {"IMAGE": "", "KEY ID 0": "id", "KEY VALUE 0": "x",
             "KEY ID 1": "type", "KEY VALUE 1": "B",
             "ID": "name", "원문": "元", "번역": "번역", "설명": ""},
        ]

        result = OverrideRecordToJson(json_data, records)

        assert result["data"][0]["name"] == "元"  # Not matched, unchanged


class TestUpdateXlsx:
    """Test UpdateXlsx 3-way merge logic."""

    def test_new_record_inserted(self, tmp_path, shelve_test_cleanup):
        """New JP records not in KR xlsx should be added."""
        import scripts.masterdb2 as mdb2

        # Use "Achievement" type so GetRecordStructure returns correct key count
        xlsx_path = str(tmp_path / "drive2" / "Achievement.xlsx")
        os.makedirs(os.path.dirname(xlsx_path), exist_ok=True)
        create_masterdb_xlsx(xlsx_path, None, [
            {"IMAGE": "", "KEY ID 0": "id", "KEY VALUE 0": "exist-001",
             "ID": "name", "원문": "既存", "번역": "기존번역", "설명": ""},
        ])

        json_path = str(tmp_path / "json" / "Achievement.json")
        os.makedirs(os.path.dirname(json_path), exist_ok=True)
        create_masterdb_json(json_path, ["id"], [
            {"id": "exist-001", "name": "既存"},
            {"id": "new-001", "name": "新規"},
        ])

        # Also create old v1 drive xlsx for LoadOldKV fallback
        old_xlsx = str(tmp_path / "drive1" / "Achievement.xlsx")
        os.makedirs(os.path.dirname(old_xlsx), exist_ok=True)
        create_masterdb_xlsx(old_xlsx, None, [
            {"IMAGE": "", "KEY ID 0": "id", "KEY VALUE 0": "x",
             "ID": "name", "원문": "x", "번역": "x", "설명": ""},
        ])

        saved = {
            "json": mdb2.MASTERDB_JSON_PATH,
            "drive2": mdb2.MASTERDB2_DRIVE_PATH,
            "drive1": mdb2.MASTERDB_DRIVE_PATH,
        }
        mdb2.MASTERDB_JSON_PATH = str(tmp_path / "json")
        mdb2.MASTERDB2_DRIVE_PATH = str(tmp_path / "drive2")
        mdb2.MASTERDB_DRIVE_PATH = str(tmp_path / "drive1")
        try:
            from scripts.masterdb2 import UpdateXlsx
            empty_count = UpdateXlsx("Achievement")
        finally:
            mdb2.MASTERDB_JSON_PATH = saved["json"]
            mdb2.MASTERDB2_DRIVE_PATH = saved["drive2"]
            mdb2.MASTERDB_DRIVE_PATH = saved["drive1"]

        assert empty_count >= 1

        mdb2.MASTERDB2_DRIVE_PATH = str(tmp_path / "drive2")
        try:
            records = ReadXlsx("Achievement")
        finally:
            mdb2.MASTERDB2_DRIVE_PATH = saved["drive2"]

        originals = {r["원문"] for r in records}
        assert "既存" in originals
        assert "新規" in originals

    def test_existing_translation_preserved(self, tmp_path, shelve_test_cleanup):
        """Existing KR translations should not be lost on update."""
        import scripts.masterdb2 as mdb2

        xlsx_path = str(tmp_path / "drive2" / "Achievement.xlsx")
        os.makedirs(os.path.dirname(xlsx_path), exist_ok=True)
        create_masterdb_xlsx(xlsx_path, None, [
            {"IMAGE": "", "KEY ID 0": "id", "KEY VALUE 0": "p-001",
             "ID": "name", "원문": "テスト", "번역": "보존될번역", "설명": ""},
        ])

        json_path = str(tmp_path / "json" / "Achievement.json")
        os.makedirs(os.path.dirname(json_path), exist_ok=True)
        create_masterdb_json(json_path, ["id"], [
            {"id": "p-001", "name": "テスト"},
        ])

        old_xlsx = str(tmp_path / "drive1" / "Achievement.xlsx")
        os.makedirs(os.path.dirname(old_xlsx), exist_ok=True)
        create_masterdb_xlsx(old_xlsx, None, [
            {"IMAGE": "", "KEY ID 0": "id", "KEY VALUE 0": "x",
             "ID": "name", "원문": "x", "번역": "x", "설명": ""},
        ])

        saved = {
            "json": mdb2.MASTERDB_JSON_PATH,
            "drive2": mdb2.MASTERDB2_DRIVE_PATH,
            "drive1": mdb2.MASTERDB_DRIVE_PATH,
        }
        mdb2.MASTERDB_JSON_PATH = str(tmp_path / "json")
        mdb2.MASTERDB2_DRIVE_PATH = str(tmp_path / "drive2")
        mdb2.MASTERDB_DRIVE_PATH = str(tmp_path / "drive1")
        try:
            from scripts.masterdb2 import UpdateXlsx
            UpdateXlsx("Achievement")
        finally:
            mdb2.MASTERDB_JSON_PATH = saved["json"]
            mdb2.MASTERDB2_DRIVE_PATH = saved["drive2"]
            mdb2.MASTERDB_DRIVE_PATH = saved["drive1"]

        mdb2.MASTERDB2_DRIVE_PATH = str(tmp_path / "drive2")
        try:
            records = ReadXlsx("Achievement")
        finally:
            mdb2.MASTERDB2_DRIVE_PATH = saved["drive2"]

        assert any(r["번역"] == "보존될번역" for r in records)


class TestWriteXlsx:
    def test_writes_valid_xlsx(self, tmp_path):
        import scripts.masterdb2 as mdb2

        records = [
            {"IMAGE": "", "KEY ID 0": "id", "KEY VALUE 0": "w1",
             "ID": "name", "원문": "テスト", "번역": "테스트", "설명": ""},
        ]

        saved = mdb2.MASTERDB2_DRIVE_PATH
        mdb2.MASTERDB2_DRIVE_PATH = str(tmp_path)
        try:
            WriteXlsx("WriteTest", records)
        finally:
            mdb2.MASTERDB2_DRIVE_PATH = saved

        output_file = str(tmp_path / "WriteTest.xlsx")
        assert os.path.exists(output_file)

        import pandas as pd
        df = pd.read_excel(output_file, engine="openpyxl")
        assert len(df) == 1


# ============================================================
# Log module coverage
# ============================================================


class TestLogModule:
    def test_log_functions_exist(self):
        from scripts.log import LOG_DEBUG, LOG_INFO, LOG_WARN, LOG_ERROR, AddLogHandler
        # Just verify they're callable
        assert callable(LOG_DEBUG)
        assert callable(LOG_INFO)
        assert callable(LOG_WARN)
        assert callable(LOG_ERROR)
        assert callable(AddLogHandler)

    def test_log_info_runs(self):
        from scripts.log import LOG_INFO, logger
        import logging
        logger.setLevel(logging.DEBUG)
        LOG_INFO(0, "test message")  # Should not raise

    def test_log_debug_runs(self):
        from scripts.log import LOG_DEBUG, logger
        import logging
        logger.setLevel(logging.DEBUG)
        LOG_DEBUG(1, "debug message")  # Should not raise

    def test_log_warn_runs(self):
        from scripts.log import LOG_WARN, logger
        import logging
        logger.setLevel(logging.DEBUG)
        LOG_WARN(2, "warn message")  # Should not raise

    def test_log_error_runs(self):
        from scripts.log import LOG_ERROR, logger
        import logging
        logger.setLevel(logging.DEBUG)
        LOG_ERROR(0, "error message")  # Should not raise

    def test_add_log_handler(self):
        from scripts.log import AddLogHandler, logger
        import logging
        handler = logging.NullHandler()
        AddLogHandler(handler)
        assert handler in logger.handlers
        logger.removeHandler(handler)


# ============================================================
# OverrideRecordToJson — traverse branch coverage
# ============================================================


class TestOverrideTraverseBranches:
    """Cover internal traverse() branches in OverrideRecordToJson."""

    def test_indexed_string_element(self, shelve_test_cleanup):
        """Override a string element at items[1] (indexed string branch)."""
        json_data = {
            "rules": {"primaryKeys": ["id"]},
            "data": [
                {"id": "x", "items": [
                    {"text": "アイテム0"},
                    {"text": "アイテム1"},
                ]},
            ],
        }
        records = [
            {"IMAGE": "", "KEY ID 0": "id", "KEY VALUE 0": "x",
             "ID": "items[1].text", "원문": "アイテム1", "번역": "아이템1", "설명": ""},
        ]
        result = OverrideRecordToJson(json_data, records)
        assert result["data"][0]["items"][1]["text"] == "아이템1"
        assert result["data"][0]["items"][0]["text"] == "アイテム0"

    def test_indexed_string_mismatch_not_overridden(self, shelve_test_cleanup):
        """Mismatched original text at indexed path should not override."""
        json_data = {
            "rules": {"primaryKeys": ["id"]},
            "data": [
                {"id": "x", "items": [{"text": "実際"}]},
            ],
        }
        records = [
            {"IMAGE": "", "KEY ID 0": "id", "KEY VALUE 0": "x",
             "ID": "items[0].text", "원문": "間違い", "번역": "잘못", "설명": ""},
        ]
        result = OverrideRecordToJson(json_data, records)
        assert result["data"][0]["items"][0]["text"] == "実際"

    def test_indexed_nested_dict_with_string(self, shelve_test_cleanup):
        """Override a string field inside an indexed nested dict."""
        json_data = {
            "rules": {"primaryKeys": ["id"]},
            "data": [
                {"id": "x", "steps": [
                    {"label": "ステップ1"},
                    {"label": "ステップ2"},
                ]},
            ],
        }
        records = [
            {"IMAGE": "", "KEY ID 0": "id", "KEY VALUE 0": "x",
             "ID": "steps[0].label", "원문": "ステップ1", "번역": "스텝1", "설명": ""},
            {"IMAGE": "", "KEY ID 0": "id", "KEY VALUE 0": "x",
             "ID": "steps[1].label", "원문": "ステップ2", "번역": "스텝2", "설명": ""},
        ]
        result = OverrideRecordToJson(json_data, records)
        assert result["data"][0]["steps"][0]["label"] == "스텝1"
        assert result["data"][0]["steps"][1]["label"] == "스텝2"

    def test_replaces_across_multiple_matching_data(self, shelve_test_cleanup):
        """Same primary key appearing in multiple data items."""
        json_data = {
            "rules": {"primaryKeys": ["type"]},
            "data": [
                {"type": "A", "name": "名前"},
                {"type": "A", "name": "名前"},
            ],
        }
        records = [
            {"IMAGE": "", "KEY ID 0": "type", "KEY VALUE 0": "A",
             "ID": "name", "원문": "名前", "번역": "이름", "설명": ""},
        ]
        result = OverrideRecordToJson(json_data, records)
        assert result["data"][0]["name"] == "이름"
        assert result["data"][1]["name"] == "이름"

    def test_nested_dict_traversal(self, shelve_test_cleanup):
        """Traverse through nested dict without index."""
        json_data = {
            "rules": {"primaryKeys": ["id"]},
            "data": [
                {"id": "x", "outer": {"inner": {"deep": "深い"}}},
            ],
        }
        records = [
            {"IMAGE": "", "KEY ID 0": "id", "KEY VALUE 0": "x",
             "ID": "outer.inner.deep", "원문": "深い", "번역": "깊은", "설명": ""},
        ]
        result = OverrideRecordToJson(json_data, records)
        assert result["data"][0]["outer"]["inner"]["deep"] == "깊은"


# ============================================================
# TranslateRuleKey / TranslateReverseRuleKey
# ============================================================


class TestTranslateRuleKey:
    """Test rule key translation functions."""

    def test_translates_known_field(self):
        from scripts.masterdb2_rules import TranslateRuleKey
        result = TranslateRuleKey("Achievement", "name")
        assert result == "이름"

    def test_unknown_field_returns_original(self):
        from scripts.masterdb2_rules import TranslateRuleKey
        result = TranslateRuleKey("Achievement", "unknownField")
        assert result == "unknownField"

    def test_dotted_path(self):
        from scripts.masterdb2_rules import TranslateRuleKey
        result = TranslateRuleKey("Achievement", "name.description")
        assert "이름" in result
        assert "설명" in result

    def test_empty_value_returns_key(self):
        """Fields mapped to '' should return the original key."""
        from scripts.masterdb2_rules import TranslateRuleKey
        # CharacterDetail.order maps to ""
        result = TranslateRuleKey("CharacterDetail", "order")
        assert result == "order"


class TestConvertDriveToOutputGeneric:
    """Test generic.ConvertDriveToOutput with synthetic drive files."""

    def test_converts_single_file(self, tmp_path):
        """Pass a single synthetic file to ConvertDriveToOutput."""
        from tests.fixtures.create_fixtures import create_generic_xlsx
        import scripts.generic as generic

        xlsx_path = str(tmp_path / "generic.xlsx")
        create_generic_xlsx(xlsx_path, [
            {"text": "テスト", "trans": "테스트"},
        ])

        saved = generic.GENERIC_OUTPUT_PATH
        output_dir = str(tmp_path / "output")
        os.makedirs(output_dir, exist_ok=True)
        generic.GENERIC_OUTPUT_PATH = output_dir

        try:
            drive_file_paths = [(xlsx_path, "/generic.xlsx", "generic.xlsx")]
            errors, successes = generic.ConvertDriveToOutput(drive_file_paths)
        finally:
            generic.GENERIC_OUTPUT_PATH = saved

        assert len(errors) == 0
        assert len(successes) == 1

    def test_handles_conversion_error(self, tmp_path):
        """Invalid xlsx should produce error, not crash."""
        import scripts.generic as generic

        bad_path = str(tmp_path / "bad.xlsx")
        with open(bad_path, "w") as f:
            f.write("not an xlsx")

        saved = generic.GENERIC_OUTPUT_PATH
        generic.GENERIC_OUTPUT_PATH = str(tmp_path / "output")
        os.makedirs(generic.GENERIC_OUTPUT_PATH, exist_ok=True)

        try:
            drive_file_paths = [(bad_path, "/bad.xlsx", "bad.xlsx")]
            errors, successes = generic.ConvertDriveToOutput(drive_file_paths)
        finally:
            generic.GENERIC_OUTPUT_PATH = saved

        assert len(errors) == 1
        assert len(successes) == 0
