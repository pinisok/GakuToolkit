"""Tests for MasterDB2 pipeline using synthetic fixtures."""

import os
import json

import pytest

from tests.fixtures.create_fixtures import create_masterdb_xlsx, create_masterdb_json
from scripts.masterdb2 import (
    check_need_export,
    path_normalize_for_pk,
    rule_key_translate_map,
    DB_save,
    DB_get,
    GetRecordStructure,
    ReadXlsx,
    ReadJson,
    WriteJson,
    OverrideRecordToJson,
)


# ============================================================
# Pure unit tests
# ============================================================


class TestCheckNeedExport:
    def test_japanese_text_needs_export(self):
        assert check_need_export("アチーブメント") is True

    def test_korean_text_needs_export(self):
        assert check_need_export("업적 설명") is True

    def test_ascii_only_no_export(self):
        assert check_need_export("ProduceCardCategory_Unknown") is False

    def test_mixed_ascii_and_cjk(self):
        assert check_need_export("hello 世界") is True

    def test_empty_string_no_export(self):
        assert check_need_export("") is False

    def test_punctuation_only_no_export(self):
        assert check_need_export("!!!???...") is False

    def test_numbers_only_no_export(self):
        assert check_need_export("12345") is False

    def test_spaces_only_no_export(self):
        assert check_need_export("   ") is False

    def test_single_non_ascii_char(self):
        assert check_need_export("あ") is True


class TestPathNormalizeForPk:
    def test_removes_single_index(self):
        assert path_normalize_for_pk("items[2].name") == "items.name"

    def test_removes_multiple_indices(self):
        assert path_normalize_for_pk("a[0].b[1].c[2]") == "a.b.c"

    def test_no_index_unchanged(self):
        assert path_normalize_for_pk("simple.path") == "simple.path"

    def test_single_field_unchanged(self):
        assert path_normalize_for_pk("name") == "name"

    def test_large_index(self):
        assert path_normalize_for_pk("items[999].value") == "items.value"

    def test_empty_string(self):
        assert path_normalize_for_pk("") == ""


class TestRuleKeyTranslateMap:
    def test_has_achievement(self):
        assert "Achievement" in rule_key_translate_map
        assert rule_key_translate_map["Achievement"]["name"] == "이름"

    def test_has_character(self):
        assert "Character" in rule_key_translate_map
        assert rule_key_translate_map["Character"]["lastName"] == "이름"

    def test_has_music(self):
        assert "Music" in rule_key_translate_map
        assert rule_key_translate_map["Music"]["title"] == "제목"

    def test_has_story(self):
        assert "Story" in rule_key_translate_map
        assert rule_key_translate_map["Story"]["title"] == "제목"

    def test_non_translatable_fields_are_empty_string(self):
        for dtype, fields in rule_key_translate_map.items():
            for field, translation in fields.items():
                assert isinstance(translation, str), (
                    f"{dtype}.{field} is not string: {translation}"
                )

    def test_map_has_many_entries(self):
        assert len(rule_key_translate_map) > 30


class TestShelveDB:
    def test_save_and_get(self, shelve_test_cleanup):
        DB_save("test_key_123", "test_value_456")
        assert DB_get("test_key_123") == "test_value_456"

    def test_get_default_for_missing(self, shelve_test_cleanup):
        assert DB_get("nonexistent_key_xyz", "default") == "default"

    def test_get_empty_default(self, shelve_test_cleanup):
        assert DB_get("nonexistent_key_abc") == ""

    def test_overwrite_existing(self, shelve_test_cleanup):
        DB_save("overwrite_key", "old")
        DB_save("overwrite_key", "new")
        assert DB_get("overwrite_key") == "new"


class TestGetRecordStructure:
    def test_has_required_fields(self):
        structure = GetRecordStructure("Achievement")
        assert "IMAGE" in structure
        assert "ID" in structure
        assert "원문" in structure
        assert "번역" in structure
        assert "설명" in structure

    def test_has_primary_key_columns(self):
        structure = GetRecordStructure("Achievement")
        assert "KEY ID 0" in structure
        assert "KEY VALUE 0" in structure

    def test_unknown_type_returns_minimal(self):
        structure = GetRecordStructure("NonExistentType")
        assert "IMAGE" in structure
        assert "ID" in structure
        assert "원문" in structure


# ============================================================
# Synthetic fixture tests
# ============================================================


class TestReadXlsxSynthetic:
    """Test ReadXlsx with synthetic xlsx files."""

    def test_reads_basic_xlsx(self, tmp_path):
        """Read a synthetic masterdb xlsx and verify records."""
        import scripts.masterdb2 as mdb2

        xlsx_path = str(tmp_path / "TestType.xlsx")
        create_masterdb_xlsx(xlsx_path, None, [
            {"IMAGE": "", "KEY ID 0": "id", "KEY VALUE 0": "test-001",
             "ID": "name", "원문": "テスト名前", "번역": "테스트 이름", "설명": ""},
            {"IMAGE": "", "KEY ID 0": "id", "KEY VALUE 0": "test-001",
             "ID": "description", "원문": "テスト説明", "번역": "테스트 설명", "설명": ""},
        ])

        original_path = mdb2.MASTERDB2_DRIVE_PATH
        mdb2.MASTERDB2_DRIVE_PATH = str(tmp_path)
        try:
            records = ReadXlsx("TestType")
        finally:
            mdb2.MASTERDB2_DRIVE_PATH = original_path

        assert len(records) == 2
        assert records[0]["원문"] == "テスト名前"
        assert records[0]["번역"] == "테스트 이름"
        assert records[1]["ID"] == "description"

    def test_empty_xlsx_returns_empty(self, tmp_path):
        """ReadXlsx for non-existent file returns empty list."""
        import scripts.masterdb2 as mdb2

        original_path = mdb2.MASTERDB2_DRIVE_PATH
        mdb2.MASTERDB2_DRIVE_PATH = str(tmp_path)
        try:
            records = ReadXlsx("NonExistent")
        finally:
            mdb2.MASTERDB2_DRIVE_PATH = original_path

        assert records == []


class TestReadWriteJsonSynthetic:
    """Test ReadJson/WriteJson with synthetic data."""

    def test_read_synthetic_json(self, tmp_path):
        """Read a synthetic masterdb JSON file."""
        import scripts.masterdb2 as mdb2

        json_path = str(tmp_path / "TestType.json")
        create_masterdb_json(json_path, ["id"], [
            {"id": "test-001", "name": "テスト", "description": "説明"},
        ])

        original_path = mdb2.MASTERDB_JSON_PATH
        mdb2.MASTERDB_JSON_PATH = str(tmp_path)
        try:
            data = ReadJson("TestType")
        finally:
            mdb2.MASTERDB_JSON_PATH = original_path

        assert "rules" in data
        assert "data" in data
        assert data["rules"]["primaryKeys"] == ["id"]
        assert len(data["data"]) == 1
        assert data["data"][0]["name"] == "テスト"

    def test_write_json(self, tmp_path):
        """WriteJson creates valid output JSON."""
        import scripts.masterdb2 as mdb2

        json_data = {
            "rules": {"primaryKeys": ["id"]},
            "data": [{"id": "x", "name": "translated"}],
        }

        original_path = mdb2.MASTERDB_OUTPUT_PATH
        mdb2.MASTERDB_OUTPUT_PATH = str(tmp_path)
        try:
            WriteJson("TestOutput", json_data)
        finally:
            mdb2.MASTERDB_OUTPUT_PATH = original_path

        output_file = str(tmp_path / "TestOutput.json")
        assert os.path.exists(output_file)
        with open(output_file, "r", encoding="utf-8") as f:
            result = json.load(f)
        assert result["data"][0]["name"] == "translated"


class TestOverrideRecordToJsonSynthetic:
    """Test translation application with synthetic data."""

    def test_basic_override(self, tmp_path, shelve_test_cleanup):
        """Apply translation from XLSX records to JSON data."""
        json_data = {
            "rules": {"primaryKeys": ["id"]},
            "data": [
                {"id": "item-001", "name": "テスト", "description": "説明テスト"},
            ],
        }
        records = [
            {"KEY ID 0": "id", "KEY VALUE 0": "item-001",
             "ID": "name", "원문": "テスト", "번역": "테스트"},
            {"KEY ID 0": "id", "KEY VALUE 0": "item-001",
             "ID": "description", "원문": "説明テスト", "번역": "설명 테스트"},
        ]

        result = OverrideRecordToJson(json_data, records)

        assert result["data"][0]["name"] == "테스트"
        assert result["data"][0]["description"] == "설명 테스트"

    def test_unmatched_record_skipped(self, tmp_path, shelve_test_cleanup):
        """Records that don't match any JSON data should be skipped."""
        json_data = {
            "rules": {"primaryKeys": ["id"]},
            "data": [
                {"id": "item-001", "name": "original"},
            ],
        }
        records = [
            {"KEY ID 0": "id", "KEY VALUE 0": "nonexistent",
             "ID": "name", "원문": "x", "번역": "y"},
        ]

        result = OverrideRecordToJson(json_data, records)

        # Original should be unchanged
        assert result["data"][0]["name"] == "original"

    def test_empty_translation_skipped(self, tmp_path, shelve_test_cleanup):
        """Empty translation should not override original."""
        json_data = {
            "rules": {"primaryKeys": ["id"]},
            "data": [
                {"id": "item-001", "name": "元のテキスト"},
            ],
        }
        records = [
            {"KEY ID 0": "id", "KEY VALUE 0": "item-001",
             "ID": "name", "원문": "元のテキスト", "번역": ""},
        ]

        result = OverrideRecordToJson(json_data, records)

        # Should keep original since translation is empty
        assert result["data"][0]["name"] == "元のテキスト"


class TestIncrementalModeSkips:
    """Test that passing empty file list skips conversion (simulates no changes from download)."""

    def test_adv_incremental_skips(self):
        from scripts.adv import ConvertDriveToOutput
        errors, successes = ConvertDriveToOutput(drive_file_paths=[])
        assert errors == []
        assert successes == []

    def test_generic_incremental_skips(self):
        from scripts.generic import ConvertDriveToOutput
        errors, successes = ConvertDriveToOutput(drive_file_paths=[])
        assert errors == []
        assert successes == []

    def test_localization_incremental_skips(self):
        from scripts.localization import ConvertDriveToOutput
        errors, successes = ConvertDriveToOutput(drive_file_paths=[])
        assert errors == []
        assert successes == []

    def test_masterdb2_incremental_skips(self):
        from scripts.masterdb2 import ConvertDriveToOutput
        errors, successes = ConvertDriveToOutput(drive_file_paths=[])
        assert errors == []
        assert successes == []
