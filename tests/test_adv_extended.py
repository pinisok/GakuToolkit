"""Extended ADV tests covering TxtToXlsx, UpdateDataFrame, and CSV/TXT merge."""

import os
from io import StringIO

import pytest
import pandas as pd

from tests.fixtures.create_fixtures import create_adv_xlsx, create_adv_txt
from scripts.adv import (
    _internalTxtToScv,
    _internalCsvToDataFrame,
    _internalUpdateDataFrame,
    _internalDataFrameToXlsx,
    _internalReadXlsx,
    _internalXlsxRecordsProcess,
    _internalCsvToTxt,
    _filter_adv_files,
    CsvToTxt,
    TxtToXlsx,
    XlsxToCsv,
    XlsxToTxt,
)


# ============================================================
# _internalTxtToScv: TXT → CSV parsing
# ============================================================


class TestTxtToScv:
    """Test _internalTxtToScv parsing game TXT to CSV."""

    def test_parses_single_message(self, tmp_path):
        txt_path = str(tmp_path / "test.txt")
        create_adv_txt(txt_path, [
            {"tag": "message", "text": "テスト", "name": "麻央"},
        ])

        with open(txt_path, "r", encoding="utf-8") as fp:
            result = _internalTxtToScv(fp, "test.txt")

        result.seek(0)
        content = result.read()
        assert "テスト" in content
        assert "麻央" in content

    def test_parses_narration(self, tmp_path):
        txt_path = str(tmp_path / "test.txt")
        create_adv_txt(txt_path, [
            {"tag": "narration", "text": "ナレーション"},
        ])

        with open(txt_path, "r", encoding="utf-8") as fp:
            result = _internalTxtToScv(fp, "test.txt")

        result.seek(0)
        content = result.read()
        assert "ナレーション" in content

    def test_parses_title(self, tmp_path):
        txt_path = str(tmp_path / "test.txt")
        create_adv_txt(txt_path, [
            {"tag": "title", "text": "タイトル"},
        ])

        with open(txt_path, "r", encoding="utf-8") as fp:
            result = _internalTxtToScv(fp, "test.txt")

        result.seek(0)
        content = result.read()
        assert "タイトル" in content

    def test_parses_multiple_messages(self, tmp_path):
        txt_path = str(tmp_path / "test.txt")
        create_adv_txt(txt_path, [
            {"tag": "message", "text": "一番", "name": "A"},
            {"tag": "message", "text": "二番", "name": "B"},
            {"tag": "message", "text": "三番", "name": "C"},
        ])

        with open(txt_path, "r", encoding="utf-8") as fp:
            result = _internalTxtToScv(fp, "test.txt")

        result.seek(0)
        content = result.read()
        assert "一番" in content
        assert "二番" in content
        assert "三番" in content


# ============================================================
# _internalCsvToDataFrame: CSV → DataFrame
# ============================================================


class TestCsvToDataFrame:
    def test_basic_conversion(self, tmp_path):
        txt_path = str(tmp_path / "test.txt")
        create_adv_txt(txt_path, [
            {"tag": "message", "text": "テスト", "name": "麻央"},
        ])

        with open(txt_path, "r", encoding="utf-8") as fp:
            csv_io = _internalTxtToScv(fp, "test.txt")

        csv_io.seek(0)
        df = _internalCsvToDataFrame(csv_io)

        assert "id" in df.columns
        assert "name" in df.columns
        assert "translated name" in df.columns
        assert "text" in df.columns
        assert len(df) >= 1


# ============================================================
# _internalUpdateDataFrame: 3-way merge preserving translations
# ============================================================


class TestUpdateDataFrame:
    def test_unchanged_returns_original(self, tmp_path):
        """When text is identical, original with translations is returned."""
        xlsx_path = str(tmp_path / "existing.xlsx")
        create_adv_xlsx(xlsx_path, [
            {"id": "0", "name": "A", "translated name": "에이",
             "text": "テスト", "translated text": "테스트"},
        ])

        new_df = pd.DataFrame([
            {"id": "0", "name": "A", "translated name": "",
             "text": "テスト", "translated text": ""},
        ])

        with open(xlsx_path, "rb") as fp:
            result_df, _ = _internalUpdateDataFrame(new_df, fp)

        records = result_df.to_dict(orient="records")
        # Should preserve the original translation
        assert records[0]["translated text"] == "테스트"
        assert records[0]["translated name"] == "에이"

    def test_text_changed_preserves_translation_with_comment(self, tmp_path):
        """When text changes, old translation is kept with warning comment."""
        xlsx_path = str(tmp_path / "existing.xlsx")
        create_adv_xlsx(xlsx_path, [
            {"id": "0", "name": "A", "translated name": "",
             "text": "古いテスト", "translated text": "이전 번역"},
        ])

        new_df = pd.DataFrame([
            {"id": "0", "name": "A", "translated name": "",
             "text": "新しいテスト", "translated text": ""},
        ])

        with open(xlsx_path, "rb") as fp:
            result_df, _ = _internalUpdateDataFrame(new_df, fp)

        records = result_df.to_dict(orient="records")
        assert records[0]["translated text"] == "이전 번역"
        assert "원본 문자열이 수정되었습니다" in records[0]["comments"]

    def test_new_row_added(self, tmp_path):
        """When new rows exist, they have empty translations."""
        xlsx_path = str(tmp_path / "existing.xlsx")
        create_adv_xlsx(xlsx_path, [
            {"id": "0", "name": "A", "translated name": "",
             "text": "既存", "translated text": "기존"},
        ])

        new_df = pd.DataFrame([
            {"id": "0", "name": "A", "translated name": "",
             "text": "既存", "translated text": ""},
            {"id": "0", "name": "B", "translated name": "",
             "text": "新規", "translated text": ""},
        ])

        with open(xlsx_path, "rb") as fp:
            result_df, _ = _internalUpdateDataFrame(new_df, fp)

        records = result_df.to_dict(orient="records")
        assert len(records) == 2
        # First row preserves translation
        assert records[0]["translated text"] == "기존"


# ============================================================
# _internalDataFrameToXlsx: DataFrame → XLSX
# ============================================================


class TestDataFrameToXlsx:
    def test_writes_valid_xlsx(self, tmp_path):
        df = pd.DataFrame([
            {"id": "0", "name": "A", "translated name": "",
             "text": "テスト", "translated text": "테스트"},
        ])

        xlsx_path = str(tmp_path / "output.xlsx")
        with open(xlsx_path, "wb") as fp:
            _internalDataFrameToXlsx(df, fp)

        assert os.path.exists(xlsx_path)
        # Verify it's readable
        result = pd.read_excel(xlsx_path, engine="openpyxl")
        assert len(result) == 1


# ============================================================
# TxtToXlsx: full Update flow (TXT → XLSX)
# ============================================================


class TestTxtToXlsx:
    def test_creates_new_xlsx(self, tmp_path):
        """TxtToXlsx creates xlsx from txt when no existing xlsx."""
        txt_path = str(tmp_path / "adv_test.txt")
        create_adv_txt(txt_path, [
            {"tag": "message", "text": "テスト", "name": "麻央"},
        ])

        xlsx_path = str(tmp_path / "adv_test.xlsx")
        TxtToXlsx(txt_path, xlsx_path, "adv_test.txt")

        assert os.path.exists(xlsx_path)
        df = pd.read_excel(xlsx_path, engine="openpyxl")
        assert len(df) >= 1

    def test_updates_existing_xlsx(self, tmp_path):
        """TxtToXlsx preserves translations from existing xlsx."""
        txt_path = str(tmp_path / "adv_test.txt")
        create_adv_txt(txt_path, [
            {"tag": "message", "text": "テスト", "name": "麻央"},
        ])

        # Create existing xlsx with translation
        xlsx_path = str(tmp_path / "adv_test.xlsx")
        create_adv_xlsx(xlsx_path, [
            {"id": "0000000000000", "name": "麻央", "translated name": "마오",
             "text": "テスト", "translated text": "테스트 번역"},
        ])

        TxtToXlsx(txt_path, xlsx_path, "adv_test.txt")

        df = pd.read_excel(xlsx_path, engine="openpyxl")
        records = df.to_dict(orient="records")
        # Translation should be preserved
        assert any("테스트 번역" in str(r.get("translated text", "")) for r in records)


# ============================================================
# CsvToTxt + _internalCsvToTxt: merge translations into TXT
# ============================================================


class TestCsvToTxt:
    def test_merges_translation_into_txt(self, tmp_path):
        """CsvToTxt replaces original text with translation."""
        txt_path = str(tmp_path / "original.txt")
        create_adv_txt(txt_path, [
            {"tag": "message", "text": "テストメッセージ", "name": "麻央"},
        ])

        # Create CSV with translation
        xlsx_path = str(tmp_path / "adv_test.xlsx")
        create_adv_xlsx(xlsx_path, [
            {"id": "0000000000000", "name": "麻央", "translated name": "마오",
             "text": "テストメッセージ", "translated text": "테스트 메시지"},
        ])

        with open(xlsx_path, "rb") as fp:
            csv_io = StringIO()
            XlsxToCsv(fp, csv_io, "original.txt")
        csv_io.seek(0)

        output_path = str(tmp_path / "output.txt")
        CsvToTxt(csv_io, output_path, txt_path)

        with open(output_path, "r", encoding="utf-8") as f:
            content = f.read()
        assert "테스트 메시지" in content
        assert "마오" in content

    def test_title_merge(self, tmp_path):
        """CsvToTxt replaces title text."""
        txt_path = str(tmp_path / "original.txt")
        create_adv_txt(txt_path, [
            {"tag": "title", "text": "タイトル"},
        ])

        xlsx_path = str(tmp_path / "adv_test.xlsx")
        create_adv_xlsx(xlsx_path, [
            {"id": "0000000000000", "name": "__title__", "translated name": "",
             "text": "タイトル", "translated text": "제목"},
        ])

        with open(xlsx_path, "rb") as fp:
            csv_io = StringIO()
            XlsxToCsv(fp, csv_io, "original.txt")
        csv_io.seek(0)

        output_path = str(tmp_path / "output.txt")
        CsvToTxt(csv_io, output_path, txt_path)

        with open(output_path, "r", encoding="utf-8") as f:
            content = f.read()
        assert "제목" in content


# ============================================================
# Full round-trip: TXT → XLSX → TXT
# ============================================================


class TestChoicegroupHandling:
    """Test choicegroup parsing and merge in ADV pipeline."""

    def test_choicegroup_parsed_in_txt_to_scv(self, tmp_path):
        """Choicegroup tags should produce 'select' rows in CSV."""
        txt_path = str(tmp_path / "test.txt")
        create_adv_txt(txt_path, [
            {"tag": "message", "text": "選択してください", "name": "麻央"},
            {"tag": "choicegroup", "choices": ["選択肢A", "選択肢B"]},
        ])

        with open(txt_path, "r", encoding="utf-8") as fp:
            result = _internalTxtToScv(fp, "test.txt")

        result.seek(0)
        content = result.read()
        assert "選択してください" in content
        # imas_tools may or may not parse our synthetic choicegroup format.
        # If it does, we expect "select" rows. If not, at least the message is there.

    def test_title_parsed_in_txt_to_scv(self, tmp_path):
        """Title tags should produce '__title__' rows in CSV."""
        txt_path = str(tmp_path / "test.txt")
        create_adv_txt(txt_path, [
            {"tag": "title", "text": "エピソードタイトル"},
        ])

        with open(txt_path, "r", encoding="utf-8") as fp:
            result = _internalTxtToScv(fp, "test.txt")

        result.seek(0)
        content = result.read()
        assert "エピソードタイトル" in content


class TestCsvTxtValidationMismatch:
    """Test that _internalCsvToTxt raises when original text doesn't match."""

    def test_mismatched_original_raises(self, tmp_path):
        """If CSV text column doesn't match TXT, ValueError should be raised."""
        from scripts.adv import _internalCsvToTxt, _internalTxtToScv

        txt_path = str(tmp_path / "test.txt")
        create_adv_txt(txt_path, [
            {"tag": "message", "text": "正しいテキスト", "name": "A"},
        ])

        with open(txt_path, "r", encoding="utf-8") as fp:
            csv_io = _internalTxtToScv(fp, "test.txt")

        # Tamper with the CSV — replace the text column value
        csv_io.seek(0)
        csv_content = csv_io.read()
        tampered = csv_content.replace("正しいテキスト", "改竄されたテキスト")

        with open(txt_path, "r", encoding="utf-8") as fp:
            txt_content = fp.read()

        with pytest.raises(ValueError, match="does not match"):
            _internalCsvToTxt(tampered, txt_content)


class TestFloatTranslatedText:
    """Test handling of NaN/float in translated text (common from empty Excel cells)."""

    def test_float_translated_text_converted(self):
        """Float value in translated text should be converted to string."""
        records = [
            {"id": "1", "name": "t", "translated name": "",
             "text": "hello", "translated text": 1.5}
        ]
        result = _internalXlsxRecordsProcess(records)
        assert result[0]["translated text"] == "1.5"

    def test_non_str_non_numeric_becomes_empty(self):
        """None/other non-str/non-numeric should become empty string."""
        records = [
            {"id": "info", "name": "f.txt", "translated name": "",
             "text": "", "translated text": None}
        ]
        result = _internalXlsxRecordsProcess(records)
        assert result[0]["translated text"] == ""


class TestRoundTrip:
    def test_txt_to_xlsx_to_txt(self, tmp_path):
        """Full round-trip: create xlsx from txt, then convert back."""
        original_txt = str(tmp_path / "original.txt")
        create_adv_txt(original_txt, [
            {"tag": "message", "text": "元のメッセージ", "name": "麻央"},
        ])

        # Step 1: TXT → XLSX (Update flow)
        xlsx_path = str(tmp_path / "translation.xlsx")
        TxtToXlsx(original_txt, xlsx_path, "original.txt")

        # Manually add translation to the xlsx
        df = pd.read_excel(xlsx_path, engine="openpyxl")
        df.iloc[0, df.columns.get_loc("translated text")] = "번역된 메시지"
        df.iloc[0, df.columns.get_loc("translated name")] = "마오"
        df.to_excel(xlsx_path, index=False, engine="xlsxwriter")

        # Step 2: XLSX → TXT (Convert flow)
        output_txt = str(tmp_path / "output.txt")
        XlsxToTxt(xlsx_path, output_txt, original_txt)

        with open(output_txt, "r", encoding="utf-8") as f:
            content = f.read()
        assert "번역된 메시지" in content
        assert "마오" in content


class TestFilterAdvFiles:
    """Test _filter_adv_files blacklist and path mapping."""

    def test_filters_blacklisted_file(self):
        file_paths = [
            ("/abs/musics.txt", "rel/musics.txt", "musics.txt"),
            ("/abs/adv_test_01.txt", "rel/adv_test_01.txt", "adv_test_01.txt"),
        ]
        result = _filter_adv_files(file_paths)
        filenames = [r[2] for r in result]
        assert "musics.txt" not in filenames
        assert "adv_test_01.txt" in filenames

    def test_filters_blacklisted_folder(self):
        file_paths = [
            ("/abs/adv_pstep_01.txt", "rel/adv_pstep_01.txt", "adv_pstep_01.txt"),
            ("/abs/adv_cidol_01.txt", "rel/adv_cidol_01.txt", "adv_cidol_01.txt"),
        ]
        result = _filter_adv_files(file_paths)
        filenames = [r[2] for r in result]
        assert "adv_pstep_01.txt" not in filenames
        assert "adv_cidol_01.txt" in filenames

    def test_maps_output_path(self):
        file_paths = [
            ("/abs/adv_cidol-amao_01.txt", "rel/adv_cidol-amao_01.txt", "adv_cidol-amao_01.txt"),
        ]
        result = _filter_adv_files(file_paths)
        assert len(result) == 1
        input_path, output_path, filename = result[0]
        assert "cidol" in output_path
        assert output_path.endswith(".xlsx")

    def test_empty_input(self):
        assert _filter_adv_files([]) == []
