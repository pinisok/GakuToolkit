"""ADV merge and conversion logic.

Functions for:
- TXT → CSV parsing (game script to spreadsheet)
- CSV → DataFrame conversion
- 3-way merge (preserving existing translations)
- DataFrame → XLSX writing
- CSV+TXT merge (applying translations back to game script)
"""

import os
import re
from typing import Optional
from io import StringIO

import pandas as pd
import xlsxwriter

from imas_tools.story.gakuen_parser import parse_messages as _externalParser
from imas_tools.story.story_csv import StoryCsv as _externalStoryCsv

from .helper import XLSX_NAME_FORMAT, XLSX_TEXT_FORMAT
from .log import LOG_DEBUG, LOG_WARN
from .adv_record import _internalOverrideXlsxColumn, _internalReadXlsx


# ============================================================
# TXT → CSV parsing
# ============================================================


def _internalTxtToScv(read_fp, file_name: str) -> StringIO:
    """Parse a game TXT file into CSV format via imas_tools parser."""
    txt = _externalParser(read_fp.read())
    story_csv = _externalStoryCsv.new_empty_csv(file_name)
    for line in txt:
        if (line["__tag__"] == "message" or line["__tag__"] == "narration") and line.get("text"):
            story_csv.append_line({
                "id": "0000000000000",
                "name": line.get("name", "__narration__"),
                "text": line["text"],
                "trans": "",
            })
        if line["__tag__"] == "title" and line.get("title"):
            story_csv.append_line({
                "id": "0000000000000",
                "name": "__title__",
                "text": line["title"],
                "trans": "",
            })
        if line["__tag__"] == "choicegroup":
            if isinstance(line["choices"], list):
                for choice in line["choices"]:
                    story_csv.append_line({
                        "id": "select",
                        "name": "",
                        "text": choice["text"],
                        "trans": "",
                    })
            elif isinstance(line["choices"], dict):
                story_csv.append_line({
                    "id": "select",
                    "name": "",
                    "text": line["choices"]["text"],
                    "trans": "",
                })
            else:
                raise ValueError(f"Unknown choice type: {line['choices']}")
    return StringIO(str(story_csv))


# ============================================================
# CSV → DataFrame
# ============================================================


def _internalCsvToDataFrame(read_fp) -> pd.DataFrame:
    """Convert CSV (from _internalTxtToScv) into a DataFrame with standard columns."""
    adv_dataframe = pd.read_csv(read_fp, index_col=False)
    adv_dataframe.insert(loc=2, column='translated name', value="")
    adv_dataframe.drop(adv_dataframe.tail(2).index, inplace=True)
    _internalOverrideXlsxColumn(adv_dataframe)
    if len(adv_dataframe.index) < 1:
        raise ValueError("No message")
    adv_dataframe['text'] = adv_dataframe['text'].str.replace("\\n", "\n")
    return adv_dataframe


# ============================================================
# 3-way merge (preserving translations)
# ============================================================


def _internalUpdateDataFrame(
    new_dataframe: pd.DataFrame, original_fp, file_name: str = ""
) -> tuple[pd.DataFrame, list[str]]:
    """Merge new source DataFrame with existing translated xlsx.

    Returns (DataFrame, warnings_list).
    """
    orig_dataframe = _internalReadXlsx(original_fp)
    orig_records = orig_dataframe.to_dict(orient='records')
    new_records = new_dataframe.to_dict(orient='records')
    warnings = []
    bOverride = False

    orig_records_length = len(orig_records)
    new_records_length = len(new_records)
    if orig_records_length != new_records_length:
        bOverride = True
        warnings.append(f"줄 수 변경 ({orig_records_length} → {new_records_length})")

    for idx in range(len(new_records)):
        if idx >= orig_records_length:
            bOverride = True
            break
        if not bOverride and new_records[idx]["text"] != orig_records[idx]["text"]:
            bOverride = True

    if bOverride:
        for idx in range(len(new_records)):
            new_records[idx]["comments"] = ""
            if idx >= orig_records_length:
                continue
            new_records[idx]["translated name"] = orig_records[idx]["translated name"]
            new_records[idx]["translated text"] = orig_records[idx]["translated text"]
            if new_records[idx]["text"] != orig_records[idx]["text"]:
                warnings.append(f"원문 불일치 at line {idx}")
                LOG_WARN(0, f"[{file_name}] Unmatch original text at line {idx}")
                LOG_WARN(0, f"'{new_records[idx]['text']}' : '{orig_records[idx]['text']}'")
                new_records[idx]["comments"] = (
                    "원본 문자열이 수정되었습니다. 번역값이 적절한지 확인 후 해당 문구를 삭제해주세요"
                )
        return pd.DataFrame.from_dict(new_records), warnings
    return pd.DataFrame.from_dict(orig_records), warnings


# ============================================================
# DataFrame → XLSX
# ============================================================


def _internalDataFrameToXlsx(dataframe: pd.DataFrame, write_fp) -> None:
    """Write a DataFrame to an XLSX file with formatting."""
    writer = pd.ExcelWriter(write_fp, engine="xlsxwriter")
    dataframe.to_excel(writer, index=False, sheet_name="Sheet1")

    workbook = writer.book
    worksheet = writer.sheets['Sheet1']
    name_format = workbook.add_format(XLSX_NAME_FORMAT)
    worksheet.set_column(0, 2, 15, name_format)
    text_format = workbook.add_format(XLSX_TEXT_FORMAT)
    worksheet.set_column(3, 4, 70, text_format)

    writer.close()


# ============================================================
# Offset-based text replacement
# ============================================================


def _replace_at_offset(text: str, old: str, new: str, offset: int) -> tuple[str, int]:
    """Replace first occurrence of `old` in `text` at or after `offset`.

    Returns (new_text, new_offset_after_replacement).
    Raises ValueError if `old` is not found after `offset`.
    """
    idx = text.find(old, offset)
    if idx == -1:
        raise ValueError(
            f"Could not find '{old[:50]}' in text after offset {offset}"
        )
    result = text[:idx] + new + text[idx + len(old):]
    return result, idx + len(new)


# ============================================================
# CSV + TXT merge (apply translations back to game script)
# ============================================================


def _internalCsvToTxt(csv_strings: str, txt_strings: str) -> str:
    """Merge CSV translations into TXT game script.

    Walks through parsed TXT messages and replaces original text
    with translated text from CSV, using offset-based replacement.
    """
    def _merger(
        original_text: str,
        translated_text: str,
        validation_original_text: Optional[str] = None,
        *,
        is_choice=False,
    ):
        translated_text = re.sub(r"(?<!\\)=", r"\\=", translated_text)
        if (
            validation_original_text is not None
            and validation_original_text != original_text
        ):
            raise ValueError(
                f"Original text does not match validation text: "
                f"'{validation_original_text}' != '{original_text}'"
            )
        return translated_text

    story_csv = _externalStoryCsv(csv_strings)
    parsed_message = _externalParser(txt_strings)
    iterator = iter(story_csv.data)
    csv_row_idx = 0
    search_offset = 0

    def _next_csv_line():
        nonlocal csv_row_idx
        try:
            row = next(iterator)
            csv_row_idx += 1
            return row
        except StopIteration:
            raise ValueError(
                f"CSV has fewer rows than TXT messages. "
                f"Ran out at row {csv_row_idx}. "
                f"CSV has {len(story_csv.data)} rows."
            )

    for line in parsed_message:
        if line["__tag__"] == "message" or line["__tag__"] == "narration":
            if line.get("text"):
                next_csv_line = _next_csv_line()
                new_text = _merger(
                    line["text"], next_csv_line["trans"], next_csv_line["text"]
                )
                txt_strings, search_offset = _replace_at_offset(
                    txt_strings,
                    f"text={line['text']}",
                    f"text={new_text}",
                    search_offset,
                )
                # Name replacement (only for message with text, not narration)
                if line["__tag__"] == "message":
                    if (
                        line.get("name")
                        and line["name"] != ""
                        and next_csv_line["name"] != ""
                    ):
                        line_end = txt_strings.find("]", search_offset)
                        if line_end == -1:
                            line_end = len(txt_strings)
                        name_old = f"name={line['name']}"
                        name_new = f"name={next_csv_line['name']}"
                        name_idx = txt_strings.find(
                            name_old, search_offset, line_end + 1
                        )
                        if name_idx != -1:
                            len_diff = len(name_new) - len(name_old)
                            txt_strings = (
                                txt_strings[:name_idx]
                                + name_new
                                + txt_strings[name_idx + len(name_old):]
                            )
                            search_offset += len_diff

        if line["__tag__"] == "title":
            if line.get("title"):
                next_csv_line = _next_csv_line()
                new_text = _merger(
                    line["title"], next_csv_line["trans"], next_csv_line["text"]
                )
                txt_strings, search_offset = _replace_at_offset(
                    txt_strings,
                    f"title={line['title']}",
                    f"title={new_text}",
                    search_offset,
                )

        if line["__tag__"] == "choicegroup":
            if isinstance(line["choices"], list):
                for choice in line["choices"]:
                    next_csv_line = _next_csv_line()
                    new_text = _merger(
                        choice["text"],
                        next_csv_line["trans"],
                        next_csv_line["text"],
                        is_choice=True,
                    )
                    txt_strings, search_offset = _replace_at_offset(
                        txt_strings,
                        f"text={choice['text']}",
                        f"text={new_text}",
                        search_offset,
                    )
            elif isinstance(line["choices"], dict):
                next_csv_line = _next_csv_line()
                new_text = _merger(
                    line["choices"]["text"],
                    next_csv_line["trans"],
                    next_csv_line["text"],
                    is_choice=True,
                )
                txt_strings, search_offset = _replace_at_offset(
                    txt_strings,
                    f'text={line["choices"]["text"]}',
                    f"text={new_text}",
                    search_offset,
                )

    return txt_strings
