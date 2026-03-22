import os, re, csv, difflib
from typing import Callable, Optional, Union
from io import StringIO
from datetime import datetime

import pandas as pd
from imas_tools.story.gakuen_parser import parse_messages as _externalParser
from imas_tools.story.story_csv import StoryCsv as _externalStoryCsv
import openpyxl
import xlsxwriter

from .helper import *
from .log import *

def _internalOverrideXlsxColumn(dataframe):
    #override xlsx's column name
    dataframe.rename(columns={
            dataframe.columns[0] : "id",
            dataframe.columns[1] : "name",
            dataframe.columns[2] : "translated name",
            dataframe.columns[3] : "text",
            dataframe.columns[4] : "translated text",
        }, inplace= True)

def _internalReadXlsx(fp):
    LOG_DEBUG(4, f"Open {fp.name} by openpyxl from pandas")
    dataframe = pd.read_excel(fp, na_values="", keep_default_na=False, na_filter=False, engine="openpyxl")
    _internalOverrideXlsxColumn(dataframe)
    return dataframe

"""

XlsxToCsv Helpers

"""

def _encode(string : str):
    string = string.replace("\n","\\n").replace("\r","\\r").replace("~","～")
    string = REGEX_DOTS_4_TO_6.sub('……', string)
    string = REGEX_DOTS_3.sub('…', string)
    return string

def _internalXlsxDataFrameProcess(dataframe:pd.DataFrame, origin_path:str):
    #Remove comments
    if len(dataframe.columns) > 5: dataframe.drop(dataframe.columns[5:], axis=1,inplace=True)
    dataframe.loc[len(dataframe)] = {"id":"info", "name" : origin_path}
    dataframe.loc[len(dataframe)] = {"id":"译者", "name" : "None"}

START_EM_LENGTH = 4
END_EM_LENGTH = 5
def _processEMtag(string: str):
    if len(string) < 1:
        return string
    start_idx = string.find("<em>")
    if start_idx == -1:
        return string
    end_idx = string[start_idx:].find("</em>")
    if end_idx == -1:
        return string
    result = string[start_idx+START_EM_LENGTH:start_idx+end_idx].replace(" ", "</em> <em>")
    return string[:start_idx] + "<em>" + result + "</em>" + _processEMtag(string[start_idx+end_idx+END_EM_LENGTH:])

def _internalXlsxRecordsProcess(records : list[dict]) -> list[dict]:
    """Process xlsx records: normalize types, translate names, encode text.
    Returns a new list of processed records (does not mutate input)."""
    processed = []
    for idx, record in enumerate(records):
        r = {**record}
        # id
        if not isinstance(r["id"], str):
            if isinstance(r["id"], int):   r["id"] = str(r["id"])
            else:                          r["id"] = ""
        # name
        if not isinstance(r["name"], str): r["name"] = ""
        else:                              r["name"] = str(r["name"])
        if "translated name" in r.keys() and \
                isinstance(r["translated name"], str) and \
                len(r["translated name"].strip()) > 0:
            r["name"] = r["translated name"]
        else:
            r["name"] = CHARACTER_REGEX_TRANS_MAP.get(r["name"], r["name"])

        # text
        if not isinstance(r["text"], str):
            r["text"] = ""

        if not isinstance(r["translated text"], str):
            if isinstance(r["translated text"], (int, float)):
                r["translated text"] = str(r["translated text"])
            else:
                r["translated text"] = ""

        r["text"] = _encode(r["text"])
        r["translated text"] = _encode(r["translated text"])
        r["translated text"] = _processEMtag(r["translated text"])
        if len(r["translated text"]) < 1 and r["id"] != "译者" and r["id"] != "info":
            raise Exception(f"Adv 파일 {idx}번째 줄 번역문 '{r['text']}'이 빈 줄 입니다. 해당 파일을 스킵합니다")
        processed.append(r)
    return processed

def _internalCsvWriter(fp, records):
    writer = csv.DictWriter(fp, fieldnames=["id","name","text","trans"], lineterminator='\n')
    writer.writerow({"id":"id","name":"name","text":"text","trans":"trans"})
    for record in records:
        writer.writerow({"id":record["id"],"name":record["name"],"text":record["text"],"trans":record["translated text"]})


"""
Helper for TxtToXlsx

"""

def _internalTxtToScv(read_fp, file_name):
    txt = _externalParser(read_fp.read())
    story_csv = _externalStoryCsv.new_empty_csv(file_name)
    for line in txt:
        if (line["__tag__"] == "message" or line["__tag__"] == "narration") and line.get("text"):
            story_csv.append_line(
                {
                    "id": "0000000000000",
                    "name": line.get("name", "__narration__"),
                    "text": line["text"],
                    "trans": "",
                }
            )
        if line["__tag__"] == "title" and line.get("title"):
            story_csv.append_line({"id": "0000000000000", "name": "__title__", "text": line["title"], "trans": ""})
        if line["__tag__"] == "choicegroup":
            if isinstance(line["choices"], list):
                for choice in line["choices"]:
                    story_csv.append_line({"id": "select", "name": "", "text": choice["text"], "trans": ""})
            elif isinstance(line["choices"], dict):
                story_csv.append_line({"id": "select", "name": "", "text": line["choices"]["text"], "trans": ""})
            else:
                raise ValueError(f"Unknown choice type: {line['choices']}")
    return StringIO(str(story_csv))

def _internalCsvToDataFrame(read_fp):
    adv_dataframe = pd.read_csv(read_fp, index_col=False)
    adv_dataframe.insert(loc=2, column='translated name', value="")
    adv_dataframe.drop(adv_dataframe.tail(2).index, inplace=True)
    _internalOverrideXlsxColumn(adv_dataframe)
    if len(adv_dataframe.index) < 1:
        raise ValueError("No message")
    adv_dataframe['text'] = adv_dataframe['text'].str.replace("\\n", "\n")
    return adv_dataframe

def _internalUpdateDataFrame(new_dataframe:pd.DataFrame, original_fp, file_name=""):
    """Merge new source DataFrame with existing translated xlsx.
    Returns (DataFrame, warnings_list)."""
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
                new_records[idx]["comments"] = "원본 문자열이 수정되었습니다. 번역값이 적절한지 확인 후 해당 문구를 삭제해주세요"
        return pd.DataFrame.from_dict(new_records), warnings
    return pd.DataFrame.from_dict(orig_records), warnings

def _internalDataFrameToXlsx(dataframe, write_fp):
    writer = pd.ExcelWriter(write_fp, engine="xlsxwriter") 
    dataframe.to_excel(writer, index=False, sheet_name="Sheet1")

    workbook = writer.book
    worksheet = writer.sheets['Sheet1']
    name_format = workbook.add_format(XLSX_NAME_FORMAT)
    worksheet.set_column(0, 2, 15, name_format)
    text_format = workbook.add_format(XLSX_TEXT_FORMAT)
    worksheet.set_column(3, 4, 70, text_format)

    writer.close()

def _replace_at_offset(text, old, new, offset):
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


def _internalCsvToTxt(csv_strings, txt_strings):
    def _merger(original_text: str,
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
                f"Original text does not match validation text: '{validation_original_text}' != '{original_text}'"
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
                # name= appears after text= on the same tag line
                if line["__tag__"] == "message":
                    if line.get("name") and line["name"] != "" and next_csv_line["name"] != "":
                        # Find the end of current tag line to limit search scope
                        line_end = txt_strings.find("]", search_offset)
                        if line_end == -1:
                            line_end = len(txt_strings)
                        name_old = f"name={line['name']}"
                        name_new = f"name={next_csv_line['name']}"
                        name_idx = txt_strings.find(name_old, search_offset, line_end + 1)
                        if name_idx != -1:
                            len_diff = len(name_new) - len(name_old)
                            txt_strings = txt_strings[:name_idx] + name_new + txt_strings[name_idx + len(name_old):]
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

"""
File Converter

"""

# 번역 > 데이터 변환
def XlsxToCsv(read_fp, write_fp, origin_path:str):
    xlsx_dataframe = _internalReadXlsx(read_fp)

    _internalXlsxDataFrameProcess(xlsx_dataframe, origin_path)
    
    xlsx_records = xlsx_dataframe.to_dict(orient="records")
    
    xlsx_records = _internalXlsxRecordsProcess(xlsx_records)
    _internalCsvWriter(write_fp, xlsx_records)

def CsvToTxt(read_fp, write_path, original_path): 
    csv_strings = "".join(read_fp.readlines())
    with open(original_path, "r", encoding='utf-8') as write_fp:
        txt_strings = "".join(write_fp.readlines())
    txt_strings = _internalCsvToTxt(csv_strings, txt_strings)
    with open(write_path, "w", encoding="utf-8") as write_fp:
        write_fp.write(txt_strings)

def XlsxToTxt_parallels(obj):
    input_path, filename = obj
    output_path = os.path.join(ADV_OUTPUT_PATH, filename[:-5]+".txt")
    original_path = os.path.join(ADV_ORIGINAL_PATH, filename[:-5]+".txt")
    converted_file_list = []
    error_file_list = []
    try:
        XlsxToTxt(input_path, output_path, original_path)
        converted_file_list.append(filename)
    except Exception as e:
        LOG_ERROR(2, f"Error converting {filename}: {e}")
        error_file_list.append((e, filename))
    return error_file_list, converted_file_list

def XlsxToTxt(input_path, write_path, original_path):
    with open(input_path, "rb") as input_fp:
        csvIO = StringIO()
        XlsxToCsv(input_fp, csvIO, os.path.basename(write_path))
    csvIO.seek(0)
    CsvToTxt(csvIO, write_path, original_path)

# 원본 > 번역 변환

def TxtToXlsx_parallels(obj):
    input_path, output_path, filename = obj
    try:
        warnings = TxtToXlsx(input_path, output_path, filename)
        return {filename: warnings} if warnings else {}
    except Exception as e:
        LOG_ERROR(2, f"Error: {e}")
        logger.exception(e)
        return {}


def TxtToXlsx(input_path, output_path, file_name:str):
    """Convert TXT to XLSX. Returns list of warnings (may be empty)."""
    with open(input_path, "r", encoding="utf-8") as input_fp:
        csv = _internalTxtToScv(input_fp, file_name)
    dataframe = _internalCsvToDataFrame(csv)
    warnings = []

    if(os.path.exists(output_path)):
        original_fp = open(output_path, "rb")
        LOG_DEBUG(4, f"Try to update original file")
        dataframe, warnings = _internalUpdateDataFrame(dataframe, original_fp, file_name)
        original_fp.close()

    LOG_DEBUG(4, f"Write result to file")
    write_fp = open(output_path, "wb")
    _internalDataFrameToXlsx(dataframe, write_fp)
    write_fp.close()
    return warnings


"""

Folder Processor Helper

"""
ADV_BLACKLIST_FILE = [
    "musics.txt",
    "adv_warmup.txt",
]
ADV_BLACKLIST_FOLDER = [
    "pstep",
    "pweek",
]

def _internalGetOutputPath(filename:str):
    splitted_name = filename[4:-4].split("_")
    folder_name = splitted_name[0]
    if splitted_name[0] == "pstory":
        folder_name += "_" + splitted_name[1]
    folder_name = folder_name.split("-")[0]
    return folder_name


"""

Folder Processor

"""


from .paths import (
    GIT_ADV_PATH, ADV_ORIGINAL_PATH, ADV_REMOTE_PATH, ADV_DRIVE_PATH,
    ADV_TEMP_PATH, ADV_OUTPUT_PATH, ADV_CACHE_FILE,
)
from .helper import load_cache_date, save_cache_date


def _filter_adv_files(file_paths):
    """Apply blacklist and map to (input_path, output_xlsx_path, filename)."""
    file_list = []
    for abs_path, rel_path, filename in file_paths:
        if filename in ADV_BLACKLIST_FILE:
            continue
        foldername = _internalGetOutputPath(filename)
        if foldername in ADV_BLACKLIST_FOLDER:
            continue
        input_path = rel_path
        output_path = os.path.join(ADV_DRIVE_PATH, foldername, filename[:-4] + ".xlsx")
        file_list.append((input_path, output_path, filename))
    return file_list


def _convert_xlsx_to_txt_batch(drive_file_paths):
    """Run XlsxToTxt in parallel via multiprocessing Pool.
    Returns (error_file_list, converted_file_list)."""
    from .parallel import run_parallel, collect_errors_and_successes

    results = run_parallel(
        XlsxToTxt_parallels,
        [(abs_path, filename) for abs_path, rel_path, filename in drive_file_paths],
        desc="XLSX→TXT",
    )
    return collect_errors_and_successes(results)


# 업데이트 반영
# Campus-Adv-txts > Google Drive
def UpdateOriginalToDrive():
    last_update_date = load_cache_date(ADV_CACHE_FILE)
    if last_update_date:
        LOG_DEBUG(2, f"Load update date {last_update_date}")
    save_cache_date(ADV_CACHE_FILE)

    if last_update_date is not None:
        LOG_DEBUG(2, "Check git diff")
        original_file_paths = Helper_GetFilesFromDirByDate(last_update_date, GIT_ADV_PATH, ".txt", "adv_")
    else:
        original_file_paths = []
    if len(original_file_paths) <= 0:
        LOG_INFO(2, "ADV is not updated, skip")
        return [], {}

    file_list = _filter_adv_files(original_file_paths)
    LOG_INFO(2, f"Updating {len(file_list)} adv files")
    from .parallel import run_parallel, collect_dict_results

    results = run_parallel(TxtToXlsx_parallels, file_list, desc="TXT→XLSX")
    all_warnings = collect_dict_results(results)

    return file_list, all_warnings


# 번역 수정사항 반영
# Google Drive > GakumasTranslationDataKor
def ConvertDriveToOutput(drive_file_paths=None, bFullUpdate=False):
    if drive_file_paths is None:
        LOG_DEBUG(2, "No file list provided, scanning local drive")
        drive_file_paths = Helper_GetFilesFromDir(ADV_DRIVE_PATH, ".xlsx", "adv_")
    if len(drive_file_paths) <= 0:
        LOG_INFO(2, "ADV is not updated, skip")
        return [], []
    LOG_INFO(2, f"Converting {len(drive_file_paths)} adv files")

    return _convert_xlsx_to_txt_batch(drive_file_paths)