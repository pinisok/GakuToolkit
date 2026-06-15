import os, sys
from datetime import datetime
import shutil

import pandas as pd
import openpyxl
import xlsxwriter

from .helper import *
from .helper import Serialize, Deserialize
from .log import *


"""
Converter

"""

def XlsxToJson(input_path, output_path):
    input_dataframe = pd.read_excel(input_path, na_values="", keep_default_na=False, na_filter=False, engine="openpyxl")
    input_dataframe = input_dataframe.convert_dtypes()
    input_dataframe.fillna("", inplace=True)
    input_records = input_dataframe.to_dict(orient="records")
    data = {}
    for input_record in input_records:
        input_record_keys = input_record.keys()
        # 수정해야되는 내용 수정
        if "/lyrics/" in input_path:
            if not "A" in input_record_keys or not type(input_record['A']) == str:
                continue
            if not "B" in input_record_keys or not type(input_record['B']) == str or input_record['B'] == "":
                LOG_DEBUG(3, f"{input_path}의 {input_record['A']}의 번역 값이 존재하지 않습니다. 넘어갑니다.")
                continue
            if input_record["B"].startswith("'"):
                data[input_record["A"].replace("\\r\\n","\r\n")] = input_record["B"][1:].replace("\\r\\n","\r\n")
            else:
                data[input_record["A"].replace("\\r\\n","\r\n")] = input_record["B"].replace("\\r\\n","\r\n")
        else:
            if not "text" in input_record_keys or not type(input_record['text']) == str:
                continue
            if not "trans" in input_record_keys or not type(input_record['trans']) == str or input_record['trans'] == "":
                LOG_DEBUG(3, f"{input_record}의 {input_record['text']}의 번역 값이 존재하지 않습니다. 넘어갑니다.")
                continue
            if input_record["trans"].startswith("'"):
                data[Deserialize(input_record["text"])] = Deserialize(input_record["trans"][1:])
            else:
                data[Deserialize(input_record["text"])] = Deserialize(input_record["trans"])
            
    os.makedirs(os.path.split(output_path)[0], exist_ok=True)
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, allow_nan=False, indent=4)
"""

Folder Processor

"""


from .paths import (
    GENERIC_REMOTE_PATH, GENERIC_DRIVE_PATH, GENERIC_TEMP_PATH,
    GENERIC_OUTPUT_PATH, GENERIC_FILE_LIST,
    GENERIC_REMOTE_LYRICS_PATH, GENERIC_DRIVE_LYRICS_PATH, GENERIC_OUTPUT_LYRICS_PATH,
)

# 업데이트 반영
# Gakumas > Google Drive
def UpdateOriginalToDrive(bFullUpdate = False):
    LOG_WARN(2, "Update generic files is not supportted")
    return []


def _generic_output_for(tpl):
    """xlsx tuple → expected output .json path. Used by stale-detection."""
    abs_path, _rel, _filename = tpl
    if not abs_path.endswith(".xlsx"):
        return None
    rel_to_drive = os.path.relpath(abs_path, GENERIC_DRIVE_PATH)
    return os.path.join(GENERIC_OUTPUT_PATH, rel_to_drive[:-5] + ".json")


def _generic_scan_all_local():
    """Walk lyrics dir + GENERIC_FILE_LIST singletons. Same coverage the
    original 'no file list provided' branch produced."""
    files = list(Helper_GetFilesFromDir(GENERIC_DRIVE_LYRICS_PATH, ".xlsx"))
    for f in GENERIC_FILE_LIST:
        abs_p = GENERIC_DRIVE_PATH + f
        if os.path.exists(abs_p):
            files.append((abs_p, f, os.path.basename(f)))
    return files


# 번역 수정사항 반영
# Google Drive > GakumasTranslationDataKor
def ConvertDriveToOutput(drive_file_paths=None, bFullUpdate=False):
    """Triggers (union):
      * Explicit `drive_file_paths` from Phase 0 (rclone diff).
      * Local mtime scan — any xlsx whose .json output is missing or older.
    """
    all_local = _generic_scan_all_local()
    stale = Helper_FilterStaleByOutput(all_local, _generic_output_for)

    if drive_file_paths is None:
        LOG_DEBUG(2, "No file list provided, using local stale-mtime scan")
        to_convert = stale
    else:
        seen, to_convert = set(), []
        for tpl in list(drive_file_paths) + stale:
            if tpl[0] in seen:
                continue
            seen.add(tpl[0])
            to_convert.append(tpl)

    if not to_convert:
        LOG_INFO(2, "Generic is up-to-date, skip")
        return [], []

    LOG_INFO(
        2,
        f"Converting {len(to_convert)} generic files "
        f"(explicit={len(drive_file_paths or [])}, stale-by-mtime={len(stale)})",
    )

    converted_file_list = []
    error_file_list = []
    for abs_path, _, filename in to_convert:  # rel_path is mixed (generic + lyrics) so we use abs_path
        input_path = abs_path
        rel_path = os.path.relpath(abs_path, GENERIC_DRIVE_PATH)
        output_path = os.path.join(GENERIC_OUTPUT_PATH, rel_path[:-5] + ".json")
        LOG_DEBUG(2, f"Start convert from drive to output '{input_path}' to '{output_path}'")
        try:
            XlsxToJson(input_path, output_path)
            converted_file_list.append(filename)
        except Exception as e:
            LOG_ERROR(2, f"Error during Convert generic file from drive to output: {e}")
            logger.exception(e)
            error_file_list.append((filename, e))
    return error_file_list, converted_file_list