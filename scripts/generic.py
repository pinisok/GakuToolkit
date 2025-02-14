import os, sys
from datetime import datetime
import shutil

import pandas as pd
import openpyxl
import xlsxwriter

from .helper import *
from . import rclone
from .log import *
"""
Converter Helper
"""
serialize_list = [
    ('\r','\\r'),
    ('\r','☢'),       #Only used in generic
    ('\t','\\t'),
]
def Serialize(string:str):
    result = string
    for obj in serialize_list:
        result = result.replace(obj[0], obj[1])
    return result

def Deserialize(string:str):
    result = string
    for obj in serialize_list:
        result = result.replace(obj[1], obj[0])
    return result


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
                LOG_WARN(3, f"{input_path}의 {input_record['A']}의 번역 값이 존재하지 않습니다. 넘어갑니다.")
                continue
            if input_record["B"].startswith("'"):
                data[input_record["A"].replace("\\r\\n","\r\n")] = input_record["B"][1:].replace("\\r\\n","\r\n")
            else:
                data[input_record["A"].replace("\\r\\n","\r\n")] = input_record["B"].replace("\\r\\n","\r\n")
        else:
            if not "text" in input_record_keys or not type(input_record['text']) == str:
                continue
            if not "trans" in input_record_keys or not type(input_record['trans']) == str or input_record['trans'] == "":
                LOG_WARN(3, f"{f}의 {input_record['text']}의 번역 값이 존재하지 않습니다. 넘어갑니다.")
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


GENERIC_REMOTE_PATH = REMOTE_PATH + "/GenericTrans"
GENERIC_DRIVE_PATH = DRIVE_PATH + "/GenericTrans"
GENERIC_TEMP_PATH = TEMP_PATH + "/GenericTrans"
GENERIC_OUTPUT_PATH = OUTPUT_PATH + "/local-files/genericTrans"

GENERIC_FILE_LIST = [
    "/generic.xlsx",
    "/generic.fmt.xlsx",
]

GENERIC_REMOTE_LYRICS_PATH = GENERIC_REMOTE_PATH + "/lyrics"
GENERIC_DRIVE_LYRICS_PATH = GENERIC_DRIVE_PATH + "/lyrics"
GENERIC_OUTPUT_LYRICS_PATH = GENERIC_OUTPUT_PATH + "/lyrics"

# 업데이트 반영
# Gakumas > Google Drive
def UpdateOriginalToDrive(bFullUpdate = False):
    LOG_WARN(2, "Update generic files is not supportted")
    return []


# 번역 수정사항 반영
# Google Drive > GakumasTranslationDataKor
def ConvertDriveToOutput(bFullUpdate=False):
    if bFullUpdate:
        LOG_DEBUG(2, "Try Full Update")
        rclone.copy(GENERIC_REMOTE_LYRICS_PATH, GENERIC_DRIVE_LYRICS_PATH)
        drive_file_paths = Helper_GetFilesFromDir(GENERIC_DRIVE_LYRICS_PATH, ".xlsx")
        for file in GENERIC_FILE_LIST:
            rclone.copy(GENERIC_REMOTE_PATH+file, GENERIC_DRIVE_PATH)
            drive_file_paths += [(GENERIC_DRIVE_PATH+file, file, os.path.basename(file))]
    else:
        LOG_DEBUG(2, "Check updated files")
        check_result = rclone.check(GENERIC_REMOTE_LYRICS_PATH, GENERIC_DRIVE_LYRICS_PATH)
        drive_file_paths = Helper_GetFilesFromDirByCheck(check_result, GENERIC_DRIVE_LYRICS_PATH, ".xlsx")
        rclone.copy(GENERIC_REMOTE_LYRICS_PATH, GENERIC_DRIVE_LYRICS_PATH)
        for file in GENERIC_FILE_LIST:
            check_result = rclone.check(GENERIC_REMOTE_PATH+file, GENERIC_DRIVE_PATH)
            if len(check_result) > 0:
                rclone.copy(GENERIC_REMOTE_PATH+file, GENERIC_DRIVE_PATH)
                drive_file_paths += [(GENERIC_DRIVE_PATH+file, file, os.path.basename(file))]

    if len(drive_file_paths) <= 0:
        LOG_INFO(2, "Generic is not updated, skip")
        return [],[]
    
    converted_file_list = []
    error_file_list = []
    for abs_path, _, filename in drive_file_paths: # Ignore related path because it combined with generic and lyrics
        input_path = abs_path
        rel_path = os.path.relpath(abs_path, GENERIC_DRIVE_PATH)
        output_path = os.path.join(GENERIC_OUTPUT_PATH, rel_path[:-5]+".json")
        LOG_DEBUG(2, f"Start convert from drive to output '{input_path}' to '{output_path}'")
        try:
            XlsxToJson(input_path, output_path)
            converted_file_list.append(filename)
        except Exception as e:
            LOG_ERROR(2, f"Error during Convert generic file from drive to output: {e}")
            logger.exception(e)
            error_file_list.append((filename, e))
    return error_file_list, converted_file_list