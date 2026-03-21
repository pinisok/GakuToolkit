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
        if not 0 in input_record_keys or not type(input_record[0]) == str:
            continue
        if not "ID" in input_record_keys or not type(input_record['ID']) == str:
            continue
        if not "번역" in input_record_keys or not type(input_record['번역']) == str:
            LOG_WARN(3, f"{input_record['ID']}({input_record[0]})의 번역 값이 존재하지 않습니다. 넘어갑니다.")
            continue
        # 수정해야되는 내용 수정
        if input_record["번역"].startswith("'"):
            data[input_record["ID"]] = Deserialize(input_record["번역"][1:])
        else:
            data[input_record["ID"]] = Deserialize(input_record["번역"])
    os.makedirs(os.path.split(output_path)[0], exist_ok=True)
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, allow_nan=False, indent=4)
"""

Folder Processor

"""


from .paths import (
    LOCALIZATION_FILE, LOCALIZATION_REMOTE_PATH,
    LOCALIZATION_DRIVE_PATH, LOCALIZATION_OUTPUT_PATH,
)


# 업데이트 반영
# Gakumas > Google Drive
def UpdateOriginalToDrive(bFullUpdate = False):
    LOG_WARN(2, "Update generic files is not supportted")
    return []


# 번역 수정사항 반영
# Google Drive > GakumasTranslationDataKor
def ConvertDriveToOutput(drive_file_paths=None, bFullUpdate=False):
    if drive_file_paths is None:
        LOG_DEBUG(2, "No file list provided, scanning local drive")
        if os.path.exists(LOCALIZATION_DRIVE_PATH):
            drive_file_paths = [(LOCALIZATION_DRIVE_PATH, LOCALIZATION_FILE, os.path.basename(LOCALIZATION_DRIVE_PATH))]
        else:
            drive_file_paths = []

    if len(drive_file_paths) <= 0:
        LOG_INFO(2, "Localization file is not updated, skip")
        return [],[]
    
    converted_file_list = []
    error_file_list = []
    
    if len(drive_file_paths) > 0:
        input_path = LOCALIZATION_DRIVE_PATH
        output_path = LOCALIZATION_OUTPUT_PATH
        LOG_DEBUG(2, f"Start convert from drive to output '{input_path}' to '{output_path}'")
        try:
            XlsxToJson(input_path, output_path)
            converted_file_list.append(os.path.basename(LOCALIZATION_DRIVE_PATH))
        except Exception as e:
            LOG_ERROR(2, f"Error during Convert generic file from drive to output: {e}")
            logger.exception(e)
            error_file_list.append((os.path.basename(LOCALIZATION_DRIVE_PATH), e))
    return error_file_list, converted_file_list