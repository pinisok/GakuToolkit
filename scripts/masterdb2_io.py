"""I/O operations for MasterDB: XLSX, JSON, YAML read/write."""

import os
import sys
import re
import shutil
import multiprocessing
from copy import deepcopy

import pandas as pd
import openpyxl
import openpyxl.utils.escape
import xlsxwriter
import tqdm
import yaml

from .helper import *
from .log import *
from . import paths as _paths
from .helper import Deserialize, SERIALIZE_LIST_BASIC
from .masterdb2_record import _Deserialize, DataToRecord


def WriteXlsx(file_name, input_records):
    output_path = os.path.join(_paths.MASTERDB2_DRIVE_PATH, file_name + ".xlsx")
    output_dataframe = pd.DataFrame.from_records(input_records)
    writer = pd.ExcelWriter(output_path, engine="xlsxwriter")
    output_dataframe.replace(r'\r', r'\\r', regex=True, inplace=True)
    output_dataframe.replace(r'\t', r'\\t', regex=True, inplace=True)
    output_dataframe.to_excel(writer, index=False, sheet_name="Sheet1")
    workbook = writer.book
    column_format = workbook.add_format({'font_name': 'Calibri', 'align': 'left'})
    worksheet = writer.sheets['Sheet1']

    key_size = (len(input_records[0].keys()) - 5) // 2
    id_format = workbook.add_format({'font_name': 'Calibri', 'bold': False, 'text_wrap': True, 'align': 'center', 'valign': 'top', 'border': 1, 'num_format': '@'})
    value_format = workbook.add_format({'font_name': 'Calibri', 'bold': False, 'text_wrap': True, 'align': 'left', 'valign': 'top', 'border': 1, 'num_format': '@'})
    worksheet.set_column(0, 0, 10, column_format)  # IMAGE
    for idx in range(1, key_size + 1):
        worksheet.set_column((2 * idx) - 1, (2 * idx) - 1, 10, id_format)
        worksheet.set_column(2 * idx, 2 * idx, 15, value_format)
    worksheet.set_column(2 * key_size + 1, 2 * key_size + 1, 12, id_format)
    worksheet.set_column(2 * key_size + 2, 2 * key_size + 2, 70, value_format)
    worksheet.set_column(2 * key_size + 3, 2 * key_size + 3, 70, value_format)
    worksheet.set_column(2 * key_size + 4, 2 * key_size + 4, 25, value_format)

    writer.close()


def ReadXlsx(file_name) -> list:
    input_path = os.path.join(_paths.MASTERDB2_DRIVE_PATH, file_name + ".xlsx")
    try:
        input_dataframe = pd.read_excel(input_path, na_values="ERROR_NA_VALUE", keep_default_na=False, na_filter=False, engine="openpyxl")
        input_dataframe.fillna("ERROR_NA_VALUE", inplace=True)
        input_dataframe["원문"] = input_dataframe["원문"].apply(openpyxl.utils.escape.unescape)
        input_dataframe["번역"] = input_dataframe["번역"].apply(openpyxl.utils.escape.unescape)
        input_dataframe.replace(r'\\r', r'\r', regex=True, inplace=True)
        input_dataframe.replace(r'\\t', r'\t', regex=True, inplace=True)
        return input_dataframe.astype("string").to_dict(orient="records")
    except Exception as e:
        LOG_WARN(2, f"ReadXlsx:{file_name} / {e}")
        return []


def ReadJson(file_name):
    with open(_paths.MASTERDB_JSON_PATH + "/" + file_name + ".json", encoding="utf-8") as f:
        json_data = json.load(f)
    return json_data


def WriteJson(file_name, obj):
    with open(_paths.MASTERDB_OUTPUT_PATH + "/" + file_name + ".json", "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=4)


def JsonToRecord(file_name) -> list[dict]:
    datas: list[dict] = ReadJson(file_name)["data"]
    results = []
    for idx, data in enumerate(datas):
        result = DataToRecord(file_name, deepcopy(data))
        results += result
    return results


def LoadOldKV(file_name: str) -> dict:
    input_path = os.path.join(_paths.MASTERDB_DRIVE_PATH, file_name + ".xlsx")
    input_dataframe = pd.read_excel(input_path, na_values="", keep_default_na=False, na_filter=False, engine="openpyxl")
    input_dataframe.fillna("", inplace=True)
    input_records = input_dataframe.to_dict(orient="records")
    data = {}
    for input_record in input_records:
        input_record_keys = input_record.keys()
        if "text" not in input_record_keys or not type(input_record['text']) == str:
            continue
        if ("trans" not in input_record_keys or not type(input_record['trans']) == str or
            (input_record['text'] != "" and input_record['trans'] == "")) \
                or input_record['text'] == input_record['trans']:
            continue
        if input_record["trans"].startswith("'"):
            data[_Deserialize(input_record["text"])] = _Deserialize(input_record["trans"][1:])
        else:
            data[_Deserialize(input_record["text"])] = _Deserialize(input_record["trans"])
    return data


# YAML conversion

_save_func = None
_CustomLoader = None


def convert_yaml_types(obj):
    file_path, file = obj
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        content = re.sub(r': (\t.*)', r': "\1"', content)
        content = content.replace("|\n", "|+\n")
        data = yaml.load(content, _CustomLoader)
        _save_func(data, file[:-5])
    except Exception as e:
        print(f"加载文件 {file_path} 时出错: {e}")


def convert_yaml_types_in_parallel(exception_list=None):
    if _paths.GIT_MASTERDB_PATH + "/scripts" not in sys.path:
        sys.path.append(_paths.GIT_MASTERDB_PATH + "/scripts")
    ORIGIN_CWD = os.getcwd()
    os.chdir(_paths.GIT_MASTERDB_PATH)

    if os.path.exists(_paths.MASTERDB_JSON_PATH):
        shutil.rmtree(_paths.MASTERDB_JSON_PATH, True)
    global _save_func
    global _CustomLoader
    from gakumasu_diff_to_json import yaml, save_json, CustomLoader
    _save_func = save_json
    _CustomLoader = CustomLoader
    folder_path = "./gakumasu-diff/orig"
    if not os.path.isdir(folder_path):
        raise FileNotFoundError(f"Folder {folder_path} is not exists")
    file_list = Helper_GetFilesFromDir(folder_path, ".yaml")
    for n, obj in reversed(list(enumerate(file_list))):
        if exception_list:
            if obj[2][:-5] not in exception_list:
                file_list.pop(n)
    file_list_size = len(file_list)
    LOG_INFO(2, f"Converting {file_list_size} MasterDB files from yaml to json")
    pool = multiprocessing.Pool()
    for _ in tqdm.tqdm(pool.imap_unordered(convert_yaml_types, [(abs_path, filename) for abs_path, rel_path, filename in file_list]), total=file_list_size):
        pass
    pool.close()
    pool.join()

    os.chdir(ORIGIN_CWD)
    return [filename[:-5] for _, _, filename in file_list]
