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


def preprocess_yaml_content(content: str) -> str:
    """Preprocess YAML content before parsing.

    1. Wrap tab-prefixed values in quotes: `: \\tval` → `: "\\tval"`
    2. Fix literal string newline chomping: `|\\n` → `|+\\n`

    Pure function — no I/O.
    """
    content = re.sub(r': (\t.*)', r': "\1"', content)
    content = content.replace("|\n", "|+\n")
    return content


def _filter_file_list(file_list, exception_list):
    """Filter file list to only include files in exception_list.

    Args:
        file_list: List of (abs_path, rel_path, filename) tuples.
        exception_list: List of file names (without .yaml extension) to keep.
            If None, returns all files unchanged.

    Returns new list (does not mutate input).
    """
    if not exception_list:
        return list(file_list)
    exception_set = set(exception_list)
    return [
        entry for entry in file_list
        if entry[2][:-5] in exception_set
    ]


_save_func = None
_CustomLoader = None


def _convert_single_yaml(file_path, file_name, loader, save_fn):
    """Read, preprocess, parse, and save a single YAML file.

    Args:
        file_path: Absolute path to the YAML file.
        file_name: Original filename (e.g. "Achievement.yaml").
        loader: YAML loader class.
        save_fn: Callable(data, name) to save parsed data.

    Returns True on success, False on error.
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        content = preprocess_yaml_content(content)
        data = yaml.load(content, loader)
        save_fn(data, file_name[:-5])
        return True
    except Exception as e:
        print(f"加载文件 {file_path} 时出错: {e}")
        return False


def convert_yaml_types(obj):
    """Multiprocessing-compatible wrapper for _convert_single_yaml."""
    file_path, file = obj
    _convert_single_yaml(file_path, file, _CustomLoader, _save_func)


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
    all_files = Helper_GetFilesFromDir(folder_path, ".yaml")
    file_list = _filter_file_list(all_files, exception_list)
    file_list_size = len(file_list)
    LOG_INFO(2, f"Converting {file_list_size} MasterDB files from yaml to json")
    pool = multiprocessing.Pool()
    for _ in tqdm.tqdm(pool.imap_unordered(convert_yaml_types, [(abs_path, filename) for abs_path, rel_path, filename in file_list]), total=file_list_size):
        pass
    pool.close()
    pool.join()

    os.chdir(ORIGIN_CWD)
    return [filename[:-5] for _, _, filename in file_list]
