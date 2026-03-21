import openpyxl.utils
import openpyxl.utils.escape
import os, sys, shutil, multiprocessing, yaml
from datetime import datetime

import pandas as pd, openpyxl, xlsxwriter, tqdm

import string

from .helper import *
from .log import *
from copy import deepcopy
import shelve
import re

_db_handle = None
_db_depth = 0


class db_session:
    """Context manager for batched shelve access.

    Opens the DB once and reuses the handle for all DB_save/DB_get calls
    within the block. Supports nesting — inner sessions reuse the outer handle.

    Usage:
        with db_session():
            DB_save("key", "value")
            val = DB_get("key")
    """

    def __enter__(self):
        global _db_handle, _db_depth
        if _db_handle is None:
            _db_handle = shelve.open('DB.dat')
        _db_depth += 1
        return _db_handle

    def __exit__(self, *exc):
        global _db_handle, _db_depth
        _db_depth -= 1
        if _db_depth <= 0 and _db_handle is not None:
            _db_handle.close()
            _db_handle = None
            _db_depth = 0


def DB_save(key, value):
    global _db_handle
    if _db_handle is not None:
        _db_handle[key] = value
    else:
        with shelve.open('DB.dat') as d:
            d[key] = value


def DB_get(key, default=""):
    global _db_handle
    if _db_handle is not None:
        return _db_handle.get(key, default)
    else:
        with shelve.open('DB.dat') as d:
            return d.get(key, default)



def path_normalize_for_pk(path_str: str) -> str:
    return re.sub(r"\[\d+\]", "", path_str)

def check_need_export(v: str) -> bool:
    if not v:
        return False
    allowed_chars = string.ascii_letters + string.digits + string.punctuation + " "
    for char in v:
        if char not in allowed_chars:
            return True
    return False

from .masterdb2_rules import (
    rule_key_translate_map,
    rule_key_reverse_translate_map,
    TranslateRuleKey,
    TranslateReverseRuleKey,
)


"""
Converter Helper
"""
from .helper import Deserialize, SERIALIZE_LIST_BASIC

def _Deserialize(string: str) -> str:
    return Deserialize(string, rules=SERIALIZE_LIST_BASIC)

# Extend of gakumasu_diff_to_json.py at gakumas-master-translation
def convert_yaml_types(obj):
    file_path, file = obj
    try:
        # 预处理文件：替换制表符为 4 个空格
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        # content = content.replace('\t', '    ')  # 替换制表符
        # content = content.replace(": \t", ": \"\t\"")  # 替换制表符
        content = re.sub(r': (\t.*)', r': "\1"', content)
        content = content.replace("|\n", "|+\n") # Fix literal strings newline chomping

        # 解析 YAML 内容
        # data = yaml.safe_load(content)
        data = yaml.load(content, _CustomLoader)
        _save_func(data, file[:-5])
        # print(f"文件: {file_path}")
        # print(f"类型: {type(data)}\n")
    except Exception as e:
        print(f"加载文件 {file_path} 时出错: {e}")

def convert_yaml_types_in_parallel(exception_list = None):
    if GIT_MASTERDB_PATH+"/scripts" not in sys.path:
        sys.path.append(GIT_MASTERDB_PATH+"/scripts")
    ORIGIN_CWD = os.getcwd()
    os.chdir(GIT_MASTERDB_PATH)

    if os.path.exists(MASTERDB_JSON_PATH):
        shutil.rmtree(MASTERDB_JSON_PATH, True)
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

def WriteXlsx(file_name, input_records):
    output_path = os.path.join(MASTERDB2_DRIVE_PATH, file_name+".xlsx")
    output_dataframe = pd.DataFrame.from_records(input_records)
    writer = pd.ExcelWriter(output_path, engine="xlsxwriter") 
    output_dataframe.replace(r'\r', r'\\r', regex=True, inplace=True)
    output_dataframe.replace(r'\t', r'\\t', regex=True, inplace=True)
    output_dataframe.to_excel(writer, index=False, sheet_name="Sheet1")
    workbook = writer.book
    column_format = workbook.add_format({'font_name': 'Calibri', 'align':'left'})
    worksheet = writer.sheets['Sheet1']

    key_size = (len(input_records[0].keys()) - 5) // 2
    id_format = workbook.add_format({'font_name': 'Calibri', 'bold':False, 'text_wrap':True, 'align':'center', 'valign':'top', 'border':1, 'num_format': '@'})
    value_format = workbook.add_format({'font_name': 'Calibri', 'bold':False, 'text_wrap':True, 'align':'left', 'valign':'top', 'border':1, 'num_format': '@'})
    worksheet.set_column(0 ,0, 10, column_format) # IMAGE
    for idx in range(1, key_size+1):
        worksheet.set_column((2 * idx) - 1, (2 * idx) - 1,  10, id_format) # KEY ID #
        worksheet.set_column(2 * idx,       2 * idx,        15, value_format) # KEY VALUE #
    worksheet.set_column(2*key_size + 1,         2*key_size + 1,      12, id_format) # ID
    worksheet.set_column(2*key_size + 2,         2*key_size + 2,      70, value_format) # 원문
    worksheet.set_column(2*key_size + 3,         2*key_size + 3,      70, value_format) # 번역
    worksheet.set_column(2*key_size + 4,         2*key_size + 4,      25, value_format) # 설명
 
    writer.close()

def ReadXlsx(file_name) -> list:
    input_path = os.path.join(MASTERDB2_DRIVE_PATH, file_name+".xlsx")
    try:
        input_dataframe = pd.read_excel(input_path, na_values="ERROR_NA_VALUE", keep_default_na=False, na_filter=False, engine="openpyxl")
        input_dataframe.fillna("ERROR_NA_VALUE", inplace=True)
        # Check Rules _X00B_
        input_dataframe["원문"] = input_dataframe["원문"].apply(openpyxl.utils.escape.unescape)
        input_dataframe["번역"] = input_dataframe["번역"].apply(openpyxl.utils.escape.unescape)
        input_dataframe.replace(r'\\r', r'\r', regex=True, inplace=True)
        input_dataframe.replace(r'\\t', r'\t', regex=True, inplace=True)

        return input_dataframe.astype("string").to_dict(orient="records")
    except Exception as e:
        LOG_WARN(2, f"ReadXlsx:{file_name} / {e}")
        return []

RULES = None
def LoadRules():
    global RULES
    # Load key rules from gkms-masterdb repo
    if GIT_MASTERDB_PATH+"/scripts" not in sys.path:
        sys.path.append(GIT_MASTERDB_PATH+"/scripts")
    ORIGIN_CWD = os.getcwd()
    os.chdir(GIT_MASTERDB_PATH)
    from gakumasu_diff_to_json import primary_key_rules
    os.chdir(ORIGIN_CWD)
    RULES = primary_key_rules
def GetRule(file_name:str) -> dict:
    if RULES == None:
        LoadRules()
    if file_name in RULES:
        return dict(RULES)[file_name]
    else:
        return [[],[]]
    
def GetRecordStructure(file_name:str) -> dict:
    rules = GetRule(file_name)
    record_structure = {}
    primary_key_idx = 0
    record_structure["IMAGE"] = ""
    for key in rules[0]:
        if key.find(".") == -1:
            record_structure[f"KEY ID {primary_key_idx}"] = key
            record_structure[f"KEY VALUE {primary_key_idx}"] = ""
        primary_key_idx += 1
    record_structure["ID"] = ""
    record_structure["원문"] = ""
    record_structure["번역"] = ""
    record_structure["설명"] = ""
    return record_structure

def DataToRecord(file_name, data):
    pk_set = set(GetRule(file_name)[0])
    record_structure:dict = GetRecordStructure(file_name)
    key_size = (len(record_structure.keys()) - 4) // 2

    for key,value in record_structure.items():
        for idx in range(key_size):
            if f"KEY ID {idx}" != key:
                continue
            data_key = record_structure.get(f"KEY ID {idx}")
            if isinstance(data[data_key], (int, float)):
                record_structure[f"KEY VALUE {idx}"] = str(data[data_key])
            else:
                record_structure[f"KEY VALUE {idx}"] = data[data_key]
    result = []
    def traverse(structure, obj, prefix=""):
        if isinstance(obj, dict):
            for k, v in obj.items():
                new_prefix = prefix + "." + k if prefix else k
                if isinstance(v, str):
                    if not check_need_export(v):
                        continue
                    if path_normalize_for_pk(new_prefix) not in pk_set:
                        structure["ID"] = new_prefix
                        structure["원문"] = v
                        result.append(deepcopy(structure))
                elif isinstance(v, list):
                    if (len(v) > 0) and (not isinstance(v[0], str)):
                        traverse(structure, v, new_prefix)
                    else:
                        new_v = "[LA_N_F]".join(v)
                        new_v = f"[LA_F]{new_v}"
                        if not check_need_export(new_v):
                            continue
                        if path_normalize_for_pk(new_prefix) not in pk_set:
                            structure["ID"] = new_prefix
                            structure["원문"] = new_v
                            result.append(deepcopy(structure))
                elif isinstance(v, dict):
                    traverse(structure, v, new_prefix)
        elif isinstance(obj, list):
            for idx, item in enumerate(obj):
                new_prefix = prefix + f"[{idx}]"
                if isinstance(item, (dict, list)):
                    traverse(structure, item, new_prefix)
    traverse(deepcopy(record_structure), data)
    for record in result:
        if record["ID"] == "":
            LOG_ERROR(2, f"Empty key ID during process {data}")
            raise ValueError(f"Empty key during process")
        if record["원문"] == "":
            LOG_ERROR(2, f"Empty key 원문 during process {data}")
            raise ValueError(f"Empty key during process")
    return result

def ReadJson(file_name):
    with open(MASTERDB_JSON_PATH+"/"+file_name+".json", encoding="utf-8") as f:
        json_data = json.load(f)
    return json_data

def WriteJson(file_name, obj):
    with open(MASTERDB_OUTPUT_PATH+"/"+file_name+".json", "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=4)
    return

def JsonToRecord(file_name) -> list[dict]:
    datas:list[dict] = ReadJson(file_name)["data"]
    results = []
    for idx, data in enumerate(datas):
        result = DataToRecord(file_name, deepcopy(data))
        results += result
    return results


PATTERN = re.compile(r"\[(\d+)\]")
def OverrideRecordToJson(json_data:dict, records: list[dict]) -> dict:
    with db_session():
        return _OverrideRecordToJson(json_data, records)

def _OverrideRecordToJson(json_data:dict, records: list[dict]) -> dict:
    data_list = json_data["data"]
    def _ExtractKeyTuple(record: dict):
        keys = {}
        idx = 0
        while f"KEY ID {idx}" in record:
            key = record[f"KEY ID {idx}"]
            value = record[f"KEY VALUE {idx}"]
            keys[key] = value
            idx += 1
        return keys, record["ID"]
    def _TestKey(primaryKV, data:dict) -> bool:
        for k,v in primaryKV.items():
            if k in data and str(data.get(k, None)) != v:
                return False
        return True
    translated_result = []
    for ridx, record in enumerate(records):
        primaryKey, subKey = _ExtractKeyTuple(record)
        if subKey == "":
            LOG_WARN(2, f"Find empty sub key(ID) for {record}")
            continue
        if record["번역"] == "":
            LOG_WARN(2, f"Skip empty translated value for {record}")
            continue
        find = []
        for didx, data in enumerate(data_list):
            if not _TestKey(primaryKey, data):
                continue
            find.append(didx)
        if len(find) < 1:
            LOG_WARN(2, f"Failed to find matching primary key for {record}")
            continue
        # Apply transalte
        def traverse(obj, key):
            subkey_list = key.split(".",1)
            subkey = subkey_list[0]
            match_result = PATTERN.search(subkey)
            try:
                # For array XXXX[5]
                if isinstance(match_result, re.Match):
                    idx = int(match_result.groups()[0])
                    subkey_name = PATTERN.sub("", subkey)
                    subobj = obj[subkey_name]
                    if isinstance(subobj[idx], dict):
                        return traverse(subobj[idx], subkey_list[1])
                    elif isinstance(subobj[idx], str):
                        if subobj[idx] != record["원문"]:
                            # LOG_WARN(2, f"Original text is not matched '{subobj[idx]}' != '{record['원문']}'")
                            return False
                        DB_save(subobj[idx], record["번역"])
                        subobj[idx] = record["번역"]
                        return True
                    elif isinstance(subobj[idx], list):
                        if (len(subobj) > 0) and (not isinstance(subobj[0], str)):
                            return traverse(subobj, subkey_list[1])
                        else:
                            trans_str:str = record["번역"]
                            if trans_str.startswith("[LA_F]"):
                                original_text = f"[LA_F]{'[LA_N_F]'.join(subobj[idx])}"
                                if original_text != record["원문"]:
                                    # LOG_WARN(2, f"Original text is not matched '{original_text}' != '{record['원문']}'")
                                    return False
                                DB_save(subobj[idx], record["번역"])
                                subobj[idx] = trans_str[len("[LA_F]"):].split("[LA_N_F]")
                                return True
                            else:
                                LOG_WARN(2, f"Except list type but get invalid translate value \"{trans_str}\" at \"{record}\" / data:[{didx}/{data_list[didx]}]")
                    else:
                        LOG_WARN(2, f"Failed to match sub key {subKey} for {record} / data:[{didx}/{data_list[didx]}]")
                        return False
                # XXXX
                else:
                    if isinstance(obj[subkey], dict):
                        if len(subkey_list) < 2:
                            LOG_WARN(2, f"Subkey {subKey} is short for object {record} / data:[{didx}/{data_list[didx]}]")
                        return traverse(obj[subkey], subkey_list[1])
                    elif isinstance(obj[subkey], str):
                        if obj[subkey] != record["원문"]:
                            # LOG_WARN(2, f"Original text is not matched '{obj[subkey]}' != '{record['원문']}'")
                            return False
                        DB_save(obj[subkey], record["번역"])
                        obj[subkey] = record["번역"]
                        return True
                    elif isinstance(obj[subkey], list):
                        if (len(obj[subkey]) > 0) and (not isinstance(obj[subkey][0], str)):
                            return traverse(obj, subkey_list[1])
                        else:
                            trans_str:str = record["번역"]
                            if trans_str.startswith("[LA_F]"):
                                original_text = f"[LA_F]{'[LA_N_F]'.join(obj[subkey])}"
                                if original_text != record["원문"]:
                                    # LOG_WARN(2, f"Original text is not matched '{original_text}' != '{record['원문']}'")
                                    return False
                                DB_save(original_text, record["번역"])
                                obj[subkey] = trans_str[len("[LA_F]"):].split("[LA_N_F]")
                                return True
                            else:
                                LOG_WARN(2, f"Except list but meet invalid translate value \"{trans_str}\" at \"{record}\" / data:[{didx}/{data_list[didx]}]")
            except Exception as e:
                LOG_WARN(2, f"Failed to match sub key {subKey} for {record} / data:[{didx}/{data_list[didx]}] / {e}")
                return False
            return False
        translated_list = []
        for idx in find:
            if traverse(data_list[idx], subKey):
                translated_list.append(idx)
        if len(translated_list) > 1:
            LOG_DEBUG(2, f"Replace multiple text \"{record}\" for \"{[(idx, data_list[idx]) for idx in translated_list]}\"")
        translated_result += translated_list
    # for idx in [idx for idx in range(len(data_list)) if idx not in translated_result]:
    #     LOG_WARN(2, f"Detect not translated line [{idx}]\"{data_list[idx]}\"")


    return json_data

def LoadOldKV(file_name:str) -> dict:
    input_path = os.path.join(MASTERDB_DRIVE_PATH, file_name+".xlsx")
    input_dataframe = pd.read_excel(input_path, na_values="", keep_default_na=False, na_filter=False, engine="openpyxl")
    input_dataframe.fillna("", inplace=True)
    input_records = input_dataframe.to_dict(orient="records")
    data = {}
    for input_record in input_records:
        input_record_keys = input_record.keys()
        if not "text" in input_record_keys or not type(input_record['text']) == str:
            continue
        if (not "trans" in input_record_keys or not type(input_record['trans']) == str or \
            (input_record['text'] != "" and input_record['trans'] == "")) \
            or input_record['text'] == input_record['trans']:
            continue
        # 수정해야되는 내용 수정
        if input_record["trans"].startswith("'"):
            data[_Deserialize(input_record["text"])] = _Deserialize(input_record["trans"][1:])
        else:
            data[_Deserialize(input_record["text"])] = _Deserialize(input_record["trans"])
    return data
"""
Converter

"""
UPDATE_TIMESTAMP = datetime.today().isoformat(" ")
# Update xlsx with gakumas-diff json
def UpdateXlsx(file_name:str) -> int:
    with db_session():
        return _UpdateXlsx(file_name)

def _UpdateXlsx(file_name:str) -> int:
    empty_value_counter = 0
    record_structure = GetRecordStructure(file_name)
    
    kr_data_records = ReadXlsx(file_name)
    kr_data_records_size = len(kr_data_records)
        
    if kr_data_records_size > 0 and len(record_structure.keys()) != len(kr_data_records[0].keys()):
        LOG_ERROR(2, f"Mismatch PrimaryKey with {file_name}.xlsx and scripts")
        LOG_ERROR(3, f"Please convert key to '{kr_data_records}'")
        raise KeyError("Mismatch PrimaryKey")
    
    jp_data_records = JsonToRecord(file_name)

    old_kr_data_kv = {}
    try:
        old_kr_data_kv = LoadOldKV(file_name)
    except FileNotFoundError as e:
        old_kr_data_kv = {}
    
    kr_touched_list = []
    for jp_idx, jp_record in enumerate(jp_data_records):
        jp_keys = list(jp_record.keys())
        kr_target_idx = -1
        kr_target_record = None
        for kr_idx, kr_record in enumerate(kr_data_records):
            kr_keys = list(kr_record.keys())
            isPrimaryKeyUnMatch = False
            for tidx in range(1,len(jp_keys)-4):
                if jp_record[jp_keys[tidx]] != kr_record[kr_keys[tidx]]:
                    isPrimaryKeyUnMatch = True
            if isPrimaryKeyUnMatch:
                continue
            if jp_record['ID'] != kr_record['ID']:
                kr_target_idx = kr_idx
                continue
            if jp_record['원문'] != kr_record['원문']:
                # LOG_WARN(2, f"Original text mismatch for jp(newer):'{jp_record}' kr(older):'{kr_record}'")
                kr_target_idx = kr_idx
                continue
            kr_record["_GAKU_TOUCHED"] = True
            if kr_target_record != None:
                # LOG_WARN(2, f"Find duplicate key match for jp:'{jp_record}' kr1:'{kr_target_record}' kr2:'{kr_record}'")
                continue
            kr_target_idx = kr_idx
            kr_target_record = kr_record
        if kr_target_record == None:
            jp_record["설명"] = "추가 : " + UPDATE_TIMESTAMP
            jp_record["번역"] = DB_get(jp_record["원문"])
            if jp_record["번역"] == "":
                jp_record["번역"] = old_kr_data_kv.get(jp_record["원문"], "") 
            empty_value_counter += 1
            if kr_target_idx == -1:
                jp_record["_GAKU_TOUCHED"] = True
                kr_data_records.append(jp_record)
            else:
                jp_record["_GAKU_TOUCHED"] = True
                kr_data_records.insert(kr_target_idx+1, jp_record)
                LOG_WARN(2, f"Find new record inside on {kr_target_idx+1} : '{jp_record}' at '{kr_data_records[kr_target_idx]}'")
    # Collect unused data
    kr_unused_list = [(idx,record) for idx, record in enumerate(kr_data_records) if "_GAKU_TOUCHED" not in record]
    for kr_idx, record in kr_unused_list:
        LOG_WARN(2, f"Unused record[{kr_idx}] : {record}")
        # record["설명"] = "미사용 : " + UPDATE_TIMESTAMP
        kr_data_records.remove(record)
    for record in kr_data_records:
        if "_GAKU_TOUCHED" in record:
            record.pop("_GAKU_TOUCHED")
        if record["번역"] == "":
            record["번역"] = DB_get(record["원문"])
            LOG_WARN(2, f"Find old untranslated record and use db : {record}")

    WriteXlsx(file_name, kr_data_records)
    return empty_value_counter

def CreateJSON(file_name):
    kr_data_records = ReadXlsx(file_name)
    jp_data_obj = ReadJson(file_name)
    json_output = OverrideRecordToJson(jp_data_obj, kr_data_records)
    WriteJson(file_name, json_output)

"""

Folder Processor

"""


from .paths import (
    MASTERDB_ORIGINAL_PATH, MASTERDB_JSON_PATH, MASTERDB_ORIGINAL_DATA_PATH,
    MASTERDB_REMOTE_PATH, MASTERDB_DRIVE_PATH,
    MASTERDB2_REMOTE_PATH, MASTERDB2_DRIVE_PATH,
    MASTERDB_TEMP_PATH, MASTERDB_OUTPUT_PATH, MASTERDB_CACHE_FILE,
    GIT_MASTERDB_PATH,
)
from .helper import load_cache_date, save_cache_date


def _filter_masterdb_files(file_paths):
    """Extract file names from paths, convert yaml→json, filter by RULES."""
    file_list = [name[:-5] for _, _, name in file_paths]
    if RULES is None:
        LoadRules()
    file_list = [
        name for name in convert_yaml_types_in_parallel(file_list)
        if name in RULES
    ]
    return file_list


def _update_masterdb_xlsx_batch(file_list):
    """Run UpdateXlsx for each file. Returns total empty (untranslated) count."""
    empty_value_count = 0
    for idx, file_name in enumerate(file_list):
        _empty_value_count = 0
        try:
            LOG_DEBUG(2, f"Converting {file_name}...({idx}/{len(file_list)})")
            _empty_value_count = UpdateXlsx(file_name)
            LOG_DEBUG(2, f"Untranslated values : {_empty_value_count}")
        except Exception as e:
            LOG_ERROR(2, f"Error {e}")
            logger.exception(e)
        empty_value_count += _empty_value_count
    return empty_value_count


def _convert_masterdb_batch(todo_list):
    """Run CreateJSON for each file. Returns (errors, successes)."""
    converted_file_list = []
    error_file_list = []
    for file_name in todo_list:
        LOG_DEBUG(2, f"Start convert from drive to output '{file_name}'")
        try:
            CreateJSON(file_name)
            converted_file_list.append(file_name)
        except Exception as e:
            LOG_ERROR(2, f"Error during Convert MasterDB drive to output: {e}")
            logger.exception(e)
            error_file_list.append((file_name, e))
    return error_file_list, converted_file_list


# 업데이트 반영
# gakumas-diff > Google Drive
def UpdateOriginalToDrive():
    last_update_date = load_cache_date(MASTERDB_CACHE_FILE)
    if last_update_date:
        LOG_DEBUG(2, f"Load update date {last_update_date}")
    save_cache_date(MASTERDB_CACHE_FILE)

    if last_update_date is not None:
        LOG_DEBUG(2, "Check git diff")
        original_file_paths = Helper_GetFilesFromDirByDate(last_update_date, MASTERDB_ORIGINAL_PATH, ".yaml")
    else:
        original_file_paths = []
    if len(original_file_paths) <= 0:
        LOG_INFO(2, "MasterDB is not updated, skip")
        return []

    file_list = _filter_masterdb_files(original_file_paths)
    LOG_INFO(2, f"Updating {len(file_list)} MasterDB files")
    empty_value_count = _update_masterdb_xlsx_batch(file_list)
    LOG_DEBUG(2, f"Sum of untranslated values : {empty_value_count}")

    return file_list


# Google Drive > GakumasTranslationDataKor
def ConvertDriveToOutput(drive_file_paths=None, bFullUpdate=False):
    if drive_file_paths is None:
        LOG_DEBUG(2, "No file list provided, scanning local drive")
        drive_file_paths = Helper_GetFilesFromDir(MASTERDB2_DRIVE_PATH, ".xlsx")
    if len(drive_file_paths) <= 0:
        LOG_INFO(2, "MasterDB is not updated, skip")
        return [], []

    todo_list = [filename[:-5] for _, _, filename in drive_file_paths]

    ORIGIN_CWD = os.getcwd()
    try:
        convert_yaml_types_in_parallel(todo_list)
    finally:
        os.chdir(ORIGIN_CWD)

    LOG_INFO(2, f"Converting {len(drive_file_paths)} MasterDB files")
    return _convert_masterdb_batch(todo_list)