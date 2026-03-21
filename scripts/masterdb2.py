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



_ARRAY_KEY_PATTERN = re.compile(r'^(\w*[Dd]escriptions)\[(\d+)\]\.text$')


def _parse_array_key(key_id: str):
    """Parse array-based key ID like 'produceDescriptions[3].text'.

    Returns (field_name, index) if it's a Descriptions array key, None otherwise.
    """
    m = _ARRAY_KEY_PATTERN.match(key_id)
    if not m:
        return None
    return m.group(1), int(m.group(2))


def _get_prev_array_element(record, all_records, field_name, idx, json_data):
    """Get the text of the previous array element (idx-1).

    Lookup order:
    1. Same primary key records in all_records with ID = field_name[idx-1].text
    2. Source JSON data

    Returns the previous element's text (translated if from records, original if from JSON),
    or None if not found or idx is 0.
    """
    if idx <= 0:
        return None

    prev_key_id = f"{field_name}[{idx - 1}].text"

    # Extract primary key values from current record for matching
    pk_fields = {}
    i = 0
    while f"KEY ID {i}" in record:
        pk_fields[record[f"KEY ID {i}"]] = record[f"KEY VALUE {i}"]
        i += 1

    # Search in all_records for matching primary key + prev ID
    for r in all_records:
        if r.get("ID") != prev_key_id:
            continue
        # Check primary key match
        match = True
        j = 0
        while f"KEY ID {j}" in r:
            if r.get(f"KEY VALUE {j}") != pk_fields.get(r.get(f"KEY ID {j}")):
                match = False
                break
            j += 1
        if match:
            # Prefer translation, fall back to original
            return r.get("번역") or r.get("원문", "")

    # Fall back to JSON data
    if json_data is not None and "data" in json_data:
        for data_entry in json_data["data"]:
            # Check primary key match
            pk_match = all(
                str(data_entry.get(k)) == v for k, v in pk_fields.items()
            )
            if not pk_match:
                continue
            # Navigate to the array field
            arr = data_entry.get(field_name)
            if isinstance(arr, list) and idx - 1 < len(arr):
                prev_elem = arr[idx - 1]
                if isinstance(prev_elem, dict):
                    return prev_elem.get("text", "")
                elif isinstance(prev_elem, str):
                    return prev_elem
            break

    return None


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


def _apply_str_translation(container, key, record, sub_key_path):
    """Apply string translation to container[key].

    Returns True if applied, False if original text mismatch.
    """
    if container[key] != record["원문"]:
        LOG_DEBUG(2, f"Original text mismatch at {sub_key_path}: '{container[key][:30]}' != '{record['원문'][:30]}'")
        return False
    DB_save(container[key], record["번역"])
    container[key] = record["번역"]
    return True


def _apply_list_translation(container, key, record, sub_key_path, debug_context=""):
    """Apply list (string array) translation to container[key].

    Returns True if applied, False on mismatch, None if invalid format.
    """
    trans_str = record["번역"]
    if not trans_str.startswith("[LA_F]"):
        LOG_WARN(2, f"Except list but meet invalid translate value \"{trans_str}\" at \"{record}\"{debug_context}")
        return None
    original_text = f"[LA_F]{'[LA_N_F]'.join(container[key])}"
    if original_text != record["원문"]:
        LOG_DEBUG(2, f"Original text mismatch (array) at {sub_key_path}")
        return False
    DB_save(original_text, record["번역"])
    container[key] = trans_str[len("[LA_F]"):].split("[LA_N_F]")
    return True


def _traverse_and_apply(obj, key_path, record, sub_key_path, debug_context=""):
    """Recursively traverse obj along key_path and apply translation.

    Args:
        obj: Current object being traversed (dict or list).
        key_path: Remaining dot-separated key path (e.g. "field[0].subfield").
        record: The translation record with "원문" and "번역".
        sub_key_path: Full sub-key path for logging.
        debug_context: Debug info string for warnings.

    Returns True if translation applied, False otherwise.
    """
    subkey_list = key_path.split(".", 1)
    subkey = subkey_list[0]
    remaining = subkey_list[1] if len(subkey_list) > 1 else None
    match_result = PATTERN.search(subkey)

    # Resolve the target value: either via array index or direct key
    if isinstance(match_result, re.Match):
        idx = int(match_result.groups()[0])
        subkey_name = PATTERN.sub("", subkey)
        parent = obj[subkey_name]
        value = parent[idx]
    else:
        parent = obj
        idx = subkey
        value = obj[subkey]

    # Recurse into dict
    if isinstance(value, dict):
        if remaining is None:
            LOG_WARN(2, f"Subkey {sub_key_path} is short for object {record}{debug_context}")
            return False
        return _traverse_and_apply(value, remaining, record, sub_key_path, debug_context)

    # Apply string translation
    if isinstance(value, str):
        return _apply_str_translation(parent, idx, record, sub_key_path)

    # Handle list value
    if isinstance(value, list):
        # Non-string list → recurse deeper
        if len(value) > 0 and not isinstance(value[0], str):
            if remaining is not None:
                return _traverse_and_apply(
                    parent if not isinstance(match_result, re.Match) else parent,
                    remaining, record, sub_key_path, debug_context,
                )
            return False
        # String array → apply list translation
        result = _apply_list_translation(parent, idx, record, sub_key_path, debug_context)
        return result is True

    LOG_WARN(2, f"Failed to match sub key {sub_key_path} for {record}{debug_context}")
    return False


def _ExtractKeyTuple(record: dict):
    """Extract primary key dict and sub-key ID from a record."""
    keys = {}
    idx = 0
    while f"KEY ID {idx}" in record:
        keys[record[f"KEY ID {idx}"]] = record[f"KEY VALUE {idx}"]
        idx += 1
    return keys, record["ID"]


def _TestKey(primaryKV, data: dict) -> bool:
    """Check if data matches all primary key-value pairs."""
    for k, v in primaryKV.items():
        if k in data and str(data.get(k, None)) != v:
            return False
    return True


def OverrideRecordToJson(json_data: dict, records: list[dict]) -> dict:
    with db_session():
        return _OverrideRecordToJson(json_data, records)


def _OverrideRecordToJson(json_data: dict, records: list[dict]) -> dict:
    data_list = json_data["data"]
    translated_result = []

    for ridx, record in enumerate(records):
        primaryKey, subKey = _ExtractKeyTuple(record)
        if subKey == "":
            LOG_WARN(2, f"Find empty sub key(ID) for {record}")
            continue
        if record["번역"] == "":
            LOG_DEBUG(2, f"Skip empty translated value for {record}")
            continue

        find = [didx for didx, data in enumerate(data_list) if _TestKey(primaryKey, data)]
        if len(find) < 1:
            LOG_WARN(2, f"Failed to find matching primary key for {record}")
            continue

        translated_list = []
        for didx in find:
            debug_ctx = f" / data:[{didx}/{data_list[didx]}]"
            try:
                if _traverse_and_apply(data_list[didx], subKey, record, subKey, debug_ctx):
                    translated_list.append(didx)
            except Exception as e:
                LOG_WARN(2, f"Failed to match sub key {subKey} for {record}{debug_ctx} / {e}")

        if len(translated_list) > 1:
            LOG_DEBUG(2, f"Replace multiple text \"{record}\" for \"{[(idx, data_list[idx]) for idx in translated_list]}\"")
        translated_result += translated_list

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
def UpdateXlsx(file_name:str):
    """Returns (empty_value_count, warnings_list)."""
    with db_session():
        return _UpdateXlsx(file_name)

def _extract_primary_key_tuple(record: dict):
    """Extract primary key fields as a hashable tuple for indexing."""
    keys = list(record.keys())
    # Primary key fields are between IMAGE and ID (indices 1..N where N = (len-4)//2 * 2)
    key_count = (len(keys) - 4) // 2
    pk_values = []
    for i in range(key_count):
        pk_values.append((record[keys[2 * i + 1]], record[keys[2 * i + 2]]))
    return tuple(pk_values)


def _build_kr_index(kr_data_records):
    """Build a primary key → list of (index, record) index for fast lookup."""
    index = {}
    for idx, record in enumerate(kr_data_records):
        pk = _extract_primary_key_tuple(record)
        index.setdefault(pk, []).append((idx, record))
    return index


def _match_kr_record(jp_record, jp_keys, candidates):
    """Find the best matching kr_record from candidates.

    Returns (kr_target_idx, kr_target_record) where kr_target_record is None
    if no exact match was found (kr_target_idx may still point to a near-match).
    """
    kr_target_idx = -1
    kr_target_record = None
    for kr_idx, kr_record in candidates:
        if jp_record['ID'] != kr_record['ID']:
            kr_target_idx = kr_idx
            continue
        if jp_record['원문'] != kr_record['원문']:
            kr_target_idx = kr_idx
            continue
        kr_record["_GAKU_TOUCHED"] = True
        if kr_target_record is not None:
            continue
        kr_target_idx = kr_idx
        kr_target_record = kr_record
    return kr_target_idx, kr_target_record


def _find_prev_record(record, all_records, field_name, idx):
    """Find the previous array element's record object in all_records.

    Returns the record dict (mutable reference) or None.
    """
    if idx <= 0:
        return None

    prev_key_id = f"{field_name}[{idx - 1}].text"
    pk_fields = {}
    i = 0
    while f"KEY ID {i}" in record:
        pk_fields[record[f"KEY ID {i}"]] = record[f"KEY VALUE {i}"]
        i += 1

    for r in all_records:
        if r.get("ID") != prev_key_id:
            continue
        match = True
        j = 0
        while f"KEY ID {j}" in r:
            if r.get(f"KEY VALUE {j}") != pk_fields.get(r.get(f"KEY ID {j}")):
                match = False
                break
            j += 1
        if match:
            return r
    return None


def _apply_particle_correction(record, all_records, json_data):
    """Apply Korean particle correction to a DB-filled translation.

    Only applies when:
    - record ID is a Descriptions array key (e.g. produceDescriptions[3].text)
    - record has a non-empty translation
    - previous array element can be found

    Mutates record["번역"] in place. Also strips trailing space from
    the previous record's translation if needed. Returns True if correction applied.
    """
    from .korean import adjust_boundary

    translation = record.get("번역", "")
    if not translation:
        return False

    parsed = _parse_array_key(record.get("ID", ""))
    if parsed is None:
        return False

    field_name, idx = parsed

    # Get prev text for boundary adjustment
    prev_text = _get_prev_array_element(record, all_records, field_name, idx, json_data)
    if not prev_text:
        return False

    adjusted_prev, adjusted_next = adjust_boundary(prev_text, translation)

    changed = False
    if adjusted_next != translation:
        LOG_DEBUG(2, f"Particle correction: '{translation}' → '{adjusted_next}' (prev: '{prev_text}')")
        record["번역"] = adjusted_next
        changed = True

    # Also update previous record's trailing space if changed
    if adjusted_prev != prev_text:
        prev_record = _find_prev_record(record, all_records, field_name, idx)
        if prev_record is not None and prev_record.get("번역") == prev_text:
            prev_record["번역"] = adjusted_prev
            LOG_DEBUG(2, f"Prev record space strip: '{prev_text}' → '{adjusted_prev}'")
            changed = True

    return changed


def _UpdateXlsx(file_name: str):
    empty_value_counter = 0
    warnings = []
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
    except FileNotFoundError:
        old_kr_data_kv = {}

    # Load source JSON for particle correction fallback
    json_data = None
    try:
        json_data = ReadJson(file_name)
    except Exception:
        pass

    # Build index for O(1) primary key lookup
    kr_index = _build_kr_index(kr_data_records)

    # Track which records got DB-filled translations (for particle correction)
    db_filled_records = []

    # Collect new records to insert (avoid modifying kr_data_records during iteration)
    # Each entry: (insert_after_idx, new_record) or (None, new_record) for append
    pending_inserts = []

    for jp_idx, jp_record in enumerate(jp_data_records):
        jp_keys = list(jp_record.keys())
        jp_pk = _extract_primary_key_tuple(jp_record)
        candidates = kr_index.get(jp_pk, [])

        kr_target_idx, kr_target_record = _match_kr_record(jp_record, jp_keys, candidates)

        if kr_target_record is None:
            jp_record["설명"] = "추가 : " + UPDATE_TIMESTAMP
            jp_record["번역"] = DB_get(jp_record["원문"])
            if jp_record["번역"] == "":
                jp_record["번역"] = old_kr_data_kv.get(jp_record["원문"], "")
            if jp_record["번역"] != "":
                db_filled_records.append(jp_record)
            empty_value_counter += 1
            jp_record["_GAKU_TOUCHED"] = True
            if kr_target_idx == -1:
                pending_inserts.append((None, jp_record))
            else:
                pending_inserts.append((kr_target_idx, jp_record))
                warnings.append(f"신규 레코드 추가 near {kr_target_idx}")
                LOG_WARN(2, f"[{file_name}] Find new record near {kr_target_idx}")

    # Apply inserts: process from highest index to lowest to preserve positions
    appends = [rec for idx, rec in pending_inserts if idx is None]
    positional = [(idx, rec) for idx, rec in pending_inserts if idx is not None]
    positional.sort(key=lambda x: x[0], reverse=True)
    for insert_idx, record in positional:
        kr_data_records.insert(insert_idx + 1, record)
    kr_data_records.extend(appends)

    # Filter out unused records (immutable approach)
    result_records = []
    for idx, record in enumerate(kr_data_records):
        if "_GAKU_TOUCHED" not in record:
            warnings.append(f"미사용 레코드 삭제 [{idx}]")
            LOG_WARN(2, f"[{file_name}] Unused record[{idx}]")
            continue
        record.pop("_GAKU_TOUCHED")
        if record["번역"] == "":
            record["번역"] = DB_get(record["원문"])
            if record["번역"] != "":
                db_filled_records.append(record)
            LOG_DEBUG(2, f"[{file_name}] Untranslated record, using DB cache")
        result_records.append(record)

    # Apply particle correction to DB-filled translations
    correction_count = 0
    for record in db_filled_records:
        if _apply_particle_correction(record, result_records, json_data):
            correction_count += 1
    if correction_count > 0:
        LOG_DEBUG(2, f"[{file_name}] Applied {correction_count} particle corrections")

    WriteXlsx(file_name, result_records)
    return empty_value_counter, warnings

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
    """Run UpdateXlsx for each file. Returns (empty_count, all_warnings)."""
    empty_value_count = 0
    all_warnings = {}
    for idx, file_name in enumerate(file_list):
        _empty_value_count = 0
        try:
            LOG_DEBUG(2, f"Converting {file_name}...({idx}/{len(file_list)})")
            _empty_value_count, file_warnings = UpdateXlsx(file_name)
            LOG_DEBUG(2, f"Untranslated values : {_empty_value_count}")
            if file_warnings:
                all_warnings[file_name] = file_warnings
        except Exception as e:
            LOG_ERROR(2, f"Error {e}")
            logger.exception(e)
        empty_value_count += _empty_value_count
    return empty_value_count, all_warnings


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
        return [], {}

    file_list = _filter_masterdb_files(original_file_paths)
    LOG_INFO(2, f"Updating {len(file_list)} MasterDB files")
    empty_value_count, all_warnings = _update_masterdb_xlsx_batch(file_list)
    LOG_DEBUG(2, f"Sum of untranslated values : {empty_value_count}")

    return file_list, all_warnings


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