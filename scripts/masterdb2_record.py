"""Record structure and data extraction for MasterDB translation pipeline.

Handles conversion between JSON data objects and flat translation records.
"""

import os
import sys
import re
import string
from copy import deepcopy

from .helper import *
from .log import *
from .helper import Deserialize, SERIALIZE_LIST_BASIC


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


def _Deserialize(string: str) -> str:
    return Deserialize(string, rules=SERIALIZE_LIST_BASIC)


from .masterdb2_rules import (
    rule_key_translate_map,
    rule_key_reverse_translate_map,
    TranslateRuleKey,
    TranslateReverseRuleKey,
)

from . import paths as _paths

RULES = None


def LoadRules():
    global RULES
    if _paths.GIT_MASTERDB_PATH + "/scripts" not in sys.path:
        sys.path.append(_paths.GIT_MASTERDB_PATH + "/scripts")
    ORIGIN_CWD = os.getcwd()
    os.chdir(_paths.GIT_MASTERDB_PATH)
    from gakumasu_diff_to_json import primary_key_rules
    os.chdir(ORIGIN_CWD)
    RULES = primary_key_rules


def GetRule(file_name: str) -> dict:
    if RULES is None:
        LoadRules()
    if file_name in RULES:
        return dict(RULES)[file_name]
    else:
        return [[], []]


def GetRecordStructure(file_name: str) -> dict:
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
    record_structure: dict = GetRecordStructure(file_name)
    key_size = (len(record_structure.keys()) - 4) // 2

    for key, value in record_structure.items():
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
            raise ValueError("Empty key during process")
        if record["원문"] == "":
            LOG_ERROR(2, f"Empty key 원문 during process {data}")
            raise ValueError("Empty key during process")
    return result
