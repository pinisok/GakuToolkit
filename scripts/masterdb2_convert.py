"""JSON override (Convert) pipeline for MasterDB.

Applies translated records from XLSX back into JSON data objects.
"""

import re

from .log import *
from .masterdb2_db import db_session, DB_save


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


def CreateJSON(file_name):
    from .masterdb2_io import ReadXlsx, ReadJson, WriteJson
    kr_data_records = ReadXlsx(file_name)
    jp_data_obj = ReadJson(file_name)
    json_output = OverrideRecordToJson(jp_data_obj, kr_data_records)
    WriteJson(file_name, json_output)
