"""MasterDB v2 pipeline orchestration.

This module re-exports all public symbols from sub-modules for backward
compatibility, and contains the folder-level pipeline functions.
"""

import os
import sys

from .helper import *
from .log import *
from .paths import (
    MASTERDB_ORIGINAL_PATH, MASTERDB_JSON_PATH, MASTERDB_ORIGINAL_DATA_PATH,
    MASTERDB_REMOTE_PATH, MASTERDB_DRIVE_PATH,
    MASTERDB2_REMOTE_PATH, MASTERDB2_DRIVE_PATH,
    MASTERDB_TEMP_PATH, MASTERDB_OUTPUT_PATH, MASTERDB_CACHE_FILE,
    GIT_MASTERDB_PATH,
)
from .helper import load_cache_date, save_cache_date

# Re-export all public symbols for backward compatibility
from .masterdb2_db import db_session, DB_save, DB_get
from .masterdb2_record import (
    path_normalize_for_pk,
    check_need_export,
    _Deserialize,
    GetRecordStructure,
    DataToRecord,
    GetRule,
    LoadRules,
    RULES,
    rule_key_translate_map,
    rule_key_reverse_translate_map,
    TranslateRuleKey,
    TranslateReverseRuleKey,
)
from .masterdb2_io import (
    WriteXlsx,
    ReadXlsx,
    ReadJson,
    WriteJson,
    JsonToRecord,
    LoadOldKV,
    convert_yaml_types,
    convert_yaml_types_in_parallel,
    preprocess_yaml_content,
    _filter_file_list,
)
from .masterdb2_convert import (
    PATTERN,
    _apply_str_translation,
    _apply_list_translation,
    _traverse_and_apply,
    _ExtractKeyTuple,
    _TestKey,
    OverrideRecordToJson,
    _OverrideRecordToJson,
    CreateJSON,
)
from .masterdb2_update import (
    UPDATE_TIMESTAMP,
    _ARRAY_KEY_PATTERN,
    _parse_array_key,
    _extract_pk_fields,
    _find_record_by_pk_and_id,
    _get_prev_array_element,
    _find_prev_record,
    _apply_particle_correction,
    _extract_primary_key_tuple,
    _build_kr_index,
    _match_kr_record,
    UpdateXlsx,
    _UpdateXlsx,
)


"""

Folder Processor

"""


def _filter_masterdb_files(file_paths):
    """Extract file names from paths, convert yaml→json, filter by RULES."""
    from .masterdb2_record import RULES as _RULES, LoadRules as _LoadRules
    file_list = [name[:-5] for _, _, name in file_paths]
    if _RULES is None:
        _LoadRules()
    # Re-import after LoadRules updates the module-level variable
    from .masterdb2_record import RULES as _RULES2
    file_list = [
        name for name in convert_yaml_types_in_parallel(file_list)
        if name in _RULES2
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
