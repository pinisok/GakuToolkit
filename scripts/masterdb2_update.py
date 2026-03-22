"""XLSX update (Update) pipeline for MasterDB.

Merges new JSON records into existing translated XLSX files.
Includes Korean particle correction for DB-cached translations.
"""

import re
from datetime import datetime

from .log import *
from .masterdb2_db import db_session, DB_get
from .masterdb2_io import ReadXlsx, WriteXlsx, ReadJson, JsonToRecord, LoadOldKV
from .masterdb2_record import GetRecordStructure


UPDATE_TIMESTAMP = datetime.today().isoformat(" ")

# Array key pattern for Descriptions fields
_ARRAY_KEY_PATTERN = re.compile(r'^(\w*[Dd]escriptions)\[(\d+)\]\.text$')


def _parse_array_key(key_id: str):
    """Parse array-based key ID like 'produceDescriptions[3].text'.

    Returns (field_name, index) if it's a Descriptions array key, None otherwise.
    """
    m = _ARRAY_KEY_PATTERN.match(key_id)
    if not m:
        return None
    return m.group(1), int(m.group(2))


def _extract_pk_fields(record):
    """Extract primary key field→value mapping from a record."""
    pk_fields = {}
    i = 0
    while f"KEY ID {i}" in record:
        pk_fields[record[f"KEY ID {i}"]] = record[f"KEY VALUE {i}"]
        i += 1
    return pk_fields


def _find_record_by_pk_and_id(target_id, pk_fields, all_records):
    """Find a record in all_records matching target_id and primary key fields.

    Returns the record dict (mutable reference) or None.
    """
    for r in all_records:
        if r.get("ID") != target_id:
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
    pk_fields = _extract_pk_fields(record)

    # Search in all_records
    prev_record = _find_record_by_pk_and_id(prev_key_id, pk_fields, all_records)
    if prev_record is not None:
        return prev_record.get("번역") or prev_record.get("원문", "")

    # Fall back to JSON data
    if json_data is not None and "data" in json_data:
        for data_entry in json_data["data"]:
            pk_match = all(
                str(data_entry.get(k)) == v for k, v in pk_fields.items()
            )
            if not pk_match:
                continue
            arr = data_entry.get(field_name)
            if isinstance(arr, list) and idx - 1 < len(arr):
                prev_elem = arr[idx - 1]
                if isinstance(prev_elem, dict):
                    return prev_elem.get("text", "")
                elif isinstance(prev_elem, str):
                    return prev_elem
            break

    return None


def _find_prev_record(record, all_records, field_name, idx):
    """Find the previous array element's record object in all_records.

    Returns the record dict (mutable reference) or None.
    """
    if idx <= 0:
        return None
    prev_key_id = f"{field_name}[{idx - 1}].text"
    pk_fields = _extract_pk_fields(record)
    return _find_record_by_pk_and_id(prev_key_id, pk_fields, all_records)


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


def _extract_primary_key_tuple(record: dict):
    """Extract primary key fields as a hashable tuple for indexing."""
    keys = list(record.keys())
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


def UpdateXlsx(file_name: str):
    """Returns (empty_value_count, warnings_list)."""
    with db_session():
        return _UpdateXlsx(file_name)


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
