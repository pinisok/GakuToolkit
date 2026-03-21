"""
Programmatic test fixture generators for each pipeline.

All functions create minimal valid files in a given directory.
Used by pytest fixtures in conftest.py via tmp_path.
"""

import json
import os

import pandas as pd


def create_adv_xlsx(path, records):
    """Create a minimal ADV xlsx file.

    Args:
        path: Output file path (.xlsx)
        records: List of dicts with keys:
            id, name, translated name, text, translated text
    """
    df = pd.DataFrame(records)
    df.to_excel(path, index=False, engine="xlsxwriter")


def create_adv_txt(path, messages):
    """Create a minimal ADV original txt file.

    The format uses [message text=... name=...] tags that imas_tools can parse.

    Args:
        path: Output file path (.txt)
        messages: List of dicts with keys:
            tag ("message"|"narration"|"title"|"choicegroup"),
            text, name (optional), choices (optional, list of str)
    """
    lines = []
    # Minimal header (background + camera are required for valid parse)
    lines.append(
        '[backgroundgroup backgrounds=[background id=bg src=env_2d_adv_school-entrance-00-noon]]'
    )
    lines.append(
        '[camerasetting setting=\\{"focalLength":30.0,"nearClipPlane":0.1,'
        '"farClipPlane":1000.0,"transform":\\{"position":\\{"x":0.0,"y":1.3,"z":1.6\\},'
        '"rotation":\\{"x":2.0,"y":180.0,"z":0.0\\},'
        '"scale":\\{"x":1.0,"y":1.0,"z":1.0\\}\\},'
        '"dofSetting":\\{"active":false,"focalPoint":4.0,"fNumber":4.0,'
        '"maxBlurSpread":3.0\\}\\}'
        ' clip=\\{"_startTime":0.0,"_duration":0.0,"_clipIn":0.0,'
        '"_easeInDuration":0.0,"_easeOutDuration":0.0,'
        '"_blendInDuration":-1.0,"_blendOutDuration":-1.0,'
        '"_mixInEaseType":1,"_mixOutEaseType":1,"_timeScale":1.0\\}]'
    )

    t = 0.8
    for msg in messages:
        clip = (
            f'clip=\\{{"_startTime":{t},"_duration":3.0,"_clipIn":0.0,'
            f'"_easeInDuration":0.0,"_easeOutDuration":0.0,'
            f'"_blendInDuration":-1.0,"_blendOutDuration":-1.0,'
            f'"_mixInEaseType":1,"_mixOutEaseType":1,"_timeScale":1.0\\}}'
        )
        tag = msg.get("tag", "message")

        if tag == "message":
            name = msg.get("name", "")
            lines.append(f'[message text={msg["text"]} name={name} {clip}]')
        elif tag == "narration":
            lines.append(f'[message text={msg["text"]} {clip}]')
        elif tag == "title":
            lines.append(f'[title title={msg["text"]} {clip}]')
        elif tag == "choicegroup":
            choices_str = " ".join(
                f'choices=[choice text={c}]' for c in msg["choices"]
            )
            lines.append(f'[choicegroup {choices_str} {clip}]')

        t += 4.0

    lines.append("[timeline]")
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def create_generic_xlsx(path, records):
    """Create a minimal Generic xlsx file.

    Args:
        path: Output file path (.xlsx)
        records: List of dicts with keys: text, trans
    """
    df = pd.DataFrame(records)
    df.to_excel(path, index=False, engine="xlsxwriter")


def create_lyrics_xlsx(path, records):
    """Create a minimal Lyrics xlsx file.

    Args:
        path: Output file path (.xlsx). Must contain "/lyrics/" in path.
        records: List of dicts with keys: A, B
    """
    df = pd.DataFrame(records)
    df.to_excel(path, index=False, engine="xlsxwriter")


def create_localization_xlsx(path, records):
    """Create a minimal Localization xlsx file.

    Args:
        path: Output file path (.xlsx)
        records: List of dicts with keys: 0 (reference), ID, 번역
    """
    df = pd.DataFrame(records)
    df.to_excel(path, index=False, engine="xlsxwriter")


def create_masterdb_xlsx(path, key_columns, records):
    """Create a minimal MasterDB2 xlsx file.

    Args:
        path: Output file path (.xlsx)
        key_columns: List of (key_id_name, key_value_name) pairs
            e.g. [("id",)] for single primary key
        records: List of dicts matching the column structure:
            IMAGE, KEY ID 0, KEY VALUE 0, ..., ID, 원문, 번역, 설명
    """
    df = pd.DataFrame(records)
    df.to_excel(path, index=False, engine="xlsxwriter")


def create_masterdb_json(path, primary_keys, data):
    """Create a minimal MasterDB2 input JSON file.

    Args:
        path: Output file path (.json)
        primary_keys: List of primary key field names, e.g. ["id"]
        data: List of data dicts
    """
    content = {
        "rules": {"primaryKeys": primary_keys},
        "data": data,
    }
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(content, f, ensure_ascii=False, indent=2)
