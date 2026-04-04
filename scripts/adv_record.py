"""ADV record and DataFrame processing.

Functions for reading XLSX into DataFrames, processing records
(type normalization, name translation, text encoding), and CSV writing.
"""

import csv

import pandas as pd

from .helper import CHARACTER_REGEX_TRANS_MAP
from .log import LOG_DEBUG
from .adv_encode import _encode, _processEMtag


def _internalOverrideXlsxColumn(dataframe: pd.DataFrame) -> None:
    """Rename DataFrame columns to standard ADV field names (mutates in place)."""
    dataframe.rename(columns={
        dataframe.columns[0]: "id",
        dataframe.columns[1]: "name",
        dataframe.columns[2]: "translated name",
        dataframe.columns[3]: "text",
        dataframe.columns[4]: "translated text",
    }, inplace=True)


def _internalReadXlsx(fp) -> pd.DataFrame:
    """Read an XLSX file into a DataFrame with standard column names."""
    LOG_DEBUG(4, f"Open {fp.name} by openpyxl from pandas")
    dataframe = pd.read_excel(
        fp, na_values="", keep_default_na=False, na_filter=False, engine="openpyxl"
    )
    _internalOverrideXlsxColumn(dataframe)
    return dataframe


def _internalXlsxDataFrameProcess(dataframe: pd.DataFrame, origin_path: str) -> None:
    """Append info and translator rows to a DataFrame (mutates in place)."""
    if len(dataframe.columns) > 5:
        dataframe.drop(dataframe.columns[5:], axis=1, inplace=True)
    dataframe.loc[len(dataframe)] = {"id": "info", "name": origin_path}
    dataframe.loc[len(dataframe)] = {"id": "译者", "name": "None"}


def _internalXlsxRecordsProcess(records: list[dict]) -> list[dict]:
    """Process xlsx records: normalize types, translate names, encode text.

    Returns a new list of processed records (does not mutate input).
    """
    processed = []
    for idx, record in enumerate(records):
        r = {**record}
        # id
        if not isinstance(r["id"], str):
            if isinstance(r["id"], int):
                r["id"] = str(r["id"])
            else:
                r["id"] = ""
        # name
        if not isinstance(r["name"], str):
            r["name"] = ""
        else:
            r["name"] = str(r["name"])
        if (
            "translated name" in r.keys()
            and isinstance(r["translated name"], str)
            and len(r["translated name"].strip()) > 0
        ):
            r["name"] = r["translated name"]
        else:
            r["name"] = CHARACTER_REGEX_TRANS_MAP.get(r["name"], r["name"])

        # text
        if not isinstance(r["text"], str):
            r["text"] = ""

        if not isinstance(r["translated text"], str):
            if isinstance(r["translated text"], (int, float)):
                r["translated text"] = str(r["translated text"])
            else:
                r["translated text"] = ""

        r["text"] = _encode(r["text"])
        r["translated text"] = _encode(r["translated text"])
        r["translated text"] = _processEMtag(r["translated text"])
        if len(r["translated text"]) < 1 and r["id"] != "译者" and r["id"] != "info":
            raise Exception(
                f"Adv 파일 {idx}번째 줄 번역문 '{r['text']}'이 빈 줄 입니다. 해당 파일을 스킵합니다"
            )
        processed.append(r)
    return processed


def _internalCsvWriter(fp, records: list[dict]) -> None:
    """Write processed records to a CSV file."""
    writer = csv.DictWriter(
        fp, fieldnames=["id", "name", "text", "trans"], lineterminator='\n'
    )
    writer.writerow({"id": "id", "name": "name", "text": "text", "trans": "trans"})
    for record in records:
        writer.writerow({
            "id": record["id"],
            "name": record["name"],
            "text": record["text"],
            "trans": record["translated text"],
        })
