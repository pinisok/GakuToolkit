import os, re, csv, difflib, multiprocessing, tqdm
from typing import Callable, Optional, Union
from io import StringIO
from datetime import datetime

import pandas as pd
from imas_tools.story.gakuen_parser import parse_messages as _externalParser
from imas_tools.story.story_csv import StoryCsv as _externalStoryCsv
import openpyxl
import xlsxwriter

from .helper import *
from . import rclone
from .log import *

def _internalOverrideXlsxColumn(dataframe):
    #override xlsx's column name
    dataframe.rename(columns={
            dataframe.columns[0] : "id",
            dataframe.columns[1] : "name",
            dataframe.columns[2] : "translated name",
            dataframe.columns[3] : "text",
            dataframe.columns[4] : "translated text",
        }, inplace= True)

def _internalReadXlsx(fp):
    LOG_DEBUG(4, f"Open {fp.name} by openpyxl from pandas")
    dataframe = pd.read_excel(fp, na_values="", keep_default_na=False, na_filter=False, engine="openpyxl")
    _internalOverrideXlsxColumn(dataframe)
    return dataframe

"""

XlsxToCsv Helpers

"""

def _encode(string : str):
    string = string.replace("\n","\\n").replace("\r","\\r").replace("~","～")
    string = REGEX_DOTS_4_TO_6.sub('……', string)
    string = REGEX_DOTS_3.sub('…', string)
    return string

def _internalXlsxDataFrameProcess(dataframe:pd.DataFrame, origin_path:str):
    #Remove comments
    if len(dataframe.columns) > 5: dataframe.drop(dataframe.columns[5:], axis=1,inplace=True)
    dataframe.loc[len(dataframe)] = {"id":"info", "name" : origin_path}
    dataframe.loc[len(dataframe)] = {"id":"译者", "name" : "None"}

def _internalXlsxRecordsProcess(records : list[dict]):
    for record in records:
        # id
        if not isinstance(record["id"], str):
            if isinstance(record["id"], int):   record["id"] = str(record["id"])
            else:                               record["id"] = ""
        # name
        if not isinstance(record["name"], str): record["name"] = ""
        else:                                   record["name"] = str(record["name"])
        if "translated name" in record.keys() and \
                isinstance(record["translated name"], str) and \
                len(record["translated name"].strip()) > 0:
            record["name"] = record["translated name"]
        else:
            # 이름을 확인해 초성화가 있는 캐릭터라면 번역을 적용
            record["name"] = CHARACTER_REGEX_TRANS_MAP.get(record["name"], record["name"])

        # text
        if not isinstance(record["text"], str):                                         
            record["text"] = ""

        if not isinstance(record["translated text"], str):
            if isinstance(record["translated text"], int) or isinstance(record["translated text"], float):  
                record["translated text"] = str(record["translated text"])
            else:                                                                       
                record["translated text"] = ""
                
        record["text"] = _encode(record["text"])
        record["translated text"] = _encode(record["translated text"])
        if len(record["translated text"]) < 1 and record["id"] != "译者" and record["id"] != "info":
            raise Exception(f"Adv 파일 {records.index(record)}번째 줄 번역문 '{record['text']}'이 빈 줄 입니다. 해당 파일을 스킵합니다")

def _internalCsvWriter(fp, records):
    writer = csv.DictWriter(fp, fieldnames=["id","name","text","trans"], lineterminator='\n')
    writer.writerow({"id":"id","name":"name","text":"text","trans":"trans"})
    for record in records:
        writer.writerow({"id":record["id"],"name":record["name"],"text":record["text"],"trans":record["translated text"]})


"""
Helper for TxtToXlsx

"""

def _internalTxtToScv(read_fp, file_name):
    txt = _externalParser(read_fp.read())
    story_csv = _externalStoryCsv.new_empty_csv(file_name)
    for line in txt:
        if (line["__tag__"] == "message" or line["__tag__"] == "narration") and line.get("text"):
            story_csv.append_line(
                {
                    "id": "0000000000000",
                    "name": line.get("name", "__narration__"),
                    "text": line["text"],
                    "trans": "",
                }
            )
        if line["__tag__"] == "title" and line.get("title"):
            story_csv.append_line({"id": "0000000000000", "name": "__title__", "text": line["title"], "trans": ""})
        if line["__tag__"] == "choicegroup":
            if isinstance(line["choices"], list):
                for choice in line["choices"]:
                    story_csv.append_line({"id": "select", "name": "", "text": choice["text"], "trans": ""})
            elif isinstance(line["choices"], dict):
                story_csv.append_line({"id": "select", "name": "", "text": line["choices"]["text"], "trans": ""})
            else:
                raise ValueError(f"Unknown choice type: {line['choices']}")
    return StringIO(str(story_csv))

def _internalCsvToDataFrame(read_fp):
    adv_dataframe = pd.read_csv(read_fp, index_col=False)
    adv_dataframe.insert(loc=2, column='translated name', value="")
    adv_dataframe.drop(adv_dataframe.tail(2).index, inplace=True)
    _internalOverrideXlsxColumn(adv_dataframe)
    if len(adv_dataframe.index) < 1:
        raise ValueError("No message")
    adv_dataframe['text'] = adv_dataframe['text'].replace("\\n", "\n")
    return adv_dataframe

def _internalUpdateDataFrame(new_dataframe, original_fp):
    orig_dataframe = _internalReadXlsx(original_fp)
    orig_records = orig_dataframe.to_dict(orient='records')
    new_records = orig_dataframe.to_dict(orient='records')
    bOverride = False

    orig_records_length = len(orig_records)
    new_records_length = len(new_records)
    if orig_records_length != new_records_length:
        bOverride = True

    for idx in range(len(new_records)):
        if idx >= orig_records_length:
            bOverride = True
            break
        if not bOverride and new_records[idx]["text"] != orig_records[idx]["text"]:
            bOverride = True

    if bOverride:
        for idx in range(len(new_records)):
            new_records[idx]["comments"] = ""
            if idx >= orig_records_length:
                continue
            new_records[idx]["translated name"] = orig_records[idx]["translated name"]
            new_records[idx]["translated text"] = orig_records[idx]["translated text"]
            if new_records[idx]["text"] != orig_records[idx]["text"]:
                diff = difflib.ndiff(orig_records[idx]["text"], new_dataframe[idx]["text"])
                LOG_WARN(4, f"Unmatch text at line {idx} : {float(len(list(diff)) / len(new_dataframe[idx]['text'])):.02f}")
                LOG_WARN(4, f"'{new_records[idx]['text']}' : '{orig_records[idx]['text']}'")
                new_records[idx]["comments"] = "원본 문자열이 수정되었습니다. 번역값이 적절한지 확인 후 해당 문구를 삭제해주세요"
        return pd.DataFrame.from_dict(new_records)
    return pd.DataFrame.from_dict(orig_records)

def _internalDataFrameToXlsx(dataframe, write_fp):
    writer = pd.ExcelWriter(write_fp, engine="xlsxwriter") 
    dataframe.to_excel(writer, index=False, sheet_name="Sheet1")

    workbook = writer.book
    worksheet = writer.sheets['Sheet1']
    name_format = workbook.add_format(XLSX_NAME_FORMAT)
    worksheet.set_column(0, 2, 15, name_format)
    text_format = workbook.add_format(XLSX_TEXT_FORMAT)
    worksheet.set_column(3, 4, 70, text_format)

    writer.close()

def _internalCsvToTxt(csv_strings, txt_strings):
    def _merger(original_text: str,
        translated_text: str,
        validation_original_text: Optional[str] = None,
        *,
        is_choice=False,
    ):
        translated_text = re.sub(r"(?<!\\)=", r"\\=", translated_text)
        if (
            validation_original_text is not None
            and validation_original_text != original_text
        ):
            raise ValueError(
                f"Original text does not match validation text: '{validation_original_text}' != '{original_text}'"
            )
        return translated_text
    story_csv = _externalStoryCsv(csv_strings)
    parsed_message = _externalParser(txt_strings)
    iterator = iter(story_csv.data)
    for line in parsed_message:
        if line["__tag__"] == "message" or line["__tag__"] == "narration":
            if line.get("text"):
                next_csv_line = next(iterator)
                new_text = _merger(
                    line["text"], next_csv_line["trans"], next_csv_line["text"]
                )
                txt_strings = txt_strings.replace(
                    f"text={line['text']}",
                    f"text={new_text}",
                    1,
                )
        if line["__tag__"] == "message":
            if line.get("name") and line["name"] != "" and next_csv_line["name"] != "":
                txt_strings = txt_strings.replace(
                    f"name={line['name']}",
                    f"name={next_csv_line['name']}",
                    1,
                )
                
        if line["__tag__"] == "title":
            if line.get("title"):
                next_csv_line = next(iterator)
                new_text = _merger(
                    line["title"], next_csv_line["trans"], next_csv_line["text"]
                )
                txt_strings = txt_strings.replace(
                    f"title={line['title']}",
                    f"title={new_text}",
                    1,
                )
        if line["__tag__"] == "choicegroup":
            if isinstance(line["choices"], list):
                for choice in line["choices"]:
                    next_csv_line = next(iterator)
                    new_text = _merger(
                        choice["text"],
                        next_csv_line["trans"],
                        next_csv_line["text"],
                        is_choice=True,
                    )
                    txt_strings = txt_strings.replace(
                        f"text={choice['text']}",
                        f"text={new_text}",
                        1,
                    )
            elif isinstance(line["choices"], dict):
                next_csv_line = next(iterator)
                new_text = _merger(
                    line["choices"]["text"],
                    next_csv_line["trans"],
                    next_csv_line["text"],
                    is_choice=True,
                )
                txt_strings = txt_strings.replace(
                    f'text={line["choices"]["text"]}',
                    f"text={new_text}",
                    1,
                )
    return txt_strings

"""
File Converter

"""

# 번역 > 데이터 변환
def XlsxToCsv(read_fp, write_fp, origin_path:str):
    xlsx_dataframe = _internalReadXlsx(read_fp)

    _internalXlsxDataFrameProcess(xlsx_dataframe, origin_path)
    
    xlsx_records = xlsx_dataframe.to_dict(orient="records")
    
    _internalXlsxRecordsProcess(xlsx_records)
    _internalCsvWriter(write_fp, xlsx_records)

def CsvToTxt(read_fp, write_path, original_path): 
    csv_strings = "".join(read_fp.readlines())
    with open(original_path, "r", encoding='utf-8') as write_fp:
        txt_strings = "".join(write_fp.readlines())
    txt_strings = _internalCsvToTxt(csv_strings, txt_strings)
    with open(write_path, "w", encoding="utf-8") as write_fp:
        write_fp.write(txt_strings)

def XlsxToTxt_parallels(obj):
    input_path, filename = obj
    output_path = os.path.join(ADV_OUTPUT_PATH, filename[:-5]+".txt")
    original_path = os.path.join(ADV_ORIGINAL_PATH, filename[:-5]+".txt")
    converted_file_list = []
    error_file_list = []
    try:
        XlsxToTxt(input_path, output_path, original_path)
        converted_file_list.append(filename)
    except Exception as e:
        # LOG_ERROR(2, f"Error: {e}")
        # logger.exception(e)
        error_file_list.append((e, filename))
    return error_file_list, converted_file_list

def XlsxToTxt(input_path, write_path, original_path):
    input_fp = open(input_path, "rb")
    csvIO = StringIO()
    XlsxToCsv(input_fp, csvIO, os.path.basename(write_path))
    csvIO.seek(0)
    CsvToTxt(csvIO, write_path, original_path)

# 원본 > 번역 변환

def TxtToXlsx_parallels(obj):
    input_path, output_path, filename = obj
    try:
        TxtToXlsx(input_path, output_path, filename)
    except Exception as e:
        LOG_ERROR(2, f"Error: {e}")
        logger.exception(e)


def TxtToXlsx(input_path, output_path, file_name:str):
    with open(input_path, "r", encoding="utf-8") as input_fp:
        csv = _internalTxtToScv(input_fp, file_name)
    dataframe = _internalCsvToDataFrame(csv)
    
    if(os.path.exists(output_path)):
        original_fp = open(output_path, "rb")
        LOG_DEBUG(4, f"Try to update original file")
        dataframe = _internalUpdateDataFrame(dataframe, original_fp)
        original_fp.close()

    LOG_DEBUG(4, f"Write result to file")
    write_fp = open(output_path, "wb")
    _internalDataFrameToXlsx(dataframe, write_fp)
    write_fp.close()


"""

Folder Processor Helper

"""
ADV_BLACKLIST_FILE = [
    "musics.txt",
    "adv_warmup.txt",
]
ADV_BLACKLIST_FOLDER = [
    "pstep",
    "pweek",
]

def _internalGetOutputPath(filename:str):
    splitted_name = filename[4:-4].split("_")
    folder_name = splitted_name[0]
    if splitted_name[0] == "pstory":
        folder_name += "_" + splitted_name[1]
    folder_name = folder_name.split("-")[0]
    return folder_name


"""

Folder Processor

"""


GIT_ADV_PATH = DEFAULT_PATH + "/res/adv"
ADV_ORIGINAL_PATH = GIT_ADV_PATH + "/Resource"
ADV_REMOTE_PATH = REMOTE_PATH + "/text assets"
ADV_DRIVE_PATH = DRIVE_PATH + "/text assets"
ADV_TEMP_PATH = TEMP_PATH + "/adv"
ADV_OUTPUT_PATH = OUTPUT_PATH + "/local-files/resource"
ADV_CACHE_FILE = "./cache/adv_update_date.txt"

# 업데이트 반영
# Campus-Adv-txts > Google Drive
def UpdateOriginalToDrive(bFullUpdate=False):
    last_update_date = None
    LOG_DEBUG(2, "Check cache file")
    if os.path.exists(ADV_CACHE_FILE):
        with open(ADV_CACHE_FILE, 'r') as f:
            try:
                last_update_date = datetime.fromisoformat(f.readlines()[0])
                LOG_DEBUG(2, f"Load update date {last_update_date}")
            except:
                LOG_WARN(2, "Invalid adv cache file, skip update")
                last_update_date = None
    LOG_DEBUG(2, "Write datetime cache file")
    with open(ADV_CACHE_FILE, 'w') as f:
        f.write(datetime.today().isoformat(" "))
    if bFullUpdate:
        LOG_DEBUG(2, "Full update")
        original_file_paths = Helper_GetFilesFromDir(ADV_ORIGINAL_PATH, ".txt", "adv_")
    elif last_update_date != None:
        LOG_DEBUG(2, "Check git diff")
        original_file_paths = Helper_GetFilesFromDirByDate(last_update_date, GIT_ADV_PATH, ".txt", "adv_")
    else:
        original_file_paths = []
    if len(original_file_paths) <= 0:
        LOG_INFO(2, "ADV is not updated, skip")
        return []
    
    if True:
        file_list = []
        for abs_path, rel_path, filename in original_file_paths:
            if filename in ADV_BLACKLIST_FILE:
                continue
            foldername = _internalGetOutputPath(filename)
            if foldername in ADV_BLACKLIST_FOLDER:
                continue
            input_path = rel_path
            output_path = os.path.join(ADV_DRIVE_PATH, foldername, filename[:-4]+".xlsx")
            file_list.append((input_path, output_path, filename))
        file_list_size = len(file_list)
        LOG_INFO(2, f"Updating {file_list_size} adv files")
        pool = multiprocessing.Pool()
        for _ in tqdm.tqdm(pool.imap_unordered(TxtToXlsx_parallels, file_list), total=file_list_size):
            pass
        pool.close()
        pool.join()
        
    else:
        for abs_path, rel_path, filename in original_file_paths:
            if filename in ADV_BLACKLIST_FILE:
                continue
            foldername = _internalGetOutputPath(filename)
            if foldername in ADV_BLACKLIST_FOLDER:
                continue
            input_path = rel_path
            output_path = os.path.join(ADV_DRIVE_PATH, foldername, filename[:-4]+".xlsx")
            LOG_DEBUG(2, f"Start convert from drive to output '{input_path}' to '{output_path}'")
            
            try:
                TxtToXlsx(input_path, output_path, filename)
            except Exception as e:
                LOG_ERROR(2, f"Error: {e}")
                logger.exception(e)
    
    file_list = rclone.check(ADV_DRIVE_PATH, ADV_REMOTE_PATH)
    LOG_WARN(2, f"There is {len(file_list)} files changed")
    LOG_DEBUG(2, f"file_list : {file_list}")
    for obj in file_list:
        if obj[0] == "*":
            LOG_DEBUG(2, f"Update '{obj[1]}' file to remote")
    for obj in file_list:
        if obj[0] == "+":
            LOG_DEBUG(2, f"Add new '{obj[1]}' file to remote")
    # TODO Check result is okay
    if False:
        rclone.sync(ADV_DRIVE_PATH, ADV_REMOTE_PATH)
    return file_list


# 번역 수정사항 반영
# Google Drive > GakumasTranslationDataKor
def ConvertDriveToOutput(bFullUpdate=False):
    if bFullUpdate:
        LOG_DEBUG(2, "Try Full Update")
        rclone.copy(ADV_REMOTE_PATH, ADV_DRIVE_PATH)
        drive_file_paths = Helper_GetFilesFromDir(ADV_DRIVE_PATH, ".xlsx", "adv_")
    else:
        LOG_DEBUG(2, "Check updated files")
        check_result = rclone.check(ADV_REMOTE_PATH, ADV_DRIVE_PATH)
        # for obj in check_result: obj[1] = os.path.basename(obj[1])[:-5]+".txt"
        # LOG_DEBUG(2, f"Check result {check_result}")
        drive_file_paths = Helper_GetFilesFromDirByCheck(check_result, ADV_DRIVE_PATH, ".xlsx", "adv_")
        rclone.copy(ADV_REMOTE_PATH, ADV_DRIVE_PATH)
    if len(drive_file_paths) <= 0:
        LOG_INFO(2, "ADV is not updated, skip")
        return [],[]
    LOG_INFO(2, f"Converting {len(drive_file_paths)} adv files")

    converted_file_list = []
    error_file_list = []

    if True: #TODO Change this when multiprocessing flag completed
        file_list_size = len(drive_file_paths)
        pool = multiprocessing.Pool()
        with tqdm.tqdm(total=file_list_size) as pbar:
            for result in pool.imap_unordered(XlsxToTxt_parallels, [(abs_path, filename) for abs_path, rel_path, filename in drive_file_paths]):
                pbar.update()
                pbar.refresh()
                error_file_list += result[0]
                converted_file_list += result[1]
        pool.close()
        pool.join()
    else:
        for abs_path, rel_path, filename in drive_file_paths:
            input_path = abs_path
            output_path = os.path.join(ADV_OUTPUT_PATH, filename[:-5]+".txt")
            original_path = os.path.join(ADV_ORIGINAL_PATH, filename[:-5]+".txt")
            LOG_DEBUG(2, f"Start convert from drive to output '{input_path}' to '{output_path}'")
            
            try:
                XlsxToTxt(input_path, output_path, original_path)
                converted_file_list.append([abs_path, rel_path, filename])
            except Exception as e:
                LOG_ERROR(2, f"Error during Convert ADV drive to output: {e}")
                logger.exception(e)
                error_file_list.append((e, filename))
    return error_file_list, converted_file_list