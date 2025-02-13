import os, sys
from datetime import datetime
import shutil

import pandas as pd
import openpyxl
import xlsxwriter

from .helper import *
from . import rclone
from .log import *
"""
Converter Helper
"""
serialize_list = [
    ('\r','\\r'),
    ('\t','\\t'),
]
def Serialize(string:str):
    result = string
    for obj in serialize_list:
        result = result.replace(obj[0], obj[1])
    return result

def Deserialize(string:str):
    result = string
    for obj in serialize_list:
        result = result.replace(obj[1], obj[0])
    return result


"""
Converter

"""
def JsonToXlsx(input_path, output_path):
    with open(input_path, 'r', encoding='utf-8') as f:
        input_data = json.load(f)
    input_records = []
    counter = 0
    for k,v in input_data.items():
        # value = v            # Uncomment this when disable use defined value
        value = MDB.get(k, "") # Comment this when disable use defined value
        if value == "":
            counter += 1
        input_records.append({"text":Serialize(k), "trans":Serialize(value)})

    output_dataframe = pd.DataFrame.from_records(input_records)
    writer = pd.ExcelWriter(output_path, engine="xlsxwriter") 
    output_dataframe.to_excel(writer, index=False, sheet_name="Sheet1")
    workbook = writer.book
    row_format = workbook.add_format({'font_name': 'Calibri', 'bold':False, 'text_wrap':True, 'align':'center', 'valign':'top', 'border':1})
    column_format = workbook.add_format({'font_name': 'Calibri', 'align':'left'})
    worksheet = writer.sheets['Sheet1']
    worksheet.set_column(0,1,70,column_format)
    worksheet.set_row(0,17,row_format)
    writer.close()
    return counter

def XlsxToJson(input_path, output_path):
    input_dataframe = pd.read_excel(input_path, na_values="", keep_default_na=False, na_filter=False, engine="openpyxl")
    input_dataframe.convert_dtypes()
    input_dataframe.fillna("", inplace=True)
    input_records = input_dataframe.to_dict(orient="records")
    data = {}
    for input_record in input_records:
        input_record_keys = input_record.keys()
        if not "text" in input_record_keys or not type(input_record['text']) == str:
            continue
        if not "trans" in input_record_keys or not type(input_record['trans']) == str:
            LOG_WARN(3, f"{f}의 {input_record['text']}의 번역 값이 존재하지 않습니다. 넘어갑니다.")
            continue
        # 수정해야되는 내용 수정
        if input_record["trans"].startswith("'"):
            data[Deserialize(input_record["text"])] = Deserialize(input_record["trans"][1:])
        else:
            data[Deserialize(input_record["text"])] = Deserialize(input_record["trans"])
    os.makedirs(os.path.split(output_path)[0], exist_ok=True)
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, allow_nan=False, indent=4)
"""

Folder Processor

"""


MASTERDB_ORIGINAL_PATH = GIT_MASTERDB_PATH + "/gakumasu-diff/orig"
MASTERDB_JSON_PATH = GIT_MASTERDB_PATH + "/gakumasu-diff/json"
MASTERDB_ORIGINAL_DATA_PATH = GIT_MASTERDB_PATH + "/data"
MASTERDB_REMOTE_PATH = REMOTE_PATH + "/masterDB"
MASTERDB_DRIVE_PATH = DRIVE_PATH + "/masterDB"
MASTERDB_TEMP_PATH = TEMP_PATH + "/masterDB"
MASTERDB_OUTPUT_PATH = OUTPUT_PATH + "/local-files/masterTrans"
MASTERDB_CACHE_FILE = "./cache/masterdb_update_date.txt"

# 업데이트 반영
# gakumas-diff > Google Drive
def UpdateOriginalToDrive(bFullUpdate = False):
    # Check modified file from git commit
    last_update_date = None
    LOG_DEBUG(2, "Check cache file")
    if os.path.exists(MASTERDB_CACHE_FILE):
        with open(MASTERDB_CACHE_FILE, 'r') as f:
            try:
                last_update_date = datetime.fromisoformat(f.readlines()[0])
                LOG_DEBUG(2, f"Load update date {last_update_date}")
            except:
                LOG_WARN(2, "Invalid masterdb cache file, skip update")
                last_update_date = None
    LOG_DEBUG(2, "Write datetime cache file")
    with open(MASTERDB_CACHE_FILE, 'w') as f:
        f.write(datetime.today().isoformat(" "))
    if bFullUpdate:
        original_file_paths = None
    elif last_update_date != None:
        LOG_DEBUG(2, "Check git diff")
        original_file_paths = Helper_GetFilesFromDirByDate(last_update_date, MASTERDB_ORIGINAL_PATH, ".yaml")
    else:
        original_file_paths = []
    if len(original_file_paths) <= 0:
        LOG_INFO(2, "MasterDB is not updated, skip")
        return []
    
    file_list = None
    if original_file_paths is not None:
        file_list = []
        for _, _, name in original_file_paths:
            file_list.append(name[:-5])

    ORIGIN_CWD = os.getcwd()
    os.chdir(GIT_MASTERDB_PATH)
    sys.path.append(GIT_MASTERDB_PATH+"/scripts")
    import gakumasu_diff_to_json
    import pretranslate_process
    gakumasu_diff_to_json.process_list = file_list
    LOG_DEBUG(2, "Convert db's yaml to json")
    if os.path.exists(MASTERDB_JSON_PATH):
        shutil.rmtree(MASTERDB_JSON_PATH, True)
    gakumasu_diff_to_json.convert_yaml_types()
    os.chdir(ORIGIN_CWD)

    #Use default value
    LOG_DEBUG(2, "Copy output masterdb data to gakumas-master-translation's data folder")
    shutil.rmtree(MASTERDB_ORIGINAL_DATA_PATH, True)
    shutil.copytree(MASTERDB_OUTPUT_PATH, MASTERDB_ORIGINAL_DATA_PATH)

    #Generate Todo
    os.chdir(GIT_MASTERDB_PATH)
    if os.path.exists(GIT_MASTERDB_PATH+"/pretranslate_todo"):
        shutil.rmtree(GIT_MASTERDB_PATH+"/pretranslate_todo")
    pretranslate_process.gen_todo("gakumasu-diff/json")
    os.chdir(ORIGIN_CWD)

    #Convert from json to xlsx
    if(len(MDB.items()) == 0):
        Helper_LoadMasterDB()
    
    input_paths = Helper_GetFilesFromDir(GIT_MASTERDB_PATH + "/pretranslate_todo/todo", ".json")
    empty_value_count = 0
    LOG_INFO(2, f"Updating {len(input_paths)} MasterDB files")
    for abs, rel, name in input_paths:
        input_path = abs
        output_path = os.path.join(MASTERDB_DRIVE_PATH, name[:-5]+".xlsx")
        try:
            LOG_DEBUG(2, f"Convert from {input_path} to {output_path}")
            _empty_value_count = JsonToXlsx(input_path, output_path)
            LOG_DEBUG(2, f"Untranslated values : {_empty_value_count}")
        except Exception as e:
            LOG_ERROR(2, f"Error {e}")
            logger.exception(e)
        empty_value_count+=_empty_value_count
    LOG_DEBUG(2, f"Sum of untranslated values : {empty_value_count}")

    
    file_list = rclone.check(MASTERDB_DRIVE_PATH, MASTERDB_REMOTE_PATH)
    LOG_WARN(2, f"There is {len(file_list)} files changed")
    LOG_DEBUG(2, f"file_list : {file_list}")
    for obj in file_list:
        if obj[0] == "*":
            LOG_WARN(2, f"Update '{obj[1]}' file to remote")
    for obj in file_list:
        if obj[0] == "+":
            LOG_WARN(2, f"Add new '{obj[1]}' file to remote")
    if False:
        LOG_DEBUG(2, f"Upload result to remote")
        rclone.sync(MASTERDB_DRIVE_PATH, MASTERDB_REMOTE_PATH)
    return file_list


# 번역 수정사항 반영
# Google Drive > GakumasTranslationDataKor
def ConvertDriveToOutput(bFullUpdate=False):
    if bFullUpdate:
        LOG_DEBUG(2, "Try Full Update")
        rclone.copy(MASTERDB_REMOTE_PATH, MASTERDB_DRIVE_PATH)
        drive_file_paths = Helper_GetFilesFromDir(MASTERDB_DRIVE_PATH, ".xlsx")
    else:
        LOG_DEBUG(2, "Check updated files")
        check_result = rclone.check(MASTERDB_REMOTE_PATH, MASTERDB_DRIVE_PATH)
        # for obj in check_result: obj[1] = os.path.basename(obj[1])[:-5]+".txt"
        # LOG_DEBUG(2, f"Check result {check_result}")
        drive_file_paths = Helper_GetFilesFromDirByCheck(check_result, MASTERDB_DRIVE_PATH, ".xlsx")
        rclone.copy(MASTERDB_REMOTE_PATH, MASTERDB_DRIVE_PATH)
    if len(drive_file_paths) <= 0:
        LOG_INFO(2, "MasterDB is not updated, skip")
        return []
    todo_list = None
    if len(drive_file_paths) > 0:
        todo_list = []
        for abs_path, rel_path, filename in drive_file_paths:
            todo_list.append(filename[:-5])

    ORIGIN_CWD = os.getcwd()
    os.chdir(GIT_MASTERDB_PATH)
    sys.path.append(GIT_MASTERDB_PATH+"/scripts")
    import gakumasu_diff_to_json
    import pretranslate_process
    gakumasu_diff_to_json.process_list = todo_list
    LOG_DEBUG(2, "Convert db's yaml to json")
    if os.path.exists(MASTERDB_JSON_PATH):
        shutil.rmtree(MASTERDB_JSON_PATH, True)
    gakumasu_diff_to_json.convert_yaml_types()
    os.chdir(ORIGIN_CWD)

    #Use default value
    LOG_DEBUG(2, "Copy output masterdb data to gakumas-master-translation's data folder")
    shutil.rmtree(MASTERDB_ORIGINAL_DATA_PATH, True)
    shutil.copytree(MASTERDB_OUTPUT_PATH, MASTERDB_ORIGINAL_DATA_PATH)

    #Generate Todo
    LOG_DEBUG(2, "Generate todo")
    os.chdir(GIT_MASTERDB_PATH)
    shutil.rmtree(GIT_MASTERDB_PATH+"/pretranslate_todo", True)
    pretranslate_process.gen_todo("gakumasu-diff/json")
    os.chdir(ORIGIN_CWD)


    LOG_INFO(2, f"Converting {len(drive_file_paths)} MasterDB files")
    for abs_path, rel_path, filename in drive_file_paths:
        input_path = abs_path
        output_path = os.path.join(GIT_MASTERDB_PATH + "/pretranslate_todo/todo/new", filename[:-5]+"_translated.json")
        LOG_DEBUG(2, f"Start convert from drive to output '{input_path}' to '{output_path}'")
        try:
            XlsxToJson(input_path, output_path)
        except Exception as e:
            LOG_ERROR(2, f"Error during Convert MasterDB drive to output: {e}")
            logger.exception(e)

    LOG_INFO(2, f"Build MasterDB files")
    os.chdir(GIT_MASTERDB_PATH)
    pretranslate_process.IGNORE_INPUT = True
    pretranslate_process.merge_todo()
    os.chdir(ORIGIN_CWD)

    return drive_file_paths