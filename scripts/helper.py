import os, re, json, subprocess
from datetime import date

from .log import *

DEFAULT_PATH = os.getcwd()
REMOTE_PATH = "gakumas:Gakumas_KR"
DRIVE_PATH = DEFAULT_PATH + "/res/drive"
TEMP_PATH = DEFAULT_PATH + "/temp"
OUTPUT_PATH = DEFAULT_PATH + "/output"
GIT_MASTERDB_PATH = DEFAULT_PATH + "/res/masterdb"

CHARACTER_REGEX_TRANS_MAP = {
    "麻央":"마오",
    "燕":"츠바메",
    "ことね":"코토네",
    "極月学園":"극월학원",
    "美鈴":"미스즈",
    "莉波":"리나미",
    "咲季":"사키",
    "佑芽":"우메",
    "邦夫":"쿠니오",
    "星南":"세나",
    "千奈":"치나",
    "リーリヤ":"릴리야",
    "あさり先生":"아사리 선생",
    "広":"히로",
    "清夏":"스미카",
    "ダンストレーナー":"댄스 트레이너",
    "ビジュアルトレーナー":"비주얼 트레이너",
    "ボーカルトレーナー":"보컬 트레이너",
    "手毬":"테마리",
}

XLSX_NAME_FORMAT = {'font_name': 'Calibri','bold':False, 'text_wrap':True, 'align':'center', 'valign':'top', 'border':1}
XLSX_TEXT_FORMAT = {'font_name': 'Calibri','bold':False, 'text_wrap':True, 'align':'left', 'valign':'top', 'border':1}

REGEX_DOTS_4_TO_6 = re.compile('\.{4,6}')
REGEX_DOTS_3 = re.compile('\.{2,3}')

"""
Get all files from dir
Output : [
    (Absoulute path, Relative path)
]
"""
def Helper_GetFilesFromDir(path:str, suffix:str = None, prefix:str = None) -> list[str]:
    finds : list[str] = []
    for root_path, _, files in os.walk(path):
        for file in files:
            if suffix != None and not file.endswith(suffix):
                continue
            if prefix != None and not file.startswith(prefix):
                continue
            file_path = os.path.join(root_path, file)
            relate_path = os.path.relpath(file_path, os.getcwd())
            finds.append((file_path, relate_path, file))
    return finds   


"""
Get all files from dir based on date
Output : [
    (Absoulute path, Relative path)
]
"""
def Helper_GetFilesFromDirByDate(target_date:str, path:str, suffix:str = None, prefix:str = None, branch:str='origin/main') -> list[str]:
    _ORIGINAL_ROOT = os.getcwd()
    CMDS = f"git rev-list --since='{target_date}' --until='{date.today()}' {branch}"
    commits = subprocess.check_output(CMDS, shell=True, text=True, cwd=path).split("\n")
    result = []
    for commit in commits:
        if commit == "": continue
        LOG_DEBUG(3, f"Run commands git diff for commit {commit}~ from '{path}'")
        result += subprocess.check_output(f"git diff --name-only {commit}~", shell=True, text=True, cwd=path).split("\n")
    LOG_DEBUG(3, f"Updated file list : {result}")

    finds : list[str] = []
    for relate_path in result:
        if relate_path == "" or relate_path == "revision": continue
        file_path = os.path.join(path, relate_path)
        relate_path = os.path.relpath(file_path, os.getcwd())
        finds.append((file_path, relate_path, os.path.basename(relate_path)))
    return finds   

"""
Get all files from dir based on rclone check
Output : [
    (Absoulute path, Relative path)
]
"""
def Helper_GetFilesFromDirByCheck(check_result:list, path:str, suffix:str = None, prefix:str = None) -> list[str]:
    finds : list[str] = []
    for diff, relate_path in check_result:
        if diff == "-": continue
        file_path = os.path.join(path, relate_path)
        relate_path = os.path.relpath(file_path, os.getcwd())
        file_name:str = os.path.basename(relate_path)
        if prefix != None and not file_name.startswith(prefix): continue
        if suffix != None and not file_name.endswith(suffix): continue
        finds.append((file_path, relate_path, file_name))
    return finds   

MDB = {}
def Helper_LoadMasterDB():
    fs : list[str] = Helper_GetFilesFromDir(GIT_MASTERDB_PATH + "/pretranslate_todo/todo", '.json')
    for abs_path, rel_path, file in fs:
        if "/new/" in rel_path:
            continue
        file_path = os.path.join(GIT_MASTERDB_PATH + "/pretranslate_todo/todo/", file)
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        for k,v in data.items():
            if v == "":
                continue
            MDB[k] = v