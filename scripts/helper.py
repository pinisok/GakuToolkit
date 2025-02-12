import os
import re
from datetime import date
import subprocess

DEFAULT_PATH = "/app"
REMOTE_PATH = "gakumas:Gakumas_KR"
DRIVE_PATH = DEFAULT_PATH + "/res/drive"
TEMP_PATH = DEFAULT_PATH + "/temp"
OUTPUT_PATH = DEFAULT_PATH + "/output"

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
            relate_path = os.path.relpath(file_path, path)
            finds.append((file_path, relate_path, file))
    return finds   


"""
Get all files from dir based on date
Output : [
    (Absoulute path, Relative path)
]
"""
def Helper_GetFilesFromDirByDate(target_date:str, path:str, suffix:str = None, prefix:str = None) -> list[str]:
    _ORIGINAL_ROOT = os.getcwd()
    os.chdir(path)
    CMDS = f"""git rev-list --since='{target_date}' --until='{date.today()}' main | \
(head -n1 && tail -n1)                                        | \
tr '\n' ' '                                                   | \
sed 's/ /../'                                                 | \
xargs git diff --name-only"""
    result = subprocess.check_output(CMDS, shell=True, text=True).split("\n")
    os.chdir(_ORIGINAL_ROOT)
    finds : list[str] = []
    for relate_path in result:
        if relate_path == "" or relate_path == "revision": continue
        file_path = os.path.join(path, relate_path)
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
        finds.append((file_path, relate_path, os.path.basename(relate_path)))
    return finds   