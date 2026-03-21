
from datetime import datetime
import argparse, io, os

from scripts import rclone, adv, masterdb2, generic, localization
from scripts.log import *

full_update = False
CONVERT = True
UPDATE = True

def Convert(ADV=True, MASTERDB=True, GENERIC=True, LOCALIZATION=True, bFullUpdate=False, changed_files=None):
    if changed_files is None:
        changed_files = {}
    ERR_ADV_FILE = []
    ADV_FILE = []
    ERR_MASTERDB_FILE = []
    MASTERDB_FILE = []
    ERR_GENERIC_FILE = []
    GENERIC_FILE = []
    ERR_LOCALIZATION_FILE = []
    LOCALIZATION_FILE = []
    if ADV:
        LOG_INFO(1, "Converting ADV")
        adv_files = changed_files.get("adv")
        ERR_ADV_FILE, ADV_FILE = adv.ConvertDriveToOutput(adv_files, bFullUpdate)
    if MASTERDB:
        LOG_INFO(1, "Converting MasterDB")
        mdb_files = changed_files.get("masterdb")
        ERR_MASTERDB_FILE, MASTERDB_FILE = masterdb2.ConvertDriveToOutput(mdb_files, bFullUpdate)
    if GENERIC:
        LOG_INFO(1, "Converting Generic")
        gen_files = changed_files.get("generic")
        ERR_GENERIC_FILE, GENERIC_FILE = generic.ConvertDriveToOutput(gen_files, bFullUpdate)
    if LOCALIZATION:
        LOG_INFO(1, "Converting Localization")
        loc_files = changed_files.get("localization")
        ERR_LOCALIZATION_FILE, LOCALIZATION_FILE = localization.ConvertDriveToOutput(loc_files, bFullUpdate)

    if len(ADV_FILE) + len(MASTERDB_FILE) + len(GENERIC_FILE) + len(LOCALIZATION_FILE) > 0:
        LOG_INFO(1, "Write version.txt")
        with open("./output/version.txt", 'w', encoding='utf-8') as f:
            f.write(datetime.today().strftime("%Y%m%d_%H%M%S"))
    else:
        LOG_INFO(1, "No files updated")
    return (ERR_ADV_FILE, ADV_FILE), (ERR_MASTERDB_FILE, MASTERDB_FILE), (ERR_GENERIC_FILE, GENERIC_FILE), (ERR_LOCALIZATION_FILE, LOCALIZATION_FILE)

def Update(ADV=True, MASTERDB=True, bFullUpdate=False):
    ADV_FILE = []
    MASTERDB_FILE = []
    if ADV:
        LOG_INFO(1, "Updating ADV")
        ADV_FILE = adv.UpdateOriginalToDrive()
    if MASTERDB:
        LOG_INFO(1, "Updating MasterDB")
        MASTERDB_FILE = masterdb2.UpdateOriginalToDrive()

    return ADV_FILE, MASTERDB_FILE
    
def getDriveUrl(file):
    try:
        link = rclone.link(os.path.join(file[1], file[2])).replace("https://drive.google.com/open?id=", "https://docs.google.com/spreadsheets/d/")
        return f"[{file[1]}]({link})"
    except Exception as e:
        LOG_INFO(1, f"{e}")
        return file[1]

def _convert_summary(NAME, ARR):
    if len(ARR[0]) + len(ARR[1]) > 0:
        LOG_INFO(1, NAME)
        if len(ARR[0]) > 0:
            ARR[0].sort(key= lambda arr: arr[1])
            for fn in ARR[0]:
                LOG_INFO(2, f"변환 중 오류 {fn[1]} : {fn[0]}")
        if len(ARR[1]) > 0:
            ARR[1].sort()
            for fn in ARR[1]:
                LOG_INFO(2, f"{fn} 번역 갱신")

def _update_summary(NAME, ARR):
    if len(ARR) > 0:
        LOG_INFO(1, NAME)
        ARR.sort()
        for fn in ARR:
            if fn[0] == "*":
                LOG_INFO(2, f"업데이트 : '{getDriveUrl(fn)}'")
            if fn[0] == "+":
                LOG_INFO(2, f"추가 : '{getDriveUrl(fn)}'")
def main(ADV=True, MASTERDB=True, GENERIC=True, LOCALIZATION=True):
    from scripts import sync

    # Phase 0: Download from Google Drive
    LOG_INFO(0, "Phase 0: Download from Drive")
    changed_files = sync.download_all(full_update, ADV, MASTERDB, GENERIC, LOCALIZATION)

    # Phase 1: Convert (local drive → output)
    if CONVERT:
        LOG_INFO(0, "Phase 1: Convert")
        C_ADV_FILE, C_MASTERDB_FILE, C_GENERIC_FILE, C_LOCALIZATION_FILE = Convert(ADV, MASTERDB, GENERIC, LOCALIZATION, full_update, changed_files)

    # Phase 2: Update (submodule → local drive)
    # Phase 3: Upload to Google Drive
    U_UPLOAD_ADV = []
    U_UPLOAD_MASTERDB = []
    if UPDATE:
        LOG_INFO(0, "Phase 2: Update")
        Update(ADV, MASTERDB, full_update)

        LOG_INFO(0, "Phase 3: Upload to Drive")
        U_UPLOAD_ADV, U_UPLOAD_MASTERDB = sync.upload_all(ADV, MASTERDB)

    has_changes = False
    logStream = io.StringIO()
    logHandler = logging.StreamHandler(logStream)
    AddLogHandler(logHandler)
    if UPDATE:
        if len(U_UPLOAD_ADV) + len(U_UPLOAD_MASTERDB) > 0:
            has_changes = True
            LOG_INFO(0, "---------------- 업데이트된 파일 요약 ----------------")

            _update_summary("ADV", U_UPLOAD_ADV)
            _update_summary("MASTERDB", U_UPLOAD_MASTERDB)

            LOG_INFO(0, "----------------------------------------------------------")
        else:
            LOG_INFO(0, "No files updated")
    if CONVERT:
        if len(C_ADV_FILE[0]) + len(C_ADV_FILE[1]) + len(C_MASTERDB_FILE[0]) + len(C_MASTERDB_FILE[1]) + len(C_GENERIC_FILE[0]) + len(C_GENERIC_FILE[1]) + len(C_LOCALIZATION_FILE[0]) + len(C_LOCALIZATION_FILE[1]) > 0:
            has_changes = True
            LOG_INFO(0, "---------------- 번역 갱신된 파일 요약 ----------------")

            _convert_summary("ADV", C_ADV_FILE)
            _convert_summary("MASTERDB", C_MASTERDB_FILE)
            _convert_summary("GENERIC", C_GENERIC_FILE)
            _convert_summary("LOCALIZATION", C_LOCALIZATION_FILE)

            LOG_INFO(0, "----------------------------------------------------------")
        else:
            LOG_INFO(0, "No files converted")
    log_content = logStream.getvalue()
    logHandler.close()
    logStream.close()
    if has_changes:
        try:
            import scripts.gspread
            scripts.gspread.log(log_content)
        except Exception as e:
            LOG_ERROR(0, f"Failed to log to Google Sheets: {e}")
    


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--fullupdate', action='store_true')
    parser.add_argument('--DEBUG', action='store_true')
    parser.add_argument('--convert', action='store_true')
    parser.add_argument('--update', action='store_true')
    parser.add_argument('--adv', action='store_true')
    parser.add_argument('--masterdb', action='store_true')
    parser.add_argument('--generic', action='store_true')
    parser.add_argument('--localization', action='store_true')
    args = parser.parse_args()
    if args.fullupdate:
        full_update = True
    if args.DEBUG:
        logger.setLevel("DEBUG")
    else:
        logger.setLevel("INFO")
        import sys, os
        sys.stdout = open(os.devnull, 'w')
        rclone.logger.addHandler(RichHandler(console=Console(stderr=True)))
    handler = logging.FileHandler(f"output_python_{(datetime.today().strftime('%Y%m%d_%H%M%S'))}.log")
    logger.addHandler(handler)
    LOG_INFO(0, "Start scripts")
    if args.convert or args.update:
        CONVERT = False
        UPDATE = False
        if args.convert:
            CONVERT = True
        if args.update:
            UPDATE = True
    ADV = True
    MASTERDB = True
    GENERIC = True
    LOCALIZATION = True
    if args.adv or args.masterdb or args.generic or args.localization:
        ADV = False
        MASTERDB = False
        GENERIC = False
        LOCALIZATION = False
        if args.adv:
            ADV = True
        if args.masterdb:
            MASTERDB = True
        if args.generic:
            GENERIC = True
        if args.localization:
            LOCALIZATION = True


    main(ADV, MASTERDB, GENERIC, LOCALIZATION)