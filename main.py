
from datetime import datetime
import argparse

from scripts import rclone, adv, masterdb, generic, localization
from scripts.log import *

full_update = False
CONVERT = True
UPDATE = True

def Convert(bFullUpdate):
    LOG_INFO(1, "Converting ADV")
    ERR_ADV_FILE, ADV_FILE = adv.ConvertDriveToOutput(bFullUpdate)
    LOG_INFO(1, "Converting MasterDB")
    ERR_MASTERDB_FILE, MASTERDB_FILE = masterdb.ConvertDriveToOutput(bFullUpdate)
    LOG_INFO(1, "Converting Generic")
    ERR_GENERIC_FILE, GENERIC_FILE = generic.ConvertDriveToOutput(bFullUpdate)
    LOG_INFO(1, "Converting Localization")
    ERR_LOCALIZATION_FILE, LOCALIZATION_FILE = localization.ConvertDriveToOutput(bFullUpdate)

    if len(ADV_FILE) + len(MASTERDB_FILE) + len(GENERIC_FILE) + len(LOCALIZATION_FILE) > 0:
        LOG_INFO(1, "Write version.txt")
        with open("./output/version.txt", 'w', encoding='utf-8') as f:
            f.write(datetime.today().isoformat(" "))
    else:
        LOG_INFO(1, "No files updated")
    return (ERR_ADV_FILE, ADV_FILE), (ERR_MASTERDB_FILE, MASTERDB_FILE), (ERR_GENERIC_FILE, GENERIC_FILE), (ERR_LOCALIZATION_FILE, LOCALIZATION_FILE)

def Update(bFullUpdate):
    LOG_INFO(1, "Updating ADV")
    ADV_FILE = adv.UpdateOriginalToDrive()
    LOG_INFO(1, "Updating MasterDB")
    MASTERDB_FILE = masterdb.UpdateOriginalToDrive()

    return ADV_FILE, MASTERDB_FILE
    
    

def _convert_summary(NAME, ARR):
    if len(ARR[0]) + len(ARR[1]) > 0:
        LOG_INFO(1, NAME)
        if len(ARR[0]) > 0:
            ARR[0].sort(key= lambda arr: arr[1])
            for fn in ARR[0]:
                LOG_INFO(2, f"Error during convert {fn[1]} : {fn[0]}")
        if len(ARR[1]) > 0:
            ARR[1].sort()
            for fn in ARR[1]:
                LOG_INFO(2, f"Convert {fn} to output")

def _update_summary(NAME, ARR):
    if len(ARR) > 0:
        LOG_INFO(1, NAME)
        ARR.sort()
        for fn in ARR:
            if fn[0] == "*":
                LOG_INFO(2, f"Update '{fn[1]}' file to remote")
            if fn[0] == "+":
                LOG_INFO(2, f"Add '{fn[1]}' file to remote")
def main():
    if CONVERT:
        LOG_INFO(0, "Start convert")
        C_ADV_FILE, C_MASTERDB_FILE, C_GENERIC_FILE, C_LOCALIZATION_FILE = Convert(full_update)
    if UPDATE:
        LOG_INFO(0, "Start update")
        U_ADV_FILE, U_MASTERDB_FILE = Update(full_update)
    
    if CONVERT:
        if C_ADV_FILE != None and len(C_ADV_FILE[0]) + len(C_ADV_FILE[1]) + len(C_MASTERDB_FILE[0]) + len(C_MASTERDB_FILE[1]) + len(C_GENERIC_FILE) + len(C_LOCALIZATION_FILE) > 0:
            LOG_INFO(0, "---------------- Summary of converted files ----------------")
            
            _convert_summary("ADV", C_ADV_FILE)
            _convert_summary("MASTERDB", C_MASTERDB_FILE)
            _convert_summary("GENERIC", C_GENERIC_FILE)
            _convert_summary("LOCALIZATION", C_LOCALIZATION_FILE)
            
            LOG_INFO(0, "----------------------------------------------------------")
        else:
            LOG_INFO(0, "No files converted")
    if UPDATE:
        if U_ADV_FILE != None and len(U_ADV_FILE) + len(U_MASTERDB_FILE) > 0:
            LOG_INFO(0, "---------------- Summary of updated files ----------------")

            _update_summary("ADV", U_ADV_FILE)
            _update_summary("MASTERDB", U_MASTERDB_FILE)

            LOG_INFO(0, "----------------------------------------------------------")
        else:
            LOG_INFO(0, "No files updated")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--fullupdate', action='store_true')
    parser.add_argument('--DEBUG', action='store_true')
    parser.add_argument('--convert', action='store_true')
    parser.add_argument('--update', action='store_true')
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
    LOG_INFO(0, "Start scripts")
    if args.convert or args.update:
        CONVERT = False
        UPDATE = False
        if args.convert:
            CONVERT = True
        if args.update:
            UPDATE = True
    main()