from datetime import datetime

from scripts import adv
from scripts import masterdb
from scripts import generic
from scripts import localization
from scripts.log import *


def Convert(bFullUpdate):
    LOG_INFO(1, "Converting ADV")
    ADV_FILE:list = adv.ConvertDriveToOutput(bFullUpdate)
    LOG_INFO(1, "Converting MasterDB")
    MASTERDB_FILE:list = masterdb.ConvertDriveToOutput(bFullUpdate)
    LOG_INFO(1, "Converting Generic")
    GENERIC_FILE:list = []
    LOG_INFO(1, "Converting Localization")
    LOCALIZATION_FILE:list = []

    if len(ADV_FILE) + len(MASTERDB_FILE) + len(GENERIC_FILE) + len(LOCALIZATION_FILE) > 0:
        LOG_INFO(1, "Write version.txt")
        with open("./output/version.txt", 'w', encoding='utf-8') as f:
            f.write(datetime.today().isoformat(" "))
    else:
        LOG_INFO(1, "No files updated")
    return ADV_FILE, MASTERDB_FILE, GENERIC_FILE, LOCALIZATION_FILE

def Update(bFullUpdate):
    LOG_INFO(1, "Updating ADV")
    ADV_FILE:list = adv.UpdateOriginalToDrive(bFullUpdate)
    LOG_INFO(1, "Updated ADV")
    LOG_INFO(1, "Updating MasterDB")
    MASTERDB_FILE:list = masterdb.UpdateOriginalToDrive(bFullUpdate)
    LOG_INFO(1, "Updating Generic")
    GENERIC_FILE:list = []
    LOG_INFO(1, "Updating Localization")
    LOCALIZATION_FILE:list = []

    return ADV_FILE, MASTERDB_FILE, GENERIC_FILE, LOCALIZATION_FILE
    
    

def main():
    LOG_INFO(0, "Start convert")
    C_ADV_FILE, C_MASTERDB_FILE, C_GENERIC_FILE, C_LOCALIZATION_FILE = Convert(False)
    LOG_INFO(0, "Start update")
    U_ADV_FILE, U_MASTERDB_FILE, U_GENERIC_FILE, U_LOCALIZATION_FILE = Update(False)
    if len(C_ADV_FILE) + len(C_MASTERDB_FILE) + len(C_GENERIC_FILE) + len(C_LOCALIZATION_FILE) > 0:
        LOG_INFO(0, "---------------- Summary of converted files ----------------")
        if len(C_ADV_FILE) > 0:
            LOG_INFO(1, "ADV")
        for fn in C_ADV_FILE:
            LOG_INFO(2, f"{fn[2]}")

        if len(C_MASTERDB_FILE) > 0:
            LOG_INFO(1, "MASTERDB")
        for fn in C_MASTERDB_FILE:
            LOG_INFO(2, f"{fn[2]}")

        if len(C_GENERIC_FILE) > 0:
            LOG_INFO(1, "GENERIC")
        for fn in C_GENERIC_FILE:
            LOG_INFO(2, f"{fn[2]}")

        if len(C_LOCALIZATION_FILE) > 0:
            LOG_INFO(1, "LOCALIZATION")
        for fn in C_LOCALIZATION_FILE:
            LOG_INFO(2, f"{fn[2]}")
        LOG_INFO(0, "----------------------------------------------------------")
    else:
        LOG_INFO(1, "No files updated")

    if len(U_ADV_FILE) + len(U_MASTERDB_FILE) + len(U_GENERIC_FILE) + len(U_LOCALIZATION_FILE) > 0:
        LOG_INFO(0, "---------------- Summary of updated files ----------------")
        if len(U_ADV_FILE) > 0:
            LOG_INFO(1, "ADV")
        for fn in U_ADV_FILE:
            LOG_INFO(2, f"{fn[1]}")

        if len(U_MASTERDB_FILE) > 0:
            LOG_INFO(1, "MASTERDB")
        for fn in U_MASTERDB_FILE:
            LOG_INFO(2, f"{fn[1]}")

        if len(U_GENERIC_FILE) > 0:
            LOG_INFO(1, "GENERIC")
        for fn in U_GENERIC_FILE:
            LOG_INFO(2, f"{fn[1]}")

        if len(U_LOCALIZATION_FILE) > 0:
            LOG_INFO(1, "LOCALIZATION")
        for fn in U_LOCALIZATION_FILE:
            LOG_INFO(2, f"{fn[1]}")
        LOG_INFO(0, "----------------------------------------------------------")
    else:
        LOG_INFO(1, "No files updated")


if __name__ == "__main__":
    logger.setLevel("DEBUG")
    LOG_INFO(0, "Start scripts")
    main()