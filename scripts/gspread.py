from .log import *
import gspread
import datetime

TARGET_SHEET = "1gjYXr-aFrDLLXUfmsA-tN_Tc5rgovtIfeDqoJM-78jM"
SHEET = None

def load():
    account = gspread.service_account(r"api.json")
    SHEET = account.open_by_key(TARGET_SHEET)

def log(logs):
    if SHEET == NONE:
        return
    worksheet = SHEET.worksheet("list")
    worksheet.insert_cols([[]],1)
    worksheet.update([
            [str(datetime.datetime.now()) + " 업데이트 기록"],
            [logs]
        ], 'A2:A3')
    worksheet.format("A2", {
        "backgroundColor": {
        "red": 0.945,
        "green": 0.760,
        "blue": 0.196
        },
        "horizontalAlignment": "CENTER",
        "textFormat": {
        "foregroundColor": {
            "red": 0.0,
            "green": 0.0,
            "blue": 0.0
        },
        "fontSize": 18,
        "bold": True
        }
    })
    worksheet.format("A3", {
        "backgroundColor": {
        "red": 1.0,
        "green": 0.95,
        "blue": 0.8
        },
        "horizontalAlignment": "LEFT",
        "textFormat": {
        "foregroundColor": {
            "red": 0.0,
            "green": 0.0,
            "blue": 0.0
        },
        "fontSize": 12,
        "bold": False
        }
    })