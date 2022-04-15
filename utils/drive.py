import utils.config as config
import utils.system as system
import sys

def drive():
    gsheet = config.gspread.authorize(config.credentials)

    listagemWorksheet = getSheet(masterSpreadsheet, masterSheet)
    logWorksheet = getSheet(masterSpreadsheet, masterLogSheet)
    nsdWorksheet = getSheet(masterSpreadsheet, masterNSDSheet)
