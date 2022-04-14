import utils.config as config
import utils.system as system
import sys

def drive()
    gsheet = gspread.authorize(credentials)

    listagemWorksheet = getSheet(masterSpreadsheet, masterSheet)
    logWorksheet = getSheet(masterSpreadsheet, masterLogSheet)
    nsdWorksheet = getSheet(masterSpreadsheet, masterNSDSheet)
