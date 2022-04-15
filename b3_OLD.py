"""Grab financial data from brazilian B3 site and organize them in free Google Sheets

Project has the following stages
1. From starting page get, clean and treat full list of companies and update a General Purpose google Spreadsheet GPS
2. For each bovespa in GPS,
2.a. Create Company coogle Spreadsheet, get specific bovespa data and update CS info
2.b. Get Company List of Reports CLR available and save CLR list in CS
3. For each report page of each bovespa, get each statement type data,
3.a. Combine them by quarter
3.b. Create a sheet for each quarter in CS and save raw numbers
4. Get and combine all quarters and all statements in a Reports sheet in CS
5. Clean and treat, and pivot data by quarter and by year in news sheets in CS
"""
# IMPORTS =========================================================================================
# rightNow()
from datetime import datetime  # date.now()

# pick
# pip install pick #(pick-1.2.0 windows-curses-2.3.0)
from pick import pick

# startBrowser()
# pip install -U selenium # (async-generator-1.10 attrs-21.4.0 certifi-2021.10.8 cffi-1.15.0 cryptography-36.0.1 h11-0.13.0 idna-3.3 outcome-1.1.0 pyOpenSSL-21.0.0 pycparser-2.21 selenium-4.1.0 six-1.16.0 sniffio-1.2.0 sortedcontainers-2.4.0)
# pip install webdriver-manager # (charset-normalizer-2.0.12 colorama-0.4.4 configparser-5.2.0 crayons-0.4.0 requests-2.27.1 webdriver-manager-3.5.3)

from selenium import webdriver  # webdriver
from selenium.webdriver.chrome.service import Service  # chrome service
from selenium.webdriver.support.ui import WebDriverWait  # browser wait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import StaleElementReferenceException
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import Select
from webdriver_manager.chrome import ChromeDriverManager


# googleAPI()
# pip install --upgrade google-api-python-client google-auth-httplib2 google-auth-oauthlib # (cachetools-4.2.4 charset-normalizer-2.0.10 google-api-core-2.4.0 google-api-python-client-2.36.0 google-auth-2.3.3 google-auth-httplib2-0.1.0 google-auth-oauthlib-0.4.6 googleapis-common-protos-1.54.0 httplib2-0.20.2 oauthlib-3.1.1 protobuf-3.19.3 pyasn1-0.4.8 pyasn1-modules-0.2.8 pyparsing-3.0.7 requests-2.27.1 requests-oauthlib-1.3.0 rsa-4.8 uritemplate-4.1.1)
# pip install gspread # (gspread-5.1.1)
from google.oauth2 import service_account  # Google Authentication
import gspread  # Python API for Google Sheets.

# pip install pandas # (numpy-1.22.3 pandas-1.4.1 python-dateutil-2.8.2 pytz-2021.3)
import pandas as pd
import numpy as np

# pip install gspread_dataframe # (gspread_dataframe-3.2.2)
from gspread_dataframe import get_as_dataframe, set_with_dataframe

# other imports
# pip install Unidecode # (Unidecode-1.3.4)
import unidecode
import re
import time
from copy import deepcopy
import socket
import csv
import random
import sys
import string

import fastload
# test from vscode in commit github
# system variations
def b31Company(updateTime):
    """full project entry point"""
    try:
        b3Companies = getCompanies()
        global cSpreadsheet

        for c, company in enumerate(b3Companies):
            if c < batch_companies and company[7] and len(company[21]) < 48 and datetime.strptime(company[21][:19], '%d/%m/%Y %H:%M:%S') < datetime.strptime(updateTime, '%d/%m/%Y %H:%M:%S'):  # and 'RELA' in company[21]
                cSpreadsheet = gsheet.open_by_url(company[7])
                company = companyUpdate(company)

                # companyQuarters = getCompanyQuarters(company)
                #
                # companyStatements = getCompanyStatements(company)
                #
                # bigData = companyBigData(company)

                # update action message for log
                action = '1 COMP'
                action = updateListagem(company, action)
        print('finish')
        browser.quit()
    except Exception as e:
        trouble(e, sys._getframe().f_code.co_name)
def b32CQuarters(updateTime):
    """full project entry point"""
    try:
        b3Companies = getCompanies()
        global cSpreadsheet

        for c, company in enumerate(b3Companies):
            if c < batch_companies and company[7] and '1 COMP' in company[21] and datetime.strptime(company[21][:19], '%d/%m/%Y %H:%M:%S') < datetime.strptime(updateTime, '%d/%m/%Y %H:%M:%S'):  # and 'RELA' in company[21]
                cSpreadsheet = gsheet.open_by_url(company[7])
                # company = companyUpdate(company)
                companyQuarters = getCompanyQuarters(company)

                # companyStatements = getCompanyStatements(company)
                #
                # bigData = companyBigData(company)

                # update action message for log
                action = '2 QUAR'
                action = updateListagem(company, action)
        print('finish')
        browser.quit()
    except Exception as e:
        trouble(e, sys._getframe().f_code.co_name)
def b33CStatements(updateTime):
    """full project entry point"""
    try:
        b3Companies = getCompanies()
        global cSpreadsheet

        for c, company in enumerate(b3Companies):
            if c < batch_companies and company[7] and '2 QUAR' in company[21] and datetime.strptime(company[21][:19], '%d/%m/%Y %H:%M:%S') < datetime.strptime(updateTime, '%d/%m/%Y %H:%M:%S'):  # and 'RELA' in company[21]
                cSpreadsheet = gsheet.open_by_url(company[7])
                # company = companyUpdate(company)
                # companyQuarters = getCompanyQuarters(company)

                companyStatements = getCompanyStatements(company)

                # bigData = companyBigData(company)

                # update action message for log
                action = '3 STAT'
                action = updateListagem(company, action)
        print('finish')
        browser.quit()
    except Exception as e:
        trouble(e, sys._getframe().f_code.co_name)
def b33CStatementsFIXITUP(updateTime):
    """full project entry point"""
    try:
        b3Companies = getCompanies()
        global cSpreadsheet

        companies = []
        for c, company in enumerate(b3Companies):
            if '2 QUAR' in company[21]:
                companies.append(company)

        for c, company in enumerate(companies):
            if c < 1 and company[7] and '2 QUAR' in company[21] and datetime.strptime(company[21][:19], '%d/%m/%Y %H:%M:%S') < datetime.strptime(updateTime, '%d/%m/%Y %H:%M:%S'):  # and 'RELA' in company[21]
                cSpreadsheet = gsheet.open_by_url(company[7])
                # company = companyUpdate(company)
                # companyQuarters = getCompanyQuarters(company)

                companyStatements = getCompanyStatementsFIXITUP(company)

                # bigData = companyBigData(company)

                # update action message for log
                action = '3 STAT'
                action = updateListagem(company, action)
        if companies:
            b33CStatementsFIXITUP(updateTime)
        print('finish')
        browser.quit()
    except Exception as e:
        trouble(e, sys._getframe().f_code.co_name)
def b34Fundamentalist(updateTime):
    """full project entry point"""
    try:
        b3Companies = getCompanies()
        global cSpreadsheet

        for c, company in enumerate(b3Companies):
            if c < batch_companies and company[7] and '3 STAT' in company[21] and datetime.strptime(company[21][:19], '%d/%m/%Y %H:%M:%S') < datetime.strptime(updateTime, '%d/%m/%Y %H:%M:%S'):  # and 'RELA' in company[21]
                cSpreadsheet = gsheet.open_by_url(company[7])
                # company = companyUpdate(company)
                # companyQuarters = getCompanyQuarters(company)

                # companyStatements = getCompanyStatements(company)

                analytics = companyBigData(company)

                timestamp = sheetLog(company, ' ', sys._getframe().f_code.co_name)
                # update action message for log
                action = '4 FUND'
                action = updateListagem(company, action)
        print('finish')
        browser.quit()
    except Exception as e:
        trouble(e, sys._getframe().f_code.co_name)
def b35uberlista(updateTime):
    """full project entry point"""
    try:
        b3Companies = getCompanies()
        global cSpreadsheet

        global uberReport
        uberReport = []

        for c, company in enumerate(b3Companies):
            if c < batch_companies and company[7] and '4 FUND' in company[21] and datetime.strptime(company[21][:19], '%d/%m/%Y %H:%M:%S') < datetime.strptime(updateTime, '%d/%m/%Y %H:%M:%S'):  # and 'RELA' in company[21]
                cSpreadsheet = gsheet.open_by_url(company[7])
                # company = companyUpdate(company)
                uberReport = uberlista(company)

                # companyStatements = getCompanyStatements(company)
                #
                # bigData = companyBigData(company)

                # update action message for log
                action = '5 UBER'
                action = updateListagem(company, action)

        # adjust lines
        ultralista = []
        for r, report in enumerate(uberReport):
            for l, line in enumerate(report):
                ultralista.append(line)

        # sorting
        ultralista.sort(key=lambda x: x[0], reverse=False)
        ultralista.sort(key=lambda x: datetime.strptime(x[4], '%d/%m/%Y'), reverse=True)
        ultralista.sort(key=lambda x: x[6], reverse=False)

        uberReportsSheet = getSheet(uberSpreadsheet, 'reports')

        # update uberlista
        cell_list = sheetUpdate(uberReportsSheet, ultralista, 2, 'USER_ENTERED')

        print('finish')
        browser.quit()
    except Exception as e:
        trouble(e, sys._getframe().f_code.co_name)
def b36Full(updateTime):
    """full project entry point"""
    try:
        b3Companies = getCompanies()
        global cSpreadsheet

        for c, company in enumerate(b3Companies):
            if c < batch_companies and company[7] and len(company[21]) < 48 and datetime.strptime(company[21][:19], '%d/%m/%Y %H:%M:%S') < datetime.strptime(updateTime, '%d/%m/%Y %H:%M:%S'):  # and 'RELA' in company[21]
                cSpreadsheet = gsheet.open_by_url(company[7])
                company = companyUpdate(company)

                # companyQuarters = getCompanyQuarters(company)
                #
                # companyStatements = getCompanyStatements(company)
                #
                # bigData = companyBigData(company)

                # update action message for log
                action = '1 COMP'
                action = updateListagem(company, action)
        for c, company in enumerate(b3Companies):
            if c < batch_companies and company[7] and '1 COMP' in company[21] and datetime.strptime(company[21][:19], '%d/%m/%Y %H:%M:%S') < datetime.strptime(updateTime, '%d/%m/%Y %H:%M:%S'):  # and 'RELA' in company[21]
                cSpreadsheet = gsheet.open_by_url(company[7])
                # company = companyUpdate(company)
                companyQuarters = getCompanyQuarters(company, True)

                # companyStatements = getCompanyStatements(company)
                #
                # bigData = companyBigData(company)

                # update action message for log
                action = '2 QUAR'
                action = updateListagem(company, action)
        for c, company in enumerate(b3Companies):
            if c < batch_companies and company[7] and '2 QUAR' in company[21] and datetime.strptime(company[21][:19], '%d/%m/%Y %H:%M:%S') < datetime.strptime(updateTime, '%d/%m/%Y %H:%M:%S'):  # and 'RELA' in company[21]
                cSpreadsheet = gsheet.open_by_url(company[7])
                # company = companyUpdate(company)
                # companyQuarters = getCompanyQuarters(company)

                companyStatements = getCompanyStatements(company)

                # bigData = companyBigData(company)

                # update action message for log
                action = '3 STAT'
                action = updateListagem(company, action)
        for c, company in enumerate(b3Companies):
            if c < batch_companies and company[7] and '3 STAT' in company[21] and datetime.strptime(company[21][:19], '%d/%m/%Y %H:%M:%S') < datetime.strptime(updateTime, '%d/%m/%Y %H:%M:%S'):  # and 'RELA' in company[21]
                cSpreadsheet = gsheet.open_by_url(company[7])
                # company = companyUpdate(company)
                # companyQuarters = getCompanyQuarters(company)

                # companyStatements = getCompanyStatements(company)

                bigData = companyBigData(company)

                # update action message for log
                action = '4 FUND'
                action = updateListagem(company, action)

        print('finish')
        browser.quit()
    except Exception as e:
        trouble(e, sys._getframe().f_code.co_name)
def b36FullByCompany(updateTime):
    """full project entry point"""
    try:
        b3Companies = getCompanies()
        global cSpreadsheet

        for c, company in enumerate(b3Companies):
            if c < batch_companies and company[7] and len(company[21]) < 48 and datetime.strptime(company[21][:19], '%d/%m/%Y %H:%M:%S') < datetime.strptime(updateTime, '%d/%m/%Y %H:%M:%S'):  # and 'RELA' in company[21]
                cSpreadsheet = gsheet.open_by_url(company[7])
                company = companyUpdate(company)

                # companyQuarters = getCompanyQuarters(company)
                #
                # companyStatements = getCompanyStatements(company)
                #
                # bigData = companyBigData(company)

                # update action message for log
                action = '1 COMP'
                action = updateListagem(company, action)

            if c < batch_companies and company[7] and '1 COMP' in company[21] and datetime.strptime(company[21][:19], '%d/%m/%Y %H:%M:%S') < datetime.strptime(updateTime, '%d/%m/%Y %H:%M:%S'):  # and 'RELA' in company[21]
                cSpreadsheet = gsheet.open_by_url(company[7])
                # company = companyUpdate(company)
                companyQuarters = getCompanyQuarters(company, True)

                # companyStatements = getCompanyStatements(company)
                #
                # bigData = companyBigData(company)

                # update action message for log
                action = '2 QUAR'
                action = updateListagem(company, action)

            if c < batch_companies and company[7] and '2 QUAR' in company[21] and datetime.strptime(company[21][:19], '%d/%m/%Y %H:%M:%S') < datetime.strptime(updateTime, '%d/%m/%Y %H:%M:%S'):  # and 'RELA' in company[21]
                cSpreadsheet = gsheet.open_by_url(company[7])
                # company = companyUpdate(company)
                # companyQuarters = getCompanyQuarters(company)

                companyStatements = getCompanyStatements(company)

                # bigData = companyBigData(company)

                # update action message for log
                action = '3 STAT'
                action = updateListagem(company, action)

            if c < batch_companies and company[7] and '3 STAT' in company[21] and datetime.strptime(company[21][:19], '%d/%m/%Y %H:%M:%S') < datetime.strptime(updateTime, '%d/%m/%Y %H:%M:%S'):  # and 'RELA' in company[21]
                cSpreadsheet = gsheet.open_by_url(company[7])
                # company = companyUpdate(company)
                # companyQuarters = getCompanyQuarters(company)

                # companyStatements = getCompanyStatements(company)

                bigData = companyBigData(company)

                # update action message for log
                action = '4 FUND'
                action = updateListagem(company, action)

        print('finish')
        browser.quit()
    except Exception as e:
        trouble(e, sys._getframe().f_code.co_name)
def b7NSD(updateTime, lenght):
    """get NSD list of documents"""
    try:
        reportsSheet = nsdWorksheet.get_all_values()
        headers = reportsSheet.pop(0)
        nsdUnique = [int(x[1]) for x in reportsSheet]
        nsdUnique = list(dict.fromkeys(nsdUnique))

        size = len(reportsSheet)
        reports = []

        nsdSeq = [int(x) for x in range(0+1, size+lenght+1) if x not in nsdUnique]

        for n, nsd in enumerate(nsdSeq):
            if n < lenght:
                nsd = int(nsd)
                url = nsdURL1 + str(nsd) + nsdURL2
                browser.get(url)

                companyXpath = '//*[@id="lblNomeCompanhia"]'
                textXpath = '//*[@id="lblDescricaoCategoria"]'
                quarterXpath = '//*[@id="lblDataDocumento"]'

                reports.append([])
                reports[n].append(url)
                reports[n].append(nsd)
                try:
                    company = browser.find_element(By.XPATH, companyXpath).text

                    company = wait2.until(EC.element_to_be_clickable((By.XPATH, companyXpath))).text.upper()
                    text = wait2.until(EC.element_to_be_clickable((By.XPATH, textXpath))).text
                    text0 = text.split(' - ')[0]
                    text1 = text.split(' - ')[1]
                    text2 = text.split(' - ')[2]

                    reports[n].append(text1)
                    reports[n].append(text0)
                    reports[n].append(text2)
                    reports[n].append(company)
                except Exception as e:
                    reports[n].append('')
                    reports[n].append('')
                    reports[n].append('')
                    reports[n].append('NENHUMA COMPANHIA')

                print(n, reports[n])
        reports.sort(key=lambda x: x[1], reverse=False)  # original is [21] False

        counter = 0
        try:
            while reports[-1][5] == 'NENHUMA COMPANHIA':
                del reports[-1]
                counter += 1
        except:
            pass

        for x in reports:
            x[5] = unidecode.unidecode(x[5])
            x[5] = x[5].translate(str.maketrans('', '', string.punctuation))
            x[5] = x[5].upper().strip()

        cell_list = nsdWorksheet.append_rows(reports, value_input_option='RAW', insert_data_option='INSERT_ROWS')

        # compatibility info
        company = ['Company Report List', '', '', '', '', '', '', 'https://docs.google.com/spreadsheets/d/1ICxStWnMlDD7ttW8mn4CyC9YYKAm7t2AI3wz-pK3SZU/edit#gid=1290770217']
        # log
        timestamp = sheetLog(company, '0 NSD', sys._getframe().f_code.co_name)

        print('finish')
        return counter
    except Exception as e:
        trouble(e, sys._getframe().f_code.co_name)
def b7NSDCompanies(updateTime):
    """get NSD list of unique companies"""
    try:
        nsdSheet = nsdWorksheet.get_all_values()
        headers = nsdSheet.pop(0)
        companiesNSD = [unidecode.unidecode(x[5]).translate(str.maketrans('', '', string.punctuation)).upper() for x in nsdSheet if x[5] != 'NENHUMA COMPANHIA' and x[3] in referenceStatements]
        companiesNSD = list(dict.fromkeys(companiesNSD))
        
        listagemSheet = listagemWorksheet.get_all_values()
        headers = listagemSheet.pop(0)
        companiesSheet = [unidecode.unidecode(x[5]).translate(str.maketrans('', '', string.punctuation)).upper() for x in listagemSheet]
        companiesSheet = list(dict.fromkeys(companiesSheet))


        b3Companies = listSubtract(companiesNSD, companiesSheet)
        b3Companies = listMerge(b3Companies, companiesSheet)

        print(len(companiesSheet), 'of', len(b3Companies))
        for c, company in enumerate(b3Companies):
            # first get company new info
            empty = 'NONE'
            company = [company, empty, '', empty, '', company, '', '', '', empty, empty, empty, '', '', '', '', '', '', '', '', '', '']
            time.sleep(sleep)
            company = companyWebInfo(company, listedCompaniesURL)  # original

            # then search in b3 listed companies, create sheet or get company data
            cell = listagemWorksheet.find(company[5])
            if cell == None:
                company = companySheetCreate(company, companyTemplateSpreadsheet) # original
                print('company sheet created', c, company[5])
            else:
                # update listagemWorksheet company row
                start_col = 1
                start_row = cell.row
                end_row = start_row
                end_col = len(company)
                range = sheetCol(start_col) + str(start_row) + ':' + sheetCol(end_col) + str(end_row)
                cell_list = listagemWorksheet.range(range)
                for cell in cell_list:
                    try:
                        r = cell.row - start_row
                        c = cell.col - start_col
                        cell.value = str(list[r][c]).strip()
                    except:
                        pass
                listagemWorksheet.update_cells(cell_list, value_input_option='USER_ENTERED')

                print('company sheet found', c, company[5]) # fast debug
            # get fresh company info
            company = companySheetUpdate(company)  # original

            # log
            timestamp = sheetLog(company, '1 COMP', sys._getframe().f_code.co_name)
            time.sleep(sleep)
        print('done b7NSDCompanies')
    except Exception as e:
        trouble(e, sys._getframe().f_code.co_name)
def nsdIndexUpdate(company, reportIndex):
    """update Index at company sheet"""
    try:
        # open worksheet index from companySpreadshet
        cSpreadsheet = gsheet.open_by_url(company[7])
        cWorksheet = cSpreadsheet.worksheet('index')
        cIndex = cWorksheet.get_all_values()
        cWorksheet.resize(7, 6)
        time.sleep(sleep)

        cIndexHeader = cIndex[:6]
        cIndex = cIndex[6:]

        # prepare and raw update indexex
        for l, line in enumerate(cIndex):
            while len(cIndex[l]) > 5:
                del cIndex[l][-1]
        for l, line in enumerate(reportIndex):
            while len(reportIndex[l]) > 5:
                del reportIndex[l][-1]
        # sort
        for l, line in enumerate(reportIndex):
            try:
                line[2] = datetime.strptime(line[2], '%d/%m/%Y')
            except:
                line[2] = datetime.strptime('01/01/' + line[2], '%d/%m/%Y')
        reportIndex.sort(key=lambda x: int(x[1]), reverse=True)
        reportIndex.sort(key=lambda x: x[2], reverse=True)
        for l, line in enumerate(reportIndex):
            line[2] = line[2].strftime('%d/%m/%Y')

        # 1 update index
        cell_list = sheetUpdate(cWorksheet, reportIndex, 7, 'USER_ENTERED')

        # 2 uppdate quarters
        quartersSheets = []
        for worksheet in cSpreadsheet.worksheets():
            quartersSheets.append(worksheet.title)
        quartersSheets = listSubtract(quartersSheets, defaultsSheets)

        newQuarters = listSubtract(reportIndex, cIndex)
        quarters = [x[2] for x in newQuarters if x[3] in referenceStatements]
        quarters = list(dict.fromkeys(quarters))
        for quarter in quarters:
            # print('open company spreadshhet, try open and clean create worksheet', quarter)
            if quarter in quartersSheets:
                try:
                    cQuarterWorksheet = cSpreadsheet.worksheet(quarter)
                    cSpreadsheet.del_worksheet(cQuarterWorksheet)
                    time.sleep(sleep)
                except:
                    pass

        # 3 update reports
        # time.sleep(sleep)
        cReportsWorksheet = cSpreadsheet.worksheet('reports')
        cReports = cReportsWorksheet.get_all_values()
        headers = cReports.pop(0)
        # remove updated reports from bigreport agreggator
        cReportsWorksheet.resize(2, 8)
        time.sleep(sleep)
        if cReports:
            cReports = [line for line in cReports if line[4] not in quarters]
            cell_list = sheetUpdate(cReportsWorksheet, cReports, 2, 'RAW')
            time.sleep(sleep)

        return reportIndex, str(cWorksheet.id)
    except Exception as e:
            trouble(e, sys._getframe().f_code.co_name)
def b7NSDIndex(updateTime):
    """get NSD list of unique companies"""
    try:
        listagemSheet = listagemWorksheet.get_all_values()
        headers = listagemSheet.pop(0)
        subtotal = len(listagemSheet)
        b3Companies = [x for x in listagemSheet]  #         b3Companies = [x for x in listagemSheet if '1 COMP' in x[21]]

        nsdSheet = nsdWorksheet.get_all_values()
        headers = nsdSheet.pop(0)
        nsdSheet = [x for x in nsdSheet if x[5] !='NENHUMA COMPANHIA']
        companiesSheet = [unidecode.unidecode(x[5]).translate(str.maketrans('', '', string.punctuation)).upper() for x in nsdSheet]
        companiesSheet = list(dict.fromkeys(companiesSheet))
        total = len(companiesSheet)

        print(len(b3Companies), 'of', subtotal, 'of', total, 'companies')
        # prepare superIndex
        superIndex = []
        for c, company in enumerate(b3Companies):
            if c % batch_table == 0:
                print(c, 'of', len(b3Companies))
            reportIndex = []
            for x in nsdSheet:
                if x[5] == company[5]:
                    reportIndex.append(x)
            if reportIndex:
                superIndex.append(reportIndex)

        # walk trought superIndex in companies
        for c, company in enumerate(b3Companies):
            if c < batch_companies and company[7] and '1 COMP' in company[21] and datetime.strptime(company[21][:19], '%d/%m/%Y %H:%M:%S') < datetime.strptime(updateTime, '%d/%m/%Y %H:%M:%S'):  # and 'RELA' in company[21]
                try:
                    nsdlog = int(company[22])
                except:
                    nsdlog = 0
                for r, reportIndex in enumerate(superIndex):
                    if company[5] in reportIndex[0]:
                        nsd = max(int(x[1]) for x in reportIndex)
                        if nsd > nsdlog:
                            # time.sleep(sleep*5)
                            reportIndex, id = nsdIndexUpdate(company, reportIndex)

                            # update listagem with max nsd log 'NSD ' + nsd
                            print('reports updated', r, company[5], company[7] + "/edit#gid=" + id)

                        else:
                            print('reports up to date', r, company[5], company[7])
                        """update in listagemWorksheet"""
                        timestamp = rightNow()
                        cell = listagemWorksheet.find(company[7])
                        row = cell.row
                        cell = 'W' + str(row)
                        update2 = listagemWorksheet.update(cell, nsd, value_input_option='RAW')

                        # log
                        timestamp = sheetLog(company, '2 QUAR', sys._getframe().f_code.co_name)
                        time.sleep(sleep)



        print('finish b7NSDIndex')

    except Exception as e:
        trouble(e, sys._getframe().f_code.co_name)
def b7NSDStatements(updateTime):
    """get statements for index for company"""
    try:
        # get index
        b3Companies = listagemWorksheet.get_all_values()
        headers = b3Companies.pop(0)
        subtotal = len(b3Companies)
        b3Companies = [x for x in b3Companies if '2 QUAR' in x[21]]
        random.shuffle(b3Companies)


        nsdSheet = nsdWorksheet.get_all_values()
        headers = nsdSheet.pop(0)
        nsdSheet = [x for x in nsdSheet if x[5] !='NENHUMA COMPANHIA']
        companiesSheet = [unidecode.unidecode(x[5]).translate(str.maketrans('', '', string.punctuation)).upper() for x in nsdSheet]
        companiesSheet = list(dict.fromkeys(companiesSheet))
        total = len(companiesSheet)

        print(len(b3Companies), 'of', subtotal, 'of', total, 'companies')
        for c, company in enumerate(b3Companies):
            if c < batch_companies and company[7] and '2 QUAR' in company[21] and company[22] and datetime.strptime(company[21][:19], '%d/%m/%Y %H:%M:%S') < datetime.strptime(updateTime, '%d/%m/%Y %H:%M:%S'):  # and 'RELA' in company[21]
            # if 'ENGIE BRASIL' in company[0]:
                cSpreadsheet = gsheet.open_by_url(company[7])
                # company = companyUpdate(company)
                # companyQuarters = getCompanyQuarters(company)

                companyStatements = getCompanyStatements(company)

                # bigData = companyBigData(company)

                # update action message for log
                timestamp = sheetLog(company, '3 STAT', sys._getframe().f_code.co_name)
        print('done b7NSDStatements')
        # translate is another different step not here
    except Exception as e:
        trouble(e, sys._getframe().f_code.co_name)
def updateListagem(company, action):
    """description"""
    # print(sys._getframe().f_code.co_name)
    try:
        timestamp = rightNow()
        cell = listagemWorksheet.find(company[7])
        row = cell.row
        cell = 'V' + str(row)
        update = listagemWorksheet.update(cell, timestamp + ' ' + action, value_input_option='USER_ENTERED')
        print(company[0], timestamp + ' ' + action)
        return action
    except Exception as e:
        trouble(e, sys._getframe().f_code.co_name)
def b7NSDFundamentalist(updateTime):
    """get statements for index for company"""
    try:
        # get index
        b3Companies = listagemWorksheet.get_all_values()
        subtotal = len(b3Companies)
        headers = b3Companies.pop(0)
        b3Companies = [x for x in b3Companies if '3 STAT' in x[21]]

        nsdSheet = nsdWorksheet.get_all_values()
        headers = nsdSheet.pop(0)
        nsdSheet = [x for x in nsdSheet if x[5] !='NENHUMA COMPANHIA']
        companiesSheet = [unidecode.unidecode(x[5]).translate(str.maketrans('', '', string.punctuation)).upper() for x in nsdSheet]
        companiesSheet = list(dict.fromkeys(companiesSheet))
        total = len(companiesSheet)

        global cSpreadsheet

        print(len(b3Companies), 'of', subtotal, 'of', total, 'companies')
        for c, company in enumerate(b3Companies):
            if c < batch_companies and company[7] and '3 STAT' in company[21] and datetime.strptime(company[21][:19], '%d/%m/%Y %H:%M:%S') < datetime.strptime(updateTime, '%d/%m/%Y %H:%M:%S'):  # and 'RELA' in company[21]
            # if 'ENGIE BRASIL' in company[0]:
                cSpreadsheet = gsheet.open_by_url(company[7])

                analytics = companyBigData(company)

                timestamp = sheetLog(company, ' ', sys._getframe().f_code.co_name)
                # update action message for log
                action = '4 FUND'
                action = updateListagem(company, action)

    except Exception as e:
        trouble(e, sys._getframe().f_code.co_name)
def formCadastral1():
    """download PDF from website for company"""
    # print(sys._getframe().f_code.co_name)
    try:
        companyData = []
        data = []

        # enter frame
        xpathFrame = '//*[@id="iFrameFormulariosFilho"]'
        iframe = wait.until(EC.element_to_be_clickable((By.XPATH, xpathFrame)))
        iframe = browser.find_elements(By.XPATH, xpathFrame)
        browser.switch_to.frame(iframe[0])

        # get Company Data
        companyData.append(wText('//*[@id="ctl00_cphPopUp_txtNomeEmpresarial"]'))  # NomeEmpresarial
        companyData.append(wText('//*[@id="ctl00_cphPopUp_txtCnpj"]'))   # CNPJ
        companyData.append(wText('//*[@id="ctl00_cphPopUp_txtCodigoCvm"]'))   # código CMV
        companyData.append(wText('//*[@id="ctl00_cphPopUp_txtDataConstituicao"]'))  # Data da Constituição
        data.append(wText('//*[@id="ctl00_cphPopUp_txtDataInicioSituacaoRegistroCvm"]'))   # Data de Início do Registro
        data.append(wText('//*[@id="ctl00_cphPopUp_txtSituacaoRegistroCvm"]'))   # Situação na CVM
        data.append(wText('//*[@id="ctl00_cphPopUp_txtPaisOrigem"]'))   # País de Origem
        data.append(wText('//*[@id="ctl00_cphPopUp_txtPaginaEmissorRedeMundialComputadores"]'))   # Página do Emissor da Rede Mundial de Computadores

        # get Company Data part 2
        click = wClick('//*[@id="ctl00_cphPopUp_tabMenuModelo_tabItem2"]')
        data.append(wText('//*[@id="ctl00_cphPopUp_txtTipoParticipante"]'))   # Tipo de Participante
        data.append(wText('//*[@id="ctl00_cphPopUp_txtCategoriaEmissor"]'))   # Categoria do Emissor
        data.append(wText('//*[@id="ctl00_cphPopUp_txtDataRegistroAtualCategoriaValor"]'))   # Data do Registro Atual da Categoria
        data.append(wText('//*[@id="ctl00_cphPopUp_txtSituacaoEmissor"]'))   # Situação do Emissor
        data.append(wText('//*[@id="ctl00_cphPopUp_txtDataInicioSituacao"]'))   # Data de Início da Situação
        data.append(wText('//*[@id="ctl00_cphPopUp_txtEspecieControleAcionario"]'))   # Espécie do Controle Acionário
        data.append(wText('//*[@id="ctl00_cphPopUp_txtDataUltimaAlteracaoControleAcionario"]'))   # Data da Última Alteração do Controle Acionário
        cdDia = wText('//*[@id="ctl00_cphPopUp_txtDia"]')   # Dia
        cdMes = wText('//*[@id="ctl00_cphPopUp_txtMes"]')   # Mês
        data.append(cdDia + '/' + cdMes)   # Encerramento do Exercício Social

        # get Company Data part 3
        click = wClick('//*[@id="ctl00_cphPopUp_tabMenuModelo_tabItem3"]')
        data.append(wText('//*[@id="ctl00_cphPopUp_txtSetorAtividade"]'))   # Setor de Atividade
        data.append(wText('//*[@id="ctl00_cphPopUp_txtDescricaoAtividadePrincipalEmpresa"]'))   # Descrição da Atividade Principal da Empresa

        # get Company Data part 4
        click = wClick('//*[@id="ctl00_cphPopUp_tabMenuModelo_tabItem4"]')
        canais = tableParser('/html/body/form/div[4]/div/div[2]/div[4]/div/table/tbody/tr')
        canaisDivulgacao = []
        for c, canal in enumerate(canais):
            try:
                canaisDivulgacao = ' '.join(canal[0].text.split())
                canaisDivulgacao = ' '.join(canal[1].text.split())
            except:
                pass

        # back to main frame
        browser.switch_to.parent_frame()

        companyData.append(data)
        return companyData

    except Exception as e:
        trouble(e, sys._getframe().f_code.co_name)
def formCadastral2():
    """download PDF from website for company"""
    # print(sys._getframe().f_code.co_name)
    try:
        companyData = []
        data = []

        # select option menu
        xpathQuadro = '//*[@id="cmbQuadro"]'
        selectBox = wait.until(EC.element_to_be_clickable((By.XPATH, xpathQuadro)))
        selectBox = Select(browser.find_element(By.XPATH, xpathQuadro))
        selectBox.select_by_visible_text('Endereço')

        # enter frame
        xpathFrame = '//*[@id="iFrameFormulariosFilho"]'
        iframe = wait.until(EC.element_to_be_clickable((By.XPATH, xpathFrame)))
        iframe = browser.find_elements(By.XPATH, xpathFrame)
        browser.switch_to.frame(iframe[0])

        # get address
        data.append(wText('//*[@id="ctl00_cphPopUp_txtLogradouroSede"]'))   # Logradouro
        data.append(wText('//*[@id="ctl00_cphPopUp_txtComplementoSede"]'))   # Complemento
        data.append(wText('//*[@id="ctl00_cphPopUp_txtBairroSede"]'))   # Bairro
        data.append(wText('//*[@id="ctl00_cphPopUp_txtMunicipioSede"]'))   # Município
        data.append(wText('//*[@id="ctl00_cphPopUp_txtUfSede"]'))   # UF
        data.append(wText('//*[@id="ctl00_cphPopUp_txtPaisSede"]'))   # País
        data.append(wText('//*[@id="ctl00_cphPopUp_txtCepSede"]'))   # CEP
        data.append(wText('//*[@id="ctl00_cphPopUp_txtEmailSede"]'))   # E-mail

        # back to main frame
        browser.switch_to.parent_frame()

        companyData.append(data)
        return companyData
    except Exception as e:
        trouble(e, sys._getframe().f_code.co_name)
def formCadastral3():
    """download PDF from website for company"""
    # print(sys._getframe().f_code.co_name)
    try:
        companyData = []
        data = []

        # select option menu
        xpathQuadro = '//*[@id="cmbQuadro"]'
        selectBox = wait.until(EC.element_to_be_clickable((By.XPATH, xpathQuadro)))
        selectBox = Select(browser.find_element(By.XPATH, xpathQuadro))
        selectBox.select_by_visible_text('Valores mobiliários')

        # enter frame
        xpathFrame = '//*[@id="iFrameFormulariosFilho"]'
        iframe = wait.until(EC.element_to_be_clickable((By.XPATH, xpathFrame)))
        iframe = browser.find_elements(By.XPATH, xpathFrame)
        browser.switch_to.frame(iframe[0])

        # get values mobiliarios
        data.append(wText(''))   #

        # back to main frame
        browser.switch_to.parent_frame()

        companyData.append(data)
        return companyData
    except Exception as e:
        trouble(e, sys._getframe().f_code.co_name)
def formCadastral4():
    """download PDF from website for company"""
    # print(sys._getframe().f_code.co_name)
    try:
        companyData = []
        data = []

        # select option menu
        xpathQuadro = '//*[@id="cmbQuadro"]'
        selectBox = wait.until(EC.element_to_be_clickable((By.XPATH, xpathQuadro)))
        selectBox = Select(browser.find_element(By.XPATH, xpathQuadro))
        selectBox.select_by_visible_text('Auditor')

        # enter frame
        xpathFrame = '//*[@id="iFrameFormulariosFilho"]'
        iframe = wait.until(EC.element_to_be_clickable((By.XPATH, xpathFrame)))
        iframe = browser.find_elements(By.XPATH, xpathFrame)
        browser.switch_to.frame(iframe[0])

        for i in range(1, 100, 2):  # two by two
            try:
                # '/html/body/div/table/tbody/tr[1]/td[2]/span'
                xpath = '/html/body/div/table/tbody/tr[' + str(i) + ']'
                test = browser.find_element(By.XPATH, xpath)
                aud = wText(xpath + '/td[2]/span')
                cnpj = wText(xpath + '/td[3]/span')
                cvm = wText(xpath + '/td[4]/span')
                jurisdicao = wText(xpath + '/td[5]/span')

                click = wClick(xpath + '/td[1]')
                # '/html/body/div/table/tbody/tr[2]/td[2]/div/div/table/tbody/tr/td[2]/table/tbody/tr[2]/td/table/tbody/tr[2]/td[2]/span'
                xpath = '/html/body/div/table/tbody/tr[' + str(i+1) + ']/'
                nome = wText(xpath + 'td[2]/div/div/table/tbody/tr/td[2]/table/tbody/tr[2]/td/table/tbody/tr[2]/td[2]/span')
                cpf = wText(xpath + 'td[2]/div/div/table/tbody/tr/td[2]/table/tbody/tr[2]/td/table/tbody/tr[2]/td[4]/span')
                inicio = wText(xpath + 'td[2]/div/div/table/tbody/tr/td[2]/table/tbody/tr[2]/td/table/tbody/tr[3]/td[2]/span')
                final = wText(xpath + 'td[2]/div/div/table/tbody/tr/td[2]/table/tbody/tr[2]/td/table/tbody/tr[3]/td[4]/span')
                data.append([aud, cnpj, cvm, jurisdicao, nome, cpf, inicio, final])

            except Exception as e:
                pass

        # back to main frame
        browser.switch_to.parent_frame()

        companyData.append(data)
        return companyData
    except Exception as e:
        trouble(e, sys._getframe().f_code.co_name)
def formCadastral5():
    """download PDF from website for company"""
    # print(sys._getframe().f_code.co_name)
    try:
        companyData = []
        data = []

        # select option menu
        xpathQuadro = '//*[@id="cmbQuadro"]'
        selectBox = wait.until(EC.element_to_be_clickable((By.XPATH, xpathQuadro)))
        selectBox = Select(browser.find_element(By.XPATH, xpathQuadro))
        selectBox.select_by_visible_text('Escriturador de ações')

        # enter frame
        xpathFrame = '//*[@id="iFrameFormulariosFilho"]'
        iframe = wait.until(EC.element_to_be_clickable((By.XPATH, xpathFrame)))
        iframe = browser.find_elements(By.XPATH, xpathFrame)
        browser.switch_to.frame(iframe[0])

        data.append(wText('//*[@id="ctl00_cphPopUp_tbDados"]/tr[2]/td'))  # Escriturador de ações

        # back to main frame
        browser.switch_to.parent_frame()

        companyData.append(data)
        return companyData
    except Exception as e:
        trouble(e, sys._getframe().f_code.co_name)
def formCadastral6():
    """download PDF from website for company"""
    # print(sys._getframe().f_code.co_name)
    try:
        companyData = []
        data = []

        # select option menu
        xpathQuadro = '//*[@id="cmbQuadro"]'
        selectBox = wait.until(EC.element_to_be_clickable((By.XPATH, xpathQuadro)))
        selectBox = Select(browser.find_element(By.XPATH, xpathQuadro))
        selectBox.select_by_visible_text('DRI ou pessoa equiparada')

        # enter frame
        xpathFrame = '//*[@id="iFrameFormulariosFilho"]'
        iframe = wait.until(EC.element_to_be_clickable((By.XPATH, xpathFrame)))
        iframe = browser.find_elements(By.XPATH, xpathFrame)
        browser.switch_to.frame(iframe[0])

        for i in range(1, 100, 2):  # two by two
            try:
                # '/html/body/form/div[4]/div/table/tbody/tr[1]/td[2]/span'
                xpath = '/html/body/form/div[4]/div/table/tbody/tr[' + str(i) + ']'
                test = browser.find_element(By.XPATH, xpath)
                nome = wText(xpath + '/td[2]/span')
                cargo = wText(xpath + '/td[3]/span')

                click = wClick(xpath + '/td[1]')
                # '/html/body/form/div[4]/div/table/tbody/tr[2]/td[2]/div/div/table/tbody/tr/td[2]/table/tbody/tr[1]/td[2]/span'
                xpath = '/html/body/form/div[4]/div/table/tbody/tr[' + str(i+1) + ']/'
                nome = wText(xpath + 'td[2]/div/div/table/tbody/tr/td[2]/table/tbody/tr[1]/td[2]/span')
                cpf = wText(xpath + 'td[2]/div/div/table/tbody/tr/td[2]/table/tbody/tr[1]/td[4]/span')
                inicio = wText(xpath + 'td[2]/div/div/table/tbody/tr/td[2]/table/tbody/tr[2]/td[2]/span')
                final = wText(xpath + 'td[2]/div/div/table/tbody/tr/td[2]/table/tbody/tr[2]/td[4]/span')
                data.append([nome, cargo, nome, cpf, inicio, final])

            except Exception as e:
                pass

        # back to main frame
        browser.switch_to.parent_frame()

        companyData.append(data)
        return companyData
    except Exception as e:
        trouble(e, sys._getframe().f_code.co_name)
def formCadastral7():
    """download PDF from website for company"""
    # print(sys._getframe().f_code.co_name)
    try:
        companyData = []
        data = []

        # select option menu
        xpathQuadro = '//*[@id="cmbQuadro"]'
        selectBox = wait.until(EC.element_to_be_clickable((By.XPATH, xpathQuadro)))
        selectBox = Select(browser.find_element(By.XPATH, xpathQuadro))
        selectBox.select_by_visible_text('DRI ou pessoa equiparada')

        # enter frame
        xpathFrame = '//*[@id="iFrameFormulariosFilho"]'
        iframe = wait.until(EC.element_to_be_clickable((By.XPATH, xpathFrame)))
        iframe = browser.find_elements(By.XPATH, xpathFrame)
        browser.switch_to.frame(iframe[0])

        for i in range(1,10, 2):  # two by two
            try:
                # '/html/body/form/div[4]/div/table/tbody/tr[1]/td[2]/span'
                nome = wText('/html/body/div/table/tbody/tr[' + i + ']/td[2]/span')
                cargo = wText('/html/body/div/table/tbody/tr[' + i + ']/td[3]/span')

                click = wClick('/html/body/form/div[4]/div/table/tbody/tr[' + i + ']/td[1]')
                # '/html/body/form/div[4]/div/table/tbody/tr[2]/td[2]/div/div/table/tbody/tr/td[2]/table/tbody/tr[1]/td[2]/span'
                nome = wText('/html/body/div/table/tbody/tr[' + i+1 + ']/td[2]/div/div/table/tbody/tr/td[2]/table/tbody/tr[2]/td/table/tbody/tr[2]/td[2]/span')
                cpf = wText('/html/body/div/table/tbody/tr[' + i+1 + ']/td[2]/div/div/table/tbody/tr/td[2]/table/tbody/tr[2]/td/table/tbody/tr[2]/td[4]/span')
                inicio = wText('/html/body/div/tbody/tr[' + i+1 + ']/td[2]/div/div/table/tbody/tr/td[2]/table/tbody/tr[2]/td/table/tbody/tr[3]/td[2]/span')
                final = wText('/html/body/div/tbody/tr[' + i+1 + ']/td[2]/div/div/table/tbody/tr/td[2]/table/tbody/tr[2]/td/table/tbody/tr[3]/td[4]/span')
                data.append([nome, cargo, nome, cpf, inicio, final])

            except:
                pass

        # back to main frame
        browser.switch_to.parent_frame()

        companyData.append(data)
        return companyData
    except Exception as e:
        trouble(e, sys._getframe().f_code.co_name)
def b7NSDStatementNumerOfStocksFix(updateTime):
    """get FIX statements for index for company - number of stocks """
    try:
        # get index
        b3Companies = listagemWorksheet.get_all_values()
        headers = b3Companies.pop(0)
        subtotal = len(b3Companies)
        b3Companies = [x for x in b3Companies if '2 QUAR' in x[21]]
        # random.shuffle(b3Companies)


        nsdSheet = nsdWorksheet.get_all_values()
        headers = nsdSheet.pop(0)
        nsdSheet = [x for x in nsdSheet if x[5] !='NENHUMA COMPANHIA']
        companiesSheet = [unidecode.unidecode(x[5]).translate(str.maketrans('', '', string.punctuation)).upper() for x in nsdSheet]
        companiesSheet = list(dict.fromkeys(companiesSheet))
        total = len(companiesSheet)

        print(len(b3Companies), 'of', subtotal, 'of', total, 'companies')
        for c, company in enumerate(b3Companies):
            if c < batch_companies and company[7] and '2 QUAR' in company[21] and company[22] and datetime.strptime(company[21][:19], '%d/%m/%Y %H:%M:%S') < datetime.strptime(updateTime, '%d/%m/%Y %H:%M:%S'):  # and 'RELA' in company[21]
            # if 'ENGIE BRASIL' in company[0]:
                cSpreadsheet = gsheet.open_by_url(company[7])
                # company = companyUpdate(company)
                # companyQuarters = getCompanyQuarters(company)

                # companyStatements = getCompanyStatements(company)
                companyStatements = getCompanyStatementsNumberOfStocks(company)
                # bigData = companyBigData(company)

                # update action message for log
                if companyStatements:
                    timestamp = sheetLog(company, '3 STAT', sys._getframe().f_code.co_name)
                else:
                    print('company trouble', company[0], company[7])
        print('done b7NSDStatementNumerOfStocksFix')
        # translate is another different step not here
    except Exception as e:
        trouble(e, sys._getframe().f_code.co_name)


def b7NSDReferenceReports(updateTime):
    """get PDF documents for company"""
    try:
        # get index
        b3Companies = listagemWorksheet.get_all_values()
        headers = b3Companies.pop(0)
        subtotal = len(b3Companies)
        b3Companies = [x for x in b3Companies]  # if '2 QUAR' in x[21]]
        # random.shuffle(b3Companies)


        nsdSheet = nsdWorksheet.get_all_values()
        headers = nsdSheet.pop(0)
        nsdSheet = [x for x in nsdSheet if x[5] !='NENHUMA COMPANHIA']
        companiesSheet = [unidecode.unidecode(x[5]).translate(str.maketrans('', '', string.punctuation)).upper() for x in nsdSheet]
        companiesSheet = list(dict.fromkeys(companiesSheet))
        total = len(companiesSheet)

        print(len(b3Companies), 'of', subtotal, 'of', total, 'companies')

        # prepare superIndex
        superIndex = []
        for c, company in enumerate(b3Companies):
            if c % batch_table == 0:
                print(c, 'of', len(b3Companies))
            reportIndex = []
            for x in nsdSheet:
                if x[5] == company[5]:
                    reportIndex.append(x)
            if reportIndex:
                superIndex.append(reportIndex)
            if c > 10:
                print('10 é brincadeira!!')
                break

        # # walk trought superIndex in companies 'Formulário Cadastral'
        # for c, company in enumerate(b3Companies):
        #     # get company reports
        #     try:
        #         nsdlog = int(company[22])
        #     except:
        #         nsdlog = 0
        #     formType = 'Formulário Cadastral'
        #     for r, reportIndex in enumerate(superIndex):
        #         formReports = [x for x in reportIndex if x[3] == formType]
        #         formReports.sort(key=lambda x: x[1], reverse=True)
        #         formReports.sort(key=lambda x: x[2], reverse=False)
        #         years = [line[2] for line in formReports]
        #         years = list(dict.fromkeys(years))
        #         for y, year in enumerate(years):
        #             companyData = []
        #             report = [x for x in formReports if x[2] == year][0]
        #             browser.get(report[0])
        #             cd1 = formCadastral1()
        #             cd2 = formCadastral2()
        #             cd3 = formCadastral3()
        #             cd4 = formCadastral4()
        #             cd5 = formCadastral5()
        #             cd6 = formCadastral6()
        #             cd7 = formCadastral7()
        #             companyData.append([year, cd1[0], cd1[1], cd1[2], cd1, cd2, cd3, cd4, cd5, cd6, cd7])
        #             companyCSV = []
        #             lst = csv_to_list('formulariocadastral')
        #             if lst:
        #                 companyCSV.append(lst)
        #                 print('aspas sao ruins')
        #             companyCSV.append(companyData)
        #             companyCSV = list_to_csv('formulariocadastral', companyCSV)
        #
        # walk trought superIndex in companies 'Formulário de Referência'
        for c, company in enumerate(b3Companies):
            # get company reports
            try:
                nsdlog = int(company[22])
            except:
                nsdlog = 0
            formType = 'Formulário de Referência'
            for r, reportIndex in enumerate(superIndex):
                formReports = [x for x in reportIndex if x[3] == formType]
                formReports.sort(key=lambda x: x[1], reverse=True)
                formReports.sort(key=lambda x: x[2], reverse=False)
                years = [line[2] for line in formReports]
                years = list(dict.fromkeys(years))
                for y, year in enumerate(years):
                    companyData = []
                    report = [x for x in formReports if x[2] == year][0]
                    browser.get(report[0])
                    cd1 = formCadastral1()
                    cd2 = formCadastral2()
                    cd3 = formCadastral3()
                    cd4 = formCadastral4()
                    cd5 = formCadastral5()
                    cd6 = formCadastral6()
                    cd7 = formCadastral7()
                    companyData.append([year, cd1[0], cd1[1], cd1[2], cd1, cd2, cd3, cd4, cd5, cd6, cd7])
                    companyCSV = []
                    lst = csv_to_list('formulariocadastral')
                    if lst:
                        companyCSV.append(lst)
                        print('aspas sao ruins')
                    companyCSV.append(companyData)
                    companyCSV = list_to_csv('formulariocadastral', companyCSV)

    except Exception as e:
        trouble(e, sys._getframe().f_code.co_name)


# warm-up
def userChoice(options):
    """description"""
    # print(sys._getframe().f_code.co_name)
    try:
        print('Por favor escolha uma opção para atualizar:')

        for i, item in enumerate(options):
            print('{}) {}'.format(i + 1, item))

        i = input('Escolha o número da opção desejada para atualizar:')
        try:
            i = int(i)
            if 1 <= i <= len(options):
                return i-1
        except:
            print('Inválido')
            userChoice(options)
        return i-1
    except Exception as e:
        trouble(e, sys._getframe().f_code.co_name)
def warmup():
    """load basic stuff"""
    # print(sys._getframe().f_code.co_name)
    try:
        a = userVariables()
        b = startBrowser()
        c = googleAPI()

        return True
    except Exception as e:
        trouble(e, sys._getframe().f_code.co_name)
def userVariables():
    """load user variables"""
    # print(sys._getframe().f_code.co_name)
    try:
        global batch
        batch = 1000  # number of items per round original is kinda 1000

        global batch_companies
        batch_companies = 1000

        global batch_quarters
        batch_quarters = 1000

        global batch_statements
        batch_statements = 1000

        global batch_table
        batch_table = 150

        global sleep
        sleep = 2

        # browser() variables =======================================
        global producao
        global chrome
        global secs_to_wait
        producao = 'AZEVEDO-SERVER'

        if socket.gethostname() == producao:
            webdriver_folder = 'C:/Users/faust/PycharmProjects/FSP'
        else:
            webdriver_folder = 'C:/Py/FSP'
        chrome = webdriver_folder + '/webdriver/chromedriver.exe'
        firefox = webdriver_folder + 'geckodriver.exe'
        iexplorer = webdriver_folder + 'IEDriverServer.exe'
        edge = webdriver_folder + 'msedgedriver.exe'
        secs_to_wait = 3  # seconds for browser to wait for elements in pages

        # googleAPI() variables =====================================
        global account_credentials
        global sheet_scope
        global google
        account_credentials = 'config/account_credentials_u1.json'
        # account_credentials = 'config/account_credentials_u2.json'
        # account_credentials = 'config/account_credentials_u3.json'

        sheet_scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/spreadsheets',
                       'https://www.googleapis.com/auth/drive']
        google = False

        # companyListSheet() variables ====================================
        global demonstrativos
        global referenceStatements
        demonstrativos = ['Balanço Patrimonial Ativo', 'Balanço Patrimonial Passivo', 'Demonstração do Resultado',
                          'Demonstração do Fluxo de Caixa', 'Demonstração de Valor Adicionado']
        referenceStatements = ['Demonstrações Financeiras Padronizadas', 'Informações Trimestrais']

        # companyListSheet() variables ====================================
        # List of Companies Spreadsheet
        global masterSpreadsheet
        global masterSheet
        global masterLogSheet
        global masterNSDSheet
        global masterBadSheet
        masterSpreadsheet = '1ICxStWnMlDD7ttW8mn4CyC9YYKAm7t2AI3wz-pK3SZU'  # '1AZzuxXbhmDbp5hFOcvYPZK0zYh91uofX7Ry0DcIvIdk'
        masterSheet = 'listagem'
        masterLogSheet = 'log'
        masterNSDSheet = 'NSD'
        masterBadSheet = 'listagem2'

        # Slave template Spreadsheet
        global companyTemplateSpreadsheet
        global slaveSheet
        global slaveFolder
        global companyShIndex
        global companyShReports
        global defaultsSheets
        global superHeaders
        global uberSpreadsheet

        companyTemplateSpreadsheet = '1gtEAfDGctrqJfecHtrcH7N0A9Qyi2ryTnibrctlw4EI'  # 1Mbs5KBpJdowZ1xqIMMQw3WZ7p0XQSN4UJ3fFREEquBw'  # '1wsaqwT-WX_UhsKRrbgqivBc1S3KvKFGjq2MaW4e9ZxI'  # 1R5YOiJOWGhjgvEQdtst_PqrPMeGLV1qe9imjA4lk6mg' old?
        slaveSheet = ['INDEX']  # , 'F-WEB', 'F', 'Glossário', 'blasterlista', 'quotes']
        slaveFolder = '1cDALm2cIV7HzDAvku72eTzIERLzQyaG4'
        companyShIndex = 'index'
        companyShReports = 'reports'
        defaultsSheets = ['index', 'uberReport', 'reports', 'Ticker', 'USDBRL', 'bigdata', 'Config', 'anual', 'trimestral', '12 meses']
        superHeaders = ['Conta ', ' Descrição ', 'Resultado', 'Demonstrativo', 'Trimestre', 'Formato', 'Empresa', 'Relatório']
        uberSpreadsheet = 'https://docs.google.com/spreadsheets/d/1pMj1qEMRVGQcvHcXLrUHsuU3YjcMgT87pSSQquwyjKs/edit#gid=1548209760'


        # companyList() variables ===================================
        global b3CapitalSocialCompaniesURL
        global  b3CapitalSocialCompaniesXpath
        b3CapitalSocialCompaniesURL = 'https://bvmf.bmfbovespa.com.br/CapitalSocial/'
        b3CapitalSocialCompaniesXpath = '//*[@id="divContainerIframeBmf"]/table/tbody/tr'

        global nsdURL1
        global nsdURL2
        nsdURL1 = 'https://www.rad.cvm.gov.br/ENET/frmGerenciaPaginaFRE.aspx?NumeroSequencialDocumento='
        nsdURL2 = '&CodigoTipoInstituicao=1'

        global listedCompaniesURL
        global listedCompaniesXpathTodas
        global listedCompaniesXpathVoltar
        listedCompaniesURL = 'https://sistemaswebb3-listados.b3.com.br/listedCompaniesPage/'
        listedCompaniesXpathTodas = '//*[@id="accordionName"]/div/app-companies-home-filter-name/form/div/div[4]/button'
        listedCompaniesXpathVoltar = '//*[@id="divContainerIframeB3"]/app-companies-overview/div/div[7]/button'

        global cvmXpath
        global companhyXpathHelper
        global helper
        global companyXpathDiv1
        global companyXpathDiv2
        global companyXpathDiv3
        global companyXpathDiv4
        global companyXpathDiv5
        global escrituradorXpath
        cvmXpath = '//*[@id="accordionBody2"]/div/span/p'
        companhyXpathHelper = '//*[@id="divContainerIframeB3"]/app-companies-overview/div/div[1]/div/div/div[2]/p[1]'
        helper = 'CNPJ'
        companyXpathDiv1 = '//*[@id="divContainerIframeB3"]/app-companies-overview/div/div[1]/div/div/div[1]/p[2]'  # null / CNPJ
        companyXpathDiv2 = '//*[@id="divContainerIframeB3"]/app-companies-overview/div/div[1]/div/div/div[2]/p[2]'  # CNPJ / atividde
        companyXpathDiv3 = '//*[@id="divContainerIframeB3"]/app-companies-overview/div/div[1]/div/div/div[3]/p[2]'  # atividade / setor
        companyXpathDiv4 = '//*[@id="divContainerIframeB3"]/app-companies-overview/div/div[1]/div/div/div[4]/p[2]'  # setor / website
        companyXpathDiv5 = '//*[@id="divContainerIframeB3"]/app-companies-overview/div/div[1]/div/div/div[5]/p[2]'  # website / null
        escrituradorXpath = '//*[@id="divContainerIframeB3"]/app-companies-overview/div/div[2]/div/div/p[2]/span[1]'

        global sheetURL
        sheetURL = 'https://docs.google.com/spreadsheets/d/'

        global cvmReportURLA
        global cvmReportURLB
        global cvmReportXpathYear
        global cvmReportURLC

        cvmReportURLA = 'https://sistemaswebb3-listados.b3.com.br/listedCompaniesPage/main/'
        cvmReportURLB = '/reports/?'
        cvmReportXpathYear = '//*[@id="selectYear"]'
        cvmReportURLC = '/reports-historic?'

        return True
    except Exception as e:
        trouble(e, sys._getframe().f_code.co_name)
def startBrowser():
    """load browser"""
    # chromedriver_win32, IEDriverServer_Win32 or IEDriverServer_x64, geckodriver-v0.30.0-win32 or geckodriver-v0.30.0-win64, edgedriver_win32

    # selenium
    # https://www.youtube.com/watch?v=IYILCEV5j6s&list=PLUDwpEzHYYLvx6SuogA7Zhb_hZl3sln66

    # just download the correct version of chromedriver
    # pip install webdriver-manager (charset-normalizer-2.0.10 colorama-0.4.4 configparser-5.2.0 crayons-0.4.0 requests-2.27.1 webdriver-manager-3.5.2)
    # from webdriver_manager.chrome import ChromeDriverManager
    # driver = webdriver.Chrome(ChromeDriverManager().install())
    # print(sys._getframe().f_code.co_name)
    try:
        global browser
        global wait
        global wait2

        # browser
        browser = webdriver.Chrome(service=Service(chrome))
        browser.minimize_window()
        exceptionsIgnore = (NoSuchElementException, StaleElementReferenceException,)
        wait = WebDriverWait(browser, secs_to_wait, ignored_exceptions=exceptionsIgnore)
        wait2 = WebDriverWait(browser, 10/1000, ignored_exceptions=exceptionsIgnore)
        return True
    except Exception as e:
        trouble(e, sys._getframe().f_code.co_name)
def googleAPI(*user):
    """startup google API and authorize gsheet"""
    # google Sheets API - https://console.cloud.google.com/apis/api/sheets.googleapis.com/overview?project=dre-empresas-listadas-b3
    # Credentials - dre-empresas-listadas-bot@dre-empresas-listadas-bovespa.iam.gserviceaccount.com em https://console.cloud.google.com/apis/api/sheets.googleapis.com/credentials?project=dre-empresas-listadas-b3
    # keys - https://console.cloud.google.com/iam-admin/serviceaccounts/details/102235163341900235117;edit=true/keys?project=dre-empresas-listadas-b3 - account_credentials_u1.json
    # https://www.youtube.com/watch?v=4ssigWmExak
    # https://www.youtube.com/watch?v=wJ6WC0G8w4o
    # https://www.youtube.com/watch?v=cnPlKLEGR7E

    # print(sys._getframe().f_code.co_name)
    try:
        global google
        try:
            google = user[0]
        except:
            google = 1

        account_credentials = chooseGoogleCredentials(google)
        credentials: object = service_account.Credentials.from_service_account_file(account_credentials, scopes=sheet_scope)


        global gsheet
        gsheet = gspread.authorize(credentials)

        global listagemWorksheet
        listagemWorksheet = getSheet(masterSpreadsheet, masterSheet)

        global logWorksheet
        logWorksheet = getSheet(masterSpreadsheet, masterLogSheet)

        global nsdWorksheet
        nsdWorksheet = getSheet(masterSpreadsheet, masterNSDSheet)

        return google
    except Exception as e:
        trouble(e, sys._getframe().f_code.co_name)
def chooseGoogleCredentials(user):
    """choose one of these google credentials"""
    # print(sys._getframe().f_code.co_name)
    try:
        folder = 'config/'
        account_credentials_1 = folder + 'account_credentials_u1.json'
        account_credentials_2 = folder + 'account_credentials_u2.json'
        account_credentials_3 = folder + 'account_credentials_u3.json'
        if user == 1:
            account_credentials = account_credentials_1
        elif user == 2:
            account_credentials = account_credentials_2
        elif user == 3:
            account_credentials = account_credentials_3
        else:
            account_credentials = account_credentials_1
            global google
            google = 0
        return account_credentials
    except Exception as e:
        trouble(e, sys._getframe().f_code.co_name)

# system wide functions
def rightNow():
    """return timestamp from the exact moment"""
    # print(sys._getframe().f_code.co_name)
    try:
        timestamp = datetime.now().strftime("%d/%m/%Y %H:%M:%S") + ' ' + socket.gethostname() + ' ' + socket.gethostbyname(socket.gethostname())
        return timestamp
    except Exception as e:
        trouble(e, sys._getframe().f_code.co_name)
def trouble(e, restart):
    """register error message and reloads"""
    # print(sys._getframe().f_code.co_name)
    try:
        print(restart, e)
        # if e.resp.status in [429, 403, 500, 503]:
        global google
        google += 1
        if google == 4:
            google = 1
        print('google user', google)
        time.sleep(sleep*5)
        # restart = b7NSD(rightNow()[:19], 2000)
        # restart = b7NSDCompanies(rightNow()[:19])
        # restart = b7NSDIndex(rightNow()[:19])
        # restart = b7NSDStatements(rightNow()[:19])
        # restart = b7NSDFundamentalist(rightNow()[:19])
        restart = b7NSDStatementNumerOfStocksFix(rightNow()[:19])

    except Exception as e:
        browser.quit()
        print('should be LOG', sys._getframe().f_code.co_name, e)
        # time.sleep(sleep*5)
        restart = b7NSDCompanies(rightNow()[:19])
        # quit()

def b3TranslateSegmentosDeMercado(text):
    """replace acronyms"""
    # print(sys._getframe().f_code.co_name, xpath, keys)
    try:
        acr = ['NM', 'N1', 'N2', 'MA', 'M2', 'MB', 'DR1', 'DR2', 'DR3', 'DRE', 'DRN']
        ext = ['Cia. Novo Mercado', 'Cia. Nível 1 de Governança Corporativa', 'Cia. Nível 2 de Governança Corporativa', 'Cia. Bovespa Mais', 'Cia. Bovespa Mais Nível 2', 'Cia. Balcão Org. Tradicional', 'BDR Nível 1', 'BDR Nível 2', 'BDR Nível 3', 'BDR de ETF', 'BDR Não Patrocinado']
        for i, item in enumerate(acr):
            text = text.replace(item, ext[i])
        return text
    except Exception as e:
        return False
def listMerge(l1, l2):
    """merge l1 + l2 and remove duplicates ['a', 'b'] + ['b', 'c'] = ['a', 'b', 'c']"""
    # print(sys._getframe().f_code.co_name)
    try:
        li3 = []
        [li3.append(i) for i in l1 + l2 if i not in li3]
        return li3
    except Exception as e:
        trouble(e, sys._getframe().f_code.co_name)
def listSubtract(l1, l2):
    """remove l2 from l1  ['a', 'b'] - ['b', 'c'] = ['a']"""
    # print(sys._getframe().f_code.co_name)
    try:
        li3 = [i for i in l1 if i not in l2]
        return li3
    except Exception as e:
        trouble(e, sys._getframe().f_code.co_name)
def listInclusives(l1, l2):
    """only common_terms ['a', 'b'] and ['b', 'c'] = ['b']"""
    # print(sys._getframe().f_code.co_name)
    try:
        li3 = [value for value in l1 if value in l2]
        return li3
    except Exception as e:
        trouble(e, sys._getframe().f_code.co_name)
def listExclusives(l1, l2):
    """only unique items from both lists ['a', 'b'] - ['b', 'c'] = ['a', 'c']"""
    # print(sys._getframe().f_code.co_name)
    try:
        li3 = [i for i in l1 + l2 if i not in l1 or i not in l2]
        return li3
    except Exception as e:
        trouble(e, sys._getframe().f_code.co_name)
def list_to_csv(file, list):
    try:
        folder = 'csv/'
        ext = '.csv'
        with open(folder + file + ext, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerows(list)
        return list
    except:
        pass
def csv_to_list(file):
    try:
        folder = 'csv/'
        ext = '.csv'
        list = []
        with open(folder + file + ext, 'r') as f:
            reader = csv.reader(f)
            for row in reader:
                list.append(row)
        return list
    except:
        return False
def bigDataAnalyticsReplaceSearchAll(uniqueLine, line, search):
    try:
        new = False
        if line[0].strip().startswith(search[1]) and len(line[0].strip()) == search[2] and all(x in line[1].lower() for x in search[0]) and line not in uniqueLine:
            line[0] = search[3]
            line[1] = search[4]
            new = True
        return uniqueLine, line, new
    except Exception as e:
        print(e)
def bigDataAnalyticsReplaceSearchAllBut(uniqueLine, line, search):
    try:
        new = False
        if line[0].strip().startswith(search[2]) and len(line[0].strip()) == search[3] and all(x in line[1].lower() for x in search[0]) and not any(x in line[1].lower() for x in search[1]) and line not in uniqueLine:
            line[0] = search[4]
            line[1] = search[5]
            new = True
        return uniqueLine, line, new
    except Exception as e:
        print(e)
def bigDataAnalyticsReplaceSearchAny(uniqueLine, line, search):
    try:
        new = False
        if line[0].strip().startswith(search[1]) and len(line[0].strip()) == search[2] and any(x in line[1].lower() for x in search[0]) and line not in uniqueLine:
            line[0] = search[3]
            line[1] = search[4]
            new = True
        return uniqueLine, line, new
    except Exception as e:
        print(e)

def wSendKeys(xpath, keys):
    """wait and insert input/keystrokes"""
    # print(sys._getframe().f_code.co_name, xpath, keys)
    try:
        input = wait.until(EC.element_to_be_clickable((By.XPATH, xpath)))
        input = input.send_keys(keys)
        return True
    except Exception as e:
        trouble(e, sys._getframe().f_code.co_name)
def wClick(xpath):
    """wait and click in element"""
    # print(sys._getframe().f_code.co_name, xpath)
    try:
        element = wait.until(EC.element_to_be_clickable((By.XPATH, xpath)))
        element.click()
        return True
    except Exception as e:
        trouble(e, sys._getframe().f_code.co_name)
def wLink(xpath):
    """wait and get link"""
    # print(sys._getframe().f_code.co_name, xpath)
    try:
        element = wait.until(EC.element_to_be_clickable((By.XPATH, xpath)))
        href = element.get_attribute('href')
        return href
    except Exception as e:
        trouble(e, sys._getframe().f_code.co_name)
def wText(xpath):
    """wait and get text"""
    # print(sys._getframe().f_code.co_name, xpath)
    try:
        element = wait.until(EC.element_to_be_clickable((By.XPATH, xpath)))
        text = element.text
        return text
    except Exception as e:
        return ''

# system specific functions
def tableParser(xpath):
    """ navigate trought any table XPath and return content in list"""
    # print(sys._getframe().f_code.co_name, xpath)
    try:
        # get lenght
        assert (EC.element_to_be_clickable((By.XPATH, xpath)))
        rows = len(browser.find_elements(By.XPATH, xpath))
        cols = len(browser.find_elements(By.XPATH, xpath + '[1]/td'))

        # parse table
        table = []
        for r in range(1, rows + 1):
            line = []
            for c in range(1, cols + 1):
                debugXpath = xpath + '[' + str(r) + ']/td[' + str(c) + ']'
                debugContent = browser.find_element(By.XPATH, debugXpath).text
                line.append(debugContent.strip())
            table.append(line)
            if r % batch_table == 0:
                print('table row ', str(r), 'of', str(rows))
        return table
    except Exception as e:
        trouble(e, sys._getframe().f_code.co_name)
def sheetCol(n):
    """transform number in column letter for gspread"""
    # print(sys._getframe().f_code.co_name)
    try:
        string = ''
        while n > 0:
            n, remainder = divmod(n - 1, 26)
            string = chr(65 + remainder) + string
        return string
    except Exception as e:
        trouble(e, sys._getframe().f_code.co_name)
def getSheet(spreadsheet, worksheet):
    """get worksheet from spreadsheet"""
    # print(sys._getframe().f_code.co_name, spreadsheet, worksheet)
    try:
        try:
            spreadsheet = gsheet.open_by_key(spreadsheet)
        except:
            spreadsheet = gsheet.open_by_url(spreadsheet)
        worksheet = spreadsheet.worksheet(worksheet)
        # print('getSheet', spreadsheet.title + "/" + worksheet.title, spreadsheet.url+"/edit#gid="+str(worksheet.id)+'&range=A'+str(worksheet.row_count))
        return worksheet
    except Exception as e:
        trouble(e, sys._getframe().f_code.co_name)
def sheetUpdate(sheet, list, start_row, mode):
    """insert new lines in google sheet"""
    # print(sys._getframe().f_code.co_name)
    try:
        if mode !='RAW':
            mode = 'USER_ENTERED'
        start_col = 1
        end_row = start_row + list.__len__()  # minus sheet header
        end_col = list[0].__len__()
        sheet.resize(start_row, sheet.col_count)

        range = sheetCol(start_col) + str(start_row) + ':' + sheetCol(end_col) + str(end_row)

        cell_list = sheet.range(range)
        for cell in cell_list:
            try:
                r = cell.row - start_row
                c = cell.col - start_col
                cell.value = str(list[r][c]).strip()
            except:
                pass
        # sheet.resize(end_row, sheet.col_count)
        sheet.update_cells(cell_list, value_input_option=mode)
        return True
    except Exception as e:
        trouble(e, sys._getframe().f_code.co_name)
def sheetLog(company, action, content):
    """log important activity"""
    # print(sys._getframe().f_code.co_name, company, content)
    try:
        timestamp = rightNow()

        """update in log"""
        try:
            log = [company[0], company[7], content, timestamp]
            update1 = logWorksheet.append_row(log)
        except:
            pass

        """update in listagemWorksheet"""
        try:
            cell = listagemWorksheet.find(company[7])
            row = cell.row
            cell = 'V' + str(row)
            update2 = listagemWorksheet.update(cell, timestamp + ' ' + action, value_input_option='USER_ENTERED')
        except:
            pass

        """update in company own sheet"""
        try:
            try:
                spreadsheet = gsheet.open_by_key(company[7])
            except:
                spreadsheet = gsheet.open_by_url(company[7])
            worksheet = spreadsheet.worksheet(spreadsheet)
            update3 = worksheet.update('F5', timestamp, value_input_option='USER_ENTERED')  # Timestamp
        except:
            pass

        return timestamp
    except Exception as e:
        trouble(e, sys._getframe().f_code.co_name)

# company
def getCompanies():
    """get and merge list of companyes from sheet and site"""
    # print(sys._getframe().f_code.co_name)
    try:
        # """fast debug"""
        # b3CompaniesWeb = fastload.b3Companies()
        # return b3CompaniesWeb

        """1A get list of companies in shhet"""
        b3CompaniesSheet = listagemWorksheet.get_all_values()
        b3CompaniesSheet.pop(0)

        b3CompaniesSheet2 = getSheet(masterSpreadsheet, masterBadSheet)
        b3CompaniesSheet2 = b3CompaniesSheet2.get_all_values()
        b3CompaniesSheet2.pop(0)

        b3CompaniesSheet = listMerge(b3CompaniesSheet, b3CompaniesSheet2)

        """2B get list of companies from site by Capital Social"""
        b3CompaniesWeb = getCompaniesWeb(b3CapitalSocialCompaniesURL, b3CapitalSocialCompaniesXpath)

        """remove empty lines"""
        for i, item in enumerate(b3CompaniesWeb):
            if not item[0]:
                b3CompaniesWeb.remove(item)

        """for each item in site, check if exist in sheet and remove it"""
        b3CompaniesWeb = [x for x in b3CompaniesWeb if x[0] not in [y[0] for y in b3CompaniesSheet]]

        """update bovespa info from site into main and company sheets"""
        for c, company in enumerate(b3CompaniesWeb):
            b3CompaniesWeb[c] = companyUpdate(company)
            b3CompaniesSheet.append(b3CompaniesWeb[c])

        """remove companies without sheet link"""
        emptyInSheet = []
        for l, line in enumerate(b3CompaniesSheet):
            if not b3CompaniesSheet[l][7]:
                emptyInSheet.append(line)
        b3CompaniesSheet = listSubtract(b3CompaniesSheet, emptyInSheet)
        emptyInSite = []
        for l, line in enumerate(b3CompaniesWeb):
            if not b3CompaniesWeb[l][7]:
                emptyInSite.append(line)
        b3CompaniesSite = listSubtract(b3CompaniesWeb, emptyInSite)

        b3Companies = listMerge(b3CompaniesSheet, b3CompaniesSite)  # merge li1 + li2 and remove duplicates ['a', 'b'] + ['b', 'c'] = ['a', 'b', 'c']

        """order list"""
        if socket.gethostname() == producao:
            random.shuffle(b3Companies)
            # b3Companies.sort(key=lambda x: x[12], reverse=True)  # original is [21] False
            # b3Companies.sort(key=lambda x: x[11], reverse=True)  # original is [21] False
            # b3Companies.sort(key=lambda x: x[10], reverse=True)  # original is [21] False
            # b3Companies.sort(key=lambda x: datetime.strptime(x[21][:19], '%d/%m/%Y %H:%M:%S'), reverse=True)  # original is [21] False
            # b3Companies.sort(key=lambda x: datetime.strptime(x[21][:19], '%d/%m/%Y %H:%M:%S'), reverse=False)  # original is [21] False
        else:
            # by alphabetical
            random.shuffle(b3Companies)
            # b3Companies.sort(key=lambda x: x[12], reverse=True)  # original is [21] False
            # b3Companies.sort(key=lambda x: x[11], reverse=True)  # original is [21] False
            # b3Companies.sort(key=lambda x: x[10], reverse=True)  # original is [21] False
            # b3Companies.sort(key=lambda x: datetime.strptime(x[21][:19], '%d/%m/%Y %H:%M:%S'), reverse=True)  # original is [21] False
            # b3Companies.sort(key=lambda x: datetime.strptime(x[21][:19], '%d/%m/%Y %H:%M:%S'), reverse=False)  # original is [21] False

        return b3Companies
    except Exception as e:
        trouble(e, sys._getframe().f_code.co_name)
def getCompaniesWeb(url, xpath):
    """get B3 companies from specific URL"""
    # print(sys._getframe().f_code.co_name, url, xpath)
    try:
        """fast debug"""
        b3CompaniesWeb = fastload.b3CompaniesWeb()
        return b3CompaniesWeb

        browser.get(url)
        b3CompaniesWeb = tableParser(xpath)

        # prepare data
        for c, company in enumerate(b3CompaniesWeb):
            # update columns
            b3CompaniesWeb[c].insert(1, '')  # list index 1 is CVM
            b3CompaniesWeb[c].insert(3, '')  # list index 3 is Pregão
            b3CompaniesWeb[c].insert(4, '')  # list index 4 is ISIN
            b3CompaniesWeb[c].insert(6, '')  # list index 6 is website
            b3CompaniesWeb[c].insert(7, '')  # list index 7 is relatórios
            b3CompaniesWeb[c].insert(8, '')  # list index 8 is cnpj
            b3CompaniesWeb[c].insert(9, '')  # list index 9 is Setor
            b3CompaniesWeb[c].insert(10, '')  # list index 10 is Subsetor
            b3CompaniesWeb[c].insert(11, '')  # list index 11 is Segmento
            b3CompaniesWeb[c].insert(12, '')  # list index 12 is Atividade Principal
            b3CompaniesWeb[c].insert(20, '')  # list index 20 is Escriturador
            b3CompaniesWeb[c].insert(21, '')  # list index 21 is Timestamp
            # data type
            b3CompaniesWeb[c][15] = float(b3CompaniesWeb[c][15].replace('.', '').replace(',', '.'))  # list index 15 is Capital Social
            b3CompaniesWeb[c][17] = int(b3CompaniesWeb[c][17].replace('.', '').replace(',', '.'))  # list index 17 is ON stocks
            b3CompaniesWeb[c][18] = int(b3CompaniesWeb[c][18].replace('.', '').replace(',', '.'))  # list index 18 is PN stocks
            b3CompaniesWeb[c][19] = int(b3CompaniesWeb[c][19].replace('.', '').replace(',', '.'))  # list index 19 is ON + PN stocks
            b3CompaniesWeb[c][16] = datetime.strptime(b3CompaniesWeb[c][16], '%d/%m/%Y').strftime('%d/%m/%Y')  # list index 16 is last company social capital approval date
        return b3CompaniesWeb
    except Exception as e:
        trouble(e, sys._getframe().f_code.co_name)


def getCompanyQuartersUpdate(company, quartersSheet, quartersWeb):
    """update only most recent quarters for company, and clean old ones"""
    # print(sys._getframe().f_code.co_name, company[0], company[7])
    try:
        # merge quarters from sheet and from web, and get new actual and old list
        quartersFull = listMerge(quartersSheet, quartersWeb)
        quartersFull = [line for line in quartersFull if line[1] != '0']
        quartersFull.sort(key=lambda x: x[4], reverse=True)
        quartersFull.sort(key=lambda x: int(x[1]), reverse=True)
        # get unique quarters
        quarters = [line[2] for line in quartersFull]
        quarters = list(dict.fromkeys(quarters))

        # get new_list, old_list and actual_list
        quartersActual = []
        quartersRemove = []
        for q, quarter in enumerate(quarters):
            nr = [line for line in quartersFull if line[2] == quarter]
            for i, item in enumerate(nr):
                if i == 0:
                    quartersActual.append(item)
                else:
                    quartersRemove.append(item)
        quartersRecent = [line for line in quartersActual if line not in quartersSheet]

        # A - for actual reports, get full report list, and do nothing
        companySheetReports = getSheet(company[7], 'reports')
        companyReports = companySheetReports.get_all_values()

        # B - for remove reports 1- empty worksheet,
        qRemove = [line[2] for line in quartersRemove]
        qRemove = list(dict.fromkeys(qRemove))
        for qq, qquarter in enumerate (qRemove):
            try:
                companySheetReport = gsheet.open_by_url(company[7])
                worksheet = companySheetReport.worksheet(qquarter)
                companySheetReport.clear()
                companySheetReport.resize(2, 8)
            except Exception as e:
                print(e)
        # B2 - remove from full report
        companyReportsRemove = []
        for l, line in enumerate(companyReports):
            for i, item in enumerate(quartersRemove):
                if line[7] == item[0]:
                    companyReportsRemove.append(line)
        companyReports = listSubtract(companyReports, companyReportsRemove)
        headers = companyReports.pop(0)
        if companyReports:
            udpate = sheetUpdate(companySheetReports, companyReports, 2, 'RAW')

        # C - for recent reports 1 - populate worksheet, 2 append to full report
        # this is up to get statements function
        for l, line in enumerate(quartersActual):
            for i, item in enumerate(quartersRemove):
                if line[7] == item[0]:
                    companyReportsRemove.append(line)
        quartersActual = listSubtract(quartersActual, companyReportsRemove)
        headers = companyReports.pop(0)
        udpate = sheetUpdate(companySheetReports, companyReports, 2, 'RAW')


        return quartersActual
    except Exception as e:
        trouble(e, sys._getframe().f_code.co_name)
def getCompanyQuarters(company, *rewrite):
    """get quarters for company"""
    # print(sys._getframe().f_code.co_name, company[0], company[7])
    try:
        # """fast debug"""
        # companyQuarters = fastload.companyQuarters()
        # return companyQuarters

        # get company quarters from sheet
        quartersSheet, companySheetIndex = getCompanyQuartersSheet(company)


        # quartersWeb = []
        # if len(quartersSheet) == 0:
        #     quartersWeb = getCompanyQuartersWeb(company, quartersSheet)
        quartersWeb = getCompanyQuartersWeb(company, quartersSheet)
        
        """nsd irregularities fix and count"""
        nsd = 1000000000
        count = 0
        for r, report in enumerate(quartersSheet):
            if nsd <= int(report[1]) and int(report[1]) != 0 and 'Versão 1' in report[3]:
                count += 1
            nsd = int(report[1])

        """merge and sort with list of quarters from web for company"""
        if count > 0:
            quartersWeb = getCompanyQuartersWeb(company, quartersSheet)

        # hard re-write
        if rewrite:
            quartersWeb = getCompanyQuartersWeb(company, quartersSheet)

        quarters = getCompanyQuartersUpdate(company, quartersSheet, quartersWeb)

        """newest quarters"""
        # quarters.sort(key=lambda x: (datetime.strptime(x[2]).month, '%d/%m/%Y'), reverse=True)
        quarters.sort(key=lambda x: int(x[1]), reverse=True)
        quarter = [line[2] for line in quarters]
        quarter = list(dict.fromkeys(quarter))
        companyQuarters = []
        for q, quart in enumerate(quarter):
            ur = [x for x in quarters if quart == x[2]]
            if ur:
                companyQuarters.append(ur[0])

        time.sleep(sleep)

        """update and log"""
        cell_list = sheetUpdate(companySheetIndex, companyQuarters, 7, 'USER_ENTERED')
        timestamp = sheetLog(company, ' ', sys._getframe().f_code.co_name)

        return companyQuarters
    except Exception as e:
        trouble(e, sys._getframe().f_code.co_name)
def getCompanyQuartersSheet(company):
    """open worksheets and sheets for bovespa"""
    # print(sys._getframe().f_code.co_name, company[0], company[7])
    try:
        companySheetIndex = getSheet(company[7], 'index')

        """get list of quarters from sheets for company"""
        companyQuarters = companySheetIndex.get_all_values()
        for l, line in enumerate(companyQuarters):
            del companyQuarters[l][-1]
        companyQuarters = [line for line in companyQuarters if 'http' in line[0] and str(line[1]) != '0' and line[3] in referenceStatements]

        quarters = [q[2] for q in companyQuarters]
        quarters = list(dict.fromkeys(quarters))

        trueCompanyQuarters = []
        for q in quarters:
            x = [x for x in companyQuarters if datetime.strptime(x[2], '%d/%m/%Y') == datetime.strptime(q, '%d/%m/%Y')]
            trueCompanyQuarters.append(x[0])

        return trueCompanyQuarters, companySheetIndex
    except Exception as e:
        trouble(e, sys._getframe().f_code.co_name)


def getCompanyQuartersWeb(company, quartersSheet):
    """get list of quarters from web for company"""
    # print(sys._getframe().f_code.co_name, company)
    try:
        # if not quartersSheet:
        browser.get(cvmReportURLA + company[1] + '/' + company[2] + cvmReportURLB)
        wClick(cvmReportXpathYear)
        select = Select(browser.find_element(By.XPATH, cvmReportXpathYear))
        years = [x.text for x in select.options]

        quartersWeb = []
        """get pages with lists of quarters by type per year for company"""
        for i, y in enumerate(years):
            quartersPerYear = [line for line in quartersSheet if datetime.strptime(line[2], '%d/%m/%Y').year == int(y)]
            # quartersPerYear = [line for line in quartersSheet if datetime.strptime(line[2], '%d/%m/%Y').year == int(y) and int(line[1]) != 0]
            if len(quartersPerYear) <= 4:  # use '< 4' to skip years with full downloaded quarters
                """get 'range' itr (trimestral)"""
                browser.get(cvmReportURLA + company[1] + '/' + company[2] + cvmReportURLC + 'language=pt-br&type=ITR&year=' + str(y))
                for x in range(1, 15):
                    alert = wText('//*[@id="divContainerIframeB3"]/app-companies-reports-historic/div[1]/div/div')
                    if alert != 'Não há dados disponíveis para esta consulta.':
                        try:
                            xpath = '//*[@id="divContainerIframeB3"]/app-companies-reports-historic/div[1]/div/div/div[' + str(x) + ']/div[1]/div[2]/p/a'
                            href = browser.find_element(By.XPATH, xpath).get_attribute('href')
                            href1 = href.split('=')[1]
                            text = browser.find_element(By.XPATH, xpath).text
                            text0 = text.split(' - ')[0]
                            text1 = text.split(' - ')[1]
                            text2 = text.split(' - ')[2]
                            quartersWeb.append([href, href1, text0, text1, text2])
                        except:
                            pass
                    else:
                        try:
                            if quartersWeb:
                                """insert empty line for no report"""
                                nData = quartersWeb[-1]
                                lastTri = datetime.strptime(nData[2], '%d/%m/%Y')
                                if lastTri.month == 12:
                                    nData = datetime(lastTri.year, 9, 30)
                                if lastTri.month == 6:
                                    nData = datetime(lastTri.year, 3, 31)
                                if lastTri.month == 9:
                                    nData = datetime(lastTri.year, 6, 30)
                                if lastTri.month == 3:
                                    nData = datetime(lastTri.year - 1, 12, 31)
                                trim = datetime.strftime(nData, '%d/%m/%Y')
                                quartersWeb.append([company[7], '0', trim, 'Nada encontrado', ''])
                        except:
                            pass
                """get 'range' dfp (anual)"""
                browser.get(cvmReportURLA + company[1] + '/' + company[
                    2] + cvmReportURLC + 'language=pt-br&type=DFP&year=' + str(y))
                for x in range(1, 5):
                    alert = wText('//*[@id="divContainerIframeB3"]/app-companies-reports-historic/div[1]/div/div')
                    if alert != 'Não há dados disponíveis para esta consulta.':
                        try:
                            xpath = '//*[@id="divContainerIframeB3"]/app-companies-reports-historic/div[1]/div/div/div[' + str(x) + ']/div[1]/div[2]/p/a'
                            href = browser.find_element(By.XPATH, xpath).get_attribute('href')
                            href1 = href.split('=')[1]
                            text = browser.find_element(By.XPATH, xpath).text
                            text0 = text.split(' - ')[0]
                            text1 = text.split(' - ')[1]
                            text2 = text.split(' - ')[2]
                            quartersWeb.append([href, href1, text0, text1, text2])
                        except:
                            pass
                        else:
                            try:
                                if quartersWeb:
                                    """insert empty line for no report"""
                                    nData = quartersWeb[-1]
                                    lastTri = datetime.strptime(nData[2], '%d/%m/%Y')
                                    if lastTri.month == 12:
                                        nData = datetime(lastTri.year, 9, 30)
                                    if lastTri.month == 6:
                                        nData = datetime(lastTri.year, 3, 31)
                                    if lastTri.month == 9:
                                        nData = datetime(lastTri.year, 6, 30)
                                    if lastTri.month == 3:
                                        nData = datetime(lastTri.year - 1, 12, 31)
                                    trim = datetime.strftime(nData, '%d/%m/%Y')
                                    quartersWeb.append([company[7], '0', trim, 'Nada encontrado', ''])
                            except:
                                pass

        return quartersWeb
    except Exception as e:
        trouble(e, sys._getframe().f_code.co_name)
# company functions
def companyWebInfo(company, url):
    """get B3 company Info from B3 individual company site"""
    # print(sys._getframe().f_code.co_name, company, url)
    try:
        xpathKeyword = '//*[@id="keyword"]'
        xpathBuscar = '//*[@aria-label="Buscar"]'
        xpathCVM = '//*[@id="nav-bloco"]/div/div/div'
        pregaoMaisXpath = '//*[@id="accordionHeading2"]/div/div/a'
        pregaoXpath = '//*[@id="accordionBody2"]/div/table/tr'

        browser.get(url)
        """search for bovespa name"""
        wSendKeys(xpathKeyword, str(company[2]) + ' ' + company[5])

        if wClick(xpathBuscar) == True:
            time.sleep(sleep)

            curr = browser.current_url
            if browser.current_url != url:
                # there are search results, navigate them
                classResults = 'card-title'
                allResults = browser.find_elements_by_class_name(classResults)

                # there are some results
                for result in allResults:
                    text = unidecode.unidecode(result.text).translate(str.maketrans('', '', string.punctuation)).upper().strip()
                    wrong = True
                    if company[5] == text:
                        # find and click
                        company5 = text
                        result.click()
                        wrong = False
                        break
                if not wrong:
                    time.sleep(sleep)
                    company[1] = browser.current_url.split('/')[5]
                    company[2] = browser.current_url.split('/')[6]
                    # if right, complete or partial info?
                    xpathCompany3 = '//*[@id="divContainerIframeB3"]/app-companies-overview/div/div[1]/div/div/p[4]/a'
                    company3 = wText(xpathCompany3)
                    if company3:
                        # full profile
                        """scrap cvm codes full profile"""
                        if wClick(pregaoMaisXpath) == True:
                            #get table of codes for isin e cvm
                            time.sleep(sleep)
                            pregao = tableParser(pregaoXpath)
                            company3 = []
                            company4 = []
                            pregao.pop(0)
                            for r in pregao:
                                company3.append(r[0].strip())
                                company4.append(r[1].strip())
                        company[3] = ' '.join(company3)  # pregao
                        company[4] = ' '.join(company4)  # isin
                        xpathCompany0 = '//*[@id="divContainerIframeB3"]/app-companies-overview/div/div[1]/div/div/p[2]'
                        xpathCompany5 = '//*[@id="divContainerIframeB3"]/div/div[1]/h2'
                        xpathCompany6 = '//*[@id="divContainerIframeB3"]/app-companies-overview/div/div[1]/div/div/div[5]/p[2]/a'
                        xpathCompany8 = '//*[@id="divContainerIframeB3"]/app-companies-overview/div/div[1]/div/div/div[2]/p[2]'
                        xpathCompany9 = '//*[@id="divContainerIframeB3"]/app-companies-overview/div/div[1]/div/div/div[4]/p[2]'
                        xpathCompany12 = '//*[@id="divContainerIframeB3"]/app-companies-overview/div/div[1]/div/div/div[3]/p[2]'
                        company[0] = wText(xpathCompany0)
                        company[5] = unidecode.unidecode(wText(xpathCompany5)).translate(str.maketrans('', '', string.punctuation)).upper().strip()
                        company[6] = wText(xpathCompany6)
                        company[8] = wText(xpathCompany8)
                        company9 = wText(xpathCompany9)
                        company[9] = company9.split(' / ')[0].strip()  # Setor
                        company[10] = company9.split(' / ')[1].strip()  # Subsetor
                        company[11] = company9.split(' / ')[2].strip()  # Segmento
                        company[12] = wText(xpathCompany12)

                        # part-page 2
                        url2 = 'https://sistemaswebb3-listados.b3.com.br/listedCompaniesPage/main/' + company[1] + '/' + company[2] + '/corporate-actions?language=pt-br'
                        browser.get(url2)
                        time.sleep(sleep)
                        try:
                            xpathCompany13 = '//*[@id="divContainerIframeB3"]/app-companies-corporate-actions/div[1]/div[1]/p[4]'
                            xpathCompany14 = '//*[@id="divContainerIframeB3"]/app-companies-corporate-actions/div[1]/div[2]/p[2]'
                            xpathCompany15 = '//*[@id="divContainerIframeB3"]/app-companies-corporate-actions/div[1]/div[2]/p[4]'
                            xpathCompany16 = '//*[@id="divContainerIframeB3"]/app-companies-corporate-actions/div[1]/div[1]/p[8]'
                            xpathCompany17 = '//*[@id="divContainerIframeB3"]/app-companies-corporate-actions/div[1]/div[2]/p[8]'
                            xpathCompany18 = '//*[@id="divContainerIframeB3"]/app-companies-corporate-actions/div[1]/div[3]/p[4]'
                            xpathCompany19 = '//*[@id="divContainerIframeB3"]/app-companies-corporate-actions/div[1]/div[2]/p[6]'
                            xpathCompany20 = '//*[@id="divContainerIframeB3"]/app-companies-corporate-actions/div[1]/div[3]/p[2]'
                            company13 = wText(xpathCompany13)
                            company[13] = b3TranslateSegmentosDeMercado(company13)
                            company[14] = wText(xpathCompany14)
                            company[15] = wText(xpathCompany15)
                            company[16] = wText(xpathCompany16)
                            company[17] = wText(xpathCompany17)
                            company[18] = wText(xpathCompany18)
                            company[19] = wText(xpathCompany19)
                            company[20] = wText(xpathCompany20).split('-')[1].strip()
                        except:
                            # there is no relevant info here
                            pass
                    else:
                        try:
                            # partial template attempt 1
                            # partial profile
                            xpathCompany0 = '//*[@id="divContainerIframeB3"]/app-companies-overview/div/div[1]/div/div/p[2]'
                            xpathCompany5 = '/html/body/app-root/app-companies-menu-select/div/div/div[1]/h2'
                            xpathCompany6 = '//*[@id="divContainerIframeB3"]/app-companies-overview/div/div[1]/div/div/div[4]/p[2]/a'
                            xpathCompany8 = '//*[@id="divContainerIframeB3"]/app-companies-overview/div/div[1]/div/div/div[1]/p[2]'
                            xpathCompany9 = '//*[@id="divContainerIframeB3"]/app-companies-overview/div/div[1]/div/div/div[3]/p[2]'
                            xpathCompany12 = '//*[@id="divContainerIframeB3"]/app-companies-overview/div/div[1]/div/div/div[2]/p[2]'
                            company[0] = wText(xpathCompany0)
                            company[5] = unidecode.unidecode(wText(xpathCompany5)).translate(str.maketrans('', '', string.punctuation)).upper().strip()
                            company[6] = wText(xpathCompany6)
                            company[6] = wText(xpathCompany6)
                            company[8] = wText(xpathCompany8)
                            company9 = wText(xpathCompany9)
                            company[9] = company9.split(' / ')[0].strip()  # Setor
                            company[10] = company9.split(' / ')[1].strip()  # Subsetor
                            company[11] = company9.split(' / ')[2].strip()  # Segmento
                            company[12] = wText(xpathCompany12)

                            # part-page 2
                            url2 = 'https://sistemaswebb3-listados.b3.com.br/listedCompaniesPage/main/' + company[
                                1] + '/' + company[2] + '/corporate-actions?language=pt-br'
                            browser.get(url2)
                            time.sleep(sleep)
                            xpathCompany13 = '//*[@id="divContainerIframeB3"]/app-companies-corporate-actions/div[1]/div[1]/p[4]'
                            xpathCompany14 = '//*[@id="divContainerIframeB3"]/app-companies-corporate-actions/div[1]/div[2]/p[2]'
                            xpathCompany15 = '//*[@id="divContainerIframeB3"]/app-companies-corporate-actions/div[1]/div[2]/p[4]'
                            xpathCompany16 = '//*[@id="divContainerIframeB3"]/app-companies-corporate-actions/div[1]/div[1]/p[8]'
                            xpathCompany17 = '//*[@id="divContainerIframeB3"]/app-companies-corporate-actions/div[1]/div[2]/p[8]'
                            xpathCompany18 = '//*[@id="divContainerIframeB3"]/app-companies-corporate-actions/div[1]/div[3]/p[4]'
                            xpathCompany19 = '//*[@id="divContainerIframeB3"]/app-companies-corporate-actions/div[1]/div[3]/p[2]'
                            company13 = wText(xpathCompany13)
                            company[13] = b3TranslateSegmentosDeMercado(company13)
                            company[14] = wText(xpathCompany14)
                            company[15] = wText(xpathCompany15)
                            company[16] = wText(xpathCompany16)
                            company[17] = wText(xpathCompany17)
                            company[18] = wText(xpathCompany18)
                            company[19] = wText(xpathCompany19)
                        except:
                            try:
                                # partial template attempt 2
                                xpathCompany0 = '//*[@id="divContainerIframeB3"]/app-companies-overview/div/div[1]/div/div/p[2]'
                                xpathCompany5 = '//*[@id="divContainerIframeB3"]/div/div[1]/h2'
                                xpathCompany6 = '//*[@id="divContainerIframeB3"]/app-companies-overview/div/div[1]/div/div/div[4]/p[2]/a'
                                xpathCompany8 = '//*[@id="divContainerIframeB3"]/app-companies-overview/div/div[1]/div/div/div[1]/p[2]'
                                company[0] = wText(xpathCompany0)
                                company[5] = unidecode.unidecode(wText(xpathCompany5)).translate(str.maketrans('', '', string.punctuation)).upper().strip()
                                company[6] = wText(xpathCompany6)
                                company[8] = wText(xpathCompany8)
                                company[9] = 'NONE'  # Setor
                                company[10] = 'NONE'  # Subsetor
                                company[11] = 'NONE'  # Segmento
                            except:
                                pass
                                company[1] = browser.current_url.split('/')[5]
                                company[2] = browser.current_url.split('/')[6]

                    company[21] = rightNow()  # Timestamp

        return company
    except Exception as e:
        trouble(e, sys._getframe().f_code.co_name)
def companyUpdate(company):
    """update info from web into google sheeet"""
    print(sys._getframe().f_code.co_name, company[0], company[7])
    try:
        # """fast debug"""
        # company = fastload.company()
        # return company


        company = companyWebInfo(company, listedCompaniesURL)
        company = companySheetCreate(company, companyTemplateSpreadsheet)
        company = companySheetUpdate(company)

        return company
    except Exception as e:
        trouble(e, sys._getframe().f_code.co_name)
def companySheetCreate(company, template):
    """create google sheet for a new company from a template"""
    # print(sys._getframe().f_code.co_name, company, template)
    try:
        if not company[7]:
            name = str(company[9][:5].upper() + ' / ' + company[10][:5].upper() + ' / ' + company[11][:5].upper() + ' - ' + company[3][:4].replace('Nenh','NONE').upper() + ' - ' + company[0] + ' - ' + company[5] + ' - ' + company[1].upper())
            newSheet = gsheet.copy(template, title=name, copy_permissions=True, folder_id=slaveFolder)
            company[7] = sheetURL + newSheet.id
            timestamp = rightNow()
            company[21] = timestamp + ' ' + '1 COMP'
            listagemWorksheet.append_row(company)
        return company
    except Exception as e:
        trouble(e, sys._getframe().f_code.co_name)
def companySheetUpdate(company):
    """update company sheet with new info from web"""
    # print(sys._getframe().f_code.co_name, company)
    try:
        companySheet = getSheet(company[7], 'index')

        cHeader = [['', '', '', '', '', ''], ['', '', '', '', '', ''], ['', '', '', '', '', ''],
                   ['', '', '', '', '', ''], ['', '', '', '', '', '']]
        cHeader[0][0] = company[0].strip() #'NOME DO PREGAO'
        cHeader[0][4] = '=IFERROR(INDEX(FILTER(Ticker!J2:J;Ticker!J2:J<>"");1;1))'
        cHeader[0][5] = company[5].strip() #'Denominação Social'
        cHeader[1][0] = company[12].strip() # 'Atividade Principal'
        cHeader[1][4] = '=IFERROR(INDEX(FILTER(Ticker!R2:R;Ticker!R2:R<>"");1;1))'
        cHeader[2][0] = company[6].strip() # '"Website'
        cHeader[3][0] = company[9].strip() # 'Setor'
        cHeader[3][1] = company[10].strip() # 'Subsetor'
        cHeader[3][2] = company[11].strip() # 'Segmento'
        cHeader[3][3] = str(company[2]).strip() # 'TICK'
        cHeader[3][4] = str(company[1]).strip() # 'CVM'
        cHeader[3][5] = company[8].strip() # 'CNPJ'
        cHeader[4][0] = company[17] # 'ON'
        cHeader[4][1] = company[18] # 'PN'
        cHeader[4][2] = company[15] # 'Capital Social'
        cHeader[4][3] = company[13].strip() # 'Segmento de Mercado'
        cHeader[4][4] = company[20].strip() # 'Escriturador'
        cHeader[4][5] = company[21] # 'Timestamp'

        start_row = 1
        start_col = 1
        end_row = len(cHeader)
        end_col = len(cHeader[0])
        range = sheetCol(start_col) + str(start_row) + ':' + sheetCol(end_col) + str(end_row)
        cell_list = companySheet.range(range)
        for cell in cell_list:
            try:
                r = cell.row - start_row
                c = cell.col - start_col
                cell.value = str(cHeader[r][c]).strip()
            except:
                pass
        companySheet.update_cells(cell_list, value_input_option='USER_ENTERED')


        return company
    except Exception as e:
        trouble(e, sys._getframe().f_code.co_name)

# quarters
def getStatementFromQuartersFIXITUP(company, companyQuarters, sheet):
    """load every financial statement for every quarter in company list of quarters"""
    # print(sys._getframe().f_code.co_name, company)
    try:
        cSpreadsheet = gsheet.open_by_url(company[7])

    except Exception as e:
        trouble(e, sys._getframe().f_code.co_name)
def getStatementFromQuarters(company, companyQuarters, quartersSheets, cSpreadsheet):
    """load every financial statement for every quarter in company list of quarters"""
    # print(sys._getframe().f_code.co_name, company)
    try:
        statements = []
        fromWeb = False
        """ different approachs accordingly to stage of quarter (statementSheet) downloaded from web """
        for q, quarter in enumerate(companyQuarters):
            # quarter[2] = datetime.strptime(quarter[2], '%d/%m/%Y').strftime('%d/%m/%Y')
            # if there is no sheet:
            if quarter[2] not in quartersSheets:
                """there is no sheet, create sheet and grab from web"""
                # get quarter from web
                statementSheet = getQuarter(company, quarter)
                fromWeb = True
                # create sheet
                quartersSheet = cSpreadsheet.add_worksheet(quarter[2], 2, 8, index=None)
            # otherwise there is sheet
            else:
                # there is sheet, get sheet
                quartersSheet = cSpreadsheet.worksheet(quarter[2])
                # get values from csv
                file = company[5] + ' ' + datetime.strptime(quarter[2], '%d/%m/%Y').strftime('%Y %m %d')
                statementSheet = csv_to_list(file)
                if not statementSheet:
                    # get values from sheet
                    statementSheet = quartersSheet.get_all_values()
                    # if there are values, keep them
                    if len(statementSheet) > 2:
                        statementSheet.pop(0)
                    # if there are no values in sheet, update quarter from web
                    else:
                        statementSheet = getQuarter(company, quarter)
                        fromWeb = True
            # save statementSheet to sheet
            statementSheet.insert(0, superHeaders)
            update = sheetUpdate(quartersSheet, statementSheet, 1, 'RAW')
            statementSheet.pop(0)
            time.sleep(sleep)
            # save also to csv
            file = company[5] + ' ' + datetime.strptime(quarter[2], '%d/%m/%Y').strftime('%Y %m %d')
            statementSheet = list_to_csv(file, statementSheet)
            # finally, append statements
            statements.append(statementSheet)

            if fromWeb:
                fromWeb = False
                action = '2 QUAR'
                timestamp = sheetLog(company, action, sys._getframe().f_code.co_name + ' ' + quarter[2])
            print('report', len(companyQuarters) - q, quarter[2], company[0], company[7] + "/edit#gid=" + str(quartersSheet.id))
            time.sleep(sleep)

        return statements
    except Exception as e:
        trouble(e, sys._getframe().f_code.co_name)
def getStatementFromQuartersNumberOfStocks(company, companyQuarters, quartersSheets, cSpreadsheet):
    """load every financial statement for every quarter in company list of quarters"""
    # print(sys._getframe().f_code.co_name, company)
    try:
        statements = []
        fromWeb = False
        """ different approachs accordingly to stage of quarter (statementSheet) downloaded from web """
        for q, quarter in enumerate(companyQuarters):
            # do the magic in web scraping web page and update sheet, re-order sheet
            statementSheet = getQuarterNumberOfStocks(cSpreadsheet, company, quarter)
            if statementSheet == False:
                return False
            statements.append(statementSheet)

        return statements
    except Exception as e:
        trouble(e, sys._getframe().f_code.co_name)

def getQuarterFIXITUP(company, quarter):
    """load all financial statement type by quarter by company"""
    # print(sys._getframe().f_code.co_name, company, quarter)
    try:
        # open quarter
        timestamp = rightNow()
        browser.get(quarter[0])
        url = browser.current_url
        demo7 = ['Demonstração de Valor Adicionado']
        quarterData = []
        for i, type in enumerate(demo7):
            try:
                tableTemp = []
                # select type of quarter
                xpathQuadro = '//*[@id="cmbQuadro"]'
                selectBox = wait.until(EC.element_to_be_clickable((By.XPATH, xpathQuadro)))
                selectBox = Select(browser.find_element(By.XPATH, xpathQuadro))
                selectBox.select_by_visible_text(type)

                # enter frame
                xpathFrame = '//*[@id="iFrameFormulariosFilho"]'
                iframe = wait.until(EC.element_to_be_clickable((By.XPATH, xpathFrame)))
                iframe = browser.find_elements(By.XPATH, xpathFrame)
                browser.switch_to.frame(iframe[0])

                # select data table
                xpathTable = '//*[@id="ctl00_cphPopUp_tbDados"]/tbody/tr'
                tableTemp = wait.until(EC.element_to_be_clickable((By.XPATH, xpathTable)))
                tableTemp = tableParser(xpathTable)
                tableTemp.pop(0)

                # data preparation
                for j, jline in enumerate(tableTemp):
                    tableTemp[j][2] = tableTemp[j][2].replace('.', '').replace(',', '.')
                    tableTemp[j].insert(3, type)
                    tableTemp[j].insert(4, quarter[2])
                    tableTemp[j].insert(5, quarter[3])
                    tableTemp[j].insert(6, company[0])
                    tableTemp[j].insert(7, quarter[0])
                    tableTemp[j].insert(8, timestamp)
                    try:
                        tableTemp[j].pop(9)
                    except:
                        pass
                    try:
                        tableTemp[j].pop(10)
                    except:
                        pass
                    try:
                        tableTemp[j].pop(11)
                    except:
                        pass
                    try:
                        tableTemp[j].pop(12)
                    except:
                        pass
                    quarterData.append(tableTemp[j])
                # browser.switch_to_default_content()
                browser.switch_to.parent_frame()
            except:
                pass
        return quarterData

    except Exception as e:
        trouble(e, sys._getframe().f_code.co_name)
def getQuarter(company, quarter):
    """load all financial statement type by quarter by company"""
    # print(sys._getframe().f_code.co_name, company, quarter)
    try:
        # open quarter
        timestamp = rightNow()
        browser.get(quarter[0])
        url = browser.current_url

        # enter frame
        xpathFrame = '//*[@id="iFrameFormulariosFilho"]'
        iframe = wait.until(EC.element_to_be_clickable((By.XPATH, xpathFrame)))
        iframe = browser.find_elements(By.XPATH, xpathFrame)
        browser.switch_to.frame(iframe[0])
        xpath = '//*[@id="TituloTabelaSemBorda"]'
        unidadeConta = wText(xpath)
        browser.switch_to.parent_frame()
        if 'Mil' in unidadeConta:
            unidadeConta = 1000
        else:
            unidadeConta = 1

        quarterData = []
        for i, type in enumerate(demonstrativos):
            try:
                tableTemp = []
                # select type of quarter
                xpathQuadro = '//*[@id="cmbQuadro"]'
                selectBox = wait.until(EC.element_to_be_clickable((By.XPATH, xpathQuadro)))
                selectBox = Select(browser.find_element(By.XPATH, xpathQuadro))
                selectBox.select_by_visible_text(type)

                # enter frame
                xpathFrame = '//*[@id="iFrameFormulariosFilho"]'
                iframe = wait.until(EC.element_to_be_clickable((By.XPATH, xpathFrame)))
                iframe = browser.find_elements(By.XPATH, xpathFrame)
                browser.switch_to.frame(iframe[0])

                # select data table
                xpathTable = '//*[@id="ctl00_cphPopUp_tbDados"]/tbody/tr'
                tableTemp = wait.until(EC.element_to_be_clickable((By.XPATH, xpathTable)))
                tableTemp = tableParser(xpathTable)
                tableTemp.pop(0)

                # data preparation
                for j, jline in enumerate(tableTemp):
                    tableTemp[j][2] = tableTemp[j][2].replace('.', '').replace(',', '.')
                    tableTemp[j].insert(3, type)
                    tableTemp[j].insert(4, quarter[2])
                    tableTemp[j].insert(5, quarter[3])
                    tableTemp[j].insert(6, company[0])
                    tableTemp[j].insert(7, quarter[0])
                    # tableTemp[j].insert(8, timestamp)
                    for k in range(0,5):
                        try:
                            tableTemp[j].pop(8)
                        except:
                            pass
                    quarterData.append(tableTemp[j])
                # browser.switch_to_default_content()
                browser.switch_to.parent_frame()
            except:
                pass
        # populate quarter sheet
        for line in quarterData:
            if line[2]:
                line[2] = int(str(float(line[2]) * unidadeConta).split('.')[0])
            else:
                line[2] = 0

        return quarterData
    except Exception as e:
        trouble(e, sys._getframe().f_code.co_name)
def getQuarterNumberOfStocks(cSpreadsheet, company, quarter):
    """load all financial statement type by quarter by company"""
    # print(sys._getframe().f_code.co_name, company, quarter)
    try:
        # open quarter
        timestamp = rightNow()
        browser.get(quarter[0])
        url = browser.current_url

        # select company info
        types = ['Dados da Empresa', 'Relatório da Administração/Comentário do Desempenho', 'Notas Explicativas', 'Comentário Sobre o Comportamento das Projeções Empresariais', 'Proposta de Orçamento de Capital', 'Outras Informações que a Companhia Entenda Relevantes', 'Pareceres e Declarações']
        type = types[0]
        xpathQuadro = '//*[@id="cmbGrupo"]'
        selectBox = wait.until(EC.element_to_be_clickable((By.XPATH, xpathQuadro)))
        selectBox = Select(browser.find_element(By.XPATH, xpathQuadro))
        selectBox.select_by_visible_text(type)

        # enter frame
        xpathFrame = '//*[@id="iFrameFormulariosFilho"]'
        iframe = wait.until(EC.element_to_be_clickable((By.XPATH, xpathFrame)))
        iframe = browser.find_elements(By.XPATH, xpathFrame)
        browser.switch_to.frame(iframe[0])
        multiplier = wText('//*[@id="UltimaTabela"]/table/tbody[1]/tr[1]/td[1]/b')
        if 'Mil' in multiplier:
            multiplier = 1000
        else:
            multiplier = 1
        on = wText('//*[@id="QtdAordCapiItgz_1"]').replace('.', '')
        on = int(on) * multiplier
        pn = wText('//*[@id="QtdAprfCapiItgz_1"]').replace('.', '')
        pn = int(pn) * multiplier
        onTes = wText('//*[@id="QtdAordTeso_1"]').replace('.', '')
        onTes = int(onTes) * multiplier
        pnTes = wText('//*[@id="QtdAprfTeso_1"]').replace('.', '')
        pnTes = int(pnTes) * multiplier

        # get values from csv
        file = company[5] + ' ' + datetime.strptime(quarter[2], '%d/%m/%Y').strftime('%Y %m %d')
        statementSheet = csv_to_list(file)

        statementSheet.append(['AÇÕES ON', 'ON', on, 'Ações Ordinárias', quarter[2], 'Capital Integralizado', company[0], quarter[0]])
        statementSheet.append(['AÇÕES PN', 'PN', pn, 'Ações Preferenciais', quarter[2], 'Capital Integralizado', company[0], quarter[0]])
        statementSheet.append(['AÇÕES ON', 'ON', onTes, 'Ações Ordinárias', quarter[2], 'Ações em Tesouraria', company[0], quarter[0]])
        statementSheet.append(['AÇÕES PN', 'PN', pnTes, 'Ações Preferenciais', quarter[2], 'Ações em Tesouraria', company[0], quarter[0]])

        # order
        statementSheet.sort(key=lambda x: x[0], reverse=False)
        statementSheet.sort(key=lambda x: datetime.strptime(x[4], '%d/%m/%Y'), reverse=True)

        # get sheet
        quartersSheet = cSpreadsheet.worksheet(quarter[2])

        # save statementSheet to sheet
        statementSheet.insert(0, superHeaders)

        statementSheetUnique = []
        for line in statementSheet:
            try:
                line[2] = int(line[2])
            except:
                pass

        # legacy cleanup
        for line in statementSheet:
            try:
                if company[2]:
                    if str(line[0]) == company[2] + '3' or str(line[0]) == company[2] + '4':
                        statementSheet.remove(line)
                    if 'TICK' in str(line[0]):
                        statementSheet.remove(line)
                else:
                    if 'TICK' in str(line[0]):
                        statementSheet.remove(line)
                if int(line[0]) == 3 or int(line[0]) == 4:
                    statementSheet.remove(line)
            except:
                pass
        for line in statementSheet:
            try:
                if company[2]:
                    if str(line[0]) == company[2] + '3' or str(line[0]) == company[2] + '4':
                        statementSheet.remove(line)
                    if 'TICK' in str(line[0]):
                        statementSheet.remove(line)
                else:
                    if 'TICK' in str(line[0]):
                        statementSheet.remove(line)
                if int(line[0]) == 3 or int(line[0]) == 4:
                    statementSheet.remove(line)
            except:
                pass

        for line in statementSheet:
            if line not in statementSheetUnique:
                statementSheetUnique.append(line)
        update = sheetUpdate(quartersSheet, statementSheetUnique, 1, 'RAW')
        statementSheetUnique.pop(0)
        time.sleep(sleep)

        # save also to csv
        file = company[5] + ' ' + datetime.strptime(quarter[2], '%d/%m/%Y').strftime('%Y %m %d')
        statementSheet = list_to_csv(file, statementSheetUnique)

        return statementSheet
    except Exception as e:
        return False
def getCompanyStatementsFIXITUP(company, *fromBigData):
    """load every financial statement for every quarter in company list of quarters"""
    # print(sys._getframe().f_code.co_name, company)
    try:
        companyQuarters, companySheetIndex = getCompanyQuartersSheet(company)

        """spreadsheet titles"""
        cSpreadsheet = gsheet.open_by_url(company[7])

        quartersSheets = []
        for worksheet in cSpreadsheet.worksheets():
            quartersSheets.append(worksheet.title)
        quartersSheets = listSubtract(quartersSheets, defaultsSheets)

        companyStatements = []
        for q, quarter in enumerate(companyQuarters):
            # companyStatements = getStatementFromQuartersFIXITUP(company, companyQuarters, sheet)
            try:
                quarter[2] = datetime.strptime(quarter[2], '%d/%m/%Y').strftime('%d/%m/%Y')

                # get data for quarter and insert in sheet
                statementSheet = getQuarterFIXITUP(company, quarter)

                cSheet = cSpreadsheet.worksheet(quarter[2])

                cell_list = cSheet.append_rows(statementSheet, value_input_option='RAW', insert_data_option='INSERT_ROWS')
                companyStatements.append(statementSheet)
                print('report valor adicionado', len(companyQuarters) - q, quarter[2], company[0], company[7] + "/edit#gid=" + str(cSheet.id))
            except Exception as e:
                print(e)
        time.sleep(sleep)

        reportAppend = [item for sublist in companyStatements for item in sublist]

        quartersSheet = cSpreadsheet.worksheet('reports')
        cell_list = quartersSheet.append_rows(reportAppend, value_input_option='RAW', insert_data_option='INSERT_ROWS')
        action = '2 QUAR'
        timestamp = sheetLog(company, action, sys._getframe().f_code.co_name)

        print(len(companyQuarters), 'quarters', company[0], company[7])
        return companyStatements
    except Exception as e:
        trouble(e, sys._getframe().f_code.co_name)
def getCompanyStatements(company, *fromBigData):
    """load every financial statement for every quarter in company list of quarters"""
    # print(sys._getframe().f_code.co_name, company)
    try:
        """ get company quarters from company index worksheet"""
        companyQuarters, companySheetIndex = getCompanyQuartersSheet(company)

        """spreadsheet titles"""
        cSpreadsheet = gsheet.open_by_url(company[7])

        quartersSheets = []
        for worksheet in cSpreadsheet.worksheets():
            quartersSheets.append(worksheet.title)
        quartersSheets = listSubtract(quartersSheets, defaultsSheets)
        quartersSheets.sort(key=lambda x: datetime.strptime(x, '%d/%m/%Y'), reverse=True)

        """ some ways it may be necessary to get full statements all company quarters """
        companyStatements = getStatementFromQuarters(company, companyQuarters, quartersSheets, cSpreadsheet)

        """ reorder report worksheets in spreadsheet """
        sheetsOrder = listMerge(defaultsSheets, quartersSheets)
        # sheetsOrder = cSpreadsheet.reorder_worksheets(sheetsOrder)
        print('reorder report worksheets in spreadsheet')

        """organize/cleanup big report from companyStatements from companyQuarters"""
        cReports = []
        for r, report in enumerate(companyStatements):
            for l, line in enumerate(report):
                cReports.append(line)

        headers = cReports.pop(0)
        cReports.sort(key=lambda x: x[0], reverse=False)
        cReports.sort(key=lambda x: datetime.strptime(x[4], '%d/%m/%Y'), reverse=True)
        cReports.insert(0, headers)

        # update to reports
        time.sleep(sleep)
        cReportWorksheet = cSpreadsheet.worksheet('reports')
        cReports = list_to_csv(company[5], cReports)
        update = sheetUpdate(cReportWorksheet, cReports, 2, 'USER_ENTERED')

        print(len(companyQuarters), 'quarters', company[0], company[7])
        return companyStatements
    except Exception as e:
        trouble(e, sys._getframe().f_code.co_name)
def getCompanyStatementsNumberOfStocks(company, *fromBigData):
    """temp"""
    # print(sys._getframe().f_code.co_name, company)
    try:
        """ get company quarters from company index worksheet"""
        companyQuarters, companySheetIndex = getCompanyQuartersSheet(company)

        """spreadsheet titles"""
        cSpreadsheet = gsheet.open_by_url(company[7])

        quartersSheets = []
        for worksheet in cSpreadsheet.worksheets():
            quartersSheets.append(worksheet.title)
        quartersSheets = listSubtract(quartersSheets, defaultsSheets)
        quartersSheets.sort(key=lambda x: datetime.strptime(x, '%d/%m/%Y'), reverse=True)

        """ some ways it may be necessary to get full statements all company quarters """
        # companyStatements = getStatementFromQuarters(company, companyQuarters, quartersSheets, cSpreadsheet)
        companyStatements = getStatementFromQuartersNumberOfStocks(company, companyQuarters, quartersSheets, cSpreadsheet)
        if companyStatements == False:
            return False
        """ reorder report worksheets in spreadsheet """
        # sheetsOrder = listMerge(defaultsSheets, quartersSheets)
        # sheetsOrder = cSpreadsheet.reorder_worksheets(sheetsOrder)
        print('reorder report worksheets in spreadsheet')


        """organize/cleanup big report from companyStatements from companyQuarters"""
        cReports = []
        for r, report in enumerate(companyStatements):
            for l, line in enumerate(report):
                cReports.append(line)

        # headers = cReports.pop(0)
        cReports.sort(key=lambda x: x[0], reverse=False)
        cReports.sort(key=lambda x: datetime.strptime(x[4], '%d/%m/%Y'), reverse=True)
        # cReports.insert(0, headers)

        # update to reports
        time.sleep(sleep)
        cReportWorksheet = cSpreadsheet.worksheet('reports')

        cReportsUnique = []
        for line in cReports:
            if line not in cReportsUnique:
                cReportsUnique.append(line)
        update = sheetUpdate(cReportWorksheet, cReportsUnique, 2, 'USER_ENTERED')
        cReports = list_to_csv(company[5], cReportsUnique)

        print(len(companyQuarters), 'quarters', company[0], company[7])
        return companyStatements
    except Exception as e:
        trouble(e, sys._getframe().f_code.co_name)
# quarter functions

# statements
def companyBigDataBuild(company):
    """get and prepare bigData for company"""
    # print(sys._getframe().f_code.co_name, company)
    try:
        # 1 rebuild bigData from reports
        bigData = []
        bigData, uDemonstrativos = companyBigDataRebuild(company)
        if not bigData:
            companyQuarters = getCompanyQuarters(company)
            if companyQuarters:
                companyStatements = getCompanyStatements(company, True)
            bigData = []
            for r, report in enumerate(companyStatements):
                for l, line in enumerate(report):
                    bigData.append(line)
            bigData, uDemonstrativos = companyBigDataRebuild(company)

        return bigData

    except Exception as e:
        trouble(e, sys._getframe().f_code.co_name)
def companyAnalyticsBuild(company, bigData):
    """get and prepare analytics data for company"""
    # print(sys._getframe().f_code.co_name, company)
    try:
        # 6. For each report in bigdata, parse and translate to fundamentalist analysis
        analytics, analyticsRefused = bigDataAnalyticsTranslate(bigData)
        analytics = bigDataAnalyticsFundamentalistic(company, analytics)

        try:
            for l, line in enumerate(analytics):
                analytics[l][4] = datetime.strptime(line[4], '%d/%m/%Y')
        except:
            pass

        # 7. Cleanup and sort
        analytics = bigDataAnalyticsFormatCleanup(analytics)

        # 8. new values and calculations - get analytics in plain pure quarters
        analytics = bigDataAnalyticsCalculations(analytics)

        try:
            for l, line in enumerate(analytics):
                analytics[l][4] = datetime.strftime(line[4], '%d/%m/%Y')

        except:
            pass

        return analytics
    except Exception as e:
        trouble(e, sys._getframe().f_code.co_name)

def companyBigData(company):
    """big data management in subroutines"""
    print(sys._getframe().f_code.co_name, company[0], company[7])
    try:
        # legacy clewanup
        sheets = ['bigdata', 'uberReport', 'anual', 'trimestral', '12 meses']
        for s in sheets:
            try:
                ws = cSpreadsheet.worksheet(s)
                cSpreadsheet.del_worksheet(ws)
            except Exception as e:
                pass

        # bigData part 1 = build up
        bigData = companyBigDataBuild(company)

        # timestamp = sheetLog(company, ' ', sys._getframe().f_code.co_name)
        # # update action message for log
        # action = '3 STAT'
        # action = updateListagem(company, action)

        # bigData part 2 = analytics
        analytics = companyAnalyticsBuild(company, bigData)

        # 10. rTrimestral + rAnual
        cat, anos = bigDataAnalyticsDict(analytics)
        analytics, analyticsYOY, analyticsAnual = analyticsYOYear(analytics)

        # analytics = createUberReport(company, analytics, rightNow())

        analytics = analyticsPandas(company, analytics, 'trimestral', 1, cat)
        analyticsAnual = analyticsPandas(company, analyticsAnual, 'anual', 1, cat)
        analyticsYOY = analyticsPandas(company, analyticsYOY, '12 meses', 1, cat)
        return analytics
    except Exception as e:
        trouble(e, sys._getframe().f_code.co_name)

# statements functions
def companyBigDataRebuild(company):
    """re build big data from company sheet report"""
    # print(sys._getframe().f_code.co_name)
    try:
        # 1 open report list from bovespa sheet
        companyReportsSheet = getSheet(company[7], 'reports')
        reportList = companyReportsSheet.get_all_values()
        headers = reportList.pop(0)
        if not reportList:
            bigData = []
            uDemonstrativos = []
            return bigData, uDemonstrativos

        # 0 clean up just in case
        for l, line in enumerate(reportList):
            try:
                isDate = datetime.strptime(line[4], '%d/%m/%Y')
            except Exception as e:
                reportList.remove(line)

        # 1.1. transform reportList in bigData
        uDemonstrativos = []
        for l, line in enumerate(reportList):
            if line[3] not in uDemonstrativos:
                uDemonstrativos.append(line[3])

        # 1.2. rebuild bigData
        bigData = []
        for d, demo in enumerate(uDemonstrativos):
            bigData.append([])
            for l, line in enumerate(reportList):
                if demo == line[3]:
                    # small google sheets corrections and transformations
                    if line[0][0] =='0': line[0] = line[0][1:]
                    line[2] = line[2].replace('.','')
                    # create bigdata
                    bigData[d].append(line)
        return bigData, uDemonstrativos

    except Exception as e:
        trouble(e, sys._getframe().f_code.co_name)
def companyBigDataRaw(bigData):
    """just re-create list"""
    # print(sys._getframe().f_code.co_name)
    try:
        bigDataRAW = deepcopy(bigData)

        uDemonstrativos = []
        for d, demo in enumerate(bigDataRAW):
            for l, line in enumerate(demo):
                if line[3] not in uDemonstrativos:
                    uDemonstrativos.append(line[3])

        bigData = []
        for u, udemo in enumerate(uDemonstrativos):
            bigData.append([])
            for d, demo in enumerate(bigDataRAW):
                for l, line in enumerate(demo):
                    if line[3] == udemo:
                        bigData[d].append(line)


        return bigData, bigDataRAW
    except Exception as e:
        trouble(e, sys._getframe().f_code.co_name)
def bigDataRebuild(bigDataRAW, uDemonstrativos):
    """organize bigdata by reports"""
    # print(sys._getframe().f_code.co_name)
    try:
        bigData = []
        for d, demonstrativo in enumerate(uDemonstrativos):
            bigData.append([])
            for r, report in enumerate(bigDataRAW):
                for l, line in enumerate(report):
                    for i, item in enumerate(line):
                        if item == demonstrativo:
                            bigData[d].append(line)
        return bigData
    except Exception as e:
        trouble(e, sys._getframe().f_code.co_name)
def bigDataUnique(bigData):
    """remove level from some items. This is very specific and prone to erros in re-leveling"""
    # print(sys._getframe().f_code.co_name)
    try:
        unique = []
        for d, demo in enumerate(bigData):
            unique.append([])
            for l, line in enumerate(demo):
                for i, item in enumerate(line):
                    if bigData[d][l][i] == demo[0][3] and line[1] not in unique[d]:
                        unique[d].append(line[1].strip())

        # get unique descrição for each demo
        for d, demo in enumerate(bigData):
            unique[d] = list(dict.fromkeys(unique[d]))
            unique[d] = [[x] for x in unique[d]]

        for ud, udescricao in enumerate(unique):
            for ui, uitem in enumerate(udescricao):  # check if this item is in bigdata. If it is, append bigdata number to lista, then lista to unique description
                lista = []
                for l, line in enumerate(bigData[ud]):
                    if uitem[0] == line[1]:
                        z = 2  # level 3
                        x = line[0].rsplit('.', z)
                        if len(x) > z:
                            del x[-1]
                            y = '.'.join(x) + '.*'
                        else:
                            y = line[0]
                        if y not in lista:
                            lista.append(y)
                if lista:
                    lista.sort()
                    unique[ud][ui].insert(0, lista)

        uniqueExpand = []
        for d, demo in enumerate(unique):
            uniqueExpand.append([])
            for l, line in enumerate(demo):
                for c, code in enumerate(line[0]):
                    lista = []
                    lista.append(code)
                    lista.append(line[1])
                    uniqueExpand[d].append(lista)

        uniqueCompact = []
        for d, demo in enumerate(unique):
            uniqueCompact.append([])
            for l, line in enumerate(demo):
                if len(line[0]) == 1:
                    lista = []
                    lista.append(line[0][0])
                    lista.append(line[1])
                    uniqueCompact[d].append(lista)

        return unique, uniqueExpand, uniqueCompact
    except Exception as e:
        trouble(e, sys._getframe().f_code.co_name)
def bigDataNewCodeDatas(bigData, uniqueExpand, uDemonstrativos):
    """brute force mode of getting some unique items"""
    # print(sys._getframe().f_code.co_name)
    try:
        for d, demo in enumerate(bigData):
            for l, line in enumerate(demo):
                for ul, uline in enumerate(uniqueExpand[d]):
                    x = line[0].split('.')
                    y = uline[0].split('.')
                    del x[-1]
                    del y[-1]
                    x1 = '.'.join(x)
                    y1 = '.'.join(y)

                    if line[1] == uline[1] and x1 == y1:
                        bigData[d][l][0] = uline[0]

            # 5.b  get unique data for each data in bigdata
            datas = []
            for ud, udemo in enumerate(uDemonstrativos):
                for d, demo in enumerate(bigData):
                    for l, line in enumerate(demo):
                        if bigData[d][l][3] == udemo and line[4] not in datas:
                            datas.append(line[4].strip())

            datas.sort(key=lambda x: datetime.strptime(x, '%d/%m/%Y'), reverse=True)
        return datas
    except Exception as e:
        trouble(e, sys._getframe().f_code.co_name)

def dreCodes():
    try:
        dreCode = []
        # ativos
        dreCode.append(['1', 'Ativos Totais'])

        # ativos curto prazo ACP
        dreCode.append(['1.01', 'Ativos Circulantes Curto Prazo ACP'])
        dreCode.append(['1.01.01', 'Caixa e Equivalentes de Caixa - Caixa ACP', 'Caixa'])
        dreCode.append(['1.01.02', 'Aplicações Financeiras - Aplicações ACP', 'Aplicações Financeiras'])
        dreCode.append(['1.01.03', 'Contas a Receber - Contas ACP', 'Contas a Receber'])
        dreCode.append(['1.01.04', 'Estoques - Estoque ACP', 'Estoque'])
        dreCode.append(['1.01.05', 'Ativos Biológicos - Bio ACP', 'Biológicos'])
        dreCode.append(['1.01.06', 'Tributos a Recuperar - Impostos ACP', 'Tributos'])
        dreCode.append(['1.01.07', 'Despesas Antecipadas - Despesas ACP', 'Despesas Antecipadas'])
        dreCode.append(['1.01.08', 'Outros Ativos Circulantes - Outros ACP', 'Outros Ativos Circulantes'])

        # Ativo Não Circulante ALP
        dreCode.append(['1.02', 'Ativos Não Circulantes ALP'])
        dreCode.append(['1.02.01', 'Aplicações Financeiras a Longo Prazo - Aplicações ALP', 'Realizáveis'])
        dreCode.append(['1.02.01.01', 'Aplicações Financeiras Avaliadas a Valor Justo através do Resultado ALP', 'Valor Justo através do Resultado'])
        dreCode.append(['1.02.01.02', 'Aplicações Financeiras Avaliadas a Valor Justo através de Outros Resultados Abrangentes ALP', 'Outros Resultados Abrangentes'])
        dreCode.append(['1.02.01.03', 'Aplicações Financeiras Avaliadas ao Custo Amortizado ALP', 'Custo Amortizado'])
        dreCode.append(['1.02.01.04', 'Contas a Receber - Contas ALP', 'Contas a Receber'])
        dreCode.append(['1.02.01.05', 'Estoques - Estoque ALP', 'Estoque'])
        dreCode.append(['1.02.01.06', 'Ativos Biológicos - Bio ALP', 'Biológicos'])
        dreCode.append(['1.02.01.07', 'Tributos Diferidos - Impostos ALP', 'Tributos'])
        dreCode.append(['1.02.01.08', 'Despesas Antecipadas - Despesas ALP', 'Despesas Antecipadas'])
        dreCode.append(['1.02.01.09', 'Créditos com Partes Relacionadas - Créditos ALP', 'Créditos '])
        dreCode.append(['1.02.01.10', 'Outros Ativos Não Circulantes - Outros ALP', 'Outros Ativos Não Circulantes'])
        dreCode.append(['1.02.02', 'Investimentos - Não Capex LP', 'Investimentos'])
        dreCode.append(['1.02.03', 'Imobilizado - Capex LP', 'Imobilizado'])
        dreCode.append(['1.02.04', 'Intangível - Capex LP', 'Intangível'])

        # passivos
        dreCode.append(['2', 'Passivos Totais'])

        # passivos circulantes PCP
        dreCode.append(['2.01', 'Passivos Circulantes Curto Prazo PCP'])
        dreCode.append(['2.01.01', 'Obrigações Sociais e Trabalhistas - Custo Trabalhista PCP', 'Obrigações Sociais e Trabalhistas'])
        dreCode.append(['2.01.02', 'Fornecedores PCP', 'Fornecedores'])
        dreCode.append(['2.01.02.01', 'Fornecedores Nacionais PCP', 'Nacional'])
        dreCode.append(['2.01.02.02', 'Fornecedores Estrangeiros PCP', 'Estrangeiros'])
        dreCode.append(['2.01.03', 'Obrigações Fiscais - Impostos PCP', 'Obrigações Fiscais'])
        dreCode.append(['2.01.03.01', 'Impostos Federais PCP', 'Federal'])
        dreCode.append(['2.01.03.02', 'Impostos Estaduais PCP', 'Estadual'])
        dreCode.append(['2.01.03.03', 'impostos Municipais PCP', 'Municipal'])
        dreCode.append(['2.01.04', 'Empréstimos e Financiamentos - Dívida PCP', 'Empréstimos e Financiamentos'])
        dreCode.append(['2.01.04.01', 'Empréstimos PCP', 'Empréstimos e Financiamentos'])
        dreCode.append(['2.01.04.01.01', 'Empréstimos em Reais PCP', 'Nacional'])
        dreCode.append(['2.01.04.01.02', 'Empréstimos em Dólar e Outros PCP', 'Estrangeira'])
        dreCode.append(['2.01.04.02', 'Debêntures PCP', 'Debêntures'])
        dreCode.append(['2.01.04.03', 'Arrendamentos PCP', 'Arrendamentos'])
        # xxxfsxxx calcular 2.01.04.0X por diferença dos subitens
        dreCode.append(['2.01.05', 'Outras Obrigações - Outros PCP', 'Outras Obrigações'])
        dreCode.append(['2.01.06', 'Provisões PCP', 'Provisões'])
        dreCode.append(['2.01.07', 'Passivos sobre Ativos Não-Correntes a Venda e Descontinuados - Descontinuado PCP', 'Passivos sobre Ativos Não-Correntes a Venda e Descontinuados'])

        # passivos não circulantes PCP
        dreCode.append(['2.02', 'Passivo Não Circulantes Longo Prazo PLP'])
        dreCode.append(['2.02.03', 'Tributos Diferidos - Impostos PLP', ''])
        dreCode.append(['2.02.01', 'Empréstimos e Financiamentos - Dívida PLP', 'Empréstimos e Financiamentos'])
        dreCode.append(['2.02.01.01', 'Empréstimos PLP', 'Empréstimos e Financiamentos'])
        dreCode.append(['2.02.01.01.01', 'Empréstimos em Reais PLP', 'Nacional'])
        dreCode.append(['2.02.01.01.02', 'Empréstimos em Dólar e Outros PLP', 'Estrangeira'])
        dreCode.append(['2.02.01.02', 'Debêntures PLP', 'Debêntures'])
        dreCode.append(['2.02.01.03', 'Arrendamentos PLP', 'Arrendamentos'])
        dreCode.append(['2.02.02', 'Outras Obrigações - Outros PLP', ''])
        dreCode.append(['2.02.04', 'Provisões PLP', ''])
        dreCode.append(['2.02.05', 'Passivos sobre Ativos Não-Correntes a Venda e Descontinuados - Descontinuados PLP', ''])
        dreCode.append(['2.02.06', 'Lucros e Receitas a Apropriar - Lucros Futuros PLP', ''])

        # patrimônio líquido
        dreCode.append(['2.03', 'Patrimônio Líquido'])
        dreCode.append(['2.03.01', 'Capital Social Realizado - PL', 'Capital Social Realizado'])
        dreCode.append(['2.03.02', 'Reservas de Capital - PL', 'Reservas de Capital'])
        dreCode.append(['2.03.03', 'Reservas de Reavaliação - PL', 'Reservas de Reavaliação'])
        dreCode.append(['2.03.04', 'Reservas de Lucros - PL', 'Reservas de Lucros'])
        dreCode.append(['2.03.05', 'Lucros/Prejuízos Acumulados - PL', 'Acumulados'])
        dreCode.append(['2.03.06', 'Ajustes de Avaliação Patrimonial - PL', 'Ajustes de Avaliação Patrimonial'])
        dreCode.append(['2.03.07', 'Ajustes Acumulados de Conversão - PL', 'Ajustes Acumulados de Conversão'])
        dreCode.append(['2.03.08', 'Outros Resultados Abrangentes - PL', 'Outros Resultados Abrangentes'])
        dreCode.append(['2.03.09', 'Participação dos Acionistas Não Controladores - PL', 'Participação dos Acionistas Não Controladores'])

        # DRE Resultados do Exercício
        dreCode.append(['3.01', 'Receita Bruta'])
        dreCode.append(['3.02', 'Custo de Produção'])
        dreCode.append(['3.03', 'Resultado Bruto'])
        dreCode.append(['3.04', 'Despesas Operacionais'])
        dreCode.append(['3.04.01', 'Despesas com Vendas - DGV', 'Venda'])
        dreCode.append(['3.04.02', 'Despesas Gerais e Administrativas - DGA', 'Administrativas'])
        dreCode.append(['3.04.02', 'Despesas Gerais e Administrativas - DGA', 'Pessoal'])
        # calcular 3.04.0X outros pela diferença dos subitens
        dreCode.append(['3.05', 'EBIT LAJIR - Resultado Antes do Resultado Financeiro e dos Tributos'])
        dreCode.append(['3.06', 'Resultado Financeiro'])
        dreCode.append(['3.07', 'Resultado Antes dos Tributos sobre o Lucro'])
        dreCode.append(['3.08', 'Imposto de Renda e Contribuição Social sobre o Lucro'])
        dreCode.append(['3.09', 'Resultado Líquido'])
        dreCode.append(['3.10', 'Resultado Descontinuado'])
        dreCode.append(['3.11', 'Resultado Líquido Consolidado'])

        # DFC Fluxo de Caixa
        dreCode.append(['6.01', 'Caixa da Operação'])
        dreCode.append(['6.01.01', 'Caixa Gerado nas Operações', 'Operações'])
        # get depreciação e amorticação from text inside 6.01.01.X -- do this outside this function, inside parent function
        dreCode.append(['6.01.02', 'Variações no Patrimônio (ativos e passivos)', 'Ativos e Passivos'])
        dreCode.append(['6.01.03', 'Outros', 'Outros'])
        dreCode.append(['6.02', 'Caixa de Investimento (Capex)'])
        # get imobilizado e intangível from text inside 6.02.X -- do this outside this function, inside parent function
        dreCode.append(['6.03', 'Caixa de Financiamento (Financeiro)'])
        dreCode.append(['6.04', 'Variação Cambial'])
        dreCode.append(['6.05', 'Quebra de Caixa'])

        return dreCode
    except Exception as e:
        trouble(e, sys._getframe().f_code.co_name)
def dreCodesAll():
    try:
        dreCodes = []
        # DRE
        dreCodes.append([['deprecia'], '6.01.01', 10, '6.01.01.01', 'DA - Depreciação e Amortização'])
        dreCodes.append([['juro'], '6', 7, '6.01.01.03', 'Despesas com Juros'])
        dreCodes.append([['conta'], '6.01.02', 10, '6.01.02.01', 'Contas'])
        dreCodes.append([['estoque'], '6.01.02', 7, '6.01.02.02', 'Estoques'])
        dreCodes.append([['fornecedor'], '6.01.02', 7, '6.01.02.03', 'Fornecedores'])
        dreCodes.append([['juro'], '6', 7, '6.01.01.03', 'Despesas com Juros'])
        # dreCodes.append([['deprecia'], '6.01.01', 10, '6.01.01.01', 'DA - Depreciação e Amortização' + ' <- ' + line[1].lower()])
        # dreCodes.append([['juro'], '6', 7, '6.01.01.03', 'Despesas com Juros' + ' <- ' + line[1].lower()])
        # dreCodes.append([['conta'], '6.01.02', 10, '6.01.02.01', 'Contas' + ' <- ' + line[1].lower()])
        # dreCodes.append([['estoque'], '6.01.02', 7, '6.01.02.02', 'Estoques' + ' <- ' + line[1].lower()])
        # dreCodes.append([['fornecedor'], '6.01.02', 7, '6.01.02.03', 'Fornecedores' + ' <- ' + line[1].lower()])
        # dreCodes.append([['juro'], '6', 7, '6.01.01.03', 'Despesas com Juros' + ' <- ' + line[1].lower()])
        dreCodes.append([['imobil'], '6.02', 7, '6.02.01', 'Capex no Imobilizado'])
        dreCodes.append([['intang'], '6.02', 7, '6.02.02', 'Capex no Intangível'])

        # Valor Adicionado
        dreCodes.append([['receit'], '7', 4, '7.01', 'Receitas DVA'])
        dreCodes.append([['venda', 'mercador', 'produt', 'servi'], '7', 7, '7.01.01','Vendas de Mercadorias, Produtos e Serviços DVA'])
        dreCodes.append([['outra', 'receit'], '7', 7, '7.01.02', 'Outras Receitas DVA'])
        dreCodes.append([['receit', 'constru', 'ativ'], '7', 7, '7.01.03', 'Receita de Construção de Ativos Próprios DVA'])
        dreCodes.append([['liquida', 'duvidos'], '7', 7, '7.01.04', 'Créditos de Liquidação Duvidosa DVA'])
        dreCodes.append([['insum', 'adquirid', 'terceir'], '7', 4, '7.02', 'Insumos Adquiridos de Terceiros DVA'])
        dreCodes.append([['cust', 'prod', 'merc', 'serv', 'vendid'], '7', 7, '7.02.01', 'Custo de Produção DVA'])
        dreCodes.append([['materi', 'energ', 'serv', 'terceir'], '7', 7, '7.02.03', 'Custo de Materiais, Energia e outros DVA'])
        dreCodes.append([['valor', 'adicionad', 'brut'], '7', 4, '7.03', 'Valor Adicionado Bruto DVA'])
        dreCodes.append([['reten'], '7', 4, '7.04', 'Retenções DVA'])
        dreCodes.append([['deprecia', 'amortiza', 'exaust'], '7', 7, '7.04.01', 'Depreciação, Amortização e Exaustão DVA'])
        dreCodes.append([['valor', 'adicionad', 'líquido'], '7', 4, '7.05', 'Valor Adicionado Líquido DVA'])
        dreCodes.append([['valor', 'adicionad', 'recebid', 'transfer'], '7', 4, '7.06','Valor Adicionado Recebido em Transferência DVA'])
        dreCodes.append([['valor', 'adicionad', 'total', 'distribuir'], '7', 4, '7.07', 'Valor Adicionado Total a Distribuir DVA'])
        dreCodes.append([['valor', 'adicionad', ' do ', 'distribu'], '7', 4, '7.08', 'Distribuição do Valor Adicionado DVA'])
        dreCodes.append([['pesso'], '7.08', 7, '7.08.01', 'Pessoal DVA'])
        dreCodes.append([['pesso'], '7.08.01', 10, '7.08.01.01', 'Remuneração Direta DVA'])
        dreCodes.append([['benef'], '7.08.01', 10, '7.08.01.02', 'Benefícios DVA'])
        dreCodes.append([['feder', 'capit', 'terceir'], '7.08', 10, '7.08.02.01', 'Impostos Federais DVA'])
        dreCodes.append([['impost', 'tax', 'contribui'], '7', 7, '7.08.02', 'Impostos DVA'])
        dreCodes.append([['outr'], '7.08.01', 10, '7.08.01.04', 'Outros DVA'])
        dreCodes.append([['estad', 'capit', 'terceir'], '7.08', 10, '7.08.02.02', 'Impostos Estaduais DVA'])
        dreCodes.append([['municip', 'capit', 'terceir'], '7.08', 10, '7.08.02.03', 'Impostos Municipais DVA'])
        dreCodes.append([['remunera', 'capit', 'terceir'], '7.08', 7, '7.08.03', 'Remuneração dos Credores DVA'])
        dreCodes.append([['jur'], '7.08.03', 10, '7.08.03.01', 'Juros DVA'])
        dreCodes.append([['alugu'], '7.08.03', 10, '7.08.03.02', 'Aluguel DVA'])
        dreCodes.append([['outr'], '7.08.03', 10, '7.08.03.03', 'Outras Remunerações de Credores DVA'])
        dreCodes.append([['remunera', 'capit', 'pr'], '7.08', 7, '7.08.04', 'Remuneração dos Acionistas DVA'])
        dreCodes.append([['juro', 'capita', 'pr'], '7.08', 10, '7.08.04.01', 'Juros Sobre o Capital Próprio DVA'])
        dreCodes.append([['divid'], '7.08', 10, '7.08.04.02', 'Dividendos DVA'])
        dreCodes.append([['lucr', 'retid', 'per'], '7.08', 10, '7.08.04.03', 'Lucros Retidos no Período DVA'])
        dreCodes.append([['part', 'control', 'lucr', 'retid'], '7.08', 10, '7.08.04.04','Lucros Retidos por Não-Controladores no Período DVA'])
        dreCodes.append([['outr'], '7.08', 7, '7.08.05', 'Outros Valores Adicionados DVA'])

        return dreCodes

    except Exception as e:
        trouble(e, sys._getframe().f_code.co_name)
def dreCodesAllBut():
    try:
        dreCodes = []

        dreCodes.append([['amortiza'], ['deprecia', 'resultado'], '6.01.01', 10, '6.01.01.02','DA - Amortização'])
        # dreCodes.append([['amortiza'], ['deprecia', 'resultado'], '6.01.01', 10, '6.01.01.02','DA - Amortização' + ' <- ' + line[1].lower()])

        return dreCodes

    except Exception as e:
        trouble(e, sys._getframe().f_code.co_name)
def dreCodesAny():
    try:
        dreCodes = []

        # Caixa
        dreCodes.append([['imposto', 'tributo'], '6.01.02', 7, '6.01.02.04', 'Impostos'])
        dreCodes.append([['financiam', 'empr'], '6.01.02', 7, '6.01.02.05','Empréstimos e Financiamentos'])
        # dreCodes.append([['imposto', 'tributo'], '6.01.02', 7, '6.01.02.04', 'Impostos' + ' <- ' + line[1].lower()])
        # dreCodes.append([['financiam', 'empr'], '6.01.02', 7, '6.01.02.05','Empréstimos e Financiamentos' + ' <- ' + line[1].lower()])

        # Valor Agregado
        dreCodes.append([['f.g.t.s.', 'fgts'], '7.08.01', 10, '7.08.01.03', 'FGTS DVA'])

        return dreCodes

    except Exception as e:
        trouble(e, sys._getframe().f_code.co_name)


def bigDataAnalyticsTranslateUnique(line, uniqueLine):
    """translate unique generic statements in standart statements, one-by-one, line by line, according to patterns"""
    # print(sys._getframe().f_code.co_name)
    try:
        new = False

        return uniqueLine, new
    except Exception as e:
        trouble(e, sys._getframe().f_code.co_name)


def fMinus(a, b):
    """divide numerator by denominator"""
    # print(sys._getframe().f_code.co_name)
    try:
        a = float([i[2] for i in demo if i[0] == a][0])  # find and flatten
        b = float([i[2] for i in demo if i[0] == b][0])  # find and flatten
        r = float(a-b)
        return r
    except:
        r = float(0)
        return r
def fValue(n):
    """sum two parts"""
    # print(sys._getframe().f_code.co_name)
    try:
        r = float([i[2] for i in demo if i[0] == n][0])  # find and flatten
        return r
    except:
        r = float(0)
        return r
def fSum(*n):
    """sum two parts"""
    # print(sys._getframe().f_code.co_name)
    try:
        r = 0
        for x in n:
            x = float([i[2] for i in demo if i[0] == x][0])  # find and flatten
            r += float(x)
        return r
    except:
        r = float(0)
        return r
def fDivision(a, b):
    """divide numerator by denominator"""
    # print(sys._getframe().f_code.co_name)
    try:
        a = float([i[2] for i in demo if i[0] == a][0])  # find and flatten
        b = float([i[2] for i in demo if i[0] == b][0])  # find and flatten
        r = float(a/b)
        return r
    except:
        r = float(0)
        return r

def bigDataAnalyticsFundamentalistic(company, analytics):
    """make unique comparisons and formulas to fundamentalist analysis"""
    # print(sys._getframe().f_code.co_name)
    try:
        quarters = [q[4] for q in analytics]
        quarters = list(dict.fromkeys(quarters))
        for q, quarter in enumerate(quarters):
            global demo
            demo = [line for line in analytics if line[4] == quarter]

            formato = 'Análise Fundamentalista'
            demonstrativo = 'Fundamentos do Balanço Patrimonial'

            l = ['8.01', 'Passivos / Ativos', demonstrativo, formato]
            s = fSum('2.01', '2.02')
            t = fValue('1')
            if t != 0:
                r = s / t
                analytics.append([l[0], l[1], r, l[2], quarter, l[3], company[0]])

            l = ['8.01', 'Ativos / Passivos', demonstrativo, formato]
            s = fValue('1')
            t = fSum('2.01', '2.02')
            if t != 0:
                r = s / t
                analytics.append([l[0], l[1], r, l[2], quarter, l[3], company[0]])

            l = ['8.01.01', 'Passivos de Curto Prazo / Ativos', demonstrativo, formato]
            r = fDivision('2.01', '1')
            analytics.append([l[0], l[1], r, l[2], quarter, l[3], company[0]])

            l = ['8.01.02', 'Passivos de Longo Prazo / Ativos', demonstrativo, formato]
            r = fDivision('2.02', '1')
            analytics.append([l[0], l[1], r, l[2], quarter, l[3], company[0]])

            l = ['8.02', 'Equity Multiplier (Ativos / Patrimônio)', demonstrativo, formato]
            r = fDivision('1', '2.03')
            analytics.append([l[0], l[1], r, l[2], quarter, l[3], company[0]])

            l = ['8.03', 'Passivos / Patrimônio', demonstrativo, formato]
            s = fSum('2.01', '2.02')
            t = fValue('2.03')
            if t != 0:
                r = s / t
                analytics.append([l[0], l[1], r, l[2], quarter, l[3], company[0]])

            l = ['8.03.01', 'Passivos de Curto Prazo / Patrimônio', demonstrativo, formato]
            r = fDivision('2.01', '2.03')
            analytics.append([l[0], l[1], r, l[2], quarter, l[3], company[0]])

            l = ['8.03.01', 'Passivos de Longo Prazo / Patrimônio', demonstrativo, formato]
            r = fDivision('2.02', '2.03')
            analytics.append([l[0], l[1], r, l[2], quarter, l[3], company[0]])

            l = ['8.04', 'Proporção de Ativos de Longo Prazo / Ativos Totais', demonstrativo, formato]
            r = fDivision('1.02', '1')*100
            analytics.append([l[0], l[1], r, l[2], quarter, l[3], company[0]])

            l = ['8.05', 'Caixa e Equivalentes de Caixa (Valores Imprecisos?)', demonstrativo, formato]
            r = fSum('1.01.01', '1.01.02', '1.02.01.10')
            analytics.append([l[0], l[1], r, l[2], quarter, l[3], company[0]])

            l = ['8.06.01', 'Liquidez Corrente (Ativos de Curto Prazo / Passivos de Curto Prazo)', demonstrativo, formato]
            r = fDivision('1.01', '2.01')
            analytics.append([l[0], l[1], r, l[2], quarter, l[3], company[0]])

            l = ['8.06.02', 'Ativos de Longo Prazo / Passivos de Longo Prazo', demonstrativo, formato]
            r = fDivision('1.02', '2.02')
            analytics.append([l[0], l[1], r, l[2], quarter, l[3], company[0]])

            l = ['8.07', 'Capital de Giro', demonstrativo, formato]
            r = fMinus('1.01', '2.01')
            analytics.append([l[0], l[1], r, l[2], quarter, l[3], company[0]])

            l = ['8.11.03', 'Contas a Receber / Faturamento %', demonstrativo, formato]
            s = fSum('1.01.03', '1.02.01.04')
            t = fValue('3.01')
            if t != 0:
                r = s / t
                r = r * 100
                analytics.append([l[0], l[1], r, l[2], quarter, l[3], company[0]])

            l = ['8.11.04', 'Estoques / Faturamento %', demonstrativo, formato]
            s = fValue('3.01')
            t = fSum('1.01.04', '1.02.01.05')
            if t != 0:
                r = s / t
                analytics.append([l[0], l[1], r, l[2], quarter, l[3], company[0]])

            l = ['8.11.05', 'Ativos Biológicos / Faturamento %', demonstrativo, formato]
            s = fSum('1.01.05', '1.02.01.06''1.01.05', '1.02.01.06')
            t = fValue('3.01')
            if t != 0:
                r = s / t
                r = r * 100
                analytics.append([l[0], l[1], r, l[2], quarter, l[3], company[0]])

            l = ['8.11.06', 'Tributos / Faturamento %', demonstrativo, formato]
            s = fSum('1.01.06', '1.02.01.07')
            t = fValue('3.01')
            if t != 0:
                r = s / t
                r = r * 100
                analytics.append([l[0], l[1], r, l[2], quarter, l[3], company[0]])

            l = ['8.11.07', 'Coeficiente de Retorno (Lucros / Ativos) %', demonstrativo, formato]
            r = fDivision('3.11', '1') * 100
            analytics.append([l[0], l[1], r, l[2], quarter, l[3], company[0]])

            l = ['8.12', 'Patrimônio Imobilizado', demonstrativo, formato]
            r = fSum('1.02.02', '1.02.03', '1.02.04')
            analytics.append([l[0], l[1], r, l[2], quarter, l[3], company[0]])

            l = ['8.12.01', 'Índice de Imobilização do Patrimônio (Patrimônio Imobilizado / Patrimônio Líquido)', demonstrativo, formato]
            s = fSum('1.02.02', '1.02.03', '1.02.04')
            t = fValue('2.03')
            if t != 0:
                r = s / t
                analytics.append([l[0], l[1], r, l[2], quarter, l[3], company[0]])

            l = ['8.13', 'Proporção de Passivos de Longo Prazo', demonstrativo, formato]
            r = fDivision('2.01', '2.02')
            analytics.append([l[0], l[1], r, l[2], quarter, l[3], company[0]])

            l = ['8.14.01', 'Dívida em Reais', demonstrativo, formato]
            r = fSum('2.01.04.01.01', '2.01.04.02', '2.01.04.03', '2.02.01.01.01', '2.02.01.02', '2..02.01.03')
            analytics.append([l[0], l[1], r, l[2], quarter, l[3], company[0]])

            l = ['8.14.02', 'Dívida em Dólares', demonstrativo, formato]
            r = fSum('2.01.04.01.02', '2.02.01.01.02')
            analytics.append([l[0], l[1], r, l[2], quarter, l[3], company[0]])

            l = ['8.14.03', 'Proporção de Nacionalização da Dívida', demonstrativo, formato]
            s = fSum('2.01.04.01.01', '2.01.04.02', '2.01.04.03', '2.02.01.01.01', '2.02.01.02', '2..02.01.03')
            t = fSum('2.01.04.01.02', '2.02.01.01.02')
            if s + t != 0:
                r = s / (s + t)
                analytics.append([l[0], l[1], r, l[2], quarter, l[3], company[0]])

            l = ['8.14.04', 'Dívida de Curto Prazo', demonstrativo, formato]
            r = fSum('2.01.04.01.01', '2.01.04.01.02', '2.01.04.02', '2.01.04.03')
            analytics.append([l[0], l[1], r, l[2], quarter, l[3], company[0]])

            l = ['8.14.05', 'Dívida de Longo Prazo', demonstrativo, formato]
            r = fSum('2.02.01.01.01', '2.02.01.01.02', '2.02.01.02', '2.02.01.03')
            analytics.append([l[0], l[1], r, l[2], quarter, l[3], company[0]])

            l = ['8.14.06', 'Proporção da Dívida de Curto Prazo', demonstrativo, formato]
            s = fSum('2.01.04.01.01', '2.01.04.01.02', '2.01.04.02', '2.01.04.03')
            t = fSum('2.02.01.01.01', '2.02.01.01.02', '2.02.01.02', '2.02.01.03')
            if s + t != 0:
                r = s / (s + t)
                analytics.append([l[0], l[1], r, l[2], quarter, l[3], company[0]])

            l = ['8.14.07', 'Dívida Bruta', demonstrativo, formato]
            r = fSum('2.01.04.01.01', '2.01.04.01.02', '2.01.04.02', '2.01.04.03', '2.02.01.01.01', '2.02.01.01.02', '2.02.01.02', '2.02.01.03')
            analytics.append([l[0], l[1], r, l[2], quarter, l[3], company[0]])

            l = ['8.14.08', 'Caixa e Equivalentes de Caixa (Valores Imprecisos?)', demonstrativo, formato]
            r = fSum('1.01.01', '1.01.02', '1.02.01.10')
            analytics.append([l[0], l[1], r, l[2], quarter, l[3], company[0]])

            l = ['8.14.09', 'Dívida Líquida (Valores Imprecisos?)', demonstrativo, formato]
            s = fSum('2.01.04.01.01', '2.01.04.01.02', '2.01.04.02', '2.01.04.03', '2.02.01.01.01', '2.02.01.01.02', '2.02.01.02', '2.02.01.03')
            t = fSum('1.01.01', '1.01.02', '1.02.01.10')
            if t != 0:
                r = s - t
                analytics.append([l[0], l[1], r, l[2], quarter, l[3], company[0]])

            l = ['8.14.08', 'Dívida Bruta / Patrimônio Líquido', demonstrativo, formato]
            s = fSum('2.01.04.01.01', '2.01.04.01.02', '2.01.04.02', '2.01.04.03', '2.02.01.01.01', '2.02.01.01.02', '2.02.01.02', '2.02.01.03')
            t = fValue('2.03')
            if t != 0:
                r = s / t
                analytics.append([l[0], l[1], r, l[2], quarter, l[3], company[0]])

            l = ['8.14.09', 'Dívida Líquida / EBITDA (Valores Imprecisos?)', demonstrativo, formato]
            r = fSum('1.01.01', '1.01.02', '1.02.01.10')
            analytics.append([l[0], l[1], r, l[2], quarter, l[3], company[0]])

            l = ['8.14.10', 'Dívida Líquida / EBITDA (Valores Imprecisos?)', demonstrativo, formato]
            s = fSum('2.01.04.01.01', '2.01.04.01.02', '2.01.04.02', '2.01.04.03', '2.02.01.01.01', '2.02.01.01.02', '2.02.01.02', '2.02.01.03')
            t = fSum('1.01.01', '1.01.02', '1.02.01.10')
            u = s - t  # dívida líquida
            v = fSum('3.05', '7.04.01')  # EBITDA
            if v != 0:
                r = u / v
                analytics.append([l[0], l[1], r, l[2], quarter, l[3], company[0]])

            l = ['8.14.11', 'Capital Investido (Valores Imprecisos?)', demonstrativo, formato]
            s = fSum('1.01.01', '1.01.02', '1.02.01.20')  # Caixa e Equivalentes de caixa
            t = fValue('2.03')  # Parimônio Líquido, Dívdida Bruta
            u = fSum('2.01.04.01.01', '2.01.04.01.02', '2.01.04.02', '2.01.04.03', '2.02.01.01.01', '2.02.01.01.02', '2.02.01.02', '2.02.01.03')
            r = s + t - u  # capital investido
            analytics.append([l[0], l[1], r, l[2], quarter, l[3], company[0]])

            l = ['8.14.12', 'Índice de Cobertura de Juros', demonstrativo, formato]
            r = fDivision('7.08.03.01', '3.05')
            analytics.append([l[0], l[1], r, l[2], quarter, l[3], company[0]])

            l = ['8.14.13', 'Endividamento Financeiro', demonstrativo, formato]
            s = fSum('2.01.04.01.01', '2.01.04.01.02', '2.01.04.02', '2.01.04.03', '2.02.01.01.01', '2.02.01.01.02', '2.02.01.02', '2.02.01.03')
            t = fValue('2.03')
            if t != 0:
                r = s / t
                analytics.append([l[0], l[1], r, l[2], quarter, l[3], company[0]])

            formato = 'Análise Fundamentalista'
            demonstrativo = 'Análise dos Resultados'

            l = ['9.01', 'Margem Bruta', demonstrativo, formato]
            r = fDivision('3.03', '3.01') * 100
            analytics.append([l[0], l[1], r, l[2], quarter, l[3], company[0]])

            l = ['9.02', 'Margem de Despesas Administrativas', demonstrativo, formato]
            s = fValue('3.04.02')
            t = fSum('3.04.01', '3.04.02')
            if t != 0:
                r = s / t * 100
                analytics.append([l[0], l[1], r, l[2], quarter, l[3], company[0]])

            l = ['9.03', 'Margem de Despesas Operacionais', demonstrativo, formato]
            r = fDivision('3.04', '3.01') * 100
            analytics.append([l[0], l[1], r, l[2], quarter, l[3], company[0]])

            l = ['9.04', 'Margem EBIT', demonstrativo, formato]
            r = fDivision('3.05', '3.01') * 100
            analytics.append([l[0], l[1], r, l[2], quarter, l[3], company[0]])

            l = ['9.04', 'Margem de Depreciação', demonstrativo, formato]
            r = fDivision('7.04.01', '3.01') * 100
            analytics.append([l[0], l[1], r, l[2], quarter, l[3], company[0]])

            l = ['9.05', 'EBITDA', demonstrativo, formato]
            r = fSum('3.05', '7.04.01')
            analytics.append([l[0], l[1], r, l[2], quarter, l[3], company[0]])

            l = ['9.05.01', 'Margem EBITDA', demonstrativo, formato]
            s = fSum('3.05', '7.04.01')
            t = fValue('3.01')
            if t != 0:
                r = s / t * 100
                analytics.append([l[0], l[1], r, l[2], quarter, l[3], company[0]])

            l = ['9.06', 'Margem Financeira', demonstrativo, formato]
            r = fDivision('3.06', '3.01') * 100
            analytics.append([l[0], l[1], r, l[2], quarter, l[3], company[0]])

            l = ['9.07', 'Margem dos Juros', demonstrativo, formato]
            r = fDivision('7.08.03.01', '3.01') * 100
            analytics.append([l[0], l[1], r, l[2], quarter, l[3], company[0]])

            l = ['9.08', 'Margem dos Impostos IRPJ e CSLL', demonstrativo, formato]
            r = fDivision('3.08', '3.01') * 100
            analytics.append([l[0], l[1], r, l[2], quarter, l[3], company[0]])

            l = ['9.09', 'Margem Líquida', demonstrativo, formato]
            r = fDivision('3.11', '3.01') * 100
            analytics.append([l[0], l[1], r, l[2], quarter, l[3], company[0]])

            l = ['9.10', 'Coeficiente de Retorno', demonstrativo, formato]
            r = fDivision('3.11', '1') * 100
            analytics.append([l[0], l[1], r, l[2], quarter, l[3], company[0]])

            l = ['9.11', 'ROE Return on Equity (Retorno sobre o Patrimônio Líquido)', demonstrativo, formato]
            r = fDivision('3.11', '2.03') * 100
            analytics.append([l[0], l[1], r, l[2], quarter, l[3], company[0]])

            l = ['9.12', 'ROIC Return on Invested Capital (Retorno sobre o Capital Investido)  (Valores Imprecisos?)', demonstrativo, formato]
            s = fSum('1.01.01', '1.01.02', '1.02.01.20')  # Caixa e Equivalentes de caixa
            t = fValue('2.03')  # Parimônio Líquido, Dívdida Bruta
            u = fSum('2.01.04.01.01', '2.01.04.01.02', '2.01.04.02', '2.01.04.03', '2.02.01.01.01', '2.02.01.01.02', '2.02.01.02', '2.02.01.03')
            v = s + t - u  # capital investido
            w = fValue('3.05')  # EBIT
            if v != 0:
                r = 0.66 * w / v
                r = r * 100
                analytics.append([l[0], l[1], r, l[2], quarter, l[3], company[0]])

            l = ['9.13', 'ROA Return on Actives (Retorno sobre os Ativos)', demonstrativo, formato]
            r = fDivision('3.05', '1') * 100
            analytics.append([l[0], l[1], r, l[2], quarter, l[3], company[0]])

            formato = 'Análise Fundamentalista'
            demonstrativo = 'Análise do Fluxo de Caixa'

            l = ['10.01', 'Caixa Total', demonstrativo, formato]
            r = fSum('6.01', '6.02', '6.03')
            analytics.append([l[0], l[1], r, l[2], quarter, l[3], company[0]])

            l = ['10.02', 'Caixa Livre', demonstrativo, formato]
            r = fSum('6.01', '6.02')
            analytics.append([l[0], l[1], r, l[2], quarter, l[3], company[0]])

            l = ['10.03', 'Proporção de Capex sobre Operação', demonstrativo, formato]
            r = fDivision('6.02', '6.01') * 100
            analytics.append([l[0], l[1], r, l[2], quarter, l[3], company[0]])

            l = ['10.03', 'Proporção de Capex sobre EBIT', demonstrativo, formato]
            r = fDivision('6.02', '3.05') * 100
            analytics.append([l[0], l[1], r, l[2], quarter, l[3], company[0]])

            l = ['10.03', 'Proporção de FCO sobre EBITDA', demonstrativo, formato]
            s = fValue('6.01')
            t = fSum('3.05', '7.04.01')
            if t != 0:
                r = s / t
                r = r * 100
                analytics.append([l[0], l[1], r, l[2], quarter, l[3], company[0]])

            l = ['10.04', 'Capex', demonstrativo, formato]
            r = fSum('6.02.01', '6.02.02')
            analytics.append([l[0], l[1], r, l[2], quarter, l[3], company[0]])

            l = ['10.05', 'FCL de Capex ou FCFF', demonstrativo, formato]
            s = fSum('6.02.01', '6.02.02')
            s = s * -1
            t = fValue('6.01')
            r = s + t
            analytics.append([l[0], l[1], r, l[2], quarter, l[3], company[0]])

        return analytics
    except Exception as e:
        trouble(e, sys._getframe().f_code.co_name)

def bigDataAnalyticsTranslate(bigDataRAW2):
    """translate unique generic statements in standart statements"""
    # print(sys._getframe().f_code.co_name)
    try:
        new = False
        analytics = []
        analyticsRefused = []
        uniqueLine = []
        dreCode = dreCodes()
        dreCodeSearchAll = dreCodesAll()
        dreCodeSearchAllBut = dreCodesAllBut()
        dreCodeSearchAny = dreCodesAny()
        for d, demo in enumerate(bigDataRAW2):
            for l, line in enumerate(demo):
                if not line[2]: line[2] = 0
                # line[2] = float(line[2])
                try:
                    line[4] = datetime.strftime(line[4], '%d/%m/%Y')
                except:
                    pass

                # Basic Almost Ready DRE Codes
                new = dreRebuild(line, dreCode)
                if new:
                    # print(d, l, line)
                    analytics.append(line)
                else:
                    if bigDataRAW2[d][l] not in analyticsRefused and bigDataRAW2[d][l] not in analytics:
                        analyticsRefused.append(bigDataRAW2[d][l])
                new = False

        for d, demo in enumerate(bigDataRAW2):
            for l, line in enumerate(demo):
                # DRE
                # Search All and Build
                for r, replace in enumerate(dreCodeSearchAll):
                    uniqueLine, line, new = bigDataAnalyticsReplaceSearchAll(uniqueLine, line, replace)
                    if new:
                        # print(d, l, r, line)
                        analytics.append(line)
                    else:
                        if bigDataRAW2[d][l] not in analyticsRefused and bigDataRAW2[d][l] not in analytics:
                            analyticsRefused.append(bigDataRAW2[d][l])
                    new = False

        for d, demo in enumerate(bigDataRAW2):
            for l, line in enumerate(demo):
                # Search All, BUT and Build
                for r, replace in enumerate( dreCodeSearchAllBut):
                    uniqueLine, line, new = bigDataAnalyticsReplaceSearchAllBut(uniqueLine, line, replace)
                    if new:
                        # print(d, l, r, line)
                        analytics.append(line)
                    else:
                        if bigDataRAW2[d][l] not in analyticsRefused and bigDataRAW2[d][l] not in analytics:
                            analyticsRefused.append(bigDataRAW2[d][l])
                    new = False

                # Search any and Build

        for d, demo in enumerate(bigDataRAW2):
            for l, line in enumerate(demo):
                for r, replace in enumerate(dreCodeSearchAny):
                    uniqueLine, line, new = bigDataAnalyticsReplaceSearchAny(uniqueLine, line, replace)
                    if new:
                        # print(d, l, r, line)
                        analytics.append(line)
                    else:
                        if bigDataRAW2[d][l] not in analyticsRefused and bigDataRAW2[d][l] not in analytics:
                            analyticsRefused.append(bigDataRAW2[d][l])
                    new = False

        for d, demo in enumerate(bigDataRAW2):
            for l, line in enumerate(demo):
                if line[3] == 'Ações Ordinárias' or line[3] == 'Ações Preferenciais':
                    analytics.append(line)
                # new = False

        analytics.sort(key=lambda x: x[0], reverse=False)
        analytics.sort(key=lambda x: datetime.strptime(x[4], '%d/%m/%Y'), reverse=True)
        analytics.sort(key=lambda x: x[6], reverse=False)

        return analytics, analyticsRefused
    except Exception as e:
        trouble(e, sys._getframe().f_code.co_name)

def dreRebuild(line, dreCode):
    """translate generical statements in standart statements"""
    # # print(sys._getframe().f_code.co_name)
    try:
        for i, item in enumerate(dreCode):
            x = item[0].split('.')
            if len(x) <= 2:
                if line[0].strip() == item[0]:
                    line[1] = item[1]
                    return True
            else:
                # x.pop()
                x = '.'.join(x)
                if line[0].strip().startswith(x) and len(line[0]) == len(item[0]) and item[2].lower() in line[1].lower():
                    line[0] = item[0]
                    line[1] = item[1]
                    return True

        return False

    except Exception as e:
        trouble(e, sys._getframe().f_code.co_name)
def bigDataAnalyticsFormatCleanup(analytics):
    """clean up just in case, data manipulation problem prevention"""
    # print(sys._getframe().f_code.co_name)
    try:
        for l, line in enumerate(analytics):
            for i, item in enumerate(line):
                try:
                    if i == 0:
                        analytics[l][i] = analytics[l][i] + ' - '
                    if i == 1:
                        analytics[l][i] = analytics[l][0] + ' ' + analytics[l][1].strip()
                    if i == 2:
                        analytics[l][i] = float(analytics[l][i])
                    analytics[l][i] = analytics[l][i].strip()

                except:
                    pass
        analytics.sort(key=lambda x: x[0], reverse=False)
        analytics.sort(key=lambda x: x[4], reverse=True)
        analytics.sort(key=lambda x: x[6], reverse=False)
        return analytics
    except Exception as e:
        trouble(e, sys._getframe().f_code.co_name)
def bigDataAnalyticsDict(analytics):
    """get unique items for specific parts"""
    # print(sys._getframe().f_code.co_name)
    try:
        cat = []
        for line in analytics:
            if line[1] not in cat:
                cat.append(line[1])
        # cat = list(dict.fromkeys(cat))
        cat.sort()

        for l, line in enumerate(analytics):
            try:
                analytics[l][4] = datetime.strptime(line[4], '%d/%m/%Y')
            except:
                pass

        anos = []
        temp = [anos.append(item) for item in [x[4].year for x in analytics] if item not in anos]
        anos.sort(reverse=True)

        return cat, anos
    except Exception as e:
        trouble(e, sys._getframe().f_code.co_name)
def bigDataAnalyticsCalculations(analytics):
    """sum statements quarters accordingly, for month and statement"""
    # print(sys._getframe().f_code.co_name)
    try:
        """ corrections DRE_DFC item[0]] = demo = uDemonstrativo first character, item[1] = datetime.month"""
        corrections = [['3', 12], ['6', 12], ['6', 9], ['6', 6], ['7', 12], ['7', 9], ['7', 6]]

        for item in corrections:
            analytics = DRE_DFC(analytics, item[0], item[1])

        return analytics
    except Exception as e:
        trouble(e, sys._getframe().f_code.co_name)
def DRE_DFC(analytics, demo, month):
    """sum statements quarters accordingly, month and statement"""
    # print(sys._getframe().f_code.co_name, month)
    try:
        analyticsRAW = deepcopy(analytics)
        codes = [line[1] for line in analyticsRAW if line[1][:1] == demo]
        codes = list(dict.fromkeys(codes))

        years = [line[4].year for line in analytics]
        years = list(dict.fromkeys(years))

        for y, year in enumerate(years):
            for c, code in enumerate(codes):
                dre = [line for line in analyticsRAW if line[4].year == year and line[1][:1] == demo and line[1] == code]
                q4 = 0
                q3 = 0
                q2 = 0
                q1 = 0
                q = 0
                newLine = ''
                for l, line in enumerate(dre):
                    try:
                        if line[4].month == 12:
                            if month == 12:
                                newLine = line
                            q4 = line[2]
                    except:
                        pass
                    try:
                        if line[4].month == 9:
                            if month == 9:
                                newLine = line
                            q3 = line[2]
                    except:
                        pass
                    try:
                        if line[4].month == 6:
                            if month == 6:
                                newLine = line
                            q2 = line[2]
                    except:
                        pass
                    try:
                        if line[4].month == 3:
                            if month == 3:
                                newLine = line
                            q1 = line[2]
                    except:
                        pass


                    # find new line in analytics and replace newLine[2] by quarter. This is tricky. If DEMO == 3, only q4 needs adjustments. If DEMO == 6, q4, q3 and q2 needs adjustments
                    try:
                        if q4 !=0 and q3 !=0 and q2 !=0 and q1 !=0:
                            if month == 12 and demo == '3':
                                q = q4 - (q3 + q2 + q1)  ## this is gold
                            if month == 12 and (demo == '6' or demo == '7'):
                                q = q4 - (q3)  ## this is gold
                        if q3 !=0 and q2 !=0 and q1 !=0:
                            if month == 9 and (demo == '6' or demo == '7'):
                                q = q3 - (q2)  ## this is gold
                        if q2 !=0 and q1 !=0:
                            if month == 6 and (demo == '6' or demo == '7'):
                                q = q2 - (q1)  ## this is gold
                        if q:
                            index = analytics.index(newLine)
                            analytics[index][2] = q

                    except:
                        pass
        return analytics
    except Exception as e:
        trouble(e, sys._getframe().f_code.co_name)
def uberlista(company):
    """update report to uberlista sheet in order to compile every other report from all companies"""
    print(sys._getframe().f_code.co_name, company[0], company[7])
    try:
        # company reports
        companyReportsSheet = getSheet(company[7], 'reports')
        reportList = companyReportsSheet.get_all_values()
        headers = reportList.pop(0)

        uberReport.append(reportList)

        timestamp = sheetLog(company, ' ', sys._getframe().f_code.co_name)

        return uberReport
    except Exception as e:
        trouble(e, sys._getframe().f_code.co_name)
def analyticsYOYear(analytics):
    """prepare analytic data from statements for pandas dataframe"""
    # print(sys._getframe().f_code.co_name, len(analytics))
    try:
        for l, line in enumerate(analytics):
            line[2] = float(line[2])
            try:
                line[4] = datetime.strptime(line[4], '%d/%m/%Y')
            except:
                pass

        analyticsYOY = analyticsYOYBuild(analytics)

        # get most recent tri and
        tri = set()
        for l, line in enumerate(analyticsYOY):
            tri.add(line[4])
        tri = list(tri)
        tri.sort(key=lambda x: x, reverse=True)
        # and add it to annual
        analyticsAnual = [x for x in analyticsYOY if x[4].month == 12 or x[4] == tri[0]]


        for l, line in enumerate(analytics):
            try:
                analytics[l][4] = analytics[l][4].strftime('%d/%m/%Y')
            except:
                pass

        for l, line in enumerate(analyticsYOY):
            try:
                analyticsYOY[l][4] = analyticsYOY[l][4].strftime('%d/%m/%Y')
            except:
                pass

        for l, line in enumerate(analyticsAnual):
            try:
                analyticsAnual[l][4] = analyticsAnual[l][4].strftime('%d/%m/%Y')
            except:
                pass

        return analytics, analyticsYOY, analyticsAnual
    except Exception as e:
        trouble(e, sys._getframe().f_code.co_name)
def analyticsYOYBuild(analytics):
    """organize statements by quarter according to the way they are constructed and displayed"""
    # print(sys._getframe().f_code.co_name)
    try:
        analyticsYOY = deepcopy(analytics)
        for l, line in enumerate(analytics):
            # q12
            if line[4].month == 12 and analyticsYOY[l][0][:1] != '1' and analyticsYOY[l][0][:1] != '2':
                q12 = analytics[l][2]
                index12_12 = [l for l, line in enumerate(analytics) if line[2] == q12]
                try:
                    q9 = [item for item in analytics if item[0] == line[0] and item[1] == line[1] and item[4].year == line[4].year and item[4].month == 9][0][2]
                    index12_9 = [l for l, line in enumerate(analytics) if line[2] == q9]
                except:
                    q9 = 0
                try:
                    q6 = [item for item in analytics if item[0] == line[0] and item[1] == line[1] and item[4].year == line[4].year and item[4].month == 6][0][2]
                    index12_6 = [l for l, line in enumerate(analytics) if line[2] == q6]
                except:
                    q6 = 0
                try:
                    q3 = [item for item in analytics if item[0] == line[0] and item[1] == line[1] and item[4].year == line[4].year and item[4].month == 3][0][2]
                    index12_3 = [l for l, line in enumerate(analytics) if line[2] == q3]
                except:
                    q3 = 0
                analyticsYOY[l][2] = (q12 + q9 + q6 + q3)
            # q9
            if line[4].month == 9 and analyticsYOY[l][0][:1] != '1' and analyticsYOY[l][0][:1] != '2':
                q9 = analytics[l][2]
                index9_9 = [l for l, line in enumerate(analytics) if line[2] == q9]
                try:
                    q6 = [item for item in analytics if item[0] == line[0] and item[1] == line[1] and item[4].year == line[4].year and item[4].month == 6][0][2]
                    index9_6 = [l for l, line in enumerate(analytics) if line[2] == q6]
                except:
                    q6 = 0
                try:
                    q3 = [item for item in analytics if item[0] == line[0] and item[1] == line[1] and item[4].year == line[4].year and item[4].month == 3][0][2]
                    index9_3 = [l for l, line in enumerate(analytics) if line[2] == q3]
                except:
                    q3 = 0
                try:
                    q12 = [item for item in analytics if item[0] == line[0] and item[1] == line[1] and item[4].year == line[4].year - 1 and item[4].month == 12][0][2]
                    index9_12 = [l for l, line in enumerate(analytics) if line[2] == q12]
                except:
                    q12 = 0
                analyticsYOY[l][2] = (q12 + q9 + q6 + q3)
            # q6
            if line[4].month == 6 and analyticsYOY[l][0][:1] != '1' and analyticsYOY[l][0][:1] != '2':
                q6 = analytics[l][2]
                index6_6 = [l for l, line in enumerate(analytics) if line[2] == q6]
                try:
                    q3 = [item for item in analytics if item[0] == line[0] and item[1] == line[1] and item[4].year == line[4].year and item[4].month == 3][0][2]
                    index6_3 = [l for l, line in enumerate(analytics) if line[2] == q3]
                except:
                    q3 = 0
                try:
                    q12 = [item for item in analytics if item[0] == line[0] and item[1] == line[1] and item[4].year == line[4].year - 1 and item[4].month == 12][0][2]
                    index6_12 = [l for l, line in enumerate(analytics) if line[2] == q12]
                except:
                    q12 = 0
                try:
                    q9 = [item for item in analytics if item[0] == line[0] and item[1] == line[1] and item[4].year == line[4].year - 1 and item[4].month == 9][0][2]
                    index6_9 = [l for l, line in enumerate(analytics) if line[2] == q9]
                except:
                    q9 = 0
                analyticsYOY[l][2] = (q12 + q9 + q6 + q3)
            # q3
            if line[4].month == 3 and analyticsYOY[l][0][:1] != '1' and analyticsYOY[l][0][:1] != '2':
                q3 = analytics[l][2]
                index3_3 = [l for l, line in enumerate(analytics) if line[2] == q3]
                try:
                    q12 = [item for item in analytics if item[0] == line[0] and item[1] == line[1] and item[4].year == line[4].year - 1 and item[4].month == 12][0][2]
                    index3_12 = [l for l, line in enumerate(analytics) if line[2] == q3]
                except:
                    q12 = 0
                try:
                    q9 = [item for item in analytics if item[0] == line[0] and item[1] == line[1] and item[4].year == line[4].year - 1 and item[4].month == 9][0][2]
                    index3_9 = [l for l, line in enumerate(analytics) if line[2] == q9]
                except:
                    q9 = 0
                try:
                    q6 = [item for item in analytics if item[0] == line[0] and item[1] == line[1] and item[4].year == line[4].year - 1 and item[4].month == 6][0][2]
                    index3_6 = [l for l, line in enumerate(analytics) if line[2] == q6]
                except:
                    q6 = 0
                analyticsYOY[l][2] = (q12 + q9 + q6 + q3)
            pass
        return analyticsYOY
    except Exception as e:
        trouble(e, sys._getframe().f_code.co_name)
def createUberReport(company, analytics, timestamp):
    """create bigdata report for translated analytics calculated financial statements"""
    # print(sys._getframe().f_code.co_name, company[0])
    try:
        # compatibility fix
        for l, line in enumerate(analytics):
            try:
                if len(line) == 9:
                    del analytics[l][-1]
                line[2] = str(line[2]).replace('.',',')
            except:
                pass

        analytics.insert(0, superHeaders)

        sheet = 'uberReport'
        index = 1
        
        # Sheet
        try:
            ursheet = cSpreadsheet.worksheet(sheet)
        except:
            cSpreadsheet.add_worksheet(sheet, 1, 1, index=index)  # create
            urSheet = cSpreadsheet.worksheet(sheet)

        # UberReport in Company SpreadSheet
        # urSheet.resize(2, 8)
        uberReport = sheetUpdate(urSheet, analytics, 1, 'USER_ENTERED')

        # ubersheet in uberlista spreadsheet
        urSpreadsheet = gsheet.open_by_url(uberSpreadsheet)
        try:
            uberSheet = urSpreadsheet.worksheet(sheet)
        except:
            urSpreadsheet.add_worksheet(sheet, 1, 1, index=index)  # create
            uberSheet = urSpreadsheet.worksheet(sheet)
        # UberReport
        found = uberSheet.find(company[0])
        if found != None:
            list = uberSheet.get_all_values()
            headers = list.pop(0)
            # remove existing company values
            list = [row for row in list if company[0] not in row]
            # insert new company values
            for l, line in enumerate(analytics):
                list.append(line)
            # update uberlista worksheetsheet
            udpate = sheetUpdate(uberSheet, list, 1, 'RAW')
        else:
            # don't remove anything, not necessary, just batch append
            analytics.pop(0)
            uberSheet.append_rows(analytics)

        return analytics
    except Exception as e:
        trouble(e, sys._getframe().f_code.co_name)

def analyticsPandas(company, analytics, sheet, index, cat):
    """create panda dataframe for statements"""
    # print(sys._getframe().f_code.co_name, company[0])
    try:
        # Sheet
        try:
            pdSheet = cSpreadsheet.worksheet(sheet)
        except:
            cSpreadsheet.add_worksheet(sheet, 1, 1, index=index)  # create
            pdSheet = cSpreadsheet.worksheet(sheet)

        # Pandas
        # BIGdata
        pdSheet.resize(1, 1)

        for l, line in enumerate(analytics):
            try:
                line[2] = float(line[2].replace(',', '.'))
            except:
                pass
            if len(line) == 9:
                del analytics[l][-1]
        analytics = [x for x in analytics if x != superHeaders]

        df = pd.DataFrame(analytics, columns=superHeaders)
        df[superHeaders[2]] = pd.to_numeric(df[superHeaders[2]], errors='coerce')
        df[superHeaders[4]] = pd.to_datetime(df[superHeaders[4]], format='%d/%m/%Y')
        df[superHeaders[1]] = df[superHeaders[1]].astype('category')
        df[superHeaders[1]].cat.set_categories(cat)

        # pivot table
        # print(superHeaders)
        table = df.pivot_table(index=[superHeaders[1]], columns=[superHeaders[4]], values=[superHeaders[2]], fill_value=0, margins=False)
        table.sort_values(by=superHeaders[4], axis=1, ascending=False, inplace=True)

        # send pivot_table to sheet
        set_with_dataframe(pdSheet, table, row=1, col=1, include_index=True, include_column_header=True)

        # organize new pivoted bigdata
        newTable = pdSheet.get_all_values()
        del newTable[:1]
        for i, item in enumerate(newTable[0]):
            if i > 0:
                try:
                    newTable[0][i] = datetime.strptime(item, '%Y-%m-%d %H:%M:%S').strftime('%d/%m/%Y')
                except:
                    pass
        sheetUpdate(pdSheet, newTable, 1, 'USER_ENTERED')
        return newTable
    except Exception as e:
        trouble(e, sys._getframe().f_code.co_name)

def template():
    """description"""
    # print(sys._getframe().f_code.co_name)
    try:

        return True
    except Exception as e:
        trouble(e, sys._getframe().f_code.co_name)



if __name__ == "__main__":
    """entry point"""
    print('please run from external file')