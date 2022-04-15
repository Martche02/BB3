import utils.config as config
import utils.system as system
import utils.company as company
import sys

import socket
from datetime import datetime 

from google.oauth2 import service_account  # Google Authentication
import gspread  # Python API for Google Sheets.


module_message = 'This is a module, not a standalone script, and will not run as a standalone script' # This is a module, not a standalone script, and will not run as a standalone script

def load():
    """
    Loads the configuration
    """
    try:
        timestamp = now()
        machine = server()
        user_variables = config.user_variables()
        browser = system.load_browser()

        return browser
    except Exception as e:
        system.trouble(e, sys._getframe().f_code.co_name) # register error message and reloads

def now():
    """return timestamp from the exact moment"""
    # print(sys._getframe().f_code.co_name)
    try:
        global timestamp
        timestamp = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        return timestamp
    except Exception as e:
        system.trouble(e, sys._getframe().f_code.co_name)
def server():
    """return timestamp from the exact moment"""
    # print(sys._getframe().f_code.co_name)
    try:
        global server
        server = socket.gethostname() + ' ' + socket.gethostbyname(socket.gethostname())
        return server
    except Exception as e:
        system.trouble(e, sys._getframe().f_code.co_name)
def user_variables():
    try:
        global batch
        global batch_table
        batch = 1000 # number of items to be processed in one batch
        batch_table = 150 # number of items to be processed in one table batch

        global producao
        producao = 'AZEVEDO-SERVER' # production server
        if socket.gethostname() == producao:
            webdriver_folder = 'C:/Users/faust/PycharmProjects/FSP' # webdriver folder
        else:
            webdriver_folder = 'C:/Py/B3C' # webdriver folder

        global chrome
        chrome = webdriver_folder + '/webdriver/chromedriver.exe' # webdriver path
        
        global secs_to_wait
        secs_to_wait = 1/1000 # seconds to wait for an element to be visible

        global data_folder
        data_folder = 'data/' # data folder

        return True
        
    except Exception as e:
        system.trouble(e, sys._getframe().f_code.co_name)

def google_api(*user):
    """startup google API 
    # google Sheets API - https://console.cloud.google.com/apis/api/sheets.googleapis.com/overview?project=dre-empresas-listadas-b3
    # Credentials - dre-empresas-listadas-bot@dre-empresas-listadas-bovespa.iam.gserviceaccount.com em https://console.cloud.google.com/apis/api/sheets.googleapis.com/credentials?project=dre-empresas-listadas-b3
    # keys - https://console.cloud.google.com/iam-admin/serviceaccounts/details/102235163341900235117;edit=true/keys?project=dre-empresas-listadas-b3 - account_credentials_u1.json

    """
    # print(sys._getframe().f_code.co_name)
    try:
        try:
            google = int(user[0]) % 3
        except:
            google = 1

        account_credentials = 'config/account_credentials_u' + user + '.json'
        credentials: object = service_account.Credentials.from_service_account_file(account_credentials, scopes=config.sheet_scope)

        return google
    except Exception as e:
        system.trouble(e, sys._getframe().f_code.co_name)

def choose(options):
    """let user choose any version or udpate"""
    # print(sys._getframe().f_code.co_name)
    try:
        print('Por favor escolha uma opção para atualizar:')

        for i, item in enumerate(options):
            print('{}) {}'.format(i + 1, item))

        i = input('Escolha o número da opção desejada para atualizar:')
        try:
            i = int(i)
            if 1 <= i <= len(options):
                return i
        except:
            print('Inválido')
            choose(options)
        return i
    except Exception as e:
        system.trouble(e, sys._getframe().f_code.co_name)
def app(timestamp):
    ''' choose main app options '''
    # print(sys._getframe().f_code.co_name)
    try:
        options = []
        options.append('HIST Historical Prices')
        options.append('STAT Financial Statements')

        project = config.choose(options)
        if project == 1:
            app_hist(config.timestamp)
        elif project == 2:
            app_stat(config.timestamp)
        else:
            quit()
        print('done')

    except Exception as e:
        system.trouble(e, sys._getframe().f_code.co_name)

def app_hist(timestamp):
    ''' choose among options for historical stock prices'''
    # print(sys._getframe().f_code.co_name)
    try:
        options = []
        options.append('COMP - Get list of COMPANIES')
        options.append('DATA - Get DATA RANGE for companies')
        options.append('STOCK - Get STCK DATA for DATA RANGE for companies')

        project = config.choose(options)
        if project == 1:
            company.grab(config.timestamp)
        elif project == 2:
            company.datarange(config.timestamp)
        elif project == 3:
            company.stock_price(config.timestamp)
        else:
            quit()
        print('done')

    except Exception as e:
        system.trouble(e, sys._getframe().f_code.co_name)


if __name__ == '__main__':
    try:
        print(module_message)
    except:
        pass
