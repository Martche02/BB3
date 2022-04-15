import utils.config as config
import utils.system as system
import utils.company_prices as company_prices
import sys

import socket
from datetime import datetime 

from google.oauth2 import service_account  # Google Authentication
import gspread  # Python API for Google Sheets.

global module_message 
module_message = 'This is a module, not a standalone script, and will not run as a standalone script' # This is a module, not a standalone script, and will not run as a standalone script

class config:
    def __init__(self):       
        self.now()
        self.server()
        self.user_variables()
        self.browser = system.load_browser()
        self.app()

    def app(self):
    # ''' choose main app options '''
    # print(sys._getframe().f_code.co_name)
        try:
            options = []
            options.append('HIST Historical Prices')
            options.append('STAT Financial Statements')

            project = self.choose(options)
            if project == 1:
                self.app_hist(self.timestamp)
            elif project == 2:
                self.app_stat(self.timestamp) #toDo
            else:
                quit()
            print('done')

        except Exception as e:
            system.trouble(e, sys._getframe().f_code.co_name)

    def app_hist(self):
        ''' choose among options for historical stock prices'''
        # print(sys._getframe().f_code.co_name)
        try:
            options = []
            options.append('COMP - Get list of COMPANIES')
            options.append('DATA - Get DATA RANGE for companies')
            options.append('STOCK - Get STCK DATA for DATA RANGE for companies')
            options.append('MARKET - Get MARKET prices by date and company')

            project = self.choose(options)
            if project == 1:
                company_prices.grab(config.timestamp)
            elif project == 2:
                company_prices.datarange(config.timestamp)
            elif project == 3:
                company_prices.stock_price(config.timestamp)
            elif project == 4:
                company_prices.market_price(config.timestamp)
            else:
                quit()
            print('done')

        except Exception as e:
            system.trouble(e, sys._getframe().f_code.co_name)
    def app_stat(self):
        ''' choose among options for historical stock prices'''
        # print(sys._getframe().f_code.co_name)
        try:
            options = []
            options.append('COMP - Get list of COMPANIES')
            options.append('DATA - Get DATA RANGE for companies')
            options.append('STOCK - Get STCK DATA for DATA RANGE for companies')
            options.append('MARKET - Get MARKET prices by date and company')

            project = self.choose(options)
            if project == 1:
                company_statements.grab(config.timestamp)
            elif project == 2:
                company_statements.datarange(config.timestamp)
            elif project == 3:
                company_statements.stock_price(config.timestamp)
            elif project == 4:
                company_statements.market_price(config.timestamp)
            else:
                quit()
            print('done')

        except Exception as e:
            system.trouble(e, sys._getframe().f_code.co_name)



    def now(self):
        """return timestamp from the exact moment"""
        # print(sys._getframe().f_code.co_name)
        try:
            self.timestamp = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        except Exception as e:
            system.trouble(e, sys._getframe().f_code.co_name)
    def server(self):
        """return timestamp from the exact moment"""
        # print(sys._getframe().f_code.co_name)
        try:
            self.server = socket.gethostname() + ' ' + socket.gethostbyname(socket.gethostname())
        except Exception as e:
            system.trouble(e, sys._getframe().f_code.co_name)
    def user_variables(self):
        try:
            self.batch = 1000 # number of items to be processed in one batch
            self.batch_table = 150 # number of items to be processed in one table batch

            self.producao = 'AZEVEDO-SERVER' # production server
            self.chrome ='C:/Users/faust/PycharmProjects/FSP' + '/webdriver/chromedriver.exe' if socket.gethostname() == self.producao else 'C:/Py/B3C' + '/webdriver/chromedriver.exe' # webdriver path
            
            self.secs_to_wait = 1/1000 # seconds to wait for an element to be visible

            self.data_folder = 'data/' # data folder
        except Exception as e:
            system.trouble(e, sys._getframe().f_code.co_name)

    def google_api(self, *user):
        """startup google API 
        # google Sheets API - https://console.cloud.google.com/apis/api/sheets.googleapis.com/overview?project=dre-empresas-listadas-b3
        # Credentials - dre-empresas-listadas-bot@dre-empresas-listadas-bovespa.iam.gserviceaccount.com em https://console.cloud.google.com/apis/api/sheets.googleapis.com/credentials?project=dre-empresas-listadas-b3
        # keys - https://console.cloud.google.com/iam-admin/serviceaccounts/details/102235163341900235117;edit=true/keys?project=dre-empresas-listadas-b3 - account_credentials_u1.json

        """
        # print(sys._getframe().f_code.co_name)
        try:
            try:
                self.google = int(user[0]) % 3
            except:
                self.google = 1

            self.account_credentials = 'config/account_credentials_u' + user + '.json'
            self.credentials: object = service_account.Credentials.from_service_account_file(self.account_credentials, scopes=config.sheet_scope)
        except Exception as e:
            system.trouble(e, sys._getframe().f_code.co_name)

    def choose(self, options):
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
                self.choose(options)
            return i
        except Exception as e:
            system.trouble(e, sys._getframe().f_code.co_name)

if __name__ == '__main__':
    try:
        print(module_message)
    except:
        pass