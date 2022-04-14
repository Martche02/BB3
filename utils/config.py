import utils.config as config
import utils.system as system
import sys

import socket
from datetime import datetime

module_message = 'This is a module, not a standalone script, and will not run as a standalone script' # This is a module, not a standalone script, and will not run as a standalone script

def load():
    """
    Loads the configuration
    """
    try:
        timestamp = now()
        machine = server()
        browser = system.browser()
    except Exception as e:
        system.trouble(e, sys._getframe().f_code.co_name) # register error message and reloads

def now():
    """return timestamp from the exact moment"""
    # print(sys._getframe().f_code.co_name)
    try:
        timestamp = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        server = socket.gethostname() + ' ' + socket.gethostbyname(socket.gethostname())
        return timestamp
    except Exception as e:
        system.trouble(e, sys._getframe().f_code.co_name)
def server():
    """return timestamp from the exact moment"""
    # print(sys._getframe().f_code.co_name)
    try:
        server = socket.gethostname() + ' ' + socket.gethostbyname(socket.gethostname())
        return server
    except Exception as e:
        system.trouble(e, sys._getframe().f_code.co_name)
def user_variables():
    try:
        batch = 1000 # number of items to be processed in one batch
        batch_table = 150 # number of items to be processed in one table batch

        producao = 'AZEVEDO-SERVER' # production server
        if socket.gethostname() == producao:
            webdriver_folder = 'C:/Users/faust/PycharmProjects/FSP' # webdriver folder
        else:
            webdriver_folder = 'C:/Py/B3C' # webdriver folder

        chrome = webdriver_folder + '/webdriver/chromedriver.exe' # webdriver path
        secs_to_wait = 1/1000 # seconds to wait for an element to be visible
        return True
        
    except Exception as e:
        system.trouble(e, sys._getframe().f_code.co_name)

if __name__ == '__main__':
    try:
        print(module_message)
    except:
        pass
