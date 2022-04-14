import utils.config as config
import sys

from selenium import webdriver  # webdriver
from selenium.webdriver.chrome.service import Service  # chrome service
from selenium.webdriver.support.ui import WebDriverWait  # browser wait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import StaleElementReferenceException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select

def browser():
    """load browser"""
    try:
        # browser
        browser = webdriver.Chrome(service=Service(config.user_variables.chrome))
        browser.minimize_window()
        exceptionsIgnore = (NoSuchElementException, StaleElementReferenceException,)
        wait = WebDriverWait(browser, config.secs_to_wait, ignored_exceptions=exceptionsIgnore)
        wait2 = WebDriverWait(browser, 10/1000, ignored_exceptions=exceptionsIgnore)
        return True
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
        print('google user', google, e)
        # time.sleep(sleep*5)
        # restart = b7NSD(rightNow()[:19], 2000)
        # restart = b7NSDCompanies(rightNow()[:19])
        # restart = b7NSDIndex(rightNow()[:19])
        # restart = b7NSDStatements(rightNow()[:19])
        # restart = b7NSDFundamentalist(rightNow()[:19])
        # restart = b7NSDStatementNumerOfStocksFix(rightNow()[:19])

    except Exception as e:
        print(e)
        # browser.quit()
        # time.sleep(sleep*5)
        # restart = b7NSDCompanies(rightNow()[:19])
        # quit()
        # print('should be LOG', sys._getframe().f_code.co_name, e)



if __name__ == '__main__':
    try:
        print(config.module_message)
    except:
        pass
