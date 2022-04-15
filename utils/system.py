import utils.system as system
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

import unidecode
import string

def table_parser(xpath):
    """ navigate trought any table XPath and return content in list"""
    # print(sys._getframe().f_code.co_name, xpath)
    try:
        # get lenght
        assert (EC.element_to_be_clickable((By.XPATH, xpath)))
        xpath_row = xpath + '/tbody/tr'
        xpath_col = xpath_row + '[3]/td'
        rows = len(browser.find_elements(By.XPATH, xpath_row))
        cols = len(browser.find_elements(By.XPATH, xpath_col ))

        # parse table
        try:
            table = []
            for r in range(1, rows + 1):
                line = []
                for c in range(1, cols + 1):
                    try:
                        table_xpath = xpath_row + '[' + str(r) + ']/td[' + str(c) + ']'
                        text = browser.find_element(By.XPATH, table_xpath).text
                        text = unidecode.unidecode(text).translate(str.maketrans('', '', string.punctuation)).upper().strip()
                        text.replace('\n', ' ')
                        line.append(text)
                    except Exception as e:
                        pass
                table.append(line)

                if r % config.batch_table == 0:
                    print('table row ', str(r), 'of', str(rows))
        except:
            pass

        return table
    except Exception as e:
        system.trouble(e, sys._getframe().f_code.co_name)

def load_browser():
    """load browser"""
    try:
        # browser
        global browser
        browser = webdriver.Chrome(service=Service(config.chrome))
        browser.minimize_window()
        exceptionsIgnore = (NoSuchElementException, StaleElementReferenceException,)
        
        global wait
        global wait_little
        wait = WebDriverWait(browser, config.secs_to_wait, ignored_exceptions=exceptionsIgnore)
        wait_little = WebDriverWait(browser, config.secs_to_wait/1000, ignored_exceptions=exceptionsIgnore)
        
        return browser
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
