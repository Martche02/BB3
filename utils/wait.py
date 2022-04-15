import utils.config as config
import utils.system as system
import sys

def send_keys(xpath, keys):
    """wait and insert input/keystrokes"""
    # print(sys._getframe().f_code.co_name, xpath, keys)
    try:
        input = system.wait.until(system.EC.element_to_be_clickable((confsystemig.By.XPATH, xpath)))
        input = input.send_keys(keys)
        return True
    except Exception as e:
        system.trouble(e, sys._getframe().f_code.co_name)
def click(xpath):
    """wait and click in element"""
    # print(sys._getframe().f_code.co_name, xpath)
    try:
        element = system.wait.until(system.EC.element_to_be_clickable((system.By.XPATH, xpath)))
        element.click()
        return True
    except Exception as e:
        system.trouble(e, sys._getframe().f_code.co_name)
def link(xpath):
    """wait and get link"""
    # print(sys._getframe().f_code.co_name, xpath)
    try:
        element = system.wait.until(system.EC.element_to_be_clickable((system.By.XPATH, xpath)))
        href = element.get_attribute('href')
        return href
    except Exception as e:
        return ''
def text(xpath):
    """wait and get text"""
    # print(sys._getframe().f_code.co_name, xpath)
    try:
        element = system.wait.until(system.EC.element_to_be_clickable((system.By.XPATH, xpath)))
        text = element.text
        return text
    except Exception as e:
        return ''


if __name__ == '__main__':
    try:
        print(config.module_message)
    except:
        pass
