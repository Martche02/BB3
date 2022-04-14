import utils.config as config
import utils.system as system
import sys

def grab():
    """
    Grabs all companies from the database
    """
    try:
        print('grab companies')
    except Exception as e:
        print(e)

def datarange():
    """
    Grabs all companies from the database
    """
    try:
        print('grab datarange')
    except Exception as e:
        system.trouble(e, sys._getframe().f_code.co_name)



if __name__ == '__main__':
    try:
        print(config.module_message)
    except:
        pass
