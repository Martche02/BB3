import utils.config as config

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
        print(config.module_message)
    except Exception as e:
        print(e)


if __name__ == '__main__':
    try:
        print(config.module_message)
    except:
        pass
