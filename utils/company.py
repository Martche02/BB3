import config

print('load companies.py')

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
        print(e)


if __name__ == '__main__':
    try:
        print(config.module_message)
    except:
        pass
