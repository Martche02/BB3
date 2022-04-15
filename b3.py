from utils import config
from utils import list
from utils import company_prices
from utils import system

if __name__ == '__main__':
    browser = config.load()

    system.browser.get('https://www.google.com/')
    
    app = config.app(config.timestamp)
    print('done')