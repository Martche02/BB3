from utils import config
from utils import list
from utils import company
from utils import system

if __name__ == '__main__':
    browser = config.load()

    system.browser.get('https://www.google.com/')

    
    shortcut = company.stock_price(config.timestamp)
    
    app = config.app(config.timestamp)
    print('done')