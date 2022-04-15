import utils.config as config
import utils.system as system
import utils.wait as wait
import utils.list as list

import sys
from datetime import datetime

def project_load():
  ''' choose the way to run: update or re-download'''  
    # print(sys._getframe().f_code.co_name)
  try:
      options = []
      options.append('UPDATE keep actual database')
      options.append('DOWNLOAD Rebase, restart brand new fresh database')
      project = config.choose(options)
      if project == 1:
        project = 'csv'
      elif project == 2:
        project = 'web'
      else:
        project = config.choose(options)
      return project
  except Exception as e:
    system.trouble(e, sys._getframe().f_code.co_name)


if __name__ == '__main__':
  try:
    print(config.module_message)
  except:
    pass
