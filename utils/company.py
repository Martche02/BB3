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

def grab(timestamp):
  """get companies from b3 bovespa"""
    # print(sys._getframe().f_code.co_name)
  try:
    filename = 'companies'
    companies_full = []
    # companies_full = [list.from_csv(filename)]
    # if not companies_full:
    #     companies_full = list.from_csv(filename)


    alphabet = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9']
    for char in alphabet:
      filename_company = filename + ' ' + char
      
      project = project_load()

      if project == 'csv':
      # get companies from csv
        companies = list.from_csv(filename_company)
      else:
        project = ''

      if not companies:
        # get companies from web
        url = 'https://bvmf.bmfbovespa.com.br/sig/FormConsultaEmpResultado.asp?strLetraInicial=' + char
        system.browser.get(url)

        # parse companies
        try:
          text = wait.text('/html/body/table[2]/tbody/tr/td/table/tbody/tr[2]/td').strip()
          if text != 'Nenhuma empresa encontrada.':
            xpath = '/html/body/table[2]/tbody/tr/td/table/tbody/tr[3]/td/table'
            companies = system.table_parser(xpath)
            header = companies.pop(0)
            companies = list.to_csv(filename_company, companies)
        except Exception as e:
          pass
    
      if companies:
        companies.sort(key=lambda x: x[0], reverse=False)
        for company in companies:
          if company != [''] and company not in companies_full:
            companies_full.append(company)
    companies_full.sort(key=lambda x: x[0], reverse=False)
    companies_full = list.to_csv(filename, companies_full)

    return companies_full
  except Exception as e:
    system.trouble(e, sys._getframe().f_code.co_name)
def datarange(timestamp):
  """get company data range"""
    # print(sys._getframe().f_code.co_name)
  try:
    project = project_load()
    # project = 'csv'

    # get companies
    filename = 'companies'
    companies = list.from_csv(filename)
    if not companies:
      companies = grab(timestamp)

    # get company data range
    for c, company in enumerate(companies):
      print(c, company[0])
      filename = company[0] + ' datarange'
      company_not_found = list.from_csv('companies datarange not_found')
      if not company_not_found:
        company_not_found = []

      if project == 'csv':
        datarange = list.from_csv(filename)
        if not datarange:
          # there is no csv, load from web
          datarange, company_not_found = datarange_from_web(company, filename, company_not_found)
      else:
        # directly load from web
        datarange, company_not_found = datarange_from_web(company, filename, company_not_found)

    return datarange
  except Exception as e:
    system.trouble(e, sys._getframe().f_code.co_name)
def datarange_from_web(company, filename, company_not_found):
  ''' load from web'''
    # print(sys._getframe().f_code.co_name)
  try:
     # if company[0] inside company_not_found not True
    if not any(company[0] in i for i in company_not_found):
      # get company data range
      url = 'https://bvmf.bmfbovespa.com.br/sig/FormConsultaHistorico.asp?strTipoResumo=HISTORICO&strSocEmissora=' + company[3]
      system.browser.get(url)

      text = wait.text('/html/body/table[2]/tbody/tr/td/table/tbody/tr[2]/td').strip()
      # get company data range info
      if text != 'Empresa Inexistente.' and 'Resumos para essa Empresa' not in text:
        datarange = []
        xpath = '//*[@id="cboAno"]'
        select = system.Select(system.browser.find_element(system.By.XPATH, xpath))
        years = [x.text for x in select.options]
        for year in years:
          #select year
          xpath = '//*[@id="cboAno"]'
          selectBox = system.Select(system.browser.find_element(system.By.XPATH, xpath))
          selectBox.select_by_visible_text(year)

          id = 'id' + str(year)
          for m in range(1, 13):
            try:
              link = wait.text('/html/body/table[2]/tbody/tr[2]/td') # just to wait for the page to load
              # link = wait.link('//*[@id="' + id + '"]/a[' + str(m) + ']')
              # link = system.browser.find_element_by_xpath('//*[@id="' + id + '"]/a[' + str(m) + ']')
              xpath = '//*[@id="' + id + '"]/a[' + str(m) + ']'
              link = system.browser.find_element(system.By.XPATH, xpath)
              link = link.get_attribute('href')
              link = datetime.strptime(link[-7:], '%m-%Y')
              datarange.append(link)
            except Exception as e:
              pass
        if datarange:
          company_datarange = []
          datarange.sort(key=lambda x: x, reverse=True)
          for data in datarange:
            data = data.strftime('%Y-%m')
            company_datarange.append([company[3], data])
          company_datarange = list.to_csv(filename, company_datarange)
      # there is no company datarange in web
      else:
        company_datarange = []
        company_not_found.append([company[0]])
        company_not_found.sort(key=lambda x: x, reverse=False)
        company_not_found = list.to_csv('companies datarange not_found', company_not_found)

    else:
      company_datarange = list.from_csv(filename)
      if not company_datarange:
        company_datarange = []
      if company_datarange:
        for line in datarange:
          if line not in company_datarange:
            company_datarange.append(line)
        company_datarange = list.to_csv(filename, company_datarange)


    return company_datarange, company_not_found
  except Exception as e:
    return [], company_not_found
def stock_price(timestamp):
    """get company stock prices"""
    # print(sys._getframe().f_code.co_name)
    try:
      # project = project_load()
      project = 'csv'

      # get companies
      filename = 'companies'
      companies = list.from_csv(filename)
      if not companies:
        companies = grab(timestamp)

      # get company data range
      company_full_datarange = []
      for c, company in enumerate(companies):
        print(c, company[0])
        filename = company[0] + ' datarange'

        # get datarange
        datarange = list.from_csv(filename)
        company_not_found = []
        if not datarange:
          # there is no csv, load from web
          datarange, company_not_found = datarange_from_web(company, filename, company_not_found)
        if datarange:
          # get historical stock prices
          if project == 'csv':
            # load from csv
            company_historical = []
            file = False
            company_historical = company_historical(c, company, datarange)
          else:
            # load from web
            pass




          if company_historical and not file:
              hist = []
              for t, table in enumerate(company_historical):
                  for l, line in enumerate(table):
                      hist.append(line)
              hist.sort(key=lambda x: x[0], reverse=True)
              # hist.sort(key=lambda x: x[1], reverse=True)
              filename = company[0] + ' historical price'
              company_historical = list_to_csv(filename, hist)

    except Exception as e:
        trouble(e, sys._getframe().f_code.co_name)

def company_historical(c, company, datarange):
  ''' load historical quotes from b3 '''
  # print(sys._getframe().f_code.co_name)
  try:
    for d, data in enumerate(datarange):
      print(k, company[0], d, data[1])
      filename_historical = company[0] + ' ' + data[0] + ' ' + data[1] + ' historical price'
      table = list.from_csv(filename_historical)




                if not (table or table == [' ']):
                    table = []
                    data_1 = datetime.strptime(data[1], '%Y-%m')
                    data_1 = data_1.strftime('%m-%Y')
                    url = 'https://bvmf.bmfbovespa.com.br/sig/FormConsultaMercVista.asp?strTipoResumo=RES_MERC_VISTA&strSocEmissora=' + data[0] + '&strDtReferencia=' + data_1 + '&intCodNivel=2&intCodCtrl=160'
                    manual_trouble = [['BEPA', '2003-08'], ['BEPA', '2003-06'], ['BEPA', '2003-04'], ['BEPA', '2003-03'], ['BEPA', '2003-02']]
                    manual_trouble_0 = ['BEPA']
                    if data[0] in manual_trouble_0:
                        print('trouble', manual_trouble_0)
                    else:
                        browser.get(url)
                        for r in range(0,20,2):
                            xpath = '//*[@id="tblResDiario"]/tbody/tr[' + str(r) + ']/td/table'
                            quotes = get_quotes(xpath, data)
                            if quotes:
                                table.append(quotes)
                        table = [item for sublist in table for item in sublist]
                        if not table:
                            table = [' ']
                        table = list_to_csv(filename_historical_price, table)
                        company_historical.append(table)

  except Exception as e:
    trouble(e, sys._getframe().f_code.co_name)


if __name__ == '__main__':
  try:
    print(config.module_message)
  except:
    pass
