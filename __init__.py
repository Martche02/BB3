"""Grab financial data from brazilian B3 site and organize them in free Google Sheets

Project has the following stages 
*0* = LOAD
load basic variables and functions
run grab code 

*A* = HISTORICAL PRICES
A1 = grab
a1.1 = companies
a1.2 = data range
a1.3 = historical prices
A2 = update
?
*B* = FINANCIAL STATEMENTS
B1 = grab
b1.1 = companies


OLD INDEX
1. From starting page get, clean and treat full list of companies and update a General Purpose google Spreadsheet GPS
2. For each bovespa in GPS,
2.a. Create Company coogle Spreadsheet, get specific bovespa data and update CS info
2.b. Get Company List of Reports CLR available and save CLR list in CS
3. For each report page of each bovespa, get each statement type data,
3.a. Combine them by quarter
3.b. Create a sheet for each quarter in CS and save raw numbers
4. Get and combine all quarters and all statements in a Reports sheet in CS
5. Clean and treat, and pivot data by quarter and by year in news sheets in CS



"""