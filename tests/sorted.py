import csv
def from_csv(folder, file):
    try:
        list = []
        with open(folder + file + '.csv', 'r') as f:
            reader = csv.reader(f)
            for row in reader:
                list.append(row)
        return list
    except:
        return False


filename = 'companies'
companies_full = from_csv('data/', filename)
print(companies_full)

companies_full.sort(key=lambda x: x[0], reverse=False)
print(companies_full)
companies_full = sorted(companies_full, key = lambda x: x[0])
print(companies_full)

print('done')
