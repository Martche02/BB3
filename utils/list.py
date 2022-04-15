import utils.config as config
import utils.system as system
import sys
import csv

def merge(l1, l2):
    """merge l1 + l2 and remove duplicates ['a', 'b'] + ['b', 'c'] = ['a', 'b', 'c']"""
    # print(sys._getframe().f_code.co_name)
    try:
        li3 = []
        [li3.append(i) for i in l1 + l2 if i not in li3]
        return li3
    except Exception as e:
        system.trouble(e, sys._getframe().f_code.co_name)
def subtract(l1, l2):
    """remove l2 from l1  ['a', 'b'] - ['b', 'c'] = ['a']"""
    # print(sys._getframe().f_code.co_name)
    try:
        li3 = [i for i in l1 if i not in l2]
        return li3
    except Exception as e:
        system.trouble(e, sys._getframe().f_code.co_name)
def inclusives(l1, l2):
    """only common_terms ['a', 'b'] and ['b', 'c'] = ['b']"""
    # print(sys._getframe().f_code.co_name)
    try:
        li3 = [value for value in l1 if value in l2]
        return li3
    except Exception as e:
        system.trouble(e, sys._getframe().f_code.co_name)
def exclusives(l1, l2):
    """only unique items from both lists ['a', 'b'] - ['b', 'c'] = ['a', 'c']"""
    # print(sys._getframe().f_code.co_name)
    try:
        li3 = [i for i in l1 + l2 if i not in l1 or i not in l2]
        return li3
    except Exception as e:
        system.trouble(e, sys._getframe().f_code.co_name)

def to_csv(file, list):
    try:
        with open(config.data_folder + file + '.csv', 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerows(list)
        return list
    except:
        pass
def from_csv(file):
    try:
        list = []
        with open(config.data_folder + file + '.csv', 'r') as f:
            reader = csv.reader(f)
            for row in reader:
                list.append(row)
        return list
    except:
        return False

if __name__ == '__main__':
    try:
        print(config.module_message)
    except:
        pass
