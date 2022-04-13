print('load lists.py')

def merge(l1, l2):
    """
    Merge two sorted lists into one sorted list
    """
    print ('list merge')
    try:
        merged = []
        while l1 and l2:
            if l1[0] < l2[0]:
                merged.append(l1.pop(0))
            else:
                merged.append(l2.pop(0))
    except Exception as e:
        print(e)

def subtract(l1, l2):
    """
    Subtract l2 from l1
    """
    print ('list subtract')
    try:
        merged = []
        while l1 and l2:
            if l1[0] < l2[0]:
                merged.append(l1.pop(0))
            else:
                merged.append(l2.pop(0))
    except Exception as e:
        print(e)

if __name__ == '__main__':
    try:
        print(config.module_message)
    except:
        pass
