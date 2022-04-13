module_message = 'This is a module, not a standalone script, and will not run as a standalone script'

if __name__ == '__main__':
    try:
        print(module_message)
    except:
        pass
