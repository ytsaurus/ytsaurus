import imp
import sys
import itertools
import zipfile

if __name__ == "__main__":
    # Variable names start with "__" to avoid accidental intersection with scope of user function
    __operation_dump = sys.argv[1]
    __modules_archive = sys.argv[2]
    __main_filename = sys.argv[3]
    __main_module_name = sys.argv[4]
    
    with zipfile.ZipFile(__modules_archive) as __zip:
        __zip.extractall("modules")
    sys.path = ["./modules"] + sys.path
    
    import yt.dill as dill

    # Magically replace out main module  by client side main
    sys.modules['__main__'] = imp.load_module(__main_module_name, open(__main_filename, 'U'), __main_filename, ('.py', 'U', 1))
    imp.reload(sys)
    
    operation = dill.load(open(__operation_dump))
    sys.stdout.writelines(itertools.chain(*itertools.imap(operation, sys.stdin.xreadlines())))


