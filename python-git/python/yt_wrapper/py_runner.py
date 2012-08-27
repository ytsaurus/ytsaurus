#!/usr/bin/env python

from dill import load

import imp
import sys
from itertools import chain, imap
from zipfile import ZipFile

if __name__ == "__main__":
    operation_dump = sys.argv[1]
    modules_archive = sys.argv[2]
    main_filename = sys.argv[3]
    main_module_name = sys.argv[4]
    
    with ZipFile(modules_archive) as zip:
        zip.extractall("modules")
    sys.path = ["./modules"] + sys.path
    
    # Magically replace out main module  by client side main
    sys.modules['__main__'] = imp.load_module(main_module_name, open(main_filename, 'U'), main_filename, ('.py', 'U', 1))
    imp.reload(sys)
    
    operation = load(open(operation_dump))
    sys.stdout.writelines(chain(*imap(operation, sys.stdin.xreadlines())))


