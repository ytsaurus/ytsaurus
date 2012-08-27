#!/usr/bin/env python

from cPickle import load

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
    
    # Magically replace out main module  by client side main
    sys.modules['__main__'] = imp.load_module(main_filename, open(main_module_name, 'U'), main_module_name, ('.py', 'U', 1))
    
    operation = load(open(operation_dump))
    sys.stdout.writelines(chain(*imap(operation, sys.stdin.xreadlines())))


