#!/usr/bin/env python

from dill import *

import sys
from itertools import chain, imap
from zipfile import ZipFile

if __name__ == "__main__":
    operation_dump = sys.argv[1]
    modules_archive = sys.argv[2]
    
    with ZipFile(modules_archive) as zip:
        zip.extractall("modules")
    
    operation = load(open(operation_dump))
    sys.stdout.writelines(chain(*imap(operation, sys.stdin.xreadlines())))


