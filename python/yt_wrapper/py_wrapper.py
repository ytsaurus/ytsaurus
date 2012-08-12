from dill import *

import os
import sys
from zipfile import ZipFile

def module_relpath(module_path):
    for path in sys.path:
        if module_path.startswith(path):
            relpath = module_path[len(path):]
            if relpath.startswith("/"):
                relpath = relpath[1:]
            return relpath

def wrap(function):
    function_filename = "/tmp/.operation.dump"
    with open(function_filename, "w") as fout:
        dump(function, fout)
    
    zip_filename = "/tmp/.modules.zip"
    with ZipFile(zip_filename, "w") as zip:
        for module in sys.modules.values():
            if hasattr(module, __file__):
                zip.write(module.__file__, module_relpath(module.__file__))

    return ("PYTHONPATH=modules:$PYTHONPATH ./py_runner.py {0} {1}".\
                format(os.path.basename(function_filename), 
                       os.path.basename(zip_filename)),
            ["py_runner.py", function_filename, zip_filename])
