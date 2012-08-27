from dill import dump

from common import YtError

import os
import sys
import shutil
from zipfile import ZipFile

def module_relpath(module):
    if module.__name__ == "__main__":
        return module.__file__
    for init in ["", "/__init__"]:
        for ext in ["py", "pyc", "so"]:
            rel_path = "%s%s.%s" % (module.__name__.replace(".", "/"), init, ext)
            if module.__file__.endswith(rel_path):
                return rel_path
    raise YtError("Cannot determine relative path of module " + str(module))
    #!!! It is wrong solution, beacause modules can affect sys.path while importing
    # module_path = module.__file__
    #for path in sys.path:
    #    if module_path.startswith(path):
    #        relpath = module_path[len(path):]
    #        if relpath.startswith("/"):
    #            relpath = relpath[1:]
    #        return relpath

def wrap(function):
    function_filename = "/tmp/.operation.dump"
    with open(function_filename, "w") as fout:
        dump(function, fout)
    
    zip_filename = "/tmp/.modules.zip"
    with ZipFile(zip_filename, "w") as zip:
        for module in sys.modules.values():
            if hasattr(module, "__file__"):
                zip.write(module.__file__, module_relpath(module))

    main_filename = "/tmp/_main_module.py"
    shutil.copy(sys.modules['__main__'].__file__, main_filename)

    return ("./py_runner.py {0} {1} {2} {3}".\
                format(os.path.basename(function_filename), 
                       os.path.basename(zip_filename),
                       os.path.basename(main_filename),
                       "_main_module"),
            ["py_runner.py", function_filename, zip_filename, main_filename])
