from pickling import dump
import config

from common import YtError

from yt.zip import ZipFile

import os
import sys
import shelve
import shutil
import tempfile
import types

LOCATION = os.path.dirname(os.path.abspath(__file__))

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
    #!!! Do not delete it to prevent wrong refactoring in the future.
    # module_path = module.__file__
    #for path in sys.path:
    #    if module_path.startswith(path):
    #        relpath = module_path[len(path):]
    #        if relpath.startswith("/"):
    #            relpath = relpath[1:]
    #        return relpath

def wrap(function, operation_type, reduce_by=None):
    assert operation_type in ["mapper", "reducer"]
    function_filename = tempfile.mkstemp(dir="/tmp", prefix=".operation.dump")[1]
    with open(function_filename, "w") as fout:
        attributes = function.attributes if hasattr(function, "attributes") else {}
        dump((function, attributes, operation_type, reduce_by), fout)

    zip_filename = tempfile.mkstemp(dir="/tmp", prefix=".modules.zip")[1]

    shutil.copy(function_filename, "//home/ignat/dump")

    # We don't use with statement for compatibility with python2.6
    with ZipFile(zip_filename, "w") as zip:
        for module in sys.modules.values():
            if hasattr(module, "__file__"):
                zip.write(module.__file__, module_relpath(module))

    main_filename = tempfile.mkstemp(dir="/tmp", prefix="_main_module")[1] + ".py"
    shutil.copy(sys.modules['__main__'].__file__, main_filename)

    config_filename = tempfile.mkstemp(dir="/tmp", prefix="config_dump")[1] + ".db"
    config_shelve = shelve.open(config_filename)

    try:
        for key in dir(config):
            value = config.__dict__[key]
            is_bad = any(isinstance(value, type)
                         for type in [types.ModuleType, types.FileType, types.EllipsisType])
            if is_bad or key.startswith("__"):# or key == "DEFAULT_STRATEGY":
                continue
            config_shelve[key] = value
    finally:
        config_shelve.close()

    return ("python _py_runner.py " + " ".join([
                os.path.basename(function_filename),
                os.path.basename(zip_filename),
                os.path.basename(main_filename),
                "_main_module",
                os.path.basename(config_filename)]),
            os.path.join(LOCATION, "_py_runner.py"),
            [function_filename, zip_filename, main_filename, config_filename])

def _init_attributes(func):
    if not hasattr(func, "attributes"):
        func.attributes = {}

def aggregator(func):
    _init_attributes(func)
    func.attributes["is_aggregator"] = True
    return func

def raw(func):
    _init_attributes(func)
    func.attributes["is_raw"] = True
    return func
