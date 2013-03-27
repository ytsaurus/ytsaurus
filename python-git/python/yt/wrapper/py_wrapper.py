from pickling import dump
import config

from common import get_value
from errors import YtError

from yt.zip import ZipFile

import os
import sys
import shutil
import tempfile
import types

LOCATION = os.path.dirname(os.path.abspath(__file__))

def module_relpath(module):
    extensions = get_value(config.PYTHON_FUNCTION_SEARCH_EXTENSIONS, ["py", "pyc", "so"])
    if module.__name__ == "__main__":
        return module.__file__
    for init in ["", "/__init__"]:
        for ext in extensions:
            rel_path = "%s%s.%s" % (module.__name__.replace(".", "/"), init, ext)
            if module.__file__.endswith(rel_path):
                return rel_path
    return None
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

    # We don't use with statement for compatibility with python2.6
    with ZipFile(zip_filename, "w") as zip:
        for module in sys.modules.values():
            if config.PYTHON_FUNCTION_MODULE_FILTER is not None and \
                    not config.PYTHON_FUNCTION_MODULE_FILTER(module):
                continue
            if hasattr(module, "__file__"):
                relpath = module_relpath(module)
                if relpath is None and config.PYTHON_FUNCTION_STRONG_CHECK:
                    raise YtError("Cannot determine relative path of module " + str(module))
                zip.write(module.__file__, relpath)

    main_filename = tempfile.mkstemp(dir="/tmp", prefix="_main_module")[1] + ".py"
    shutil.copy(sys.modules['__main__'].__file__, main_filename)

    config_filename = tempfile.mkstemp(dir="/tmp", prefix="config_dump")[1]
    config_dict = {}
    for key in dir(config):
        value = config.__dict__[key]
        is_bad = any(isinstance(value, type)
                     for type in [types.ModuleType, types.FileType, types.EllipsisType])
        if is_bad or key.startswith("__"):# or key == "DEFAULT_STRATEGY":
            continue
        config_dict[key] = value
    with open(config_filename, "w") as fout:
        dump(config_dict, fout)

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
