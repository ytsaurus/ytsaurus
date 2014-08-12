from pickling import dump
import config
import format
import format_config

import yt.yson
from yt.zip import ZipFile
import yt.logger as logger
from errors import YtError

import imp
import os
import sys
import shutil
import tempfile
import types

LOCATION = os.path.dirname(os.path.abspath(__file__))

def module_relpath(module_name, module_file):
    if config.PYTHON_FUNCTION_SEARCH_EXTENSIONS is None:
        suffixes = [suf for suf, _, _ in imp.get_suffixes()]
    else:
        suffixes = ["." + ext for ext in config.PYTHON_FUNCTION_SEARCH_EXTENSIONS]

    if module_name == "__main__":
        return module_file
    for init in ["", "/__init__"]:
        for suf in suffixes:
            rel_path = ''.join([module_name.replace(".", "/"), init, suf])
            if module_file.endswith(rel_path):
                return rel_path
    return None
    #!!! It is wrong solution, because modules can affect sys.path while importing
    #!!! Do not delete it to prevent wrong refactoring in the future.
    # module_path = module.__file__
    #for path in sys.path:
    #    if module_path.startswith(path):
    #        relpath = module_path[len(path):]
    #        if relpath.startswith("/"):
    #            relpath = relpath[1:]
    #        return relpath

def find_file(path):
    while path != "/":
        if os.path.isfile(path):
            return path
        path = os.path.dirname(path)

def wrap(function, operation_type, input_format=None, output_format=None, reduce_by=None):
    assert operation_type in ["mapper", "reducer", "reduce_combiner"]
    function_filename = tempfile.mkstemp(dir=config.LOCAL_TMP_DIR, prefix=".operation.dump")[1]
    with open(function_filename, "w") as fout:
        attributes = function.attributes if hasattr(function, "attributes") else {}
        dump((function, attributes, operation_type, input_format, output_format, reduce_by), fout)

    if isinstance(input_format, format.YsonFormat) and yt.yson.TYPE == "PYTHON":
        raise YtError("Using python implementation of yson parser in operations "
                      "is forbidden because of memory limit issues. "
                      "Install yandex-yt-python-yson to fix this problem.")

    compressed_files = set()
    zip_filename = tempfile.mkstemp(dir=config.LOCAL_TMP_DIR, prefix=".modules.zip")[1]
    with ZipFile(zip_filename, "w") as zip:
        for module in sys.modules.values():
            if config.PYTHON_FUNCTION_MODULE_FILTER is not None and \
                    not config.PYTHON_FUNCTION_MODULE_FILTER(module):
                continue
            if hasattr(module, "__file__"):
                file = find_file(module.__file__)
                if file is None or not os.path.isfile(file):
                    logger.warning("Cannot find file of module %s", module.__file__)
                    continue

                if config.PYTHON_DO_NOT_USE_PYC and file.endswith(".pyc"):
                    file = file[:-1]

                relpath = module_relpath(module.__name__, file)
                if relpath is None:
                    raise YtError("Cannot determine relative path of module " + str(module))

                if relpath in compressed_files:
                    continue
                compressed_files.add(relpath)

                zip.write(file, relpath)

    main_filename = tempfile.mkstemp(dir=config.LOCAL_TMP_DIR, prefix="_main_module", suffix=".py")[1]
    shutil.copy(sys.modules['__main__'].__file__, main_filename)

    config_filename = tempfile.mkstemp(dir=config.LOCAL_TMP_DIR, prefix="config_dump")[1]
    config_dict = {}
    for key in dir(format_config):
        value = format_config.__dict__[key]
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
    """Decorate mapper function to consume *iterator of rows* instead of single row."""
    _init_attributes(func)
    func.attributes["is_aggregator"] = True
    return func

def raw(func):
    """Decorate mapper function to consume *raw data stream* instead of single row."""
    _init_attributes(func)
    func.attributes["is_raw"] = True
    return func
