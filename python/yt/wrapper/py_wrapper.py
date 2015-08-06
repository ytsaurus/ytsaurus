import config
from config import get_config
from pickling import Pickler

from yt.zip import ZipFile
import yt.logger as logger

import imp
import inspect
import os
import sys
import shutil

LOCATION = os.path.dirname(os.path.abspath(__file__))

# Modules below are imported to force their addition to modules archive
OPERATION_REQUIRED_MODULES = ['_py_runner_helpers']

def is_running_interactively():
    # Does not work in bpython
    if hasattr(sys, 'ps1'):
        return True
    else:
        # Old IPython (0.12 at least) has no sys.ps1 defined
        try:
            __IPYTHON__
        except NameError:
            return False
        else:
            return True

def module_relpath(module_name, module_file, client):
    search_extensions = get_config(client)["pickling"]["search_extensions"]
    if search_extensions is None:
        suffixes = [suf for suf, _, _ in imp.get_suffixes()]
    else:
        suffixes = ["." + ext for ext in search_extensions]

    if module_name == "__main__":
        return module_file
    for init in ["", "/__init__"]:
        for suf in suffixes:
            rel_path = ''.join([module_name.replace(".", "/"), init, suf])
            if module_file.endswith(rel_path):
                return rel_path
    if module_file.endswith(".egg"):
        return os.path.basename(module_file)
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

def create_modules_archive(tempfiles_manager, client):
    create_modules_archive_function = get_config(client)["pickling"]["create_modules_archive_function"]
    if create_modules_archive_function is not None:
        return create_modules_archive_function()

    for module_name in OPERATION_REQUIRED_MODULES:
        module_info = imp.find_module(module_name, [LOCATION])
        imp.load_module(module_name, *module_info)

    compressed_files = set()
    zip_filename = tempfiles_manager.create_tempfile(dir=get_config(client)["local_temp_directory"],
                                                     prefix=".modules.zip")
    module_filter = get_config(client)["pickling"]["module_filter"]
    with ZipFile(zip_filename, "w") as zip:
        for module in sys.modules.values():
            if module_filter is not None and \
                    not module_filter(module):
                continue
            if hasattr(module, "__file__"):
                file = find_file(module.__file__)
                if file is None or not os.path.isfile(file):
                    logger.warning("Cannot find file of module %s", module.__file__)
                    continue

                if get_config(client)["pickling"]["force_using_py_instead_of_pyc"] and file.endswith(".pyc"):
                    file = file[:-1]

                relpath = module_relpath(module.__name__, file, client)
                if relpath is None:
                    logger.warning("Cannot determine relative path of module " + str(module))
                    continue

                if relpath in compressed_files:
                    continue
                compressed_files.add(relpath)

                zip.write(file, relpath)

    return zip_filename

def get_function_name(function):
    if hasattr(function, "__name__"):
        return function.__name__
    elif hasattr(function, "__class__") and hasattr(function.__class__, "__name__"):
        return function.__class__.__name__
    else:
        return "operation"

def wrap(function, operation_type, tempfiles_manager, input_format=None, output_format=None, reduce_by=None, client=None):
    assert operation_type in ["mapper", "reducer", "reduce_combiner"]
    local_temp_directory = get_config(client)["local_temp_directory"]
    function_filename = tempfiles_manager.create_tempfile(dir=local_temp_directory,
                                                          prefix=get_function_name(function) + ".")

    pickler = Pickler(get_config(client)["pickling"]["framework"])

    with open(function_filename, "w") as fout:
        attributes = function.attributes if hasattr(function, "attributes") else {}
        pickler.dump((function, attributes, operation_type, input_format, output_format, reduce_by), fout)

    config_filename = tempfiles_manager.create_tempfile(dir=local_temp_directory,
                                                        prefix="config_dump")
    with open(config_filename, "w") as fout:
        Pickler(config.DEFAULT_PICKLING_FRAMEWORK).dump(get_config(client), fout)

    if attributes.get('is_simple', False):
        files = [function_filename, config_filename] + [os.path.join(LOCATION, module + ".py")
                                                        for module in OPERATION_REQUIRED_MODULES]
        return ("PYTHONPATH=. python _py_runner.py " + " ".join([
                    os.path.basename(function_filename),
                    os.path.basename(config_filename)]),
                os.path.join(LOCATION, "_py_runner.py"), files)

    zip_filename = create_modules_archive(tempfiles_manager, client)
    main_filename = tempfiles_manager.create_tempfile(dir=local_temp_directory,
                                                      prefix="_main_module", suffix=".py")
    main_module_type = "PY_SOURCE"
    if is_running_interactively():
        try:
            function_source_filename = inspect.getfile(function)
        except TypeError:
            function_source_filename = None
        else:
            # If function is defined in terminal path is <stdin> or
            # <ipython-input-*>
            if not os.path.exists(function_source_filename):
                function_source_filename = None
    else:
        function_source_filename = sys.modules['__main__'].__file__
        if function_source_filename.endswith("pyc"):
            main_module_type = "PY_COMPILED"
    if function_source_filename:
        shutil.copy(function_source_filename, main_filename)

    return ("python _py_runner.py " + " ".join([
                os.path.basename(function_filename),
                os.path.basename(config_filename),
                os.path.basename(zip_filename),
                os.path.basename(main_filename),
                "_main_module",
                main_module_type]),
            os.path.join(LOCATION, "_py_runner.py"),
            [function_filename, config_filename, zip_filename, main_filename])

def _set_attribute(func, key, value):
    if not hasattr(func, "attributes"):
        func.attributes = {}
    func.attributes[key] = value
    return func

def aggregator(func):
    """Decorate function to consume *iterator of rows* instead of single row."""
    return _set_attribute(func, "is_aggregator", True)

def raw(func):
    """Decorate function to consume *raw data stream* instead of single row."""
    return _set_attribute(func, "is_raw", True)

def raw_io(func):
    """Decorate function to run as is. No arguments are passed. Function handles IO."""
    return _set_attribute(func, "is_raw_io", True)

def simple(func):
    """Decorate function to be simple - without code or variable dependencies outside of body.
    It prevents library to collect dependency modules and send it with function."""
    return _set_attribute(func, "is_simple", True)
