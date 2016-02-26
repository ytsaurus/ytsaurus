import config
from config import get_config
from pickling import Pickler
from common import get_python_version, YtError

from yt.packages.importlib import import_module
from yt.zip import ZipFile
import yt.logger as logger

import imp
import pipes
import inspect
import os
import sys
import shutil
import pickle as standard_pickle
import time

LOCATION = os.path.dirname(os.path.abspath(__file__))
TMPFS_SIZE_MULTIPLIER = 1.05
TMPFS_SIZE_ADDEND = 16 * 1024 * 1024

# Modules below are imported to force their addition to modules archive.
OPERATION_REQUIRED_MODULES = ["yt.wrapper.py_runner_helpers"]

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
    origin_path = path
    while path != "/":
        if os.path.isfile(path):
            return path
        dirname = os.path.dirname(path)
        if dirname == path:
            raise YtError("Incorrect module path " + origin_path)
        path = dirname


def create_modules_archive_default(tempfiles_manager, client):
    for module_name in OPERATION_REQUIRED_MODULES:
        import_module(module_name)

    files_to_compress = {}
    module_filter = get_config(client)["pickling"]["module_filter"]
    for module in sys.modules.values():
        if module_filter is not None and \
                not module_filter(module):
            continue
        if hasattr(module, "__file__"):
            file = find_file(module.__file__)
            if file is None or not os.path.isfile(file):
                logger.warning("Cannot find file of module %s", module.__file__)
                continue

            file = os.path.abspath(file)

            if get_config(client)["pickling"]["force_using_py_instead_of_pyc"] and file.endswith(".pyc"):
                file = file[:-1]

            relpath = module_relpath(module.__name__, file, client)
            if relpath is None:
                logger.warning("Cannot determine relative path of module " + str(module))
                continue

            if relpath in files_to_compress:
                continue

            files_to_compress[relpath] = file
        else:
            # Module can be a package without __init__.py, for example,
            # if module is added from *.pth file or manually added in client code.
            # Such module is package if it has __path__ attribute. See st/YT-3337 for more details.
            if hasattr(module, "__path__") and \
                    get_config(client)["pickling"]["create_init_file_for_package_modules"]:
                init_file = tempfiles_manager.create_tempfile(
                    dir=get_config(client)["local_temp_directory"],
                    prefix="__init__.py")

                with open(init_file, "w") as f:
                    f.write("#")  # Should not be empty. Empty comment is ok.

                module_name_parts = module.__name__.split(".") + ["__init__.py"]
                destination_name = os.path.join(*module_name_parts)
                files_to_compress[destination_name] = init_file

    zip_filename = tempfiles_manager.create_tempfile(dir=get_config(client)["local_temp_directory"],
                                                     prefix=".modules", suffix=".zip")

    fresh_zip_filename = tempfiles_manager.create_tempfile(dir=get_config(client)["local_temp_directory"],
                                                           prefix=".fresh.modules", suffix=".zip")

    total_size = 0
    fresh_total_size = 0
    now = time.time()
    with ZipFile(zip_filename, "w") as zip:
        with ZipFile(fresh_zip_filename, "w") as fresh_zip:
            for relpath, filepath in sorted(files_to_compress.items()):
                age = now - os.path.getmtime(filepath)
                if age > get_config(client)["pickling"]["fresh_files_threshold"]:
                    zip.write(filepath, relpath)
                    total_size += os.path.getsize(filepath)
                else:
                    fresh_zip.write(filepath, relpath)
                    fresh_total_size += os.path.getsize(filepath)

    result = [{
        "filename": zip_filename,
        "tmpfs": get_config(client)["pickling"]["enable_tmpfs_archive"],
        "size": total_size}]

    if fresh_total_size > 0:
        result.append({
            "filename": fresh_zip_filename,
            "tmpfs": get_config(client)["pickling"]["enable_tmpfs_archive"],
            "size": fresh_total_size})

    return result


def create_modules_archive(tempfiles_manager, client):
    create_modules_archive_function = get_config(client)["pickling"]["create_modules_archive_function"]
    if create_modules_archive_function is not None:
        if inspect.isfunction(create_modules_archive_function):
            args_spec = inspect.getargspec(create_modules_archive_function)
            args_count = len(args_spec.args)
        elif hasattr(create_modules_archive_function, "__call__"):
            args_spec = inspect.getargspec(create_modules_archive_function.__call__)
            args_count = len(args_spec.args) - 1
        else:
            raise YtError("Cannot determine whether create_modules_archive_function callable or not")
        if args_count > 0:
            return create_modules_archive_function(tempfiles_manager)
        else:
            return create_modules_archive_function()

    return create_modules_archive_default(tempfiles_manager, client)


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
        pickler.dump((function, attributes, operation_type, input_format, output_format, reduce_by, get_python_version()), fout)

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

    modules_info = create_modules_archive(tempfiles_manager, client)
    # COMPAT: previous version of create_modules_archive returns string.
    if isinstance(modules_info, str):
        modules_info = [{"filename": modules_info, "tmpfs": False}]
    modules_filenames = [info["filename"] for info in modules_info]
    tmpfs_size = sum([info["size"] for info in modules_info if info["tmpfs"]])
    if tmpfs_size > 0:
        tmpfs_size = TMPFS_SIZE_ADDEND + TMPFS_SIZE_MULTIPLIER * tmpfs_size

    for info in modules_info:
        info["filename"] = os.path.basename(info["filename"])

    modules_info_filename = tempfiles_manager.create_tempfile(dir=local_temp_directory,
                                                              prefix="_modules")
    with open(modules_info_filename, "w") as fout:
        standard_pickle.dump(modules_info, fout)

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
        # XXX(asaitgalin): when tests are run in parallel their __main__
        # module does not have __file__ attribute.
        if not hasattr(sys.modules["__main__"], "__file__"):
            function_source_filename = None
        else:
            function_source_filename = sys.modules["__main__"].__file__
            if function_source_filename.endswith("pyc"):
                main_module_type = "PY_COMPILED"

    if function_source_filename:
        shutil.copy(function_source_filename, main_filename)

    return (" ".join([get_config(client)["pickling"]["python_binary"],
                      "_py_runner.py",
                      # NB: Function filename may contain special symbols.
                      pipes.quote(os.path.basename(function_filename)),
                      os.path.basename(config_filename),
                      os.path.basename(modules_info_filename),
                      os.path.basename(main_filename),
                      "_main_module",
                      main_module_type]),
            os.path.join(LOCATION, "_py_runner.py"),
            [function_filename, config_filename, modules_info_filename, main_filename] + modules_filenames,
            tmpfs_size)

def _set_attribute(func, key, value):
    if not hasattr(func, "attributes"):
        func.attributes = {}
    func.attributes[key] = value
    return func

def aggregator(func):
    """Decorate function to consume *iterator of rows* instead of single row."""
    return _set_attribute(func, "is_aggregator", True)

def reduce_aggregator(func):
    """Decorate function to consume *iterator of pairs* where each pair consists of key and records with this key."""
    return _set_attribute(func, "is_reduce_aggregator", True)

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
