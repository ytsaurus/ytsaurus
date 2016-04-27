import config
from config import get_config
from pickling import Pickler
from cypress_commands import get
from common import get_python_version, YtError, chunk_iter_stream, chunk_iter_string, get_value
from errors import YtResponseError
from py_runner_helpers import process_rows

from yt.packages.importlib import import_module
from yt.zip import ZipFile
import yt.logger as logger

import imp
import string
import inspect
import os
import shutil
import socket
import tempfile
import hashlib
import sys
import time
import logging
import pickle as standard_pickle

LOCATION = os.path.dirname(os.path.abspath(__file__))
TMPFS_SIZE_MULTIPLIER = 1.01
TMPFS_SIZE_ADDEND = 1024 * 1024

# Modules below are imported to force their addition to modules archive.
OPERATION_REQUIRED_MODULES = ["yt.wrapper.py_runner_helpers"]

SINGLE_INDEPENDENT_BINARY_CASE = False

# Md5 tools.
def init_md5():
    return []

def calc_md5_from_file(filename):
    with open(filename, mode="rb") as fin:
        h = hashlib.md5()
        for buf in chunk_iter_stream(fin, 1024):
            h.update(buf)
    return tuple(map(ord, h.digest()))

def calc_md5_from_string(string):
    h = hashlib.md5()
    for buf in chunk_iter_string(string, 1024):
        h.update(buf)
    return tuple(map(ord, h.digest()))

def merge_md5(lhs, rhs):
    return lhs + [rhs]

def hex_md5(md5_array):
    def to_hex(md5):
        # String "zip_salt_" is neccessary to distinguish empty archive from empty file.
        return "zip_salt_" + "".join(["{0:02x}".format(num) for num in md5])

    md5_array.sort()
    return to_hex(calc_md5_from_string("".join(map(to_hex, md5_array))))

def calc_md5_string_from_file(filename):
    return hex_md5([calc_md5_from_file(filename)])

# Misc functions.
def round_up_to(num, divider):
    if num % divider == 0:
        return num
    else:
        return (1 + (num / divider)) * divider

def get_disk_size(filepath):
    stat = os.stat(filepath)
    if hasattr(stat, "st_blocks") and hasattr(stat, "st_blksize"):
        return stat.st_blocks * stat.st_blksize
    else:
        return round_up_to(stat.st_size, 4 * 1024)

def is_running_interactively():
    # Does not work in bpython
    if hasattr(sys, 'ps1'):
        return True
    else:
        # Old IPython (0.12 at least) has no sys.ps1 defined
        return "__IPYTHON__" in globals()

def is_local_mode(client):
    local_mode = get_config(client)["pickling"]["local_mode"]
    if local_mode is not None:
        return local_mode

    fqdn = None
    try:
        fqdn = get("//sys/@local_mode_fqdn", client=client)
    except YtResponseError as err:
        if not err.is_resolve_error():
            raise

    return fqdn == socket.getfqdn()

class TempfilesManager(object):
    def __init__(self, remove_temp_files):
        self._remove_temp_files = remove_temp_files
        self._tempfiles_pool = []

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        if self._remove_temp_files:
            for file in self._tempfiles_pool:
                os.remove(file)

    def create_tempfile(self, *args, **kwargs):
        """Use syntax tempfile.mkstemp"""
        fd, filepath = tempfile.mkstemp(*args, **kwargs)
        os.close(fd)
        # NB: files should be accesible from jobs in local mode.
        os.chmod(filepath, 0o755)
        self._tempfiles_pool.append(filepath)
        return filepath

def module_relpath(module_name, module_file, client):
    search_extensions = get_config(client)["pickling"]["search_extensions"]
    if search_extensions is None:
        suffixes = [suf for suf, _, _ in imp.get_suffixes()]
    else:
        suffixes = ["." + ext for ext in search_extensions]

    if module_name == "__main__":
        return module_file
    for init in ["", os.sep + "__init__"]:
        for suf in suffixes:
            rel_path = ''.join([module_name.replace(".", os.sep), init, suf])
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
    if path == "<frozen>":
        return None
    while path != "/":
        if os.path.isfile(path):
            return path
        dirname = os.path.dirname(path)
        if dirname == path:
            return None
        path = dirname

class Zip(object):
    def __init__(self, prefix, tempfiles_manager, client):
        self.prefix = prefix
        self.filename = tempfiles_manager.create_tempfile(dir=get_config(client)["local_temp_directory"],
                                                          prefix=prefix, suffix=".zip")
        self.size = 0
        self.hash = init_md5()

    def __enter__(self):
        self.zip = ZipFile(self.filename, "w")
        self.zip.__enter__()
        return self

    def append(self, filepath, relpath):
        self.zip.write(filepath, relpath)
        self.size += get_disk_size(filepath)
        self.hash = merge_md5(self.hash, calc_md5_from_file(filepath))

    def __exit__(self, type, value, traceback):
        self.zip.__exit__(type, value, traceback)
        if type is None:
            self.md5 = hex_md5(self.hash)

def create_modules_archive_default(tempfiles_manager, client):
    for module_name in OPERATION_REQUIRED_MODULES:
        import_module(module_name)

    logging_level = logging._levelNames[get_config(client)["pickling"]["find_module_file_error_logging_level"]]

    files_to_compress = {}
    module_filter = get_config(client)["pickling"]["module_filter"]
    for module in sys.modules.values():
        if module_filter is not None and \
                not module_filter(module):
            continue
        if hasattr(module, "__file__"):
            file = find_file(module.__file__)
            if file is None or not os.path.isfile(file):
                if logger.LOGGER.isEnabledFor(logging_level):
                    logger.log(logging_level, "Cannot find file of module %s", module.__file__)
                continue

            file = os.path.abspath(file)

            if get_config(client)["pickling"]["force_using_py_instead_of_pyc"] and file.endswith(".pyc"):
                file = file[:-1]

            relpath = module_relpath(module.__name__, file, client)
            if relpath is None:
                if logger.LOGGER.isEnabledFor(logging_level):
                    logger.log(logging_level, "Cannot determine relative path of module " + str(module))
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

                with open(init_file, "wb") as f:
                    f.write("#")  # Should not be empty. Empty comment is ok.

                module_name_parts = module.__name__.split(".") + ["__init__.py"]
                destination_name = os.path.join(*module_name_parts)
                files_to_compress[destination_name] = init_file

    now = time.time()
    with Zip(prefix="modules", tempfiles_manager=tempfiles_manager, client=client) as zip:
        with Zip(prefix="fresh_modules", tempfiles_manager=tempfiles_manager, client=client) as fresh_zip:
            for relpath, filepath in sorted(files_to_compress.items()):
                age = now - os.path.getmtime(filepath)
                if age > get_config(client)["pickling"]["fresh_files_threshold"]:
                    zip.append(filepath, relpath)
                else:
                    fresh_zip.append(filepath, relpath)
            for filepath, relpath in get_value(get_config(client)["pickling"]["additional_files_to_archive"], []):
                zip.append(filepath, relpath)

    archives = [zip]
    if fresh_zip.size > 0:
        archives.append(fresh_zip)

    result = [{
            "filename": archive.filename,
            "tmpfs": get_config(client)["pickling"]["enable_tmpfs_archive"] and not is_local_mode(client),
            "size": archive.size,
            "hash": archive.md5
        }
        for archive in archives]

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

def simplify(function_name):
    def fix(sym):
        if sym not in string.ascii_letters and sym not in string.digits:
            return "_"
        return sym
    return "".join(map(fix, function_name[:30]))

def get_function_name(function):
    if hasattr(function, "__name__"):
        return simplify(function.__name__)
    elif hasattr(function, "__class__") and hasattr(function.__class__, "__name__"):
        return simplify(function.__class__.__name__)
    else:
        return "operation"

def wrap(tempfiles_manager, client, **kwargs):
    local_mode = is_local_mode(client)
    remove_temp_files = get_config(client)["clear_local_temp_files"] and not local_mode

    if tempfiles_manager is None:
        with TempfilesManager(remove_temp_files) as new_tempfiles_manager:
            return do_wrap(tempfiles_manager=new_tempfiles_manager, client=client, local_mode=local_mode, **kwargs)
    else:
        return do_wrap(tempfiles_manager=tempfiles_manager, client=client, local_mode=local_mode, **kwargs)

def do_wrap(function, operation_type, tempfiles_manager, input_format, output_format, group_by, local_mode, uploader, client):
    assert operation_type in ["mapper", "reducer", "reduce_combiner"]
    local_temp_directory = get_config(client)["local_temp_directory"]
    function_filename = tempfiles_manager.create_tempfile(dir=local_temp_directory,
                                                          prefix=get_function_name(function) + ".")

    pickler = Pickler(get_config(client)["pickling"]["framework"])

    with open(function_filename, "wb") as fout:
        attributes = function.attributes if hasattr(function, "attributes") else {}
        pickler.dump((function, attributes, operation_type, input_format, output_format, group_by, get_python_version()), fout)

    config_filename = tempfiles_manager.create_tempfile(dir=local_temp_directory,
                                                        prefix="config_dump")
    with open(config_filename, "wb") as fout:
        Pickler(config.DEFAULT_PICKLING_FRAMEWORK).dump(get_config(client), fout)

    if SINGLE_INDEPENDENT_BINARY_CASE:
        files = map(os.path.abspath, [
            sys.argv[0],
            function_filename,
            config_filename])

        if local_mode:
            file_args = files
            uploaded_files = []
            local_files_to_remove = tempfiles_manager._tempfiles_pool
        else:
            file_args = map(os.path.basename, files)
            uploaded_files = uploader(files)
            local_files_to_remove = []

        cmd = "./" + " ".join(file_args)

        return cmd, uploaded_files, local_files_to_remove, 0

    modules_info = create_modules_archive(tempfiles_manager, client)
    # COMPAT: previous version of create_modules_archive returns string.
    if isinstance(modules_info, basestring):
        modules_info = [{"filename": modules_info, "hash": calc_md5_string_from_file(modules_info), "tmpfs": False}]
    modules_filenames = [{"filename": info["filename"], "hash": info["hash"]}
                         for info in modules_info]
    tmpfs_size = sum([info["size"] for info in modules_info if info["tmpfs"]])
    if tmpfs_size > 0:
        tmpfs_size = int(TMPFS_SIZE_ADDEND + TMPFS_SIZE_MULTIPLIER * tmpfs_size)

    for info in modules_info:
        if local_mode:
            info["filename"] = os.path.abspath(info["filename"])
        else:
            info["filename"] = os.path.basename(info["filename"])

    modules_info_filename = tempfiles_manager.create_tempfile(dir=local_temp_directory,
                                                              prefix="_modules")
    with open(modules_info_filename, "wb") as fout:
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

    files = map(os.path.abspath, [
        os.path.join(LOCATION, "_py_runner.py"),
        function_filename,
        config_filename,
        modules_info_filename,
        main_filename])

    if local_mode:
        file_args = files
        uploaded_files = []
        local_files_to_remove = tempfiles_manager._tempfiles_pool
    else:
        file_args = map(os.path.basename, files)
        uploaded_files = uploader(files + modules_filenames)
        local_files_to_remove = []

    cmd = " ".join([get_config(client)["pickling"]["python_binary"]] + file_args + ["_main_module", main_module_type])

    return cmd, uploaded_files, local_files_to_remove, tmpfs_size

def enable_python_job_processing_for_standalone_binary():
    """ Enables alternative method to run python functions as jobs in YT operations.
    This method sends into the job only pickled function and various program settings
    and do not send modules that used by the program. Therefore this method works
    correctly only if your script is a standalone binary and executed as binary.

    You should call this function in the beggining of the program.
    """
    global SINGLE_INDEPENDENT_BINARY_CASE
    if os.environ.get("YT_WRAPPER_IS_INSIDE_JOB"):
        process_rows(sys.argv[1], sys.argv[2], start_time=None)
        sys.exit(0)
    else:
        SINGLE_INDEPENDENT_BINARY_CASE = True

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

