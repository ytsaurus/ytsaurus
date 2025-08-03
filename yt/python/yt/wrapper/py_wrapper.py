from __future__ import print_function

from . import config
from .config import get_config
from .pickling import Pickler
from .common import (get_python_version, YtError, chunk_iter_stream, get_value, which,
                     get_disk_size, is_arcadia_python, get_arg_spec, is_inside_job)
from .file_commands import LocalFile
from .py_runner_helpers import process_rows
from .local_mode import is_local_mode, enable_local_files_usage_in_job
from ._py_runner import get_platform_version, main as run_py_runner

import yt.logger as logger
import yt.subprocess_wrapper as subprocess

try:
    from importlib import import_module
except ImportError:
    from yt.packages.importlib import import_module

import re
import copy
import string
import inspect
import itertools
import os
import shutil
import shlex
import tarfile
import typing
import gzip
import tempfile
import hashlib
import sys
import logging
import pickle as standard_pickle
import platform

LOCATION = os.path.dirname(os.path.abspath(__file__))
TMPFS_SIZE_MULTIPLIER = 1.01
TMPFS_SIZE_ADDEND = 1024 * 1024

# Modules below are imported to force their addition to modules archive.
OPERATION_REQUIRED_MODULES = ["yt.wrapper.py_runner_helpers"]

SINGLE_INDEPENDENT_BINARY_CASE = None

YT_RESPOWNED_IN_CONTAINER_KEY = "YT_RESPAWNED_IN_CONTAINER"
_INHERIT_SYS_ARGV = object()


class TarInfo(tarfile.TarInfo):
    @property
    def mtime(self):
        return 0

    @mtime.setter
    def mtime(self, value):
        pass


class WrapResult(object):
    __slots__ = ["cmd", "tmpfs_size", "environment", "local_files_to_remove", "title", "pickling_encryption_key"]

    def __init__(self, cmd, tmpfs_size=0, environment=None, local_files_to_remove=None, title=None, pickling_encryption_key=None):
        self.cmd = cmd
        self.tmpfs_size = tmpfs_size
        self.environment = environment
        self.local_files_to_remove = local_files_to_remove
        self.title = title
        self.pickling_encryption_key = pickling_encryption_key


class OperationParameters(object):
    __slots__ = ["input_format", "output_format", "operation_type", "job_type", "group_by", "should_process_key_switch",
                 "input_table_count", "output_table_count", "use_yamr_descriptors", "attributes", "python_version",
                 "is_local_mode", "has_state", "pickling_encryption_key"]

    def __init__(self, input_format=None, output_format=None, operation_type=None, job_type=None, group_by=None,
                 should_process_key_switch=None, python_version=None, input_table_count=None, output_table_count=None,
                 use_yamr_descriptors=None, attributes=None, is_local_mode=None, has_state=False,
                 pickling_encryption_key=None):
        self.input_format = input_format
        self.output_format = output_format
        self.operation_type = operation_type
        self.job_type = job_type
        self.group_by = group_by
        self.should_process_key_switch = should_process_key_switch
        self.input_table_count = input_table_count
        self.output_table_count = output_table_count
        self.use_yamr_descriptors = use_yamr_descriptors
        self.attributes = attributes
        self.python_version = python_version
        self.is_local_mode = is_local_mode
        self.has_state = has_state
        self.pickling_encryption_key = pickling_encryption_key
        # pickling_encryption_key: None - disabled, "" - autogenerate, "xxx" - use this key


def get_local_temp_directory(client):
    local_temp_directory = get_config(client)["local_temp_directory"]
    if local_temp_directory is not None:
        return local_temp_directory
    return tempfile.gettempdir()


def calc_md5_from_file(filename):
    with open(filename, mode="rb") as fin:
        md5_hash = hashlib.md5()
        for buf in chunk_iter_stream(fin, 1024):
            md5_hash.update(buf)
    return md5_hash.hexdigest()


def is_running_interactively():
    # Does not work in bpython
    if hasattr(sys, 'ps1'):
        return True
    else:
        # Old IPython (0.12 at least) has no sys.ps1 defined
        return "__IPYTHON__" in globals()


class TempfilesManager(object):
    def __init__(self, remove_temp_files, directory):
        self._remove_temp_files = remove_temp_files
        self._tempfiles_pool = []
        self._root_directory = directory

    def __enter__(self):
        self.tmp_dir = tempfile.mkdtemp(prefix="yt_python_tmp_files", dir=self._root_directory)
        # NB: directory should be accessible from jobs in local mode.
        os.chmod(self.tmp_dir, 0o755)
        return self

    def __exit__(self, type, value, traceback):
        if self._remove_temp_files:
            shutil.rmtree(self.tmp_dir, ignore_errors=True)

    def create_tempfile(self, suffix="", prefix="", dir=None):
        """Use syntax tempfile.mkstemp"""
        filepath = os.path.join(self.tmp_dir, prefix + suffix)
        if dir == self._root_directory and not os.path.exists(filepath):
            open(filepath, "a").close()
        else:
            fd, filepath = tempfile.mkstemp(suffix, prefix, dir)
            os.close(fd)
        # NB: files should be accessible from jobs in local mode.
        os.chmod(filepath, 0o755)
        self._tempfiles_pool.append(filepath)
        return filepath


def module_relpath(module_names, module_file, client):
    """
    !!! This comment shows trivial but wrong solution, because modules can affect sys.path while importing.
    !!! Do not delete it to prevent wrong refactoring in the future.
    module_path = module.__file__
    for path in sys.path:
        if module_path.startswith(path):
            relpath = module_path[len(path):]
            if relpath.startswith("/"):
                relpath = relpath[1:]
            return relpath
    """
    search_extensions = get_config(client)["pickling"]["search_extensions"]
    if search_extensions is None:
        import importlib.machinery
        suffixes = importlib.machinery.all_suffixes()
    else:
        suffixes = ["." + ext for ext in search_extensions]

    module_file_parts = module_file.split(os.sep)

    if any(name == "__main__" for name in module_names):
        return module_file

    for suf in suffixes:
        for name in module_names:
            parts = name.split(".")

            for rel_path_parts in (parts[:-1] + [parts[-1] + suf], parts + ["__init__" + suf]):
                if module_file_parts[-len(rel_path_parts):] == rel_path_parts:
                    return os.sep.join(rel_path_parts)

    if module_file.endswith(".egg"):
        return os.path.basename(module_file)

    return None


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


def list_dynamic_library_dependencies(library_path):
    if not which("ldd"):
        raise YtError("Failed to list dynamic library dependencies, ldd not found. To disable automatic shared "
                      "libraries collection set pickling/dynamic_libraries/enable_auto_collection option in "
                      "config to False")

    pattern = re.compile(r"\t(.*) => (.*) \(0x")
    result = []

    ldd_output = subprocess.check_output(["ldd", library_path])
    for line in ldd_output.splitlines():
        match = pattern.match(line)
        if match:
            lib_path = match.group(2)
            if lib_path:
                result.append(lib_path)
    return result


class Tar(object):
    def __init__(self, prefix, tempfiles_manager, client):
        self.prefix = prefix
        self._compression_codec = get_config(client)["pickling"]["modules_archive_compression_codec"]
        if sys.version_info[:2] == (2, 6):
            self._compression_codec = None

        suffix = ".tar"
        if self._compression_codec == "gzip":
            suffix += ".gz"
        self.filename = tempfiles_manager.create_tempfile(
            dir=get_local_temp_directory(client),
            prefix=prefix,
            suffix=suffix)
        self.size = 0
        self.python_eggs = []
        self.client = client

    def __enter__(self):
        if self._compression_codec == "gzip":
            compression_level = get_config(self.client)["pickling"]["modules_archive_compression_level"]
            self._gz_fileobj = gzip.GzipFile(self.filename, "w", compresslevel=compression_level, mtime=0)
            self.tar = tarfile.TarFile(self.filename, "w", self._gz_fileobj, tarinfo=TarInfo, dereference=True)
        else:
            self.tar = tarfile.TarFile(self.filename, "w", tarinfo=TarInfo, dereference=True)
        self.tar.__enter__()
        return self

    def append(self, filepath, relpath):
        if relpath.endswith(".egg"):
            self.python_eggs.append(relpath)

        self.tar.add(filepath, relpath)
        self.size += get_disk_size(filepath)

    def __exit__(self, type, value, traceback):
        if type is not None:
            logger.error("Failed to write tar file %s", self.filename)

        self.tar.__exit__(type, value, traceback)
        if self._compression_codec == "gzip":
            self._gz_fileobj.__exit__(type, value, traceback)

        if type is None:
            self.md5 = calc_md5_from_file(self.filename)


def load_function(func):
    if isinstance(func, str):
        func = eval(func)
    return func


def split_files_into_chunks(files, chunk_size):
    chunk = []
    size = 0
    for relpath, filepath in files:
        chunk.append((relpath, filepath))
        size += get_disk_size(filepath)
        if size >= chunk_size:
            yield chunk
            chunk = []
            size = 0
    if chunk:
        yield chunk


def create_modules_archive_default(tempfiles_manager, custom_python_used, client):
    for module_name in OPERATION_REQUIRED_MODULES:
        import_module(module_name)

    logging_level = logging.getLevelName(get_config(client)["pickling"]["find_module_file_error_logging_level"])

    files_to_compress = {}
    module_filter = load_function(get_config(client)["pickling"]["module_filter"])
    extra_modules = getattr(sys, "extra_modules", set())

    auto_collection_enabled = \
        get_config(client)["pickling"]["dynamic_libraries"]["enable_auto_collection"] \
        and sys.platform.startswith("linux")
    dynamic_libraries = set()

    system_module_patterns = tuple([
        re.compile(pattern)
        for pattern in get_config(client)["pickling"]["system_module_patterns"]
    ])

    def add_file_to_compress(file, module_name, name):
        relpath = module_relpath([module_name, name], file, client)
        if relpath is None:
            if logger.LOGGER.isEnabledFor(logging_level):
                logger.log(logging_level, "Cannot determine relative path of module " + str(module))
            return

        if relpath in files_to_compress:
            return

        if relpath.endswith(".so") and auto_collection_enabled:
            library_filter = get_config(client)["pickling"]["dynamic_libraries"]["library_filter"]
            for library in list_dynamic_library_dependencies(file):
                if library in dynamic_libraries:
                    continue
                if library_filter is not None and not library_filter(library):
                    continue
                library_relpath = os.path.join("_shared", os.path.basename(library))
                dynamic_libraries.add(library)
                files_to_compress[library_relpath] = library

        files_to_compress[relpath] = file

    def is_system_module(_module):
        def is_system_module_path(_path):
            return any(pattern.search(_path) for pattern in system_module_patterns)

        if getattr(_module, "__file__", None):
            # Regular module.
            return is_system_module_path(_module.__file__)
        elif hasattr(_module, "__path__") and hasattr(_module.__path__, "__iter__"):
            # Looks like a namespaced module.
            return any(is_system_module_path(path) for path in _module.__path__)
        else:
            # We didn't figure out what it is, so let's safely assume it is not a system package.
            return False

    for name, module in list(sys.modules.items()):
        if module_filter is not None and not module_filter(module):
            continue

        if get_config(client)["pickling"]["ignore_system_modules"] and is_system_module(module):
            continue

        # NB: python3 tests could not properly pickle pkg_resources package.
        if "pkg_resources" in str(module):
            continue

        if hasattr(module, "__file__"):
            if module.__file__ is None:
                continue

            if custom_python_used:
                # NB: Ignore frozen and compiled in binary modules.
                if module.__name__ in extra_modules or module.__name__ + ".__init__" in extra_modules:
                    continue
                if module.__file__ == "<frozen>":
                    continue

            file = find_file(module.__file__)
            if file is None or not os.path.isfile(file):
                if logger.LOGGER.isEnabledFor(logging_level):
                    logger.log(
                        logging_level,
                        "Cannot locate file of the module (__name__: %s, __file__: %s)",
                        module.__name__,
                        module.__file__)
                continue

            file = os.path.abspath(file)

            if get_config(client)["pickling"]["force_using_py_instead_of_pyc"] and file.endswith(".pyc"):
                file = file[:-1]

            add_file_to_compress(file, module.__name__, name)

            if get_config(client)["pickling"]["enable_modules_compatibility_filter"] and file.endswith(".pyc") and \
                    os.path.exists(file[:-1]):
                add_file_to_compress(file[:-1], module.__name__, name)
        else:
            # Module can be a package without __init__.py, for example,
            # if module is added from *.pth file or manually added in client code.
            # Such module is package if it has __path__ attribute. See st/YT-3337 for more details.
            if hasattr(module, "__path__") and \
                    get_config(client)["pickling"]["create_init_file_for_package_modules"]:
                init_file = tempfiles_manager.create_tempfile(
                    dir=get_local_temp_directory(client),
                    prefix="__init__.py")

                with open(init_file, "w") as f:
                    f.write("#")  # Should not be empty. Empty comment is ok.

                module_name_parts = module.__name__.split(".") + ["__init__.py"]
                destination_name = os.path.join(*module_name_parts)
                files_to_compress[destination_name] = init_file

    additional_files = get_value(get_config(client)["pickling"]["additional_files_to_archive"], [])
    additional_files = [(relpath, filepath) for filepath, relpath in additional_files]
    all_files = list(files_to_compress.items()) + additional_files

    files_sorted = sorted(all_files, key=lambda item: os.path.getmtime(item[1]))
    file_chunks = split_files_into_chunks(files_sorted, get_config(client)["pickling"]["modules_chunk_size"])

    archives = []
    for index, chunk in enumerate(file_chunks):
        with Tar(prefix="modules_{:02d}".format(index), tempfiles_manager=tempfiles_manager, client=client) as tar:
            for relpath, filepath in chunk:
                tar.append(filepath, relpath)
            archives.append(tar)

    mount_sandbox_in_tmpfs = get_config(client)["mount_sandbox_in_tmpfs"]
    if isinstance(mount_sandbox_in_tmpfs, bool):  # COMPAT
        enable_mount_sandbox_in_tmpfs = mount_sandbox_in_tmpfs
    else:
        enable_mount_sandbox_in_tmpfs = mount_sandbox_in_tmpfs["enable"]

    enable_tmpfs_archive = get_config(client)["pickling"]["enable_tmpfs_archive"] or \
        enable_mount_sandbox_in_tmpfs

    result = [{
        "filename": archive.filename,
        "tmpfs": enable_tmpfs_archive and not is_local_mode(client),
        "size": archive.size,
        "hash": archive.md5,
        "eggs": archive.python_eggs}
        for archive in archives]

    return result


def create_modules_archive(tempfiles_manager, custom_python_used, client):
    create_modules_archive_function = get_config(client)["pickling"]["create_modules_archive_function"]
    if create_modules_archive_function is not None:
        if inspect.isfunction(create_modules_archive_function):
            args_spec = get_arg_spec(create_modules_archive_function)
            args_count = len(args_spec.args)
        elif hasattr(create_modules_archive_function, "__call__"):
            args_spec = get_arg_spec(create_modules_archive_function.__call__)
            args_count = len(args_spec.args) - 1
        else:
            raise YtError("Cannot determine whether create_modules_archive_function callable or not")
        if args_count > 0:
            return create_modules_archive_function(tempfiles_manager)
        else:
            return create_modules_archive_function()

    return create_modules_archive_default(tempfiles_manager, custom_python_used, client)


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


def get_use_local_python_in_jobs(client):
    python_binary = get_config(client)["pickling"]["python_binary"]
    use_local_python_in_jobs = get_config(client)["pickling"]["use_local_python_in_jobs"]
    if use_local_python_in_jobs is not None and python_binary is not None:
        raise YtError("Options pickling/use_local_python_in_jobs and pickling/python_binary cannot be "
                      "specified simultaneously")

    if python_binary is None and use_local_python_in_jobs is None and is_arcadia_python():
        use_local_python_in_jobs = True

    return use_local_python_in_jobs


def build_caller_arguments(is_standalone_binary, use_local_python_in_jobs, file_argument_builder, environment, client):
    use_py_runner = None
    arguments = []

    if is_standalone_binary:
        use_py_runner = False
        executable = None
        if hasattr(sys, "frozen"):
            executable = sys.executable
        else:
            executable = sys.argv[0]
        arguments = [file_argument_builder(executable, caller=True)]
    else:
        use_py_runner = True

        python_binary = get_config(client)["pickling"]["python_binary"]
        if python_binary is not None:
            arguments = [python_binary]
        else:
            if use_local_python_in_jobs is not None and use_local_python_in_jobs:
                arguments = [file_argument_builder(sys.executable, caller=True)]
                if is_arcadia_python() and "yt.wrapper._py_runner" in getattr(sys, "extra_modules", []):
                    use_py_runner = False
            else:
                major_version = get_python_version()[0]
                arguments = ["python" + str(major_version)]

    if use_py_runner:
        arguments.append(file_argument_builder(os.path.join(LOCATION, "_py_runner.py")))
    else:
        environment["Y_PYTHON_ENTRY_POINT"] = "__yt_entry_point__"

    return arguments


def build_function_and_config_arguments(function, create_temp_file, file_argument_builder,
                                        is_local_mode, params: OperationParameters, client) -> typing.Tuple[typing.List[str], str]:
    function_filename = create_temp_file(prefix=get_function_name(function) + ".pickle")

    pickling_config = get_config(client)["pickling"]
    pickler_name = pickling_config["framework"]
    pickler = Pickler(pickler_name)
    if params.pickling_encryption_key is not None:
        encryption_key = pickler.enable_encryption(key=params.pickling_encryption_key)
    else:
        encryption_key = None
    dump_kwargs = {}
    if pickler_name == "dill":
        if pickling_config["load_additional_dill_types"]:
            pickler.load_types()
        if is_running_interactively():
            dump_kwargs["recurse"] = True
            dump_kwargs["byref"] = False

    dump_kwargs.update({item["key"]: item["value"] for item in pickling_config["pickler_kwargs"]})

    with open(function_filename, "wb") as fout:
        params.attributes = function.attributes if hasattr(function, "attributes") else {}
        params.python_version = get_python_version()
        params.is_local_mode = is_local_mode

        pickler.dump((function, params), fout, **dump_kwargs)

    config_filename = create_temp_file(prefix="config_dump")
    with open(config_filename, "wb") as fout:
        config_copy = copy.deepcopy(get_config(client))
        if "token" in config_copy:
            del config_copy["token"]
        config_pickler = Pickler(config.DEFAULT_PICKLING_FRAMEWORK)
        if encryption_key:
            config_pickler.enable_encryption(key=encryption_key)
        config_pickler.dump(config_copy, fout)

    return (list(map(file_argument_builder, [function_filename, config_filename])), encryption_key)


def build_modules_arguments(modules_info, create_temp_file, file_argument_builder, client):
    # COMPAT: previous version of create_modules_archive returns string.
    if isinstance(modules_info, (str, bytes)):
        modules_info = [{"filename": modules_info, "hash": calc_md5_from_file(modules_info), "tmpfs": False}]

    tmpfs_size = sum([info["size"] for info in modules_info if info["tmpfs"]])
    if tmpfs_size > 0:
        tmpfs_size = int(TMPFS_SIZE_ADDEND + TMPFS_SIZE_MULTIPLIER * tmpfs_size)

    for info in modules_info:
        info["filename"] = file_argument_builder({"filename": info["filename"], "hash": info["hash"]})

    modules_info = {"modules": modules_info,
                    "platform_version": get_platform_version(),
                    "python_version": platform.python_version(),
                    "ignore_yson_bindings": get_config(client)["pickling"]["ignore_yson_bindings_for_incompatible_platforms"],
                    "enable_modules_compatibility_filter": get_config(client)["pickling"]["enable_modules_compatibility_filter"]}

    modules_info_filename = create_temp_file(prefix="_modules_info")
    with open(modules_info_filename, "wb") as fout:
        # NB: python3.8 has changes DEFAULT_PROTOCTOL to 4.
        # We set protocol implicitly for client<->server compatibility.
        protocol = min(standard_pickle.HIGHEST_PROTOCOL, 3)
        standard_pickle.dump(modules_info, fout, protocol=protocol)

    return [file_argument_builder(modules_info_filename)], tmpfs_size


def build_main_file_arguments(function, create_temp_file, file_argument_builder):
    main_filename_local = create_temp_file(prefix="_main_module", suffix=".py")
    main_filename_remote = "modules/_main_module.py"
    main_module_type = "PY_SOURCE"
    module_import_path = "_main_module"
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
            main_module = sys.modules["__main__"]
            function_source_filename = main_module.__file__
            if main_module.__package__ is not None and main_module.__package__:
                module_import_path = "{0}.{1}".format(main_module.__package__, main_module.__name__)
            if function_source_filename.endswith("pyc"):
                main_module_type = "PY_COMPILED"

    if function_source_filename:
        shutil.copy(function_source_filename, main_filename_local)

    return [
        file_argument_builder(
            main_filename_local,
            attributes={"file_name": main_filename_remote},
        ),
        module_import_path,
        main_module_type,
    ]


def do_wrap(function, tempfiles_manager, local_mode, file_manager, params: OperationParameters, client) -> WrapResult:
    assert params.job_type in ["mapper", "reducer", "reduce_combiner", "vanilla"]

    def create_temp_file(prefix="", suffix=""):
        return tempfiles_manager.create_tempfile(
            dir=get_local_temp_directory(client),
            prefix=prefix,
            suffix=suffix)

    def file_argument_builder(file_params, caller=False, attributes=None):
        if isinstance(file_params, str):
            file_params = {"filename": file_params}
        else:
            file_params = copy.deepcopy(file_params)

        if enable_local_files_usage_in_job(client):
            return os.path.abspath(file_params["filename"])
        else:
            bypass_artifacts_cache = get_config(client)["pickling"]["modules_bypass_artifacts_cache"]
            if bypass_artifacts_cache is not None:
                if attributes is None:
                    attributes = {}
                attributes["bypass_artifacts_cache"] = bypass_artifacts_cache
            file_params["filename"] = LocalFile(file_params["filename"], attributes=attributes)
            file_manager.add_files(file_params)
            return ("./" if caller else "") + file_params["filename"].file_name

    is_standalone_binary = SINGLE_INDEPENDENT_BINARY_CASE or \
        (SINGLE_INDEPENDENT_BINARY_CASE is None and getattr(sys, "is_standalone_binary", False))

    use_local_python_in_jobs = get_use_local_python_in_jobs(client)

    # XXX(asaitgalin): Some flags are needed before operation (and config) is unpickled
    # so these flags are passed through environment variables.
    environment = {}
    environment["YT_FORBID_REQUESTS_FROM_JOB"] = "1"

    if get_config(client)["pickling"]["use_function_name_as_title"]:
        title = get_function_name(function)
    else:
        title = None

    caller_arguments = build_caller_arguments(is_standalone_binary, use_local_python_in_jobs,
                                              file_argument_builder, environment, client)
    function_and_config_files, function_and_config_files_key = build_function_and_config_arguments(
        function,
        create_temp_file,
        file_argument_builder,
        local_mode,
        params,
        client)

    if is_standalone_binary:
        tmpfs_size = 0
        modules_arguments = []
        main_file_arguments = []
    else:
        modules_info = create_modules_archive(
            tempfiles_manager,
            is_standalone_binary or use_local_python_in_jobs,
            client)
        modules_arguments, tmpfs_size = build_modules_arguments(
            modules_info,
            create_temp_file,
            file_argument_builder,
            client)
        main_file_arguments = build_main_file_arguments(
            function,
            create_temp_file,
            file_argument_builder)

    cmd = " ".join(caller_arguments + function_and_config_files + modules_arguments + main_file_arguments)
    return WrapResult(cmd=cmd, tmpfs_size=tmpfs_size, environment=environment, title=title, local_files_to_remove=None,
                      pickling_encryption_key=function_and_config_files_key)


def wrap(client, **kwargs) -> WrapResult:
    result = do_wrap(client=client, **kwargs)
    result.local_files_to_remove = []
    return result


def enable_python_job_processing_for_standalone_binary():
    """Enables alternative method to run python functions as jobs in YT operations.
    This method sends into the job only pickled function and various program settings
    and do not send modules that used by the program. Therefore this method works
    correctly only if your script is a standalone binary and executed as binary.

    This function used as entry point if yt library built in python/program from arcadia.
    """
    global SINGLE_INDEPENDENT_BINARY_CASE
    # Check whether we inside job or not.
    if os.environ.get("YT_FORBID_REQUESTS_FROM_JOB"):
        is_standalone_binary = SINGLE_INDEPENDENT_BINARY_CASE or \
            (SINGLE_INDEPENDENT_BINARY_CASE is None and getattr(sys, "is_standalone_binary", False))
        if is_standalone_binary:
            process_rows(sys.argv[1], sys.argv[2], start_time=None)
        else:
            run_py_runner()
        sys.exit(0)
    else:
        SINGLE_INDEPENDENT_BINARY_CASE = True


def initialize_python_job_processing():
    """Checks if program is build as standalone binary or arcadia python used.
    And call enable_python_job_processing_for_standalone_binary if it is the case.

    You should call this function in the beginning of the program.
    """
    if getattr(sys, "is_standalone_binary", False) or is_arcadia_python():
        enable_python_job_processing_for_standalone_binary()


def _get_callable_func(func):
    if inspect.isfunction(func):
        return func
    else:
        if not hasattr(func, "__call__"):
            raise TypeError(
                'Failed to apply yt decorator since object "{0}" is not a function '
                "or (instance of) class with method __call__"
                .format(repr(func)))
        return func.__call__


def _set_attribute(func, key, value):
    if not hasattr(func, "attributes"):
        func.attributes = {}
    func.attributes[key] = value
    return func


def aggregator(func):
    """Decorates function to consume *iterator of rows* instead of single row."""
    return _set_attribute(func, "is_aggregator", True)


def reduce_aggregator(func):
    """Decorates function to consume *iterator of pairs* where each pair consists \
       of key and records with this key."""
    return _set_attribute(func, "is_reduce_aggregator", True)


def raw(func):
    """Decorates function to consume *raw data stream* instead of single row."""
    return _set_attribute(func, "is_raw", True)


def raw_io(func):
    """Decorates function to run as is. No arguments are passed. Function handles IO."""
    return _set_attribute(func, "is_raw_io", True)


def with_context(func):
    """Decorates function to run with control attributes argument."""
    callable = _get_callable_func(func)
    if "context" not in get_arg_spec(callable).args:
        raise TypeError('Decorator "with_context" applied to function {0} that has no argument "context"'
                        .format(func.__name__))
    return _set_attribute(func, "with_context", True)


def with_skiff_schemas(func):
    """Mark python function as skiff-compatible."""
    callable = _get_callable_func(func)
    if "skiff_input_schemas" not in get_arg_spec(callable).args:
        raise TypeError(
            'Decorator "with_skiff_schemas" applied to function {0} that has no argument "skiff_input_schemas"'
            .format(func.__name__))
    if "skiff_output_schemas" not in get_arg_spec(callable).args:
        raise TypeError(
            'Decorator "with_skiff_schemas" applied to function {0} that has no argument "skiff_output_schemas"'
            .format(func.__name__))
    return _set_attribute(func, "with_skiff_schemas", True)


def with_formats(func, input_format=None, output_format=None):
    if input_format is not None:
        _set_attribute(func, "input_format", input_format)
    if output_format is not None:
        _set_attribute(func, "output_format", output_format)
    return func


class DockerRespawner:
    def __init__(
            self,
            image,
            target_platform,
            docker,
            python,
            cli_args=_INHERIT_SYS_ARGV,
            env=None,
            main_scipt_path=None,
            cwd=None,
            homedir=None,
            python_lib_paths=None,
            mount=None,
            need_sudo=False,
    ):
        self._image = image
        self._platform = target_platform
        self._docker = docker
        self._python = python
        self._env = env
        self._cli_args = cli_args
        if self._cli_args is _INHERIT_SYS_ARGV:
            self._cli_args = sys.argv[1:]
        if self._env is None:
            # By default, we forward only YT-specific variables.
            self._env = {
                key: value
                for key, value in os.environ.items()
                if key.startswith("YT_")
            }
        self._main_script_path = main_scipt_path if main_scipt_path is not None else os.path.abspath(sys.argv[0])
        self._cwd = cwd if cwd is not None else os.path.abspath(os.getcwd())
        self._homedir = homedir if homedir is not None else os.path.expanduser("~")
        # We need to provide all python libraries to a container to ensure pickling works.
        self._python_lib_paths = (
            python_lib_paths
            if python_lib_paths is not None else
            [os.path.abspath(path) for path in sys.path]
        )
        self._mount = mount
        self._need_sudo = need_sudo

    def _make_docker_env_args(self, env):
        return list(itertools.chain.from_iterable(
            (
                ("-e", "{0}={1}".format(key, value))
                for key, value in env.items()
            )
        ))

    def _make_docker_mount_args(self, paths):
        return list(itertools.chain.from_iterable(
            (
                ("-v", "{0}:{0}".format(path))
                for path in paths
            )
        ))

    def _make_mount_paths(self):
        if self._mount is not None:
            return self._mount
        # If we mount the entire local file system in docker under an arbitrary name
        # we will break the symlinks,
        # so we have to find only the paths necessary for work and mount them along the same paths.
        mount_paths = [
            path
            for path in [self._cwd, os.path.dirname(self._main_script_path), *self._python_lib_paths]
            if path is not None and os.path.commonpath([path, self._homedir]) != self._homedir
        ] + [self._homedir]
        mount_paths = list(set(mount_paths))
        mount_paths.sort()
        return mount_paths

    def _make_docker_platform(self):
        if self._platform is not None:
            return "--platform", self._platform
        return tuple()

    def _make_sudo(self):
        if self._need_sudo:
            return ("sudo",)
        return tuple()

    def make_command(self):
        env = self._env.copy()
        # Ensure using the same image in all operations.
        env["YT_BASE_LAYER"] = env.get("YT_BASE_LAYER", self._image)
        # ATTENTION
        # We use linux-specific separator for PYTHONPATH.
        pythonpath_env = ":".join(self._python_lib_paths)

        mount_paths = self._make_mount_paths()
        docker_env_args = self._make_docker_env_args(
            {
                "CWD": self._cwd,
                "PYTHONPATH": pythonpath_env,
                YT_RESPOWNED_IN_CONTAINER_KEY: "1",
                **env,
            }
        )
        docker_mount_args = self._make_docker_mount_args(mount_paths)
        command = [
            *self._make_sudo(),
            self._docker, "run",
            *self._make_docker_platform(),
            "-it", "--rm",
            *docker_env_args,
            *docker_mount_args,
            self._image,
            self._python, self._main_script_path, *self._cli_args,
        ]
        command = [
            shlex.quote(c)
            for c in command
        ]
        return command

    def run(self):
        command = self.make_command()
        logger.info("Respawn in docker (command: {0})".format(" ".join(command)))
        process = subprocess.Popen(
            command,
            stdin=sys.stdin,
            stdout=sys.stdout,
            stderr=sys.stderr,
        )
        exit_code = process.wait()
        if exit_code:
            sys.exit(exit_code)


def respawn_in_docker(
        image,
        target_platform=None,
        docker="docker",
        python="python3",
        cli_args=_INHERIT_SYS_ARGV,
        env=None,
        mount=None,
        need_sudo=False,
):
    """
    Decorator for function main to prevent problems with pickle.
    The wrapped function will be executed inside the Docker container in order to provide
    the most similar execution environment as on the YTSaurus cluster.
    All operation invocations like yt.run_map automatically assume the specified docker image.

    Args:
        :param str image: docker image (same format as in the operation's spec)
        :param Optional[str] target_platform: target platform for the docker container
        :param str docker: local docker executable name/path
        :param str python: python executable name/path in docker
        :param Union[_INHERIT_SYS_ARGV, list[str]] cli_args: command line args, sys.argv[1:] by default
        :param Optional[dict] env: environment variables to pass to the docker container
        If not set it will use global variables with the YT_ prefix
        :param Optional[list[str]] mount: mount points for the docker container
        If not set it will use homedir, cwd and python lib paths
        :param bool need_sudo: whether sudo privileges are needed to run docker
    Example:
        from yt import wrapper
        from .lib import mapper

        @respawn_in_docker("python3.10")
        def main():
            client = wrapper.YtClient()
            client.run_map(
                mapper,
                source_table="//tmp/foo",
                destination_table="//tmp/bar",
            )

        if __name__ == "__main__":
            main()
    """
    def inner(func):
        def wrapped(*args, **kwargs):
            if os.environ.get(YT_RESPOWNED_IN_CONTAINER_KEY) == "1" or is_inside_job():
                return func(*args, **kwargs)
            respawner = DockerRespawner(
                image=image,
                cli_args=cli_args,
                target_platform=target_platform,
                docker=docker,
                env=env,
                mount=mount,
                need_sudo=need_sudo,
                python=python,
            )
            respawner.run()
        return wrapped
    return inner
