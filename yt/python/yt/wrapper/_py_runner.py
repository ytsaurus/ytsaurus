from __future__ import print_function


def lsb_release():
    """ Simplified implementation of distro.linux_distribution().

    We cannot use distro module since this function is used in _py_runner
    before loading user-side modules.
    """
    import os
    import sys
    import subprocess

    with open(os.devnull, "w") as devnull:
        try:
            cmd = ("lsb_release", "-a")
            stdout = subprocess.check_output(cmd, stderr=devnull)
        except OSError:  # Command not found.
            return tuple()

    lines = stdout.decode(sys.getfilesystemencoding()).splitlines()

    props = {}
    for line in lines:
        kv = line.strip("\n").split(":", 1)
        if len(kv) != 2:
            # Ignore lines without colon.
            continue
        k, v = kv
        props.update({k.replace(" ", "_").lower(): v.strip()})

    return (props["distributor_id"], props["release"], props["codename"])


def get_platform_version():
    import sys

    version = []
    name = sys.platform
    version.append(name)
    if name in ("linux", "linux2"):
        try:
            version.append(lsb_release())
        except Exception:
            # In some cases libc_ver detection can fail
            # because of inability to open sys.executable file.
            pass
    return tuple(version)


def filter_out_modules(module_path, filter_function):
    import os
    for path, dirnames, filenames in os.walk(module_path):
        for file_name in filenames:
            file_path = os.path.join(path, file_name)
            if filter_function(file_path):
                os.remove(file_path)


def main():
    # We should use local imports because of replacing __main__ module cause cleaning globals.
    import os
    import gzip
    import sys
    import time
    import tarfile
    import pickle as standard_pickle
    import platform

    # Exclude _py_runner.py directory from sys.path
    sys.path.pop(0)

    start_time = time.time()

    # Variable names start with "__" to avoid accidental intersection with scope of user function.
    __operation_dump_filename = sys.argv[1]
    __config_dump_filename = sys.argv[2]

    if len(sys.argv) > 3:
        __modules_info_filename = sys.argv[3]
        __main_filename = sys.argv[4]
        __main_module_name = sys.argv[5]

        with open(__modules_info_filename, "rb") as fin:
            modules_info = standard_pickle.load(fin)

        __python_eggs = []
        for info in modules_info["modules"]:
            destination = "modules"
            if info.get("tmpfs") and os.path.exists("tmpfs"):
                destination = "tmpfs/modules"

            # Python eggs which will be added to sys.path
            eggs = info.get("eggs")
            if eggs is not None:
                __python_eggs.extend([os.path.join(".", destination, egg) for egg in eggs])

            # Unfortunately we cannot use fixed version of TarFile.
            compression_codec = None
            if info["filename"].endswith(".gz"):
                compression_codec = "gzip"

            if compression_codec == "gzip":
                __gz_fileobj = gzip.GzipFile(info["filename"], "r")
                __tar = tarfile.TarFile(info["filename"], "r", __gz_fileobj)
            else:
                __tar = tarfile.TarFile(info["filename"], "r")
            __tar.extractall(destination)
            __tar.close()
            if compression_codec == "gzip":
                __gz_fileobj.close()

        module_locations = ["./modules", "./tmpfs/modules"]
        sys.path = __python_eggs + module_locations + sys.path

        client_version = modules_info["platform_version"]
        client_python_version = modules_info["python_version"]
        server_version = get_platform_version()
        server_python_version = platform.python_version()

        if modules_info["enable_modules_compatibility_filter"]:

            def python_filter_function(path):
                return path.endswith(".pyc") and os.path.exists(path[:-1]) or "hashlib" in path

            def platform_filter_function(path):
                return path.endswith(".so") and not path.endswith("yson_lib.so") or python_filter_function(path)

            if client_version != server_version or client_python_version != server_python_version:
                for module_path in module_locations:
                    if client_version != server_version:
                        filter_out_modules(module_path, platform_filter_function)
                    else:
                        filter_out_modules(module_path, python_filter_function)

        if client_version != server_version and modules_info["ignore_yson_bindings"]:
            from shutil import rmtree
            for location in module_locations:
                yson_bindings_path = os.path.join(location, "yt_yson_bindings")
                if os.path.exists(yson_bindings_path):
                    rmtree(yson_bindings_path)

        # Should be imported as early as possible to check python interpreter version.
        import yt.wrapper.version_check

        if "." in __main_module_name:
            __main_module_package = __main_module_name.rsplit(".", 1)[0]
            __import__(__main_module_package)
        import importlib.util
        spec = importlib.util.spec_from_file_location(__main_module_name, __main_filename)
        main_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(main_module)

        main_module_dict = globals()
        if "__main__" in sys.modules:
            main_module_dict = sys.modules["__main__"].__dict__
        for name in dir(main_module):
            main_module_dict[name] = main_module.__dict__[name]

    import yt.wrapper

    yt.wrapper.py_runner_helpers.process_rows(__operation_dump_filename, __config_dump_filename, start_time=start_time)


if __name__ == "__main__":
    main()
