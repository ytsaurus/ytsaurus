def main():
    # We should use local imports because of replacing __main__ module cause cleaning globals.
    import os
    import sys
    import itertools
    import imp
    import zipfile
    import pickle as standard_pickle


    # Variable names start with "__" to avoid accidental intersection with scope of user function.
    __operation_dump = sys.argv[1]
    __config_dump_filename = sys.argv[2]

    if len(sys.argv) > 3:
        __modules_info_filename = sys.argv[3]
        __main_filename = sys.argv[4]
        __main_module_name = sys.argv[5]
        __main_module_type = sys.argv[6]

        with open(__modules_info_filename) as fin:
            modules_info = standard_pickle.load(fin)

        for info in modules_info:
            destination = "modules"
            if info.get("tmpfs") and os.path.exists("tmpfs"):
                destination = "tmpfs/modules"
            # Unfortunately we cannot use fixed version of ZipFile.
            __zip = zipfile.ZipFile(info["filename"])
            __zip.extractall(destination)
            __zip.close()

        sys.path = ["./modules", "./tmpfs/modules"] + sys.path
        # Python with pre-3.3 version does not support namespace packages,
        # so we scan modules directory and manually create them in sys.modules.
        # See https://www.python.org/dev/peps/pep-0420/ and YT-3337 for more details.
        from _py_runner_helpers import _create_namespace_packages
        if os.path.exists("modules"):
            _create_namespace_packages("./modules")
        if os.path.exists("tmpfs"):
            _create_namespace_packages("./tmpfs/modules")

        sys.modules['__main__'] = imp.load_module(__main_module_name,
                                                  open(__main_filename, 'rU'),
                                                  __main_filename,
                                                  ('', 'rU', imp.__dict__[__main_module_type]))

        for name in dir(sys.modules['__main__']):
            globals()[name] = sys.modules['__main__'].__dict__[name]

    import _py_runner_helpers
    import yt.yson
    import yt.wrapper.config
    from yt.wrapper.format import YsonFormat, extract_key
    from yt.wrapper.pickling import Unpickler
    yt.wrapper.config.config = \
        Unpickler(yt.wrapper.config.DEFAULT_PICKLING_FRAMEWORK).load(open(__config_dump_filename))

    unpickler = Unpickler(yt.wrapper.config.config["pickling"]["framework"])

    __operation, __attributes, __operation_type, __input_format, __output_format, __keys, __python_version = \
        unpickler.load(open(__operation_dump))

    if yt.wrapper.config["pickling"]["check_python_version"] and yt.wrapper.common.get_python_version() != __python_version:
        sys.stderr.write("Python version on cluster differs from local python version")
        sys.exit(1)

    if __attributes.get("is_raw_io", False):
        __operation()
        return

    raw = __attributes.get("is_raw", False)

    if isinstance(__input_format, YsonFormat) and yt.yson.TYPE != "BINARY":
        sys.stderr.write("YSON bindings not found. To resolve the problem "
                         "try to use JsonFormat format or install yandex-yt-python-yson package.")
        sys.exit(1)

    __rows = __input_format.load_rows(sys.stdin, raw=raw)

    __start, __run, __finish = _py_runner_helpers._extract_operation_methods(__operation)
    wrap_stdin = wrap_stdout = yt.wrapper.config["pickling"]["safe_stream_mode"]
    with _py_runner_helpers.WrappedStreams(wrap_stdin, wrap_stdout) as streams:
        if __attributes.get("is_aggregator", False):
            __result = __run(__rows)
        else:
            if __operation_type == "mapper" or raw:
                __result = itertools.chain(
                    __start(),
                    itertools.chain.from_iterable(itertools.imap(__run, __rows)),
                    __finish())
            else:
                if __attributes.get("is_reduce_aggregator"):
                    __result = __run(itertools.groupby(__rows, lambda row: extract_key(row, __keys)))
                else:
                    __result = itertools.chain(
                        __start(),
                        itertools.chain.from_iterable(
                            itertools.starmap(__run,
                                itertools.groupby(__rows, lambda row: extract_key(row, __keys)))),
                        __finish())

        __output_format.dump_rows(__result, streams.get_original_stdout(), raw=raw)

    # Read out all input
    for row in __rows:
        pass

if __name__ == "__main__":
    main()

