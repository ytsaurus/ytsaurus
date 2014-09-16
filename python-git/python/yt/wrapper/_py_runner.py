def main():
    # We should use local imports because of replacing __main__ module cause cleaning globals
    import sys
    import itertools
    import imp
    import zipfile

    # Variable names start with "__" to avoid accidental intersection with scope of user function
    __operation_dump = sys.argv[1]
    __modules_archive = sys.argv[2]
    __main_filename = sys.argv[3]
    __main_module_name = sys.argv[4]
    __main_module_type = sys.argv[5]
    __config_dump_filename = sys.argv[6]

    # Unfortunately we cannot use fixes version of ZipFile
    __zip = zipfile.ZipFile(__modules_archive)
    __zip.extractall("modules")
    __zip.close()


    sys.path = ["./modules"] + sys.path

    sys.modules['__main__'] = imp.load_module(__main_module_name,
                                              open(__main_filename, 'rU'),
                                              __main_filename,
                                              ('', 'rU', imp.__dict__[__main_module_type]))

    for name in dir(sys.modules['__main__']):
        globals()[name] = sys.modules['__main__'].__dict__[name]

    from yt.wrapper.pickling import load
    __operation, __attributes, __operation_type, __input_format, __output_format, __keys = load(open(__operation_dump))

    import yt.wrapper.format_config as format_config
    config_dict = load(open(__config_dump_filename))
    for key, value in config_dict.iteritems():
        format_config.__dict__[key] = value

    from yt.wrapper.record import extract_key

    is_raw = __attributes.get("is_raw", False)

    if is_raw:
        __records = sys.stdin
    else:
        __records = __input_format.load_rows(sys.stdin)

    if __operation_type == "mapper" or is_raw:
        if __attributes.get("is_aggregator", False):
            __result = __operation(__records)
        else:
            __result = itertools.chain.from_iterable(itertools.imap(__operation, __records))
    else:
        __result = \
            itertools.chain.from_iterable(
                itertools.starmap(__operation,
                    itertools.groupby(__records, lambda rec: extract_key(rec, __keys))))

    if is_raw:
        for line in __result:
            sys.stdout.write(line)
    else:
        __output_format.dump_rows(__result, sys.stdout)

if __name__ == "__main__":
    main()

