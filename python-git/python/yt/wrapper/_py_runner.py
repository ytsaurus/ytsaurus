def main():
    # We should use local imports because of replacing __main__ module cause cleaning globals
    import sys
    import itertools
    import imp
    import shelve
    import zipfile

    # Variable names start with "__" to avoid accidental intersection with scope of user function
    __operation_dump = sys.argv[1]
    __modules_archive = sys.argv[2]
    __main_filename = sys.argv[3]
    __main_module_name = sys.argv[4]
    __config_dump_filename = sys.argv[5]

    # Unfortunately we cannot use fixes version of ZipFile
    __zip = zipfile.ZipFile(__modules_archive)
    __zip.extractall("modules")
    __zip.close()

    sys.path = ["./modules"] + sys.path

    sys.modules['__main__'] = imp.load_module(__main_module_name, open(__main_filename, 'rU'), __main_filename, ('', 'rU', imp.PY_SOURCE))

    for name in dir(sys.modules['__main__']):
        globals()[name] = sys.modules['__main__'].__dict__[name]

    from yt.wrapper.pickling import load
    __operation, __attributes, __operation_type, __keys = load(open(__operation_dump))

    import yt.wrapper.config as config
    config_shelve = shelve.open(__config_dump_filename)
    try:
        for key, value in config_shelve.iteritems():
            config.__dict__[key] = value
    finally:
        config_shelve.close()

    import yt.wrapper as yt
    if __attributes.get("is_raw", False):
        __result = itertools.chain(*itertools.imap(__operation, sys.stdin.xreadlines()))
    else:
        __records = itertools.imap(yt.line_to_record, sys.stdin.xreadlines())
        if __operation_type == "mapper":
            if __attributes.get("is_aggregator", False):
                __result = __operation(__records)
            else:
                __result = itertools.chain(*itertools.imap(__operation, __records))
        else:
            __result = itertools.chain.from_iterable(itertools.starmap(__operation, itertools.groupby(__records, yt.extract_key)))
        __result = itertools.imap(yt.record_to_line, __result)
    sys.stdout.writelines(__result)

if __name__ == "__main__":
    main()

