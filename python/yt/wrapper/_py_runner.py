def main():
    # We should use local imports because of replacing __main__ module cause cleaning globals
    import sys
    import itertools
    #import imp
    import shelve
    import zipfile

    # Variable names start with "__" to avoid accidental intersection with scope of user function
    __operation_dump = sys.argv[1]
    __modules_archive = sys.argv[2]
    #__main_filename = sys.argv[3]
    #__main_module_name = sys.argv[4]
    __config_dump_filename = sys.argv[5]

    # Unfortunately we cannot use fixes version of ZipFile
    __zip = zipfile.ZipFile(__modules_archive)
    __zip.extractall("modules")
    __zip.close()

    sys.path = ["./modules"] + sys.path

    # TODO(ignat): remove if it is unnesessary.
    # I don't understand now what the reason of this action.
    # ...
    # Magically replace out main module by client side main. It is get from mapreducelib.py
    #sys.modules['__main__'] = imp.load_module(__main_module_name, open(__main_filename, 'rU'), __main_filename, ('', 'rU', imp.PY_SOURCE))

    from yt.wrapper.pickling import load
    operation = load(open(__operation_dump))

    import yt.wrapper.config as config
    config_shelve = shelve.open(__config_dump_filename)
    try:
        for key, value in config_shelve.iteritems():
            config.__dict__[key] = value
    finally:
        config_shelve.close()

    sys.stdout.writelines(itertools.chain(*itertools.imap(operation, sys.stdin.xreadlines())))

if __name__ == "__main__":
    main()

