def main():
    # We should use local imports because of replacing __main__ module cause cleaning globals.
    import os
    import sys
    import imp
    import time
    import zipfile
    import pickle as standard_pickle

    start_time = time.time()

    # Variable names start with "__" to avoid accidental intersection with scope of user function.
    __operation_dump_filename = sys.argv[1]
    __config_dump_filename = sys.argv[2]

    if len(sys.argv) > 3:
        __modules_info_filename = sys.argv[3]
        __main_filename = sys.argv[4]
        __main_module_name = sys.argv[5]
        __main_module_type = sys.argv[6]

        with open(__modules_info_filename, "rb") as fin:
            modules_info = standard_pickle.load(fin)

        __python_eggs = []
        for info in modules_info:
            destination = "modules"
            if info.get("tmpfs") and os.path.exists("tmpfs"):
                destination = "tmpfs/modules"

            # Python eggs which will be added to sys.path
            eggs = info.get("eggs")
            if eggs is not None:
                __python_eggs.extend([os.path.join(".", destination, egg) for egg in eggs])

            # Unfortunately we cannot use fixed version of ZipFile.
            __zip = zipfile.ZipFile(info["filename"])
            __zip.extractall(destination)
            __zip.close()

        sys.path = __python_eggs + ["./modules", "./tmpfs/modules"] + sys.path

        main_module = imp.load_module(__main_module_name,
                                      open(__main_filename, 'rb'),
                                      __main_filename,
                                      ('', 'rb', imp.__dict__[__main_module_type]))

        for name in dir(main_module):
            globals()[name] = main_module.__dict__[name]


    import yt.wrapper
    yt.wrapper.py_runner_helpers.process_rows(__operation_dump_filename, __config_dump_filename, start_time=start_time)

if __name__ == "__main__":
    main()

