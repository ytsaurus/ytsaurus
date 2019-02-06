try:
    import __res
except ImportError:
    pass  # Not Arcadia.
else:
    py_main = __res.find('PY_MAIN')  # None, "mod:func" or "mod".
    if py_main:
        import importlib
        mod_name = py_main.split(b':', 1)[0]
        importlib.import_module(mod_name.decode('UTF-8'))
    else:
        # Python 2 with __main__.py.
        __res.importer.load_module("__main__", fix_name="__yt_main__")

import yt.wrapper

yt.wrapper.initialize_python_job_processing()
