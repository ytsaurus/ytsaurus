try:
    import __res
except ImportError:
    pass  # Not Arcadia.
else:
    py_main = __res.find("PY_MAIN")  # None, "mod:func" or "mod".
    if py_main:
        import importlib
        module_name = py_main.split(b":", 1)[0]
        module = importlib.import_module(module_name.decode("utf-8"))
        if b":" not in py_main:
            # __main__.py compatibility mode.
            import sys
            sys.modules["__main__"] = module
    else:
        # Python 2 with __main__.py.
        __res.importer.load_module("__main__", fix_name="__yt_main__")

import yt.wrapper

yt.wrapper.initialize_python_job_processing()
