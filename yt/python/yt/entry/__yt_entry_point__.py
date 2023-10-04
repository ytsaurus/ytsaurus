try:
    import __res
except ImportError:
    pass  # Not Arcadia.
else:
    py_main = __res.find("PY_MAIN")  # None, "some_mod:some_func" or "some_mod" or ":main".
    if py_main == b":main":  # PY_MAIN=:main (python repl) - arcadia standalone python interpreter
        pass
    elif py_main is not None and py_main != b":main":  # PY_MAIN=some_mod:some_func or some_mod - arcadia single binary file
        import importlib
        module_name = py_main.split(b":", 1)[0]
        module = importlib.import_module(module_name.decode("utf-8"))
        if b":" not in py_main:
            # __main__.py compatibility mode.
            import sys
            sys.modules["__main__"] = module
    elif py_main is None:  # Python 2 with __main__.py.
        __res.importer.load_module("__main__", fix_name="__yt_main__")
    else:
        raise RuntimeError("Unknown PY_MAIN ({})".format(py_main))

import yt.wrapper

yt.wrapper.initialize_python_job_processing()
