import sys

if "__main__" in sys.extra_modules:
    # Load hidden __main__ of programs without PY_MAIN.
    sys.meta_path[0].load_module("__main__", fix_name="__yt_main__")

import yt.wrapper

yt.wrapper.initialize_python_job_processing()
