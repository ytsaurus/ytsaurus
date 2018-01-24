from __future__ import print_function

AVAILABLE = False
try:
    from yt_yson_bindings import load_skiff as load, dump_skiff as dump, SkiffRecord, SkiffSchema, SkiffTableSwitch
    AVAILABLE = True
except ImportError as error:
    message = str(error)
    if "No module named" not in message:
        import sys as _sys
        print("Warning! Failed to import skiff bindings: " + message, file=_sys.stderr)
