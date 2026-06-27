"""Entry point for the YT gdb tooling.

gdb has no "source a whole directory" command, so this package bootstraps
itself: source this file once

    (gdb) source <arcadia>/yt/yt/scripts/gdb_plugin/lib/__init__.py

and it auto-discovers and imports every sibling *.py module. Each module
self-registers its gdb commands / pretty-printers at import time, so dropping a
new file into this directory needs no edit here.
"""

import os
import sys
import importlib

# Don't litter the source tree with .pyc files.
sys.dont_write_bytecode = True

_HERE = os.path.dirname(os.path.abspath(__file__))
# Put this dir on sys.path so siblings import by bare name (and can import
# each other the same way).
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)


def _load_all():
    loaded, failed = [], []
    for fname in sorted(os.listdir(_HERE)):
        if not fname.endswith(".py"):
            continue
        if fname == "__init__.py" or fname.startswith("_"):
            continue
        mod = fname[:-3]
        try:
            importlib.import_module(mod)
            loaded.append(mod)
        except Exception as e:  # one broken module must not sink the rest
            failed.append((mod, e))
    if loaded:
        print("[yt-gdb] loaded: %s" % ", ".join(loaded))
    for mod, e in failed:
        print("[yt-gdb] FAILED to load %s: %s" % (mod, e))


_load_all()
