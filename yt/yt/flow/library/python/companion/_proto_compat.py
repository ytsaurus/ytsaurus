"""
Setup sys.modules aliases so that proto modules generated with PROTO_NAMESPACE(yt)
can be imported via 'yt.flow...' even though the actual Python
module path is 'yt.yt.flow...'.

Call ensure_proto_imports() before any proto import.
"""

import importlib
import sys

_ALIAS_PREFIX = "yt.flow.library.cpp"
_REAL_PREFIX = "yt.yt.flow.library.cpp"

# Subpackages that need aliasing (companion/proto, common/proto).
_SUBPATHS = [
    "",
    ".companion",
    ".companion.proto",
    ".common",
    ".common.proto",
]

# Also alias intermediate packages above library.cpp.
_INTERMEDIATE = [
    ("yt.flow", "yt.yt.flow"),
    ("yt.flow.library", "yt.yt.flow.library"),
]

_initialized = False


def ensure_proto_imports():
    """Ensure yt.flow.* aliases are registered in sys.modules."""
    global _initialized
    if _initialized:
        return
    _initialized = True

    for alias, real in _INTERMEDIATE:
        if alias not in sys.modules:
            try:
                importlib.import_module(real)
                sys.modules[alias] = sys.modules[real]
            except ImportError:
                pass

    for sub in _SUBPATHS:
        alias = _ALIAS_PREFIX + sub
        real = _REAL_PREFIX + sub
        if alias not in sys.modules:
            try:
                importlib.import_module(real)
                sys.modules[alias] = sys.modules[real]
            except ImportError:
                pass
