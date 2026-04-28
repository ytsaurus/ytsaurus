import yt.logger as logger

import functools
import os

EXPERIMENTAL_WARNING = (
    "This functionality is experimental and the interface may change. "
    "To suppress this warning, set: export YT_SUPPRESS_ADMIN_EXPERIMENTAL_WARNING=1"
)

EXPERIMENTAL_HELP_SUFFIX = "This functionality is experimental and the interface may change."


def warn_experimental(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        val = os.environ.get("YT_SUPPRESS_ADMIN_EXPERIMENTAL_WARNING", "")
        if val.lower() not in ("1", "true", "yes"):
            logger.warning(EXPERIMENTAL_WARNING)
        return func(*args, **kwargs)
    return wrapper
