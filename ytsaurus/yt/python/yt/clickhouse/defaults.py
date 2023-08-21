import yt.logger as logger

try:
    from yt.packages.six import PY3, iteritems
except ImportError:
    from six import PY3, iteritems

from yt.common import update

import inspect

CYPRESS_DEFAULTS_PATH = "//sys/clickhouse/defaults"


def _get_kwargs_names(fn):
    if PY3:
        argspec = inspect.getfullargspec(fn)
    else:
        argspec = inspect.getargspec(fn)
    kwargs_len = len(argspec.defaults)
    kwargs_names = argspec.args[-kwargs_len:]
    return kwargs_names


def patch_defaults(fn):
    kwargs_names = _get_kwargs_names(fn)

    def wrapped_fn(*args, **kwargs):
        defaults_dict = kwargs.pop("defaults")
        logger.debug("Applying following argument defaults: %s", defaults_dict)
        recognized_defaults = {}
        for key, default_value in iteritems(defaults_dict):
            if key in kwargs_names:
                recognized_defaults[key] = default_value
        # We should remove entries like smth = None, as it will override our defaults.
        filtered_kwargs = {key: value for key, value in iteritems(kwargs) if value is not None}
        kwargs = update(recognized_defaults, filtered_kwargs)
        logger.debug("Resulting arguments: %s", kwargs)
        return fn(*args, **kwargs)

    wrapped_fn.__doc__ = fn.__doc__

    return wrapped_fn
