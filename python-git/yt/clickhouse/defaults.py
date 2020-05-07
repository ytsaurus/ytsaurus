import yt.logger as logger

import inspect
from yt.packages.six import PY3, iteritems


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
        for key, default_value in iteritems(defaults_dict):
            if key in kwargs_names:
                current_value = kwargs.get(key)
                if current_value is None:
                    kwargs[key] = default_value
        logger.debug("Resulting arguments: %s", kwargs)
        return fn(*args, **kwargs)

    wrapped_fn.__doc__ = fn.__doc__

    return wrapped_fn
