import yt.logger as logger

from yt.common import update

import inspect

CYPRESS_DEFAULTS_PATH = "//sys/clickhouse/defaults"


def _get_kwargs_names(fn):
    argspec = inspect.getfullargspec(fn)
    kwargs_len = len(argspec.defaults)
    kwargs_names = argspec.args[-kwargs_len:]
    return kwargs_names


def patch_defaults(fn):
    kwargs_names = _get_kwargs_names(fn)

    def wrapped_fn(*args, **kwargs):
        defaults_dict = kwargs.pop("defaults")
        logger.debug("Applying following argument defaults: %s", defaults_dict)
        recognized_defaults = {}
        for key, default_value in defaults_dict.items():
            if key in kwargs_names:
                recognized_defaults[key] = default_value
        # We should remove entries like smth = None, as it will override our defaults.
        filtered_kwargs = {key: value for key, value in kwargs.items() if value is not None}
        kwargs = update(recognized_defaults, filtered_kwargs)
        logger.debug("Resulting arguments: %s", kwargs)
        return fn(*args, **kwargs)

    wrapped_fn.__doc__ = fn.__doc__

    return wrapped_fn
