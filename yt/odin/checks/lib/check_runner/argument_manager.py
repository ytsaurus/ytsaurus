from yt_odin.logserver import FULLY_AVAILABLE_STATE, PARTIALLY_AVAILABLE_STATE, UNAVAILABLE_STATE

import yt.wrapper as yt

from six import PY2, text_type, binary_type

import inspect


class Namespace(object):
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class CheckArgumentManager(object):
    def __init__(self, check_logger, stdin_args):
        states = {
            "FULLY_AVAILABLE_STATE": FULLY_AVAILABLE_STATE,
            "UNAVAILABLE_STATE": UNAVAILABLE_STATE,
            "PARTIALLY_AVAILABLE_STATE": PARTIALLY_AVAILABLE_STATE,
        }
        self._kwargs = dict(
            options=stdin_args["options"],
            yt_client=yt.YtClient(**stdin_args["yt_client_params"]),
            logger=check_logger,
            states=Namespace(**states),
            secrets=stdin_args.get("secrets"),
        )

    def make_arguments(self, check_function):
        if PY2:
            spec = inspect.getargspec(check_function)
        else:
            spec = inspect.getfullargspec(check_function)
        if spec.varargs:
            raise TypeError("Check function should not have '*{}' argument".format(spec.varargs))
        if getattr(spec, "keywords" if PY2 else "varkw"):
            raise TypeError("Check function should not have '**{}' argument".format(getattr(spec, "keywords" if PY2 else "varkw")))
        if spec.defaults:
            count = len(spec.defaults)
            raise TypeError("Check function should not have default argument values: {}"
                            .format(", ".join("{}={}".format(key, value)
                                              for key, value in zip(spec.args[-count:], spec.defaults))))
        arguments = {}
        for arg in spec.args:
            if not isinstance(arg, (text_type, binary_type)):
                raise TypeError("Check function should not have nested arguments: {}".format(arg))
            if arg not in self._kwargs:
                raise TypeError("Unknown check argument: '{}'".format(arg))
            if self._kwargs[arg] is None:
                raise ValueError("No value for argument: '{}'".format(arg))
            arguments[arg] = self._kwargs[arg]
        return arguments
