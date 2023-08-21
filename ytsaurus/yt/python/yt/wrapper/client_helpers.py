from __future__ import print_function

from .common import update, get_arg_spec
from .default_config import get_default_config

try:
    from yt.packages.six import PY3
except ImportError:
    from six import PY3

from copy import deepcopy
import inspect

import sys


def create_class_method(func):
    original_func = func

    def decorator(self, *args, **kwargs):
        return original_func(*args, client=self, **kwargs)

    return create_class_method_impl(func, decorator)


def create_class_method_impl(func, decorator):
    is_class = False
    if inspect.isclass(func):
        func = func.__dict__["__init__"]
        is_class = True

    assert inspect.isfunction(func)

    arg_spec = get_arg_spec(func)

    arg_names = arg_spec.args
    defaults = arg_spec.defaults

    method_arg_names = deepcopy(arg_names)
    callee_arg_names = deepcopy(arg_names)

    if "client" in arg_names:
        client_index = arg_names.index("client")
        assert client_index == len(arg_names) - 1, \
            'By convention "client" argument should be last in function signature. ' \
            'Function "{0}" should be fixed.'.format(func.__name__)

        method_arg_names.remove("client")
        callee_arg_names.remove("client")

        defaults_shift = len(arg_names) - len(defaults)
        last_positional_argument_index = client_index - defaults_shift

        modified_defaults = list(defaults)
        assert modified_defaults[last_positional_argument_index] is None
        modified_defaults.pop(last_positional_argument_index)
        defaults = tuple(modified_defaults)
        if not defaults:
            defaults = None

    if is_class:
        callee_arg_names.pop(0)
    else:
        method_arg_names.insert(0, "self")

    if arg_spec.varargs:
        method_arg_names.append('*' + arg_spec.varargs)
        callee_arg_names.append('*' + arg_spec.varargs)

    if PY3:
        keywords = arg_spec.varkw
    else:
        keywords = arg_spec.keywords
    if keywords is not None:
        method_arg_names.append("**" + keywords)
        callee_arg_names.append("**" + keywords)

    method_signature = ", ".join(method_arg_names)
    callee_signature = ", ".join(callee_arg_names)

    evaldict = globals().copy()
    evaldict["decorator"] = decorator

    method_template = """
def {name}({method_signature}):
    return decorator(self, {callee_signature})
"""
    src = method_template.format(name=func.__name__, method_signature=method_signature, callee_signature=callee_signature)

    # Ensure each generated function has a unique filename for profilers
    # (such as cProfile) that depend on the tuple of (<filename>,
    # <definition line>, <function name>) being unique.
    filename = "<client-method-{}>".format(func.__name__)
    try:
        code = compile(src, filename, "single")
        exec(code, evaldict)
    except:  # noqa
        print("Error compiling code", file=sys.stderr)
        print(src, file=sys.stderr)
        raise

    wrapped_func = evaldict[func.__name__]
    wrapped_func.__name__ = func.__name__
    wrapped_func.__doc__ = func.__doc__
    wrapped_func.__dict__ = func.__dict__.copy()
    wrapped_func.__defaults__ = defaults
    wrapped_func.__annotations__ = getattr(func, "annotations", None)
    wrapped_func.__module__ = func.__module__
    wrapped_func.__dict__["__source__"] = src
    return wrapped_func


def are_signatures_equal(lhs, rhs):
    return get_arg_spec(lhs) == get_arg_spec(rhs)


def initialize_client(client, proxy, token, config):
    client.config = get_default_config()
    if config is not None:
        client.config = update(client.config, config)

    if proxy is not None:
        client.config["proxy"]["url"] = proxy
    if token is not None:
        client.config["token"] = token
