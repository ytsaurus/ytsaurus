from .common import update
from .default_config import get_default_config

from yt.packages.decorator import FunctionMaker
from yt.packages.six import PY3

from copy import deepcopy
import inspect

def fix_argspec(argspec, is_init_method):
    args = deepcopy(argspec.args)
    defaults = deepcopy(argspec.defaults)

    if not is_init_method:
        args.insert(0, "self")

    if "client" in args:
        index = args.index("client")

        defaults_shift = len(args) - len(defaults)
        defaults_index = index - defaults_shift

        args.pop(index)

        modified_defaults = list(defaults)
        assert modified_defaults[defaults_index] is None
        modified_defaults.pop(defaults_index)
        defaults = tuple(modified_defaults)

    # NOTE: In Python 3 argspec is namedtuple and forbids mutations so new instance is created.
    if PY3:
        new_argspec_args = {}
        for name in argspec._fields:
            if name in ["args", "defaults"]:
                continue
            new_argspec_args[name] = getattr(argspec, name)
        new_argspec_args["args"] = args
        new_argspec_args["defaults"] = defaults
        return inspect.FullArgSpec(**new_argspec_args)

    argspec.args = args
    argspec.defaults = defaults
    return argspec

def create_class_method(func):
    original_func = func
    def decorator(self, *args, **kwargs):
        return original_func(*args, client=self, **kwargs)

    is_class = False
    if inspect.isclass(func):
        func = func.__dict__["__init__"]
        is_class = True

    arg_spec = inspect.getargspec(func)
    arg_names = arg_spec.args

    if "client" in arg_names:
        client_index = arg_names.index("client")
        assert client_index == len(arg_names) - 1, \
            'By convention "client" argument should be last in function signature. ' \
            'Function "{0}" should be fixed.'.format(func.__name__)
        arg_names.pop(client_index)

    if is_class:
        arg_names.pop(0)
    evaldict = globals().copy()
    evaldict["decorator"] = decorator

    if arg_spec.varargs is not None:
        arg_names.append("*" + arg_spec.varargs)
    if arg_spec.keywords is not None:
        arg_names.append("**" + arg_spec.keywords)

    return FunctionMaker.create(
        func,
        "return decorator(self, {0})".format(", ".join(arg_names)),
        evaldict,
        undecorated=func,
        __wrapped__=func,
        argspec_transformer=lambda args: fix_argspec(args, is_class))

def initialize_client(client, proxy, token, config):
    client.config = get_default_config()
    if config is not None:
        client.config = update(client.config, config)

    if proxy is not None:
        client.config["proxy"]["url"] = proxy
    if token is not None:
        client.config["token"] = token
