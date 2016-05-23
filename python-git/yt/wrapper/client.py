from common import update
from default_config import get_default_config
from client_state import ClientState
import client_api

from yt.packages.decorator import FunctionMaker

import inspect

def fix_argspec(argspec, is_init_method):
    if not is_init_method:
        argspec.args.insert(0, "self")

    if "client" in argspec.args:
        index = argspec.args.index("client")

        defaults_shift = len(argspec.args) - len(argspec.defaults)
        defaults_index = index - defaults_shift

        argspec.args.pop(index)

        modified_defaults = list(argspec.defaults)
        assert modified_defaults[defaults_index] is None
        modified_defaults.pop(defaults_index)
        argspec.defaults = tuple(modified_defaults)

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
        arg_names.pop(arg_names.index("client"))
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

class YtClient(ClientState):
    def __init__(self, proxy=None, token=None, config=None):
        ClientState.__init__(self)

        self.config = get_default_config()
        if config is not None:
            self.config = update(self.config, config)

        if proxy is not None:
            self.config["proxy"]["url"] = proxy
        if token is not None:
            self.config["token"] = token


for name in client_api.all_names:
    setattr(YtClient, name, create_class_method(getattr(client_api, name)))

# Backward compatibility.
Yt = YtClient

