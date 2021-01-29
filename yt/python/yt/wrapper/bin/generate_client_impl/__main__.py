from __future__ import print_function

from yt.packages.six import PY3

import os
import argparse
import inspect
from copy import deepcopy


def _fix_indentation(text, width):
    indentation = " " * width
    lines = []
    for line in text.split("\n"):
        if all(map(str.isspace, line)):
            line = ""
        else:
            line = indentation + line.lstrip()
        lines.append(line)
    return "\n".join(lines)


def main():
    os.environ["YT_OLD_STYLE_CLIENT"] = "1"

    from yt.wrapper.cypress_commands import _KwargSentinelClass, _MapOrderSorted
    from yt.wrapper import client_api

    def dump_value(value):
        if isinstance(value, str):
            return repr(value)
        elif isinstance(value, _KwargSentinelClass):
            return "_KwargSentinelClass()"
        elif isinstance(value, _MapOrderSorted):
            return "_MapOrderSorted()"
        else:
            return str(value)

    parser = argparse.ArgumentParser(description="Generate client_impl")
    parser.add_argument("output")
    cli_args = parser.parse_args()

    with open(cli_args.output, "w") as fout:
        fout.write('''# This file is auto-generate by yt/python/yt/wrapper/bin/generate_client_impl, please do not edit it manually!

from .cypress_commands import _KwargSentinelClass, _MapOrderSorted
from .client_helpers import initialize_client
from .client_state import ClientState
from . import client_api

class YtClient(ClientState):
    """Implements YT client."""

    def __init__(self, proxy=None, token=None, config=None):
        super(YtClient, self).__init__()
        initialize_client(self, proxy, token, config)
''')
        for name in client_api.all_names:
            func = getattr(client_api, name)
            doc = _fix_indentation(func.__doc__, 8)

            is_class = False
            if inspect.isclass(func):
                func = func.__dict__["__init__"]
                is_class = True

            if PY3:
                arg_spec = inspect.getfullargspec(func)
                var_args = arg_spec.varargs
                var_kwargs = arg_spec.varkw
                defaults = arg_spec.defaults
            else:
                arg_spec = inspect.getargspec(func)
                var_args = arg_spec.varargs
                var_kwargs = arg_spec.keywords
                defaults = arg_spec.defaults

            if defaults:
                defaults = list(defaults)
            else:
                defaults = []

            all_args = deepcopy(arg_spec.args)
            if all_args and all_args[-1] == "client":
                all_args.pop()
                if defaults:
                    defaults.pop()

            if defaults:
                args = all_args[:-len(defaults)]
                kwargs_names = all_args[-len(defaults):]
            else:
                args = all_args
                kwargs_names = []

            if is_class:
                args.pop(0)

            if var_args:
                args.append("*" + var_args)

            kwargs = ["{}={}".format(key, dump_value(value)) for key, value in zip(kwargs_names, defaults)]
            kwargs_passed = ["{}={}".format(key, key) for key in kwargs_names]
            if var_kwargs:
                kwargs.append("**" + var_kwargs)
                kwargs_passed.append("**" + var_kwargs)

            if args and kwargs:
                fout.write('''
    def {name}(self, {args}, {kwargs}):
        \"\"\"
{doc}
        \"\"\"
        return client_api.{name}({args}, client=self, {kwargs_passed})
'''.format(name=name, args=", ".join(args), kwargs=", ".join(kwargs), kwargs_passed=", ".join(kwargs_passed), doc=doc))
            elif args:
                fout.write('''
    def {name}(self, {args}):
        \"\"\"
{doc}
        \"\"\"
        return client_api.{name}({args}, client=self)
'''.format(name=name, args=", ".join(args), doc=doc))
            elif kwargs:
                fout.write('''
    def {name}(self, {kwargs}):
        \"\"\"
{doc}
        \"\"\"
        return client_api.{name}(client=self, {kwargs_passed})
'''.format(name=name, kwargs=", ".join(kwargs), kwargs_passed=", ".join(kwargs_passed), doc=doc))
            else:
                fout.write('''
    def {name}(self):
        \"\"\"
{doc}
        \"\"\"
        return client_api.{name}(client=self)
'''.format(name=name, doc=doc))


if __name__ == "__main__":
    main()
