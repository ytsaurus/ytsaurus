import argparse
import inspect
import os
from copy import deepcopy


def _fix_indentation(text, width):
    indentation = " " * width
    lines = []
    for line in text.split("\n"):
        if all(map(str.isspace, line)):
            line = ""
        else:
            line = indentation + line.lstrip()
        lines.append(line.replace("\\", "\\\\"))
    return "\n".join(lines)


def join_args(args_tokens, indentation, line_limit=100):
    lines = []
    current_tokens = []
    current_len = len(indentation) + 1
    for token in args_tokens:
        current_tokens.append(token)
        current_len += 3 + len(token)
        if current_len > line_limit:
            lines.append(", ".join(current_tokens))
            current_len = len(indentation)
            current_tokens = []

    if current_tokens:
        lines.append(", ".join(current_tokens))

    return (",\n" + indentation).join(lines)


def main():
    os.environ["GENERATE_CLIENT"] = "YES"

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
        for name in sorted(client_api.all_names):
            func = getattr(client_api, name)
            doc = _fix_indentation(func.__doc__, 8)

            is_class = False
            if inspect.isclass(func):
                func = func.__dict__["__init__"]
                is_class = True

            arg_spec = inspect.getfullargspec(func)
            var_args = arg_spec.varargs
            var_kwargs = arg_spec.varkw
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

            tab = " " * 4

            if args and kwargs:
                fout.write('''
    def {name}(
            self,
            {args},
            {kwargs}):
        \"\"\"
{doc}
        \"\"\"
        return client_api.{name}(
            {args},
            client=self,
            {kwargs_passed})
'''.format(name=name, args=join_args(args, indentation=tab * 3), kwargs=join_args(kwargs, indentation=tab * 3), kwargs_passed=join_args(kwargs_passed, indentation=tab * 3), doc=doc))
            elif args:
                fout.write('''
    def {name}(
            self,
            {args}):
        \"\"\"
{doc}
        \"\"\"
        return client_api.{name}(
            {args},
            client=self)
'''.format(name=name, args=join_args(args, indentation=tab * 3), doc=doc))
            elif kwargs:
                fout.write('''
    def {name}(
            self,
            {kwargs}):
        \"\"\"
{doc}
        \"\"\"
        return client_api.{name}(
            client=self,
            {kwargs_passed})
'''.format(name=name, kwargs=join_args(kwargs, indentation=tab * 3), kwargs_passed=join_args(kwargs_passed, indentation=tab * 3), doc=doc))
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
