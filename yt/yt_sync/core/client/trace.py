from typing import Callable
from typing import Iterable


def do_trace(log_func: Callable, prefix: str, func_name: str, args: Iterable, kwargs: dict):
    args = ", ".join(repr(x) for x in args)
    kwargs = ", ".join("%s=%r" % x for x in kwargs.items())
    log_func("%s:%s(%s)", prefix, func_name, ", ".join(k for k in (args, kwargs) if k))
