from yt.common import YtError
from yt.wrapper.common import EMPTY_GENERATOR
import inspect
import sys
import types
import os

class YtStandardStreamAccessError(YtError):
    pass

class StreamWrapper(object):
    ALLOWED_ATTRIBUTES = set(["fileno", "isatty", "tell", "encoding", "name", "mode"])

    def __init__(self, original_stream):
        self.original_stream = original_stream

    def __getattr__(self, attr):
        if attr in self.ALLOWED_ATTRIBUTES:
            return self.original_stream.__getattribute__(attr)
        raise YtStandardStreamAccessError("Stdin, stdout are inaccessible for Python operations"
                                          " without raw_io attribute")

class WrappedStreams(object):
    def __init__(self, wrap_stdin=True, wrap_stdout=True):
        self.wrap_stdin = wrap_stdin
        self.wrap_stdout = wrap_stdout

    def __enter__(self):
        self.stdin = sys.stdin
        self.stdout = sys.stdout
        if self.wrap_stdin:
            sys.stdin = StreamWrapper(self.stdin)
        if self.wrap_stdout:
            sys.stdout = StreamWrapper(self.stdout)
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
       sys.stdin = self.stdin
       sys.stdout = self.stdout

    def get_original_stdin(self):
        return self.stdin

    def get_original_stdout(self):
        return self.stdout

def _convert_callable_to_generator(func):
    def generator(*args):
        result = func(*args)
        if isinstance(result, types.GeneratorType):
            return result
        elif result is not None:
            raise YtError('Non-yielding operation function should return generator or None.'
                          ' Did you mean "yield" instead of "return"?')
        return EMPTY_GENERATOR

    return generator

def _extract_operation_methods(operation):
    if hasattr(operation, "start") and inspect.ismethod(operation.start):
        start = _convert_callable_to_generator(operation.start)
    else:
        start = lambda: EMPTY_GENERATOR

    if hasattr(operation, "finish") and inspect.ismethod(operation.finish):
        finish = _convert_callable_to_generator(operation.finish)
    else:
        finish = lambda: EMPTY_GENERATOR

    return start, _convert_callable_to_generator(operation), finish

def _create_namespace_packages(search_path):
    def visit(root, package_name_parts):
        if package_name_parts:
            init_path = os.path.join(root, "__init__.py")
            package_module_name = ".".join(package_name_parts)

            if not os.path.isfile(init_path) and not os.path.isfile(init_path + "c") and \
                    package_module_name not in sys.modules:
                package_module = types.ModuleType(package_module_name)
                # XXX(asaitgalin): If the module is a package (either regular or namespace),
                # the module __path__ attribute must be set.
                package_module.__dict__["__path__"] = [root]
                sys.modules[package_module_name] = package_module

        for entry in os.listdir(root):
            entry_path = os.path.join(root, entry)
            if os.path.isdir(entry_path):
                visit(entry_path, package_name_parts + [entry])

    visit(os.path.abspath(search_path), [])
