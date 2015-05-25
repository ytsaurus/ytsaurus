from yt.common import YtError
import sys

class YtStandardStreamAccessError(YtError):
    pass

class StreamWrapper(object):
    ALLOWED_ATTRIBUTES = set("fileno,isatty,tell,encoding,name,mode".split(","))

    def __init__(self, original_stream):
        self.original_stream = original_stream

    def __getattr__(self, attr):
        if attr in self.ALLOWED_ATTRIBUTES:
            return self.original_stream.__getattribute__(attr)
        raise YtStandardStreamAccessError("Stdin, stdout are inaccessible for not raw io operation")

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

