from yt.common import (require, flatten, update, update_inplace, which, YtError, update_from_env,
                       unlist, get_value, filter_dict, date_string_to_timestamp, datetime_to_string,
                       uuid_to_parts, declare_deprecated, deprecated_with_message, deprecated)
import yt.yson as yson

from yt.packages.decorator import decorator
from yt.packages.six import iteritems, itervalues, PY3, Iterator
from yt.packages.six.moves import xrange, map as imap, filter as ifilter, zip as izip

try:
    from collections import Iterable
except ImportError:
    from collections.abc import Iterable

import argparse
import collections
import getpass
import os
import platform
import random
import socket
import sys
import threading
import types
try:
    from yt.packages.distro import linux_distribution
except ImportError:
    try:
        from platform import linux_distribution
    except ImportError:
        linux_distribution = None

from yt.packages import tqdm

from multiprocessing.pool import ThreadPool
from multiprocessing.dummy import (Process as DummyProcess,
                                   current_process as dummy_current_process)
from collections import Mapping
from itertools import chain, starmap
from functools import reduce
from copy import copy, deepcopy

EMPTY_GENERATOR = (i for i in [])

MB = 1024 * 1024
GB = 1024 * MB


class CustomTqdm(tqdm.tqdm):
    # Disable the monitor thread.
    monitor_interval = 0

    def __init__(self, *args, **kwargs):
        kwargs = update(dict(unit="b", unit_scale=True, ascii=True), kwargs)
        super(CustomTqdm, self).__init__(*args, **kwargs)

    @classmethod
    def format_meter(cls, n, total, *args, **kwargs):
        # NB: `super(cls)` does not support static methods, so we need `super(cls, cls)`.
        # https://stackoverflow.com/questions/26788214/super-and-staticmethod-interaction
        meter = super(cls, cls).format_meter(n, total, *args, **kwargs)
        if total:
            return meter
        else:
            # Quick way to remove colon from the progress bar
            ind = meter.find(" :")
            return meter[:ind] + meter[ind + 2:]

def compose(*args):
    def compose_two(f, g):
        return lambda x: f(g(x))
    return reduce(compose_two, args)

def parse_bool(word):
    """Converts "true" and "false" and something like this to Python bool

    Raises :class:`YtError <yt.common.YtError>` if input word is incorrect.
    """
    # Compatibility with api/v3
    if word is False or word is True or isinstance(word, yson.YsonBoolean):
        return word

    word = word.lower()
    if word == "true":
        return True
    elif word == "false":
        return False
    else:
        raise YtError("Cannot parse boolean from %s" % word)

def bool_to_string(bool_value):
    """Convert Python bool value to "true" or "false" string

    Raises :class:`YtError <yt.common.YtError>` if value is incorrect.
    """
    if bool_value in ["false", "true"]:
        return bool_value
    if bool_value not in [False, True]:
        raise YtError("Incorrect bool value '{0}'".format(bool_value))
    if bool_value:
        return "true"
    else:
        return "false"

def is_prefix(list_a, list_b):
    if len(list_a) > len(list_b):
        return False
    for i in xrange(len(list_a)):
        if list_a[i] != list_b[i]:
            return False
    return True

def prefix(iterable, n):
    counter = 0
    for value in iterable:
        if counter == n:
            break
        counter += 1
        yield value

def dict_depth(obj):
    if not isinstance(obj, dict):
        return 0
    else:
        return 1 + max(imap(dict_depth, itervalues(obj)))

def first_not_none(iter):
    return next(ifilter(None, iter))

def merge_dicts(*dicts):
    return dict(chain(*[iteritems(d) for d in dicts]))

def group_blobs_by_size(lines, chunk_size):
    """Unite lines into large chunks."""
    size = 0
    chunk = []
    for line in lines:
        size += len(line)
        chunk.append(line)
        if size >= chunk_size:
            yield chunk
            size = 0
            chunk = []

    if chunk:
        yield chunk

def chunk_iter_list(lines, chunk_size):
    size = 0
    chunk = []
    for line in lines:
        size += 1
        chunk.append(line)
        if size >= chunk_size:
            yield chunk
            size = 0
            chunk = []

    if chunk:
        yield chunk

def chunk_iter_stream(stream, chunk_size):
    while True:
        chunk = stream.read(chunk_size)
        if not chunk:
            break
        yield chunk

def chunk_iter_rows(stream, chunk_size):
    for blob in group_blobs_by_size(stream.read_rows(), chunk_size):
        yield b"".join(blob)

def chunk_iter_string(string, chunk_size):
    index = 0
    while True:
        lower_index = index * chunk_size
        upper_index = min((index + 1) * chunk_size, len(string))
        if lower_index >= len(string):
            return
        yield string[lower_index:upper_index]
        index += 1

def get_stream_size_or_none(stream):
    if PY3 and isinstance(stream, (bytes, str)) or not PY3 and isinstance(stream, basestring):
        return len(stream)
    return None

def total_seconds(td):
    return float(td.microseconds + (td.seconds + td.days * 24 * 3600) * 10 ** 6) / 10 ** 6

def generate_int64(generator=None):
    if generator is None:
        generator = random
    return generator.randint(-2**63, 2**63 - 1)

def generate_uuid(generator=None):
    if generator is None:
        generator = random
    def get_int():
        return hex(generator.randint(0, 2**32 - 1))[2:].rstrip("L")
    return "-".join([get_int() for _ in xrange(4)])

def get_version():
    """ Returns python wrapper version """
    try:
        from .version import VERSION
    except:
        VERSION = None

    # Add svn revision to version if it presented.
    try:
        import library.python.svn_version
        svn_revision = "r" + str(library.python.svn_version.svn_revision())
        if VERSION is None:
            VERSION = svn_revision
        else:
            VERSION = '{0} (r{1})'.format(VERSION, svn_revision)
    except ImportError:
        pass

    if VERSION is None:
        VERSION = "unknown"

    return VERSION

def get_python_version():
    return sys.version_info[:3]

def get_platform():
    if sys.platform in ("linux", "linux2"):
        if linux_distribution is not None:
            return "{0} {1} ({2})".format(*linux_distribution())
        else:
            return platform.uname().system
    elif sys.platform == "darwin":
        return "Mac OS " + platform.mac_ver()[0]
    elif sys.platform == "win32":
        return "Windows {0} {1}".format(*platform.win32_ver()[:2])
    else:
        return None

def get_user_info():
    try:
        return {"user": getpass.getuser()}
    except Exception:
        return {"user_id": os.getuid()}

def get_started_by_short():
    return update({"pid": os.getpid()}, get_user_info())

def get_started_by():
    python_version = "{0}.{1}.{2}".format(*get_python_version())

    started_by = {
        "hostname": socket.getfqdn(),
        "pid": os.getpid(),
        "command": sys.argv,
        "wrapper_version": get_version(),
        "python_version": python_version
    }

    started_by = update(started_by, get_user_info())

    platform = get_platform()
    if platform is not None:
        started_by["platform"] = platform

    return started_by

def is_inside_job():
    """Returns `True` if the code is currently being run in the context of a YT job."""
    if "YT_FORBID_REQUESTS_FROM_JOB" in os.environ:
        return bool(int(os.environ.get("YT_FORBID_REQUESTS_FROM_JOB", "0")))
    else:
        return "YT_JOB_ID" in os.environ


@decorator
def forbidden_inside_job(func, *args, **kwargs):
    flag = os.environ.get("YT_ALLOW_HTTP_REQUESTS_TO_YT_FROM_JOB", "0")
    if is_inside_job() and not bool(int(flag)):
        raise YtError('HTTP requests to YT are forbidden inside jobs by default. '
                      'Did you forget to surround code that runs YT operations with '
                      'if __name__ == "__main__"? If not and you need to make requests '
                      'you can override this behaviour by turning on '
                      '"allow_http_requests_to_yt_from_job" option in config.')
    return func(*args, **kwargs)

class DoNotReplaceAction(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        if not getattr(namespace, self.dest):
            setattr(namespace, self.dest, values)

def round_up_to(num, divider):
    if num % divider == 0:
        return num
    else:
        return (1 + (num // divider)) * divider

def get_disk_size(filepath, round=4*1024):
    stat = os.stat(filepath)
    if round:
        return round_up_to(stat.st_size, round)
    else:
        return stat.st_size

def get_binary_std_stream(stream):
    if PY3:
        return stream.buffer
    else:
        return stream

def get_disk_space_from_resources(resources):
    # Backwards compatibility.
    if "disk_space" in resources:
        return resources["disk_space"]
    else:
        return resources["disk_space_per_medium"].get("default", 0)

def set_param(params, name, value, transform=None):
    if value is not None:
        if transform is not None:
            params[name] = transform(value)
        else:
            params[name] = value
    return params

def remove_nones_from_dict(obj):
    result = deepcopy(obj)
    for key, value in iteritems(obj):
        if value is None:
            del result[key]
            continue
        if isinstance(value, Mapping):
            result[key] = remove_nones_from_dict(value)
    return result

def is_arcadia_python():
    try:
        import __res
        assert __res
        return True
    except ImportError:
        pass

    return hasattr(sys, "extra_modules")


HashPair = collections.namedtuple("HashPair", ["lo", "hi"])

def uuid_hash_pair(uuid):
    id_hi, id_lo = uuid_to_parts(uuid)
    return HashPair(yson.YsonUint64(id_lo), yson.YsonUint64(id_hi))

def object_type_from_uuid(uuid):
    i3, i2, i1, i0 = (int(s, 16) for s in uuid.split("-"))
    return i1 & 0xffff

def is_master_transaction(transaction_id):
    return object_type_from_uuid(transaction_id) in (1, 4)


if not PY3:
    class _DummyProcess(DummyProcess):
        def start(self):
            assert self._parent is dummy_current_process()
            self._start_called = True
            if hasattr(self._parent, "_children"):
                self._parent._children[self] = None
            threading.Thread.start(self)

    class ThreadPoolHelper(ThreadPool):
        # See: http://bugs.python.org/issue10015
        Process = _DummyProcess
else:
    class ThreadPoolHelper(ThreadPool):
        pass

def escape_c(string):
    """Escapes string literal to be used in query language."""
    def is_printable(symbol):
        num = ord(symbol)
        return 32 <= num and num <= 126
    def is_oct_digit(symbol):
        return "0" <= symbol and symbol <= "7"
    def is_hex_digit(symbol):
        return ("0" <= symbol and symbol <= "9") or \
            ("A" <= symbol and symbol <= "F") or \
            ("a" <= symbol and symbol <= "f")
    def oct_digit(num):
        return chr(ord("0") + num)
    def hex_digit(num):
        return chr(ord("0") + num) if num < 10 else chr(ord("A") + num - 10)

    def escape_symbol(symbol, next):
        if symbol == '"':
            return '\\"'
        elif symbol == '\\':
            return "\\\\"
        elif is_printable(symbol) and not (symbol == "?" and next == "?"):
            return symbol
        elif symbol == "\r":
            return "\\r"
        elif symbol == "\n":
            return "\\n"
        elif symbol == "\t":
            return "\\t"
        elif ord(symbol) < 8 and not is_oct_digit(next):
            return "\\" + oct_digit(ord(symbol))
        elif not is_hex_digit(next):
            num = ord(symbol)
            return "\\x" + hex_digit(num // 16) + hex_digit(num % 16)
        else:
            num = ord(symbol)
            return "\\" + oct_digit(num // 64) + oct_digit((num // 8) % 8) + oct_digit(num % 8)

    return "".join(starmap(escape_symbol, izip(string, string[1:] + chr(0))))

def simplify_structure(obj):
    """Replace all wrapper replacement objects (like :class:`YPath <yt.wrapper.ypath.YPath>`) in the given object
    with their YSON representations suitable for passing to the driver or to the HTTP request"""
    attributes = None
    is_yson_type = isinstance(obj, yson.YsonType)
    if is_yson_type and obj.has_attributes():
        attributes = simplify_structure(obj.attributes)

    if isinstance(obj, list):
        list_cls = yson.YsonList if is_yson_type else list
        obj = list_cls(imap(simplify_structure, obj))
    elif isinstance(obj, dict):
        dict_cls = yson.YsonMap if is_yson_type else dict
        obj = dict_cls((k, simplify_structure(v)) for k, v in iteritems(obj))
    elif hasattr(obj, "to_yson_type"):
        obj = obj.to_yson_type()
    else:
        obj = copy(obj)

    if attributes is not None:
        obj.attributes = attributes

    return obj

class NullContext(object):
    def __enter__(self):
        pass

    def __exit__(self, type, value, traceback):
        pass

def format_disk_space(num, suffix="B"):
    for unit in ("","Ki","Mi","Gi","Ti","Pi","Ei","Zi"):
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, "Yi", suffix)

def is_of_iterable_type(obj):
    iterable_types = (list, types.GeneratorType, Iterator, Iterable)
    return isinstance(obj, iterable_types)
