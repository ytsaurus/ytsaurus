from yt.common import (require, flatten, update, update_inplace, which, YtError, update_from_env, unlist,  # noqa
                       get_value, filter_dict, date_string_to_timestamp, datetime_to_string, date_string_to_datetime,
                       uuid_to_parts, declare_deprecated, deprecated_with_message, deprecated, underscore_case_to_camel_case)
import yt.yson as yson
import yt.json_wrapper as json

try:
    from yt.packages.six import iteritems, itervalues, PY3, Iterator, text_type, binary_type, string_types
    from yt.packages.six.moves import xrange, map as imap, filter as ifilter, zip as izip
except ImportError:
    from six import iteritems, itervalues, PY3, Iterator, text_type, binary_type, string_types
    from six.moves import xrange, map as imap, filter as ifilter, zip as izip

try:
    from collections.abc import Iterable
except ImportError:
    from collections import Iterable
try:
    from yt.packages.decorator import decorator
except ImportError:
    from decorator import decorator

try:
    import typing  # noqa
except ImportError:
    import yt.packages.typing as typing  # noqa

import argparse
import collections
import getpass
import inspect
import os
import platform
import random
import re
import socket
import sys
import threading
import types

try:
    import yt.packages.distro as distro
except ImportError:
    try:
        import distro
    except ImportError:
        distro = None
        try:
            from platform import linux_distribution
        except ImportError:
            linux_distribution = None

from multiprocessing.pool import ThreadPool
from multiprocessing.dummy import (Process as DummyProcess,
                                   current_process as dummy_current_process)
# Python3 compatibility
try:
    from collections.abc import Mapping
except ImportError:
    from collections import Mapping
from itertools import chain, starmap
from functools import reduce
from copy import copy, deepcopy
from datetime import timedelta

EMPTY_GENERATOR = (i for i in [])

KB = 1024
MB = 1024 * KB
GB = 1024 * MB


def hide_fields(object, fields, prefixes, prefix_re=None, hidden_value="hidden"):
    if isinstance(object, dict):
        for key in fields:
            if key in object:
                object[key] = hidden_value
        for key, value in iteritems(object):
            if prefixes and isinstance(value, text_type if PY3 else binary_type) and any(value.startswith(prefix) for prefix in prefixes):
                object[key] = hidden_value
            elif prefix_re and isinstance(value, text_type if PY3 else binary_type) and prefix_re.search(value):
                object[key] = prefix_re.sub(hidden_value, value)
            else:
                hide_fields(value, fields, prefixes, prefix_re, hidden_value)
    elif isinstance(object, list):
        for index in xrange(len(object)):
            value = object[index]
            if prefixes and isinstance(value, text_type if PY3 else binary_type) and any(value.startswith(prefix) for prefix in prefixes):
                object[index] = hidden_value
            elif prefix_re and isinstance(value, text_type if PY3 else binary_type) and prefix_re.search(value):
                object[index] = prefix_re.sub(hidden_value, value)
            else:
                hide_fields(value, fields, prefixes, prefix_re, hidden_value)


def hide_secure_vault(params):
    params = deepcopy(params)
    hide_fields(params, fields=("secure_vault",), prefixes=("AQAD-", "y1_AQAD-"))
    return params


def hide_arguments(args):
    args = deepcopy(args)
    hide_fields(args, fields=(), prefixes=(), prefix_re=re.compile(r"\b(\w+_)?AQAD-\S+"))
    return args


def hide_auth_headers(headers):
    headers = deepcopy(headers)
    if "Authorization" in headers:
        headers["Authorization"] = "x" * 32
    if "X-Ya-Service-Ticket" in headers:
        headers["X-Ya-Service-Ticket"] = "x" * 32

    return headers


def hide_auth_headers_in_request_info(request_info):
    copied_request_info = None
    for key in ("headers", "request_header", "response_headers"):
        if key not in request_info:
            continue
        if copied_request_info is None:
            copied_request_info = deepcopy(request_info)
        copied_request_info[key] = hide_auth_headers(request_info[key])
    return copied_request_info if copied_request_info is not None else request_info


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


def merge_blobs_by_size(blobs, chunk_size):
    """Unite blobs into larger chunks."""
    chunks = []
    total_size = 0
    closed = False
    try:
        for blob in blobs:
            chunks.append(blob)
            total_size += len(blob)
            if total_size >= chunk_size:
                yield b"".join(chunks)
                chunks = []
                total_size = 0
    except GeneratorExit:
        closed = True
        return
    finally:
        if not closed and len(chunks) > 0:
            yield b"".join(chunks)
            chunks = []
            total_size = 0


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
    for blob in merge_blobs_by_size(stream._read_rows(), chunk_size):
        yield blob


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
    if (
        PY3 and isinstance(stream, (bytes, str))
        or not PY3 and isinstance(stream, string_types)
    ):
        return len(stream)
    return None


def total_seconds(td):
    return float(td.microseconds + (td.seconds + td.days * 24 * 3600) * 10 ** 6) / 10 ** 6


def time_option_to_milliseconds(td):
    if isinstance(td, timedelta):
        return total_seconds(td) * 1000.0
    elif td is None:
        return 0
    else:
        return td


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


def generate_traceparent(generator=None):
    if generator is None:
        generator = random

    def get_int():
        return generator.randint(0, 2**64 - 1)

    return "00-%016x%016x-%016x-01" % (get_int(), get_int(), get_int())


def get_home_dir():
    home_dir = None
    try:
        home_dir = os.path.expanduser("~")
    except KeyError:
        pass
    return home_dir


def get_version():
    """ Returns python wrapper version """
    try:
        from .version import VERSION
    except ImportError:
        VERSION = None

    # Add svn revision or git commit to version if any of them is present.
    try:
        import library.python.svn_version
        svn_revision = library.python.svn_version.svn_revision()
        git_commit = library.python.svn_version.commit_id()
        suffix = None

        if svn_revision != -1:
            suffix = "r" + str(svn_revision)
        elif git_commit != "":
            suffix = "git:" + git_commit[:10]

        if suffix is not None:
            if VERSION is None:
                VERSION = suffix
            else:
                VERSION = '{0} ({1})'.format(VERSION, suffix)
    except (ImportError, AttributeError):
        pass

    if VERSION is None:
        VERSION = "unknown"

    return VERSION


def get_python_version():
    return sys.version_info[:3]


def get_platform():
    if sys.platform in ("linux", "linux2"):
        if distro is not None:
            return "{0} {1} ({2})".format(distro.name(), distro.version(), distro.codename())
        elif linux_distribution is not None:
            return "{0} {1} ({2})".format(*linux_distribution())
        else:
            return platform.uname().system
    elif sys.platform == "darwin":
        return "Mac OS " + platform.mac_ver()[0]
    elif sys.platform == "win32":
        return "Windows {0} {1}".format(*platform.win32_ver()[:2])
    else:
        return None


def get_user_agent():
    user_agent = "Python wrapper " + get_version()
    if "_ARGCOMPLETE" in os.environ:
        user_agent += " [argcomplete mode]"
    return user_agent


def get_user_info():
    try:
        return {"user": getpass.getuser()}
    except Exception:
        return {"user_id": os.getuid()}


def get_started_by_short():
    return update({"pid": os.getpid()}, get_user_info())


def _maybe_truncate(list_of_strings, length_limit):
    if length_limit is None or sum(map(len, list_of_strings)) <= length_limit:
        return list_of_strings
    truncated = []
    total_length = 0
    for s in list_of_strings:
        total_length += len(s)
        if total_length > length_limit:
            overflow = total_length - length_limit
            truncated.append(s[:len(s) - overflow] + "...truncated")
            break
        truncated.append(s)
    return truncated


def try_get_nirvana_block_url_from_context():
    home_dir = get_home_dir()
    if home_dir == "/slot/sandbox/j" and os.getenv("NV_YT_OPERATION_ID"):
        nirvana_job_context_path = os.path.join(home_dir, "job_context.json")
        if os.path.exists(nirvana_job_context_path):
            nirvana_context = {}
            try:
                with open(nirvana_job_context_path) as ctx_file:
                    nirvana_context = json.loads(ctx_file)
            except Exception:
                pass
            if "meta" in nirvana_context:
                meta = nirvana_context["meta"]
                if "blockURL" in meta:
                    return yson.convert.to_yson_type(meta["blockURL"], attributes={"_type_tag": "url"})
    return None


def get_started_by(command_length_limit=None):
    python_version = "{0}.{1}.{2}".format(*get_python_version())

    command = hide_arguments(sys.argv)
    command = _maybe_truncate(list(map(str, command)), command_length_limit)

    started_by = {
        "hostname": socket.getfqdn(),
        "pid": os.getpid(),
        "command": command,
        "wrapper_version": get_version(),
        "python_version": python_version
    }

    if is_arcadia_python():
        import __res
        started_by["arcadia_main_module"] = __res.find('PY_MAIN')

    started_by = update(started_by, get_user_info())

    platform = get_platform()
    if platform is not None:
        started_by["platform"] = platform

    nirvana_block_url = try_get_nirvana_block_url_from_context()
    if nirvana_block_url:
        started_by["nirvana_block_url"] = nirvana_block_url

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
                      'if __name__ == "__main__"? If not, and you indeed need to make '
                      'requests, obtain an explicit permission on doing that at '
                      'yt-admin@.')

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


def get_disk_size(filepath, round=4 * 1024):
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
    """Escapes string/bytes literal to be used in query language."""
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

    if isinstance(string, bytes) and not hasattr(string, 'encode') or isinstance(string, bytearray):
        return "".join("\\x" + hex_digit(bt // 16) + hex_digit(bt % 16) for bt in string)
    else:
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
    elif isinstance(obj, yson.YsonType) and obj.has_attributes():
        obj.attributes = simplify_structure(obj.attributes)

    return obj


class NullContext(object):
    def __enter__(self):
        pass

    def __exit__(self, type, value, traceback):
        pass


def format_disk_space(num, suffix="B"):
    for unit in ("", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"):
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, "Yi", suffix)


def is_of_iterable_type(obj):
    iterable_types = (list, types.GeneratorType, Iterator, Iterable)
    return isinstance(obj, iterable_types)


def get_arg_spec(func):
    if PY3:
        return inspect.getfullargspec(func)
    else:
        return inspect.getargspec(func)
