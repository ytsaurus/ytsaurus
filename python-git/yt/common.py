from yt.packages.six import iteritems, PY3, text_type, binary_type, string_types
from yt.packages.six.moves import map as imap
import yt.json_wrapper as json

# Fix for thread unsafety of datetime module.
# See http://bugs.python.org/issue7980 for more details.
import _strptime

# Python3 compatibility
try:
    from collections.abc import Mapping
except ImportError:
    from collections import Mapping
from datetime import datetime
from itertools import chain
from functools import wraps

import calendar
import copy
import ctypes
import errno
import functools
import os
import re
import signal
import socket
import sys
import time
import types
import string
import warnings

# Standard YT time representation

YT_DATETIME_FORMAT_STRING = "%Y-%m-%dT%H:%M:%S.%fZ"

YT_NULL_TRANSACTION_ID = "0-0-0-0"

# Deprecation stuff.

class YtDeprecationWarning(DeprecationWarning):
    """Custom warnings category, because built-in category is ignored by default."""

warnings.simplefilter("default", category=YtDeprecationWarning)

DEFAULT_DEPRECATION_MESSAGE = "{0} is deprecated and will be removed in the next major release, " \
                              "use {1} instead"

ERROR_TEXT_MATCHING_DEPRECATION_MESSAGE = "Matching errors by their messages using string patterns is highly " \
                                          "discouraged. It is recommended to use contains_code(code) method instead. " \
                                          "If there is no suitable error code for your needs, ask yt@ for creating one."

def declare_deprecated(functional_name, alternative_name, condition=None, message=None):
    if condition or condition is None:
        message = get_value(message, DEFAULT_DEPRECATION_MESSAGE.format(functional_name, alternative_name))
        warnings.warn(message, YtDeprecationWarning)

def deprecated_with_message(message):
    def function_decorator(func):
        @wraps(func)
        def deprecated_function(*args, **kwargs):
            warnings.warn(message, YtDeprecationWarning)
            return func(*args, **kwargs)
        return deprecated_function
    return function_decorator

def deprecated(alternative):
    def function_decorator(func):
        warn_message = DEFAULT_DEPRECATION_MESSAGE.format(func.__name__, alternative)
        return deprecated_with_message(warn_message)(func)
    return function_decorator

class YtError(Exception):
    """Base class for all YT errors."""
    def __init__(self, message="", code=1, inner_errors=None, attributes=None):
        self.message = message
        self.code = code
        self.inner_errors = inner_errors if inner_errors is not None else []
        self.attributes = attributes if attributes else {}
        if "host" not in self.attributes:
            self.attributes["host"] = self._get_fqdn()
        if "datetime" not in self.attributes:
            self.attributes["datetime"] = datetime_to_string(datetime.utcnow())

    def simplify(self):
        """Transforms error (with inner errors) to standard python dict."""
        result = {"message": self.message, "code": self.code}
        if self.attributes:
            result["attributes"] = self.attributes
        if self.inner_errors:
            result["inner_errors"] = []
            for error in self.inner_errors:
                result["inner_errors"].append(
                    error.simplify() if isinstance(error, YtError) else
                    error)
        return result

    @classmethod
    def from_dict(cls, dict_):
        """Restores YtError instance from standard python dict. Reverses simplify()."""
        inner_errors = [cls.from_dict(inner) for inner in dict_.get("inner_errors", [])]
        return cls(message=dict_["message"], code=dict_["code"], attributes=dict_.get("attributes"),
                   inner_errors=inner_errors)

    def find_matching_error(self, code=None, predicate=None):
        """
        Find a suberror contained in the error (possibly the error itself) which is either:
        - having error code equal to `code';
        - or satisfying custom predicate `predicate'.

        Exactly one condition should be specified.

        Returns either first error matching the condition or None if no matching found.
        """

        if sum(argument is not None for argument in (code, predicate)) != 1:
            raise ValueError("Exactly one condition should be specified")

        if code is not None:
            predicate = lambda error: int(error.code) == code

        def find_recursive(error):
            # error may be Python dict; if so, transform it to YtError.
            if not isinstance(error, YtError):
                error = YtError(**error)

            if predicate(error):
                return error
            for inner_error in error.inner_errors:
                inner_result = find_recursive(inner_error)
                if inner_result:
                    return inner_result
            return None

        return find_recursive(self)

    def contains_code(self, code):
        """Check if error or one of its inner errors contains specified error code."""
        return self.find_matching_error(code=code) is not None

    def _contains_text(self, text):
        """Inner method, do not call explicitly."""
        return self.find_matching_error(predicate=lambda error: text in error.message) is not None

    @deprecated_with_message(ERROR_TEXT_MATCHING_DEPRECATION_MESSAGE)
    def contains_text(self, text):
        """
        Check if error or one of its inner errors contains specified substring in message.

        It is not recommended to use this helper; consider using contains_code instead.
        If the error you are seeking is not distinguishable by code, please send a message to yt@
        and we will fix that.
        """

        return self._contains_text(text)

    def _matches_regexp(self, pattern):
        """Inner method, do not call explicitly."""
        return self.find_matching_error(predicate=lambda error: re.match(pattern, error.message) is not None) is not None

    @deprecated_with_message(ERROR_TEXT_MATCHING_DEPRECATION_MESSAGE)
    def matches_regexp(self, pattern):
        """
        Check if error message or one of its inner error messages matches given regexp.

        It is not recommended to use this helper; consider using contains_code instead.
        If the error you are seeking is not distinguishable by code, please send a message to yt@
        and we will fix that.
        """

        return self._matches_regexp(pattern)

    def __str__(self):
        return format_error(self)

    def __repr__(self):
        return "%s(%s)" % (
            self.__class__.__name__,
            _pretty_format_messages_flat(self))

    @staticmethod
    def _get_fqdn():
        if not hasattr(YtError, "_cached_fqdn"):
            YtError._cached_fqdn = socket.getfqdn()
        return YtError._cached_fqdn

class YtResponseError(YtError):
    """Represents an error in YT response."""
    def __init__(self, error):
        super(YtResponseError, self).__init__()
        self.error = error
        self.inner_errors = [self.error]

    def is_resolve_error(self):
        """Resolution error."""
        return self.contains_code(500)

    def is_access_denied(self):
        """Access denied."""
        return self.contains_code(901)

    def is_concurrent_transaction_lock_conflict(self):
        """Deprecated! Transaction lock conflict."""
        return self.contains_code(402)

    def is_cypress_transaction_lock_conflict(self):
        """Transaction lock conflict."""
        return self.contains_code(402)

    def is_tablet_transaction_lock_conflict(self):
        """Transaction lock conflict."""
        return self.contains_code(1700)

    @deprecated(alternative='use is_request_queue_size_limit_exceeded')
    def is_request_rate_limit_exceeded(self):
        """Request rate limit exceeded."""
        return self.contains_code(904)

    def is_request_queue_size_limit_exceeded(self):
        """Request rate limit exceeded."""
        return self.contains_code(108) or self.contains_code(904)

    def is_rpc_unavailable(self):
        """Rpc unavailable."""
        return self.contains_code(105)

    def is_master_communication_error(self):
        """Chunk unavailable."""
        return self.contains_code(712)

    def is_chunk_unavailable(self):
        """Chunk unavailable."""
        return self.contains_code(716)

    def is_request_timed_out(self):
        """Request timed out."""
        return self.contains_code(3)

    def is_concurrent_operations_limit_reached(self):
        """Too many concurrent operations."""
        return self.contains_code(202)

    def is_no_such_transaction(self):
        """No such transaction."""
        return self.contains_code(11000)

    def is_no_such_job(self):
        """No such job."""
        return self.contains_code(203)

    def is_shell_exited(self):
        """Shell exited."""
        return self.contains_code(1800) or self.contains_code(1801)

    def is_no_such_service(self):
        """No such service."""
        return self.contains_code(102)

    def is_tablet_in_intermediate_state(self):
        """Tablet is in intermediate state."""
        # TODO(ifsmirnov) migrate to error code, YT-10993
        return self._matches_regexp("Tablet .* is in state .*")

    def is_no_such_tablet(self):
        """No such tablet."""
        return self.contains_code(1701)

    def is_tablet_not_mounted(self):
        """Tablet is not mounted."""
        return self.contains_code(1702)

    def is_all_target_nodes_failed(self):
        """Failed to write chunk since all target nodes have failed."""
        return self.contains_code(700)

    def is_no_such_attribute(self, attributes_list=None):
        """Operation attribute is not supported."""
        if attributes_list is None:
            pred_new = lambda err: err.code == 1920
        else:
            pred_new = lambda err: (err.attributes.get("attribute_name") in attributes_list) and (err.code == 1920)
        pred_old = lambda err: ("Attribute" in err.message) and ("is not allowed" in err.message)
        # COMPAT: remove old version
        return self.find_matching_error(predicate=pred_new) or self.find_matching_error(predicate=pred_old)

class PrettyPrintableDict(dict):
    pass


def _pretty_format_escape(value):
    def escape(char):
        if char in string.printable:
            return char
        return "\\x{0:02x}".format(ord(char))
    value = value.replace("\n", "\\n").replace("\t", "\\t")
    return "".join(imap(escape, value))


def _pretty_format_attribute(name, value, attribute_length_limit):
    name = to_native_str(name)
    if isinstance(value, PrettyPrintableDict):
        value = json.dumps(value, indent=2)
        value = value.replace("\n", "\n" + " " * (15 + 1 + 4))
    else:
        if isinstance(value, string_types):
            value = to_native_str(value)
        else:
            value = str(value)
        value = _pretty_format_escape(value)
        if attribute_length_limit is not None and len(value) > attribute_length_limit:
            value = value[:attribute_length_limit] + "...message truncated..."
    return " " * 4 + "%-15s %s" % (name, value)


def _pretty_simplify_error(error):
    if isinstance(error, YtError):
        error = error.simplify()
    elif isinstance(error, (Exception, KeyboardInterrupt)):
        error = {"code": 1, "message": str(error)}
    return error


def _pretty_extract_messages(error, depth=0):
    """
    YtError -> [(depth: int, message: str), ...], in tree order.
    """
    error = _pretty_simplify_error(error)

    if not error.get("attributes", {}).get("transparent", False):
        yield (depth, to_native_str(error["message"]))
        depth += 1

    for inner_error in error.get("inner_errors", []):
        for subitem in _pretty_extract_messages(inner_error, depth=depth):
            yield subitem


def _pretty_format_messages_flat(error):
    prev_depth = 0
    result = []
    for depth, message in _pretty_extract_messages(error):
        if depth > prev_depth:
            result.append(" ")
            result.append("(" * (depth - prev_depth))
        elif prev_depth > depth:
            result.append(")" * (prev_depth - depth))
        elif result:
            result.append(", ")
        result.append(repr(message))
        prev_depth = depth

    result.append(")" * prev_depth)
    return "".join(result)


def _pretty_format_messages(error, indent=0, indent_step=4):
    result = []
    for depth, message in _pretty_extract_messages(error):
        result.append("{indent}{message}".format(
            indent=" " * (indent + depth * indent_step),
            message=message))

    return "\n".join(result)


def _pretty_format_full_errors(error, attribute_length_limit):
    error = _pretty_simplify_error(error)

    lines = []
    if "message" in error:
        lines.append(to_native_str(error["message"]))

    if "code" in error and int(error["code"]) != 1:
        lines.append(_pretty_format_attribute(
            "code", error["code"], attribute_length_limit=attribute_length_limit))

    attributes = error.get("attributes", {})

    origin_keys = ["host", "datetime"]
    origin_cpp_keys = ["pid", "tid", "fid"]
    if all(key in attributes for key in origin_keys):
        date = attributes["datetime"]
        if isinstance(date, datetime):
            date = date.strftime("%y-%m-%dT%H:%M:%S.%fZ")
        value = "{0} on {1}".format(attributes["host"], date)
        if all(key in attributes for key in origin_cpp_keys):
            value += " (pid %(pid)d, tid %(tid)x, fid %(fid)x)" % attributes
        lines.append(_pretty_format_attribute(
            "origin", value, attribute_length_limit=attribute_length_limit))

    location_keys = ["file", "line"]
    if all(key in attributes for key in location_keys):
        lines.append(_pretty_format_attribute(
            "location",
            "%(file)s:%(line)d" % attributes,
            attribute_length_limit=attribute_length_limit))

    for key, value in iteritems(attributes):
        if key in origin_keys or key in location_keys or key in origin_cpp_keys:
            continue
        lines.append(_pretty_format_attribute(
            key, value, attribute_length_limit=attribute_length_limit))

    result = (" " * 4 + "\n").join(lines)
    if "inner_errors" in error:
        for inner_error in error["inner_errors"]:
            result += "\n" + _pretty_format_full_errors(
                inner_error, attribute_length_limit=attribute_length_limit)

    return result


def _pretty_format(error, attribute_length_limit=None):

    return "{}\n\n***** Details:\n{}\n".format(
        _pretty_format_messages(error),
        _pretty_format_full_errors(error, attribute_length_limit=attribute_length_limit))


def format_error(error, attribute_length_limit=300):
    return _pretty_format(error, attribute_length_limit)


def which(name, flags=os.X_OK):
    """Return list of files in system paths with given name."""
    # TODO: check behavior when dealing with symlinks
    result = []
    for dir in os.environ.get("PATH", "").split(os.pathsep):
        path = os.path.join(dir, name)
        if os.access(path, flags):
            result.append(path)
    return result

def unlist(l):
    try:
        return l[0] if len(l) == 1 else l
    except TypeError: # cannot calculate len
        return l

def require(condition, exception_func):
    if not condition:
        raise exception_func()

def update_inplace(object, patch):
    if isinstance(patch, Mapping) and isinstance(object, Mapping):
        for key, value in iteritems(patch):
            if key in object:
                object[key] = update_inplace(object[key], value)
            else:
                object[key] = value
    elif isinstance(patch, list) and isinstance(object, list):
        for index, value in enumerate(patch):
            if index < len(object):
                object[index] = update_inplace(object[index], value)
            else:
                object.append(value)
    else:
        object = patch
    return object

def update(object, patch):
    if patch is None:
        return copy.deepcopy(object)
    elif object is None:
        return copy.deepcopy(patch)
    else:
        return update_inplace(copy.deepcopy(object), patch)

def flatten(obj, list_types=(list, tuple, set, frozenset, types.GeneratorType)):
    """Create flat list from all elements."""
    if isinstance(obj, list_types):
        return list(chain(*imap(flatten, obj)))
    return [obj]

def update_from_env(variables):
    """Update variables dict from environment."""
    for key, value in iteritems(os.environ):
        prefix = "YT_"
        if not key.startswith(prefix):
            continue

        key = key[len(prefix):]
        if key not in variables:
            continue

        var_type = type(variables[key])
        # Using int we treat "0" as false, "1" as "true"
        if var_type == bool:
            try:
                value = int(value)
            except:
                pass
        # None type is treated as str
        if isinstance(None, var_type):
            var_type = str

        variables[key] = var_type(value)

def get_value(value, default):
    if value is None:
        return default
    else:
        return value

def filter_dict(predicate, dictionary):
    return dict([(k, v) for (k, v) in iteritems(dictionary) if predicate(k, v)])

def set_pdeathsig():
    if sys.platform.startswith("linux"):
        ctypes.cdll.LoadLibrary("libc.so.6")
        libc = ctypes.CDLL("libc.so.6")
        PR_SET_PDEATHSIG = 1
        libc.prctl(PR_SET_PDEATHSIG, signal.SIGTERM)

def remove_file(path, force=False):
    try:
        os.remove(path)
    except OSError:
        if not force:
            raise

def makedirp(path):
    try:
        os.makedirs(path)
    except OSError as err:
        if err.errno != errno.EEXIST:
            raise

def touch(path):
    if not os.path.exists(path):
        makedirp(os.path.dirname(path))
        with open(path, "w"):
            pass

def date_string_to_datetime(date):
    return datetime.strptime(date, YT_DATETIME_FORMAT_STRING)

def date_string_to_timestamp(date):
    return calendar.timegm(date_string_to_datetime(date).timetuple())

def date_string_to_timestamp_mcs(time_str):
    dt = date_string_to_datetime(time_str)
    return int(calendar.timegm(dt.timetuple()) * (10 ** 6) + dt.microsecond)

def datetime_to_string(date, is_local=False):
    if is_local:
        date = datetime.utcfromtimestamp(time.mktime(date.timetuple()))
    return date.strftime(YT_DATETIME_FORMAT_STRING)

def make_non_blocking(fd):
    # Use local import to support Windows.
    import fcntl
    flags = fcntl.fcntl(fd, fcntl.F_GETFL)
    fcntl.fcntl(fd, fcntl.F_SETFL, flags | os.O_NONBLOCK)

def to_native_str(string, encoding="utf-8"):
    if not PY3 and isinstance(string, text_type):
        return string.encode(encoding)
    if PY3 and isinstance(string, binary_type):
        return string.decode(encoding)
    return string

def copy_docstring_from(documented_function):
    """Decorator that copies docstring from one function to another.

    :param documented_function: function that provides docstring.

    Usage::

        def foo(...):
            "USEFUL DOCUMENTATION"
            ...

        @copy_docstring_from(foo)
        def bar(...)
            # docstring will be copied from `foo' function
            ...
    """
    return functools.wraps(documented_function, assigned=("__doc__",), updated=())

def is_process_alive(pid):
    try:
        os.kill(pid, 0)
    except OSError as err:
        if err.errno == errno.ESRCH:
            return False
        elif err.errno == errno.EPERM:
            return True
        else:
            # According to "man 2 kill" possible error values are
            # (EINVAL, EPERM, ESRCH)
            raise
    return True

def uuid_to_parts(guid):
    id_parts = guid.split("-")
    id_hi = int(id_parts[2], 16) << 32 | int(id_parts[3], 16)
    id_lo = int(id_parts[0], 16) << 32 | int(id_parts[1], 16)
    return id_hi, id_lo

def parts_to_uuid(id_hi, id_lo):
    guid = id_lo << 64 | id_hi
    mask = 0xFFFFFFFF

    parts = []
    for i in range(4):
        parts.append((guid & mask) >> (i * 32))
        mask <<= 32

    return "-".join(reversed(["{:x}".format(part) for part in parts]))

# TODO(asaitgalin): Remove copy-paste from YP.
def underscore_case_to_camel_case(str):
    result = []
    first = True
    upper = True
    for c in str:
        if c == "_":
            upper = True
        else:
            if upper:
                if not c in string.ascii_letters and not first:
                    result.append("_")
                c = c.upper()
            result.append(c)
            upper = False
        first = False
    return "".join(result)
