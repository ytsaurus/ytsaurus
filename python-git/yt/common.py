from yt.packages.six import iteritems, PY3, text_type, binary_type, string_types
from yt.packages.six.moves import map as imap
import yt.json_wrapper as json

# Fix for thread unsafety of datetime module.
# See http://bugs.python.org/issue7980 for more details.
import _strptime

from collections import Mapping
from datetime import datetime
from itertools import chain

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

# Standard YT time representation
YT_DATETIME_FORMAT_STRING = "%Y-%m-%dT%H:%M:%S.%fZ"

YT_NULL_TRANSACTION_ID = "0-0-0-0"

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

    def __str__(self):
        return format_error(self)

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

    def is_request_rate_limit_exceeded(self):
        """Request rate limit exceeded."""
        return self.contains_code(904)

    def is_request_queue_size_limit_exceeded(self):
        """Request rate limit exceeded."""
        return self.contains_code(108)

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
        return self.matches_regexp("Tablet .* is in state .*")

    def is_no_such_tablet(self):
        """No such tablet."""
        return self.contains_code(1701)

    def is_tablet_not_mounted(self):
        """Tablet is not mounted."""
        return self.contains_code(1702)

    def contains_code(self, code):
        """Check if HTTP response has specified error code."""
        def contains_code_recursive(error, error_code):
            if int(error.get("code", 0)) == error_code:
                return True
            for inner_error in error.get("inner_errors", []):
                if contains_code_recursive(inner_error, error_code):
                    return True
            return False

        return contains_code_recursive(self.error, code)

    def contains_text(self, text):
        """Check if HTTP response has specified status code."""
        def contains_text_recursive(error, text):
            message = ""
            if "message" in error:
                message = error["message"]

            if text in message:
                return True

            for inner_error in error.get("inner_errors", []):
                if contains_text_recursive(inner_error, text):
                    return True
            return False

        return contains_text_recursive(self.error, text)

    def matches_regexp(self, pattern):
        """Check if HTTP response has specified status code."""
        def matches_regexp_recursive(error, pattern):
            message = ""
            if "message" in error:
                message = error["message"]

            if re.match(pattern, message) is not None:
                return True

            for inner_error in error.get("inner_errors", []):
                if matches_regexp_recursive(inner_error, pattern):
                    return True
            return False

        return matches_regexp_recursive(self.error, pattern)

class PrettyPrintableDict(dict):
    pass

def _pretty_format(error, attribute_length_limit=None):
    def _escape(value):
        def escape(c):
            if c in string.printable:
                return c
            return "\\x{0:02x}".format(ord(c))
        value = value.replace("\n", "\\n").replace("\t", "\\t")
        return "".join(imap(escape, value))

    def format_attribute(name, value):
        name = to_native_str(name)
        if isinstance(value, PrettyPrintableDict):
            value = json.dumps(value, indent=2)
            value = value.replace("\n", "\n" + " " * (15 + 1 + 4))
        else:
            if isinstance(value, string_types):
                value = to_native_str(value)
            else:
                value = str(value)
            value = _escape(value)
            if attribute_length_limit is not None and len(value) > attribute_length_limit:
                value = value[:attribute_length_limit] + "...message truncated..."
        return " " * 4 + "%-15s %s" % (name, value)

    def simplify_error(error):
        if isinstance(error, YtError):
            error = error.simplify()
        elif isinstance(error, (Exception, KeyboardInterrupt)):
            error = {"code": 1, "message": str(error)}
        return error

    def format_messages(error, indent):
        error = simplify_error(error)

        result = []
        if not error.get("attributes", {}).get("transparent", False):
            result.append(" " * indent + to_native_str(error["message"]))
            new_indent = indent + 4
        else:
            new_indent = indent

        for inner_error in error.get("inner_errors", []):
            result.append(format_messages(inner_error, indent=new_indent))

        return "\n".join(result)

    def format_full_errors(error):
        error = simplify_error(error)

        lines = []
        if "message" in error:
            lines.append(to_native_str(error["message"]))

        if "code" in error and int(error["code"]) != 1:
            lines.append(format_attribute("code", error["code"]))

        attributes = error.get("attributes", {})

        origin_keys = ["host", "datetime"]
        origin_cpp_keys = ["pid", "tid", "fid"]
        if all(key in attributes for key in origin_keys):
            date = attributes["datetime"]
            if isinstance(date, datetime):
                date = date.strftime("%y-%m-%dT%H:%M:%S.%fZ")
            value = "{0} in {1}".format(attributes["host"], date)
            if all(key in attributes for key in origin_cpp_keys):
                value += " (pid %d, tid %x, fid %x)" % (attributes["pid"],attributes["tid"], attributes["fid"])
            lines.append(format_attribute("origin", value))

        location_keys = ["file", "line"]
        if all(key in attributes for key in location_keys):
            lines.append(format_attribute("location", "%s:%d" % (attributes["file"], attributes["line"])))

        for key, value in iteritems(attributes):
            if key in origin_keys or key in location_keys or key in origin_cpp_keys:
                continue
            lines.append(format_attribute(key, value))

        result = (" " * 4 + "\n").join(lines)
        if "inner_errors" in error:
            for inner_error in error["inner_errors"]:
                result += "\n" + format_full_errors(inner_error)

        return result

    return "{0}\n\n***** Details:\n{1}\n"\
        .format(format_messages(error, indent=0), format_full_errors(error))

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

def date_string_to_datetime(date):
    return datetime.strptime(date, YT_DATETIME_FORMAT_STRING)

def date_string_to_timestamp(date):
    return calendar.timegm(date_string_to_datetime(date).timetuple())

def date_string_to_timestamp_mcs(time_str):
    dt = date_string_to_datetime(time_str)
    return int(calendar.timegm(dt.timetuple()) * 1000000 + dt.microsecond)

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
