# -*- coding: utf-8 -*-

"""``yson`` exposes an API familiar to users of the standard library
:mod:`marshal` and :mod:`pickle` modules.

Serializable types (rules applied top to bottom):
int, long -> yson int
str -> yson string
unicode -> yson string (using specified encoding)
any Mapping -> yson dict
any Iterable -> yson list

Simple examples::

    >>> import yson
    >>> b = [4, 5, 6]
    >>> print yson.dumps({"a" : b, "b" : b}, indent="  ")
    {
      "a" : [
        4;
        5;
        6;
      ];
      "b" : [
        4;
        5;
        6;
      ];
    }
    >>> print yson.dumps(("a", "a"), indent="  ")
    [
      "a";
      "a";
    ]
    >>> print yson.dumps(123456)
    123456
    >>> print yson.dumps(u'"Hello world!" -- "Превед, медвед!"')
    "\"Hello world!\" -- \"\xd0\x9f\xd1\x80\xd0\xb5\xd0\xb2\xd0\xb5\xd0\xb4, \xd0\xbc\xd0\xb5\xd0\xb4\xd0\xb2\xd0\xb5\xd0\xb4!\""
"""

from .common import YsonError
from . import yson_types

from yt.packages.six.moves import map as imap
from yt.packages.six import integer_types, text_type, binary_type, iteritems, PY3

import math
import re
from collections import Iterable, Mapping

__all__ = ["dump", "dumps"]

ESCAPE = re.compile(br'[\x00-\x1f\\"\b\f\n\r\t\x7f-\xff]')
ESCAPE_DICT = {
    b"\\": b"\\\\",
    b'"': b'\\"',
    b"\t": b"\\t",
    b"\n": b"\\n",
    b"\r": b"\\r",
}

def _escape_bytes(obj):
    def replace(match):
        try:
            return ESCAPE_DICT[match.group(0)]
        except KeyError:
            return "\\x{0:02x}".format(ord(match.group(0))).encode("ascii")

    return ESCAPE.sub(replace, obj)

def dump(object, stream, yson_format=None, indent=None, check_circular=True, encoding="utf-8", yson_type=None,
         boolean_as_string=False):
    """Serializes `object` as a YSON formatted stream to `stream`.

    :param str yson_format: format of YSON, one of ["binary", "text", "pretty"].
    :param str yson_type: type of YSON, one of ["node", "list_fragment", "map_fragment"].
    :param int indent: number of identation spaces in pretty format.
    :param str encoding: encoding that uses to encode unicode strings.
    :param bool boolean_as_string: whether dump boolean values as YSON strings (needed for backward compatibility).
    """

    stream.write(dumps(object, yson_format=yson_format, check_circular=check_circular, encoding=encoding,
                       indent=indent, yson_type=yson_type, boolean_as_string=boolean_as_string))

class YsonContext(object):
    def __init__(self):
        self.path_parts = []
        self.row_index = None

    def push(self, key_or_index):
        self.path_parts.append(key_or_index)

    def pop(self):
        self.path_parts.pop()

def _raise_error_with_context(message, context):
    attributes = {}
    if context.row_index is not None:
        attributes["row_index"] = context.row_index

    path_parts = imap(str, context.path_parts)
    if context.path_parts:
        attributes["row_key_path"] = "/" + "/".join(path_parts)
    raise YsonError(message, attributes=attributes)

def dumps(object, yson_format=None, indent=None, check_circular=True, encoding="utf-8", yson_type=None,
          boolean_as_string=False):
    """Serializes `object` as a YSON formatted stream to string and returns it. See :func:`dump <.dump>`."""
    if indent is None:
        indent = 4
    if isinstance(indent, int):
        indent = b" " * indent
    if yson_format is None:
        yson_format = "text"
    if yson_format not in ["pretty", "text"]:
        raise YsonError("{0} format is not supported".format(yson_format))
    if yson_format == "text":
        indent = None
    if yson_type is not None:
        if yson_type not in ["list_fragment", "map_fragment", "node"]:
            raise YsonError("YSON type {0} is not supported".format(yson_type))
    else:
        yson_type = "node"

    d = Dumper(check_circular, encoding, indent, yson_type, boolean_as_string)
    return d.dumps(object, YsonContext())

class Dumper(object):
    def __init__(self, check_circular, encoding, indent, yson_type, boolean_as_string):
        self.yson_type = yson_type
        self.boolean_as_string = boolean_as_string

        self._seen_objects = None
        if check_circular:
            self._seen_objects = {}

        self._encoding = encoding
        self._format = FormatDetails(indent)
        if yson_type == "node":
            self._level = -1
        else:
            self._level = -2  # Stream elements are one level deep, but need not be indented

    def _has_attributes(self, obj):
        if hasattr(obj, "has_attributes"):
            return obj.has_attributes()
        return hasattr(obj, "attributes")

    def dumps(self, obj, context):
        self._level += 1
        attributes = b""
        if self._has_attributes(obj):
            if not isinstance(obj.attributes, dict):
                _raise_error_with_context('Invalid field "attributes": it must be string or None', context)
            if obj.attributes:
                attributes = self._dump_attributes(obj.attributes, context)

        result = None
        if obj is False or (isinstance(obj, yson_types.YsonBoolean) and not obj):
            if self.boolean_as_string:
                result = b'"false"'
            else:
                result = b"%false"
        elif obj is True or (isinstance(obj, yson_types.YsonBoolean) and obj):
            if self.boolean_as_string:
                result = b'"true"'
            else:
                result = b"%true"
        elif isinstance(obj, integer_types):
            if obj < -2 ** 63 or obj >= 2 ** 64:
                _raise_error_with_context("Integer {0} cannot be represented in YSON "
                                          "since it is out of range [-2^63, 2^64 - 1])".format(obj), context)

            greater_than_max_int64 = obj >= 2 ** 63
            if isinstance(obj, yson_types.YsonUint64) and obj < 0:
                _raise_error_with_context("Can not dump negative integer as YSON uint64", context)
            if isinstance(obj, yson_types.YsonInt64) and greater_than_max_int64:
                _raise_error_with_context("Can not dump integer greater than 2^63-1 as YSON int64", context)

            if type(obj) in (yson_types.YsonInt64, yson_types.YsonUint64):
                obj_str = str(yson_types._YsonIntegerBase(obj))
            else:
                obj_str = str(obj)

            result = obj_str.encode("ascii")
            if not PY3:
                result = result.rstrip(b"L")
            if greater_than_max_int64 or isinstance(obj, yson_types.YsonUint64):
                result += b"u"
        elif isinstance(obj, float):
            if math.isnan(obj):
                result = b"%nan"
            elif math.isinf(obj):
                if obj > 0:
                    result = b"%inf"
                else:
                    result = b"%-inf"
            else:
                if type(obj) == yson_types.YsonDouble:
                    obj_str = str(float(obj))
                else:
                    obj_str = str(obj)
                result = obj_str.encode("ascii")
        elif isinstance(obj, (text_type, binary_type)):
            result = self._dump_string(obj, context)
        elif isinstance(obj, Mapping):
            result = self._dump_map(obj, context)
        elif isinstance(obj, Iterable):
            result = self._dump_list(obj, context)
        elif isinstance(obj, yson_types.YsonEntity) or obj is None:
            result = b"#"
        else:
            _raise_error_with_context("{0!r} is not Yson serializable".format(obj), context)
        self._level -= 1
        return attributes + result

    def _dump_string(self, obj, context):
        result = [b'"']
        if isinstance(obj, binary_type):
            if self._encoding is not None and PY3:
                _raise_error_with_context('Bytes object {0!r} cannot be encoded to {1}. To disable this behaviour '
                                          'and allow byte string dumping set "encoding" '
                                          'parameter to None'.format(obj, self._encoding),
                                          context)
            result.append(_escape_bytes(obj))
        elif isinstance(obj, text_type):
            if self._encoding is None:
                _raise_error_with_context('Cannot encode unicode object {0!r} to bytes since "encoding" '
                                          'parameter is None. Consider using byte strings '
                                          'instead or specify encoding'.format(obj),
                                          context)
            result.append(_escape_bytes(obj.encode(self._encoding)))
        else:
            assert False
        result.append(b'"')
        return b"".join(result)

    def _dump_map(self, obj, context):
        is_stream = self.yson_type == "map_fragment" and self._level == -1
        result = []
        if not is_stream:
            result += [b"{", self._format.nextline()]

        for k, v in iteritems(obj):
            if not isinstance(k, (text_type, binary_type)):
                _raise_error_with_context("Only string can be Yson map key. Key: {0!r}".format(k), context)

            @self._circular_check(v)
            def process_item():
                context.push(k)
                item = [self._format.prefix(self._level + 1),
                        self._dump_string(k, context), self._format.space(), b"=",
                        self._format.space(), self.dumps(v, context), b";", self._format.nextline(is_stream)]
                context.pop()
                return item

            result += process_item()

        if not is_stream:
            result += [self._format.prefix(self._level), b"}"]

        return b"".join(result)

    def _dump_list(self, obj, context):
        is_stream = self.yson_type == "list_fragment" and self._level == -1
        result = []
        if not is_stream:
            result += [b"[", self._format.nextline()]

        for index, v in enumerate(obj):
            @self._circular_check(v)
            def process_item():
                if is_stream:
                    context.row_index = index
                else:
                    context.push(index)
                item = [self._format.prefix(self._level + 1),
                        self.dumps(v, context), b";", self._format.nextline(is_stream)]
                if not is_stream:
                    context.pop()
                return item

            result += process_item()

        if not is_stream:
            result += [self._format.prefix(self._level), b"]"]

        return b"".join(result)

    def _dump_attributes(self, obj, context):
        result = [b"<", self._format.nextline()]
        for k, v in obj.items():
            if not isinstance(k, (text_type, binary_type)):
                _raise_error_with_context("Only string can be Yson map key. Key: {0!r}".format(obj), context)

            @self._circular_check(v)
            def process_item():
                context.push("@" + k)
                item = [self._format.prefix(self._level + 1),
                        self._dump_string(k, context), self._format.space(), b"=",
                        self._format.space(), self.dumps(v, context), b";", self._format.nextline()]
                context.pop()
                return item

            result += process_item()
        result += [self._format.prefix(self._level), b">"]
        return b"".join(result)

    def _circular_check(self, obj):
        def decorator(fn):
            def wrapper(*args, **kwargs):
                obj_id = None
                if not self._seen_objects is None:
                    obj_id = id(obj)
                    if obj_id in self._seen_objects:
                        raise YsonError("Circular reference detected. Object: {0!r}".format(obj))
                    else:
                        self._seen_objects[obj_id] = obj

                result = fn(*args, **kwargs)

                if self._seen_objects:
                    del self._seen_objects[obj_id]
                return result

            return wrapper
        return decorator


class FormatDetails(object):
    def __init__(self, indent):
        self._indent = indent

    def prefix(self, level):
        if self._indent:
            return b"".join([self._indent] * level)
        else:
            return b""

    def nextline(self, force=False):
        if force or self._indent:
            return b"\n"
        else:
            return b""

    def space(self):
        if self._indent:
            return b" "
        else:
            return b""
