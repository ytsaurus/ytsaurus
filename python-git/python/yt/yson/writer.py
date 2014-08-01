#!/usr/bin/python
#!-*-coding:utf-8-*-

from common import YsonError

'''``yson`` exposes an API familiar to users of the standard library
:mod:`marshal` and :mod:`pickle` modules.

Serializable types (rules applied top to bottom):
int, long -> yson int
str -> yson string
unicode -> yson string (using specified encoding)
any Mapping -> yson dict
any Iterable -> yson list

Simple examples:
>>> import yson
>>> b = [4, 5, 6]
>>> print yson.dumps({'a' : b, 'b' : b}, indent='  ')
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
>>> print yson.dumps(('a', 'a'), indent='  ')
[
  "a";
  "a";
]
>>> print yson.dumps(123456)
123456
>>> print yson.dumps(u'"Hello world!" -- "Превед, медвед!"')
"\"Hello world!\" -- \"\xd0\x9f\xd1\x80\xd0\xb5\xd0\xb2\xd0\xb5\xd0\xb4, \xd0\xbc\xd0\xb5\xd0\xb4\xd0\xb2\xd0\xb5\xd0\xb4!\""
'''

from collections import Iterable, Mapping

from yson_types import YsonEntity

__all__ = ["dump", "dumps"]

def dump(object, stream, yson_format=None, indent=None, check_circular=True, encoding='utf-8', yson_type=None):
    '''Serialize ``object`` as a Yson formatted stream to ``fp`` (a
    ``.write()``-supporting file-like object).'''
    stream.write(dumps(object, yson_format=yson_format, check_circular=check_circular, encoding=encoding, indent=indent, yson_type=yson_type))


def dumps(object, yson_format=None, indent=None, check_circular=True, encoding='utf-8', yson_type=None):
    '''Serialize ``object`` as a Yson formatted string'''
    if indent is None:
        indent = 4
    if isinstance(indent, int):
        indent = " " * indent
    if yson_format is None:
        yson_format = "pretty"
    if yson_format not in ["pretty", "text"]:
        raise YsonError("%s format is not supported" % yson_format)
    if yson_format == "text":
        indent = None

    d = Dumper(check_circular, encoding, indent, yson_type)
    return d.dumps(object)


class Dumper(object):
    def __init__(self, check_circular, encoding, indent, yson_type):
        self.yson_type = yson_type

        self._seen_objects = None
        if check_circular:
            self._seen_objects = {}

        self._encoding = encoding
        self._format = FormatDetails(indent)
        self._level = -1

    def dumps(self, obj):
        self._level += 1

        attributes = ''
        if hasattr(obj, 'attributes') and obj.attributes:
            attributes = self._dump_attributes(obj.attributes)

        result = None
        if obj is False:
            result = "%false"
        elif obj is True:
            result = "%true"
        elif isinstance(obj, (int, long, float)):
            result = str(obj)
        elif isinstance(obj, basestring):
            result = self._dump_string(obj)
        elif isinstance(obj, Mapping):
            result = self._dump_map(obj)
        elif isinstance(obj, Iterable):
            result = self._dump_list(obj)
        elif isinstance(obj, YsonEntity) or obj is None:
            result = "#"
        else:
            raise TypeError(repr(obj) + " is not Yson serializable.")
        self._level -= 1
        return attributes + result

    def _dump_string(self, obj):
        if isinstance(obj, str):
            return _fix_repr(repr(str(obj)))
        elif isinstance(obj, unicode):
            return _fix_repr(repr(str(obj.encode(self._encoding))))
        else:
            assert False

    def _dump_map(self, obj):
        result = ['{', self._format.nextline()]
        for k, v in obj.items():
            if not isinstance(k, basestring):
                raise TypeError("Only string can be Yson map key. Key: %s" % repr(obj))

            @self._circular_check(v)
            def process_item():
                return [self._format.prefix(self._level + 1),
                    self._dump_string(k), self._format.space(), '=',
                    self._format.space(), self.dumps(v), ';', self._format.nextline()]

            result += process_item()

        result += [self._format.prefix(self._level), '}']
        return ''.join(result)

    def _dump_list(self, obj):
        result = [self._format.nextline()]
        for v in obj:
            @self._circular_check(v)
            def process_item():
                return [self._format.prefix(self._level + 1),
                    self.dumps(v), ';', self._format.nextline()]

            result += process_item()

        result += [self._format.prefix(self._level)]
        if self.yson_type == "list_fragment" and self._level == 0:
            return ''.join(result)
        else:
            return "[%s]" % ''.join(result)

    def _dump_attributes(self, obj):
        result = ['<', self._format.nextline()]
        for k, v in obj.items():
            if not isinstance(k, basestring):
                raise TypeError("Only string can be Yson map key. Key: %s" % repr(obj))

            @self._circular_check(v)
            def process_item():
                return [self._format.prefix(self._level + 1),
                    self._dump_string(k), self._format.space(), '=',
                    self._format.space(), self.dumps(v), ';', self._format.nextline()]

            result += process_item()

        result += [self._format.prefix(self._level), '>']
        return ''.join(result)

    def _circular_check(self, obj):
        def decorator(fn):
            def wrapper(*args, **kwargs):
                obj_id = None
                if not self._seen_objects is None:
                    obj_id = id(obj)
                    if obj_id in self._seen_objects:
                        raise ValueError("Circular reference detected. " \
                            "Object: %s" % repr(obj))
                    else:
                        self._seen_objects[obj_id] = obj

                result = fn(*args, **kwargs)

                if self._seen_objects:
                    del self._seen_objects[obj_id]
                return result

            return wrapper
        return decorator

def _fix_repr(s):
    '''Dirty hack to make yson-readable C-style escaping from
    ``repr``-encoded string if python have chosen single-quoted
    representation (as he usually does).
    See PyString_Repr() function in python sources for details.'''
    if s.startswith("'"):
        s = s[1:-1].replace(r"\'", "'").replace('"', r'\"')
        return '"%s"' % s
    else:
        return s

class FormatDetails(object):
    def __init__(self, indent):
        self._indent = indent

    def prefix(self, level):
        if self._indent:
            return ''.join([self._indent] * level)
        else:
            return ''

    def nextline(self):
        if self._indent:
            return '\n'
        else:
            return ''

    def space(self):
        if self._indent:
            return ' '
        else:
            return ''
