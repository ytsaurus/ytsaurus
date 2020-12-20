from yt.packages.six import PY3, integer_types, binary_type, text_type
from yt.common import YtError

class YsonType(object):
    def __getattr__(self, attribute):
        if attribute == "attributes":
            self.__dict__[attribute] = {}
            return self.__dict__[attribute]
        raise AttributeError('Attribute "{0}" not found'.format(attribute))

    def has_attributes(self):
        try:
            return "attributes" in self.__dict__ and self.attributes is not None and self.attributes != {}
        except:
            return False

    def __eq__(self, other):
        try:
            has_attributes = other.has_attributes()
        except AttributeError:
            has_attributes = False
        if has_attributes:
            return self.attributes == other.attributes
        return not self.has_attributes()

    def __ne__(self, other):
        return not (self == other)

    def to_str(self, base_type, str_func):
        if self.has_attributes():
            return str_func({"value": base_type(self), "attributes": self.attributes})
        return str_func(base_type(self))

    def base_hash(self, type_):
        if self.has_attributes():
            raise TypeError("unhashable type: YSON has non-trivial attributes")
        return hash(type_(self))

class YsonString(binary_type, YsonType):
    def __eq__(self, other):
        # COMPAT: With implicit promotion of str to unicode it can make sense
        # to compare binary YsonString to unicode string.
        if not isinstance(other, (binary_type, text_type)):
            return NotImplemented
        return binary_type(self) == binary_type(other) and YsonType.__eq__(self, other)

    def __ne__(self, other):
        return not (self == other)

    def __hash__(self):
        return self.base_hash(binary_type)

    def __repr__(self):
        return self.to_str(binary_type, repr)

    def is_unicode(self):
        return False

    def get_bytes(self):
        return self

class YsonUnicode(text_type, YsonType):
    def __new__(cls, s, encoding="utf-8"):
        obj = text_type.__new__(cls, s)
        obj._encoding = encoding
        return obj

    def __eq__(self, other):
        if not isinstance(other, text_type):
            return NotImplemented
        return text_type(self) == text_type(other) and YsonType.__eq__(self, other)

    def __ne__(self, other):
        return not (self == other)

    def __hash__(self):
        return self.base_hash(text_type)

    def __repr__(self):
        return self.to_str(text_type, repr)

    def is_unicode(self):
        return True

    def get_bytes(self):
        return self.encode(self._encoding)


class NotUnicodeError(YtError):
    pass


def truncate(s, length=50):
    assert isinstance(s, bytes)
    if len(s) < length:
        return s
    return s[:length] + b"..."


def make_raise_not_unicode_error(name):
    def fun(self, *args, **kwargs):
        raise NotUnicodeError('Method "{}" is not allowed: YSON string "{}" '
                              "could not be decoded to Unicode, "
                              "see THE DOC".format(name, truncate(self._b)))
    return fun


def proxy(cls):
    ALLOWED_METHODS = [
        "get_bytes",
        "is_unicode",
        "__hash__",
        "__eq__",
        "__ne__",
        "__repr__",
        "__dict__",
        "__qualname__",
        "__class__",
        "__mro__",
        "__new__",
        "__init__",
        "__getattr__",
        "__setattr__",
        "__getattribute__",
    ]

    ADDITIONAL_METHODS = [
        "__radd__",
    ]

    for name in dir(text_type):
        attr = getattr(text_type, name)
        if callable(attr) and name not in ALLOWED_METHODS:
            setattr(cls, name, make_raise_not_unicode_error(name))
    for name in ADDITIONAL_METHODS:
        setattr(cls, name, make_raise_not_unicode_error(name))
    return cls


# NB: This class is never returned by library in Python2.
@proxy
class YsonStringProxy(YsonType):
    def __init__(self, b):
        assert PY3, "YsonStringProxy should not be used in Python 2"
        assert isinstance(b, bytes)
        self._b = b

    def is_unicode(self):
        return False

    def get_bytes(self):
        return self._b

    def __repr__(self):
        value = "<YsonStringProxy>{!r}".format(self._b)
        if self.has_attributes():
            return repr({"attributes": self.attributes, "value": value})
        return value

    def __hash__(self):
        return hash(self._b)

    def __eq__(self, other):
        if isinstance(other, bytes):
            return self._b == bytes(other) and YsonType.__eq__(self, other)
        elif isinstance(other, YsonStringProxy):
            return self._b == other._b and YsonType.__eq__(self, other)
        else:
            return NotImplemented

    def __ne__(self, other):
        return not (self == other)


if PY3:
    _YsonIntegerBase = int
else:
    _YsonIntegerBase = long

class YsonIntegerBase(_YsonIntegerBase, YsonType):
    def __eq__(self, other):
        if not isinstance(other, integer_types):
            return NotImplemented
        return _YsonIntegerBase(self) == _YsonIntegerBase(other) and YsonType.__eq__(self, other)

    def __ne__(self, other):
        return not (self == other)

    def __hash__(self):
        return self.base_hash(_YsonIntegerBase)

    def __repr__(self):
        return self.to_str(_YsonIntegerBase, repr)

    def __str__(self):
        return self.to_str(_YsonIntegerBase, str)

class YsonInt64(YsonIntegerBase):
    pass

class YsonUint64(YsonIntegerBase):
    pass

class YsonDouble(float, YsonType):
    def __eq__(self, other):
        if not isinstance(other, float):
            return NotImplemented
        return float(self) == float(other) and YsonType.__eq__(self, other)

    def __ne__(self, other):
        return not (self == other)

    def __hash__(self):
        return self.base_hash(float)

    def __repr__(self):
        return self.to_str(float, repr)

    def __str__(self):
        return self.to_str(float, str)

class YsonBoolean(int, YsonType):
    def __eq__(self, other):
        if not isinstance(other, int):
            return NotImplemented
        return (int(self) == 0) == (int(other) == 0) and YsonType.__eq__(self, other)

    def __ne__(self, other):
        return not (self == other)

    def __hash__(self):
        return self.base_hash(bool)

    # NB: do not change this representation, because
    # this type required to be JSON serializable.
    # JSON encoder thinks that it is interger and calls str.
    def __repr__(self):
        return "true" if self else "false"

    def __str__(self):
        return self.__repr__()

class YsonList(list, YsonType):
    def __eq__(self, other):
        if not isinstance(other, list):
            return NotImplemented
        return list(self) == list(other) and YsonType.__eq__(self, other)

    def __ne__(self, other):
        return not (self == other)

    def __hash__(self):
        raise TypeError('unhashable type "YsonList"')

    def __repr__(self):
        return self.to_str(list, repr)

    def __str__(self):
        return self.to_str(list, str)

class YsonMap(dict, YsonType):
    def __eq__(self, other):
        if not isinstance(other, dict):
            return NotImplemented
        return dict(self) == dict(other) and YsonType.__eq__(self, other)

    def __ne__(self, other):
        return not (self == other)

    def __hash__(self):
        raise TypeError('unhashable type "YsonMap"')

    def __repr__(self):
        return self.to_str(dict, repr)

    def __str__(self):
        return self.to_str(dict, str)

class YsonEntity(YsonType):
    def __init__(self, value=None):
        assert value is None

    def __eq__(self, other):
        if other is None and not self.attributes:
            return True
        if not isinstance(other, YsonEntity):
            return NotImplemented
        return YsonType.__eq__(self, other)

    def __ne__(self, other):
        return not (self == other)

    def __bool__(self):
        return False

    def __repr__(self):
        if self.attributes:
            return repr({"value": "YsonEntity", "attributes": self.attributes})
        else:
            return "YsonEntity"

    def __str__(self):
        return self.__repr__()

    __nonzero__ = __bool__
