from yt.packages.six import PY3, integer_types, binary_type, text_type

class YsonType(object):
    def __getattr__(self, attribute):
        if attribute == "attributes":
            self.__dict__[attribute] = {}
            return self.__dict__[attribute]
        raise AttributeError("Attribute '%s' not found" % attribute)

    def __eq__(self, other):
        if hasattr(other, "attributes"):
            return self.attributes == other.attributes
        return not self.attributes

    def __ne__(self, other):
        return not (self == other)

    def to_str(self, base_type, str_func):
        if self.attributes:
            return str_func({"value": base_type(self), "attributes": self.attributes})
        return str_func(base_type(self))

    def base_hash(self, type_):
        if self.attributes:
            raise TypeError("unhashable type: YSON has non-trivial attributes")
        return hash(type_(self))

class YsonString(binary_type, YsonType):
    def __eq__(self, other):
        # COMPAT: With implicit promotion of str to unicode it can make sense
        # to compare binary YsonString to unicode string.
        if not isinstance(other, (binary_type, text_type)):
            return False
        return binary_type(self) == binary_type(other) and YsonType.__eq__(self, other)

    def __ne__(self, other):
        return not (self == other)

    def __hash__(self):
        return self.base_hash(binary_type)

    def __repr__(self):
        return self.to_str(binary_type, repr)

class YsonUnicode(text_type, YsonType):
    def __eq__(self, other):
        if not isinstance(other, text_type):
            return False
        return text_type(self) == text_type(other) and YsonType.__eq__(self, other)

    def __ne__(self, other):
        return not (self == other)

    def __hash__(self):
        return self.base_hash(text_type)

    def __repr__(self):
        return self.to_str(text_type, repr)

if PY3:
    _YsonIntegerBase = int
else:
    _YsonIntegerBase = long

class YsonIntegerBase(_YsonIntegerBase, YsonType):
    def __eq__(self, other):
        if not isinstance(other, integer_types):
            return False
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
            return False
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
            return False
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
            return False
        return list(self) == list(other) and YsonType.__eq__(self, other)

    def __ne__(self, other):
        return not (self == other)

    def __hash__(self):
        raise TypeError("unhashable type 'YsonList'")

    def __repr__(self):
        return self.to_str(list, repr)

    def __str__(self):
        return self.to_str(list, str)

class YsonMap(dict, YsonType):
    def __eq__(self, other):
        if not isinstance(other, dict):
            return False
        return dict(self) == dict(other) and YsonType.__eq__(self, other)

    def __ne__(self, other):
        return not (self == other)

    def __hash__(self):
        raise TypeError("unhashable type 'YsonMap'")

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
        return isinstance(other, YsonEntity) and YsonType.__eq__(self, other)

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
