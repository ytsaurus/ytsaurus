class YsonType(object):
    def __init__(self, *kargs, **kwargs):
        self.attributes = {}

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

    def base_hash(self, type):
        if self.attributes:
            raise TypeError("unhashable type: YSON has non-trivial attributes")
        return hash(type(self))

class YsonString(str, YsonType):
    def __eq__(self, other):
        if not isinstance(other, basestring):
            return False
        return str(self) == str(other) and YsonType.__eq__(self, other)

    def __ne__(self, other):
        return not (self == other)

    def __hash__(self):
        return self.base_hash(str)

    def __repr__(self):
        return self.to_str(str, repr)

class YsonIntegerBase(long, YsonType):
    def __eq__(self, other):
        if not isinstance(other, (int, long)):
            return False
        return long(self) == long(other) and YsonType.__eq__(self, other)

    def __ne__(self, other):
        return not (self == other)

    def __hash__(self):
        return self.base_hash(long)

    def __repr__(self):
        return self.to_str(long, repr)

    def __str__(self):
        return self.to_str(long, str)

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
    def __init__(self, *kargs, **kwargs):
        YsonType.__init__(self, *kargs, **kwargs)
        list.__init__(self, *kargs, **kwargs)

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
    def __init__(self, *kargs, **kwargs):
        YsonType.__init__(self, *kargs, **kwargs)
        dict.__init__(self, *kargs, **kwargs)

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
    def __eq__(self, other):
        if other is None and not self.attributes:
            return True
        return isinstance(other, YsonEntity) and YsonType.__eq__(self, other)

    def __ne__(self, other):
        return not (self == other)

    def __repr__(self):
        if self.attributes:
            return repr({"value": "YsonEntity", "attributes": self.attributes})
        else:
            return "YsonEntity"

    def __str__(self):
        return self.__repr__()
