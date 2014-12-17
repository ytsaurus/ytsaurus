class YsonType(object):
    def __init__(self, *kargs, **kwargs):
        self.attributes = {}

    def __eq__(self, other):
        if hasattr(other, "attributes"):
            return self.attributes == other.attributes
        return not self.attributes

    def repr(self, base_type):
        if self.attributes:
            return repr({"value": base_type(self), "attributes": self.attributes})
        return repr(base_type(self))

    def base_hash(self, type):
        if self.attributes:
            raise TypeError("unhashable type: YSON has non-trivial attributes")
        return hash(type(self))

class YsonString(str, YsonType):
    def __eq__(self, other):
        return str(self) == str(other) and YsonType.__eq__(self, other)

    def __hash__(self):
        return self.base_hash(str)

    def __repr__(self):
        return self.repr(str)

class YsonInt64(int, YsonType):
    def __eq__(self, other):
        return int(self) == int(other) and YsonType.__eq__(self, other)

    def __hash__(self):
        return self.base_hash(int)

    def __repr__(self):
        return self.repr(int)

    def __str__(self):
        return self.__repr__()

class YsonUint64(long, YsonType):
    def __eq__(self, other):
        return long(self) == long(other) and YsonType.__eq__(self, other)

    def __hash__(self):
        return self.base_hash(long)

    def __repr__(self):
        return self.repr(long)

    def __str__(self):
        return self.__repr__()

class YsonDouble(float, YsonType):
    def __eq__(self, other):
        return float(self) == float(other) and YsonType.__eq__(self, other)

    def __hash__(self):
        return self.base_hash(float)

    def __repr__(self):
        return self.repr(float)

    def __str__(self):
        return self.__repr__()

class YsonBoolean(int, YsonType):
    def __eq__(self, other):
        return (int(self) == 0) == (int(other) == 0) and YsonType.__eq__(self, other)

    def __hash__(self):
        return self.base_hash(bool)

    def __repr__(self):
        return "true" if self else "false"

    def __str__(self):
        return self.__repr__()

class YsonList(list, YsonType):
    def __init__(self, *kargs, **kwargs):
        YsonType.__init__(self, *kargs, **kwargs)
        list.__init__(self, *kargs, **kwargs)

    def __eq__(self, other):
        return list(self) == list(other) and YsonType.__eq__(self, other)

    def __hash__(self):
        raise TypeError("unhashable type 'YsonList'")

    def __repr__(self):
        return self.repr(list)

    def __str__(self):
        return self.__repr__()

class YsonMap(dict, YsonType):
    def __init__(self, *kargs, **kwargs):
        YsonType.__init__(self, *kargs, **kwargs)
        dict.__init__(self, *kargs, **kwargs)

    def __eq__(self, other):
        return dict(self) == dict(other) and YsonType.__eq__(self, other)

    def __hash__(self):
        raise TypeError("unhashable type 'YsonMap'")

    def __repr__(self):
        return self.repr(dict)

    def __str__(self):
        return self.__repr__()

class YsonEntity(YsonType):
    def __eq__(self, other):
        if other is None and not self.attributes:
            return True
        return isinstance(other, YsonEntity) and YsonType.__eq__(self, other)

    def __repr__(self):
        if self.attributes:
            return repr({"value": "YsonEntity", "attributes": self.attributes})
        else:
            return "YsonEntity"

    def __str__(self):
        return self.__repr__()
