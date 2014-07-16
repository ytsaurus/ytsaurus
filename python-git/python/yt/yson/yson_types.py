# Do not inherit from object because of performance issues.
class YsonType(object):
    def __init__(self, *kargs, **kwargs):
        self.attributes = {}

    def __eq__(self, other):
        if hasattr(other, "attributes"):
            return self.attributes == other.attributes
        return not self.attributes

    def repr(self, type):
        if self.attributes:
            return repr({"value": type(self), "attributes": type(self.attributes)})
        return repr(type(self))

    def base_hash(self, type):
        if self.attributes:
            raise TypeError("unhashable type: yson hash non-tribial attributes")
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

class YsonDouble(float, YsonType):
    def __eq__(self, other):
        return float(self) == float(other) and YsonType.__eq__(self, other)

    def __hash__(self):
        return self.base_hash(float)

    def __repr__(self):
        return self.repr(float)

class YsonBoolean(int, YsonType):
    def __eq__(self, other):
        return (int(self) == 0) == (int(other) == 0) and YsonType.__eq__(self, other)

    def __hash__(self):
        return self.base_hash(bool)

    def __repr__(self):
        return self.repr(bool)

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

class YsonEntity(YsonType):
    def __repr__(self):
        if self.attributes:
            return repr({"value": "YsonEntity", "attributes": self.attributes})
        else:
            return "YsonEntity"
