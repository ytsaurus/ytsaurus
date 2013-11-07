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

class YsonString(str, YsonType):
    def __eq__(self, other):
        return str(self) == str(other) and YsonType.__eq__(self, other)

    def __repr__(self):
        return self.repr(str)

class YsonInteger(int, YsonType):
    def __eq__(self, other):
        return int(self) == int(other) and YsonType.__eq__(self, other)

    def __repr__(self):
        return self.repr(int)

class YsonDouble(float, YsonType):
    def __eq__(self, other):
        return float(self) == float(other) and YsonType.__eq__(self, other)

    def __repr__(self):
        return self.repr(float)

class YsonList(list, YsonType):
    def __init__(self, *kargs, **kwargs):
        YsonType.__init__(self, *kargs, **kwargs)
        list.__init__(self, *kargs, **kwargs)

    def __eq__(self, other):
        return list(self) == list(other) and YsonType.__eq__(self, other)

    def __repr__(self):
        return self.repr(list)

class YsonMap(dict, YsonType):
    def __init__(self, *kargs, **kwargs):
        YsonType.__init__(self, *kargs, **kwargs)
        dict.__init__(self, *kargs, **kwargs)

    def __eq__(self, other):
        return dict(self) == dict(other) and YsonType.__eq__(self, other)

    def __repr__(self):
        return self.repr(dict)

class YsonEntity(YsonType):
    def __repr__(self):
        if self.attributes:
            return repr({"value": "YsonEntity", "attributes": self.attributes})
        else:
            return "YsonEntity"
