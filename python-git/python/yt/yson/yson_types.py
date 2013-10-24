# Do not inherit from object because of performance issues.
class YsonType(object):
    def __init__(self, *kargs, **kwargs):
        self.attributes = {}

    def __eq__(self, other):
        if hasattr(other, "attributes"):
            return self.attributes == other.attributes
        return not self.attributes

class YsonString(str, YsonType):
    def __eq__(self, other):
        return str(self) == str(other) and YsonType.__eq__(self, other)

class YsonInteger(int, YsonType):
    def __eq__(self, other):
        return int(self) == int(other) and YsonType.__eq__(self, other)

class YsonDouble(float, YsonType):
    def __eq__(self, other):
        return float(self) == float(other) and YsonType.__eq__(self, other)

class YsonList(list, YsonType):
    def __init__(self, *kargs, **kwargs):
        YsonType.__init__(self, *kargs, **kwargs)
        list.__init__(self, *kargs, **kwargs)

    def __eq__(self, other):
        return list(self) == list(other) and YsonType.__eq__(self, other)

class YsonMap(dict, YsonType):
    def __init__(self, *kargs, **kwargs):
        YsonType.__init__(self, *kargs, **kwargs)
        dict.__init__(self, *kargs, **kwargs)

    def __eq__(self, other):
        return dict(self) == dict(other) and YsonType.__eq__(self, other)

class YsonEntity(YsonType):
    pass
