class YsonType(object):
    def __init__(self, *kargs, **kwargs):
        self.attributes = {}

class YsonString(str, YsonType):
    pass

class YsonInteger(int, YsonType):
    pass

class YsonDouble(float, YsonType):
    pass

class YsonList(list, YsonType):
    def __init__(self, *kargs, **kwargs):
        YsonType.__init__(self, *kargs, **kwargs)
        list.__init__(self, *kargs, **kwargs)

class YsonMap(dict, YsonType):
    def __init__(self, *kargs, **kwargs):
        YsonType.__init__(self, *kargs, **kwargs)
        dict.__init__(self, *kargs, **kwargs)

class YsonEntity(YsonType):
    pass
