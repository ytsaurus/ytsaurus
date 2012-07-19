#!/usr/bin/python
#!-*-coding:utf-8-*-

class YSONType(object):
    def __init__(self, *kargs, **kwargs):
        self.attributes = {}

class YSONString(str, YSONType):
    pass

class YSONInteger(int, YSONType):
    pass

class YSONDouble(float, YSONType):
    pass

class YSONList(list, YSONType):
    def __init__(self, *kargs, **kwargs):
        YSONType.__init__(self, *kargs, **kwargs)
        list.__init__(self, *kargs, **kwargs)

class YSONMap(dict, YSONType):
    def __init__(self, *kargs, **kwargs):
        YSONType.__init__(self, *kargs, **kwargs)
        dict.__init__(self, *kargs, **kwargs)

class YSONEntity(YSONType):
    pass

def convert_to_YSON_type(value, attributes = None):
    if isinstance(value, str):
        result = YSONString(value)
    elif isinstance(value, int):
        result = YSONInteger(value)
    elif isinstance(value, float):
        result = YSONDouble(value)
    elif isinstance(value, list):
        result = YSONList(value)
    elif isinstance(value, dict):
        result = YSONMap(value)
    else:
        result = YSONEntity()
    if attributes is not None:
        result.attributes = attributes
    return result
