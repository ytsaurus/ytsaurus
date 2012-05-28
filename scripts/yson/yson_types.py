#!/usr/bin/python
#!-*-coding:utf-8-*-

class YSONString(str):
    def __init__(self, *kargs, **kwargs):
        str.__init__(self, *kargs, **kwargs)
        self.attributes = {}

class YSONInteger(int):
    def __init__(self, *kargs, **kwargs):
        int.__init__(self, *kargs, **kwargs)
        self.attributes = {}

class YSONDouble(float):
    def __init__(self, *kargs, **kwargs):
        float.__init__(self, *kargs, **kwargs)
        self.attributes = {}

class YSONList(list):
    def __init__(self, *kargs, **kwargs):
        list.__init__(self, *kargs, **kwargs)
        self.attributes = {}

class YSONMap(dict):
    def __init__(self, *kargs, **kwargs):
        dict.__init__(self, *kargs, **kwargs)
        self.attributes = {}

class YSONEntity(object):
    def __init__(self, *kargs, **kwargs):
        object.__init__(self, *kargs, **kwargs)
        self.attributes = {}

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
