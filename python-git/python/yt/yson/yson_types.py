#!/usr/bin/python
#!-*-coding:utf-8-*-

class YsonType(object):
    def __init__(self, *kargs, **kwargs):
        self.attributes = {}

class YsonString(str, YsonType):
    pass

class YsonInteger(int, YsonType):
    pass

class YsonLongInteger(long, YsonType):
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

def convert_to_yson_type(value, attributes = None):
    if isinstance(value, basestring):
        result = YsonString(value)
    elif isinstance(value, int):
        result = YsonInteger(value)
    elif isinstance(value, long):
        result = YsonLongInteger(value)
    elif isinstance(value, float):
        result = YsonDouble(value)
    elif isinstance(value, list):
        result = YsonList(value)
    elif isinstance(value, dict):
        result = YsonMap(value)
    else:
        result = YsonEntity()
    if attributes is not None:
        result.attributes = attributes
    return result

def convert_to_yson_type_from_tree(tree):
    has_attrs = "$value" in tree
    value = tree["$value"] if has_attrs else tree
    if isinstance(value, basestring):
        result = YsonString(value)
    elif isinstance(value, int):
        result = YsonInteger(value)
    elif isinstance(value, long):
        result = YsonLongInteger(value)
    elif isinstance(value, float):
        result = YsonDouble(value)
    elif isinstance(value, list):
        result = map(convert_to_yson_type_from_tree, YsonList(value))
    elif isinstance(value, dict):
        result = YsonMap((k, convert_to_yson_type_from_tree(v)) for k, v in YsonMap(value).iteritems())
    if has_attrs and tree["$attributes"]:
        result.attributes = convert_to_yson_type_from_tree(tree["$attributes"])
    return result

