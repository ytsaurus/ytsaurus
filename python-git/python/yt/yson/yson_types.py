#!/usr/bin/python
#!-*-coding:utf-8-*-

import copy
import sys
from itertools import imap

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
    if isinstance(value, unicode):
        result = YsonString(str(bytearray(value, 'utf-8')))
    if isinstance(value, str):
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

def convert_to_yson_tree(json_tree):
    has_attrs = hasattr(json_tree, "__contains__") and "$value" in json_tree
    value = json_tree["$value"] if has_attrs else json_tree
    if isinstance(value, unicode):
        result = YsonString(str(bytearray(value, 'utf-8')))
    elif isinstance(value, str):
        result = YsonString(value)
    elif isinstance(value, int):
        result = YsonInteger(value)
    elif isinstance(value, long):
        result = YsonLongInteger(value)
    elif isinstance(value, float):
        result = YsonDouble(value)
    elif isinstance(value, list):
        result = YsonList(map(convert_to_yson_tree, value))
    elif isinstance(value, dict):
        result = YsonMap((k, convert_to_yson_tree(v)) for k, v in YsonMap(value).iteritems())
    elif value is None:
        result = YsonEntity()
    else:
        print >>sys.stderr, "Unknown type:", type(value)

    if has_attrs and json_tree["$attributes"]:
        result.attributes = convert_to_yson_tree(json_tree["$attributes"])
    return result

def convert_to_json_tree(yson_tree, print_attributes=True):
    if yson_tree.attributes and print_attributes:
        return {"$attributes": yson_tree.attributes,
                "$value": convert_to_json_tree(yson_tree, print_attributes=False)}
    if isinstance(yson_tree, YsonList):
        return map(convert_to_json_tree, yson_tree)
    elif isinstance(yson_tree, YsonMap):
        return dict((k, convert_to_json_tree(v)) for k, v in yson_tree.iteritems())
    elif isinstance(yson_tree, YsonEntity):
        return None
    else:
        bases = type(yson_tree).__bases__
        if YsonType in bases:
            other = list(set(bases) - set([YsonType]))[0]
            return other(yson_tree)
        return yson_tree

def simplify(tree):
    if isinstance(tree, dict):
        return YsonMap((k, simplify(v)) for k, v in tree.iteritems())
    elif isinstance(tree, list):
        return YsonList(imap(simplify, tree))
    elif isinstance(tree, YsonEntity):
        return None
    else:
        return copy.deepcopy(tree)


