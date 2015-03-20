from yt.yson.yson_types import *

from common import YsonError

def to_yson_type(value, attributes = None):
    """ Wrap value with YSON type """
    if isinstance(value, unicode):
        result = YsonString(str(bytearray(value, 'utf-8')))
    if isinstance(value, str):
        result = YsonString(value)
    elif value is False or value is True:
        return YsonBoolean(value)
    elif isinstance(value, (int, long)):
        if value < -2 ** 63 or value >= 2 ** 64:
            raise TypeError("Integer {0} cannot be represented in YSON "
                            "since it is out of range [-2^63, 2^64 - 1])".format(value))
        greater_than_max_int64 = value >= 2 ** 63
        if greater_than_max_int64 or isinstance(value, YsonUint64):
            return YsonUint64(value)
        else:
            return YsonInt64(value)
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

def json_to_yson(json_tree):
    """ Converts json representation to YSON representation """
    has_attrs = isinstance(json_tree, dict) and "$value" in json_tree
    value = json_tree["$value"] if has_attrs else json_tree
    if isinstance(value, unicode):
        result = YsonString(str(bytearray(value, 'utf-8')))
    elif isinstance(value, str):
        result = YsonString(value)
    elif value is False or value is True:
        result = YsonBoolean(value)
    elif isinstance(value, int):
        result = YsonInt64(value)
    elif isinstance(value, long):
        result = YsonUint64(value)
    elif isinstance(value, float):
        result = YsonDouble(value)
    elif isinstance(value, list):
        result = YsonList(map(json_to_yson, value))
    elif isinstance(value, dict):
        result = YsonMap((json_to_yson(k), json_to_yson(v)) for k, v in YsonMap(value).iteritems())
    elif value is None:
        result = YsonEntity()
    else:
        raise YsonError("Unknown type:", type(value))

    if has_attrs and json_tree["$attributes"]:
        result.attributes = json_to_yson(json_tree["$attributes"])
    return result

def yson_to_json(yson_tree, print_attributes=True):
    def process_dict(d):
        return dict((k, yson_to_json(v)) for k, v in d.iteritems())

    if hasattr(yson_tree, "attributes") and yson_tree.attributes and print_attributes:
        return {"$attributes": process_dict(yson_tree.attributes),
                "$value": yson_to_json(yson_tree, print_attributes=False)}
    if isinstance(yson_tree, list):
        return map(yson_to_json, yson_tree)
    elif isinstance(yson_tree, dict):
        return process_dict(yson_tree)
    elif isinstance(yson_tree, YsonEntity):
        return None
    elif isinstance(yson_tree, bool):
        return "true" if yson_tree else "false"
    else:
        bases = type(yson_tree).__bases__
        if YsonType in bases:
            other = list(set(bases) - set([YsonType]))[0]
            return other(yson_tree)
        return yson_tree

