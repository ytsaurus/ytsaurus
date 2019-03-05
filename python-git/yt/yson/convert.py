from .yson_types import *
from .common import YsonError

from yt.packages.six import text_type, binary_type, integer_types, iteritems, PY3
from yt.packages.six.moves import map as imap

def to_yson_type(value, attributes=None, always_create_attributes=True):
    """Wraps value with YSON type."""
    if not always_create_attributes and attributes is None:
        if isinstance(value, text_type) and not PY3:
            return value.encode("utf-8")
        return value

    if isinstance(value, text_type):
        if PY3:
            result = YsonUnicode(value)
        else:  # COMPAT
            result = YsonString(value.encode("utf-8"))
    elif isinstance(value, binary_type):
        result = YsonString(value)
    elif value is False or value is True:
        result = YsonBoolean(value)
    elif isinstance(value, integer_types):
        if value < -2 ** 63 or value >= 2 ** 64:
            raise TypeError("Integer {0} cannot be represented in YSON "
                            "since it is out of range [-2^63, 2^64 - 1])".format(value))
        greater_than_max_int64 = value >= 2 ** 63
        if greater_than_max_int64 or isinstance(value, YsonUint64):
            result = YsonUint64(value)
        else:
            result = YsonInt64(value)
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
    else:
        result.attributes = {}

    return result

def json_to_yson(json_tree, encode_key=False, encoding=None):
    """Converts json representation to YSON representation."""
    if encode_key:
        if json_tree.startswith("$"):
            if not json_tree.startswith("$$"):
                raise YsonError("Keys should not start with single dollar sign")
            json_tree = json_tree[1:]
    has_attrs = isinstance(json_tree, dict) and "$value" in json_tree
    value = json_tree["$value"] if has_attrs else json_tree
    if isinstance(value, text_type):
        encoding = "utf-8" if encoding is None else encoding
        if PY3:
            result = YsonUnicode(value)
        else:  # COMPAT
            result = YsonString(value.encode(encoding))
    elif isinstance(value, binary_type):
        result = YsonString(value)
    elif value is False or value is True:
        result = YsonBoolean(value)
    elif isinstance(value, integer_types):
        greater_than_max_int64 = value >= 2 ** 63
        if greater_than_max_int64:
            result = YsonUint64(value)
        else:
            result = YsonInt64(value)
    elif isinstance(value, float):
        result = YsonDouble(value)
    elif isinstance(value, list):
        result = YsonList(imap(lambda item: json_to_yson(item, encoding=encoding), value))
    elif isinstance(value, dict):
        result = YsonMap((json_to_yson(k, True, encoding=encoding), json_to_yson(v, encoding=encoding)) for k, v in iteritems(YsonMap(value)))
    elif value is None:
        result = YsonEntity()
    else:
        raise YsonError("Unknown type:", type(value))

    if has_attrs and json_tree["$attributes"]:
        result.attributes = json_to_yson(json_tree["$attributes"], encoding=encoding)
    return result

def yson_to_json(yson_tree, print_attributes=True, encoding=None):
    def fix_key(key):
        if key and key[0] == "$":
            return "$" + key
        return key

    def process_dict(d):
        return dict((fix_key(yson_to_json(k)), yson_to_json(v)) for k, v in iteritems(d))

    if hasattr(yson_tree, "attributes") and yson_tree.attributes and print_attributes:
        return {"$attributes": process_dict(yson_tree.attributes),
                "$value": yson_to_json(yson_tree, print_attributes=False)}
    if isinstance(yson_tree, list):
        return list(imap(yson_to_json, yson_tree))
    elif isinstance(yson_tree, dict):
        return process_dict(yson_tree)
    elif isinstance(yson_tree, YsonEntity):
        return None
    elif PY3 and (isinstance(yson_tree, YsonString) or isinstance(yson_tree, binary_type)):
        return yson_tree.decode("utf-8" if encoding is None else encoding)
    elif isinstance(yson_tree, bool) or isinstance(yson_tree, YsonBoolean):
        return True if yson_tree else False
    else:
        if type(yson_tree) is YsonEntity:
            return None

        bases = type(yson_tree).__bases__
        iter = 0
        while len(bases) == 1 and YsonType not in bases:
            bases = bases[0].__bases__
            iter += 1

        if YsonType in bases:
            other = list(set(bases) - set([YsonType]))[0]
            return other(yson_tree)
        return yson_tree

