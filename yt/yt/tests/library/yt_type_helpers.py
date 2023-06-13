import yt.yson as yson

import copy


def make_column(name, type_v3, **attributes):
    result = {
        "name": name,
        "type_v3": type_v3,
    }
    for k in attributes:
        result[k] = attributes[k]
    return result


def make_deleted_column(stable_name):
    return {
        "stable_name": stable_name,
        "deleted": True
    }


def make_sorted_column(name, type_v3, **attributes):
    return make_column(name, type_v3, sort_order="ascending", **attributes)


def make_schema(columns, **attributes):
    schema = yson.YsonList()
    for column_schema in columns:
        column_schema = column_schema.copy()
        schema.append(column_schema)
    for attr, value in list(attributes.items()):
        schema.attributes[attr] = value
    return schema


def normalize_schema(schema):
    """Remove 'type_v2' / 'type_v3' field from schema, useful for schema comparison."""
    result = copy.deepcopy(schema)
    for column in result:
        column.pop("type_v2", None)
        column.pop("type_v3", None)
    return result


def normalize_schema_v3(schema):
    """Remove "type" / "required" / "type_v2" fields from schema, useful for schema comparison."""
    result = copy.deepcopy(schema)
    for column in result:
        for f in ["type", "required", "type_v2"]:
            if f in column:
                del column[f]
    return result


def optional_type(element_type):
    return {
        "type_name": "optional",
        "item": element_type,
    }


def make_struct_members(fields):
    result = []
    for name, type in fields:
        result.append(
            {
                "name": name,
                "type": type,
            }
        )
    return result


def make_tuple_elements(elements):
    result = []
    for type in elements:
        result.append(
            {
                "type": type,
            }
        )
    return result


def struct_type(fields):
    """
    Create yson description of struct type.
    fields is a list of (name, type) pairs.
    """
    result = {
        "type_name": "struct",
        "members": make_struct_members(fields),
    }
    return result


def list_type(element_type):
    return {
        "type_name": "list",
        "item": element_type,
    }


def tuple_type(elements):
    return {
        "type_name": "tuple",
        "elements": make_tuple_elements(elements),
    }


def variant_struct_type(fields):
    result = {
        "type_name": "variant",
        "members": make_struct_members(fields),
    }
    return result


def variant_tuple_type(elements):
    return {"type_name": "variant", "elements": make_tuple_elements(elements)}


def dict_type(key_type, value_type):
    return {
        "type_name": "dict",
        "key": key_type,
        "value": value_type,
    }


def tagged_type(tag, element_type):
    return {
        "type_name": "tagged",
        "tag": tag,
        "item": element_type,
    }


def decimal_type(precision, scale):
    return {
        "type_name": "decimal",
        "precision": precision,
        "scale": scale,
    }
