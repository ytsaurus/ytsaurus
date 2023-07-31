from .constants import YSON_PACKAGE_INSTALLATION_TEXT
from .errors import YtError

try:
    from yt.skiff import SkiffRecord, SkiffSchema, SkiffTableSwitch, SkiffOtherColumns # noqa
except ImportError:
    pass

import yt.skiff


def check_skiff_bindings():
    if not yt.skiff.AVAILABLE:
        raise YtError('Skiff bindings are not available. '
                      'Try to use other format or install bindings. '
                      'Bindings are shipped as additional package and '
                      'can be installed ' + YSON_PACKAGE_INSTALLATION_TEXT)


def load(*args, **kwargs):
    check_skiff_bindings()
    return yt.skiff.load(*args, **kwargs)


def loads(*args, **kwargs):
    check_skiff_bindings()
    return yt.skiff.loads(*args, **kwargs)


def dump(*args, **kwargs):
    check_skiff_bindings()
    return yt.skiff.dump(*args, **kwargs)


def dumps(*args, **kwargs):
    check_skiff_bindings()
    return yt.skiff.dumps(*args, **kwargs)


def load_structured(*args, **kwargs):
    check_skiff_bindings()
    return yt.skiff.load_structured(*args, **kwargs)


def dump_structured(*args, **kwargs):
    check_skiff_bindings()
    return yt.skiff.dump_structured(*args, **kwargs)


def _convert_to_skiff_type(type_name):
    types_dict = {
        "string": "string32",
        "any": "yson32",
        "int64": "int64",
        "uint64": "uint64",
        "double": "double",
        "boolean": "boolean"
    }
    return types_dict[type_name]


def convert_to_skiff_schema(table_schema):
    """Converts table schema to skiff format schema."""
    strict = getattr(table_schema, "attributes", {}).get("strict", False)
    children = []
    for field in table_schema:
        wire_type = _convert_to_skiff_type(field["type"])
        required = field.get("required", False)
        if required:
            child = {
                "wire_type": wire_type,
                "name": field["name"]
            }
        else:
            child = {
                "wire_type": "variant8",
                "children": [
                    {
                        "wire_type": "nothing"
                    },
                    {
                        "wire_type": wire_type
                    }
                ],
                "name": field["name"]
            }
        children.append(child)

    if not strict:
        children.append({
            "wire_type": "yson32",
            "name": "$other_columns"
        })

    return {
        "wire_type": "tuple",
        "children": children,
    }
