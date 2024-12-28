from .common import OrmReferencesTableCardinality

from google.protobuf import text_format, message as pb_message
from google.protobuf.descriptor import EnumValueDescriptor

from inflection import pluralize

from typing import Any, Callable, TYPE_CHECKING

from re import sub

if TYPE_CHECKING:
    from .reference import OrmReferencesTable

_TABLE_NAME_FORMATTER = None
_COLLECTIVE_NAME_FORMATTER = None
_COLLECTIVE_FOREIGN_NAME_FORMATTER = None
_FOREIGN_KEY_PREFIX_FORMATTER = None
_FOREIGN_VIEW_NAME_FORMATTER = None
_REFERENCE_FIELD_FORMATTER = None
_CUSTOM_FILTERS = {}


def set_table_name_formatter(formatter):
    global _TABLE_NAME_FORMATTER
    _TABLE_NAME_FORMATTER = formatter


def set_custom_filter(name: str, filter: Callable):
    global _CUSTOM_FILTERS
    _CUSTOM_FILTERS[name] = filter


def set_collective_name_formatter(formatter):
    global _COLLECTIVE_NAME_FORMATTER
    _COLLECTIVE_NAME_FORMATTER = formatter


def set_collective_foreign_name_formatter(formatter):
    global _COLLECTIVE_FOREIGN_NAME_FORMATTER
    _COLLECTIVE_FOREIGN_NAME_FORMATTER = formatter


def set_foreign_key_prefix_formatter(formatter):
    global _FOREIGN_KEY_PREFIX_FORMATTER
    _FOREIGN_KEY_PREFIX_FORMATTER = formatter


def set_foreign_view_name_formatter(formatter):
    global _FOREIGN_VIEW_NAME_FORMATTER
    _FOREIGN_VIEW_NAME_FORMATTER = formatter


def set_reference_field_name_formatter(formatter):
    global _REFERENCE_FIELD_FORMATTER
    _REFERENCE_FIELD_FORMATTER = formatter


def test_yields(callable, result, *args, **kwargs):
    return callable(*args, **kwargs) == result


def raise_helper(msg):
    raise Exception(msg)


def setup(env):
    env.filters["table_name"] = table_name
    env.filters["collective_name"] = collective_name
    env.filters["collective_foreign_name"] = collective_foreign_name
    env.filters["foreign_key_prefix"] = foreign_key_prefix
    env.filters["decapitalize"] = decapitalize
    env.filters["capitalize"] = capitalize
    env.filters["camel_to_snake_case"] = camel_to_snake_case
    env.filters["snake_to_camel_case"] = snake_to_camel_case
    env.filters["references_table_camel_case"] = references_table_camel_case
    env.filters["references_table_snake_case"] = references_table_snake_case
    env.filters["proto_to_cpp_namespace"] = proto_to_cpp_namespace
    env.filters["message_cpp_class_name"] = message_cpp_class_name
    env.filters["message_cpp_full_name"] = message_cpp_full_name
    env.filters["to_cpp_bool"] = to_cpp_bool
    env.filters["references_table_suffix_camel_case"] = references_table_suffix_camel_case
    env.filters["python_value_to_proto"] = python_value_to_proto
    env.filters["proto_message_to_text"] = proto_message_to_text
    env.filters["quote"] = quote
    env.filters["combine"] = combine

    for name, func in _CUSTOM_FILTERS.items():
        env.filters[name] = func

    env.tests["yields"] = test_yields

    env.globals["raise"] = raise_helper


def table_name(obj):
    """Given an object, return the table name on YT."""
    if _TABLE_NAME_FORMATTER:
        return _TABLE_NAME_FORMATTER(obj)
    return pluralize(obj.snake_case_name)


def collective_name(obj):
    """Given an object, return the type name."""
    if _COLLECTIVE_NAME_FORMATTER:
        return _COLLECTIVE_NAME_FORMATTER(obj)
    return pluralize(obj.camel_case_name)


def collective_foreign_name(obj):
    """Given an object, return the type name."""
    if _COLLECTIVE_FOREIGN_NAME_FORMATTER:
        return _COLLECTIVE_FOREIGN_NAME_FORMATTER(obj)
    return pluralize(obj.camel_case_foreign_name)


def foreign_key_prefix(obj):
    """Given an object, return the foreign key prefix for the object."""
    if _FOREIGN_KEY_PREFIX_FORMATTER:
        return _FOREIGN_KEY_PREFIX_FORMATTER(obj)
    return obj.snake_case_foreign_name


def decapitalize(word):
    """Given a word, return the word which first letter is in lower case."""
    return word[:1].lower() + word[1:]


def capitalize(word):
    return word if not word else (word[:1].upper() + word[1:])


def camel_to_snake_case(name: str) -> str:
    return sub(r"(?<!^)(?=[A-Z])", "_", name).lower()


def snake_case_from_parts(parts: list[str]) -> str:
    return "_".join(parts)


def camel_case_from_parts(parts: list[str]) -> str:
    return "".join([capitalize(part) for part in parts])


def snake_to_camel_case(name: str) -> str:
    return camel_case_from_parts(name.split("_"))


def references_table_suffix_snake_case(references_table: "OrmReferencesTable") -> str:
    return f"_for_{references_table.suffix_snake_case}" if references_table.suffix_snake_case else ""


def references_table_suffix_camel_case(references_table: "OrmReferencesTable") -> str:
    return f"For{snake_to_camel_case(references_table.suffix_snake_case)}" if references_table.suffix_snake_case else ""


def reference_target_camel_case_name(references_table: "OrmReferencesTable") -> str:
    if references_table.target_name_snake_case:
        name = snake_to_camel_case(references_table.target_name_snake_case)
        return pluralize(name) if references_table.source_cardinality == OrmReferencesTableCardinality.MANY else name
    return (
        collective_name(references_table.target)
        if references_table.source_cardinality == OrmReferencesTableCardinality.MANY
        else references_table.target.camel_case_name
    )


def reference_target_snake_case_name(references_table: "OrmReferencesTable") -> str:
    if references_table.target_name_snake_case:
        name = references_table.target_name_snake_case
        return pluralize(name) if references_table.source_cardinality == OrmReferencesTableCardinality.MANY else name
    return (
        table_name(references_table.target)
        if references_table.source_cardinality == OrmReferencesTableCardinality.MANY
        else references_table.target.snake_case_name
    )


def references_table_camel_case(references_table: "OrmReferencesTable") -> str:
    return (
        f"{reference_target_camel_case_name(references_table)}To{collective_name(references_table.source)}"
        f"{references_table_suffix_camel_case(references_table)}"
    )


def references_table_snake_case(references_table: "OrmReferencesTable"):
    return (
        f"{reference_target_snake_case_name(references_table)}_to_{table_name(references_table.source)}"
        f"{references_table_suffix_snake_case(references_table)}"
    )


def proto_to_cpp_namespace(proto_namespace: str) -> str:
    return proto_namespace.replace(".", "::")


def message_cpp_class_name(message: "OrmMessage") -> str:
    # See https://protobuf.dev/reference/cpp/cpp-generated/#nested-types.
    return message.middle_name.replace(".", "_")


def message_cpp_full_name(message: "OrmMessage") -> str:
    return "::".join((proto_to_cpp_namespace(message.proto_namespace), message_cpp_class_name(message)))


def to_cpp_bool(b: bool) -> str:
    return "true" if b else "false"


def python_value_to_proto(value: Any) -> str | list[str]:
    if value is True:
        return "true"
    if value is False:
        return "false"
    if isinstance(value, str):
        return f'"{value}"'
    if isinstance(value, list):
        return [python_value_to_proto(x) for x in value]
    if type(value).__name__ in ("RepeatedScalarContainer", "RepeatedCompositeContainer"):
        return python_value_to_proto([x for x in value])
    if isinstance(value, pb_message.Message):
        return "{{{}}}".format(text_format.MessageToString(value, as_one_line=True))
    if isinstance(value, EnumValueDescriptor):
        return f"{value.name}"
    return str(value)


def proto_message_to_text(message) -> str:
    return text_format.MessageToString(message)


def quote(value) -> str | list[str]:
    if isinstance(value, str):
        return f"\"{value}\""
    if hasattr(value, "__iter__"):
        return [f"\"{element}\"" for element in value]
    return f"\"{value}\""


def foreign_view_snake_case(foreign_key_field: "OrmField") -> str:
    if foreign_key_field.custom_foreign_view_name:
        return foreign_key_field.custom_foreign_view_name
    elif _FOREIGN_VIEW_NAME_FORMATTER is not None:
        return _FOREIGN_VIEW_NAME_FORMATTER(foreign_key_field)
    else:
        return foreign_key_field.snake_case_name + "_view"


def reference_field_camel_case(reference_field: "OrmField") -> str:
    if _REFERENCE_FIELD_FORMATTER is not None:
        return _REFERENCE_FIELD_FORMATTER(reference_field)
    return reference_field.camel_case_name


def combine(d1: dict, d2: dict) -> dict:
    return {**d1, **d2}
