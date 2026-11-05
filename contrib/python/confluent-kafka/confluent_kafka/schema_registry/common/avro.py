import decimal
import json
import logging
import re
from collections import defaultdict
from copy import deepcopy
from io import BytesIO
from typing import Dict, Optional, Set, Tuple, Union, cast

from fastavro import repository, validate
from fastavro.schema import load_schema

from confluent_kafka.schema_registry.serde import FieldTransform, FieldType, RuleConditionError, RuleContext

from .schema_registry_client import RuleKind, Schema

__all__ = [
    'AvroMessage',
    'AvroSchema',
    '_schema_loads',
    'LocalSchemaRepository',
    'parse_schema_with_repo',
    'transform',
    '_transform_field',
    'get_type',
    '_disjoint',
    '_resolve_union',
    '_collapse_schema',
    'get_inline_tags',
    '_get_inline_tags_recursively',
    '_implied_namespace',
]

AVRO_TYPE = "AVRO"

AvroMessage = Union[
    None,  # 'null' Avro type
    str,  # 'string' and 'enum'
    float,  # 'float' and 'double'
    int,  # 'int' and 'long'
    decimal.Decimal,  # 'fixed'
    bool,  # 'boolean'
    bytes,  # 'bytes'
    list,  # 'array'
    dict,  # 'map' and 'record'
    tuple,  # wrapped union type
]
AvroSchema = Union[str, list, dict]

log = logging.getLogger(__name__)


class _ContextStringIO(BytesIO):
    """
    Wrapper to allow use of StringIO via 'with' constructs.
    """

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
        return False


def _schema_loads(schema_str: str) -> Schema:
    """
    Instantiate a Schema instance from a declaration string.

    Args:
        schema_str (str): Avro Schema declaration.

    .. _Schema declaration:
        https://avro.apache.org/docs/current/spec.html#schemas

    Returns:
        Schema: A Schema instance.
    """

    schema_str = schema_str.strip()

    # canonical form primitive declarations are not supported
    if schema_str[0] != "{" and schema_str[0] != "[":
        schema_str = '{"type":' + schema_str + '}'

    return Schema(schema_str, schema_type='AVRO')


class LocalSchemaRepository(repository.AbstractSchemaRepository):
    def __init__(self, schemas):
        self.schemas = schemas

    def load(self, subject):
        return self.schemas.get(subject)


def parse_schema_with_repo(schema_str: str, named_schemas: Dict[str, AvroSchema]) -> AvroSchema:
    copy = deepcopy(named_schemas)
    copy["$root"] = json.loads(schema_str)
    repo = LocalSchemaRepository(copy)
    return load_schema("$root", repo=repo)


def transform(
    ctx: RuleContext, schema: AvroSchema, message: AvroMessage, field_transform: FieldTransform
) -> AvroMessage:
    if message is None or schema is None:
        return message
    field_ctx = ctx.current_field()
    if field_ctx is not None:
        field_ctx.field_type = get_type(schema)
    if isinstance(schema, list):
        subschema, submessage = _resolve_union(schema, message)
        if subschema is None:
            return message
        submessage = transform(ctx, subschema, submessage, field_transform)
        if isinstance(message, tuple) and len(message) == 2:
            return (message[0], submessage)
        return submessage
    elif isinstance(schema, dict):
        schema_type = schema.get("type")
        if schema_type == 'array':
            if not isinstance(message, list):
                log.warning("Incompatible message type for array schema")
                return message
            return [transform(ctx, schema["items"], item, field_transform) for item in message]
        elif schema_type == 'map':
            if not isinstance(message, dict):
                log.warning("Incompatible message type for map schema")
                return message
            return {key: transform(ctx, schema["values"], value, field_transform) for key, value in message.items()}
        elif schema_type == 'record':
            if not isinstance(message, dict):
                log.warning("Incompatible message type for record schema")
                return message
            fields = schema["fields"]
            for field in fields:
                if field["name"] not in message:
                    continue
                _transform_field(ctx, schema, field, message, field_transform)
            return message

    if field_ctx is not None:
        rule_tags = ctx.rule.tags
        if not rule_tags or not _disjoint(set(rule_tags), field_ctx.tags):
            return field_transform(ctx, field_ctx, message)
    return message


def _transform_field(ctx: RuleContext, schema: dict, field: dict, message: dict, field_transform: FieldTransform):
    field_type = field["type"]
    name = field["name"]
    full_name = schema["name"] + "." + name
    try:
        ctx.enter_field(message, full_name, name, get_type(field_type), None)
        value = message[name]
        new_value = transform(ctx, field_type, value, field_transform)
        if ctx.rule.kind == RuleKind.CONDITION:
            if new_value is False:
                raise RuleConditionError(ctx.rule)
        else:
            message[name] = new_value
    finally:
        ctx.exit_field()


def get_type(schema: AvroSchema) -> FieldType:
    if isinstance(schema, list):
        return FieldType.COMBINED
    elif isinstance(schema, dict):
        schema_type = schema.get("type")
    else:
        # string schemas; this could be either a named schema or a primitive type
        schema_type = schema

    if schema_type == 'record':
        return FieldType.RECORD
    elif schema_type == 'enum':
        return FieldType.ENUM
    elif schema_type == 'array':
        return FieldType.ARRAY
    elif schema_type == 'map':
        return FieldType.MAP
    elif schema_type == 'union':
        return FieldType.COMBINED
    elif schema_type == 'fixed':
        return FieldType.FIXED
    elif schema_type == 'string':
        return FieldType.STRING
    elif schema_type == 'bytes':
        return FieldType.BYTES
    elif schema_type == 'int':
        return FieldType.INT
    elif schema_type == 'long':
        return FieldType.LONG
    elif schema_type == 'float':
        return FieldType.FLOAT
    elif schema_type == 'double':
        return FieldType.DOUBLE
    elif schema_type == 'boolean':
        return FieldType.BOOLEAN
    elif schema_type == 'null':
        return FieldType.NULL
    else:
        return FieldType.NULL


def _disjoint(tags1: Set[str], tags2: Set[str]) -> bool:
    for tag in tags1:
        if tag in tags2:
            return False
    return True


def _resolve_union(schema: AvroSchema, message: AvroMessage) -> Tuple[Optional[AvroSchema], AvroMessage]:
    is_wrapped_union = isinstance(message, tuple) and len(message) == 2
    is_typed_union = isinstance(message, dict) and '-type' in message
    for subschema in schema:
        try:
            if is_wrapped_union:
                if isinstance(subschema, dict):
                    dict_schema = cast(dict, subschema)
                    tuple_message = cast(tuple, message)
                    if dict_schema["name"] == tuple_message[0]:
                        return (dict_schema, tuple_message[1])
            elif is_typed_union:
                if isinstance(subschema, dict):
                    dict_schema = cast(dict, subschema)
                    dict_message = cast(dict, message)
                    if dict_schema["name"] == dict_message['-type']:
                        return (dict_schema, dict_message)
            else:
                validate(message, _collapse_schema(deepcopy(subschema)))
                return (subschema, message)
        except:  # noqa: E722
            continue
    return (None, message)


def _collapse_schema(schema: AvroSchema, encountered_references=None) -> AvroSchema:
    """
    Collapses a schema to conform to the Avro specification if it has been previously expanded.
    Recursively replaces record, fixed, or enum definitions with their name when they have been already defined.
    Mutates the incoming schema.

    Args:
        schema: An (expanded) Avro schema
        encountered_references: A list of encountered references (used in the recursion)

    Returns:
        AvroSchema: A collapsed Avro schema.
    """
    if encountered_references is None:
        encountered_references = []
    if isinstance(schema, str):
        return schema
    elif isinstance(schema, list):
        return [_collapse_schema(subschema, encountered_references) for subschema in schema]
    elif isinstance(schema, dict):
        schema_type = schema.get("type")
        if schema_type == 'array':
            schema["items"] = _collapse_schema(schema['items'], encountered_references)
            return schema
        elif schema_type == 'map':
            schema["values"] = _collapse_schema(schema["values"], encountered_references)
            return schema
        elif schema_type == "record":
            name = schema.get("name")
            namespace = schema.get("namespace")
            full_name = name if namespace is None or (name is not None and '.' in name) else f"{namespace}.{name}"
            if full_name in encountered_references:
                return schema["name"]
            encountered_references.append(full_name)
            if schema.get("aliases") is not None:
                for alias in schema["aliases"]:
                    full_alias = alias if namespace is None or '.' in alias else f"{namespace}.{alias}"
                    encountered_references.append(full_alias)
            schema["fields"] = _collapse_schema(schema["fields"], encountered_references)
            return schema
        elif schema_type == "fixed" or schema_type == "enum":
            if schema["name"] in encountered_references:
                return schema["name"]
            encountered_references.append(schema["name"])
            if schema.get("aliases") is not None:
                for alias in schema["aliases"]:
                    encountered_references.append(alias)
            return schema
        schema["type"] = _collapse_schema(schema["type"], encountered_references)
        return schema
    return schema


def get_inline_tags(schema: AvroSchema) -> Dict[str, Set[str]]:
    inline_tags: Dict[str, Set[str]] = defaultdict(set)
    _get_inline_tags_recursively('', '', schema, inline_tags)
    return inline_tags


def _get_inline_tags_recursively(ns: str, name: str, schema: Optional[AvroSchema], tags: Dict[str, Set[str]]):
    if schema is None:
        return
    if isinstance(schema, list):
        for subschema in schema:
            _get_inline_tags_recursively(ns, name, subschema, tags)
    elif not isinstance(schema, dict):
        # string schemas; this could be either a named schema or a primitive type
        return
    else:
        schema_type = schema.get("type")
        if schema_type == 'array':
            _get_inline_tags_recursively(ns, name, schema.get("items"), tags)
        elif schema_type == 'map':
            _get_inline_tags_recursively(ns, name, schema.get("values"), tags)
        elif schema_type == 'record':
            record_ns = schema.get("namespace")
            record_name = schema.get("name")
            if record_ns is None:
                record_ns = _implied_namespace(name)
            if record_ns is None:
                record_ns = ns
            # Ensure record_name is not None and doesn't already have namespace prefix
            if record_name is not None and record_ns != '' and not record_name.startswith(record_ns):
                record_name = f"{record_ns}.{record_name}"
            fields = schema["fields"]
            for field in fields:
                field_tags = field.get("confluent:tags")
                field_name = field.get("name")
                field_type = field.get("type")
                # Ensure all required fields are present before building tag key
                if field_tags is not None and field_name is not None and record_name is not None:
                    tags[record_name + '.' + field_name].update(field_tags)
                if field_type is not None and record_name is not None:
                    _get_inline_tags_recursively(record_ns, record_name, field_type, tags)


def _implied_namespace(name: str) -> Optional[str]:
    match = re.match(r"^(.*)\.[^.]+$", name)
    return match.group(1) if match else None
