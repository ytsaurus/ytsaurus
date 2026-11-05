import decimal
import logging
from io import BytesIO
from typing import Any, List, Optional, Set, Union

import httpx
import referencing
from jsonschema import ValidationError, validate
from referencing import Registry, Resource
from referencing._core import Resolver

from confluent_kafka.schema_registry import RuleKind
from confluent_kafka.schema_registry.serde import FieldTransform, FieldType, RuleConditionError, RuleContext

__all__ = [
    'JsonMessage',
    'JsonSchema',
    'DEFAULT_SPEC',
    '_retrieve_via_httpx',
    'transform',
    '_transform_field',
    '_validate_subschemas',
    'get_type',
    '_disjoint',
    'get_inline_tags',
    '_json_loads',
    '_json_dumps',
    '_HAS_ORJSON',
]

# JSON codec: prefer orjson for speed, but fall back to the stdlib json module
# when orjson is unavailable (e.g. on free-threaded CPython builds that do not
# yet have orjson wheels). Both implementations accept str/bytes/bytearray for
# loads and return a str from dumps. The stdlib fallback mirrors orjson's wire
# output (compact separators, non-ASCII preserved) so serialized bytes stay
# consistent.
#
# Catch any exception (not just ImportError): orjson is a compiled extension and
# may be present yet fail to import/initialize (ABI mismatch, broken shared lib,
# init error raising OSError/RuntimeError/etc.). In that case we still degrade to
# the stdlib codec rather than letting the failure break this module -- and with
# it all JSON (de)serialization. Note `except Exception` deliberately does not
# catch KeyboardInterrupt/SystemExit (those are BaseException).
try:
    import orjson

    _HAS_ORJSON = True

    def _json_loads(data: Union[str, bytes, bytearray]) -> Any:
        return orjson.loads(data)

    def _json_dumps(obj: Any) -> str:
        return orjson.dumps(obj).decode("utf-8")

except Exception as _orjson_exc:
    if not isinstance(_orjson_exc, ImportError):
        # Absence (ImportError) is the normal optional-dependency case and is
        # silent; a present-but-broken orjson is unexpected, so surface it.
        logging.getLogger(__name__).warning(
            "orjson is installed but failed to import; falling back to the " "stdlib json module",
            exc_info=True,
        )

    import json as _stdlib_json

    _HAS_ORJSON = False

    def _json_loads(data: Union[str, bytes, bytearray]) -> Any:
        # json.loads accepts bytes/bytearray (auto-detecting the encoding)
        # since Python 3.6, matching orjson.loads.
        return _stdlib_json.loads(data)

    def _json_dumps(obj: Any) -> str:
        return _stdlib_json.dumps(obj, separators=(",", ":"), ensure_ascii=False)


JSON_TYPE = "JSON"

JsonMessage = Union[
    None,  # 'null' Avro type
    str,  # 'string' and 'enum'
    float,  # 'float' and 'double'
    int,  # 'int' and 'long'
    decimal.Decimal,  # 'fixed'
    bool,  # 'boolean'
    list,  # 'array'
    dict,  # 'map' and 'record'
]

JsonSchema = Union[bool, dict]

DEFAULT_SPEC = referencing.jsonschema.DRAFT7  # type: ignore[attr-defined]

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


def _retrieve_via_httpx(uri: str):
    response = httpx.get(uri)
    return Resource.from_contents(response.json(), default_specification=DEFAULT_SPEC)


def transform(
    ctx: RuleContext,
    schema: JsonSchema,
    ref_registry: Registry,
    ref_resolver: Resolver,
    path: str,
    message: JsonMessage,
    field_transform: FieldTransform,
) -> Optional[JsonMessage]:
    # Only proceed to transform the message if schema is of dict type
    if message is None or schema is None or isinstance(schema, bool):
        return message

    field_ctx = ctx.current_field()
    if field_ctx is not None:
        field_ctx.field_type = get_type(schema)
    original_type = schema.get("type")
    if isinstance(original_type, list) and len(original_type) > 0:
        subschema = _validate_subtypes(schema, message, ref_registry)
        try:
            if subschema is not None:
                return transform(ctx, subschema, ref_registry, ref_resolver, path, message, field_transform)
        finally:
            schema["type"] = original_type  # restore original type
    all_of = schema.get("allOf")
    any_of = schema.get("anyOf")
    one_of = schema.get("oneOf")
    if all_of is not None or any_of is not None or one_of is not None:
        if all_of is not None:
            for subschema in all_of:
                message = transform(ctx, subschema, ref_registry, ref_resolver, path, message, field_transform)
        elif one_of is not None:
            for subschema in one_of:
                resolved = _validate_subschema(subschema, message, ref_registry, ref_resolver)
                if resolved is not None:
                    message = transform(ctx, resolved, ref_registry, ref_resolver, path, message, field_transform)
                    break
        elif any_of is not None:
            for subschema in any_of:
                resolved = _validate_subschema(subschema, message, ref_registry, ref_resolver)
                if resolved is not None:
                    message = transform(ctx, resolved, ref_registry, ref_resolver, path, message, field_transform)
        # Also visit sibling properties/items at this level
        # (siblings to allOf/anyOf/oneOf).
        props = schema.get("properties")
        if props is not None and isinstance(message, dict):
            for prop_name, prop_schema in props.items():
                if isinstance(prop_schema, dict):
                    _transform_field(
                        ctx, path, prop_name, message, prop_schema, ref_registry, ref_resolver, field_transform
                    )
        items = schema.get("items")
        if items is not None and isinstance(message, list):
            message = [
                transform(ctx, items, ref_registry, ref_resolver, path, item, field_transform) for item in message
            ]
        return message
    items = schema.get("items")
    if items is not None:
        if isinstance(message, list):
            return [transform(ctx, items, ref_registry, ref_resolver, path, item, field_transform) for item in message]
    ref = schema.get("$ref")
    if ref is not None:
        ref_schema = ref_resolver.lookup(ref)
        return transform(ctx, ref_schema.contents, ref_registry, ref_resolver, path, message, field_transform)

    schema_type = get_type(schema)
    if schema_type == FieldType.RECORD:
        props = schema.get("properties")
        if not isinstance(message, dict):
            log.warning("Incompatible message type for record schema")
            return message
        if props is not None:
            for prop_name, prop_schema in props.items():
                if isinstance(prop_schema, dict):
                    _transform_field(
                        ctx, path, prop_name, message, prop_schema, ref_registry, ref_resolver, field_transform
                    )
        return message
    if schema_type in (FieldType.ENUM, FieldType.STRING, FieldType.INT, FieldType.DOUBLE, FieldType.BOOLEAN):
        if field_ctx is not None:
            rule_tags = ctx.rule.tags
            if not rule_tags or not _disjoint(set(rule_tags), field_ctx.tags):
                return field_transform(ctx, field_ctx, message)
    return message


def _transform_field(
    ctx: RuleContext,
    path: str,
    prop_name: str,
    message: dict,
    prop_schema: dict,
    ref_registry: Registry,
    ref_resolver: Resolver,
    field_transform: FieldTransform,
):
    full_name = path + "." + prop_name
    try:
        ctx.enter_field(message, full_name, prop_name, get_type(prop_schema), get_inline_tags(prop_schema))
        value = message.get(prop_name)
        if value is not None:
            new_value = transform(ctx, prop_schema, ref_registry, ref_resolver, full_name, value, field_transform)
            if ctx.rule.kind == RuleKind.CONDITION:
                if new_value is False:
                    raise RuleConditionError(ctx.rule)
            else:
                message[prop_name] = new_value
    finally:
        ctx.exit_field()


def _validate_subtypes(schema: dict, message: JsonMessage, registry: Registry) -> Optional[JsonSchema]:
    """
    Validate the message against the subtypes.
    Args:
        schema: The schema to validate the message against.
        message: The message to validate.
        registry: The registry to use for the validation.
    Returns:
        The validated schema if the message is valid against the subtypes, otherwise None.
    """
    schema_type = schema.get("type")
    if not isinstance(schema_type, list) or len(schema_type) == 0:
        return None
    for typ in schema_type:
        schema["type"] = typ
        try:
            validate(instance=message, schema=schema, registry=registry)
            return schema
        except ValidationError:
            pass
    return None


def _validate_subschemas(
    subschemas: List[JsonSchema],
    message: JsonMessage,
    registry: Registry,
    resolver: Resolver,
) -> Optional[JsonSchema]:
    """
    Validate the message against the subschemas.
    Args:
        subschemas: The list of subschemas to validate the message against.
        message: The message to validate.
        registry: The registry to use for the validation.
        resolver: The resolver to use for the validation.
    Returns:
        The validated schema if the message is valid against the subschemas, otherwise None.
    """
    for subschema in subschemas:
        resolved = _validate_subschema(subschema, message, registry, resolver)
        if resolved is not None:
            return resolved
    return None


def _validate_subschema(
    subschema: JsonSchema,
    message: JsonMessage,
    registry: Registry,
    resolver: Resolver,
) -> Optional[JsonSchema]:
    """
    Validate the message against a single subschema.
    Returns the resolved subschema (with $ref followed) if valid, otherwise None.
    """
    if not isinstance(subschema, dict):
        return None
    try:
        ref = subschema.get("$ref")
        if ref is not None:
            resolved = resolver.lookup(ref)
            subschema = resolved.contents
            # Pass _resolver (not resolver) to use the new referencing library's
            # Resolver with correct context for nested $ref resolution
            validate(instance=message, schema=subschema, registry=registry, _resolver=resolved.resolver)
        else:
            validate(instance=message, schema=subschema, registry=registry)
        return subschema
    except ValidationError:
        return None


def get_type(schema: JsonSchema) -> FieldType:
    if isinstance(schema, bool):
        return FieldType.COMBINED

    schema_type = schema.get("type")
    if schema.get("const") is not None or schema.get("enum") is not None:
        return FieldType.ENUM
    if schema_type == "object":
        props = schema.get("properties")
        if not props:
            return FieldType.MAP
        return FieldType.RECORD
    if schema_type == "array":
        return FieldType.ARRAY
    if schema_type == "string":
        return FieldType.STRING
    if schema_type == "integer":
        return FieldType.INT
    if schema_type == "number":
        return FieldType.DOUBLE
    if schema_type == "boolean":
        return FieldType.BOOLEAN
    if schema_type == "null":
        return FieldType.NULL

    props = schema.get("properties")
    if props is not None:
        return FieldType.RECORD

    return FieldType.NULL


def _disjoint(tags1: Set[str], tags2: Set[str]) -> bool:
    for tag in tags1:
        if tag in tags2:
            return False
    return True


def get_inline_tags(schema: dict) -> Set[str]:
    tags = schema.get("confluent:tags")
    if tags is None:
        return set()
    else:
        return set(tags)
