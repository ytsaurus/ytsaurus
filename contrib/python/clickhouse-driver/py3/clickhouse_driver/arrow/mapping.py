import json

import pyarrow as pa

from ..columns.util import (
    get_inner_columns, get_inner_columns_with_types, get_inner_spec
)

SIMPLE_TYPES = {
    'Int8': pa.int8(),
    'Int16': pa.int16(),
    'Int32': pa.int32(),
    'Int64': pa.int64(),
    'UInt8': pa.uint8(),
    'UInt16': pa.uint16(),
    'UInt32': pa.uint32(),
    'UInt64': pa.uint64(),
    'Float32': pa.float32(),
    'Float64': pa.float64(),
    'Date': pa.date32(),
    'Date32': pa.date32(),
    'Bool': pa.bool_(),
    'Nothing': pa.null()
}

# Types represented by rich Python objects are stringified.
STRINGIFIED_TYPES = {'UUID', 'IPv4', 'IPv6'}

DECIMAL_PRECISIONS = {
    'Decimal32': 9,
    'Decimal64': 18,
    'Decimal128': 38,
    'Decimal256': 76
}


#: Types with no default Arrow representation: an explicit
#: ``arrow_types`` entry is required for such columns.
UNSUPPORTED = object()


def json_as_text(column_name):
    """
    JSON column value to JSON text. Requires
    output_format_native_write_json_as_string=1: the server sends JSON
    text which is passed through as is. There is deliberately no
    client-side serialization fallback to avoid performance bottlenecks.
    """
    def converter(value):
        if isinstance(value, str):
            return value or '{}'

        raise ValueError(
            "Column '{0}': JSON text output requires server-side "
            'serialization. Set '
            'output_format_native_write_json_as_string=1 in query '
            'settings (ClickHouse 24.10+) or transform the column in '
            'the query with toJSONString().'.format(column_name)
        )

    return converter


def json_as_object(value):
    """JSON column value to python objects for structured types."""
    if isinstance(value, str):
        return json.loads(value) if value else {}
    return value


def get_type_and_converter(spec, strings_as_bytes=False):
    """
    Maps ClickHouse type spec into pair (Arrow type, converter).

    Arrow type is ``None`` for types without explicit mapping. Their Arrow
    type is inferred from data later.

    Converter is a callable applied to each non-NULL value before array
    construction or ``None`` if values can be handled by Arrow as is.
    """
    spec = spec.strip()

    if spec in SIMPLE_TYPES:
        return SIMPLE_TYPES[spec], None

    if spec in STRINGIFIED_TYPES:
        return pa.string(), str

    if spec == 'JSON' or spec.startswith('JSON('):
        # No default representation: dynamic paths make every implicit
        # choice either lossy or unstable. Requires arrow_types.
        return UNSUPPORTED, None

    if spec == 'String' or spec.startswith('FixedString'):
        if strings_as_bytes:
            return pa.binary(), None
        return pa.string(), None

    if spec.startswith('Enum'):
        return pa.string(), None

    if spec.startswith('Nullable('):
        return get_type_and_converter(
            get_inner_spec('Nullable', spec), strings_as_bytes
        )

    if spec.startswith('LowCardinality('):
        return get_type_and_converter(
            get_inner_spec('LowCardinality', spec), strings_as_bytes
        )

    if spec.startswith('SimpleAggregateFunction('):
        inner_spec = get_inner_columns(
            get_inner_spec('SimpleAggregateFunction', spec)
        )[-1]
        return get_type_and_converter(inner_spec, strings_as_bytes)

    if spec.startswith('Array('):
        inner_type, inner_converter = get_type_and_converter(
            get_inner_spec('Array', spec), strings_as_bytes
        )
        if inner_type is None or inner_type is UNSUPPORTED:
            return inner_type, None

        if inner_converter is None:
            converter = None
        else:
            def converter(value, _converter=inner_converter):
                return [
                    None if x is None else _converter(x) for x in value
                ]

        return pa.list_(inner_type), converter

    if spec.startswith('Map('):
        key_spec, value_spec = get_inner_columns(get_inner_spec('Map', spec))
        key_type, key_converter = get_type_and_converter(
            key_spec, strings_as_bytes
        )
        value_type, value_converter = get_type_and_converter(
            value_spec, strings_as_bytes
        )
        for t in (key_type, value_type):
            if t is None or t is UNSUPPORTED:
                return t, None

        if key_converter is None and value_converter is None:
            converter = None
        else:
            def converter(value, _kc=key_converter, _vc=value_converter):
                return [
                    (
                        _kc(k) if _kc else k,
                        (None if v is None else _vc(v)) if _vc else v
                    )
                    for k, v in value.items()
                ]

        return pa.map_(key_type, value_type), converter

    if spec.startswith("Object('json')"):
        # Legacy JSON type (servers 22.3-24.x): same dynamic nature,
        # same treatment as JSON.
        return UNSUPPORTED, None

    if spec.startswith('Tuple(') or spec.startswith('Nested('):
        # No explicit mapping (inference applies), but unsupported
        # element types must not leak into inference.
        prefix = 'Tuple' if spec.startswith('Tuple(') else 'Nested'
        inner_columns = get_inner_columns_with_types(
            get_inner_spec(prefix, spec)
        )
        for _, inner_spec in inner_columns:
            inner_type, _ = get_type_and_converter(
                inner_spec, strings_as_bytes
            )
            if inner_type is UNSUPPORTED:
                return UNSUPPORTED, None
        return None, None

    if spec.startswith('Decimal'):
        if spec.startswith('Decimal('):
            precision, scale = [
                int(x) for x in get_inner_spec('Decimal', spec).split(',')
            ]
        else:
            prefix = spec.split('(')[0]
            if prefix not in DECIMAL_PRECISIONS:
                return None, None
            precision = DECIMAL_PRECISIONS[prefix]
            scale = int(get_inner_spec(prefix, spec))

        if precision <= 38:
            return pa.decimal128(precision, scale), None
        return pa.decimal256(precision, scale), None

    if spec.startswith('DateTime64('):
        params = get_inner_columns(get_inner_spec('DateTime64', spec))
        precision = int(params[0])
        tz = params[1].strip("'") if len(params) > 1 else None

        if precision == 0:
            unit = 's'
        elif precision <= 3:
            unit = 'ms'
        elif precision <= 6:
            unit = 'us'
        else:
            unit = 'ns'

        return pa.timestamp(unit, tz=tz), None

    if spec.startswith('DateTime'):
        tz = None
        if spec.startswith('DateTime('):
            tz = get_inner_spec('DateTime', spec).strip("'")
        return pa.timestamp('s', tz=tz), None

    return None, None
