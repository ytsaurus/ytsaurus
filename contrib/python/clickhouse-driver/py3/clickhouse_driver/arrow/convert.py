import pyarrow as pa

try:
    import numpy as np
except ImportError:
    np = None

try:
    import pandas as pd
except ImportError:
    pd = None

try:
    from ..columns.arrow.stringcolumn import ArrowStringBuffers
except ImportError:
    ArrowStringBuffers = None

from .. import errors
from ..columns.util import get_inner_columns, get_inner_spec
from ..protocol import ServerPacketTypes
from .mapping import (
    UNSUPPORTED, get_type_and_converter, json_as_object, json_as_text
)


class ArrowStreamState(object):
    """
    Tracks whether a streamed query was consumed to the end.
    ``pyarrow.RecordBatchReader.close`` doesn't reach the underlying
    generator, so the client uses this state to cancel unfinished
    streams before the next query.
    """
    __slots__ = ('connection', 'finished', 'cancelled')

    def __init__(self, connection):
        self.connection = connection
        self.finished = False
        self.cancelled = False


def create_record_batch_reader(packet_generator, context, state=None,
                               field_metadata=True, arrow_types=None):
    """
    Creates RecordBatchReader yielding one record batch per ClickHouse
    block. Schema is built from the header block, so it's available before
    any data block is received.

    Unless ``field_metadata`` is disabled, the original ClickHouse type
    of each column is attached to its Arrow field as ``clickhouse_type``
    metadata.

    ``arrow_types`` maps column names to Arrow types, overriding the
    default mapping. Columns of types without a default Arrow
    representation (``JSON``) require an entry.
    """
    strings_as_bytes = context.client_settings.get('strings_as_bytes', False)

    blocks = _data_blocks(packet_generator)

    first_block = next(blocks, None)
    if first_block is None:
        if state is not None:
            state.finished = True
        return pa.RecordBatchReader.from_batches(pa.schema([]), iter([]))

    columns_with_types = first_block.columns_with_types
    fields = _resolve_fields(
        columns_with_types, strings_as_bytes, arrow_types
    )

    # Header block contains no rows.
    buffered = [first_block] if first_block.num_rows else []

    # Types without explicit mapping are inferred from the first non-empty
    # block. It has to be received before the schema can be built.
    if any(type_ is None for type_, _ in fields):
        while not buffered:
            block = next(blocks, None)
            if block is None:
                break
            if block.num_rows:
                buffered.append(block)

        fields = _infer_missing_types(fields, buffered)

    schema = pa.schema([
        pa.field(
            name, type_,
            metadata={'clickhouse_type': spec} if field_metadata else None
        )
        for (name, spec), (type_, _) in zip(columns_with_types, fields)
    ])

    def batches():
        for block in buffered:
            yield _block_to_batch(block, schema, fields)

        while True:
            # The stream may have been cancelled by a subsequent query
            # on the same client. Reading further would consume packets
            # of that query.
            if state is not None and state.cancelled:
                raise errors.PartiallyConsumedQueryError()

            block = next(blocks, None)
            if block is None:
                break

            if block.num_rows:
                yield _block_to_batch(block, schema, fields)

        if state is not None:
            state.finished = True

    return pa.RecordBatchReader.from_batches(schema, batches())


def _is_json_spec(spec):
    return spec == 'JSON' or spec.startswith('JSON(')


def _json_declared_converter(spec, declared, name):
    """
    Composes JSON converters for a declared Arrow type, following
    Nullable/Array/Map containers down to JSON positions. Returns
    ``None`` when the spec contains no JSON at positions matched by
    the declared type.
    """
    if _is_json_spec(spec):
        if pa.types.is_string(declared) or \
                pa.types.is_large_string(declared):
            return json_as_text(name)
        return json_as_object

    if spec.startswith('Nullable('):
        return _json_declared_converter(
            get_inner_spec('Nullable', spec), declared, name
        )

    if spec.startswith('Array(') and (
            pa.types.is_list(declared) or pa.types.is_large_list(declared)):
        inner = _json_declared_converter(
            get_inner_spec('Array', spec), declared.value_type, name
        )
        if inner is None:
            return None

        def converter(value, _converter=inner):
            return [None if x is None else _converter(x) for x in value]
        return converter

    if spec.startswith('Map(') and pa.types.is_map(declared):
        _, value_spec = get_inner_columns(get_inner_spec('Map', spec))
        value_converter = _json_declared_converter(
            value_spec, declared.item_type, name
        )
        if value_converter is None:
            return None

        def converter(value, _vc=value_converter):
            return [
                (k, None if v is None else _vc(v))
                for k, v in value.items()
            ]
        return converter

    return None


def _resolve_fields(columns_with_types, strings_as_bytes, arrow_types):
    fields = []
    for name, spec in columns_with_types:
        type_, converter = get_type_and_converter(spec, strings_as_bytes)

        declared = (arrow_types or {}).get(name)
        if declared is not None:
            json_converter = _json_declared_converter(spec, declared, name)
            if json_converter is not None:
                converter = json_converter
            type_ = declared

        elif type_ is UNSUPPORTED:
            raise ValueError(
                "Column '{0}' of type {1} has no default Arrow "
                "representation. Pass arrow_types={{'{0}': "
                "pyarrow.string()}} for JSON text, declare a struct "
                'type for structured output, or transform the column '
                'in the query (e.g. toJSONString).'.format(name, spec)
            )

        fields.append((type_, converter))
    return fields


def _data_blocks(packet_generator):
    for packet in packet_generator:
        if getattr(packet, 'type', None) == ServerPacketTypes.DATA:
            block = getattr(packet, 'block', None)
            if block is not None:
                yield block


def _infer_missing_types(fields, buffered):
    rv = []
    for i, (type_, converter) in enumerate(fields):
        if type_ is None:
            if buffered:
                column = buffered[0].get_columns()[i]
                type_ = _column_to_array(column, None, converter).type
            else:
                type_ = pa.null()
        rv.append((type_, converter))
    return rv


def _block_to_batch(block, schema, fields):
    arrays = [
        _column_to_array(column, type_, converter)
        for column, (type_, converter) in zip(block.get_columns(), fields)
    ]
    return pa.RecordBatch.from_arrays(arrays, schema=schema)


def _column_to_array(column, type_, converter):
    if ArrowStringBuffers is not None and \
            isinstance(column, ArrowStringBuffers):
        return _string_buffers_to_array(column, type_)

    if np is not None and isinstance(column, np.ma.MaskedArray):
        return _masked_to_array(column, type_, converter)

    # Numeric and datetime64 NumPy columns are handled by Arrow without
    # copying.
    if np is not None and isinstance(column, np.ndarray) and \
            column.dtype.kind in 'iufbM':
        return pa.array(column, type=type_)

    if pd is not None and isinstance(column, pd.Categorical):
        if converter is None:
            # Categorical maps to Arrow dictionary array without
            # iterating over values.
            dictionary = pa.array(column)
            return dictionary.cast(type_) if type_ is not None \
                else dictionary
        column = [None if pd.isna(x) else x for x in column]

    if converter is not None:
        column = [None if x is None else converter(x) for x in column]

    return pa.array(column, type=type_)


def _string_buffers_to_array(column, type_):
    """
    Assembles a string/binary array directly from wire-format buffers:
    concatenated bytes + offsets, no per-string Python objects. The
    binary -> string cast validates UTF-8 in C.
    """
    n_items = len(column)
    offsets = np.frombuffer(column.offsets, dtype=np.int64)

    if offsets[-1] > 2 ** 31 - 1:
        raise ValueError(
            'Block string data exceeds 2GiB. '
            'Lower max_block_size to stream it.'
        )

    validity = None
    if column.nulls_map is not None:
        validity = pa.py_buffer(
            np.packbits(column.nulls_map == 0, bitorder='little')
        )

    binary = pa.Array.from_buffers(pa.binary(), n_items, [
        validity,
        pa.py_buffer(offsets.astype(np.int32)),
        pa.py_buffer(column.data)
    ])

    if type_ is None or type_ == pa.string():
        return binary.cast(pa.string())
    return binary


def _masked_to_array(column, type_, converter):
    """
    Nullable columns come from the NumPy path as masked arrays: raw
    values with placeholders in NULL slots plus the nulls map. Arrow
    stores exactly that, so the values buffer is reused as is and the
    nulls map is packed into the validity bitmap.
    """
    mask = np.ma.getmaskarray(column)
    data = column.data

    if converter is not None:
        values = [
            None if is_null else converter(x)
            for x, is_null in zip(data, mask)
        ]
        return pa.array(values, type=type_, mask=mask)

    if data.dtype.kind not in 'iufbM':
        return pa.array(data, type=type_, mask=mask)

    # pa.array copies values slot by slot when mask is passed. Packing
    # the validity bitmap manually is ~20x faster.
    data_array = pa.array(data) if type_ is None else \
        pa.array(data, type=type_)
    validity = pa.py_buffer(np.packbits(~mask, bitorder='little'))
    return pa.Array.from_buffers(
        data_array.type, len(data_array),
        [validity, data_array.buffers()[1]]
    )
