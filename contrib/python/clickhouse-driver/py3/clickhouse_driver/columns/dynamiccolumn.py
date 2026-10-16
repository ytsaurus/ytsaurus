"""
Reader for ClickHouse's Dynamic / Variant column families.

These are not exposed as user-facing types in clickhouse-driver yet — they
exist here so the JSON column reader can compose them instead of
hand-rolling Variant deserialization. The byte layout mirrors
``SerializationDynamic`` and ``SerializationVariant`` in ClickHouse 25.5.
"""

from .base import Column
from .stringcolumn import ByteString
from ..reader import (
    read_binary_str,
    read_binary_uint64,
)
from ..varint import read_varint


__all__ = [
    'DynamicColumn',
    'SharedValueDecoder',
    'DYNAMIC_V1',
    'DYNAMIC_V2',
    'VARIANT_MODE_BASIC',
    'VARIANT_MODE_COMPACT',
    'NULL_DISCRIMINATOR',
    'SHARED_VARIANT_NAME',
]


# DynamicSerializationVersion enum (ClickHouse 25.5). Note these
# numeric values differ from ObjectSerializationVersion in
# newjsoncolumn — each enum is independent on the server side.
DYNAMIC_V1 = 1
DYNAMIC_V2 = 2

# VariantDiscriminatorsSerializationMode.
VARIANT_MODE_BASIC = 0
VARIANT_MODE_COMPACT = 1

# ColumnVariant::NULL_DISCRIMINATOR — the global discriminator value used
# to mark a NULL row.
NULL_DISCRIMINATOR = 0xFF

# SHARED_VARIANT_TYPE_NAME from ClickHouse src/Columns/ColumnDynamic.h.
# Participates in the alphabetical sort of variant names alongside
# user-declared variants.
SHARED_VARIANT_NAME = "SharedVariant"


class DynamicColumn(Column):
    """
    Reader for a ClickHouse ``Dynamic`` column.

    Construction does not know the inner variant types yet — those come
    from the wire in ``read_state_prefix``. The variant list always ends
    with an implicit ``SharedVariant`` (a ``ByteString`` carrying
    ``encodeDataType + serializeBinary`` blobs) that catches values
    whose type does not match any of the declared variants.
    """

    py_types = (object,)

    def __init__(self, column_by_spec_getter, shared_value_decoder=None,
                 **kwargs):
        self.column_by_spec_getter = column_by_spec_getter
        # Pre-built decoder for the implicit SharedVariant. Sharing one
        # across all DynamicColumns of a block (plus the JSON column
        # itself) lets the handler/column caches accumulate across
        # every overflow value in the block.
        if shared_value_decoder is None:
            shared_value_decoder = SharedValueDecoder(
                column_by_spec_getter)
        self.shared_value_decoder = shared_value_decoder
        self._column_kwargs = kwargs
        self.variant_specs = []
        self.variant_columns = []
        self.discriminators_mode = VARIANT_MODE_BASIC
        super(DynamicColumn, self).__init__(**kwargs)

    def read_state_prefix(self, buf):
        # ObjectStructure stream:
        #   UInt64 LE  structure_version  (V1 = 1, V2 = 2)
        #   if V1: VarUInt num_dynamic_types (legacy max_dynamic_types
        #                                     slot, value reused for the
        #                                     variant count — discarded
        #                                     by the server too)
        #   VarUInt    num_dynamic_types (excludes the implicit
        #                                 SharedVariant)
        #   N × String variant type spec
        # SerializationVariant prefix:
        #   UInt64 LE  discriminators_mode (BASIC = 0, COMPACT = 1)
        structure_version = read_binary_uint64(buf)
        if structure_version not in (DYNAMIC_V1, DYNAMIC_V2):
            raise NotImplementedError(
                "Unsupported Dynamic serialization version {}".format(
                    structure_version))
        if structure_version == DYNAMIC_V1:
            read_varint(buf)  # legacy slot
        num_dynamic_types = read_varint(buf)
        declared_specs = [
            read_binary_str(buf) for _ in range(num_dynamic_types)
        ]

        # SerializationVariant / DataTypeVariant sort their global
        # discriminators by ``DataType::getName()`` alphabetically. The
        # SharedVariant type name ("SharedVariant") participates in the
        # sort, so we have to interleave it back in to know which
        # discriminator index it claims.
        all_names = list(declared_specs) + [SHARED_VARIANT_NAME]
        sorted_names = sorted(all_names)
        self.variant_specs = list(declared_specs)
        self.variant_columns = []
        self._shared_variant_index = None
        for i, name in enumerate(sorted_names):
            if name == SHARED_VARIANT_NAME:
                self.variant_columns.append(
                    ByteString(**self._column_kwargs))
                self._shared_variant_index = i
            else:
                self.variant_columns.append(
                    self.column_by_spec_getter(name))

        self.discriminators_mode = read_binary_uint64(buf)
        if self.discriminators_mode not in (
                VARIANT_MODE_BASIC, VARIANT_MODE_COMPACT):
            raise NotImplementedError(
                "Unsupported Variant discriminators mode {}".format(
                    self.discriminators_mode))
        # Variant element state prefixes — most primitives are no-ops but
        # any composed type (LowCardinality, etc.) needs to consume its
        # own bytes. The SharedVariant String has no prefix.
        for i, column in enumerate(self.variant_columns):
            if i == self._shared_variant_index:
                continue
            column.read_state_prefix(buf)

    def read_items(self, n_items, buf):
        """
        Read ``n_items`` rows of the SerializationVariant body in BASIC
        mode. Wire layout per ClickHouse 25.5:

          * ``n_items × UInt8`` global discriminators (``255`` for
            NULL).
          * For each variant in global discriminator order, the
            per-variant column data sized by how many rows landed in
            that variant.

        Rows that hit the SharedVariant slot are passed through
        ``self.shared_value_decoder`` to recover their Python form.
        """
        if self.discriminators_mode == VARIANT_MODE_COMPACT:
            raise NotImplementedError(
                "Compact Variant discriminators are not supported yet")

        variant_columns = self.variant_columns
        shared_variant_index = self._shared_variant_index
        shared_variant_decoder = self.shared_value_decoder.decode

        discriminators = buf.read(n_items)
        if len(discriminators) != n_items:
            raise EOFError(
                "Variant discriminators truncated: got {} bytes, "
                "want {}".format(len(discriminators), n_items))

        # Count rows per variant (preserving wire-order row appearance).
        per_variant_indices = [[] for _ in range(len(variant_columns))]
        for row, disc in enumerate(discriminators):
            if disc == NULL_DISCRIMINATOR:
                continue
            if disc >= len(variant_columns):
                raise ValueError(
                    "Variant discriminator {} out of range "
                    "(have {} variants)".format(
                        disc, len(variant_columns)))
            per_variant_indices[disc].append(row)

        values_by_row = [None] * n_items
        for variant_idx, rows in enumerate(per_variant_indices):
            if not rows:
                continue
            column = variant_columns[variant_idx]
            # ``read_data`` handles nulls maps / nested column wiring;
            # ``read_items`` is a thinner low-level API. Pick whichever
            # the underlying column exposes — most primitives implement
            # both.
            if hasattr(column, 'read_data'):
                chunk = column.read_data(len(rows), buf)
            else:
                chunk = column.read_items(len(rows), buf)
            chunk = list(chunk)
            if variant_idx == shared_variant_index:
                chunk = [shared_variant_decoder(b) for b in chunk]
            for row, value in zip(rows, chunk):
                values_by_row[row] = value

        return values_by_row


# ----------------------------------------------------------------------
# encodeDataType / serializeBinary decoder for SharedVariant blobs
# ----------------------------------------------------------------------

# BinaryTypeIndex tags from ClickHouse 25.5
# (src/DataTypes/DataTypesBinaryEncoding.cpp). Only the tags reachable
# from values that fit in a JSON document are mapped; unmapped tags
# raise on read so we surface gaps loudly instead of silently dropping
# data.
_TAG_NOTHING = 0x00
_TAG_UINT8 = 0x01
_TAG_UINT16 = 0x02
_TAG_UINT32 = 0x03
_TAG_UINT64 = 0x04
_TAG_UINT128 = 0x05
_TAG_UINT256 = 0x06
_TAG_INT8 = 0x07
_TAG_INT16 = 0x08
_TAG_INT32 = 0x09
_TAG_INT64 = 0x0A
_TAG_INT128 = 0x0B
_TAG_INT256 = 0x0C
_TAG_FLOAT32 = 0x0D
_TAG_FLOAT64 = 0x0E
_TAG_DATE = 0x0F
_TAG_DATE32 = 0x10
_TAG_DATETIME_UTC = 0x11
_TAG_DATETIME_TZ = 0x12
_TAG_DATETIME64_UTC = 0x13
_TAG_DATETIME64_TZ = 0x14
_TAG_STRING = 0x15
_TAG_FIXED_STRING = 0x16
_TAG_ARRAY = 0x1E
_TAG_TUPLE_UNNAMED = 0x1F
_TAG_TUPLE_NAMED = 0x20
_TAG_NULLABLE = 0x23
_TAG_BOOL = 0x2D
_TAG_JSON = 0x30


def _read_varint(reader):
    """LEB128 decoder for any reader exposing ``read_one`` returning an
    ``int`` byte. The Cython ``read_varint`` in ``..varint`` only works
    against the driver's buffered reader; shared-value decoding works
    against an in-memory blob, so we re-implement the trivial loop here
    to keep the hot path free of adapter layers."""
    shift = 0
    result = 0
    while True:
        b = reader.read_one()
        result |= (b & 0x7F) << shift
        shift += 7
        if b < 0x80:
            return result


def _read_binary_string(reader):
    length = _read_varint(reader)
    return reader.read(length)


_PRIMITIVE_TYPE_NAMES = {
    _TAG_NOTHING: "Nothing",
    _TAG_UINT8: "UInt8",
    _TAG_UINT16: "UInt16",
    _TAG_UINT32: "UInt32",
    _TAG_UINT64: "UInt64",
    _TAG_UINT128: "UInt128",
    _TAG_UINT256: "UInt256",
    _TAG_INT8: "Int8",
    _TAG_INT16: "Int16",
    _TAG_INT32: "Int32",
    _TAG_INT64: "Int64",
    _TAG_INT128: "Int128",
    _TAG_INT256: "Int256",
    _TAG_FLOAT32: "Float32",
    _TAG_FLOAT64: "Float64",
    _TAG_DATE: "Date",
    _TAG_DATE32: "Date32",
    _TAG_DATETIME_UTC: "DateTime",
    _TAG_STRING: "String",
    _TAG_BOOL: "Bool",
}


def _decode_type_spec(buf):
    """
    Parse the ``encodeDataType`` byte stream into a ClickHouse type
    string suitable for ``column_by_spec_getter``. The string form is
    intentional: it keeps the SharedVariant handler cache keyable by a
    single hash and lets us reuse the driver's existing column-by-spec
    machinery instead of reimplementing it on a parallel AST.
    """
    tag = buf.read(1)
    if not tag:
        raise EOFError("Unexpected end of encoded type stream")
    tag = tag[0]

    name = _PRIMITIVE_TYPE_NAMES.get(tag)
    if name is not None:
        return name

    if tag == _TAG_FIXED_STRING:
        n = _read_varint(buf)
        return "FixedString({})".format(n)

    if tag == _TAG_DATETIME_TZ:
        tz = _read_binary_string(buf).decode('utf-8')
        return "DateTime('{}')".format(tz)

    if tag == _TAG_DATETIME64_UTC:
        scale = _read_varint(buf)
        return "DateTime64({})".format(scale)

    if tag == _TAG_DATETIME64_TZ:
        scale = _read_varint(buf)
        tz = _read_binary_string(buf).decode('utf-8')
        return "DateTime64({}, '{}')".format(scale, tz)

    if tag == _TAG_NULLABLE:
        inner = _decode_type_spec(buf)
        return "Nullable({})".format(inner)

    if tag == _TAG_ARRAY:
        inner = _decode_type_spec(buf)
        return "Array({})".format(inner)

    if tag == _TAG_TUPLE_UNNAMED:
        size = _read_varint(buf)
        elems = [_decode_type_spec(buf) for _ in range(size)]
        return "Tuple({})".format(", ".join(elems))

    if tag == _TAG_TUPLE_NAMED:
        size = _read_varint(buf)
        parts = []
        for _ in range(size):
            element_name = _read_binary_string(buf).decode('utf-8')
            element_type = _decode_type_spec(buf)
            parts.append("{} {}".format(element_name, element_type))
        return "Tuple({})".format(", ".join(parts))

    if tag == _TAG_JSON:
        # encodeDataType layout for JSON (ClickHouse 25.5):
        #   UInt8   serialization version
        #   VarUInt max_dynamic_paths
        #   UInt8   max_dynamic_types
        #   VarUInt typed_paths_count, then per path: String + type
        #   VarUInt paths_to_skip_count, then per path: String
        #   VarUInt path_regexps_count, then per regexp: String
        # None of these parameters change how a JSON value is laid out on
        # the wire (every path is dynamic), so the spec collapses to the
        # bare "JSON" handler once the bytes are consumed.
        buf.read_one()       # serialization version
        _read_varint(buf)    # max_dynamic_paths
        buf.read_one()       # max_dynamic_types
        for _ in range(_read_varint(buf)):   # typed paths
            _read_binary_string(buf)
            _decode_type_spec(buf)
        for _ in range(_read_varint(buf)):   # paths to skip
            _read_binary_string(buf)
        for _ in range(_read_varint(buf)):   # path regexps to skip
            _read_binary_string(buf)
        return "JSON"

    raise NotImplementedError(
        "Cannot decode binary type tag 0x{:02x} in shared JSON value"
        .format(tag))


def _nothing_handler(reader):
    return None


def _insert_dotted_path(obj, dotted, value):
    """Place ``value`` into ``obj`` under a dotted JSON path, creating the
    intermediate dicts. ``{"a.b": 1}`` becomes ``{"a": {"b": 1}}`` so a
    nested JSON value read out of a SharedVariant matches the shape the
    top-level reader produces."""
    parts = dotted.split('.')
    cursor = obj
    for part in parts[:-1]:
        child = cursor.get(part)
        if not isinstance(child, dict):
            child = {}
            cursor[part] = child
        cursor = child
    cursor[parts[-1]] = value


def _split_tuple_elements(inner):
    """Split a ``Tuple(...)`` body on top-level commas."""
    parts = []
    depth = 0
    start = 0
    for i, ch in enumerate(inner):
        if ch == '(':
            depth += 1
        elif ch == ')':
            depth -= 1
        elif ch == ',' and depth == 0:
            parts.append(inner[start:i].strip())
            start = i + 1
    last = inner[start:].strip()
    if last:
        parts.append(last)
    return parts


class SharedValueDecoder:
    """
    Per-block decoder for the ``encodeDataType + serializeBinary``
    payloads found in a SharedVariant's underlying String column.

    Two caches keep the hot path tight:
      * ``_handler_cache`` maps a decoded type spec (e.g. ``"Int64"``,
        ``"Array(Nullable(String))"``) to a precompiled callable that
        reads only the value bytes — no per-value ``startswith`` chain
        or recursive ``_decode_value_by_spec`` walk.
      * ``_column_cache`` keeps the scalar column readers alive across
        values so we don't reconstruct ``Int64Column`` / ``String``
        for every overflow entry.

    A single ``_SharedValueReader`` is reused for every value rather
    than allocating an ``io.BytesIO`` + adapter pair per call.
    """

    __slots__ = (
        '_column_by_spec_getter',
        '_column_cache',
        '_handler_cache',
        '_reader',
    )

    def __init__(self, column_by_spec_getter):
        self._column_by_spec_getter = column_by_spec_getter
        self._column_cache = {}
        self._handler_cache = {}
        self._reader = _SharedValueReader()

    def decode(self, blob):
        if not blob:
            return None
        reader = self._reader
        reader.reset(blob)
        return self._decode_dynamic_value(reader)

    def _decode_dynamic_value(self, reader):
        """Decode one ``encodeDataType + serializeBinary`` value from the
        current reader position. Used both for a top-level SharedVariant
        blob and, recursively, for each dynamic path of a nested JSON
        value."""
        type_spec = _decode_type_spec(reader)
        handler = self._handler_cache.get(type_spec)
        if handler is None:
            handler = self._build_handler(type_spec)
            self._handler_cache[type_spec] = handler
        return handler(reader)

    def _build_handler(self, type_spec):
        if type_spec == "Nothing":
            return _nothing_handler

        if type_spec == "JSON":
            def json_handler(reader, _decode=self._decode_dynamic_value):
                # VarUInt path count, then per path: String name + a
                # dynamic value. Absent paths arrive as Nothing-typed
                # values; drop them so the object matches its input.
                obj = {}
                for _ in range(_read_varint(reader)):
                    path = _read_binary_string(reader).decode('utf-8')
                    value = _decode(reader)
                    if value is not None:
                        _insert_dotted_path(obj, path, value)
                return obj
            return json_handler

        if type_spec.startswith("Nullable("):
            inner_spec = type_spec[len("Nullable("):-1]
            inner_handler = self._lookup_handler(inner_spec)

            def nullable_handler(reader, _inner=inner_handler):
                if reader.read_one():
                    return None
                return _inner(reader)
            return nullable_handler

        if type_spec.startswith("Array("):
            inner_spec = type_spec[len("Array("):-1]
            inner_handler = self._lookup_handler(inner_spec)

            def array_handler(reader, _inner=inner_handler):
                size = _read_varint(reader)
                return [_inner(reader) for _ in range(size)]
            return array_handler

        if type_spec.startswith("Tuple("):
            elements = _split_tuple_elements(
                type_spec[len("Tuple("):-1])
            element_handlers = tuple(
                self._lookup_handler(e) for e in elements)

            def tuple_handler(reader, _handlers=element_handlers):
                return tuple(h(reader) for h in _handlers)
            return tuple_handler

        # Scalar — bind the column reader once and let it handle bytes.
        column = self._column_cache.get(type_spec)
        if column is None:
            column = self._column_by_spec_getter(type_spec)
            self._column_cache[type_spec] = column

        def scalar_handler(reader, _column=column):
            return _column.read_items(1, reader)[0]
        return scalar_handler

    def _lookup_handler(self, type_spec):
        handler = self._handler_cache.get(type_spec)
        if handler is None:
            handler = self._build_handler(type_spec)
            self._handler_cache[type_spec] = handler
        return handler


class _SharedValueReader:
    """
    Reusable reader over an in-memory bytes blob. Exposes the surface
    the driver's column readers need (``read``, ``read_one``,
    ``read_strings``) without allocating an ``io.BytesIO`` plus an
    adapter every call — both are hot-loop overhead on the shared
    path. The owning :class:`SharedValueDecoder` resets the underlying
    bytes between values.
    """

    __slots__ = ('_blob', '_pos')

    def __init__(self):
        self._blob = b''
        self._pos = 0

    def reset(self, blob):
        self._blob = blob
        self._pos = 0

    def read(self, n):
        start = self._pos
        end = start + n
        self._pos = end
        return self._blob[start:end]

    def read_one(self):
        pos = self._pos
        b = self._blob[pos]
        self._pos = pos + 1
        return b

    def read_strings(self, n_items, encoding=None):
        out = []
        for _ in range(n_items):
            length = _read_varint(self)
            chunk = self.read(length)
            if encoding is not None:
                chunk = chunk.decode(encoding)
            out.append(chunk)
        return out
