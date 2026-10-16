"""
Wire protocol for UnversionedRow serialization.

Uses protobuf varint encoding, except doubles, which are stored as fixed
8-byte little-endian values (matching NTableClient WriteRowValue/ReadRowValue).
"""

import struct
from dataclasses import dataclass, field
from enum import IntEnum
from io import BytesIO
from typing import Any, Optional


class ColumnValueType(IntEnum):
    MIN = 0x00
    THE_BOTTOM = 0x01
    NULL = 0x02
    INT64 = 0x03
    UINT64 = 0x04
    DOUBLE = 0x05
    BOOLEAN = 0x06
    STRING = 0x10
    ANY = 0x11
    COMPOSITE = 0x12
    MAX = 0xEF

    def is_string_like_type(self):
        return self in (ColumnValueType.STRING, ColumnValueType.ANY, ColumnValueType.COMPOSITE)

    def is_value_type(self):
        return self in (ColumnValueType.INT64, ColumnValueType.UINT64, ColumnValueType.DOUBLE, ColumnValueType.BOOLEAN)


_CVT_BY_VALUE = {}
for _member in ColumnValueType:
    _CVT_BY_VALUE[int(_member)] = _member


def column_value_type_from_value(value: int) -> ColumnValueType:
    result = _CVT_BY_VALUE.get(value)
    if result is None:
        raise ValueError(f"Unknown column value type: {value:#x}")
    return result


@dataclass
class UnversionedValue:
    column_id: int
    type: ColumnValueType
    value: Any = None


@dataclass
class UnversionedRow:
    values: list[UnversionedValue] = field(default_factory=list)


# Max lengths for string-like types.
MAX_STRING_VALUE_LENGTH = 128 * 1024 * 1024  # 128 MB
MAX_ANY_VALUE_LENGTH = 128 * 1024 * 1024
MAX_COMPOSITE_VALUE_LENGTH = 128 * 1024 * 1024
MAX_ROW_VALUE_COUNT = 32768


def _read_varint(data: bytes, pos: int) -> tuple[int, int]:
    """Read a protobuf varint (unsigned) from data at pos. Returns (value, new_pos)."""
    result = 0
    shift = 0
    while True:
        if pos >= len(data):
            raise ValueError("Unexpected end of data while reading varint")
        b = data[pos]
        pos += 1
        result |= (b & 0x7F) << shift
        if (b & 0x80) == 0:
            break
        shift += 7
    return result, pos


def _zigzag_decode(n: int) -> int:
    """Decode a zigzag-encoded uint64 to a signed int64."""
    return (n >> 1) ^ -(n & 1)


def _zigzag_encode(n: int) -> int:
    """Encode a signed int64 to zigzag-encoded uint64."""
    return (n << 1) ^ (n >> 63)


def _write_varint(value: int) -> bytes:
    """Write a protobuf varint (unsigned)."""
    result = bytearray()
    # Ensure value is treated as unsigned 64-bit.
    value &= 0xFFFFFFFFFFFFFFFF
    while value > 0x7F:
        result.append((value & 0x7F) | 0x80)
        value >>= 7
    result.append(value & 0x7F)
    return bytes(result)


class WireProtocolReader:
    """Reads UnversionedRow from wire protocol binary data."""

    def __init__(self, data: bytes):
        self._data = data
        self._pos = 0

    def _read_uint32(self) -> int:
        value, self._pos = _read_varint(self._data, self._pos)
        return value & 0xFFFFFFFF

    def _read_uint64(self) -> int:
        value, self._pos = _read_varint(self._data, self._pos)
        return value & 0xFFFFFFFFFFFFFFFF

    def _read_sint64(self) -> int:
        raw = self._read_uint64()
        return _zigzag_decode(raw)

    def _read_raw_byte(self) -> int:
        if self._pos >= len(self._data):
            raise ValueError("Unexpected end of data")
        b = self._data[self._pos]
        self._pos += 1
        return b

    def _read_raw_bytes(self, length: int) -> bytes:
        if self._pos + length > len(self._data):
            raise ValueError(f"Unexpected end of data: need {length} bytes at pos {self._pos}")
        result = self._data[self._pos:self._pos + length]
        self._pos += length
        return result

    def _read_double(self) -> float:
        # The C++ counterpart (NTableClient WriteRowValue/ReadRowValue) stores doubles
        # as fixed 8-byte little-endian values, not varints.
        return struct.unpack("<d", self._read_raw_bytes(8))[0]

    def _read_bool(self) -> bool:
        value = self._read_uint64()
        return value != 0

    def _read_string_data(self, value_type: ColumnValueType, length: int) -> bytes:
        if value_type == ColumnValueType.STRING:
            limit = MAX_STRING_VALUE_LENGTH
        elif value_type == ColumnValueType.ANY:
            limit = MAX_ANY_VALUE_LENGTH
        elif value_type == ColumnValueType.COMPOSITE:
            limit = MAX_COMPOSITE_VALUE_LENGTH
        else:
            limit = 0

        if length < 0 or length > limit:
            raise ValueError(f"Unsupported {value_type.name} data length {length}")

        return self._read_raw_bytes(length)

    def read_unversioned_row(self) -> Optional[UnversionedRow]:
        """Read a single UnversionedRow from the wire data."""
        version = self._read_uint32()
        if version != 0:
            raise ValueError("Unversioned row does not support versions.")

        value_count = self._read_uint32()
        # -1 (as uint32, 0xFFFFFFFF) indicates null row.
        if value_count == 0xFFFFFFFF:
            return None

        if value_count > MAX_ROW_VALUE_COUNT:
            raise ValueError(f"Row value count {value_count} exceeds maximum {MAX_ROW_VALUE_COUNT}")

        values = []
        for _ in range(value_count):
            column_id = self._read_uint32() & 0xFFFF
            type_byte = self._read_raw_byte() & 0xFF
            value_type = column_value_type_from_value(type_byte)

            value = None
            if value_type.is_string_like_type():
                size = self._read_uint64()
                value = self._read_string_data(value_type, size)
            elif value_type.is_value_type():
                if value_type == ColumnValueType.INT64:
                    value = self._read_sint64()
                elif value_type == ColumnValueType.UINT64:
                    value = self._read_uint64()
                elif value_type == ColumnValueType.DOUBLE:
                    value = self._read_double()
                elif value_type == ColumnValueType.BOOLEAN:
                    value = self._read_bool()
                else:
                    raise ValueError(f"{value_type.name} cannot be represented as raw bits")
            # For NULL, MIN, MAX, THE_BOTTOM: value remains None.

            values.append(UnversionedValue(column_id=column_id, type=value_type, value=value))

        return UnversionedRow(values=values)


class WireProtocolWriter:
    """Writes UnversionedRow to wire protocol binary data."""

    def __init__(self):
        self._buf = BytesIO()

    def _write_uint32(self, value: int):
        self._buf.write(_write_varint(value & 0xFFFFFFFF))

    def _write_uint64(self, value: int):
        self._buf.write(_write_varint(value & 0xFFFFFFFFFFFFFFFF))

    def _write_sint64(self, value: int):
        self._write_uint64(_zigzag_encode(value))

    def _write_raw_byte(self, value: int):
        self._buf.write(bytes([value & 0xFF]))

    def _write_raw_bytes(self, data: bytes):
        self._buf.write(data)

    def _write_double(self, value: float):
        self._buf.write(struct.pack("<d", value))

    def write_unversioned_row(self, row: UnversionedRow) -> bytes:
        """Serialize an UnversionedRow to wire protocol bytes."""
        self._buf = BytesIO()

        # Header: version=0, value_count.
        self._write_uint32(0)
        self._write_uint32(len(row.values))

        for uv in row.values:
            # Column ID.
            self._write_uint32(uv.column_id)
            # Type byte.
            self._write_raw_byte(int(uv.type))
            # Value data.
            if uv.type == ColumnValueType.INT64:
                self._write_sint64(uv.value)
            elif uv.type in (ColumnValueType.UINT64, ColumnValueType.BOOLEAN):
                if uv.type == ColumnValueType.BOOLEAN:
                    self._write_uint64(1 if uv.value else 0)
                else:
                    self._write_uint64(uv.value)
            elif uv.type == ColumnValueType.DOUBLE:
                self._write_double(uv.value)
            elif uv.type.is_string_like_type():
                data = uv.value if isinstance(uv.value, bytes) else uv.value.encode("utf-8")
                self._write_uint64(len(data))
                self._write_raw_bytes(data)
            # NULL, MIN, MAX, THE_BOTTOM: no value data.

        return self._buf.getvalue()
