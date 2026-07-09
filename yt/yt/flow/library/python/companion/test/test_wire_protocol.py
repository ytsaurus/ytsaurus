"""Tests for wire protocol serialization/deserialization."""

import pytest

from yt.yt.flow.library.python.companion.wire_protocol import (
    ColumnValueType,
    UnversionedRow,
    UnversionedValue,
    WireProtocolReader,
    WireProtocolWriter,
    _zigzag_decode,
    _zigzag_encode,
)


class TestZigzag:
    def test_encode_decode_zero(self):
        assert _zigzag_decode(_zigzag_encode(0)) == 0

    def test_encode_decode_positive(self):
        for v in [1, 127, 128, 1000, 2**31 - 1, 2**62]:
            assert _zigzag_decode(_zigzag_encode(v)) == v

    def test_encode_decode_negative(self):
        for v in [-1, -128, -1000, -(2**31), -(2**62)]:
            assert _zigzag_decode(_zigzag_encode(v)) == v

    def test_known_values(self):
        assert _zigzag_encode(0) == 0
        assert _zigzag_encode(-1) == 1
        assert _zigzag_encode(1) == 2
        assert _zigzag_encode(-2) == 3


class TestRoundTrip:
    """Round-trip serialization tests for all value types."""

    def _roundtrip(self, row: UnversionedRow) -> UnversionedRow:
        writer = WireProtocolWriter()
        data = writer.write_unversioned_row(row)
        reader = WireProtocolReader(data)
        return reader.read_unversioned_row()

    def test_empty_row(self):
        row = UnversionedRow(values=[])
        result = self._roundtrip(row)
        assert result is not None
        assert len(result.values) == 0

    def test_null_value(self):
        row = UnversionedRow(values=[
            UnversionedValue(column_id=0, type=ColumnValueType.NULL),
        ])
        result = self._roundtrip(row)
        assert len(result.values) == 1
        assert result.values[0].type == ColumnValueType.NULL
        assert result.values[0].value is None

    def test_int64_positive(self):
        row = UnversionedRow(values=[
            UnversionedValue(column_id=0, type=ColumnValueType.INT64, value=42),
        ])
        result = self._roundtrip(row)
        assert result.values[0].type == ColumnValueType.INT64
        assert result.values[0].value == 42

    def test_int64_negative(self):
        row = UnversionedRow(values=[
            UnversionedValue(column_id=0, type=ColumnValueType.INT64, value=-12345),
        ])
        result = self._roundtrip(row)
        assert result.values[0].value == -12345

    def test_int64_zero(self):
        row = UnversionedRow(values=[
            UnversionedValue(column_id=0, type=ColumnValueType.INT64, value=0),
        ])
        result = self._roundtrip(row)
        assert result.values[0].value == 0

    def test_int64_max(self):
        row = UnversionedRow(values=[
            UnversionedValue(column_id=0, type=ColumnValueType.INT64, value=2**63 - 1),
        ])
        result = self._roundtrip(row)
        assert result.values[0].value == 2**63 - 1

    def test_int64_min(self):
        row = UnversionedRow(values=[
            UnversionedValue(column_id=0, type=ColumnValueType.INT64, value=-(2**63)),
        ])
        result = self._roundtrip(row)
        assert result.values[0].value == -(2**63)

    def test_uint64(self):
        row = UnversionedRow(values=[
            UnversionedValue(column_id=0, type=ColumnValueType.UINT64, value=12345678),
        ])
        result = self._roundtrip(row)
        assert result.values[0].type == ColumnValueType.UINT64
        assert result.values[0].value == 12345678

    def test_uint64_max(self):
        row = UnversionedRow(values=[
            UnversionedValue(column_id=0, type=ColumnValueType.UINT64, value=2**64 - 1),
        ])
        result = self._roundtrip(row)
        assert result.values[0].value == 2**64 - 1

    def test_double(self):
        row = UnversionedRow(values=[
            UnversionedValue(column_id=0, type=ColumnValueType.DOUBLE, value=3.14159),
        ])
        result = self._roundtrip(row)
        assert result.values[0].type == ColumnValueType.DOUBLE
        assert abs(result.values[0].value - 3.14159) < 1e-10

    def test_double_zero(self):
        row = UnversionedRow(values=[
            UnversionedValue(column_id=0, type=ColumnValueType.DOUBLE, value=0.0),
        ])
        result = self._roundtrip(row)
        assert result.values[0].value == 0.0

    def test_double_negative(self):
        row = UnversionedRow(values=[
            UnversionedValue(column_id=0, type=ColumnValueType.DOUBLE, value=-1.5e10),
        ])
        result = self._roundtrip(row)
        assert abs(result.values[0].value - (-1.5e10)) < 1

    def test_double_uses_fixed64_encoding(self):
        # 1.0 has raw bits 0x3FF0000000000000; doubles are stored as fixed
        # 8 little-endian bytes (the C++ counterpart format), not as varints.
        data = bytes([
            0x00,  # version
            0x01,  # number of values in row
            0x00,  # value id
            0x05,  # ColumnValueType.DOUBLE
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F,  # 1.0, little-endian
        ])
        reader = WireProtocolReader(data)
        result = reader.read_unversioned_row()
        assert result.values[0].type == ColumnValueType.DOUBLE
        assert result.values[0].value == 1.0

        writer = WireProtocolWriter()
        assert writer.write_unversioned_row(result) == data

    def test_boolean_true(self):
        row = UnversionedRow(values=[
            UnversionedValue(column_id=0, type=ColumnValueType.BOOLEAN, value=True),
        ])
        result = self._roundtrip(row)
        assert result.values[0].type == ColumnValueType.BOOLEAN
        assert result.values[0].value is True

    def test_boolean_false(self):
        row = UnversionedRow(values=[
            UnversionedValue(column_id=0, type=ColumnValueType.BOOLEAN, value=False),
        ])
        result = self._roundtrip(row)
        assert result.values[0].value is False

    def test_string(self):
        data = b"hello world"
        row = UnversionedRow(values=[
            UnversionedValue(column_id=0, type=ColumnValueType.STRING, value=data),
        ])
        result = self._roundtrip(row)
        assert result.values[0].type == ColumnValueType.STRING
        assert result.values[0].value == data

    def test_string_empty(self):
        row = UnversionedRow(values=[
            UnversionedValue(column_id=0, type=ColumnValueType.STRING, value=b""),
        ])
        result = self._roundtrip(row)
        assert result.values[0].value == b""

    def test_string_large(self):
        data = b"x" * 100000
        row = UnversionedRow(values=[
            UnversionedValue(column_id=0, type=ColumnValueType.STRING, value=data),
        ])
        result = self._roundtrip(row)
        assert result.values[0].value == data

    def test_any(self):
        data = b'{"key": "value"}'
        row = UnversionedRow(values=[
            UnversionedValue(column_id=0, type=ColumnValueType.ANY, value=data),
        ])
        result = self._roundtrip(row)
        assert result.values[0].type == ColumnValueType.ANY
        assert result.values[0].value == data

    def test_multiple_values(self):
        row = UnversionedRow(values=[
            UnversionedValue(column_id=0, type=ColumnValueType.INT64, value=42),
            UnversionedValue(column_id=1, type=ColumnValueType.STRING, value=b"test"),
            UnversionedValue(column_id=2, type=ColumnValueType.BOOLEAN, value=True),
            UnversionedValue(column_id=3, type=ColumnValueType.NULL),
        ])
        result = self._roundtrip(row)
        assert len(result.values) == 4
        assert result.values[0].value == 42
        assert result.values[1].value == b"test"
        assert result.values[2].value is True
        assert result.values[3].type == ColumnValueType.NULL

    def test_column_ids_preserved(self):
        row = UnversionedRow(values=[
            UnversionedValue(column_id=5, type=ColumnValueType.INT64, value=1),
            UnversionedValue(column_id=10, type=ColumnValueType.INT64, value=2),
        ])
        result = self._roundtrip(row)
        assert result.values[0].column_id == 5
        assert result.values[1].column_id == 10


class TestReaderErrors:
    def test_invalid_version(self):
        # Write version=1 manually.
        from yt.yt.flow.library.python.companion.wire_protocol import _write_varint
        data = _write_varint(1) + _write_varint(0)
        reader = WireProtocolReader(data)
        with pytest.raises(ValueError, match="does not support versions"):
            reader.read_unversioned_row()
