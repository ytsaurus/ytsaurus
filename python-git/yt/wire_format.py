# DISCLAIMER: All code in this file is considered private. Use it at your own risk.
# It is possible that in future this code will become official part or YT Python API,
# until that time this code could be freely modified without backward compatibility.
# It is presented for demonstration purposes to ease first GRPC API usage.

from yt.common import YtError

import yt.yson as yson

from yt.packages.six import PY3, iteritems, integer_types, binary_type, text_type

import struct

try:
    from cStringIO import StringIO as BytesIO
except ImportError:  # Python 3
    from io import BytesIO

try:
    xrange
except NameError:  # Python 3
    xrange = range

SERIALIZATION_ALIGNMENT = 8

# Unversioned value type
VT_NULL    = 0x02
VT_INT64   = 0x03
VT_UINT64  = 0x04
VT_DOUBLE  = 0x05
VT_BOOLEAN = 0x06
VT_STRING  = 0x10
VT_ANY     = 0x11

SCHEMA_TYPE_TO_VALUE_TYPE = {
    "int64":   VT_INT64,
    "uint64":  VT_UINT64,
    "double":  VT_DOUBLE,
    "boolean": VT_BOOLEAN,
    "string":  VT_STRING,
    "any":     VT_ANY
}

# uint64(-1)
NULL_ROW_MARKER = 0xFFFFFFFFFFFFFFFF

def _validate_supported_types(expected_types, expected_type_in_schema, value):
    if not isinstance(value, expected_types):
        raise YtError('Cannot serialize value of type "{}" to "{}"'.format(
            type(value),
            expected_type_in_schema))

def _null_validate_supported_type(expected_types, expected_type_in_schema, value):
    pass

def align_up(size):
    return (size + SERIALIZATION_ALIGNMENT - 1) & ~(SERIALIZATION_ALIGNMENT - 1)

# Allows to read multiple attachments as one continuous byte stream.
class AttachmentStream(object):
    def __init__(self, response, message_size):
        if len(response) < message_size:
            raise YtError("Proto message size is greater than received byte count")

        self.stream = BytesIO(response)
        self.stream.seek(message_size)

        self.available_bytes = 0

    def read(self, size):
        while self.available_bytes == 0:
            attachment_size_str = self.stream.read(4)

            # EOS.
            if not attachment_size_str:
                return b""

            if len(attachment_size_str) < 4:
                raise YtError("Unexpected end of stream")

            attachment_size = struct.unpack("<I", attachment_size_str)[0]

            # Empty attachment marker.
            if attachment_size != 0xFFFFFFFF:
                self.available_bytes = attachment_size
                break

        to_read = min(self.available_bytes, size)
        read = self.stream.read(to_read)
        self.available_bytes -= to_read

        return read

    def read_exact(self, size):
        read = self.read(size)
        if len(read) < size:
            raise YtError("Unexpected end of stream")
        return read

def build_columns_from_schema(schema):
    type_to_value_type = SCHEMA_TYPE_TO_VALUE_TYPE
    return [{"name": entry["name"], "type": type_to_value_type[entry["type"]]} for entry in schema]

def serialize_rows_to_unversioned_wire_format(stream, rows, schema, enable_value_type_validation=True, encoding="utf-8"):
    key_to_type = dict((entry["name"], entry["type"]) for entry in schema)
    key_to_index = dict(zip([entry["name"] for entry in schema], xrange(len(schema))))
    aggregate_keys = set(entry["key"] for entry in schema if entry.get("aggregate", False))

    buffer_ = [struct.pack("<Q", len(rows))]
    total_data_size = 8  # Packed row count

    validate_value_type = _validate_supported_types if enable_value_type_validation else _null_validate_supported_type
    schema_type_to_value_type = SCHEMA_TYPE_TO_VALUE_TYPE

    for row in rows:
        if row is None:
            raise YtError("Row cannot be None")

        # NOTE(asaitgalin): Some of the values can be None so writing to stream
        # only when all values are processed.
        value_count = 0
        values_buffer = []

        for key, value in iteritems(row):
            expected_value_type = key_to_type.get(key)

            if expected_value_type is None:
                raise YtError('Column "{}" is not mentioned in schema'.format(key))
            if key in aggregate_keys:
                raise YtError('Aggregate key "{}" cannot be specified in a row'.format(key))

            if value is None:
                continue

            if expected_value_type == "any":
                serialized_value = yson.dumps(value)
            elif expected_value_type == "int64" or expected_value_type == "uint64":
                validate_value_type(integer_types, expected_value_type, value)
                formatter = "<q" if expected_value_type == "int64" else "<Q"
                serialized_value = struct.pack(formatter, value)
            elif expected_value_type == "boolean":
                validate_value_type((bool,) + integer_types, expected_value_type, value)
                serialized_value = struct.pack("<Q", int(value))
            elif expected_value_type == "double":
                validate_value_type((float,), expected_value_type, value)
                serialized_value = struct.pack("<d", value)
            elif expected_value_type == "string":
                validate_value_type((text_type, binary_type), expected_value_type, value)
                if isinstance(value, text_type):
                    if encoding is None:
                        raise YtError('Cannot encode unicode string, encoding is not specified')
                    serialized_value = value.encode(encoding)
                else:
                    serialized_value = value
            else:
                raise YtError('Incorrect type in schema: "{}"'.format(expected_value_type))

            value_header = struct.pack(
                "<HBBI",
                key_to_index[key],
                schema_type_to_value_type[expected_value_type],
                False,  # "aggregate" = False
                len(serialized_value))

            assert len(value_header) == 8

            serialized_value_sz = len(serialized_value)
            aligned_serialized_value_sz = align_up(serialized_value_sz)

            values_buffer.append(value_header)
            values_buffer.append(serialized_value)
            values_buffer.append(b"\x00" * (aligned_serialized_value_sz - serialized_value_sz))

            value_count += 1

        buffer_.append(struct.pack("<Q", value_count))
        total_data_size += 8

        for part in values_buffer:
            buffer_.append(part)
            total_data_size += len(part)

    stream.write(struct.pack("<I", total_data_size))
    for chunk in buffer_:
        stream.write(chunk)

def deserialize_rows_from_unversioned_wire_format(stream, column_names, skip_none_values=True, encoding="utf-8"):
    result = []

    count = struct.unpack("<Q", stream.read_exact(8))[0]

    for _ in xrange(count):
        column_count = struct.unpack("<Q", stream.read_exact(8))[0]

        if column_count == NULL_ROW_MARKER:
            result.append(None)

        row = {}

        for _ in xrange(column_count):
            id_, type_, aggregate, length = struct.unpack("<HBBI", stream.read_exact(8))
            assert not aggregate, "Aggregate values are not currently supported"

            if type_ == VT_ANY:
                if not PY3:
                    value = yson.loads(stream.read_exact(length))
                else:
                    value = yson.loads(stream.read_exact(length), encoding=encoding)

                stream.read_exact(align_up(length) - length)
            elif type_ == VT_STRING:
                value = stream.read_exact(length)
                if PY3 and encoding is not None:
                    value = value.decode(encoding)
                stream.read_exact(align_up(length) - length)
            elif type_ == VT_INT64:
                value = struct.unpack("<q", stream.read_exact(8))[0]
            elif type_ == VT_UINT64:
                value = struct.unpack("<Q", stream.read_exact(8))[0]
            elif type_ == VT_BOOLEAN:
                value = bool(struct.unpack("<Q", stream.read_exact(8))[0])
            elif type_ == VT_DOUBLE:
                value = struct.unpack("<d", stream.read_exact(8))[0]
            elif type_ == VT_NULL:
                value = None
            else:
                assert False, "Type " + str(type_) + " is not supported"

            if skip_none_values and value is None:
                continue

            row[column_names[id_]] = value

        result.append(row)

    return result
