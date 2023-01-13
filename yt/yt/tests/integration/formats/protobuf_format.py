import yt.yson

import copy
import ctypes
import collections
import os
import struct
from io import BytesIO


class Enumeration(object):
    def __init__(self, dict_):
        self._name_to_value = dict_
        self._value_to_name = {value: name for name, value in dict_.items()}

    def name_to_value(self, name):
        return self._name_to_value[name]

    def value_to_name(self, value):
        return self._value_to_name[value]


def create_enumerations(dict_):
    return {name: Enumeration(d) for name, d in dict_.items()}


class BytesReader(object):
    def __init__(self, inp):
        self.inp = inp

    def try_read_chunk(self, size):
        if size == 0:
            return b""
        result = self.inp.read(size)
        assert size == len(result) or len(result) == 0
        return result if len(result) else None

    def read_chunk(self, size):
        if size == 0:
            return b""
        result = self.try_read_chunk(size)
        assert result
        return result

    def is_exhausted(self):
        if len(self.inp.read(1)) == 0:
            return True
        self.inp.seek(-1, os.SEEK_CUR)
        return False


TYPE_NAME_TO_STRUCT_FORMAT = {
    "bool": "?",
    "double": "<d",
    "float": "<f",
    "fixed32": "<I",
    "sfixed32": "<i",
    "fixed64": "<Q",
    "sfixed64": "<q",
}


WIRE_TYPE_VARINT = 0
WIRE_TYPE_64_BIT = 1
WIRE_TYPE_LENGTH_DELIMITED = 2
WIRE_TYPE_32_BIT = 5


TypeInfo = collections.namedtuple("TypeInfo", ["name", "writer_function", "reader_function", "wire_type"])


def get_wire_type(type_info, is_packed):
    if is_packed:
        return WIRE_TYPE_LENGTH_DELIMITED
    else:
        return type_info.wire_type


def create_type_name_to_type_info():
    TYPE_NAME_TO_WIRE_TYPE = {
        "varint": WIRE_TYPE_VARINT,
        "zigzag_varint": WIRE_TYPE_VARINT,
        "length_delimited": WIRE_TYPE_LENGTH_DELIMITED,
        "bool": WIRE_TYPE_VARINT,
        "double": WIRE_TYPE_64_BIT,
        "float": WIRE_TYPE_32_BIT,
        "fixed32": WIRE_TYPE_32_BIT,
        "sfixed32": WIRE_TYPE_32_BIT,
        "fixed64": WIRE_TYPE_64_BIT,
        "sfixed64": WIRE_TYPE_64_BIT,
    }

    def create_simple_type_info(type_name, reader_postprocessor=None):
        if reader_postprocessor is None:
            def reader_function(reader):
                return reader.read_value(type_name)
        else:
            def reader_function(reader):
                return reader_postprocessor(reader.read_value(type_name))
        return TypeInfo(
            name=type_name,
            writer_function=lambda writer, value: writer.write_value(type_name, value),
            reader_function=reader_function,
            wire_type=TYPE_NAME_TO_WIRE_TYPE[type_name],
        )

    def raise_not_implemented(type_name):
        raise NotImplementedError("Reader and writer functions for type {} are not implemented".format(type_name))

    def create_type_info_without_io_functions(type_name, wire_type):
        return TypeInfo(
            name=type_name,
            writer_function=lambda writer, value: raise_not_implemented(type_name),
            reader_function=lambda reader: raise_not_implemented(type_name),
            wire_type=wire_type,
        )

    def create_int_type_info(original_type):
        return TypeInfo(
            name="varint",
            writer_function=lambda writer, value: writer.write_varint(ctypes.c_uint64(value).value),
            reader_function=lambda reader: original_type(reader.read_varint()).value,
            wire_type=WIRE_TYPE_VARINT,
        )

    result = {
        "int64": create_int_type_info(ctypes.c_int64),
        "uint64": create_simple_type_info("varint"),
        "sint64": create_simple_type_info("zigzag_varint"),
        "int32": create_int_type_info(ctypes.c_int32),
        "uint32": create_simple_type_info("varint"),
        "sint32": create_simple_type_info("zigzag_varint"),
        "string": create_simple_type_info("length_delimited", reader_postprocessor=lambda value: value.decode("utf-8")),
        "bytes": create_simple_type_info("length_delimited"),
        "message": create_simple_type_info("length_delimited"),
        "enum_int": create_int_type_info(ctypes.c_int32),
        # Reader and writer functions for the following types will raise an error,
        # so they must be handled explicitly.
        "enum_string": create_type_info_without_io_functions("enum_string", WIRE_TYPE_VARINT),
        "other_columns": create_type_info_without_io_functions("other_columns", WIRE_TYPE_LENGTH_DELIMITED),
        "structured_message": create_type_info_without_io_functions("structured_message", WIRE_TYPE_LENGTH_DELIMITED),
        "any": TypeInfo(
            name="any",
            writer_function=lambda writer, value: writer.write_length_delimited(yt.yson.dumps(value)),
            reader_function=lambda reader: yt.yson.loads(reader.read_length_delimited()),
            wire_type=WIRE_TYPE_LENGTH_DELIMITED,
        ),
    }
    for type_ in TYPE_NAME_TO_STRUCT_FORMAT:
        result[type_] = create_simple_type_info(type_)

    return result


TYPE_NAME_TO_TYPE_INFO = create_type_name_to_type_info()


class LenvalFormat(object):
    TABLE_INDEX = "table_index"
    KEY_SWITCH = "key_switch"
    RANGE_INDEX = "range_index"
    ROW_INDEX = "row_index"
    TABLET_INDEX = "tablet_index"

    TABLE_INDEX_VALUE = -1
    KEY_SWITCH_VALUE = -2
    RANGE_INDEX_VALUE = -3
    ROW_INDEX_VALUE = -4
    END_OF_STREAM_VALUE = -5
    TABLET_INDEX_VALUE = -6


class LenvalReader(LenvalFormat, BytesReader):
    def __init__(self, stream):
        super(LenvalReader, self).__init__(stream)
        self._row_index = 0
        self._table_index = 0
        self._tablet_index = 0

    def _read_int(self):
        result = self._try_read_int()
        assert result is not None
        return result

    def _try_read_int(self):
        chunk = self.try_read_chunk(4)
        if chunk is None:
            return None
        return struct.unpack("<i", chunk)[0]

    def _read_int64(self):
        chunk = self.try_read_chunk(8)
        assert chunk is not None
        return struct.unpack("<q", chunk)[0]

    def read_next(self):
        lenval_value = self._try_read_int()
        if lenval_value is None:
            return None
        if lenval_value >= 0:
            attrs = {
                "row_index": self._row_index,
                "table_index": self._table_index,
                "tablet_index": self._tablet_index,
            }
            self._row_index += 1
            return (self.read_chunk(lenval_value), attrs)
        elif lenval_value == self.TABLE_INDEX_VALUE:
            self._table_index = self._read_int()
            return self.read_next()
        elif lenval_value == self.KEY_SWITCH_VALUE:
            return self.read_next()
        elif lenval_value == self.RANGE_INDEX_VALUE:
            return self.read_next()
        elif lenval_value == self.ROW_INDEX_VALUE:
            self._row_index = self._read_int64()
            return self.read_next()
        elif lenval_value == self.TABLET_INDEX_VALUE:
            self._tablet_index = self._read_int64()
            return self.read_next()
        else:
            assert False, "Unexpected lenval value {}".format(lenval_value)


class ProtobufReader(BytesReader):
    def try_read_varint(self):
        result = 0
        shift = 0
        finish = False
        chunk = self.try_read_chunk(1)
        if chunk is None:
            return None
        while not finish:
            b = ord(chunk)
            finish = not bool(b & 0x80)
            result |= (b & ~0x80) << shift
            shift += 7
            if not finish:
                chunk = self.try_read_chunk(1)
                assert chunk is not None
        return result

    def read_varint(self):
        result = self.try_read_varint()
        assert result is not None
        return result

    def read_zigzag_varint(self):
        x = self.read_varint()
        return ((x + 1) // 2) * (-1) ** (x % 2)

    def read_length_delimited(self):
        length = self.read_varint()
        return self.read_chunk(length)

    def read_value(self, type_):
        if type_ in TYPE_NAME_TO_STRUCT_FORMAT:
            format_ = TYPE_NAME_TO_STRUCT_FORMAT[type_]
            size = struct.calcsize(format_)
            chunk = self.read_chunk(size)
            return struct.unpack(format_, chunk)[0]
        else:
            method_name = "read_{}".format(type_)
            assert hasattr(self, method_name)
            return getattr(self, method_name)()


def parse_protobuf_message(message, field_configs, enumerations):
    def parse(reader, field_config, type_info):
        if field_config.get("packed", False):
            inner_reader = ProtobufReader(BytesIO(reader.read_length_delimited()))
            result = []
            while not inner_reader.is_exhausted():
                result.append(type_info.reader_function(inner_reader))
            return result

        field_type = field_config["proto_type"]
        if field_type == "structured_message":
            return parse_protobuf_message(
                reader.read_length_delimited(),
                field_config["fields"],
                enumerations,
            )
        elif field_type == "enum_string":
            enumeration = enumerations[field_config["enumeration_name"]]
            return enumeration.value_to_name(ctypes.c_int32(reader.read_varint()).value)
        elif field_type == "other_columns":
            return yt.yson.loads(reader.read_length_delimited())
        else:
            return type_info.reader_function(reader)

    field_number_to_field_config = {}
    for field_config in field_configs:
        if field_config["proto_type"] == "oneof":
            for subfield_config in field_config["fields"]:
                assert subfield_config["proto_type"] != "oneof"
                subfield_config_copy = copy.deepcopy(subfield_config)
                subfield_config_copy["oneof_name"] = field_config["name"]
                field_number_to_field_config[subfield_config["field_number"]] = subfield_config_copy
        else:
            field_number_to_field_config[field_config["field_number"]] = field_config

    result = {}
    bufio = BytesIO(message)
    reader = ProtobufReader(bufio)
    while True:
        tag = reader.try_read_varint()
        if tag is None:
            break
        field_number = tag >> 3
        field_config = field_number_to_field_config[field_number]
        field_type, field_name = field_config["proto_type"], field_config["name"]
        type_info = TYPE_NAME_TO_TYPE_INFO[field_type]
        packed = field_config.get("packed", False)
        assert tag & 0b111 == get_wire_type(type_info, packed)

        value = parse(reader, field_config, type_info)
        if "oneof_name" in field_config:
            result[field_config["oneof_name"]] = [field_config["name"], value]
        elif field_config.get("repeated", False) and not field_config.get("packed", False):
            result.setdefault(field_name, []).append(value)
        elif field_type == "other_columns":
            result.update(value)
        else:
            result[field_name] = value
    return result


def parse_lenval_protobuf(data, format, enable_control_attributes=False):
    bufio = BytesIO(data)
    result = []
    reader = LenvalReader(bufio)
    assert str(format) == "protobuf"
    assert len(format.attributes["tables"]) == 1
    proto_config = format.attributes["tables"][0]
    enumerations = create_enumerations(format.attributes.get("enumerations", {}))
    while True:
        item = reader.read_next()
        if item is None:
            return result
        data, attrs = item
        message = parse_protobuf_message(
            data,
            proto_config["columns"],
            enumerations,
        )
        if enable_control_attributes:
            message = yt.yson.to_yson_type(message, attributes=attrs)
        result.append(message)


class LenvalWriter(object):
    def __init__(self, output):
        self.output = output

    def write_data(self, data):
        self.output.write(struct.pack("<i", len(data)))
        self.output.write(data)


class ProtobufWriter(object):
    def __init__(self, output):
        self.output = output

    def write_varint(self, value):
        assert value >= 0
        while value >= 0x80:
            b = (value & 0xFF) | 0x80
            self.output.write(bytes([b]))
            value = value >> 7
        self.output.write(bytes([value]))

    def write_zigzag_varint(self, value):
        encoded = (2 * abs(value)) - (1 if value < 0 else 0)
        self.write_varint(encoded)

    def write_length_delimited(self, data):
        if isinstance(data, str):
            encoded_data = data.encode("utf-8")
            self.write_varint(len(encoded_data))
            self.output.write(encoded_data)
        else:  # bytes
            self.write_varint(len(data))
            self.output.write(data)

    def write_value(self, type_, value):
        if type_ in TYPE_NAME_TO_STRUCT_FORMAT:
            format_ = TYPE_NAME_TO_STRUCT_FORMAT[type_]
            self.output.write(struct.pack(format_, value))
        else:
            method_name = "write_{}".format(type_)
            assert hasattr(self, method_name)
            return getattr(self, method_name)(value)


def create_protobuf_tag(type_info, field_number, packed):
    wire_type = WIRE_TYPE_LENGTH_DELIMITED if packed else type_info.wire_type
    return (field_number << 3) | wire_type


def write_protobuf_message(message_dict, field_configs, enumerations):
    def write(writer, tag, field_config, type_info, value):
        field_type = field_config["proto_type"]
        writer.write_varint(tag)
        if field_type == "structured_message":
            serizalized_submessage = write_protobuf_message(value, field_config["fields"], enumerations)
            writer.write_length_delimited(serizalized_submessage)
        elif field_type == "enum_string":
            enumeration = enumerations[field_config["enumeration_name"]]
            writer.write_varint(ctypes.c_uint64(enumeration.name_to_value(value)).value)
        else:
            type_info.writer_function(writer, value)

    field_name_to_field_config = {
        field_config["name"]: field_config
        for field_config in field_configs
        if field_config["proto_type"] != "other_columns"
    }
    other_columns_configs = [c for c in field_configs if c["proto_type"] == "other_columns"]
    assert len(other_columns_configs) <= 1
    other_columns_config = None
    other_columns = None
    if other_columns_configs:
        other_columns_config = other_columns_configs[0]
        other_columns = {}
    bufio = BytesIO()
    writer = ProtobufWriter(bufio)
    for name, value in message_dict.items():
        if name not in field_name_to_field_config:
            if other_columns_config is not None:
                other_columns[name] = value
            continue
        if value == yt.yson.YsonEntity():
            continue

        field_config = field_name_to_field_config[name]
        if field_config["proto_type"] == "oneof":
            alternative_name, value = value[0], value[1]
            subfields = [sf for sf in field_config["fields"] if sf["name"] == alternative_name]
            assert len(subfields) == 1
            field_config = subfields[0]
        field_type = field_config["proto_type"]
        type_info = TYPE_NAME_TO_TYPE_INFO[field_type]
        tag = create_protobuf_tag(
            type_info,
            field_config["field_number"],
            field_config.get("packed", False),
        )
        if field_config.get("repeated", False):
            assert isinstance(value, list)
            if field_config.get("packed", False):
                if len(value) == 0:
                    continue
                writer.write_varint(tag)
                inner_bufio = BytesIO()
                inner_writer = ProtobufWriter(inner_bufio)
                for el in value:
                    type_info.writer_function(inner_writer, el)
                writer.write_length_delimited(inner_bufio.getvalue())
            else:
                for el in value:
                    write(writer, tag, field_config, type_info, el)
        else:
            write(writer, tag, field_config, type_info, value)

    if other_columns is not None:
        tag = create_protobuf_tag(
            TYPE_NAME_TO_TYPE_INFO["other_columns"],
            other_columns_config["field_number"],
        )
        writer.write_varint(tag)
        writer.write_length_delimited(yt.yson.dumps(other_columns))

    return bufio.getvalue()


def write_lenval_protobuf(message_dicts, format):
    bufio = BytesIO()
    writer = LenvalWriter(bufio)
    assert str(format) == "protobuf"
    assert len(format.attributes["tables"]) == 1
    proto_config = format.attributes["tables"][0]
    enumerations = create_enumerations(format.attributes.get("enumerations", {}))
    for message_dict in message_dicts:
        serizalized_message = write_protobuf_message(message_dict, proto_config["columns"], enumerations)
        writer.write_data(serizalized_message)
    return bufio.getvalue()
