# Copyright 2024 Confluent, Inc.
# Copyright 2023 Buf Technologies, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import typing

from celpy import celtypes
from google.protobuf import __version__ as _protobuf_version
from google.protobuf import descriptor, message, message_factory

from confluent_kafka.schema_registry.rules.cel import string_format
from confluent_kafka.schema_registry.rules.cel.cel_field_presence import in_has

# protobuf 7 removed the deprecated FieldDescriptor.label property in favor of the
# is_repeated/is_required boolean properties. Track the major version so we keep
# working on both old (<7, has .label) and new (>=7, only .is_repeated) runtimes.
PROTOBUF_MAJOR_VERSION = int(_protobuf_version.split('.')[0])


def _is_repeated(field: descriptor.FieldDescriptor) -> bool:
    if PROTOBUF_MAJOR_VERSION >= 7:
        return field.is_repeated
    return field.label == descriptor.FieldDescriptor.LABEL_REPEATED


class CompilationError(Exception):
    pass


def make_key_path(field_name: str, key: celtypes.Value) -> str:
    return f"{field_name}[{string_format.format_value(key)}]"  # type: ignore[str-bytes-safe]


def make_duration(msg: message.Message) -> celtypes.DurationType:
    return celtypes.DurationType(
        seconds=msg.seconds,
        nanos=msg.nanos,
    )


def make_timestamp(msg: message.Message) -> celtypes.TimestampType:
    return make_duration(msg) + celtypes.TimestampType(1970, 1, 1)  # type: ignore[return-value]


def unwrap(msg: message.Message) -> celtypes.Value:
    return _field_to_cel(msg, msg.DESCRIPTOR.fields_by_name["value"])


_MSG_TYPE_URL_TO_CTOR = {
    "google.protobuf.Duration": make_duration,
    "google.protobuf.Timestamp": make_timestamp,
    "google.protobuf.StringValue": unwrap,
    "google.protobuf.BytesValue": unwrap,
    "google.protobuf.Int32Value": unwrap,
    "google.protobuf.Int64Value": unwrap,
    "google.protobuf.UInt32Value": unwrap,
    "google.protobuf.UInt64Value": unwrap,
    "google.protobuf.FloatValue": unwrap,
    "google.protobuf.DoubleValue": unwrap,
    "google.protobuf.BoolValue": unwrap,
}


class MessageType(celtypes.MapType):
    msg: message.Message
    desc: descriptor.Descriptor

    def __init__(self, msg: message.Message):
        super().__init__()
        self.msg = msg
        self.desc = msg.DESCRIPTOR
        field: descriptor.FieldDescriptor
        for field in self.desc.fields:
            if field.containing_oneof is not None and not self.msg.HasField(field.name):
                continue
            self[field.name] = _field_to_cel(self.msg, field)

    def __getitem__(self, name):
        field = self.desc.fields_by_name[name]
        if field.has_presence and not self.msg.HasField(name):
            if in_has():
                raise KeyError()
            else:
                return _zero_value(field)
        return super().__getitem__(name)


def _msg_to_cel(msg: message.Message) -> typing.Dict[str, celtypes.Value]:
    ctor = _MSG_TYPE_URL_TO_CTOR.get(msg.DESCRIPTOR.full_name)
    if ctor is not None:
        return ctor(msg)  # type: ignore[return-value]
    return MessageType(msg)  # type: ignore[return-value]


_TYPE_TO_CTOR = {
    descriptor.FieldDescriptor.TYPE_MESSAGE: _msg_to_cel,
    descriptor.FieldDescriptor.TYPE_GROUP: _msg_to_cel,
    descriptor.FieldDescriptor.TYPE_ENUM: celtypes.IntType,
    descriptor.FieldDescriptor.TYPE_BOOL: celtypes.BoolType,
    descriptor.FieldDescriptor.TYPE_BYTES: celtypes.BytesType,
    descriptor.FieldDescriptor.TYPE_STRING: celtypes.StringType,
    descriptor.FieldDescriptor.TYPE_FLOAT: celtypes.DoubleType,
    descriptor.FieldDescriptor.TYPE_DOUBLE: celtypes.DoubleType,
    descriptor.FieldDescriptor.TYPE_INT32: celtypes.IntType,
    descriptor.FieldDescriptor.TYPE_INT64: celtypes.IntType,
    descriptor.FieldDescriptor.TYPE_UINT32: celtypes.UintType,
    descriptor.FieldDescriptor.TYPE_UINT64: celtypes.UintType,
    descriptor.FieldDescriptor.TYPE_SINT32: celtypes.IntType,
    descriptor.FieldDescriptor.TYPE_SINT64: celtypes.IntType,
    descriptor.FieldDescriptor.TYPE_FIXED32: celtypes.UintType,
    descriptor.FieldDescriptor.TYPE_FIXED64: celtypes.UintType,
    descriptor.FieldDescriptor.TYPE_SFIXED32: celtypes.IntType,
    descriptor.FieldDescriptor.TYPE_SFIXED64: celtypes.IntType,
}


def _proto_message_has_field(msg: message.Message, field: descriptor.FieldDescriptor) -> typing.Any:
    if field.is_extension:
        return msg.HasExtension(field)
    else:
        return msg.HasField(field.name)


def _proto_message_get_field(msg: message.Message, field: descriptor.FieldDescriptor) -> typing.Any:
    if field.is_extension:
        return msg.Extensions[field]
    else:
        return getattr(msg, field.name)


def _scalar_field_value_to_cel(val: typing.Any, field: descriptor.FieldDescriptor) -> celtypes.Value:
    ctor = _TYPE_TO_CTOR.get(field.type)
    if ctor is None:
        msg = "unknown field type"
        raise CompilationError(msg)
    return ctor(val)  # type: ignore[operator]


def _field_value_to_cel(val: typing.Any, field: descriptor.FieldDescriptor) -> celtypes.Value:
    if _is_repeated(field):
        if field.message_type is not None and field.message_type.GetOptions().map_entry:
            return _map_field_value_to_cel(val, field)
        return _repeated_field_value_to_cel(val, field)
    return _scalar_field_value_to_cel(val, field)


def _is_empty_field(msg: message.Message, field: descriptor.FieldDescriptor) -> bool:
    if field.has_presence:
        return not _proto_message_has_field(msg, field)
    if _is_repeated(field):
        return len(_proto_message_get_field(msg, field)) == 0
    return _proto_message_get_field(msg, field) == field.default_value


def _repeated_field_to_cel(msg: message.Message, field: descriptor.FieldDescriptor) -> celtypes.Value:
    if field.message_type is not None and field.message_type.GetOptions().map_entry:
        return _map_field_to_cel(msg, field)
    return _repeated_field_value_to_cel(_proto_message_get_field(msg, field), field)


def _repeated_field_value_to_cel(val: typing.Any, field: descriptor.FieldDescriptor) -> celtypes.Value:
    result = celtypes.ListType()
    for item in val:
        result.append(_scalar_field_value_to_cel(item, field))
    return result


def _map_field_value_to_cel(mapping: typing.Any, field: descriptor.FieldDescriptor) -> celtypes.Value:
    result = celtypes.MapType()
    key_field = field.message_type.fields[0]
    val_field = field.message_type.fields[1]
    for key, val in mapping.items():
        result[_field_value_to_cel(key, key_field)] = _field_value_to_cel(val, val_field)
    return result


def _map_field_to_cel(msg: message.Message, field: descriptor.FieldDescriptor) -> celtypes.Value:
    return _map_field_value_to_cel(_proto_message_get_field(msg, field), field)


def _field_to_cel(msg: message.Message, field: descriptor.FieldDescriptor) -> celtypes.Value:
    if _is_repeated(field):
        return _repeated_field_to_cel(msg, field)
    elif field.message_type is not None and not _proto_message_has_field(msg, field):
        return None
    else:
        return _scalar_field_value_to_cel(_proto_message_get_field(msg, field), field)


def check_field_type(field: descriptor.FieldDescriptor, expected: int, wrapper_name: typing.Optional[str] = None):
    if field.type != expected and (
        field.type != descriptor.FieldDescriptor.TYPE_MESSAGE or field.message_type.full_name != wrapper_name
    ):
        msg = f"field {field.name} has type {field.type} but expected {expected}"
        raise CompilationError(msg)


def _is_map(field: descriptor.FieldDescriptor):
    return _is_repeated(field) and field.message_type is not None and field.message_type.GetOptions().map_entry


def _is_list(field: descriptor.FieldDescriptor):
    return _is_repeated(field) and not _is_map(field)


def _zero_value(field: descriptor.FieldDescriptor):
    if field.message_type is not None and not _is_repeated(field):
        return _field_value_to_cel(message_factory.GetMessageClass(field.message_type)(), field)
    else:
        return _field_value_to_cel(field.default_value, field)
