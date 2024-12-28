from .common import OrmStructure
from .descriptions import PATH_TO_DESCRIPTION, OrmDescription

from yt_proto.yt.core.yson.proto import protobuf_interop_pb2

from yt_proto.yt.orm.client.proto import object_pb2

from copy import deepcopy
from dataclasses import dataclass, field as dataclass_field
from google.protobuf import descriptor, descriptor_pb2 as protobuf_descriptor_pb2
from inflection import camelize, underscore
from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from .package import OrmPackage, OrmModule
    from .message import OrmMessage


@dataclass
class OrmEnumValue:
    name: str
    number: int
    enum_value_name: str
    deprecated: bool
    description: Optional[OrmDescription] = None

    @classmethod
    def make(cls, proto_enum_value_descriptor: descriptor.EnumValueDescriptor):
        name = proto_enum_value_descriptor.name.removeprefix("MIXIN_")
        options = proto_enum_value_descriptor.GetOptions()
        enum_value_name = options.Extensions[protobuf_interop_pb2.enum_value_name]
        return cls(
            name=name,
            number=proto_enum_value_descriptor.number,
            enum_value_name=enum_value_name,
            deprecated=options.deprecated,
        )

    @property
    def cpp_enum_value_name(self):
        if self.enum_value_name:
            return camelize(self.enum_value_name)
        else:
            # Default enum naming in proto is upper snake case, so for C++ it is firstly lowercased
            # and then camelized.
            return camelize(underscore(self.name))

    @property
    def options(self):
        if not self.enum_value_name:
            return ""
        return f" [(NYT.NYson.NProto.enum_value_name) = \"{self.enum_value_name}\"]"

    def __hash__(self):
        return hash(self.number)


@dataclass
class OrmEnum(OrmStructure):
    string_serializable: Optional[bool] = None
    values_by_number: dict[int, set[OrmEnumValue]] = dataclass_field(default_factory=dict)
    values_by_name: dict[str, OrmEnumValue] = dataclass_field(default_factory=dict)
    values_by_enum_value_name: dict[str, OrmEnumValue] = dataclass_field(default_factory=dict)
    option_source_module_paths: set[str] = dataclass_field(default_factory=set)
    description: Optional[OrmDescription] = None

    @classmethod
    def make(
        cls,
        proto_enum_descriptor: descriptor.EnumDescriptor,
        *,  # Keyword only from here.
        package: Optional["OrmPackage"] = None,
        module: Optional["OrmModule"] = None,
        containing_message: Optional["OrmMessage"] = None,
    ):
        result = cls(
            containing_message=containing_message,
        )
        result._analyze_name(proto_enum_descriptor.name, proto_enum_descriptor.full_name)
        result._analyze_options(proto_enum_descriptor)
        result._analyze_values(proto_enum_descriptor)
        result._analyze_reserved(proto_enum_descriptor)
        result.description = PATH_TO_DESCRIPTION.get(proto_enum_descriptor.full_name, None)
        result.filename = proto_enum_descriptor.file.name
        if package and module:
            result._analyze_disposition(package, module)
        return result

    @classmethod
    def merge(cls, base: "OrmEnum", mixin: "OrmEnum"):
        assert base is not mixin

        assert not (
            base.string_serializable is not None
            and mixin.string_serializable is not None
            and base.string_serializable != mixin.string_serializable
        ), f"{base.name} and {mixin.name} have different string_serializable flags, merge is not possible"
        result = cls(
            values_by_number=deepcopy(base.values_by_number),
            values_by_name=deepcopy(base.values_by_name),
            values_by_enum_value_name=deepcopy(base.values_by_enum_value_name),
            reserved_numbers=deepcopy(base.reserved_numbers),
            reserved_names=deepcopy(base.reserved_names),
            option_source_module_paths=base.option_source_module_paths | mixin.option_source_module_paths,
            description=OrmDescription.merge(base.description, mixin.description),
        )
        if mixin.string_serializable is not None:
            result.string_serializable = mixin.string_serializable
        else:
            result.string_serializable = base.string_serializable

        result._merge(base, mixin)

        for reserved_number in mixin.reserved_numbers:
            result._add_reserved_number(reserved_number)
        for reserved_name in mixin.reserved_names:
            result._add_reserved_name(reserved_name)
        for values in mixin.values_by_number.values():
            for value in values:
                result._add_value(value)
        return result

    def _add_reserved_number(self, reserved_number):
        assert (
            reserved_number not in self.values_by_number
        ), f"Enum {self.name} already contains a value with number {reserved_number}"
        self.reserved_numbers.add(reserved_number)

    def _add_reserved_name(self, reserved_name):
        assert (
            reserved_name not in self.values_by_name
        ), f"Enum {self.name} already contains a value with name {reserved_name}"
        self.reserved_names.add(reserved_name)

    @property
    def allow_alias(self):
        return any(len(x) > 1 for x in self.values_by_number.values())

    @property
    def values(self):
        result = []
        for values in self.values_by_number.values():
            result.extend(values)
        return result

    @property
    def values_formatted(self):
        return sorted(self.values, key=lambda x: 0xFFFFFFFF + x.number if x.number < 0 else x.number)

    def source_module_paths(self):
        return self.option_source_module_paths

    def _analyze_reserved(self, proto_enum_descriptor: descriptor.EnumDescriptor):
        proto = protobuf_descriptor_pb2.EnumDescriptorProto()
        proto_enum_descriptor.CopyToProto(proto)
        for reserved_range in proto.reserved_range:
            for reserved_number in range(reserved_range.start, reserved_range.end + 1):
                self._add_reserved_number(reserved_number)
        for reserved_name in proto.reserved_name:
            self._add_reserved_name(reserved_name)

    def _analyze_options(self, proto_enum_descriptor: descriptor.EnumDescriptor):
        options = proto_enum_descriptor.GetOptions()
        extensions = options.Extensions
        if options.HasExtension(object_pb2.enum_output_filename):
            self._set_explicit_output_filename(extensions[object_pb2.enum_output_filename])

    def _analyze_values(self, proto_enum_descriptor: descriptor.EnumDescriptor):
        has_protobuf_interop_extension = False
        for proto_enum_value_descriptor in proto_enum_descriptor.values:
            enum_value = OrmEnumValue.make(proto_enum_value_descriptor)
            for full_name in self.original_full_names:
                value_name = f"{full_name}.{proto_enum_value_descriptor.name}"
                if value_name in PATH_TO_DESCRIPTION:
                    enum_value.description = PATH_TO_DESCRIPTION[value_name]
            if enum_value.enum_value_name:
                has_protobuf_interop_extension = True
            self._add_value(enum_value)
        if has_protobuf_interop_extension:
            self.option_source_module_paths.add("yt_proto/yt/core/yson/proto/protobuf_interop.proto")

    def _add_value(self, value: OrmEnumValue):
        if value.enum_value_name and value.enum_value_name in self.values_by_enum_value_name:
            assert value.number == self.values_by_enum_value_name[value.enum_value_name].number, (
                f"Invalid enum {self.name} duplicate enum_value_name {value.enum_value_name} for values with number "
                f"{value.number} and {self.values_by_enum_value_name[value.enum_value_name].number}"
            )
        assert (
            value.number not in self.reserved_numbers
        ), f"Invalid enum {self.name} cannot use reserved number {value.number} for value {value.name}"
        if value.name in self.values_by_name:
            assert value == self.values_by_name[value.name], (
                f"Invalid enum {self.name} duplicate name {value.name} for values {value} and "
                f"{self.values_by_name[value.name]}"
            )
        assert (
            value.name not in self.reserved_names
        ), f"Invalid enum {self.name} cannot use reserved name {value.name} for values with number {value.number}"

        self.values_by_name[value.name] = value
        self.values_by_number.setdefault(value.number, set()).add(value)
        if value.enum_value_name:
            self.values_by_enum_value_name.setdefault(value.enum_value_name, value)


def enum_value_name_to_value(enum: OrmEnum, name: str) -> OrmEnumValue:
    assert name in enum.values_by_enum_value_name, f"Value with name {name} not found in enum {enum.name}"
    return enum.values_by_enum_value_name[name]
