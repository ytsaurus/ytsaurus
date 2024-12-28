from . import compat, filters
from .common import (
    OrmContext,
    OrmStructure,
    resolve_options_value,
    DEFAULT_LOCK_GROUP,
    DEFAULT_DATA_GROUP,
    ETC_NAME_SEPARATOR,
)
from .descriptions import PATH_TO_DESCRIPTION, OrmDescription
from .enum import OrmEnum, OrmEnumValue, enum_value_name_to_value
from .field import OrmField

from yt_proto.yt.orm.client.proto import object_pb2

from collections import defaultdict
from dataclasses import dataclass, field as dataclass_field, replace
from functools import cached_property
from google.protobuf import descriptor, descriptor_pb2 as protobuf_descriptor_pb2
from typing import DefaultDict, Optional, TYPE_CHECKING
from inflection import camelize

if TYPE_CHECKING:
    from .package import OrmModule, OrmPackage
    from .object import OrmObject


OPTIONS_BLACKLIST = {
    "NYT.NOrm.NClient.NProto.output_filename",
    "NYT.NOrm.NClient.NProto.no_etc",
}


@dataclass
class OrmEtc:
    message: "OrmMessage"
    lock_group: Optional[str] = None
    data_group: Optional[str] = None
    fields: list["OrmField"] = dataclass_field(default_factory=list)
    no_etc_column_suffix: bool = False
    column: Optional["OrmColumn"] = None

    def suffix_parts(self):
        parts = []
        if self.lock_group and (self.message.lock_group or DEFAULT_LOCK_GROUP) != self.lock_group:
            parts.append(self.lock_group)
        if self.data_group and (self.message.data_group or DEFAULT_DATA_GROUP) != self.data_group:
            parts.append(self.data_group)
        return parts

    def _etc_schema_suffix(self):
        if self.no_etc_column_suffix:
            return ""
        return ".etc"

    def _etc_field_suffix(self):
        if self.no_etc_column_suffix:
            return ""
        return "Etc"

    @property
    def field_suffix(self):
        parts = self.suffix_parts()
        if not parts:
            return self._etc_field_suffix()
        return f"{self._etc_field_suffix()}{''.join(camelize(v) for v in parts)}"

    @property
    def camel_case_name(self):
        parts = self.suffix_parts()
        if not parts:
            return ""
        return "_".join(camelize(v) for v in parts)

    @property
    def schema_suffix(self):
        parts = self.suffix_parts()
        if not parts:
            return self._etc_schema_suffix()
        return f"{self._etc_schema_suffix()}{ETC_NAME_SEPARATOR}{'.'.join(parts)}"

    @property
    def proto_suffix(self):
        return "".join(camelize(v) for v in self.suffix_parts())

    @property
    def cpp_type_suffix(self):
        return "_".join(camelize(v) for v in self.suffix_parts())

    @property
    def cpp_name_suffix(self):
        return self.proto_suffix

    @property
    def name(self):
        return "_".join(self.suffix_parts())


@dataclass
class OrmMessage(OrmStructure):
    in_place: bool = False
    consumed: bool = False
    etc_consumed: bool = False
    has_etc: bool = True
    initialized: bool = False
    linked: bool = False
    finalized: bool = False
    attributes_updatable_by_superuser_only: bool = False
    _lock_group: Optional[str] = None
    _data_group: Optional[str] = None
    no_etc_column_suffix: bool = False
    extensible: bool = False
    force_overwrite_update_mode: bool = False
    mandatory: bool = False
    ignore_builtin: bool = False

    object: Optional["OrmObject"] = None
    cpp_object_field_name: Optional[str] = None

    fields_by_number: dict[int, OrmField] = dataclass_field(default_factory=dict)
    fields_by_name: dict[str, OrmField] = dataclass_field(default_factory=dict)

    reference_fields_deprecated: list[OrmField] = dataclass_field(default_factory=list)
    composite_fields: list[OrmField] = dataclass_field(default_factory=list)
    column_fields: list[OrmField] = dataclass_field(default_factory=list)
    direct_fields: list[OrmField] = dataclass_field(default_factory=list)

    nested_enums_by_name: dict[str, OrmEnum] = dataclass_field(default_factory=dict)
    nested_messages_by_name: dict[str, "OrmMessage"] = dataclass_field(default_factory=dict)
    option_source_module_paths: set[str] = dataclass_field(default_factory=set)
    description: Optional[OrmDescription] = None

    message_tag_strs: list[str] = dataclass_field(default_factory=list)
    message_tags: list[OrmEnumValue] = dataclass_field(default_factory=list)
    options: list[str] = dataclass_field(default_factory=list)

    etc_columns: list["OrmColumn"] = dataclass_field(default_factory=list)
    instantiated_etcs: list[OrmEtc] = dataclass_field(default_factory=list)

    instantiated_from_field: Optional["OrmField"] = None
    original_message: Optional["OrmMessage"] = None

    is_object_message: bool = False

    def __repr__(self) -> str:
        return f"({self.path})"

    @classmethod
    def make(
        cls,
        proto_descriptor: descriptor.Descriptor,
        *,  # Keyword only from here.
        package: Optional["OrmPackage"] = None,
        module: Optional["OrmModule"] = None,
        containing_message: Optional["OrmMessage"] = None,
    ):
        result = cls(
            containing_message=containing_message,
        )

        proto_descriptor_pb2 = protobuf_descriptor_pb2.DescriptorProto()
        proto_descriptor.CopyToProto(proto_descriptor_pb2)

        result._analyze_name(proto_descriptor.name, proto_descriptor.full_name)
        result._analyze_options(proto_descriptor)
        result._analyze_tags(proto_descriptor)
        result._analyze_fields(proto_descriptor, proto_descriptor_pb2, package)
        result._analyze_nested_enums(proto_descriptor, package, module)
        result._analyze_nested_messages(proto_descriptor, package, module)
        result._analyze_reserved(proto_descriptor_pb2)
        result._analyze_oneofs(proto_descriptor)
        result.description = PATH_TO_DESCRIPTION.get(proto_descriptor.full_name, None)
        result.filename = proto_descriptor.file.name
        if package and module:
            result._analyze_disposition(package, module)
        return result

    @classmethod
    def merge(cls, base: "OrmMessage", mixin: "OrmMessage"):
        assert base is not mixin
        if mixin is None:
            return base
        if base.in_place and mixin.is_generic:
            return base
        if compat.yp() and not (base.is_mixin and mixin.is_mixin):
            if mixin.is_mixin:
                return base
            else:
                return mixin

        assert base.name == mixin.name or mixin.is_generic, f"Trying to merge {base.name} and {mixin.name}"
        assert base.is_mixin and mixin.is_mixin, f"Trying to merge non-mixin {base.name} and {mixin.name}"

        result = cls(
            consumed=base.consumed,
            etc_consumed=base.etc_consumed,
            attributes_updatable_by_superuser_only=base.attributes_updatable_by_superuser_only,
            _lock_group=base._lock_group or mixin._lock_group,
            _data_group=base._data_group or mixin._data_group,
            cpp_object_field_name=base.cpp_object_field_name,
            reference_fields_deprecated=base.reference_fields_deprecated.copy(),
            nested_enums_by_name=base.nested_enums_by_name.copy(),
            nested_messages_by_name=base.nested_messages_by_name.copy(),
            reserved_numbers=base.reserved_numbers.copy(),
            reserved_names=base.reserved_names.copy(),
            message_tag_strs=base.message_tag_strs + mixin.message_tag_strs,
            options=base.options + mixin.options,
            option_source_module_paths=base.option_source_module_paths | mixin.option_source_module_paths,
            description=OrmDescription.merge(base.description, mixin.description),
            object=base.object,
        )

        result._merge(base, mixin)

        result.in_place = base.in_place or mixin.in_place
        result.has_etc = base.has_etc and mixin.has_etc  # one no_etc is enough to kill etc
        result.no_etc_column_suffix = mixin.no_etc_column_suffix or base.no_etc_column_suffix
        result.extensible = mixin.extensible or base.extensible
        result.force_overwrite_update_mode = mixin.force_overwrite_update_mode or base.force_overwrite_update_mode
        result.mandatory = mixin.mandatory or base.mandatory
        result.ignore_builtin = base.ignore_builtin or mixin.ignore_builtin
        for field in base.fields_by_number.values():
            result._add_field(replace(field, parent=result))
        for field in mixin.fields_by_number.values():
            result._add_field(replace(field, parent=result))
        for nested_enum in mixin.nested_enums_by_name.values():
            result._add_nested_enum(nested_enum)
        for nested_message in mixin.nested_messages_by_name.values():
            result._add_nested_message(nested_message)
        for reserved_number in mixin.reserved_numbers:
            result._add_reserved_number(reserved_number)
        for reserved_name in mixin.reserved_names:
            result._add_reserved_name(reserved_name)
        result.reference_fields_deprecated.extend(mixin.reference_fields_deprecated)
        if mixin.is_generic:
            # Generics may contain covariant field types, e.g., TGenericObjectSpec. These cannot be
            # processed with the common rename mechanism because they mix into multiple classes, so
            # at the time of merge we handle the special case
            # |NSomewhere.TGenericObjectFoo| -> |NHere.TMyObjectFoo|.
            for mixin_name in mixin.original_full_names:
                result._update_field_types(mixin_name, result.full_name)
        return result

    @cached_property
    def path(self):
        if self.containing_message:
            return ".".join([self.containing_message.path, self.name])
        else:
            return self.name

    @property
    def lock_group(self) -> Optional[str]:
        if self._lock_group:
            return self._lock_group
        elif self.containing_message:
            return self.containing_message.lock_group
        return None

    @property
    def data_group(self) -> Optional[str]:
        if self._data_group:
            return self._data_group
        elif self.containing_message:
            return self.containing_message.data_group
        return None

    def initialize(self, context: OrmContext):
        if self.initialized:
            return
        self.initialized = True

        for nested_message in self.nested_messages:
            nested_message.initialize(context)

        for field in self.all_fields:
            field.initialize(context)

        self._rebuild_field_lists()

        self.options.sort(key=lambda option: option.split(" = "))

    def link(self, context: OrmContext):
        if not self.initialized:
            self.initialize(context)
        if self.linked:
            return
        self.linked = True

        fields = list(self.all_fields)  # Adding views will change the list.
        for field in fields:
            field.link(context)

    def finalize(self, context: OrmContext):
        assert self.linked
        if self.finalized:
            return
        self.finalized = True

        for field in self.all_fields:
            field.finalize(context)

        for reference in self.reference_fields_deprecated:
            reference.finalize(context)

    def _rebuild_field_lists(self):
        self.composite_fields = []
        self.column_fields = []
        self.direct_fields = []
        has_noncomputed_direct_fields = False

        for field in self.fields:
            if field.value_message:
                if field.value_message.is_composite and not field.ignore_columns:
                    assert not field.deprecated, "Deprecated option must not be used on composite fields"
                    assert not field.is_column, "Composite field {} cannot be column".format(field.snake_case_name)
                    assert not field.is_map and not field.is_repeated, (
                        f"Composite field {field.snake_case_name} cannot be map or repeated "
                        "(consider using `ignore_columns` options)"
                    )
                    self.composite_fields.append(field)
                    continue

            if field.is_column:
                self.column_fields.append(field)
            else:
                if not field.computed:
                    has_noncomputed_direct_fields = True
                self.direct_fields.append(field)

            if field.reference:
                field.reference.field = field

            field.parent = self

        if not self.has_etc and has_noncomputed_direct_fields:
            # |no_etc| has been applied but there are plain fields.
            # No complex fields are allowed.
            fields = self.composite_fields + self.column_fields + self.reference_fields_deprecated
            assert not fields, (
                f"The message {self.name} has columns or references "
                f"{list(map(lambda field: field.snake_case_name, fields))}. Either get rid of the no_etc option or "
                "move noncomputed fields to columns"
            )

        assert (
            self.has_etc or not self.no_etc_column_suffix
        ), f"Using no_etc_column_suffix in message {self.name}, where no_etc is true"

    def instantiate(self, instantiated_from_field: "OrmField") -> "OrmMessage":
        assert self.initialized, f"Requesting instance of non-initialized message {self.name}"

        if self.instantiated_from_field:
            assert (
                self.instantiated_from_field is instantiated_from_field
            ), f"Trying to use a top-level message {self.name} as field {instantiated_from_field.snake_case_name}"
            return self

        object = instantiated_from_field.parent.object

        new_instance = replace(
            self,
            fields_by_number={},
            fields_by_name={},
            reference_fields_deprecated=[],
            object=object,
        )
        new_instance.instantiated_from_field = instantiated_from_field
        new_instance.original_message = self

        for field in self.fields_by_number.values():
            new_instance._add_field(replace(field, parent=new_instance))

        for field in self.reference_fields_deprecated:
            new_instance.reference_fields_deprecated.append(replace(field, parent=new_instance))

        new_instance._rebuild_field_lists()

        return new_instance

    @property
    def is_composite(self):
        assert self.initialized
        return bool(self.composite_fields) or bool(self.column_fields)

    @property
    def is_cpp_composite(self):
        return self.is_composite or bool(self.reference_fields) or self.has_etc

    @property
    def fields(self):
        return [field for field in self.all_fields if not (field.deprecated or field.reference)]

    @property
    def reference_fields(self):
        return [field for field in self.all_fields if field.reference]

    @property
    def view_fields_deprecated(self) -> list[OrmField]:
        return [field for field in self.all_fields if field.foreign_view_key_field is not None]

    @property
    def view_fields(self) -> list[OrmField]:
        return [field for field in self.all_fields if field.viewed_reference]

    @property
    def children_view_fields(self) -> list[OrmField]:
        return [field for field in self.all_fields if field.children_view]

    @property
    def all_fields(self):
        """NB: Deprecated fields are included."""
        return self.fields_by_number.values()

    @property
    def nested_enums(self):
        return self.nested_enums_by_name.values()

    @property
    def nested_messages(self):
        return self.nested_messages_by_name.values()

    @property
    def oneofs(self) -> dict[str, list[OrmField]]:
        result: dict[str, list[OrmField]] = {}
        for field in self.fields:
            if not field.oneof_name:
                continue
            result.setdefault(field.oneof_name, []).append(field)
        return result

    @property
    def source_module_paths(self) -> set[str]:
        result = set(self.option_source_module_paths)
        for field in self.fields:
            result.update(field.source_module_paths)
            result.update(field.option_source_module_paths)
        for message in self.nested_messages:
            result.update(message.source_module_paths)
        for enum in self.nested_enums_by_name.values():
            result.update(enum.source_module_paths())
        result.discard(None)
        result.discard("default")
        return result

    @property
    def etc_type_name(self) -> str:
        if self.is_nested:
            outer_name = self.containing_message.etc_type_name
            inner_name = self.name.removeprefix("T")
            return f"{outer_name}{inner_name}"
        return f"{self.name}"

    @property
    def computed_fields(self):
        return filter(
            lambda field: (field.computed and not (field.is_view or field.system)),
            self.fields,
        )

    def compute_tags(self, tags_enum: OrmEnum):
        self.message_tags = [enum_value_name_to_value(tags_enum, tag) for tag in self.message_tag_strs]
        for field in self.fields:
            field.compute_tags(tags_enum)
        for message in self.nested_messages:
            message.compute_tags(tags_enum)

    @property
    def etcs(self) -> list[OrmEtc]:
        if self.instantiated_etcs:
            return self.instantiated_etcs
        result: list[OrmEtc] = []
        if not self.has_etc and self.is_composite:
            return result
        fields_by_groups: DefaultDict[tuple[str, str], list[OrmField]] = defaultdict(list)
        parent_groups = (self.lock_group or DEFAULT_LOCK_GROUP, self.data_group or DEFAULT_DATA_GROUP)
        fields_by_groups[parent_groups] = []
        for field in self.direct_fields:
            if field.system or field.computed:
                continue
            field_groups = (field.lock_group or parent_groups[0], field.data_group or parent_groups[1])
            fields_by_groups[field_groups].append(field)
        for groups, fields in fields_by_groups.items():
            lock, data = groups
            result.append(
                OrmEtc(
                    lock_group=lock,
                    data_group=data,
                    message=self,
                    fields=fields,
                    no_etc_column_suffix=self.no_etc_column_suffix or not self.has_etc,
                )
            )
        return sorted(result, key=lambda v: (v.lock_group, v.data_group))

    def add_reference_field_deprecated(self, reference: "OrmReferenceDeprecated"):
        self.reference_fields_deprecated.append(OrmField.make_reference_field_deprecated(self, reference))

    def add_default_reference_field(
        self, name: str, other_reference: "OrmReference", other_object: "OrmObject", other_reference_path: str
    ) -> OrmField:
        field = OrmField.make_default_reference_field(name, self, other_reference, other_object, other_reference_path)
        self._add_field(field)
        return field

    def add_view_field(self, name: Optional[str], number: Optional[int], reference: "OrmReference") -> OrmField:
        field = OrmField.make_view_field(name, number, self, reference)
        self._add_field(field)
        return field

    def _analyze_options(self, proto_descriptor: descriptor.Descriptor):
        options = proto_descriptor.GetOptions()
        extensions = options.Extensions
        if extensions[object_pb2.in_place]:
            self.in_place = True
        if extensions[object_pb2.no_etc]:
            self.has_etc = False
        self.no_etc_column_suffix = extensions[object_pb2.no_etc_column_suffix]
        self.extensible = extensions[object_pb2.extensible]
        self.force_overwrite_update_mode = extensions[object_pb2.force_overwrite_update_mode]
        self.mandatory = extensions[object_pb2.message_mandatory]
        if extensions[object_pb2.attributes_updatable_by_superuser_only]:
            self.attributes_updatable_by_superuser_only = True
        if options.HasExtension(object_pb2.message_lock_group):
            self._lock_group = extensions[object_pb2.message_lock_group]
        if options.HasExtension(object_pb2.message_data_group):
            self._data_group = extensions[object_pb2.message_data_group]
        if options.HasExtension(object_pb2.output_filename):
            self._set_explicit_output_filename(extensions[object_pb2.output_filename])
        if extensions[object_pb2.ignore_builtin]:
            self.ignore_builtin = True
        extensions = proto_descriptor.GetOptions().Extensions
        for option in extensions:
            if option.full_name not in OPTIONS_BLACKLIST:
                name = f"({option.full_name})"
                value = resolve_options_value(option, extensions[option], self)
                proto_value = filters.python_value_to_proto(value)
                if isinstance(proto_value, list):
                    self.options.extend(f"{name} = {x}" for x in proto_value)
                else:
                    self.options.append(f"{name} = {proto_value}")

    def _analyze_tags(self, proto_descriptor: descriptor.Descriptor):
        extensions = proto_descriptor.GetOptions().Extensions
        self.message_tag_strs = list(extensions[object_pb2.message_tags])

    def _tag_str_to_enum(self, tags_enum: OrmEnum, tag: str) -> OrmEnumValue:
        if tag in tags_enum.values_by_enum_value_name:
            return tags_enum.values_by_enum_value_name[tag]
        if tag in tags_enum.values_by_name:
            return tags_enum.values_by_name[tag]
        assert False, f"Tag {tag} not found for message {self.name}"

    def _analyze_fields(
        self,
        proto_descriptor: descriptor.Descriptor,
        proto_descriptor_pb2: protobuf_descriptor_pb2.DescriptorProto,
        package: "OrmPackage",
    ):
        field_descriptors_pb2 = {field.number: field for field in proto_descriptor_pb2.field}
        for field_descriptor in proto_descriptor.fields:
            self._add_field(
                OrmField.make(self, field_descriptor, package, field_descriptors_pb2[field_descriptor.number])
            )

    def _add_field(self, field: OrmField):
        assert field.number not in self.reserved_numbers, (
            f"Cannot add field {field.snake_case_name}: message {self.name} already contains reserved "
            f"number {field.number}"
        )
        assert (
            field.snake_case_name not in self.reserved_names
        ), f"Cannot add field {field.snake_case_name}: message {self.name} already contains this name as reserved"

        if field.number in self.fields_by_number:
            existing_field = self.fields_by_number[field.number]
            assert field.snake_case_name == existing_field.snake_case_name, (
                f"Trying to merge mismatching field {field.number}('{field.snake_case_name}') with existing "
                f"{existing_field.number}('{existing_field.snake_case_name}') in {self.name}"
            )
            assert (
                self.fields_by_number[field.number] is self.fields_by_name[field.snake_case_name]
            ), f"Sanity check failed with field {field.snake_case_name} in {self.name}"
            field = OrmField.merge(existing_field, field)
        else:
            assert field.snake_case_name not in self.fields_by_name, (
                f"Trying to merge mismatching field '{field.snake_case_name}'({field.number}) with existing "
                f"'{field.snake_case_name}'({self.fields_by_name[field.snake_case_name]}) in {self.name}"
            )
        # Override with the more refined variant.
        self.fields_by_number[field.number] = field
        self.fields_by_name[field.snake_case_name] = field

    def try_remove_field(self, field_name: str, reserve: bool) -> None:
        field = self.fields_by_name.get(field_name)
        if field:
            del self.fields_by_name[field_name]
            del self.fields_by_number[field.number]
            if reserve:
                self._add_reserved_name(field_name)
                self._add_reserved_number(field.number)
        else:
            assert self.in_place

    def renumber_field(self, field_name: str, from_number: int, to_number: int):
        field = self.fields_by_name.get(field_name)
        assert field
        assert field.number == from_number
        del self.fields_by_number[from_number]
        field.number = to_number
        self.fields_by_number[to_number] = field

    def _analyze_nested_enums(
        self,
        proto_descriptor: descriptor.Descriptor,
        package: "OrmPackage",
        module: "OrmModule",
    ):
        for nested_enum_descriptor in proto_descriptor.enum_types:
            nested_enum = OrmEnum.make(
                nested_enum_descriptor,
                containing_message=self,
                package=package,
                module=module,
            )
            self._add_nested_enum(nested_enum)

    def _add_nested_enum(self, nested_enum: OrmEnum):
        if nested_enum.name in self.nested_enums_by_name:
            existing_enum = self.nested_enums_by_name[nested_enum.name]
            merged_enum = OrmEnum.merge(existing_enum, nested_enum)
            self.nested_enums_by_name[nested_enum.name] = merged_enum
        else:
            self.nested_enums_by_name[nested_enum.name] = nested_enum

    def _analyze_nested_messages(
        self,
        proto_descriptor: descriptor.Descriptor,
        package: "OrmPackage",
        module: "OrmModule",
    ):
        for nested_message_descriptor in proto_descriptor.nested_types:
            if nested_message_descriptor.GetOptions().map_entry:
                continue
            nested_message = OrmMessage.make(
                nested_message_descriptor,
                containing_message=self,
                package=package,
                module=module,
            )
            self._add_nested_message(nested_message)

    def _add_nested_message(self, nested_message: "OrmMessage"):
        if nested_message.name in self.nested_messages_by_name:
            existing_message = self.nested_messages_by_name[nested_message.name]
            merged_message = OrmMessage.merge(existing_message, nested_message)
            self.nested_messages_by_name[nested_message.name] = merged_message
        else:
            self.nested_messages_by_name[nested_message.name] = nested_message

    def _analyze_reserved(self, proto_descriptor_pb2: protobuf_descriptor_pb2.DescriptorProto):
        for reserved_range in proto_descriptor_pb2.reserved_range:
            for reserved_number in range(reserved_range.start, reserved_range.end):
                self._add_reserved_number(reserved_number)
        for reserved_name in proto_descriptor_pb2.reserved_name:
            self._add_reserved_name(reserved_name)

    def _add_reserved_number(self, reserved_number):
        assert reserved_number not in self.fields_by_number
        self.reserved_numbers.add(reserved_number)

    def _add_reserved_name(self, reserved_name):
        assert reserved_name not in self.fields_by_name
        self.reserved_names.add(reserved_name)

    def _analyze_oneofs(self, proto_descriptor: descriptor.Descriptor):
        for oneof_descriptor in proto_descriptor.oneofs:
            if len(oneof_descriptor.fields) == 1:
                field_entry = oneof_descriptor.fields[0]
                field = self.fields_by_number[field_entry.number]
                if field.proto3_optional:
                    # Ignore synthetic oneof.
                    continue
            for field_entry in oneof_descriptor.fields:
                field = self.fields_by_number[field_entry.number]
                assert not field.in_oneof
                field.oneof_name = oneof_descriptor.name

    @property
    def output_to_default(self) -> bool:
        return not self.in_place and super().output_to_default

    def _analyze_disposition(self, package: "OrmPackage", module: "OrmModule"):
        super()._analyze_disposition(package, module)

        if module.is_source:
            # NB: We want to make everything source modules and get rid of in_place.
            self.in_place = False
        if self.in_place:
            module._is_public = True

    def _update_field_types(self, old_prefix, new_prefix):
        for field in self.fields:
            field_type = field.proto_value_type
            if field_type.startswith(old_prefix):
                field.proto_value_type = field_type.replace(old_prefix, new_prefix, 1)

        for nested_message in self.nested_messages:
            nested_message._update_field_types(old_prefix, new_prefix)


def _reserved_range_to_string(reserved_range):
    if reserved_range.start == reserved_range.end - 1:
        return str(reserved_range.start)
    return f"{reserved_range.start}-{reserved_range.end - 1}"
