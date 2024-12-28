from . import compat, filters
from .common import (
    OrmColumn,
    OrmContext,
    OrmOptionEnumValueWrapper,
    resolve_options_value,
    DEFAULT_LOCK_GROUP,
    DEFAULT_DATA_GROUP,
    storage_type_to_cpp_yt_type,
)
from .descriptions import PATH_TO_DESCRIPTION, OrmDescription
from .enum import OrmEnum, OrmEnumValue, enum_value_name_to_value
from .index import OrmIndex
from .reference import OrmReference, OrmReferenceDeprecated, OrmTransitiveReference

from yt_proto.yt.orm.client.proto import object_pb2

from yt_proto.yt.core.yson.proto import protobuf_interop_pb2

from google.protobuf import descriptor, descriptor_pb2 as protobuf_descriptor_pb2

from dataclasses import dataclass, field as dataclass_field
import posixpath
from typing import Optional, Any, TYPE_CHECKING

if TYPE_CHECKING:
    from .message import OrmMessage
    from .package import OrmPackage


@dataclass
class OrmTypeDescription:
    compatible_storage_types_by_priority: list[str] = dataclass_field(default_factory=list)
    proto_type: Optional[str] = None
    cpp_type: Optional[str] = None
    storage_type_to_enum_yson_storage_type: Optional[dict[str, OrmOptionEnumValueWrapper]] = None


TYPE_DESCRIPTIONS: dict[protobuf_descriptor_pb2.FieldDescriptorProto.Type.ValueType, OrmTypeDescription] = {
    # Order matches descriptor.proto
    protobuf_descriptor_pb2.FieldDescriptorProto.TYPE_DOUBLE: OrmTypeDescription(
        proto_type="double",
        cpp_type="double",
        compatible_storage_types_by_priority=["double"],
    ),
    protobuf_descriptor_pb2.FieldDescriptorProto.TYPE_FLOAT: OrmTypeDescription(
        proto_type="float",
        cpp_type="float",
        compatible_storage_types_by_priority=["float", "double"],
    ),
    protobuf_descriptor_pb2.FieldDescriptorProto.TYPE_INT64: OrmTypeDescription(
        proto_type="int64",
        cpp_type="i64",
        compatible_storage_types_by_priority=["int64"],
    ),
    protobuf_descriptor_pb2.FieldDescriptorProto.TYPE_UINT64: OrmTypeDescription(
        proto_type="uint64",
        cpp_type="ui64",
        compatible_storage_types_by_priority=["uint64"],
    ),
    protobuf_descriptor_pb2.FieldDescriptorProto.TYPE_INT32: OrmTypeDescription(
        proto_type="int32",
        cpp_type="i32",
        compatible_storage_types_by_priority=["int32", "int64"],
    ),
    protobuf_descriptor_pb2.FieldDescriptorProto.TYPE_FIXED64: OrmTypeDescription(
        proto_type="fixed64",
        cpp_type="ui64",
        compatible_storage_types_by_priority=["uint64"],
    ),
    protobuf_descriptor_pb2.FieldDescriptorProto.TYPE_FIXED32: OrmTypeDescription(
        proto_type="fixed32",
        cpp_type="ui32",
        compatible_storage_types_by_priority=["uint32", "uint64"],
    ),
    protobuf_descriptor_pb2.FieldDescriptorProto.TYPE_BOOL: OrmTypeDescription(
        proto_type="bool",
        cpp_type="bool",
        compatible_storage_types_by_priority=["boolean"],
    ),
    protobuf_descriptor_pb2.FieldDescriptorProto.TYPE_STRING: OrmTypeDescription(
        proto_type="string",
        cpp_type="TString",
        compatible_storage_types_by_priority=["utf8", "string"],
    ),
    # TYPE_GROUP not supported.
    protobuf_descriptor_pb2.FieldDescriptorProto.TYPE_MESSAGE: OrmTypeDescription(
        compatible_storage_types_by_priority=["any"],
    ),
    protobuf_descriptor_pb2.FieldDescriptorProto.TYPE_BYTES: OrmTypeDescription(
        proto_type="bytes",
        cpp_type="TString",
        compatible_storage_types_by_priority=["string"],
    ),
    protobuf_descriptor_pb2.FieldDescriptorProto.TYPE_UINT32: OrmTypeDescription(
        proto_type="uint32",
        cpp_type="ui32",
        compatible_storage_types_by_priority=["uint32", "uint64"],
    ),
    protobuf_descriptor_pb2.FieldDescriptorProto.TYPE_ENUM: OrmTypeDescription(
        compatible_storage_types_by_priority=["string", "int64"],
        storage_type_to_enum_yson_storage_type={
            "string": OrmOptionEnumValueWrapper("EYST_STRING"),
            "int64": OrmOptionEnumValueWrapper("EYST_INT"),
        },
    ),
    protobuf_descriptor_pb2.FieldDescriptorProto.TYPE_SFIXED32: OrmTypeDescription(
        proto_type="sfixed32",
        cpp_type="i32",
        compatible_storage_types_by_priority=["int32", "int64"],
    ),
    protobuf_descriptor_pb2.FieldDescriptorProto.TYPE_SFIXED64: OrmTypeDescription(
        proto_type="sfixed64",
        cpp_type="i64",
        compatible_storage_types_by_priority=["int64"],
    ),
    protobuf_descriptor_pb2.FieldDescriptorProto.TYPE_SINT32: OrmTypeDescription(
        proto_type="sint32",
        cpp_type="i32",
        compatible_storage_types_by_priority=["int32", "int64"],
    ),
    protobuf_descriptor_pb2.FieldDescriptorProto.TYPE_SINT64: OrmTypeDescription(
        proto_type="sint64",
        cpp_type="i64",
        compatible_storage_types_by_priority=["int64"],
    ),
}

FIELD_PATH_SEP = "/"
FOREIGN_VIEW_FIELD_OFFSET = 10000
OPAQUE_OPTIONS = [object_pb2.allow_mixin_overwrite_options, object_pb2.base_column]


@dataclass
class OrmField:
    snake_case_name: str
    lower_camel_case_name: str

    # |value| means either the map value type, the repeated element type or the singular type.

    # Either the primitive proto type or the fullname of the value message/enum.
    proto_value_type: Optional[str] = None

    value_message: Optional["OrmMessage"] = None
    value_enum: Optional["OrmEnum"] = None

    map_key: Optional["OrmField"] = None

    primitive_cpp_value_type: Optional[str] = None

    cpp_yt_value_type: Optional[str] = None
    yt_schema_value_type: Optional[str] = None

    _cpp_getter: Optional[str] = None
    _cpp_descriptor: Optional[str] = None

    @property
    def cpp_getter(self):
        assert self.finalized
        return self._cpp_getter

    @property
    def cpp_descriptor(self):
        assert self.finalized
        return self._cpp_descriptor

    parent: Optional["OrmMessage"] = None
    source_module_paths: set[str] = dataclass_field(default_factory=set)
    option_source_module_paths: set[str] = dataclass_field(default_factory=set)
    number: int = 0
    is_primary: bool = False
    store_field_to_meta_response: Optional[bool] = False
    is_required: bool = False
    is_repeated: bool = False
    is_primitive: bool = True
    initialized: bool = False
    linked: bool = False
    finalized: bool = False
    indexed_by: list[OrmIndex] = dataclass_field(default_factory=list)
    used_in_predicate: bool = False
    index_over_reference_table: Optional[OrmIndex] = None
    generation_policy: Optional[str] = None
    min_length: Optional[int] = None
    max_length: Optional[int] = None
    valid_charset: Optional[str] = None
    min_value: Optional[int] = None
    max_value: Optional[int] = None
    index_for_increment: Optional[str] = None
    reference_deprecated: Optional[OrmReferenceDeprecated] = None
    reference: Optional[OrmReference] = None
    foreign_object_type: Optional[str] = None
    custom_foreign_view_name: Optional[str] = None
    forbid_removal_with_non_empty_references: Optional[bool] = None
    forbid_target_object_removal_with_non_empty_references: Optional[bool] = None
    references_table_suffix: Optional[str] = None
    reference_name: Optional[str] = None
    is_embedded_semaphore: bool = False
    oneof_name: Optional[str] = None
    foreign_view_key_field: Optional["OrmField"] = None
    viewed_reference: Optional["OrmReference"] = None
    column: Optional["OrmColumn"] = None
    update_policy: Optional[str] = None
    transitive_reference: Optional[OrmTransitiveReference] = None
    transitive_key: Optional[str] = None
    description: Optional[OrmDescription] = None
    proto3_optional: Optional[bool] = None
    explicit_presence: Optional[bool] = None
    is_control_touch: bool = False
    allow_mixin_overwrite_options: list[str] = dataclass_field(default_factory=list)
    is_timestamp_attribute: bool = False
    is_access_control_parent: bool = False
    method_argument_type: Optional[str] = None
    children_view: Optional[str] = None
    children_view_object: Optional["OrmObject"] = None

    path: Optional[str] = None

    _add_tags: list[OrmEnumValue] = dataclass_field(default_factory=list)
    _remove_tags: list[OrmEnumValue] = dataclass_field(default_factory=list)

    _custom_column_name: Optional[str] = None

    _lock_group: Optional[str] = None
    _data_group: Optional[str] = None
    _options: dict[str, Any] = dataclass_field(default_factory=dict)

    def __repr__(self) -> str:
        return f"({self.path})"

    @property
    def camel_case_name(self) -> str:
        return self.lower_camel_case_name[:1].upper() + self.lower_camel_case_name[1:]

    @property
    def cpp_camel_case_name(self) -> str:
        if self.reference is not None or self.reference_deprecated is not None:
            return filters.reference_field_camel_case(self)
        return self.camel_case_name

    @classmethod
    def make(
        cls,
        parent: "OrmMessage",
        field_descriptor: descriptor.FieldDescriptor,
        package: "OrmPackage",
        field_descriptor_pb2: protobuf_descriptor_pb2.FieldDescriptorProto = None,
    ):
        result = cls(
            parent=parent,
            snake_case_name=field_descriptor.name,
            lower_camel_case_name=field_descriptor.camelcase_name,
            number=field_descriptor.number,
        )

        result._analyze_options(field_descriptor)
        result._analyze_label(field_descriptor, field_descriptor_pb2)
        result._analyze_type(field_descriptor, package)
        result._analyze_yt_type(field_descriptor, package)
        result.description = PATH_TO_DESCRIPTION.get(field_descriptor.full_name, None)

        options = field_descriptor.GetOptions()
        result.foreign_object_type = options.Extensions[object_pb2.foreign_key]
        result.custom_foreign_view_name = options.Extensions[object_pb2.custom_foreign_view_name]
        assert (
            not result.custom_foreign_view_name or result.foreign_object_type
        ), "`custom_foreign_view_name` can be used only with `foreign_key` specified"

        result.references_table_suffix = options.Extensions[object_pb2.references_table_suffix]
        result.reference_name = options.Extensions[object_pb2.reference_name]
        assert (
            not result.references_table_suffix or result.foreign_object_type
        ), "`references_table_suffix` can be used only with `foreign_key` specified"
        assert (
            not result.reference_name or result.foreign_object_type
        ), "`reference_name` can be used only with `foreign_key` specified"
        assert (
            not result.no_foreign_view or result.foreign_object_type
        ), "`no_foreign_view` can be used only with `foreign_key` specified"

        result.allow_mixin_overwrite_options = list(options.Extensions[object_pb2.allow_mixin_overwrite_options])

        result.is_timestamp_attribute = options.Extensions[object_pb2.is_timestamp_attribute]

        result.children_view = options.Extensions[object_pb2.children_view]
        if result.children_view:
            assert result.is_repeated, "Children view fields have to be repeated"
            result.set_not_initializable()
            result.set_computed()
            result.set_opaque()
            result.set_update_policy(object_pb2.UP_OPAQUE_READ_ONLY)

        if options.HasExtension(object_pb2.field_lock_group):
            result._lock_group = options.Extensions[object_pb2.field_lock_group]

        if options.HasExtension(object_pb2.field_data_group):
            result._data_group = options.Extensions[object_pb2.field_data_group]

        if options.HasExtension(object_pb2.method_argument_type):
            result.method_argument_type = options.Extensions[object_pb2.method_argument_type]

        if options.HasExtension(object_pb2.forbid_removal_with_non_empty_references):
            result.forbid_removal_with_non_empty_references = options.Extensions[
                object_pb2.forbid_removal_with_non_empty_references
            ]

        if options.HasExtension(object_pb2.forbid_target_object_removal_with_non_empty_references):
            result.forbid_target_object_removal_with_non_empty_references = options.Extensions[
                object_pb2.forbid_target_object_removal_with_non_empty_references
            ]

        assert not result.forbid_removal_with_non_empty_references or result.foreign_object_type, (
            "`forbid_removal_with_non_empty_references` " "can be used only with `foreign_key` specified"
        )

        assert not result.forbid_target_object_removal_with_non_empty_references or result.foreign_object_type, (
            "`forbid_target_object_removal_with_non_empty_references` " "can be used only with `foreign_key` specified"
        )

        result._analyze_generation_policy(field_descriptor)
        result._analyze_update_policy(field_descriptor)

        transitive_key = options.Extensions[object_pb2.transitive_key]
        if transitive_key:
            result.transitive_key = transitive_key
            result.set_update_policy(object_pb2.UP_OPAQUE_READ_ONLY)

        if result.is_implied_column or result.is_base_column:
            result.set_column()

        if options.HasExtension(object_pb2.store_field_to_meta_response):
            result.store_field_to_meta_response = options.Extensions[object_pb2.store_field_to_meta_response]

        result._custom_column_name = result.get_option(object_pb2.custom_column_name)

        if result._custom_column_name is not None:
            assert (
                result.is_column
            ), f"Custom column name works only for columnar fields, but used on {result.snake_case_name}"

        return result

    @classmethod
    def merge(cls, base: "OrmField", mixin: "OrmField") -> "OrmField":
        if base.proto_value_type == "google.protobuf.Any":
            value_message = mixin.value_message
            proto_value_type = mixin.proto_value_type
            source_module_paths = mixin.source_module_paths
        else:
            assert (
                (mixin.proto_value_type == "google.protobuf.Any")
                or
                # TODO Need better approach to check enums and messages w/o package name.
                base.proto_value_type.rsplit(".", 1)[-1] == mixin.proto_value_type.rsplit(".", 1)[-1]
            ), "Cannot merge fields with mismatching types {} ({} and {})".format(
                base.snake_case_name, base.proto_value_type, mixin.proto_value_type
            )
            value_message = base.value_message
            proto_value_type = base.proto_value_type
            source_module_paths = base.source_module_paths
        assert base.is_required == mixin.is_required, "Cannot merge fields with mismatching required label {}".format(
            base.snake_case_name,
        )
        assert base.is_repeated == mixin.is_repeated, "Cannot merge fields with mismatching repeated label {}".format(
            base.snake_case_name,
        )

        allow_mixin_overwrite_options = base.allow_mixin_overwrite_options + mixin.allow_mixin_overwrite_options

        def optional_attribute_value(attribute: str):
            base_value = getattr(base, attribute)
            mixin_value = getattr(mixin, attribute)

            valid_overwrite = base_value == mixin_value or attribute in allow_mixin_overwrite_options
            assert (
                valid_overwrite or not base_value or not mixin_value
            ), "Cannot merge fields with different {} {} ({} and {})".format(
                attribute,
                base.snake_case_name,
                base_value,
                mixin_value,
            )
            return base_value if base_value is not None else mixin_value

        result = cls(
            snake_case_name=base.snake_case_name,
            lower_camel_case_name=base.lower_camel_case_name,
            proto_value_type=proto_value_type,
            value_message=value_message,
            value_enum=base.value_enum or mixin.value_enum,
            map_key=base.map_key,
            primitive_cpp_value_type=base.primitive_cpp_value_type,
            cpp_yt_value_type=base.cpp_yt_value_type,
            yt_schema_value_type=base.yt_schema_value_type,
            parent=base.parent,
            source_module_paths=source_module_paths,
            option_source_module_paths=base.option_source_module_paths | mixin.option_source_module_paths,
            number=base.number,
            is_primary=base.is_primary or mixin.is_primary,
            store_field_to_meta_response=optional_attribute_value("store_field_to_meta_response"),
            is_required=base.is_required,
            is_repeated=base.is_repeated,
            is_primitive=base.is_primitive or mixin.is_primitive,
            generation_policy=optional_attribute_value("generation_policy"),
            min_length=optional_attribute_value("min_length"),
            max_length=optional_attribute_value("max_length"),
            valid_charset=optional_attribute_value("valid_charset"),
            min_value=optional_attribute_value("min_value"),
            max_value=optional_attribute_value("max_value"),
            indexed_by=base.indexed_by + mixin.indexed_by,
            index_for_increment=optional_attribute_value("index_for_increment"),
            reference=optional_attribute_value("reference"),
            foreign_object_type=optional_attribute_value("foreign_object_type"),
            custom_foreign_view_name=optional_attribute_value("custom_foreign_view_name"),
            forbid_removal_with_non_empty_references=(
                base.forbid_removal_with_non_empty_references or mixin.forbid_removal_with_non_empty_references
            ),
            forbid_target_object_removal_with_non_empty_references=(
                base.forbid_target_object_removal_with_non_empty_references
                or mixin.forbid_target_object_removal_with_non_empty_references
            ),
            references_table_suffix=optional_attribute_value("references_table_suffix"),
            reference_name=optional_attribute_value("reference_name"),
            index_over_reference_table=optional_attribute_value("index_over_reference_table"),
            is_embedded_semaphore=base.is_embedded_semaphore or mixin.is_embedded_semaphore,
            oneof_name=optional_attribute_value("oneof_name"),
            column=optional_attribute_value("column"),
            description=OrmDescription.merge(base.description, mixin.description),
            update_policy=optional_attribute_value("update_policy"),
            proto3_optional=optional_attribute_value("proto3_optional"),
            explicit_presence=optional_attribute_value("explicit_presence"),
            is_control_touch=base.is_control_touch or mixin.is_control_touch,
            foreign_view_key_field=base.foreign_view_key_field or mixin.foreign_view_key_field,
            allow_mixin_overwrite_options=allow_mixin_overwrite_options,
            is_timestamp_attribute=base.is_timestamp_attribute or mixin.is_timestamp_attribute,
            is_access_control_parent=base.is_access_control_parent or mixin.is_access_control_parent,
            method_argument_type=optional_attribute_value("method_argument_type"),
            _add_tags=base._add_tags + mixin._add_tags,
            _remove_tags=base._remove_tags + mixin._remove_tags,
            _lock_group=optional_attribute_value("_lock_group"),
            _data_group=optional_attribute_value("_data_group"),
            _options=OrmField.merge_options(base._options, mixin._options, allow_mixin_overwrite_options),
        )

        if result.reference:
            result.reference.field = result

        return result

    def merge_options(base_options, mixin_options, allowed_overwrite_options):
        def sanitize_name(option_name):
            """Extracts name when option_name is expressed as '(Namespace.OptionName)'."""
            return option_name[1:-1].split(".")[-1]

        result = {name: value for name, value in base_options.items()}
        for name, mixin_value in mixin_options.items():
            if name not in base_options:
                result[name] = mixin_value
                continue

            if sanitize_name(name) in allowed_overwrite_options:
                continue

            value = result[name]
            if isinstance(value, list) or type(value).__name__ == "RepeatedScalarContainer":
                result_values = []
                for v in value:
                    result_values.append(v)
                for v in mixin_value:
                    result_values.append(v)
                result[name] = result_values
            else:
                assert (
                    value == mixin_value
                ), "Cannot merge fields with mismatching singular options {} ({} and {})".format(
                    name,
                    type(value),
                    type(mixin_value),
                )
        return result

    @classmethod
    def make_foreign_view_field_deprecated(
        cls,
        parent_message: "OrmMessage",
        foreign_key_field: "OrmField",
    ):
        snake_case_name = filters.foreign_view_snake_case(foreign_key_field)
        camel_case_name = filters.snake_to_camel_case(snake_case_name)
        target = foreign_key_field.reference_deprecated.table.target
        target_name = target.camel_case_name

        result = cls(
            parent=parent_message,
            snake_case_name=snake_case_name,
            lower_camel_case_name=camel_case_name[:1].lower() + camel_case_name[1:],
            number=foreign_key_field.number + FOREIGN_VIEW_FIELD_OFFSET,
            proto_value_type="{}.T{}".format(parent_message.proto_namespace, target_name),
            is_repeated=foreign_key_field.is_repeated,
            foreign_object_type=foreign_key_field.foreign_object_type,
            foreign_view_key_field=foreign_key_field,
            source_module_paths={target.root.output_filename},
        )

        result.set_not_initializable()
        result.set_computed()
        result.set_opaque()
        result.set_update_policy(object_pb2.UP_OPAQUE_READ_ONLY)
        return result

    @classmethod
    def make_view_field(
        cls,
        name: str,
        number: int,
        parent_message: "OrmMessage",
        reference: OrmReference,
    ):
        snake_case_name = name or filters.foreign_view_snake_case(reference.field)
        camel_case_name = filters.snake_to_camel_case(snake_case_name)
        number = number or (reference.field.number + FOREIGN_VIEW_FIELD_OFFSET)
        target_name = reference.foreign_object.camel_case_name

        result = cls(
            parent=parent_message,
            snake_case_name=snake_case_name,
            lower_camel_case_name=camel_case_name[:1].lower() + camel_case_name[1:],
            number=number,
            proto_value_type="{}.T{}".format(parent_message.proto_namespace, target_name),
            is_repeated=reference.field.is_repeated,
            viewed_reference=reference,
            source_module_paths={reference.foreign_object.root.output_filename},
        )

        result.set_not_initializable()
        result.set_computed()
        result.set_opaque()
        result.set_update_policy(object_pb2.UP_OPAQUE_READ_ONLY)
        return result

    @classmethod
    def make_reference_field_deprecated(
        cls,
        parent_message: "OrmMessage",
        reference_deprecated: OrmReferenceDeprecated,
    ):
        source_object = reference_deprecated.source_object
        camel_case_name = filters.collective_name(source_object) + filters.references_table_suffix_camel_case(
            reference_deprecated.table
        )
        snake_case_name = (
            source_object.snake_case_name
            + filters.references_table_suffix_snake_case(reference_deprecated.table)
            + "_ids"
        )
        assert len(parent_message.object.primary_key) == 1, "Composite foreign keys not yet supported"
        result = cls(
            parent=parent_message,
            snake_case_name=snake_case_name,
            lower_camel_case_name=camel_case_name[:1].lower() + camel_case_name[1:],
            proto_value_type=source_object.primary_key[0].proto_type,
            primitive_cpp_value_type=source_object.primary_key[0].cpp_type,
            cpp_yt_value_type=source_object.primary_key[0].cpp_yt_type,
            yt_schema_value_type=source_object.primary_key[0].schema_yt_type,
            is_repeated=True,
            index_over_reference_table=reference_deprecated.source_field.index_over_reference_table,
            reference_deprecated=reference_deprecated,
        )
        result.set_column()
        return result

    @classmethod
    def make_default_reference_field(
        cls,
        name: str,
        parent_message: "OrmMessage",
        other_reference: OrmReference,
        other_object: "OrmObject",
        other_reference_path: str,
    ):
        camel_case_name = filters.snake_to_camel_case(name)
        assert other_reference.foreign_backref_number
        result = cls(
            parent=parent_message,
            snake_case_name=name,
            lower_camel_case_name=camel_case_name[:1].lower() + camel_case_name[1:],
            proto_value_type="NYT.NOrm.NClient.NProto.TReference",
            is_repeated=True,
            number=other_reference.foreign_backref_number,
        )

        result.reference = OrmReference(
            field=result,
            foreign_type=other_object.snake_case_name,
            foreign_backref_path=other_reference_path,
            key_storage_kind="Tabular",
            generate_view=False,
            store_parent_key=True,
        )

        return result

    def initialize(self, context: OrmContext):
        if self.initialized:
            return
        self.initialized = True

        context.current_fields.append(self)

        if self.map_key:
            self.map_key.initialize(context)

        if self.proto_value_type in context.original_type_renames:
            self.proto_value_type = context.original_type_renames[self.proto_value_type]

        removal_policy = context.package.database_options.object_with_reference_removal_policy
        if self.forbid_removal_with_non_empty_references is None:
            self.forbid_removal_with_non_empty_references = removal_policy in (
                object_pb2.EObjectWithReferenceRemovalPolicy.OWRRP_SOURCE_REMOVAL_FORBIDDEN,
                object_pb2.EObjectWithReferenceRemovalPolicy.OWRRP_REMOVAL_FORBIDDEN,
            )
        if self.forbid_target_object_removal_with_non_empty_references is None:
            self.forbid_target_object_removal_with_non_empty_references = removal_policy in (
                object_pb2.EObjectWithReferenceRemovalPolicy.OWRRP_TARGET_REMOVAL_FORBIDDEN,
                object_pb2.EObjectWithReferenceRemovalPolicy.OWRRP_REMOVAL_FORBIDDEN,
            )

        enum_type_key = "({})".format(object_pb2.enum_type_name.full_name)
        enum_type_name = self._options.get(enum_type_key)
        if enum_type_name:
            if enum_type_name in context.original_type_renames:
                enum_type_name = context.original_type_renames[enum_type_name]
            self._options[enum_type_key] = enum_type_name

        if not self.is_view:  # Don't descend into other objects.
            self.value_message = self.value_message or context.package.get_message(self.proto_value_type)
            if self.value_message:
                self.is_control_touch = (
                    self.value_message.name == "TObjectTouch"
                    and self.value_message.proto_namespace == "NYT.NOrm.NDataModel"
                )
                self.value_message.initialize(context)
            self.value_enum = context.package.enums_by_name.get(self.proto_value_type)
            if self.value_enum is not None:
                self._initialize_enum_field(context.package)
            if self.aggregate is not None:
                if self.bool_option(object_pb2.opaque, default=None) is None:
                    self.set_opaque()
                if self.update_policy is None:
                    self.set_update_policy(object_pb2.UP_OPAQUE_UPDATABLE)

        self._update_source_path()

        if self.reference:
            self.reference.initialize(context)

        context.current_fields.pop()

    def link(self, context: OrmContext):
        if not self.initialized:
            self.initialize(context)
        if self.linked:
            return
        self.linked = True

        context.current_fields.append(self)

        if self.map_key:
            self.map_key.link(context)
        if self.value_message:
            self.value_message.link(context)
        if self.reference:
            self.reference.link(context)

        context.current_fields.pop()

    def finalize(self, context: OrmContext):
        if not self.linked:
            self.link(context)
        if self.finalized:
            return
        self.finalized = True

        context.current_fields.append(self)

        self._make_cpp_accessors(context)

        if self.in_oneof:
            assert not self.is_column and not self.is_primary, f"Oneof field {self.snake_case_name} cannot be columnar"

        if self.aggregate is not None:
            assert self.is_column, f"Aggregate field {self.snake_case_name} must be columnar"
            assert not self.is_primary, f"Aggregate field {self.snake_case_name} cannot be primary"
            assert (
                not self.is_timestamp_attribute
            ), f"Aggregate field {self.snake_case_name} cannot be timestamp attribute"
            assert not self.indexed_by and self.index_over_reference_table is None and not self.used_in_predicate
        if self.map_key:
            self.map_key.finalize(context)
        # Do not call finalize for foreign view, because it must be called on object finalization.
        if self.value_message and not self.is_view:
            self.value_message.finalize(context)
        if self.reference:
            self.reference.finalize(context)
        if self.reference_deprecated:
            self.reference_deprecated.finalize(context)

        context.current_fields.pop()

    def _make_cpp_accessors(self, context: OrmContext):
        if not (self.is_column or self.reference or self.is_view):
            return

        fields = context.current_fields
        assert fields
        if fields[0].snake_case_name == "meta":
            fields = fields[1:]

        self._cpp_getter = "().".join([field.camel_case_name for field in fields]) + "()"

        fields = fields[:-1]

        partial_descriptor = "::T".join(
            [context.current_object.camel_case_name] + [field.camel_case_name for field in fields]
        )

        self._cpp_descriptor = f"T{partial_descriptor}::{self.camel_case_name}Descriptor"

    def set_option(self, option, value):
        self._options[f"({option.full_name})"] = value

    def get_option(self, option, default=None):
        return self._options.get(f"({option.full_name})", default)

    def bool_option(self, option, default=False):
        assert option.type == protobuf_descriptor_pb2.FieldDescriptorProto.TYPE_BOOL
        return self.get_option(option, default)

    def set_column(self):
        if not self.is_column:
            self.set_option(object_pb2.column, True)

    @property
    def is_view(self):
        return self.viewed_reference or self.children_view or self.foreign_view_key_field

    @property
    def strict_enum_value_check(self):
        return self.bool_option(protobuf_interop_pb2.strict_enum_value_check, default=None)

    def set_effective_strict_enum_value_check(self, value: bool):
        if self.strict_enum_value_check is None:
            self.set_option(protobuf_interop_pb2.strict_enum_value_check, value)

    @property
    def opaque(self):
        return self.bool_option(object_pb2.opaque)

    @property
    def emit_opaque_as_entity(self):
        return self.bool_option(object_pb2.emit_opaque_as_entity, True)

    @property
    def is_column(self):
        return self.bool_option(object_pb2.column)

    @property
    def cpp_override(self):
        return self.bool_option(object_pb2.cpp_override)

    @property
    def is_implied_column(self):
        return bool(self.foreign_object_type) or bool(self.transitive_key)

    @property
    def system(self):
        return self.bool_option(object_pb2.system)

    @property
    def is_base_column(self):
        return self.bool_option(object_pb2.base_column)

    @property
    def computed(self):
        return self.bool_option(object_pb2.computed)

    @property
    def mandatory(self):
        return self.bool_option(object_pb2.mandatory)

    @property
    def nullable(self):
        return self.bool_option(object_pb2.nullable, True)

    @property
    def column_name(self):
        if self._custom_column_name:
            return self._custom_column_name
        return self.snake_case_name

    @property
    def is_map(self):
        return bool(self.map_key)

    @property
    def no_foreign_view(self):
        return self.bool_option(object_pb2.no_foreign_view)

    @property
    def deprecated(self):
        return self.bool_option(object_pb2.deprecated) or self.bool_option(object_pb2.hidden_from_client)

    @property
    def ignore_columns(self):
        return self.bool_option(object_pb2.ignore_columns)

    @property
    def preferred_storage_type(self):
        return self.get_option(object_pb2.storage_type)

    @property
    def preferred_enum_yson_storage_type(self):
        return self.get_option(protobuf_interop_pb2.enum_yson_storage_type)

    @property
    def aggregate(self):
        return self.get_option(object_pb2.aggregate)

    @property
    def has_foreign_view(self):
        return self.reference_deprecated is not None and not self.no_foreign_view

    @property
    def proto_type(self):
        if self.is_map:
            return "map<{}, {}>".format(self.map_key.proto_type, self.proto_value_type)
        else:
            return self.proto_value_type

    @property
    def cpp_value_type(self):
        return self.primitive_cpp_value_type or self.proto_value_type.replace(".", "::")

    @property
    def cpp_type(self):
        if self.is_map:
            return f"THashMap<{self.map_key.cpp_type}, {self.cpp_value_type}>"
        elif self.is_repeated:
            return f"std::vector<{self.cpp_value_type}>"
        else:
            return self.cpp_value_type

    @property
    def cpp_proto_type(self):
        if self.is_map:
            return f"::google::protobuf::Map<{self.map_key.cpp_proto_type}, {self.cpp_value_type}>"
        elif self.is_repeated:
            return f"::google::protobuf::RepeatedField<{self.cpp_value_type}>"
        else:
            return self.cpp_value_type

    @property
    def cpp_yt_type(self):
        if self.is_repeated or self.is_map:
            return "NYT::NTableClient::EValueType::Any"
        else:
            return self.cpp_yt_value_type

    @property
    def schema_yt_type(self):
        if self.is_repeated or self.is_map:
            return "any"
        else:
            return self.yt_schema_value_type

    @property
    def lock_group(self):
        if self.is_primary:
            return None
        if self._lock_group:
            return self._lock_group
        if not self.ignore_columns and self.value_message and self.value_message.lock_group:
            return self.value_message.lock_group
        if self.parent and self.parent.lock_group:
            return self.parent.lock_group
        return DEFAULT_LOCK_GROUP

    @property
    def data_group(self):
        if self._data_group:
            return self._data_group
        if not self.ignore_columns and self.value_message and self.value_message.data_group:
            return self.value_message.data_group
        if self.parent and self.parent.data_group:
            return self.parent.data_group
        return DEFAULT_DATA_GROUP

    @property
    def method_arg_type(self):
        if self.method_argument_type:
            return self.method_argument_type
        else:
            return "TVoid"

    @property
    def in_oneof(self) -> bool:
        return bool(self.oneof_name)

    @property
    def options_formatted(self):
        if not self._options:
            return ""
        entries = []
        opaque_options_names = {"({})".format(o.full_name) for o in OPAQUE_OPTIONS}
        for name, value in self._options.items():
            if name in opaque_options_names:
                continue
            proto_value = filters.python_value_to_proto(value)
            if isinstance(proto_value, list):
                entries.extend("{} = {}".format(name, x) for x in proto_value)
            else:
                entries.append("{} = {}".format(name, proto_value))
        if not entries:
            return ""
        return " [{}]".format(", ".join(entries))

    @property
    def has_custom_policy(self) -> bool:
        return self.generation_policy == "Custom"

    @property
    def is_parent_key_field(self):
        return self.column and self.column.is_parent_key_part

    def is_optional(self, proto3: bool) -> bool:
        return self.label(proto3) == "optional "

    def label(self, proto3: bool):
        if self.is_repeated:
            return "repeated "
        if self.is_required:
            return "required "
        if (proto3 and not self.explicit_presence) or self.is_map:
            return ""
        return "optional "

    def set_update_policy(self, policy: int):
        self.set_option(object_pb2.update_policy, object_pb2.update_policy.enum_type.values_by_number[policy])
        if policy == object_pb2.UP_UPDATABLE:
            self.update_policy = "Updatable"
        elif policy == object_pb2.UP_OPAQUE_UPDATABLE:
            self.update_policy = "OpaqueUpdatable"
        elif policy == object_pb2.UP_OPAQUE_READ_ONLY:
            self.update_policy = "OpaqueReadOnly"
        elif policy == object_pb2.UP_READ_ONLY:
            self.update_policy = "ReadOnly"
        else:
            raise Exception("Unknown update policy {}".format(policy))

        if "ReadOnly" in self.update_policy:
            self._set_not_updatable()

    def _set_not_updatable(self):
        self.set_option(object_pb2.not_updatable, True)

    def set_opaque(self):
        self.set_option(object_pb2.opaque, True)

    def set_computed(self):
        self.set_option(object_pb2.computed, True)

    def set_system(self):
        self.set_option(object_pb2.system, True)

    def set_is_base_column(self, new_value=True):
        self.set_option(object_pb2.base_column, new_value)

    def set_not_initializable(self):
        self.set_option(object_pb2.not_initializable, True)

    def set_yson_map(self):
        self.set_option(protobuf_interop_pb2.yson_map, True)

    def set_lock_group(self, lock_group):
        self._lock_group = lock_group

    def set_name(self, snake_case_name):
        self.snake_case_name = snake_case_name
        self.lower_camel_case_name = filters.decapitalize(filters.snake_to_camel_case(snake_case_name))

    def set_column_name(self, column_name: str):
        self._custom_column_name = column_name

    def _analyze_label(
        self,
        field_descriptor: descriptor.FieldDescriptor,
        field_descriptor_pb2: protobuf_descriptor_pb2.FieldDescriptorProto,
    ):
        if field_descriptor_pb2 and field_descriptor_pb2.proto3_optional:
            self.proto3_optional = True

        if field_descriptor.label == protobuf_descriptor_pb2.FieldDescriptorProto.LABEL_REQUIRED:
            # TODO: steer everybody away from |required|
            self.is_required = True
        if field_descriptor.label == protobuf_descriptor_pb2.FieldDescriptorProto.LABEL_REPEATED:
            self.is_repeated = True
        if hasattr(field_descriptor, "has_presence"):
            self.explicit_presence = field_descriptor.has_presence
        elif self.proto3_optional:
            self.explicit_presence = True
        elif hasattr(field_descriptor, "file") and hasattr(field_descriptor.file, "syntax"):
            self.explicit_presence = field_descriptor.file.syntax == "proto2"
        else:
            self.explicit_presence = None

    def _analyze_type(self, field_descriptor: descriptor.FieldDescriptor, package: "OrmPackage"):
        if field_descriptor.type == protobuf_descriptor_pb2.FieldDescriptorProto.TYPE_MESSAGE:
            self.is_primitive = False
            if field_descriptor.message_type.has_options and field_descriptor.message_type.GetOptions().map_entry:
                if not compat.yp():
                    self.set_yson_map()
                self.is_repeated = False

                value_field_descriptor = field_descriptor.message_type.fields_by_name["value"]
                # Proto maps are never nested, so this will set up value properties correctly.
                # Make sure there are no "leaks".
                self._analyze_type(value_field_descriptor, package)

                key_field_descriptor = field_descriptor.message_type.fields_by_name["key"]
                self.map_key = OrmField.make(self.parent, key_field_descriptor, package)

                assert (
                    not self.is_column
                    or value_field_descriptor.type == protobuf_descriptor_pb2.FieldDescriptorProto.TYPE_MESSAGE
                ), f'Map field {self.snake_case_name} with scalar value cannot be a column'
            else:
                self.proto_value_type = field_descriptor.message_type.full_name
                self.source_module_paths.add(field_descriptor.message_type.file.name)
                self.is_embedded_semaphore = field_descriptor.message_type.name == "TEmbeddedSemaphore"

        elif field_descriptor.type == protobuf_descriptor_pb2.FieldDescriptorProto.TYPE_ENUM:
            self.proto_value_type = field_descriptor.enum_type.full_name
            self.source_module_paths.add(field_descriptor.enum_type.file.name)

        else:
            type_description = TYPE_DESCRIPTIONS[field_descriptor.type]
            self.proto_value_type = type_description.proto_type
            self.primitive_cpp_value_type = type_description.cpp_type

    def _analyze_yt_type(self, field_descriptor: descriptor.FieldDescriptor, package: "OrmPackage"):
        type_description = TYPE_DESCRIPTIONS[field_descriptor.type]

        if self.preferred_storage_type:
            assert (
                self.preferred_storage_type in type_description.compatible_storage_types_by_priority
            ), "Bad preferred storage type {} for {}, compatible storage types: {}".format(
                self.preferred_storage_type, self.snake_case_name, type_description.compatible_storage_types_by_priority
            )
            self.yt_schema_value_type = self.preferred_storage_type
        elif (
            field_descriptor.type == protobuf_descriptor_pb2.FieldDescriptorProto.TYPE_ENUM
            and package.database_options.default_enum_storage_type
        ):
            default_enum_storage_type = package.database_options.default_enum_storage_type
            assert (
                default_enum_storage_type in type_description.compatible_storage_types_by_priority
            ), "Bad default enum storage type {} for {}, compatible storage types: {}".format(
                default_enum_storage_type, self.snake_case_name, type_description.compatible_storage_types_by_priority
            )
            self.yt_schema_value_type = default_enum_storage_type
        else:
            self.yt_schema_value_type = type_description.compatible_storage_types_by_priority[0]

        self.cpp_yt_value_type = storage_type_to_cpp_yt_type(self.yt_schema_value_type)

    def _update_source_path(self):
        if not self.source_module_paths:
            return
        new_source_module_paths = set()
        for source_module_path in self.source_module_paths:
            value_struct = self.value_enum or self.value_message
            while value_struct and value_struct.filename == source_module_path:
                if not value_struct.output_to_default and value_struct.output_filename:
                    new_path = posixpath.join(posixpath.dirname(source_module_path), value_struct.output_filename)
                    new_source_module_paths.add(new_path)
                    break
                value_struct = value_struct.containing_message
            else:
                new_source_module_paths.add(source_module_path)
        self.source_module_paths = new_source_module_paths

    def _analyze_options(self, field_descriptor: descriptor.FieldDescriptor):
        if field_descriptor.has_default_value:
            value = field_descriptor.default_value
            if field_descriptor.enum_type:
                enum_value = field_descriptor.enum_type.values_by_number[value]
                self._options["default"] = enum_value
            else:
                self._options["default"] = value

        # NB: protobufs and ORM have separate deprecation mechanisms.
        if field_descriptor.GetOptions().deprecated:
            self._options["deprecated"] = True

        extensions = field_descriptor.GetOptions().Extensions
        for option in extensions:
            if option is object_pb2.reference:
                self.reference = OrmReference.make(self, extensions[option])
            ext = resolve_options_value(option, extensions[option], self)
            self._options["({})".format(option.full_name)] = ext

        if field_descriptor.type == protobuf_descriptor_pb2.FieldDescriptorProto.TYPE_ENUM:
            if self.preferred_enum_yson_storage_type is not None:
                assert (
                    self.preferred_storage_type is not None
                ), "`enum_yson_storage_type` option should be used only in pair with `storage_type` option. (Field: {})".format(
                    self.snake_case_name
                )
                type_description = TYPE_DESCRIPTIONS[field_descriptor.type]
                assert (
                    type_description.storage_type_to_enum_yson_storage_type[self.preferred_storage_type]
                    == self.preferred_enum_yson_storage_type
                ), (
                    f'`storage_type` "{self.preferred_storage_type}" and `enum_yson_storage_type` '
                    f'"{self.preferred_enum_yson_storage_type}" options conflict for field "{self.snake_case_name}"'
                )
        else:
            assert (
                self.preferred_enum_yson_storage_type is None
            ), f"Non enum field {self.snake_case_name} can not have `enum_yson_storage_type` option"

    def compute_tags(self, tags_enum: OrmEnum):
        add_tags = self.get_option(object_pb2.add_tags, [])
        remove_tags = self.get_option(object_pb2.remove_tags, [])
        if self.system:
            add_tags.append("system")
        if self.computed:
            add_tags.append("computed")
        if self.mandatory:
            add_tags.append("mandatory")
        if self.opaque:
            add_tags.append("opaque")
        if self.bool_option(object_pb2.not_updatable):
            add_tags.append("readonly")

        self._add_tags = [enum_value_name_to_value(tags_enum, tag) for tag in add_tags]
        self._remove_tags = [enum_value_name_to_value(tags_enum, tag) for tag in remove_tags]

    def _analyze_generation_policy(self, field_descriptor: descriptor.FieldDescriptor):
        options = field_descriptor.GetOptions()

        policy = options.Extensions[object_pb2.generation_policy]
        if not policy or policy == object_pb2.GP_UNDEFINED:
            self.generation_policy = None
        elif policy == object_pb2.GP_MANUAL:
            self.generation_policy = "Manual"
        elif policy == object_pb2.GP_RANDOM:
            self.generation_policy = "Random"
        elif policy == object_pb2.GP_TIMESTAMP:
            self.generation_policy = "Timestamp"
        elif policy == object_pb2.GP_BUFFERED_TIMESTAMP:
            self.generation_policy = "BufferedTimestamp"
        elif policy == object_pb2.GP_INDEXED_INCREMENT:
            self.generation_policy = "IndexedIncrement"
        elif policy == object_pb2.GP_CUSTOM:
            self.generation_policy = "Custom"
        else:
            raise Exception("Unknown generation policy {}".format(policy))

        self.min_length = options.Extensions[object_pb2.min_length]
        self.max_length = options.Extensions[object_pb2.max_length]
        self.valid_charset = options.Extensions[object_pb2.valid_charset]

        assert self.primitive_cpp_value_type == "TString" or not (
            self.min_length or self.max_length or self.valid_charset
        ), "Policy parameters `min_length`, `max_length` and `valid_charset` are supported only for string fields"

        assert self.generation_policy is not None or not (self.min_length or self.max_length or self.valid_charset), (
            "Policy parameters `min_length`, `max_length` and `valid_charset` are supported only "
            "when `generation_policy` is explicitly provided"
        )

        self.min_value = options.Extensions[object_pb2.min_value]
        self.max_value = options.Extensions[object_pb2.max_value]

        assert (
            self.primitive_cpp_value_type
            and self.primitive_cpp_value_type != "TString"
            or not (self.min_value or self.max_value)
        ), "Policy parameters `min_value` and `max_value` are supported only for integer fields"

        assert self.generation_policy is not None or not (self.min_value or self.max_value), (
            "Policy parameters `min_value` and `max_value` are supported only "
            "when `generation_policy` is explicitly provided"
        )

        if self.valid_charset:
            self.valid_charset = '"{}"'.format(self.valid_charset)

        self.index_for_increment = options.Extensions[object_pb2.index_for_increment]
        assert bool(self.index_for_increment) == (self.generation_policy == "IndexedIncrement"), (
            'Policy parameter `index_for_increment` is required and is only supported '
            'with the "indexed_increment" `generation_policy`'
        )

    def _analyze_update_policy(self, field_descriptor: descriptor.FieldDescriptor):
        options = field_descriptor.GetOptions()
        assert not (
            options.HasExtension(object_pb2.update_policy) and options.HasExtension(object_pb2.not_updatable)
        ), f"`not_updatable` option is deprecated, use `update_policy` for field {self.snake_case_name}"
        policy = options.Extensions[object_pb2.update_policy]
        not_updatable = options.Extensions[object_pb2.not_updatable]

        if policy:
            self.set_update_policy(policy)
        elif not_updatable:
            self.set_update_policy(object_pb2.UP_READ_ONLY)

    def _initialize_enum_field(self, package: "OrmPackage"):
        assert self.value_enum is not None

        # Public enums get C++ variants.
        self.primitive_cpp_value_type = self.value_enum.name

        if self.is_map:
            # Configuration enum map field's value storage type with preferred_(yson_)?storage_type is not implemented,
            # https://st.yandex-team.ru/YTORM-889.
            assert self.preferred_storage_type is None and self.preferred_enum_yson_storage_type is None, (
                f"Map field {self.snake_case_name} enum value storage type can only be set "
                "via OrmDatabaseOptions's `default_enum_storage_type` db_option"
            )
            return

        need_protobuf_interop_module = False
        if not self.preferred_storage_type:
            assert package.database_options.default_enum_storage_type
            self.set_option(object_pb2.storage_type, package.database_options.default_enum_storage_type)
            need_protobuf_interop_module = True
        if not self.preferred_enum_yson_storage_type:
            option = self.preferred_storage_type or package.database_options.default_enum_storage_type
            assert option
            self.set_option(
                protobuf_interop_pb2.enum_yson_storage_type,
                TYPE_DESCRIPTIONS[
                    protobuf_descriptor_pb2.FieldDescriptorProto.TYPE_ENUM
                ].storage_type_to_enum_yson_storage_type[option],
            )
            need_protobuf_interop_module = True

        # Option `*storage_type` requires module protobuf_interop.proto,
        # so need add it to `option_source_module_paths`.
        if need_protobuf_interop_module:
            self.option_source_module_paths.add("yt_proto/yt/core/yson/proto/protobuf_interop.proto")

        # Fix #yt_schema_value_type according to #option_value because of disambiguation in OrmMessage merge ordering, https://st.yandex-team.ru/YTORM-924.
        # Only field options can be our source of truth now. :(
        if self.preferred_storage_type != self.yt_schema_value_type:
            self.yt_schema_value_type = self.preferred_storage_type

        # "string_serializable" flag is used only for enum that is stored in separate column,
        # right defines are selected based on it.
        if self.is_column:
            string_serializable = bool(self.yt_schema_value_type == "string")
            if self.value_enum.string_serializable is not None:
                assert (
                    self.value_enum.string_serializable == string_serializable
                ), f"Columnar enum {self.proto_value_type} cannot simultaneously have different storage types"
            else:
                self.value_enum.string_serializable = string_serializable
