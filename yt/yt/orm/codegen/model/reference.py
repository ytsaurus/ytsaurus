from .common import (
    OrmColumn,
    OrmContext,
    OrmReferencesTableCardinality,
    OrmReferenceType,
    object_key_constructor_params,
)
from .filters import (
    references_table_snake_case,
    references_table_camel_case,
    to_cpp_bool,
)

from yt_proto.yt.orm.client.proto import object_pb2

from dataclasses import dataclass, field as dataclass_field
from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from .object import OrmAttribute, OrmObject
    from .field import OrmField
    from .index import OrmIndex


@dataclass
class OrmReferencesTable:
    source_cardinality: OrmReferencesTableCardinality
    source: "OrmObject"
    target: "OrmObject"
    indexed: bool
    hash_expression: Optional[str] = None
    suffix_snake_case: Optional[str] = None
    target_name_snake_case: Optional[str] = None
    store_parent_key: bool = False

    @property
    def is_self_referential(self):
        return self.target == self.source or (self.indexed and self.source.parent == self.target)

    @property
    def table_name(self):
        """The primary YT table storing this reference."""
        return references_table_snake_case(self)

    @property
    def table_columns(self):
        if self.hash_expression:
            yield OrmColumn.make_hash(expression=self.hash_expression, group=None)

        yield from self.target_key_columns

        yield from self.source_key_columns

        yield OrmColumn.make_dummy()

    @property
    def target_key_columns(self):
        yield from self.target.key_columns(
            target=True,
            self_ref=self.is_self_referential,
            override_prefix=self.target_name_snake_case,
            group=None,
        )

    @property
    def source_key_columns(self):
        if self.store_parent_key or (self.indexed and self.source.parent):
            yield from self.source.parent.key_columns(
                source=True,
                parent=True,
                self_ref=self.is_self_referential,
                group=None,
            )

        yield from self.source.key_columns(source=True, self_ref=self.is_self_referential, group=None)

    def full_cpp_name(self, cpp_name):
        return f"{references_table_camel_case(self)}Table.Fields.{cpp_name}"


@dataclass
class OrmReferenceDeprecated:  # Generates the deprecated $CardinalityTo$Cardinality attributes.
    type: OrmReferenceType
    table: OrmReferencesTable
    source_object: "OrmObject"
    source_attribute: Optional["OrmAttribute"] = None
    source_field: Optional["OrmField"] = None
    forbid_non_empty_removal: bool = False
    nullable: bool = True

    def __repr__(self) -> str:
        return f"(type={self.type},table={self.table},object={self.source_object},field={self.source_field})"

    @property
    def is_source_side(self):
        return not self.source_field

    @property
    def owner(self) -> "OrmObject":
        return self.table.source if self.is_source_side else self.table.target

    @property
    def foreign_object(self) -> "OrmObject":
        return self.table.target if self.is_source_side else self.table.source

    def finalize(self, context: OrmContext):
        self.owner.foreign_objects.append(self.foreign_object)


@dataclass
class OrmReference:
    field: "OrmField"
    foreign_type: str
    foreign_backref_path: str
    foreign_backref_number: int = 0
    key_storage_kind: Optional[str] = None
    key_storage_options: dict[str, str] = dataclass_field(default_factory=dict)

    store_parent_key: Optional[bool] = None
    allow_non_empty_removal: Optional[bool] = None
    local_modification_only: Optional[bool] = None

    _modification_policy: Optional[str] = None
    _presence_policy: Optional[str] = None

    foreign_object: Optional["OrmObject"] = None
    foreign_backref_field: Optional["OrmField"] = None
    table: Optional[OrmReferencesTable] = None
    matched_indices: list["OrmIndex"] = dataclass_field(default_factory=list)

    initialized: bool = False
    linked: bool = False
    finalized: bool = False

    columnar_key_storage_paths: Optional[list[str]] = None
    proto_key_storage_path: Optional[str] = None
    proto_key_storage_suffixes: Optional[list[str]] = None

    table_suffix: Optional[str] = None
    override_prefix: Optional[str] = None

    view_path: Optional[str] = None
    view_number: Optional[int] = None
    generate_view: bool = True

    def __repr__(self):
        return f"(type={self.foreign_type},object={self.foreign_object},field={self.field})"

    @classmethod
    def make(
        cls,
        field: "OrmField",
        reference_option: object_pb2.TReferenceOption,
    ):
        result = cls(
            field=field,
            foreign_type=reference_option.foreign_type,
            foreign_backref_path=reference_option.foreign_backref_path,
        )

        if reference_option.HasField("foreign_backref_number"):
            assert reference_option.foreign_backref_number
            result.foreign_backref_number = reference_option.foreign_backref_number

        if reference_option.HasField("columnar_key_storage"):
            result.key_storage_kind = "Columnar"

            assert reference_option.columnar_key_storage.paths
            result.columnar_key_storage_paths = reference_option.columnar_key_storage.paths

        elif reference_option.HasField("proto_key_storage"):
            result.key_storage_kind = "Proto"

            assert reference_option.proto_key_storage.path
            assert reference_option.proto_key_storage.suffixes
            result.proto_key_storage_path = reference_option.proto_key_storage.path
            result.proto_key_storage_suffixes = reference_option.proto_key_storage.suffixes

        elif reference_option.HasField("tabular_key_storage"):
            result.key_storage_kind = "Tabular"
            result.table_suffix = reference_option.tabular_key_storage.table_suffix
            result.override_prefix = reference_option.tabular_key_storage.override_prefix
            result.generate_view = False
            result.store_parent_key = True  # Default to table keys in tables.

        else:
            raise NotImplementedError

        result._analyze_settings(reference_option)

        if reference_option.HasField("view"):
            result.view_path = reference_option.view.path
            result.view_number = reference_option.view.number

            if not result.view_path:
                result.generate_view = False

        return result

    @property
    def kind(self):
        if self.field.is_repeated:
            return "Multi"
        else:
            return "Single"

    @property
    def settings(self):
        result = {}
        if self.store_parent_key is not None:
            result["StoreParentKey"] = to_cpp_bool(self.store_parent_key)
        if self.allow_non_empty_removal is not None:
            result["AllowNonEmptyRemoval"] = to_cpp_bool(self.allow_non_empty_removal)
        if self.local_modification_only is not None:
            result["LocalModificationOnly"] = to_cpp_bool(self.local_modification_only)
        return result

    @property
    def key_storage_paths(self):
        if self.columnar_key_storage_paths:
            return self.columnar_key_storage_paths
        if self.proto_key_storage_path:
            return [self.proto_key_storage_path]
        return []

    def initialize(self, context: OrmContext):
        assert not self.initialized
        self.initialized = True
        self.field = context.current_fields[-1]

    def link(self, context: OrmContext):
        assert self.initialized
        assert not self.linked
        self.linked = True

        assert self.field == context.current_fields[-1]
        self.foreign_object = context.object_by_snake_case_name[self.foreign_type]
        if not self.foreign_object.parent:
            self.store_parent_key = False
        foreign_backref_fields = self.foreign_object.resolve_fields(self.foreign_backref_path, True)
        foreign_backref_field = foreign_backref_fields[-1]  # Not self yet.
        if foreign_backref_field.proto_value_type != "NYT.NOrm.NClient.NProto.TReference":
            foreign_message = foreign_backref_field.value_message
            assert foreign_message, f"No message at {self.foreign_backref_path}"
            name = self.foreign_backref_path.rsplit("/", 1)[-1]
            foreign_backref_field = foreign_message.add_default_reference_field(
                name,
                self,
                context.current_object,
                context.current_path,
            )

        if self.foreign_backref_number:
            assert self.foreign_backref_number == foreign_backref_field.reference.field.number
        else:
            self.foreign_backref_number = foreign_backref_field.reference.field.number

        for path in self.key_storage_paths:
            fields = context.current_object.resolve_fields(path)
            for field in fields:
                for index in field.indexed_by:
                    foreign_backref_field.reference._link_index(index, self.key_storage_paths)

        if self.generate_view:
            view_name = None
            view_message = context.current_fields[-2].value_message
            if self.view_path:
                view_fields = context.current_object.resolve_fields(self.view_path, True)
                view_field = view_fields[-1]
                view_message = view_field.value_message
                assert view_message, f"No message at {self.view_path}"
                view_name = self.view_path.rsplit("/", 1)[-1]
            view_field = view_message.add_view_field(view_name, self.view_number, self)
            if not self.view_path:
                self.view_path = f"{context.current_path_except_last_field}/{view_field.snake_case_name}"
            if not self.view_number:
                self.view_number = view_field.number

    def _link_index(self, index: "OrmIndex", paths: list[str]):
        if index in self.matched_indices or index.predicate:
            return

        if self.key_storage_kind != "Tabular":
            return

        for attribute in index.index_attributes:
            if attribute.full_path not in paths:
                return

        self.matched_indices.append(index)

    def finalize(self, context: OrmContext):
        assert self.linked
        assert not self.finalized
        self.finalized = True

        context.current_object.foreign_objects.append(self.foreign_object)
        foreign_backref_fields = self.foreign_object.resolve_fields(self.foreign_backref_path, True)
        self.foreign_backref_field = foreign_backref_fields[-1]
        assert self.foreign_backref_field.proto_value_type == "NYT.NOrm.NClient.NProto.TReference"
        assert self.foreign_backref_field.reference.foreign_backref_path == context.current_path, (
            f"Foreign reference path {self.foreign_backref_field.reference.foreign_backref_path} "
            f"does not correspond to current {context.current_path}"
        )

        storage_key_length = len(self.foreign_object.primary_key)
        if self.store_parent_key:
            storage_key_length += len(self.foreign_object.parent.primary_key)

        if self.key_storage_kind == "Columnar":
            assert storage_key_length == len(self.columnar_key_storage_paths)
            locators: list[str] | str = []
            for path in self.columnar_key_storage_paths:
                descriptor, suffix = self._find_descriptor(context.current_object, path)
                locators.append(f'{{ &{descriptor}, "{suffix}" }}')

            locators = ", ".join(locators)
            self.key_storage_options["KeyLocators"] = f"{{ {locators} }}"

        elif self.key_storage_kind == "Proto":
            assert storage_key_length == len(self.proto_key_storage_suffixes)
            descriptor, proto_suffix = self._find_descriptor(context.current_object, self.proto_key_storage_path)
            self.key_storage_options["KeyAttributeDescriptor"] = f"&{descriptor}"
            self.key_storage_options["ProtoSuffix"] = f'"{proto_suffix}"'
            if proto_suffix.endswith("/*"):
                proto_suff = proto_suffix.removesuffix("/*")
                self.key_storage_options["ProtoSuffixEnd"] = f'"{proto_suff}/end"'

            field_suffixes: list[str] | str = []
            suffixes: list[str] | str = []

            for suffix in self.proto_key_storage_suffixes:
                field_suffixes.append(f'"{suffix}"')
                suffixes.append(f'"{proto_suffix}{suffix}"')
            field_suffixes = ", ".join(field_suffixes)
            suffixes = ", ".join(suffixes)

            self.key_storage_options["FieldSuffixes"] = f"{{ {field_suffixes} }}"
            self.key_storage_options["Suffixes"] = f"{{ {suffixes} }}"

        elif self.key_storage_kind == "Tabular":
            self.table = OrmReferencesTable(
                source_cardinality=(
                    OrmReferencesTableCardinality.MANY
                    if self.foreign_backref_field.reference.field.is_repeated
                    else OrmReferencesTableCardinality.ONE
                ),
                source=self.foreign_object,
                target=context.current_object,
                indexed=bool(self.matched_indices),
                suffix_snake_case=self.table_suffix,
                target_name_snake_case=self.override_prefix,
                store_parent_key=self.store_parent_key,
            )
            context.references_tables.append(self.table)
            for index in self.matched_indices:
                index.underlying_table = references_table_camel_case(self.table)
                index.underlying_table_snake_case = references_table_snake_case(self.table)

            self.key_storage_options["Table"] = f"&{references_table_camel_case(self.table)}Table"

            owner_key_fields: list[str] | str = [
                "&" + self.table.full_cpp_name(column.cpp_name)
                for column in self.table.target_key_columns
                if not column.is_parent_key_part
            ]
            owner_key_fields = ", ".join(owner_key_fields)
            self.key_storage_options["OwnerKeyFields"] = f"{{ {owner_key_fields} }}"

            foreign_key_fields: list[str] | str = [
                "&" + self.table.full_cpp_name(column.cpp_name) for column in self.table.source_key_columns
            ]
            foreign_key_fields = ", ".join(foreign_key_fields)
            self.key_storage_options["ForeignKeyFields"] = f"{{ {foreign_key_fields} }}"

        else:
            raise NotImplementedError

        self._finalize_settings()

    def _find_descriptor(self, obj: "OrmObject", path: str) -> tuple[str, str]:
        descriptor_prefix = "T" + obj.camel_case_name
        descriptor = ""
        suffix = ""
        fields = obj.resolve_fields(path)
        if fields[0].camel_case_name == "Meta":
            fields = fields[1:]
        for field in fields:
            if field.is_column:
                descriptor = f"{descriptor_prefix}::{field.camel_case_name}Descriptor"
                suffix = ""
            elif etcs := field.parent.etcs:
                for etc in etcs:
                    if field not in etc.fields:
                        continue
                    descriptor = f"{descriptor_prefix}::{etc.camel_case_name}EtcDescriptor"
                    suffix = f"/{field.snake_case_name}"
            else:
                suffix = f"{suffix}/{field.snake_case_name}"

            descriptor_prefix = f"{descriptor_prefix}::T{field.camel_case_name}"
            if field.is_repeated:
                suffix = f"{suffix}/*"

        assert descriptor
        return descriptor, suffix

    def _analyze_settings(self, option: object_pb2.TReferenceOption):
        if option.HasField("store_parent_key"):
            self.store_parent_key = option.store_parent_key
        if option.HasField("allow_non_empty_removal"):
            self.allow_non_empty_removal = option.allow_non_empty_removal
        if option.HasField("local_modification_only"):
            self.local_modification_only = option.local_modification_only

        match option.presence_policy:
            case object_pb2.TReferenceOption.PP_DEFAULT:
                pass
            case object_pb2.TReferenceOption.PP_CONTAINER:
                self._presence_policy = "container_entry"
            case object_pb2.TReferenceOption.PP_NULL_KEY_CHECK:
                self._presence_policy = "null_key"
            case _:
                raise NotImplementedError

    def _finalize_settings(self):
        if self.local_modification_only is None:
            hazardous_modification = (
                self.key_storage_kind == "Proto" and self.field.is_repeated and self._presence_policy != "null_key"
            )
            assert not hazardous_modification, (
                f"Repeated proto local storage at {self.field.path} may contain auxiliary data "
                "that could be lost when detaching references. Either explicitly allow/disallow "
                "|local_modification_only| or choose the PP_NULL_KEY presence policy."
            )

        if not self._presence_policy:
            if self.field.is_repeated:
                self._presence_policy = "container_entry"
            else:
                self._presence_policy = "null_key"

        match self._presence_policy:
            case "container_entry":
                pass
            case "null_key":
                assert (
                    self.key_storage_kind == "Proto" or not self.field.is_repeated
                ), f"Null key presence policy is not compatible with {self.field.path}"
                self.key_storage_options["NullKey"] = self._foreign_null_key_constructor()
            case _:
                raise NotImplementedError

    def _foreign_null_key_constructor(self):
        null_key_values = self.foreign_object.null_key_values
        key_fields = self.foreign_object.key_fields
        if self.store_parent_key and self.foreign_object.parent:
            null_key_values = self.foreign_object.parent.null_key_values + null_key_values
            key_fields = self.foreign_object.all_key_fields
        params = object_key_constructor_params(null_key_values, key_fields)
        return f'NYT::NOrm::NClient::NObjects::TObjectKey{{ {params} }}'


@dataclass
class OrmTransitiveReferenceLink:
    object_from: "OrmObject"
    object_to: "OrmObject"
    field: "OrmField"


@dataclass
class OrmTransitiveReference:
    owner: "OrmObject"
    foreign_object: "OrmObject"
    links: list[OrmTransitiveReferenceLink]
