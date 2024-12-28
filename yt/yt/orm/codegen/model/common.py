from dataclasses import dataclass, field as dataclass_field
from enum import Enum
from typing import Optional, Union, TYPE_CHECKING

if TYPE_CHECKING:
    from .object import OrmObject
    from .field import OrmField
    from .package import OrmModule, OrmPackage
    from .enum import OrmEnum
    from .index import OrmIndex
    from .reference import OrmReferencesTable
    from .snapshot import OrmSnapshot
    from .message import OrmMessage

DEFAULT_LOCK_GROUP = "api"
DEFAULT_DATA_GROUP = "default"
PRIMARY_DATA_GROUP = "primary"
ETC_NAME_SEPARATOR = "$"


def storage_type_to_cpp_yt_type(storage_type: str) -> str:
    mapping = {
        "int32": "NYT::NTableClient::EValueType::Int64",
        "int64": "NYT::NTableClient::EValueType::Int64",
        "uint32": "NYT::NTableClient::EValueType::Uint64",
        "uint64": "NYT::NTableClient::EValueType::Uint64",
        "string": "NYT::NTableClient::EValueType::String",
        "utf8": "NYT::NTableClient::EValueType::String",
        "boolean": "NYT::NTableClient::EValueType::Boolean",
        "float": "NYT::NTableClient::EValueType::Double",
        "double": "NYT::NTableClient::EValueType::Double",
        "any": "NYT::NTableClient::EValueType::Any",
    }
    assert storage_type in mapping, f"Unrecognized storage type {storage_type}"
    return mapping[storage_type]


@dataclass
class OrmContext:
    """Gradually accumulating workset of the model building process."""

    # Initial data.
    aux_parameters: dict
    source_packages: list
    inplace_packages: list

    # Filled at |initialize|.
    package: Optional["OrmPackage"] = None
    object_type_options: Optional[list["OrmObject"]] = None
    objects: Optional[list["OrmObject"]] = None
    object_by_camel_case_name: dict[str, "OrmObject"] = dataclass_field(default_factory=dict)
    object_by_snake_case_name: dict[str, "OrmObject"] = dataclass_field(default_factory=dict)
    original_type_renames: dict[str, str] = dataclass_field(default_factory=dict)

    # Filled at |link|.
    tags_enum: Optional["OrmEnum"] = None
    indexes: Optional[list["OrmIndex"]] = None
    references_tables: Optional[list["OrmReferencesTable"]] = None
    snapshots: Optional[dict[str, "OrmSnapshot"]] = None

    # Temporary set during traversals
    current_object: Optional["OrmObject"] = None
    current_foreign_objects: set["OrmObject"] = dataclass_field(default_factory=set)
    current_fields: list["OrmField"] = dataclass_field(default_factory=list)

    @property
    def current_path(self) -> str:
        return self._get_path(self.current_fields)

    @property
    def current_path_except_last_field(self) -> str:
        assert self.current_fields
        return self._get_path(self.current_fields[:-1])

    @staticmethod
    def _get_path(fields: list["OrmField"]) -> str:
        field_names = [""] + [field.snake_case_name for field in fields]
        return "/".join(field_names)


class OrmReferencesTableCardinality(Enum):
    ONE = "One"
    MANY = "Many"


class OrmReferenceType(Enum):
    ONE_TO_MANY = "OneToMany"
    MANY_TO_ONE = "ManyToOne"
    MANY_TO_MANY_INLINE = "ManyToManyInline"
    MANY_TO_MANY_TABULAR = "ManyToManyTabular"


@dataclass
class OrmColumn:
    name: str
    type: str

    sort_order: Optional[str] = None
    lock: Optional[str] = None
    group: Optional[str] = None
    expression: Optional[str] = None
    aggregate: Optional[str] = None

    cpp_name: Optional[str] = None
    cpp_yt_type: Optional[str] = None

    is_reference_source: Optional[bool] = False
    is_reference_target: Optional[bool] = False
    is_parent_key_part: Optional[bool] = False
    is_base: Optional[bool] = False
    is_dummy: Optional[bool] = False

    field: Optional["OrmField"] = None

    def __post_init__(self):
        assert (
            not self.sort_order or not self.lock
        ), f"Table key column {self.name} cannot have both sort order and lock group present"

        if not self.group:
            self.group = DEFAULT_DATA_GROUP
        if not self.lock and not self.is_key and not self.is_dummy:
            self.lock = DEFAULT_LOCK_GROUP

    @classmethod
    def make(cls, field, prefix="", cpp_prefix="", **kwargs):
        kwargs.setdefault("name", f"{prefix}.{field.column_name}")
        kwargs.setdefault("type", field.schema_yt_type)
        kwargs.setdefault("group", field.data_group)
        if field.is_primary:
            kwargs.setdefault("sort_order", "ascending")
        else:
            kwargs.setdefault("lock", field.lock_group)

        kwargs.setdefault("cpp_name", f"{cpp_prefix}{field.camel_case_name}")
        kwargs.setdefault("cpp_yt_type", field.cpp_yt_type)

        kwargs.setdefault("aggregate", field.aggregate)
        kwargs.setdefault("is_base", field.is_base_column)

        result = cls(field=field, **kwargs)
        field.column = result
        return result

    @classmethod
    def make_hash(cls, expression, group=PRIMARY_DATA_GROUP):
        return cls(
            name="hash",
            cpp_name="Hash",
            sort_order="ascending",
            expression=expression,
            group=group,
            type="uint64",
            cpp_yt_type=storage_type_to_cpp_yt_type("uint64"),
        )

    @classmethod
    def make_dummy(cls):
        return cls(name="dummy", type="boolean", is_dummy=True)

    @property
    def full_cpp_name(self):
        if self.cpp_name is None:
            return None
        return f"Fields.{self.cpp_name}"

    @property
    def yt_schema(self):
        return {
            attribute: getattr(self, attribute)
            for attribute in ("name", "type", "sort_order", "lock", "expression", "group", "aggregate")
            if getattr(self, attribute) is not None
        }

    @property
    def is_key(self):
        return bool(self.sort_order)

    @property
    def is_hash(self):
        return self.name == "hash"


@dataclass
class OrmStructure:
    """Common machinery for Message and Enum."""

    name: Optional[str] = None
    proto_namespace: Optional[str] = None
    original_full_names: set[str] = dataclass_field(default_factory=set)
    filename: Optional[str] = None

    is_mixin: bool = False
    is_generic: bool = False

    reserved_numbers: set[int] = dataclass_field(default_factory=set)
    reserved_names: set[str] = dataclass_field(default_factory=set)

    _output_filename: Optional[str] = None
    _explicit_output_filename: bool = False

    containing_message: Optional["OrmMessage"] = None  # This structure is nested in that message.

    def __repr__(self) -> str:
        return f"({self.full_name})"

    @property
    def full_name(self):
        return ".".join([self.proto_namespace, self.middle_name])

    # Includes the containing message but not the namespace, like a patronymic.
    @property
    def middle_name(self):
        if self.is_nested:
            return ".".join([self.containing_message.middle_name, self.name])
        else:
            return self.name

    @property
    def output_filename(self) -> Optional[str]:
        if self.is_nested:
            return self.containing_message.output_filename
        else:
            return self._output_filename

    @property
    def output_to_default(self) -> bool:
        if self.is_nested:
            return self.containing_message.output_to_default
        else:
            return not self._output_filename or self._output_filename == "default"

    @property
    def is_nested(self):
        return self.containing_message is not None

    @property
    def reserved_ranges_formatted(self):
        reserved_ranges: list[str] = []
        first = None
        last = None

        def append():
            nonlocal reserved_ranges, first, last
            if first is None:
                pass
            elif first == last:
                reserved_ranges.append(str(first))
            else:
                reserved_ranges.append(f"{first} to {last}")

            first = None
            last = None

        for reserved_number in sorted(self.reserved_numbers):
            if reserved_number - 1 == last:
                last = reserved_number
            else:
                append()
                first = reserved_number
                last = reserved_number

        append()  # The final range.

        return ", ".join(reserved_ranges)

    @property
    def reserved_names_formatted(self):
        quoted = [f"\"{reserved_name}\"" for reserved_name in sorted(self.reserved_names)]
        return ", ".join(quoted)

    def _merge(self, base: "OrmStructure", mixin: "OrmStructure"):
        assert not self.is_nested

        self.name = base.name
        self.proto_namespace = base.proto_namespace
        self.containing_message = base.containing_message
        self.filename = base.filename
        self.original_full_names |= base.original_full_names
        if not mixin.is_generic:  # See comment in message.py
            self.original_full_names |= mixin.original_full_names
        self.is_mixin = base.is_mixin
        self.is_generic = base.is_generic

        if mixin._explicit_output_filename and not base._explicit_output_filename:
            self._output_filename = mixin._output_filename
            self._explicit_output_filename = mixin._explicit_output_filename
        else:
            self._output_filename = base._output_filename
            self._explicit_output_filename = base._explicit_output_filename

    def _analyze_name(self, name: str, full_name: str):
        self.name = name
        self.original_full_names.add(full_name)
        if name.endswith("Base"):  # Deprecated.
            self.name = name.removesuffix("Base")
            self.is_mixin = True
        if name.endswith("Mixin"):
            self.name = name.removesuffix("Mixin")
            self.is_mixin = True
        if name.startswith("TGenericObject"):
            self.is_generic = True
            self.is_mixin = True
        # There is no generic enum mechanism at the moment.

    def _set_explicit_output_filename(self, output_filename: str):
        assert not self.is_nested
        self._output_filename = output_filename
        self._explicit_output_filename = True

    def _analyze_disposition(self, package: "OrmPackage", module: "OrmModule"):
        if self.is_mixin and not module.is_source:
            self.proto_namespace = package.target_proto_namespace
        else:
            self.proto_namespace = module.target_proto_namespace

        if not self.is_nested and not self._output_filename and module.is_source:
            self._output_filename = module.filename

    def __str__(self):
        return self.full_name


@dataclass
class OrmOptionEnumValueWrapper:
    name: str

    def __str__(self):
        return self.name


def resolve_options_value(option, option_value, container: Optional[Union["OrmField", "OrmMessage"]] = None):
    if container is not None and option.is_extension:
        container.option_source_module_paths.add(option.file.name)
        if option.enum_type:
            container.option_source_module_paths.add(option.enum_type.file.name)
        if option.message_type:
            container.option_source_module_paths.add(option.message_type.file.name)

    if not option.enum_type:
        return option_value

    enum_descriptor = option.enum_type
    values = list()
    if isinstance(option_value, int):
        values.append(option_value)
    else:
        values = list(option_value)

    result = list()
    for val in values:
        value_descriptor = enum_descriptor.values_by_number.get(val, None)
        if value_descriptor is not None:
            result.append(OrmOptionEnumValueWrapper(value_descriptor.name))
    return result if option.label == option.LABEL_REPEATED else result[0]


def snake_case_to_upper_camel_case(name: str) -> str:
    return "".join([word.capitalize() for word in name.split("_")])


def object_key_constructor_params(key_values: list[str], key_fields: list["OrmField"]) -> str:
    def param(field, value):
        if field.cpp_type == "TString":
            value = f'"{value}"'
        return f"{field.cpp_type}({value})"

    return ", ".join(param(field, value) for field, value in zip(key_fields, key_values, strict=True))
