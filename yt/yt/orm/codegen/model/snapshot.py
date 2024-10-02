from .common import OrmContext
from .object import OrmObject
from .field import OrmField
from .filters import foreign_key_prefix, snake_to_camel_case, decapitalize

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class OrmSnapshotFieldConfig:
    path: Optional[str] = None
    snake_case_name: Optional[str] = None
    camel_case_name: Optional[str] = None
    lower_camel_case_name: Optional[str] = None
    cpp_type: Optional[str] = None
    treat_falsy_value_as_invalid_reference: bool = False


class OrmSnapshotField:
    _config: OrmSnapshotFieldConfig
    _parent_object: OrmObject
    # TODO(andrewln): _field should be mandatory, but current codegen model
    # does not provides OrmField instance for /labels attribute
    _field: Optional[OrmField]

    def __init__(
        self,
        config: OrmSnapshotFieldConfig,
        parent_object: OrmObject,
        field: Optional[OrmField],
    ):
        self._config = config
        self._parent_object = parent_object
        self._field = field

    @property
    def snake_case_name(self):
        # TODO(andrewln): We don't have OrmField instance for "/labels" because
        # OrmObject.resolve_attribute_by_path("/labels") is broken.
        if self._is_labels():
            return self._config.path.split("/")[-1]

        if self._config.snake_case_name:
            return self._config.snake_case_name

        # For some reason, OrmField that represents parent table key contains
        # name without parent table name prefix (e.g., OrmField that
        # corresponds to yp:pod:/meta/pod_set_id, has snake_case_name "id",
        # but not "pod_set_id"). Have to add that prefix manually.
        if self._field.is_parent_key_field:
            return f"{foreign_key_prefix(self._parent_object)}_{self._field.snake_case_name}"

        return self._field.snake_case_name

    @property
    def camel_case_name(self):
        if self._config.camel_case_name:
            return self._config.camel_case_name

        return snake_to_camel_case(self.snake_case_name)

    @property
    def lower_camel_case_name(self):
        if self._config.lower_camel_case_name:
            return self._config.lower_camel_case_name

        return decapitalize(self.camel_case_name)

    @property
    def path(self):
        if not self._config.path:
            return f"/meta/{self.snake_case_name}"

        return self._config.path

    @property
    def cpp_type(self):
        if self._config.cpp_type:
            return self._config.cpp_type

        # TODO(andrewln): We don't have OrmField instance for "/labels" because
        # OrmObject.resolve_attribute_by_path("/labels") is broken. So we have
        # to hardcode its c++ type here.
        if self._is_labels():
            return "NYT::NYTree::IMapNodePtr"

        # TODO(andrewln): For some reason, OrmField that represents parent table
        # key (e.g., yp:pod:/meta/pod_set_id) contains invalid c++ type.
        if self._field.cpp_type == "string":
            return "TString"

        return self._field.cpp_type

    @property
    def optional(self):
        # TODO(andrewln): Yeah, this is kinda hacky, but i'm not ready to
        # introduce full blown 'CppType' abstraction yet.
        return self.cpp_type.startswith("std::optional<")

    @property
    def falsy_value_is_invalid_reference(self):
        return self._config.treat_falsy_value_as_invalid_reference

    def _is_labels(self):
        if self._config.path is None:
            return False

        return self._config.path == "/labels" or self._config.path.startswith("/labels/")


@dataclass
class OrmSnapshotManyToOneReferenceConfig:
    snake_case_name: str
    local_field_paths: list[str]
    remote_object_name: str


class OrmSnapshotManyToOneReference:
    local_fields: list[OrmSnapshotField]
    remote_object: "OrmSnapshotObject"

    _config: OrmSnapshotManyToOneReferenceConfig

    def __init__(
        self,
        snapshot: "OrmSnapshot",
        local_object: "OrmSnapshotObject",
        config: OrmSnapshotManyToOneReferenceConfig,
    ):
        self.local_fields = [local_object.get_field_by_path(path) for path in config.local_field_paths]
        self.remote_object = snapshot.get_object_by_snake_case_name(config.remote_object_name)
        self._config = config

    @property
    def snake_case_name(self):
        return self._config.snake_case_name

    @property
    def camel_case_name(self):
        return snake_to_camel_case(self.snake_case_name)

    @property
    def cpp_key_getter_return_type(self):
        if self.optional:
            return f"std::optional<{self.remote_object.cpp_key_type}>"

        return self.remote_object.cpp_key_type

    @property
    def optional(self):
        return any(field.optional or field.falsy_value_is_invalid_reference for field in self.local_fields)


@dataclass
class OrmSnapshotOneToManyReferenceConfig:
    snake_case_name: str
    remote_object_name: str
    remote_many_to_one_reference_name: str


class OrmSnapshotOneToManyReference:
    remote_object: "OrmSnapshotObject"
    remote_many_to_one_reference: OrmSnapshotManyToOneReference

    _config: OrmSnapshotOneToManyReferenceConfig

    def __init__(
        self,
        snapshot: "OrmSnapshot",
        config: OrmSnapshotOneToManyReferenceConfig,
    ):
        self.remote_object = snapshot.get_object_by_snake_case_name(config.remote_object_name)
        self.remote_many_to_one_reference = self.remote_object.get_many_to_one_reference_by_snake_case_name(
            config.remote_many_to_one_reference_name
        )
        self._config = config

    @property
    def snake_case_name(self):
        return self._config.snake_case_name

    @property
    def camel_case_name(self):
        return snake_to_camel_case(self.snake_case_name)


@dataclass
class OrmSnapshotObjectConfig:
    snake_case_name: str
    fields: list[OrmSnapshotFieldConfig] = field(default_factory=list)
    many_to_one_references: list[OrmSnapshotManyToOneReferenceConfig] = field(default_factory=list)
    one_to_many_references: list[OrmSnapshotOneToManyReferenceConfig] = field(default_factory=list)


class OrmSnapshotObject:
    key_fields: list[OrmSnapshotField]
    data_fields: list[OrmSnapshotField]
    many_to_one_references: list[OrmSnapshotManyToOneReference]
    one_to_many_references: list[OrmSnapshotOneToManyReference]

    _config: OrmSnapshotObjectConfig
    _object: OrmObject

    def __init__(
        self,
        context: OrmContext,
        config: OrmSnapshotObjectConfig,
    ):
        def try_get_orm_field(path) -> Optional[OrmField]:
            try:
                return self._object.resolve_attribute_by_path(path).field
            except Exception:
                return None

        self._config = config
        self._object = context.object_by_snake_case_name[config.snake_case_name]
        self.key_fields = [
            OrmSnapshotField(OrmSnapshotFieldConfig(), self._object.parent, field)
            for field in self._object.all_key_fields
        ]
        self.data_fields = [
            OrmSnapshotField(field_config, self._object.parent, try_get_orm_field(field_config.path))
            for field_config in config.fields
        ]

    def init_many_to_one_references(self, snapshot: "OrmSnapshot"):
        self.many_to_one_references = [
            OrmSnapshotManyToOneReference(snapshot, self, config) for config in self._config.many_to_one_references
        ]

    def init_one_to_many_references(self, snapshot: "OrmSnapshot"):
        self.one_to_many_references = [
            OrmSnapshotOneToManyReference(snapshot, config) for config in self._config.one_to_many_references
        ]

    @property
    def snake_case_name(self):
        return self._config.snake_case_name or self._object.snake_case_name

    @property
    def human_readable_name(self):
        return self.snake_case_name.replace("_", " ")

    @property
    def camel_case_name(self):
        return snake_to_camel_case(self.snake_case_name)

    @property
    def type_value(self):
        return self._object.type_value

    @property
    def cpp_type(self):
        return "T" + self.camel_case_name

    @property
    def cpp_key_type(self):
        return self.cpp_type + "Key"

    @property
    def cpp_data_type(self):
        return self.cpp_type + "Data"

    @property
    def cpp_data_ptr_type(self):
        return self.cpp_type + "DataPtr"

    @property
    def all_fields(self):
        return self.key_fields + self.data_fields

    def get_field_by_path(self, path: str):
        for f in self.all_fields:
            if f.path == path:
                return f

        raise Exception(f"Invalid object attribute path: {self.snake_case_name}:{path}")

    def get_many_to_one_reference_by_snake_case_name(self, name: str):
        for reference in self.many_to_one_references:
            if reference.snake_case_name == name:
                return reference

        raise Exception(f"Invalid many to one reference name: {self.snake_case_name}:{name}")


@dataclass
class OrmSnapshotConfig:
    cpp_namespace: Optional[str] = None
    cpp_includes: list[str] = field(default_factory=list)
    objects: list[OrmSnapshotObjectConfig] = field(default_factory=list)


class OrmSnapshot:
    objects: list[OrmSnapshotObject]

    _config: OrmSnapshotConfig

    def __init__(
        self,
        context: OrmContext,
        config: OrmSnapshotConfig,
    ):
        self._config = config
        self.objects = [OrmSnapshotObject(context, object_config) for object_config in config.objects]

        for object in self.objects:
            object.init_many_to_one_references(self)

        for object in self.objects:
            object.init_one_to_many_references(self)

    @property
    def cpp_namespace(self):
        return self._config.cpp_namespace

    @property
    def cpp_includes(self):
        return self._config.cpp_includes

    @property
    def one_to_many_references(self):
        for object in self.objects:
            for reference in object.one_to_many_references:
                yield (object, reference)

    @property
    def has_one_to_many_references(self):
        return next(self.one_to_many_references, None) is not None

    def get_object_by_snake_case_name(self, name: str):
        for object in self.objects:
            if object.snake_case_name == name:
                return object

        raise Exception(f"Invalid object name: {name}")
