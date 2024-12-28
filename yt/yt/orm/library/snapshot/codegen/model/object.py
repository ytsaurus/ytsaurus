from .field import SnapshotField
from .many_to_one_reference import SnapshotManyToOneReference
from .one_to_many_reference import SnapshotOneToManyReference

from yt.yt.orm.library.snapshot.codegen.config import (
    SnapshotFieldConfig,
    SnapshotObjectConfig,
)

from typing import Optional


class SnapshotObject:
    snapshot: "Snapshot"
    fields: list[SnapshotField]
    many_to_one_references: list[SnapshotManyToOneReference]
    one_to_many_references: list[SnapshotOneToManyReference]
    direct_parents: list["SnapshotObject"]
    all_parents: list["SnapshotObject"]

    _config: SnapshotObjectConfig

    def __init__(self, snapshot: "Snapshot", config: SnapshotObjectConfig):
        if not config.name:
            raise ValueError(
                f'Error rendering snapshot "{snapshot.name}": "name" has not been specified for an object.'
            )

        if config.type_value is None:
            raise ValueError(
                f'Error rendering snapshot "{snapshot.name}": "type_value" has not been '
                f'specified for object "{config.name}".'
            )

        self._config = config
        self.snapshot = snapshot
        self.fields = [SnapshotField(self, field_config) for field_config in config.fields]

    def _init_inheritance(self):
        self.direct_parents = []
        for parent_snapshot in self.snapshot.direct_parents:
            parent_object = parent_snapshot.try_get_object_by_name(self.name.snake_case)
            if parent_object is not None:
                self.direct_parents.append(parent_object)

        self.all_parents = []
        for parent_snapshot in self.snapshot.all_parents:
            parent_object = parent_snapshot.try_get_object_by_name(self.name.snake_case)
            if parent_object is not None:
                self.all_parents.append(parent_object)

        def inherit_field(field) -> SnapshotField:
            return SnapshotField(
                self,
                SnapshotFieldConfig(
                    path=field.path,
                    cpp_type=field.cpp_type,
                    treat_falsy_value_as_invalid_reference=field.falsy_value_is_invalid_reference,
                    is_key=field.is_key,
                ),
            )

        def check_fields_compatibility(local_field: SnapshotField, parent_field: SnapshotField):
            # TODO(andrewln)
            pass

        for parent in self.direct_parents:
            for parent_field in parent.fields:
                local_field = self.try_get_field_by_path(parent_field.path)
                if local_field is None:
                    self.fields.append(inherit_field(parent_field))
                else:
                    check_fields_compatibility(local_field, parent_field)

    def _init_many_to_one_references(self):
        self.many_to_one_references = [
            SnapshotManyToOneReference(self, config) for config in self._config.many_to_one_references
        ]

    def _init_one_to_many_references(self):
        self.one_to_many_references = [
            SnapshotOneToManyReference(self, config) for config in self._config.one_to_many_references
        ]

    def __str__(self):
        return f"SnapshotObject({self.snapshot.name}:{self.name})"

    @property
    def name(self):
        return self._config.name

    @property
    def type_value(self):
        return self._config.type_value

    @property
    def cpp_type(self):
        return f"T{self.name.pascal_case}"

    @property
    def cpp_key_type(self):
        return f"T{self.name.pascal_case}Key"

    @property
    def cpp_data_interface_type(self):
        return f"I{self.name.pascal_case}Data"

    @property
    def cpp_data_impl_type(self):
        return f"T{self.name.pascal_case}Data"

    @property
    def cpp_data_ptr_type(self):
        return f"I{self.name.pascal_case}DataPtr"

    @property
    def key_fields(self):
        return [field for field in self.fields if field.is_key]

    @property
    def data_fields(self):
        return [field for field in self.fields if not field.is_key]

    def try_get_field_by_path(self, path: str) -> Optional[SnapshotField]:
        for f in self.fields:
            if f.path == path:
                return f

        return None

    def try_get_many_to_one_reference_by_name(self, name: str) -> Optional[SnapshotManyToOneReference]:
        for reference in self.many_to_one_references:
            if reference.name.snake_case == name:
                return reference

        return None
