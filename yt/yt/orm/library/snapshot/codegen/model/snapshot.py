from .object import SnapshotObject

from yt.yt.orm.library.snapshot.codegen.config import (
    SnapshotConfig,
    SnapshotObjectConfig,
)

from typing import Optional


class Snapshot:
    objects: list[SnapshotObject]
    direct_parents: list["Snapshot"]
    all_parents: list["Snapshot"]

    _config: SnapshotConfig

    def __init__(
        self,
        config: SnapshotConfig,
    ):
        self._config = config
        self.objects = [SnapshotObject(self, object_config) for object_config in config.objects]

    def _init_inheritance(self, snapshots: dict[str, "Snapshot"]):
        self.direct_parents = [snapshots[name] for name in self.direct_parent_names]

        self.all_parents = []
        for parent in self.direct_parents:
            for transitive in parent.all_parents:
                if transitive not in self.all_parents:
                    self.all_parents.append(transitive)
            self.all_parents.append(parent)

        def inherit_object(parent_object: SnapshotObject) -> SnapshotObject:
            local_object = SnapshotObject(
                self,
                SnapshotObjectConfig(
                    name=parent_object.name,
                    type_value=parent_object.type_value,
                ),
            )
            return local_object

        def check_object_compatibility(local_object: SnapshotObject, parent_object: SnapshotObject):
            # TODO(andrewln)
            pass

        for parent in self.direct_parents:
            for parent_object in parent.objects:
                local_object = self.try_get_object_by_name(parent_object.name.snake_case)
                if local_object is None:
                    self.objects.append(inherit_object(parent_object))
                else:
                    check_object_compatibility(local_object, parent_object)

        for object in self.objects:
            object._init_inheritance()

    def _init_references(self):
        for object in self.objects:
            object._init_many_to_one_references()

        for object in self.objects:
            object._init_one_to_many_references()

    def __str__(self):
        return f"SnapshotObject({self.name})"

    @property
    def name(self):
        return self._config.name

    @property
    def cpp_path(self):
        return self._config.cpp_path

    @property
    def cpp_namespace(self):
        return self._config.cpp_namespace

    @property
    def cpp_includes(self):
        return self._config.cpp_includes

    @property
    def cpp_dependencies(self):
        return self._config.cpp_dependencies

    @property
    def direct_parent_names(self):
        return self._config.inherits

    @property
    def one_to_many_references(self):
        for object in self.objects:
            for reference in object.one_to_many_references:
                yield (object, reference)

    @property
    def has_one_to_many_references(self):
        return next(self.one_to_many_references, None) is not None

    def try_get_object_by_name(self, name: str) -> Optional[SnapshotObject]:
        for object in self.objects:
            if object.name.snake_case == name:
                return object

        return None
