from .field import SnapshotField

from yt.yt.orm.library.snapshot.codegen.config import SnapshotManyToOneReferenceConfig


class SnapshotManyToOneReference:
    local_object: "SnapshotObject"
    local_fields: list[SnapshotField]
    remote_object: "SnapshotObject"

    _config: SnapshotManyToOneReferenceConfig

    def __init__(
        self,
        local_object: "SnapshotObject",
        config: SnapshotManyToOneReferenceConfig,
    ):
        snapshot = local_object.snapshot

        if not config.name:
            raise ValueError(
                f'Error rendering snapshot "{snapshot.name}": missing "name" '
                f'for many-to-one reference of object "{local_object.name}". Please '
                f'make sure that appropriate name provided for every reference.'
            )

        def get_local_field(path: str):
            field = local_object.try_get_field_by_path(path)
            if field is None:
                raise ValueError(
                    f'Error rendering snapshot "{snapshot.name}": one of '
                    f'"local_field_paths" elements of many-to-one reference '
                    f'"{config.name.human_readable}" of object "{local_object.name}" '
                    f'contains invalid attribute path "{path}".'
                )
            return field

        self.local_object = local_object
        self.local_fields = [get_local_field(path) for path in config.local_field_paths]

        self.remote_object = snapshot.try_get_object_by_name(config.remote_object_name)
        if self.remote_object is None:
            raise ValueError(
                f'Error rendering snapshot "{snapshot.name}": invalid '
                f'"remote_object_name"="{config.remote_object_name}" specified in many-to-one '
                f'reference "{config.name.human_readable}" of object "{local_object.name}". '
                f'Please make sure that "remote_object_name" contains existing object name.'
            )

        self._config = config

    def __str__(self):
        return f"SnapshotManyToOneReference({self.local_object.snapshot.name}:{self.local_object.name}:{self.name})"

    @property
    def name(self):
        return self._config.name

    @property
    def cpp_key_getter_return_type(self):
        if self.optional:
            return f"std::optional<{self.remote_object.cpp_key_type}>"

        return self.remote_object.cpp_key_type

    @property
    def optional(self):
        return any(field.optional or field.falsy_value_is_invalid_reference for field in self.local_fields)
