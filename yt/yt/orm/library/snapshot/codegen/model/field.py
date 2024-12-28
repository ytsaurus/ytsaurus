from yt.yt.orm.library.snapshot.codegen.config import SnapshotFieldConfig
from yt.yt.orm.library.snapshot.codegen.name import SnapshotEntityName


class SnapshotField:
    object: "SnapshotObject"
    name: SnapshotEntityName
    full_name: SnapshotEntityName

    _config: SnapshotFieldConfig

    def __init__(self, object: "SnapshotObject", config: SnapshotFieldConfig):
        if not config.path:
            raise ValueError(
                f'Error rendering snapshot "{object.snapshot.name}": missing "path" '
                f'for object "{object.name}" field. Please, make sure that correct attribute '
                f'path provided for every object field in snapshot configuration.'
            )

        if not config.cpp_type:
            raise ValueError(
                f'Error rendering snapshot "{object.snapshot.name}": missing "cpp_type" '
                f'for attribute path "{config.path}" of object "{object.name}".'
            )

        self.object = object
        self.name = config.name or SnapshotEntityName(config.path.split("/")[-1])
        self.full_name = SnapshotEntityName(config.path[1:].replace("/", "_"))
        self._config = config

    def __str__(self):
        return f"SnapshotField({self.object.snapshot.name}:{self.object.name}:{self.path})"

    @property
    def path(self):
        return self._config.path

    @property
    def cpp_type(self):
        return self._config.cpp_type

    @property
    def is_key(self):
        return self._config.is_key

    @property
    def optional(self):
        # TODO(andrewln): Yeah, this is kinda hacky, but i'm not ready to
        # introduce full blown 'CppType' abstraction yet.
        return self.cpp_type.startswith("std::optional<")

    @property
    def falsy_value_is_invalid_reference(self):
        return self._config.treat_falsy_value_as_invalid_reference
