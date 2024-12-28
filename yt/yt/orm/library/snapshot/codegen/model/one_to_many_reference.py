from .many_to_one_reference import SnapshotManyToOneReference

from yt.yt.orm.library.snapshot.codegen.config import SnapshotOneToManyReferenceConfig


class SnapshotOneToManyReference:
    local_object: "SnapshotObject"
    remote_object: "SnapshotObject"
    remote_many_to_one_reference: SnapshotManyToOneReference

    _config: SnapshotOneToManyReferenceConfig

    def __init__(
        self,
        local_object: "SnapshotObject",
        config: SnapshotOneToManyReferenceConfig,
    ):
        snapshot = local_object.snapshot

        if not config.name:
            raise ValueError(
                f'Error rendering snapshot "{snapshot.name}": missing "name" '
                f'for one-to-many reference of object "{local_object.name}". Please '
                f'make sure that appropriate name provided for every reference.'
            )

        self.local_object = local_object

        self.remote_object = snapshot.try_get_object_by_name(config.remote_object_name)
        if self.remote_object is None:
            raise ValueError(
                f'Error rendering snapshot "{snapshot.name}": invalid '
                f'"remote_object_name"="{config.remote_object_name}" specified in one-to-many '
                f'reference "{config.name.human_readable}" of object "{local_object.name}". '
                f'Please make sure that "remote_object_name" contains existing object name.'
            )

        self.remote_many_to_one_reference = self.remote_object.try_get_many_to_one_reference_by_name(
            config.remote_many_to_one_reference_name
        )
        if self.remote_many_to_one_reference is None:
            raise ValueError(
                f'Error rendering {snapshot.name} snapshot: invalid '
                f'"remote_many_to_one_reference_name"="{config.remote_many_to_one_reference_name}" '
                f'specified in one-to-many reference "{config.name.human_readable}" of object '
                f'"{local_object.name}". Please make sure that "remote_many_to_one_reference_name" '
                f'contains name of an existing many-to-one reference'
            )

        self._config = config

    def __str__(self):
        return f"SnapshotOneToManyReference({self.local_object.snapshot.name}:{self.local_object.name}:{self.name})"

    @property
    def name(self):
        return self._config.name
