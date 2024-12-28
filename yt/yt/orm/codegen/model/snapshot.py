from .common import OrmContext
from .field import OrmField
from .object import OrmObject
from .filters import foreign_key_prefix

from typing import Optional

from yt.yt.orm.library.snapshot.codegen.config import (
    SnapshotConfig,
    SnapshotFieldConfig,
    SnapshotObjectConfig,
)
from yt.yt.orm.library.snapshot.codegen.name import SnapshotEntityName


def _deduce_field_cpp_type(
    snapshot_name: SnapshotEntityName,
    object_name: SnapshotEntityName,
    path: str,
    field: Optional[OrmField],
) -> str:
    is_labels = path == "/labels" or path.startswith("/labels/")
    if is_labels:
        # TODO(andrewln): We don't have OrmField instance for "/labels" because
        # OrmObject.resolve_attribute_by_path("/labels") is broken. So we have
        # to hardcode its c++ type here.
        return "NYT::NYTree::IMapNodePtr"

    if field is None:
        raise ValueError(
            f'Error rendering snapshot "{snapshot_name}": invalid "path"="{path}" '
            f'has been specified for object "{object_name}" field. Please make sure that "path" '
            f'contains correct attribute path.'
        )

    # TODO(andrewln): For some reason, OrmField that represents parent table
    # key (e.g., yp:pod:/meta/pod_set_id) contains invalid c++ type.
    if field.cpp_type == "string":
        return "TString"

    return field.cpp_type


def _enrich_field_config(
    snapshot_name: SnapshotEntityName,
    object_name: SnapshotEntityName,
    config: SnapshotFieldConfig,
    field: Optional[OrmField],
) -> SnapshotFieldConfig:
    if not config.path:
        raise ValueError(
            f'Error rendering snapshot "{snapshot_name}": missing "path" for object '
            f'"{object_name}" field. Please make sure that every object field contains correct '
            f'attribute path inside "path" config key.'
        )

    if config.cpp_type is None:
        config.cpp_type = _deduce_field_cpp_type(snapshot_name, object_name, config.path, field)

    return config


def _deduce_key_field_config(
    snapshot_name: SnapshotEntityName,
    object: OrmObject,
    field: OrmField,
) -> SnapshotFieldConfig:
    # For some reason, OrmField that represents parent table key contains
    # name without parent table name prefix (e.g., OrmField that
    # corresponds to yp:pod:/meta/pod_set_id, has snake_case_name "id",
    # but not "pod_set_id"). Have to add that prefix manually.
    if field.is_parent_key_field:
        snake_case_name = f"{foreign_key_prefix(object.parent)}_{field.snake_case_name}"
    else:
        snake_case_name = field.snake_case_name

    return _enrich_field_config(
        snapshot_name,
        SnapshotEntityName(object.snake_case_name),
        SnapshotFieldConfig(
            path="/meta/" + snake_case_name,
            is_key=True,
        ),
        field,
    )


def _enrich_object_config(
    snapshot_name: SnapshotEntityName,
    config: SnapshotObjectConfig,
    context: OrmContext,
) -> SnapshotObjectConfig:
    if not config.name:
        raise ValueError(
            f'Error rendering snapshot "{snapshot_name}": missing "name" inside '
            f'snapshot object configuration. Please make sure that every object in the snapshot '
            f'contains correct name.'
        )

    if config.name.snake_case not in context.object_by_snake_case_name:
        raise ValueError(
            f'Error rendering snapshot "{snapshot_name}": invalid "name"='
            f'"{config.name}". Please make sure that "name" contains '
            f'existing object name.'
        )

    object = context.object_by_snake_case_name[config.name.snake_case]

    if config.type_value is None:
        config.type_value = object.type_value

    key_fields = [_deduce_key_field_config(snapshot_name, object, field) for field in object.all_key_fields]

    def try_get_field(path) -> Optional[OrmField]:
        try:
            return object.resolve_attribute_by_path(path).field
        except Exception:
            return None

    data_fields = [
        _enrich_field_config(snapshot_name, config.name, field_config, try_get_field(field_config.path))
        for field_config in config.fields
    ]

    config.fields = key_fields + data_fields

    return config


def enrich_snapshot_config(
    config: SnapshotConfig,
    context: OrmContext,
) -> SnapshotConfig:
    config.objects = [_enrich_object_config(config.name, object_config, context) for object_config in config.objects]

    return config
