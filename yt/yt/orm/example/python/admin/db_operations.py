from .data_model_traits import (  # noqa
    ACTUAL_DB_VERSION,
    INITIAL_DB_VERSION,
    EXAMPLE_DATA_MODEL_TRAITS,
)

from yt.yt.orm.example.python.client.data_model import EObjectType

from yt.orm.admin.db_operations import (  # noqa
    Freezer as OrmFreezer,
    Migrator as OrmMigrator,
    add_ace,
    backup_orm,
    create_schemas,
    freeze_orm,
    get_db_version as orm_get_db_version,
    get_schema_acl,
    orm_init_yt_cluster,
    reset_orm,
    restore_orm,
    try_create,
    unfreeze_orm,
)

from yt.orm.library.common import iter_proto_enum_value_names


def _build_order_of_object_type_removal():
    object_types = ["interceptor"] + ["book", "illustrator", "typographer", "mother_ship"] + list(
        iter_proto_enum_value_names(EObjectType, exclude="null")
    )
    seen = set()
    return [
        object_type
        for object_type in object_types
        if not (object_type in seen or seen.add(object_type))
    ]


ORDER_OF_OBJECT_TYPE_REMOVAL = _build_order_of_object_type_removal()


def get_db_version(yt_client, example_path):
    return orm_get_db_version(yt_client, example_path, EXAMPLE_DATA_MODEL_TRAITS)


def init_yt_cluster(yt_client, path, **kwargs):
    orm_init_yt_cluster(yt_client, path, EXAMPLE_DATA_MODEL_TRAITS, EObjectType, **kwargs)


def backup_example(yt_client, example_path, backup_paths=None, table_names=None):
    return backup_orm(yt_client, example_path, EXAMPLE_DATA_MODEL_TRAITS, backup_paths, table_names=table_names)


def restore_example(yt_client, example_path, backup_path):
    return restore_orm(yt_client, example_path, EXAMPLE_DATA_MODEL_TRAITS, backup_path)


def freeze_example(yt_client, example_path):
    return freeze_orm(yt_client, example_path, EXAMPLE_DATA_MODEL_TRAITS)


def unfreeze_example(yt_client, example_path):
    return unfreeze_orm(yt_client, example_path, EXAMPLE_DATA_MODEL_TRAITS)


class Freezer(OrmFreezer):
    def __init__(self, yt_client, example_path, table_names=None):
        super(Freezer, self).__init__(yt_client, example_path, EXAMPLE_DATA_MODEL_TRAITS, table_names=table_names)


# Best-effort to restore clean Example database state. Useful for tests.
def reset_example(example_client):
    # This function is called per every test, try to fine-tune its performance, please.

    reset_orm(
        example_client,
        ORDER_OF_OBJECT_TYPE_REMOVAL,
    )


class Migrator(OrmMigrator):
    def __init__(
        self, yt_client, example_path, target_version, backup_path=OrmMigrator.DEFAULT_BACKUP_PATH
    ):
        super(Migrator, self).__init__(
            yt_client, example_path, target_version, EXAMPLE_DATA_MODEL_TRAITS, backup_path
        )
