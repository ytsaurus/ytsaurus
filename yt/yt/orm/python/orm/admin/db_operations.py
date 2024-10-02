from .db_manager import (
    DbManager,
    batch_apply,
    convert_static_to_dynamic_db_by_prototype,
    get_yt_cluster_name,
    is_watch_log,
    get_schema_acl,
)

from .replicated_db_manager import ReplicatedDbManager

from yt.orm.library.common import (
    ClientError,
    NoSuchObjectError,
    YtResponseError,
    wait,
)

from yt.orm.library.dynamic_config import OrmDynamicConfig

from yt.common import update
from yt.wrapper.dynamic_table_commands import BackupManifest, ClusterBackupManifest
from yt.wrapper.operations_tracker import copy_client
from yt.wrapper.retries import run_with_retries
from yt.wrapper.ypath import ypath_join
from yt.yson.yson_types import YsonEntity

import yt.logger as logger
import yt.wrapper as yt
import yt.yson as yson

from datetime import datetime, timedelta

from abc import ABCMeta, abstractmethod
import os
import shutil
import sys
import time

ZERO_UPSTREAM_REPLICA_ID = "0-0-0-0"


def _format_yt_timestamp(yt_timestamp):
    return datetime.fromtimestamp(yt_timestamp / (2.0**30)).strftime("%Y-%m-%d %H:%M:%S")


def _check_ace(ace):
    required_keys = ["subjects", "permissions", "action"]
    optional_keys = ["inheritance_mode"]
    for key in required_keys:
        if key not in ace:
            raise ClientError("Cannot find required key '{}' in ACE: '{}'".format(key, ace))
    for key in ace:
        if key not in optional_keys and key not in required_keys:
            raise ClientError("Found unknown key '{}' in ACE: '{}'".format(key, ace))


def _are_equal_aces(lhs, rhs):
    if sorted(lhs["subjects"]) != sorted(rhs["subjects"]):
        return False
    if sorted(lhs["permissions"]) != sorted(rhs["permissions"]):
        return False
    if lhs["action"] != rhs["action"]:
        return False
    if lhs.get("inheritance_mode", "object_and_descendants") != rhs.get(
        "inheritance_mode", "object_and_descendants"
    ):
        return False
    return True


def _need_to_add_ace(new_ace, acl):
    _check_ace(new_ace)
    for ace in acl:
        if _are_equal_aces(new_ace, ace):
            return False
    return True


def add_ace(path, ace, client):
    acl = client.get(path + "/@acl")

    if _need_to_add_ace(ace, acl):
        client.set(path + "/@acl/end", ace)
    else:
        logger.warning("ACE '%s' is already present in %s/@acl", ace, path)


def _make_backup_path(orm_path, dir, prefix):
    return ypath_join(orm_path, dir, "{}.{}".format(prefix, time.strftime("%d%m%Y_%H%M%S")))


def _get_orm_backup_path(orm_path, db_version):
    return _make_backup_path(orm_path, "backups", "v{}".format(db_version))


def _get_orm_migration_path(orm_path, old_version, new_version):
    return _make_backup_path(
        orm_path, "tmp", "migration_v{}_to_v{}".format(old_version, new_version)
    )


def _get_temp_clone_path(orm_path):
    return _make_backup_path(orm_path, "tmp", "clone")


def create_schemas(db_manager, types):
    db_manager.create_schemas(types)


def get_db_version(yt_client, orm_path, data_model_traits):
    db_manager = DbManager(yt_client, orm_path, data_model_traits)
    return db_manager.read_version()


def try_create(client, type_, **kwargs):
    try:
        client.create(type_, **kwargs)
        return True
    except YtResponseError as err:
        # COMPAT(ignat): remove error code check.
        if err.contains_code(501) or err.is_already_exists():
            logger.warning(
                "Cannot create object of type '%s' with arguments '%s': object already exists",
                type_,
                kwargs,
            )
            return False
        else:
            raise


def get_if_exists(yt_client, path, default=None):
    try:
        return yt_client.get(path)
    except YtResponseError as err:
        if err.is_resolve_error():
            return default
        raise


def _do_orm_initialize_layout(yt_client, path):
    yt_client.create(
        "map_node", ypath_join(path, "master", "instances"), ignore_existing=True, recursive=True
    )
    yt_client.create(
        "map_node",
        ypath_join(path, "master", "instances_banned_by_fqdn"),
        ignore_existing=True,
        recursive=True,
    )
    yt_client.create(
        "map_node",
        ypath_join(path, "master", "instance_tags"),
        ignore_existing=True,
        recursive=True,
    )
    yt_client.create(
        "map_node", ypath_join(path, "master", "leader"), ignore_existing=True, recursive=True
    )
    yt_client.create(
        "document",
        ypath_join(path, "master", "config"),
        ignore_existing=True,
        attributes={"value": {}},
    )
    yt_client.create("map_node", ypath_join(path, "db"), ignore_existing=True)
    yt_client.create("map_node", ypath_join(path, "backups"), ignore_existing=True)
    yt_client.create("map_node", ypath_join(path, "tmp"), ignore_existing=True)

    try_create(yt_client, "map_node", path=ypath_join(path, "private", "tokens"), recursive=True)


def orm_initialize_layout(db_manager):
    for yt_client, path in db_manager.iterate_replicas():
        _do_orm_initialize_layout(yt_client, path)


def _do_orm_initialize_account(yt_client, orm_account, user, resource_kind_to_limit, used_media):
    try_create(yt_client, "account", attributes=dict(name=orm_account))
    try_create(yt_client, "user", attributes=dict(name=user))

    assert yt_client.exists("//sys/accounts/{}".format(orm_account))
    assert yt_client.exists("//sys/users/{}".format(user))

    yt_client.set(
        "//sys/accounts/{}/@acl".format(orm_account),
        [{"subjects": [user], "action": "allow", "permissions": ["use"]}],
    )

    for account in ("sys", "default", "tmp", orm_account):
        for resource_kind, resource_limit in resource_kind_to_limit.items():
            yt_client.set(
                "//sys/accounts/{}/@resource_limits/{}".format(account, resource_kind),
                resource_limit,
            )

    # Set default replication factor to 3 for media in use. See YT-13689 for details.
    # Do not configure //sys/media/{}/@config/max_*_replicas_per_rack:
    # - Single-DC clusters (e.g.: yp-sas) do not have racks configured properly;
    # - Multi-DC clusters (e.g.: yp-xdc) rely on YT automation.
    for medium in used_media:
        if yt_client.exists("//sys/media/{}".format(medium)):
            yt_client.set("//sys/media/{}/@config/max_replication_factor".format(medium), 3)

    yt_client.set(
        "//sys/schemas/orchid/@acl/end",
        {"action": "allow", "permissions": ["create"], "subjects": [user]},
    )


def orm_initialize_account(db_manager, account, user, resource_kind_to_limit=None):
    if resource_kind_to_limit is None:
        resource_kind_to_limit = dict()

    used_media = db_manager.get_used_media()
    default_limits = {
        "node_count": 10000,
        "chunk_count": 100000,
        "tablet_count": 1000000,
        "disk_space_per_medium": dict(),
        "tablet_static_memory": 2**30,
        "master_memory/total": 2**30,
        "master_memory/chunk_host": 2**30,
    }

    resource_kind_to_limit = update(default_limits, resource_kind_to_limit)

    for yt_client, _ in db_manager.iterate_replicas():
        disk_space_per_medium = {}
        media = set()
        for medium in used_media:
            if yt_client.exists("//sys/media/{}".format(medium)):
                disk_space_per_medium[medium] = 2**30
                media.add(medium)

        resource_kind_to_limit["disk_space_per_medium"] = disk_space_per_medium
        _do_orm_initialize_account(yt_client, account, user, resource_kind_to_limit, used_media)


def orm_initialize_permissions(db_manager, account, user):
    for yt_client, path in db_manager.iterate_replicas():
        yt_client.create("map_node", path, ignore_existing=True, recursive=True)
        if user:
            try:
                yt_client.set(
                    path + "/@acl",
                    [
                        {
                            "subjects": [user],
                            "action": "allow",
                            "permissions": ["read", "write", "remove", "mount"],
                        }
                    ],
                )
            except:  # noqa
                logger.exception("Cannot set acl for path {}".format(path))
        yt_client.set(path + "/@account", account)


def orm_initialize_objects(db_manager, object_type_enum):
    db_manager.insert_rows(
        "users",
        [
            {
                "meta.id": "root",
                "spec": {
                    "request_weight_rate_limit": 0,
                    "request_queue_size_limit": 0,
                },
            }
        ],
    )

    db_manager.insert_rows(
        "groups",
        [
            {
                "meta.id": "superusers",
                "spec": {"members": ["root"]},
            }
        ],
    )

    db_manager.insert_rows(
        "subject_to_type",
        [
            {"subject_id": "root", "type": object_type_enum.Value("OT_USER")},
            {"subject_id": "superusers", "type": object_type_enum.Value("OT_GROUP")},
        ],
        object_table=False,
    )


def orm_init_yt_cluster(
    yt_client,
    path,
    data_model_traits,
    object_type_enum,
    replica_clients=None,
    to_actual_version=False,
    initialize_account=False,
    db_manager=None,
):
    name = data_model_traits.get_human_readable_name()

    do_not_finalize = db_manager is not None
    if db_manager is None:
        db_version = data_model_traits.get_initial_db_version()
        if to_actual_version:
            db_version = data_model_traits.get_actual_db_version()

        if replica_clients:
            db_manager = ReplicatedDbManager(
                yt_client,
                path,
                replica_clients,
                data_model_traits,
                db_version,
            )
        else:
            db_manager = DbManager(
                yt_client,
                path,
                data_model_traits,
                db_version,
            )

    logger.info("Initializing environment for {} in underlying YT cluster".format(name))

    # Initialize layout.
    user = data_model_traits.get_user_name()
    account = data_model_traits.get_account()
    if initialize_account:
        orm_initialize_account(db_manager, account, user)
    orm_initialize_permissions(db_manager, account, user)
    orm_initialize_layout(db_manager)
    db_manager.initialize_tables()

    db_manager.mount_unmounted_tables()

    # Builtin rows.
    orm_initialize_objects(db_manager, object_type_enum)

    # Schemas.
    db_manager.create_schemas()

    if not do_not_finalize:
        db_manager.finalize()
        logger.info("Environment for {} is initialized".format(name))


def orm_create_table_collocation(db_manager, db_schema):
    collocation_id = None
    yt_client = db_manager.get_yt_client()
    table_paths = []
    for tables in (db_schema.TABLES, db_schema.INDEXES):
        for table in tables:
            table_path = db_manager.get_table_path(table)
            current_id = get_if_exists(
                yt_client, "{}/@replication_collocation_id".format(table_path)
            )

            if current_id:
                assert (
                    collocation_id is None or collocation_id == current_id
                ), "ORM has more than one table replication collocation {} != {}".format(
                    collocation_id, current_id
                )
                collocation_id = current_id
            else:
                table_paths.append(table_path)
    if collocation_id is None:
        logger.info(
            "Creating a new table replication collocation (name: %s).",
            db_manager.get_snake_case_orm_name(),
        )
        yt_client.create(
            "table_collocation",
            attributes={
                "name": db_manager.get_snake_case_orm_name(),
                "collocation_type": "replication",
                "table_paths": table_paths,
            },
        )
    else:
        for table_path in table_paths:
            logger.info(
                "Adding table to replication collocation (name: %s, table_path: %s)",
                table_path,
                db_manager.get_snake_case_orm_name(),
            )
            yt_client.set("{}/@replication_collocation_id".format(table_path), collocation_id)


def set_preferred_medium_for_backup(target_manager, preferred_medium):
    tables = target_manager.get_tables_list(attributes=["primary_medium"])
    tables = list(
        filter(lambda table: table.attributes["primary_medium"] != preferred_medium, tables)
    )
    if tables:
        logger.info(
            "Resetting tables primary medium to preferable (Tables: {}, Medium: {})".format(
                tables, preferred_medium
            )
        )
        target_manager.batch_apply(
            lambda table_path, client: client.set(
                ypath_join(table_path, "@primary_medium"), preferred_medium
            ),
            tables,
        )


def _create_ts_generator(
    source_manager,
    data_model_traits,
    tables,
    meta_cluster_yt_client=None,
):
    tables_attributes = source_manager.get_tables_attributes(
        tables, ["upstream_replica_id", "replication_card_id"]
    )

    def partition(iterable, predicate):
        satisfy, not_satisfy = [], []
        for table in iterable:
            if predicate(table):
                satisfy.append(table)
            else:
                not_satisfy.append(table)
        return satisfy, not_satisfy

    upstream_replica_id_presence, upstream_replica_id_absence = partition(
        tables_attributes,
        lambda table: table.attributes["upstream_replica_id"] != ZERO_UPSTREAM_REPLICA_ID,
    )
    replication_card_id_presence, replication_card_id_absence = partition(
        tables_attributes, lambda table: "replication_card_id" in table.attributes
    )

    if replication_card_id_presence:
        assert (
            not upstream_replica_id_absence
            and not replication_card_id_absence
            and meta_cluster_yt_client is not None
        ), 'Tables without "upstream_replica_id": {}, without "replication_card_id": {}, meta_cluster client presence: {}'.format(
            upstream_replica_id_absence,
            replication_card_id_absence,
            meta_cluster_yt_client is not None,
        )
        meta_manager = DbManager(
            meta_cluster_yt_client, source_manager.get_orm_path(), data_model_traits
        )
        ts_generator = ConsistentTimestampGenerator.create_chaos(
            tables, meta_db_manager=meta_manager, db_manager=source_manager
        )
    elif upstream_replica_id_presence:
        assert (
            not upstream_replica_id_absence and meta_cluster_yt_client is not None
        ), 'Tables without "upstream_replica_id": {}, meta_cluster client presence: {}'.format(
            upstream_replica_id_absence, meta_cluster_yt_client is not None
        )
        meta_manager = DbManager(
            meta_cluster_yt_client, source_manager.get_orm_path(), data_model_traits
        )
        ts_generator = ConsistentTimestampGenerator.create_replicated(
            tables, meta_db_manager=meta_manager, replica_db_manager=source_manager
        )
    else:
        ts_generator = ConsistentTimestampGenerator.create_simple(tables, db_manager=source_manager)

    return ts_generator


def _local_copy_db(
    source_manager,
    target_manager,
    tables,
    data_model_traits,
    meta_cluster_yt_client=None,
):
    ts_generator = _create_ts_generator(
        source_manager,
        data_model_traits,
        tables,
        meta_cluster_yt_client=meta_cluster_yt_client,
    )

    target_manager.get_yt_client().create(
        "map_node",
        target_manager.get_orm_path(),
        recursive=True,
    )

    consistent_timestamp = ts_generator.generate()
    with Freezer(
        source_manager.get_yt_client(),
        source_manager.get_orm_path(),
        data_model_traits,
        table_names=tables,
    ):
        if not ts_generator.try_check_generated_timestamp_obsoletion(consistent_timestamp):
            logger.info("Check generated timestamp obsoletion failed, generate once again")
            consistent_timestamp = ts_generator.generate()

        logger.info(
            "Tables are frozen, start local copying from {} to {}".format(
                source_manager.get_orm_path(), target_manager.get_orm_path()
            )
        )

        def copy_table(table_name, client):
            client.copy(
                source_manager.get_table_path(table_name),
                target_manager.get_table_path(table_name),
                recursive=True,
            )

        batch_apply(copy_table, tables, source_manager.get_yt_client())
        target_manager.batch_apply(
            lambda table_path, client: client.alter_table(
                table_path, upstream_replica_id=ZERO_UPSTREAM_REPLICA_ID
            ),
            tables,
        )
        logger.info("Local copying successful")

    return consistent_timestamp


def backup_by_copy(
    source_yt_client,
    source_orm_path,
    target_yt_client,
    target_orm_path,
    remove_if_exists,
    data_model_traits,
    action_after_copy=None,
    exclude_tables=None,
    include_tables=None,
    expiration_timeout=None,
    pool_size=10,
    meta_cluster_yt_client=None,
    preserve_medium=False,
    preferred_medium="default",
    preferred_remote_medium="default",
):
    if target_yt_client.exists(target_orm_path):
        if remove_if_exists:
            target_yt_client.remove(target_orm_path, force=True, recursive=True)
        else:
            raise RuntimeError('Target directory "{}" already exists'.format(target_orm_path))

    if expiration_timeout is None:
        expiration_timeout = data_model_traits.backup_expiration_timeout()

    source_manager = DbManager(source_yt_client, source_orm_path, data_model_traits)
    db_version = source_manager.read_version()
    tables = source_manager.get_tables_for_backup(
        exclude_tables=exclude_tables, include_tables=include_tables
    )

    profiling = {}
    start_time = time.time()
    if get_yt_cluster_name(source_yt_client) == get_yt_cluster_name(target_yt_client):
        try:
            target_manager = DbManager(
                target_yt_client, target_orm_path, data_model_traits, db_version
            )
            consistent_timestamp = _local_copy_db(
                source_manager,
                target_manager,
                tables,
                data_model_traits,
                meta_cluster_yt_client=meta_cluster_yt_client,
            )
            profiling = {"local_copying": time.time() - start_time}
            if not preserve_medium:
                set_preferred_medium_for_backup(target_manager, preferred_medium)
        except Exception as ex:
            _clean_snapshot(target_manager, logger)
            raise ex
    else:  # NB: YT does not support remote copy of dynamic tables.
        try:
            tmp_copy_path = _get_temp_clone_path(source_orm_path)
            tmp_copy_manager = DbManager(
                source_yt_client, tmp_copy_path, data_model_traits, db_version
            )
            consistent_timestamp = _local_copy_db(
                source_manager,
                tmp_copy_manager,
                tables,
                data_model_traits,
                meta_cluster_yt_client=meta_cluster_yt_client,
            )
            profiling = {"local_copying": time.time() - start_time}

            try:
                target_manager = DbManager(
                    target_yt_client, target_orm_path, data_model_traits, db_version
                )
                start_time = time.time()
                run_remote_copy_job(
                    tmp_copy_manager,
                    target_manager,
                    tables,
                    action_after_copy,
                    pool_size=pool_size,
                    preserve_medium=preserve_medium,
                    preferred_remote_medium=preferred_remote_medium,
                )
                profiling["remote_copying"] = time.time() - start_time
            except Exception as ex:
                _clean_snapshot(target_manager, logger)
                raise ex
        finally:
            _clean_snapshot(tmp_copy_manager, logger)

    start_time = time.time()
    db_proxy = source_yt_client.config["proxy"]["url"]
    _apply_db_meta(
        target_manager,
        source_manager,
        data_model_traits,
        db_proxy,
        db_version,
        tables,
        consistent_timestamp,
    )
    target_manager.batch_apply(
        lambda path, client: client.set(path + "/@monitor_table_statistics", False),
        tables,
    )
    if action_after_copy is not None:
        with yt.OperationsTrackerPool(pool_size=pool_size, client=target_yt_client) as tracker:
            for table in tables:
                action_after_copy(tracker, target_yt_client, target_manager.get_table_path(table))
    if expiration_timeout:
        target_yt_client.set(ypath_join(target_orm_path, "@expiration_timeout"), expiration_timeout)
    profiling["after_copying_actions"] = time.time() - start_time

    return consistent_timestamp, profiling


def backup_orm(
    yt_client,
    orm_path,
    data_model_traits,
    backup_paths=None,
    table_names=None,
    timestamp=None,
    expiration_timeout=None,
    disable_table_statistics_monitoring=True,
    preserve_medium=False,
    preferred_medium="default",
    use_native_backup=False,
    convert_to_dynamic=False,
    preserve_account=False,
    checkpoint_timestamp_delay=None,
    checkpoint_check_timeout=None,
):
    if expiration_timeout is None:
        expiration_timeout = data_model_traits.backup_expiration_timeout()
    if checkpoint_timestamp_delay is None:
        checkpoint_timestamp_delay = data_model_traits.backup_checkpoint_timestamp_delay()
    if checkpoint_check_timeout is None:
        checkpoint_check_timeout = data_model_traits.backup_checkpoint_check_timeout()

    source_manager = DbManager(yt_client, orm_path, data_model_traits)
    db_version = source_manager.read_version()

    assert (
        not convert_to_dynamic or use_native_backup
    ), "convert_to_dynamic can only be use with native backups enabled"
    assert (
        not preserve_account or use_native_backup
    ), "preserve_account can only be use with native backups enabled"

    if table_names is not None and not isinstance(table_names, list):
        raise RuntimeError("Parameter table_names must be list or None")

    if backup_paths is None:
        backup_paths = [_get_orm_backup_path(orm_path, db_version)]

    if not isinstance(backup_paths, list):
        raise RuntimeError("Parameter backup_paths must be list or None")

    if not backup_paths:
        raise RuntimeError(
            "Empty list was passed to backup_{}".format(data_model_traits.get_snake_case_name())
        )

    if not table_names:
        table_names = source_manager.get_tables_list()

    if not use_native_backup:
        tables_tablet_state = source_manager.get_tables_attributes(table_names, ["tablet_state"])
        for table in tables_tablet_state:
            if table.attributes["tablet_state"] not in ("frozen", "unmounted"):
                raise RuntimeError(
                    "Backup requires table {} to be frozen or unmounted".format(table)
                )

    cluster = get_yt_cluster_name(yt_client)

    for backup_path in backup_paths:
        target_manager = DbManager(yt_client, backup_path, data_model_traits)
        yt_client.create("map_node", target_manager.get_db_path(), recursive=True)
        logger.info(
            "Backing up {} {} environment from {} to {}".format(
                data_model_traits.get_human_readable_name(),
                "(Tables: {})".format(table_names),
                orm_path,
                backup_path,
            )
        )
        source_db_attributes = source_manager.get_db_attributes(
            ["acl", "inherit_acl", "finalization_timestamp"]
        )

        def copy_tables(table_name, client):
            client.copy(
                source_manager.get_table_path(table_name), target_manager.get_table_path(table_name)
            )

        if use_native_backup:
            backup_manifest = BackupManifest()
            cluster_manifest = ClusterBackupManifest()
            for table in table_names:
                cluster_manifest.add_table(
                    source_manager.get_table_path(table), target_manager.get_table_path(table)
                )
            backup_manifest.add_cluster(cluster, cluster_manifest)
            logger.info("Running native backup")
            backup_request_timeout = 3 * 60 * 1000
            backup_client = yt.YtClient(
                config=update(
                    yt_client.config, {"proxy": {"request_timeout": backup_request_timeout}}
                )
            )
            backup_client.create_table_backup(
                backup_manifest,
                checkpoint_timestamp_delay=checkpoint_timestamp_delay,
                checkpoint_check_timeout=checkpoint_check_timeout,
                # TODO(dgolear): Enable when this option is available on yp-* clusters.
                preserve_account=preserve_account,
            )
            if convert_to_dynamic:
                _convert_native_backup_to_dynamic(
                    backup_client, target_manager, table_names, preserve_account
                )

        else:
            batch_apply(copy_tables, table_names, yt_client)

        # User might not have "administer" rights.
        try:
            yt_client.set(
                ypath_join(target_manager.get_db_path(), "@acl"), source_db_attributes["acl"]
            )
            yt_client.set(
                ypath_join(target_manager.get_db_path(), "@inherit_acl"),
                source_db_attributes["inherit_acl"],
            )
            yt_client.set(
                ypath_join(target_manager.get_db_path(), "@finalization_timestamp"),
                source_db_attributes["finalization_timestamp"],
            )
        except:  # noqa
            logger.exception(
                "Cannot set rights for backup path {}".format(target_manager.get_db_path())
            )

        if disable_table_statistics_monitoring:
            target_manager.batch_apply(
                lambda table_path, client: client.remove(table_path + "/@monitor_table_statistics"),
                table_names,
            )
        if not preserve_medium:
            set_preferred_medium_for_backup(target_manager, preferred_medium)

        if expiration_timeout:
            yt_client.set(ypath_join(backup_path, "@expiration_timeout"), expiration_timeout)

        logger.info(
            "{} environment backed up from {} to {}".format(
                data_model_traits.get_human_readable_name(), orm_path, backup_path
            )
        )

        if timestamp is not None:
            yt_client.set(
                ypath_join(target_manager.get_db_path(), "@consistent_timestamp"), timestamp
            )


def run_merge_job(
    source_manager,
    target_manager,
    data_model_traits,
    tables_to_locked_node_ids,
    timestamp,
    transaction_id=None,
    temporary=True,
    combine_chunks=False,
    data_size_per_job=None,
    pool_size=10,
    preserve_medium=False,
    preferred_medium="default",
):
    """Makes a consistent temp copy of a running DB. Does not compact or mount."""
    expiration = None
    target_orm_path_attributes = {"monitor_table_statistics": False}
    if temporary:
        expiration = (datetime.utcnow() + timedelta(hours=12)).isoformat()
        target_orm_path_attributes["expiration_time"] = expiration

    logger.info(
        "Snapshotting {} environment from {} to {} at timestamp {}{}".format(
            data_model_traits.get_human_readable_name(),
            source_manager.get_orm_path(),
            target_manager.get_orm_path(),
            timestamp,
            ", expires at {}".format(expiration) if temporary else "",
        )
    )

    target_manager.yt_client.create(
        "map_node",
        target_manager.get_orm_path(),
        recursive=True,
        attributes=target_orm_path_attributes,
    )
    with yt.OperationsTrackerPool(pool_size=pool_size, client=target_manager.yt_client) as tracker:
        for table, node_id in tables_to_locked_node_ids.items():
            source_table_path = yt.YPath(node_id, attributes={"timestamp": timestamp})
            if transaction_id:
                source_table_path.attributes["transaction_id"] = transaction_id
            target_table_path = target_manager.get_table_path(table)
            schema = source_manager.get_table_schema(node_id=node_id)
            table_attributes = source_manager.yt_client.get(
                node_id,
                attributes=[
                    "uncompressed_data_size",
                    "tablet_count",
                    "optimize_for",
                    "monitor_table_statistics",
                    "primary_medium",
                ],
            )
            attributes = dict(
                dynamic=False,
                optimize_for=table_attributes.attributes["optimize_for"],
                primary_medium=(
                    table_attributes.attributes["primary_medium"]
                    if preserve_medium
                    else preferred_medium
                ),
            )

            target_manager.create_table(table, schema=schema, attributes=attributes, recursive=True)
            target_manager.yt_client.remove(
                ypath_join(target_table_path, "@monitor_table_statistics"), force=True
            )
            desired_chunk_size = min(
                table_attributes.attributes["uncompressed_data_size"]
                // max(table_attributes.attributes["tablet_count"], 1)
                // 2,
                100 * 1024**2,
            )
            if not desired_chunk_size:
                desired_chunk_size = 100 * 1024**2

            # Choose some reasonable time limit to prevent too many hung operations.
            # We suppose that operations are executed in fair share pool.
            time_limit_in_minutes = 90 + table_attributes.attributes["uncompressed_data_size"] // (
                20 * 1024**3
            )

            spec = (
                yt.spec_builders.MergeSpecBuilder()
                .title("snapshot {} table".format(table))
                .input_table_paths(source_table_path)
                .output_table_path(target_table_path)
                .mode("ordered")
                .combine_chunks(combine_chunks)
                .time_limit(time_limit_in_minutes * 60 * 1000)
                .begin_job_io()
                .table_writer({"block_size": 256 * 1024, "desired_chunk_size": desired_chunk_size})
                .end_job_io()
            )  # noqa
            if combine_chunks or data_size_per_job:
                spec = spec.data_size_per_job(data_size_per_job or (2 * 1024**3))
            tracker.add(spec)

    logger.info("{} environment snapshotted".format(data_model_traits.get_human_readable_name()))


def _clean_snapshot(db_manager, logger):
    logger.info(
        "Removing temporary/failed snapshot {} from cluster {}".format(
            db_manager.get_orm_path(), get_yt_cluster_name(db_manager.get_yt_client())
        )
    )
    try:
        db_manager.yt_client.remove(db_manager.get_orm_path(), force=True, recursive=True)
    except:  # noqa
        logger.exception("Error removing snapshot")


def restore_orm(yt_client, orm_path, data_model_traits, backup_path):
    logger.info(
        "Restoring %s environment from %s to %s",
        data_model_traits.get_human_readable_name(),
        backup_path,
        orm_path,
    )
    preparer = DbManager(yt_client, orm_path, data_model_traits)
    preparer.unmount_all_tables()
    yt_client.copy(
        ypath_join(backup_path, "db"),
        ypath_join(orm_path, "db"),
        recursive=True,
        force=True,
    )
    preparer.mount_unmounted_tables()
    logger.info(
        "%s environment restored from %s to %s",
        data_model_traits.get_human_readable_name(),
        backup_path,
        orm_path,
    )


def freeze_orm(yt_client, orm_path, data_model_traits, table_names=None):
    logger.info(
        "Freezing {} database {}".format(
            data_model_traits.get_human_readable_name(),
            "(Tables: {})".format(table_names) if table_names else "",
        )
    )

    db_manager = DbManager(yt_client, orm_path, data_model_traits)
    tables = db_manager.get_tables_list() if not table_names else table_names
    db_manager.freeze_tables(tables, verify_previous_state=False)
    logger.info("{} database is frozen".format(data_model_traits.get_human_readable_name()))


def unfreeze_orm(yt_client, orm_path, data_model_traits, table_names=None):
    logger.info(
        "Unfreezing {} database {}".format(
            data_model_traits.get_human_readable_name(),
            "(Tables: {})".format(table_names) if table_names else "",
        )
    )
    db_manager = DbManager(yt_client, orm_path, data_model_traits)
    tables = db_manager.get_tables_list() if not table_names else table_names
    db_manager.unfreeze_tables(tables, verify_previous_state=False)
    logger.info("{} database is unfrozen".format(data_model_traits.get_human_readable_name()))


class Freezer(object):
    def __init__(self, yt_client, orm_path, data_model_traits, table_names=None):
        self._yt_client = yt_client
        self._orm_path = orm_path
        self._data_model_traits = data_model_traits
        self._table_names = table_names
        self._leave_frozen_on_exit = False

    def __enter__(self):
        freeze_orm(
            self._yt_client, self._orm_path, self._data_model_traits, table_names=self._table_names
        )
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        if self._leave_frozen_on_exit:
            logger.info(
                "Leaving {} database frozen".format(
                    self._data_model_traits.get_human_readable_name()
                )
            )
        else:
            try:
                unfreeze_orm(
                    self._yt_client,
                    self._orm_path,
                    self._data_model_traits,
                    table_names=self._table_names,
                )
            except:  # noqa
                if exc_value:
                    # Log and drop inner exception.
                    logger.exception("Failed to unfreeze database while handling exception")
                else:
                    raise

    def leave_frozen_on_exit(self):
        self._leave_frozen_on_exit = True


def dump_db(yt_client, orm_path, dump_dir, data_model_traits):
    if os.path.exists(dump_dir):
        shutil.rmtree(dump_dir)

    db_orm_path = ypath_join(orm_path, "db")
    db_dump_dir = os.path.join(dump_dir, "tables")

    os.makedirs(dump_dir)
    os.makedirs(db_dump_dir)
    db_manager = DbManager(yt_client, orm_path, data_model_traits)

    path_name = "{}_path".format(data_model_traits.get_snake_case_name())
    dump_meta = {}
    dump_meta["yt_proxy"] = yt_client.config["proxy"]["url"]
    dump_meta[path_name] = orm_path
    dump_meta["version"] = db_manager.read_version()
    dump_meta["dump_date"] = time.strftime("%d.%m.%Y %H:%M:%S")

    tables = db_manager.get_tables_for_backup()
    ts_generator = ConsistentTimestampGenerator.create_simple(tables, db_manager=db_manager)
    _, timestamp = ts_generator.get_retained_and_unflushed_timestamp_bounds()

    dump_meta["tables"] = tables
    dump_meta["flushed_timestamp"] = timestamp

    for table in dump_meta["tables"]:
        with open(os.path.join(db_dump_dir, table), "w") as table_dump:
            table_path = ypath_join(db_orm_path, table)
            for row in yt_client.select_rows(
                "* from [{}]".format(table_path),
                timestamp=dump_meta["flushed_timestamp"],
            ):
                table_dump.write("{}\n".format(yson.dumps(row, yson_format="text")))

    with open(os.path.join(dump_dir, "dump_meta.yson"), "wb") as fout:
        yson.dump(dump_meta, fout, yson_format="pretty")
        fout.write(b"\n")


def _convert_native_backup_to_dynamic(yt_client, db_manager, table_names, preserve_account=False):
    cluster = get_yt_cluster_name(yt_client)
    tables = db_manager.get_tables_attributes(
        table_names, absolute=True, attributes=["in_memory_mode", "enable_lookup_hash_table"]
    )

    backup_manifest = BackupManifest()
    cluster_manifest = ClusterBackupManifest()
    for table_path in tables:
        cluster_manifest.add_table(table_path, table_path + "_restored")
    backup_manifest.add_cluster(cluster, cluster_manifest)

    logger.info("Restoring backup tables to dynamic (Manifest: {})".format(backup_manifest))
    run_with_retries(
        lambda: yt_client.restore_table_backup(backup_manifest, preserve_account=preserve_account),
        backoff=3.0,
    )

    def apply(fn, msg=None, iterable=tables):
        if msg:
            logger.info(msg)
        return batch_apply(fn, iterable, yt_client)

    apply(
        lambda path, client: client.move(path + "_restored", path, force=True),
        "Moving restored tables",
    )

    def apply_attributes(path, client, in_memory_mode, enable_lookup_hash_table):
        client.set_attribute(path, "in_memory_mode", in_memory_mode)
        client.set_attribute(path, "enable_lookup_hash_table", enable_lookup_hash_table)

    apply(
        lambda path, client: apply_attributes(path, client, "none", False),
        "Disabling in_memory_mode",
    )
    apply(
        lambda path, client: client.set(path + "/@forced_chunk_view_compaction_revision", 1),
        "Forcing chunk view compaction",
    )
    db_manager.mount_tables(tables)
    apply(lambda path, client: client.remove(path + "/@forced_chunk_view_compaction_revision"))

    def no_chunk_views():
        chunk_list_ids = apply(
            lambda path, client: client.get(path + "/@chunk_list_id"),
            "Getting tables chunk list id",
        )
        per_table_tablet_chunk_list_ids = apply(
            lambda id, client: client.get("#" + id + "/@child_ids"),
            iterable=chunk_list_ids,
        )
        result = apply(
            lambda id, client: client.get("#" + id + "/@child_ids"),
            "Getting chunk list ids",
            iterable=[id for ids in per_table_tablet_chunk_list_ids for id in ids],
        )

        def is_chunk_view(id):
            type = str(id).split("-")[2][-4:].lstrip("0")
            return type == "7b"

        return all(not is_chunk_view(id) for id in result)

    wait(no_chunk_views)

    db_manager.unmount_tables(tables)
    apply(
        lambda path, client: apply_attributes(
            path,
            client,
            path.attributes.get("in_memory_mode", "none"),
            path.attributes.get("enable_lookup_hash_table", False),
        ),
        "Restoring in_memory_mode",
    )


def acquire_snapshot_lock(db_manager, tables):
    logger.info("Acquiring locks")
    locks = db_manager.batch_apply(
        lambda path, client: client.lock(path, mode="snapshot"),
        tables,
    )
    return {table: "#" + lock["node_id"] for table, lock in zip(tables, locks)}


def _make_db_snapshot(
    source_manager,
    target_manager,
    data_model_traits,
    tables,
    temporary=False,
    combine_chunks=False,
    data_size_per_job=None,
    pool_size=10,
    meta_cluster_yt_client=None,
    preserve_medium=False,
    preferred_medium="default",
):
    ts_generator = _create_ts_generator(
        source_manager,
        data_model_traits,
        tables,
        meta_cluster_yt_client=meta_cluster_yt_client,
    )

    consistent_timestamp = ts_generator.generate()
    with source_manager.yt_client.Transaction() as transaction:
        tables_to_locked_node_ids = acquire_snapshot_lock(source_manager, tables)

        if not ts_generator.try_check_generated_timestamp_obsoletion(consistent_timestamp):
            logger.info("Check generated timestamp obsoletion failed, generate once again")
            consistent_timestamp = ts_generator.generate()

        try:
            run_merge_job(
                source_manager,
                target_manager,
                data_model_traits,
                tables_to_locked_node_ids,
                consistent_timestamp,
                transaction_id=transaction.transaction_id,
                temporary=temporary,
                combine_chunks=combine_chunks,
                data_size_per_job=data_size_per_job,
                pool_size=pool_size,
                preserve_medium=preserve_medium,
                preferred_medium=preferred_medium,
            )
        except Exception as ex:
            _clean_snapshot(target_manager, logger)
            raise ex
    return consistent_timestamp


def run_remote_copy_job(
    source_manager,
    target_manager,
    tables,
    action_after_copy,
    pool_size=10,
    preserve_medium=False,
    preferred_remote_medium="default",
):
    target_yt_client = target_manager.get_yt_client()
    source_cluster_name = get_yt_cluster_name(source_manager.get_yt_client())
    target_cluster_name = get_yt_cluster_name(target_yt_client)
    # Unspecified network name usually corresponds to the "fastbone".
    # YP clusters live within non-MTN networks and do not support fastbone,
    # so the "default" network must be specified explicitly.
    remote_copy_network_name = None
    if source_cluster_name.startswith("yp-") or target_cluster_name.startswith("yp-"):
        remote_copy_network_name = "default"
        logger.info(
            "Specifying %s remote copy network name explicitly because source %s or target %s cluster name matches yp-* pattern",
            remote_copy_network_name,
            source_cluster_name,
            target_cluster_name,
        )

    with yt.OperationsTrackerPool(pool_size=pool_size, client=target_yt_client) as tracker:
        logger.info("Copying snapshot to target cluster")
        for table in tables:
            source_table_path = source_manager.get_table_path(table)
            table_attributes = source_manager.yt_client.get(
                source_table_path, attributes=["primary_medium"]
            )

            target_medium = (
                table_attributes.attributes["primary_medium"]
                if preserve_medium
                else preferred_remote_medium
            )
            target_table_path = target_manager.get_table_path(table)
            target_yt_client.create(
                "table",
                target_table_path,
                attributes=dict(primary_medium=target_medium),
                recursive=True,
                ignore_existing=True,
            )

            remote_copy_spec = {
                "time_limit": 2
                * 60
                * 60
                * 1000  # 2 hours in milliseconds; prevent too many hung operations.
            }

            logger.info(
                "Running remote copy operation from {} to {}".format(
                    source_table_path, target_table_path
                )
            )
            spec = (
                yt.spec_builders.RemoteCopySpecBuilder()
                .title("Remote copy {} table".format(table))
                .input_table_paths(source_table_path)
                .output_table_path(target_table_path)
                .schema_inference_mode("from_input")
                .cluster_name(source_cluster_name)
                .network_name(remote_copy_network_name)
                .copy_attributes(True)
                .spec(remote_copy_spec)
            )  # noqa
            tracker.add(spec)
    if action_after_copy:
        with yt.OperationsTrackerPool(pool_size=pool_size, client=target_yt_client) as tracker:
            for table in tables:
                action_after_copy(tracker, target_yt_client, target_manager.get_table_path(table))


def _apply_db_meta(
    target_manager,
    source_manager,
    data_model_traits,
    db_proxy,
    db_version,
    tables,
    timestamp,
):
    path_name = "{}_path".format(data_model_traits.get_snake_case_name())
    db_meta = {
        "yt_proxy": db_proxy,
        path_name: source_manager.get_orm_path(),
        "version": db_version,
        "tables": tables,
        "consistent_timestamp": timestamp,
    }
    for key, value in db_meta.items():
        target_manager.get_yt_client().set(
            ypath_join(target_manager.get_db_path(), "@" + key), value
        )


def clone_db(
    source_yt_client,
    source_orm_path,
    target_yt_client,
    target_orm_path,
    remove_if_exists,
    data_model_traits,
    action_after_copy=None,
    exclude_tables=None,
    include_tables=None,
    data_size_per_job=None,
    expiration_timeout=None,
    local_pool_size=10,
    remote_pool_size=10,
    meta_cluster_yt_client=None,
    preserve_medium=False,
    preferred_medium="default",
    preferred_remote_medium="default",
):
    if target_yt_client.exists(target_orm_path):
        if remove_if_exists:
            target_yt_client.remove(target_orm_path, force=True, recursive=True)
        else:
            raise yt.YtError("Target directory already exists")
    if expiration_timeout is None:
        expiration_timeout = data_model_traits.backup_expiration_timeout()

    source_cluster_name = get_yt_cluster_name(source_yt_client)
    target_cluster_name = get_yt_cluster_name(target_yt_client)

    db_version = get_db_version(source_yt_client, source_orm_path, data_model_traits)
    db_proxy = source_yt_client.config["proxy"]["url"]

    source_manager = DbManager(
        source_yt_client,
        source_orm_path,
        data_model_traits,
        db_version,
    )
    target_manager = DbManager(
        copy_client(target_yt_client),
        target_orm_path,
        data_model_traits,
        db_version,
    )
    tables = source_manager.get_tables_for_backup(
        exclude_tables=exclude_tables, include_tables=include_tables
    )
    if source_cluster_name == target_cluster_name:
        profiling = _clone_local_db(
            source_manager,
            target_manager,
            tables,
            data_model_traits,
            action_after_copy,
            db_version,
            db_proxy,
            data_size_per_job=data_size_per_job,
            pool_size=local_pool_size,
            meta_cluster_yt_client=meta_cluster_yt_client,
            preserve_medium=preserve_medium,
            preferred_medium=preferred_medium,
        )
    else:
        profiling = _clone_remote_db(
            source_manager,
            target_manager,
            tables,
            data_model_traits,
            action_after_copy,
            db_version,
            db_proxy,
            data_size_per_job=data_size_per_job,
            local_pool_size=local_pool_size,
            remote_pool_size=remote_pool_size,
            meta_cluster_yt_client=meta_cluster_yt_client,
            preserve_medium=preserve_medium,
            preferred_medium=preferred_medium,
            preferred_remote_medium=preferred_remote_medium,
        )
    if expiration_timeout:
        target_yt_client.set(ypath_join(target_orm_path, "@expiration_timeout"), expiration_timeout)
    return profiling


def _clone_local_db(
    source_manager,
    target_manager,
    tables,
    data_model_traits,
    action_after_copy,
    db_version,
    db_proxy,
    data_size_per_job=None,
    pool_size=10,
    meta_cluster_yt_client=None,
    preserve_medium=False,
    preferred_medium="default",
):
    start_time = time.time()
    timestamp = _make_db_snapshot(
        source_manager,
        target_manager,
        data_model_traits,
        tables,
        temporary=False,
        combine_chunks=True,
        data_size_per_job=data_size_per_job,
        pool_size=pool_size,
        meta_cluster_yt_client=meta_cluster_yt_client,
        preserve_medium=preserve_medium,
        preferred_medium=preferred_medium,
    )

    if action_after_copy:
        with yt.OperationsTrackerPool(
            pool_size=pool_size, client=target_manager.get_yt_client()
        ) as tracker:
            for table in tables:
                target_table_path = target_manager.get_table_path(table)
                action_after_copy(tracker, target_manager.get_yt_client(), target_table_path)

    profiling = {"create_snapshot_duration": time.time() - start_time}

    _apply_db_meta(
        target_manager,
        source_manager,
        data_model_traits,
        db_proxy,
        db_version,
        tables,
        timestamp,
    )

    profiling["total_duration"] = time.time() - start_time
    return profiling


def _clone_remote_db(
    source_manager,
    target_manager,
    tables,
    data_model_traits,
    action_after_copy,
    db_version,
    db_proxy,
    data_size_per_job=None,
    local_pool_size=10,
    remote_pool_size=10,
    meta_cluster_yt_client=None,
    preserve_medium=False,
    preferred_medium="default",
    preferred_remote_medium="default",
):
    snapshot_path = _get_temp_clone_path(source_manager.get_orm_path())
    snapshot_manager = DbManager(
        copy_client(source_manager.get_yt_client()),
        snapshot_path,
        data_model_traits,
        db_version,
    )

    start_time = time.time()
    timestamp = _make_db_snapshot(
        source_manager,
        snapshot_manager,
        data_model_traits,
        tables,
        temporary=True,
        combine_chunks=True,
        data_size_per_job=data_size_per_job,
        pool_size=local_pool_size,
        meta_cluster_yt_client=meta_cluster_yt_client,
        preserve_medium=preserve_medium,
        preferred_medium=preferred_medium,
    )
    profiling = {"create_snapshot_duration": time.time() - start_time}

    try:
        start_copy_time = time.time()
        run_remote_copy_job(
            snapshot_manager,
            target_manager,
            tables,
            action_after_copy,
            pool_size=remote_pool_size,
            preserve_medium=preserve_medium,
            preferred_remote_medium=preferred_remote_medium,
        )
        profiling["remote_copy_duration"] = time.time() - start_copy_time
    finally:
        _clean_snapshot(snapshot_manager, logger)

    _apply_db_meta(
        target_manager,
        source_manager,
        data_model_traits,
        db_proxy,
        db_version,
        tables,
        timestamp,
    )

    profiling["total_duration"] = time.time() - start_time
    return profiling


def diff_db(yt_client, source_orm_path, target_orm_path, data_model_traits):
    def _build_db_full_data(yt_client, path, data_model_traits):
        db_manager = DbManager(yt_client, path, data_model_traits)
        db_manager.mount_all_tables()

        result = {}
        LIMIT = 10000000
        for table in db_manager.get_tables_list():
            if is_watch_log(table):
                result[table] = None
            else:
                result[table] = list(
                    yt_client.select_rows(
                        "* FROM [{}] LIMIT {}".format(db_manager.get_table_path(table), LIMIT),
                        format="json",
                    )
                )
        return result

    source_db = _build_db_full_data(yt_client, source_orm_path, data_model_traits)
    target_db = _build_db_full_data(yt_client, target_orm_path, data_model_traits)

    for table in set(source_db.keys()) | set(target_db.keys()):
        if table in source_db and table not in target_db:
            print("{} table found only in {}".format(table, source_orm_path))
            continue

        if table in target_db and table not in source_db:
            print("{} table found only in {}".format(table, target_orm_path))
            continue

        # TODO: Show content diff
        if source_db[table] != target_db[table]:
            print("{} content has diff!".format(table))


# Best-effort to restore clean ORM database state. Useful for tests.
def reset_orm(
    orm_client,
    order_of_object_type_removal,
    ignored_object_type_ids=None,
):
    # This function is called per every test, try to fine-tune its performance, please.

    final_updates = []
    object_type_ids = (
        ("user", "root"),
        ("group", "superusers"),
    ) + (ignored_object_type_ids or tuple())

    for object_type in order_of_object_type_removal:
        if object_type == "schema":
            continue

        final_updates.append(
            dict(
                object_type="schema",
                object_id=object_type,
                set_updates=[dict(path="/meta/acl", value=get_schema_acl(object_type))],
            )
        )

        class BatchRemover(object):
            def __init__(self):
                self.type_ids_to_remove = self.select_type_ids_to_remove()
                self.max_batch_size = 16
                self.batch_size = 16

            def select_type_ids_to_remove(self):
                responses = orm_client.select_objects(
                    object_type,
                    selectors=(["/meta/key"]),
                    common_options={"allow_full_scan": True},
                )
                type_ids_to_remove = []
                for response in responses:
                    type_id = (object_type, response[0])
                    if type_id not in object_type_ids:
                        type_ids_to_remove.append(type_id)
                return type_ids_to_remove

            def __call__(self):
                while len(self.type_ids_to_remove) > 0:
                    batch = self.type_ids_to_remove[: self.batch_size]
                    # Batch size will be divided by two in case of failure
                    # and increased by one in case of success.
                    self.batch_size = max(1, self.batch_size // 2)
                    try:
                        orm_client.remove_objects(
                            batch, allow_removal_with_non_empty_reference=True
                        )
                    except NoSuchObjectError:
                        # Probably previously timed out request has removed some objects.
                        self.type_ids_to_remove = self.select_type_ids_to_remove()
                        raise
                    self.batch_size = min(self.max_batch_size, (self.batch_size * 2) + 1)
                    self.type_ids_to_remove = self.type_ids_to_remove[len(batch) :]

        batch_remover = BatchRemover()

        # Wait for the scheduler to remove orphaned allocations.
        # Retry to overcome tablet lock conflicts.
        run_with_retries(
            batch_remover,
            retry_count=30,
            backoff=0.5,
            exceptions=(Exception,),
        )

    final_updates.append(
        dict(
            object_type="group",
            object_id="superusers",
            set_updates=[
                dict(
                    path="/spec/members",
                    value=["root"],
                )
            ],
        )
    )

    orm_client.update_objects(final_updates)


def drop_na(row):
    for key in list(row):
        if row[key] is None or isinstance(row[key], YsonEntity):
            row.pop(key)
    return row


def cleanup_history_table(
    yt_client, orm_path, object_type, data_model_traits, start_time, finish_time=None
):
    assert (finish_time is None) or (start_time <= finish_time)

    def mapper(row):
        if (
            (row["object_type"] == object_type)
            and (start_time <= row["time"])
            and (finish_time is None or row["time"] <= finish_time)
        ):
            pass
        else:
            yield row

    table_name = "history_events"
    db_manager = DbManager(
        yt_client,
        orm_path,
        data_model_traits,
    )
    db_manager.read_version()
    table_path = db_manager.get_table_path(table_name)
    db_manager.unmount_tables([table_name])

    backup_table_path = _make_backup_path(
        orm_path, "backups", "history/{}_v{}".format(table_name, db_manager.get_version())
    )
    logger.info("Copying from '%s' to '%s'", table_path, backup_table_path)
    yt_client.copy(table_path, backup_table_path, recursive=True)
    logger.info("Backup created: %s", backup_table_path)

    db_manager.run_map(table_name, mapper)
    db_manager.mount_tables([table_path])


class Migrator(object):
    DEFAULT_BACKUP_PATH = object()

    def __init__(
        self,
        yt_client,
        orm_path,
        target_version,
        data_model_traits,
        backup_path=DEFAULT_BACKUP_PATH,
        mount_wait_time=None,
        expiration_timeout=None,
        forced_compaction=None,
        preserve_medium=False,
        preferred_medium="default",
        use_native_backup=False,
        checkpoint_timestamp_delay=None,
        checkpoint_check_timeout=None,
    ):
        self._yt_client = yt_client
        self._yt_proxy = self._yt_client.config["proxy"]["url"]
        self._orm_path = orm_path
        self._orm_tmp_path = ypath_join(orm_path, "tmp")
        self._data_model_traits = data_model_traits
        self._mount_wait_time = mount_wait_time
        self._init_versions(target_version)
        self.set_backup_path(backup_path)
        if expiration_timeout is None:
            self._expiration_timeout = data_model_traits.backup_expiration_timeout()
        else:
            self._expiration_timeout = expiration_timeout
        self._forced_compaction = forced_compaction or "wait"
        self._preserve_medium = preserve_medium
        self._preferred_medium = preferred_medium
        self._use_native_backup = use_native_backup
        self._checkpoint_timestamp_delay = checkpoint_timestamp_delay
        self._checkpoint_check_timeout = checkpoint_check_timeout

    def set_backup_path(self, backup_path=DEFAULT_BACKUP_PATH):
        """Set to None to disable backup."""
        if backup_path is self.DEFAULT_BACKUP_PATH:
            self._backup_path = _get_orm_backup_path(self._orm_path, self._current_version)
        else:
            self._backup_path = backup_path

    def _create_db_manager(self, path, **kwargs):
        return DbManager(
            self._yt_client,
            path,
            self._data_model_traits,
            self._current_version,
            tmp_path=self._orm_tmp_path,
            **kwargs
        )

    def migrate_dry_run(self, remove_snapshot=True, rethrow_error=False):
        working_copy_path = _get_orm_migration_path(
            self._orm_path, self._current_version, self._target_version
        )
        logger.info("Migration dry-run via " + working_copy_path)
        source_manager = self._create_db_manager(self._orm_path)
        self._db_manager = self._create_db_manager(working_copy_path)
        tables = source_manager.get_tables_list(skip_watch_logs=True)
        try:
            self._prepare_db_snapshot(source_manager, tables)
            self._db_manager.mount_all_tables()
            self._perform_migrations()
            self._db_manager.finalize()
            self._compact_tables()
            self._db_manager = None
        except:  # noqa
            logger.exception(
                "Migration failed. Do you wish to remove temp database {}? [Yy/Nn]".format(
                    working_copy_path
                )
            )
            if rethrow_error:
                remove_snapshot = True
                raise
            answer = sys.stdin.readline().strip()
            remove_snapshot = answer == "Y" or answer == "y"
        else:
            logger.info("Migration successful")
        finally:
            if remove_snapshot:
                self._yt_client.remove(working_copy_path, recursive=True, force=True)
                logger.info("Temp database removed")

    def migrate_in_place(self, verify_schemas=False):
        if not self.is_migration_needed():
            return
        logger.info("Migrating in place")

        working_copy_path, _ = self.start_readonly_migration()
        self.finish_readonly_migration(working_copy_path)
        if verify_schemas:
            self._db_manager.verify_all_tables_schema()

    def start_readonly_migration(self):
        with Freezer(self._yt_client, self._orm_path, self._data_model_traits) as freezer:
            working_copy_path = _get_orm_migration_path(
                self._orm_path, self._current_version, self._target_version
            )
            logger.info("Migrating via " + working_copy_path)

            self._backup()
            # NB! Separate call to backup because this backup does not request timeout/medium configuration.
            backup_orm(
                self._yt_client,
                self._orm_path,
                self._data_model_traits,
                backup_paths=[working_copy_path],
                expiration_timeout=0,
                disable_table_statistics_monitoring=False,
                preserve_medium=True,
            )
            self._db_manager = self._create_db_manager(
                working_copy_path, mount_wait_time=self._mount_wait_time
            )
            self._before_migrations()
            self._perform_migrations()
            self._after_migrations()
            self._db_manager = None
            freezer.leave_frozen_on_exit()
            return working_copy_path, self._backup_path

    def finish_readonly_migration(self, working_copy_path):
        self._move_migrated_tables_from(working_copy_path)
        self._db_manager = self._create_db_manager(self._orm_path)

        assert self._db_manager.read_version() == self._target_version

        self._db_manager.mount_all_tables()
        self._yt_client.remove(working_copy_path, recursive=True, force=True)
        logger.info("Migration successful, DB operations may be initiated")

    def _before_migrations(self):
        assert self._db_manager

        self._per_table_attributes = self._db_manager.get_tables_list(
            attributes=["in_memory_mode", "enable_lookup_hash_table"], absolute=True
        )

        def disable_attributes(table_path, client):
            client.set(ypath_join(table_path, "@in_memory_mode"), "none")
            client.set(ypath_join(table_path, "@enable_lookup_hash_table"), False)

        batch_apply(
            disable_attributes,
            self._per_table_attributes,
            self._yt_client,
        )

        self._db_manager.mount_all_tables()

    def _after_migrations(self):
        assert self._db_manager

        self._db_manager.finalize()
        self._compact_tables()
        self._db_manager.unmount_all_tables()

        actual_tables = set(str(t) for t in self._db_manager.get_tables_list(absolute=True))

        def enable_attributes(table_path, client):
            if str(table_path) not in actual_tables:
                logger.info("Table {} deleted during migrations".format(table_path))
                return
            for attribute in table_path.attributes:
                client.set(
                    ypath_join(table_path, "@" + attribute), table_path.attributes[attribute]
                )

        batch_apply(
            enable_attributes,
            self._per_table_attributes,
            self._yt_client,
        )

    def _validate_version_ordering(
        self, old_version_name, old_version, new_version_name, new_version
    ):
        if old_version > new_version:
            raise ClientError(
                "{} DB version {} is larger than {} DB version {}".format(
                    old_version_name, old_version, new_version_name, new_version
                ),
            )

    def is_migration_needed(self):
        if self._current_version == self._target_version:
            logger.info(
                "Current and target DB versions are already the same ({})".format(
                    self._target_version
                )
            )
            return False
        return True

    def _init_versions(self, target_version):
        self._current_version = get_db_version(
            self._yt_client, self._orm_path, self._data_model_traits
        )
        self._target_version = (
            target_version if target_version is not None else self._current_version
        )

        self._validate_version_ordering(
            "Minimal known",
            self._data_model_traits.get_initial_db_version(),
            "current",
            self._current_version,
        )
        self._validate_version_ordering(
            "Current", self._current_version, "target", self._target_version
        )
        self._validate_version_ordering(
            "Target",
            self._target_version,
            "maximal known",
            self._data_model_traits.get_actual_db_version(),
        )

    def _get_backup_paths(self):
        backup_paths = []
        if self._backup_path:
            backup_paths.append(self._backup_path)
        return backup_paths

    def _backup(self):
        if self._get_backup_paths():
            backup_orm(
                self._yt_client,
                self._orm_path,
                self._data_model_traits,
                backup_paths=self._get_backup_paths(),
                expiration_timeout=self._expiration_timeout,
                preserve_medium=self._preserve_medium,
                preferred_medium=self._preferred_medium,
            )

    def _prepare_db_snapshot(self, source_manager, tables):
        attributes_override = dict(in_memory_mode="none", enable_lookup_hash_table=False)
        if not self._preserve_medium:
            attributes_override["preferred_medium"] = self._preferred_medium
        if self._use_native_backup:
            backup_orm(
                self._yt_client,
                self._orm_path,
                self._data_model_traits,
                backup_paths=[self._db_manager.get_orm_path()],
                expiration_timeout=self._expiration_timeout,
                preserve_medium=self._preserve_medium,
                preferred_medium=self._preferred_medium,
                use_native_backup=True,
                convert_to_dynamic=True,
                checkpoint_timestamp_delay=self._checkpoint_timestamp_delay,
                checkpoint_check_timeout=self._checkpoint_check_timeout,
            )

            def apply_attributes(table_path, client):
                for attribute in attributes_override:
                    client.set(
                        ypath_join(table_path, "@" + attribute), attributes_override[attribute]
                    )

            self._db_manager.batch_apply(apply_attributes, self._db_manager.get_tables_list())
        else:
            _make_db_snapshot(
                source_manager,
                self._db_manager,
                self._data_model_traits,
                tables,
                temporary=True,
                preserve_medium=self._preserve_medium,
                preferred_medium=self._preferred_medium,
            )
            convert_static_to_dynamic_db_by_prototype(
                self._db_manager, source_manager, attributes_override
            )

    def _perform_migrations(self):
        while self._current_version < self._target_version:
            next_version = self._current_version + 1
            migration_method = self._data_model_traits.get_migration_methods().get(next_version)
            logger.info(
                "Started migrating {} environment from version {} to version {}".format(
                    self._data_model_traits.get_human_readable_name(),
                    self._current_version,
                    next_version,
                )
            )
            self._db_manager.set_version(next_version)
            migration_method(self._db_manager)
            logger.info(
                "Finished migrating {} environment from version {} to version {}".format(
                    self._data_model_traits.get_human_readable_name(),
                    self._current_version,
                    next_version,
                )
            )
            self._current_version = next_version

    def _compact_tables(self):
        if self._forced_compaction == "skip":
            return

        def does_table_have_proper_chunks(table):
            if not table.attributes["sorted"]:
                logger.debug("Table %s is ordered", table)
                return True
            chunk_count = table.attributes["chunk_count"]
            chunk_format_statistics = table.attributes["table_chunk_format_statistics"]
            chunk_count_per_format = dict(
                [
                    (format, chunk_format_statistics.get(format, {}).get("chunk_count", 0))
                    for format in chunk_format_statistics
                ]
            )
            versioned_chunk_formats = ("versioned_simple", "versioned_columnar")
            versioned_chunk_count = sum(
                [chunk_count_per_format.get(format, 0) for format in versioned_chunk_formats]
            )
            if versioned_chunk_count != chunk_count:
                logger.info(
                    "Found table with chunk(s) of unversioned format (table: %s, chunk_count: %s, chunk_count_per_format: %s)",
                    table,
                    chunk_count,
                    chunk_count_per_format,
                )
                return False
            return True

        def list_tables():
            return self._db_manager.get_tables_list(
                absolute=True,
                attributes=["chunk_count", "table_chunk_format_statistics", "sorted"],
            )

        def get_tables_with_improper_chunks():
            return [table for table in list_tables() if not does_table_have_proper_chunks(table)]

        # Set and remount actions must be separate,
        # because it is not safe to perform dependent actions within
        # the same batch client session due to retries.
        tables_to_compact = get_tables_with_improper_chunks()

        table_names = [yt.ypath_split(table)[-1] for table in tables_to_compact]
        logger.info("Requesting forced compaction for tables {}".format(table_names))
        batch_apply(
            lambda table, client: client.set(ypath_join(table, "@forced_compaction_revision"), 1),
            tables_to_compact,
            self._yt_client,
        )

        logger.info("Remounting tables {}".format(table_names))
        batch_apply(
            # Remount is supposed to preserve tablet state.
            lambda table, client: client.remount_table(table),
            tables_to_compact,
            self._yt_client,
        )

        if self._forced_compaction == "request":
            return
        assert self._forced_compaction == "wait", "Unexpected forced compaction mode"

        logger.info("Waiting for all data to become compacted")

        def check():
            return all(map(does_table_have_proper_chunks, list_tables()))

        wait(check, iter=200, sleep_backoff=2)

    def _move_migrated_tables_from(self, working_copy_path):
        logger.info(
            "Moving %s environment from %s to %s",
            self._data_model_traits.get_human_readable_name(),
            working_copy_path,
            self._orm_path,
        )
        run_with_retries(
            lambda: self._yt_client.move(
                ypath_join(working_copy_path, "db"),
                ypath_join(self._orm_path, "db"),
                force=True,
            ),
            retry_count=10,
            backoff=10.0,
        )

    def list_migrations_without_downtime(self, quiet=False):
        migration_methods = self._data_model_traits.get_migrations_without_downtime()
        dynconfig = OrmDynamicConfig(self._yt_client, self._orm_path)

        if quiet and not dynconfig.is_usable():
            return None

        active_migrations = []
        for name, migration in migration_methods.items():
            current_mode = dynconfig.get_effective_config(migration.dynconfig_field)

            if current_mode in [migration.reading_new_way, migration.after_migration]:
                if not quiet:
                    logger.info("Migration {} is already finished".format(name))
                continue

            active_migrations.append(name)

        return active_migrations

    def migrate_without_downtime(
        self, sleep_between_phases=120, dry_run=False, backup_first=False, migration_name=None
    ):
        logger.info("Migrating without downtime")

        migration_methods = self._data_model_traits.get_migrations_without_downtime()
        active_migrations = self.list_migrations_without_downtime()

        if migration_name:
            assert migration_name in migration_methods, "Migration {} is not known".format(
                migration_name
            )
            assert migration_name in active_migrations, "Migration {} is already finished".format(
                migration_name
            )
            active_migrations = [migration_name]

        if not active_migrations:
            logger.info("No active migrations without downtime")
            return

        dynconfig = OrmDynamicConfig(self._yt_client, self._orm_path)
        source_manager = self._create_db_manager(self._orm_path)
        tables_to_backup = source_manager.get_tables_list(skip_watch_logs=True)

        def _do_backup(name, backup_path):
            backup_path = backup_path or _make_backup_path(
                self._orm_path, "tmp", "migration_{}".format(name)
            )
            db_manager = self._create_db_manager(backup_path)
            self._db_manager = db_manager
            self._prepare_db_snapshot(source_manager, tables_to_backup)
            self._db_manager = None
            return db_manager

        if backup_first:
            assert self._backup_path
            _do_backup(None, self._backup_path)

        for name in active_migrations:
            migration = migration_methods[name]
            current_mode = dynconfig.get_effective_config(migration.dynconfig_field)

            if current_mode != migration.writing_new_way and not dry_run:
                dynconfig.set_config(migration.dynconfig_field, migration.writing_new_way)
                time.sleep(sleep_between_phases)
                source_manager.flush_dynamic_store(
                    migration.tables_to_flush, avoid_unneeded_flush=True
                )

            migration_source_manager = source_manager

            if dry_run:
                migration_source_manager = _do_backup(name, None)
                migration_source_manager.mount_all_tables()

            migration.migration_function(self._yt_client, migration_source_manager.orm_path, self._orm_tmp_path)

            if migration.verification_function:
                time.sleep(sleep_between_phases)
                migration_source_manager.flush_dynamic_store(
                    migration.tables_to_flush, avoid_unneeded_flush=True
                )

                if dry_run:
                    backup_path = migration_source_manager.orm_path
                else:
                    backup_path = _do_backup(name, None).orm_path

                migration.verification_function(self._yt_client, backup_path, self._orm_tmp_path)

            if dry_run:
                continue

            dynconfig.set_config(migration.dynconfig_field, migration.reading_new_way)
            time.sleep(sleep_between_phases)
            dynconfig.set_config(migration.dynconfig_field, migration.after_migration)

        logger.info("Finished migrations without downtime")


class ConsistentTimestampGenerator(object):
    class GenerateTimestampError(Exception):
        pass

    class CommonGenerator(object):
        __metaclass__ = ABCMeta

        def __init__(self, tables):
            self.tables = tables

        @abstractmethod
        def generate_timestamp(self):
            raise NotImplementedError()

        @abstractmethod
        def get_timestamp_right_bound_based_on_replication(self):
            raise NotImplementedError()

        def _tighten_timestamp_bounds_by_retained_and_unflushed(self, manager, ts_upper_bound):
            tables = manager.get_tables_attributes(
                self.tables,
                attributes=[
                    "retained_timestamp",
                    "unflushed_timestamp",
                    "merge_rows_on_flush",
                    "enable_dynamic_store_read",
                ],
            )
            max_retained_ts = None
            retained_ts_table = None
            min_unflushed_ts = None
            unflushed_ts_table = None
            for table in tables:
                assert (
                    "retained_timestamp" in table.attributes
                    and "unflushed_timestamp" in table.attributes
                ), "Table {} does not have required attributes".format(table)
                retained_ts = table.attributes["retained_timestamp"]
                unflushed_ts = table.attributes["unflushed_timestamp"]
                check_dynamic_stores = table.attributes.get(
                    "enable_dynamic_store_read"
                ) and not table.attributes.get("merge_rows_on_flush")
                if max_retained_ts is None or max_retained_ts < retained_ts:
                    max_retained_ts = retained_ts
                    retained_ts_table = table
                if not check_dynamic_stores and (
                    min_unflushed_ts is None or min_unflushed_ts > unflushed_ts
                ):
                    min_unflushed_ts = unflushed_ts
                    unflushed_ts_table = table

            logger.info(
                "Max retained timestamp is {} (table: {}), {}".format(
                    max_retained_ts,
                    retained_ts_table,
                    (
                        "dynamic_store_read is enabled for all tables"
                        if min_unflushed_ts is None
                        else "min unflushed timestamp is {} (table: {})".format(
                            min_unflushed_ts, unflushed_ts_table
                        )
                    ),
                )
            )
            ts_lower_bound = max_retained_ts
            if min_unflushed_ts is not None:
                ts_upper_bound = min(min_unflushed_ts - 1, ts_upper_bound)
            if ts_upper_bound < ts_lower_bound:
                raise ConsistentTimestampGenerator.GenerateTimestampError(
                    "Timestamp upper bound {} is less than timestamp lower bound {}".format(
                        ts_upper_bound, ts_lower_bound
                    )
                )

            return (ts_lower_bound, ts_upper_bound)

    class SimpleTableGenerator(CommonGenerator):
        def __init__(self, tables, db_manager):
            super(ConsistentTimestampGenerator.SimpleTableGenerator, self).__init__(tables)
            self._db_manager = db_manager

        def generate_timestamp(self):
            return self._db_manager.yt_client.generate_timestamp()

        def get_timestamp_right_bound_based_on_replication(self):
            return self._db_manager.yt_client.generate_timestamp()

        def tighten_timestamp_bounds_by_retained_and_unflushed(self, ts_upper_bound):
            return self._tighten_timestamp_bounds_by_retained_and_unflushed(
                self._db_manager, ts_upper_bound
            )

    class ReplicatedTableGenerator(CommonGenerator):
        def __init__(self, tables, meta_db_manager, replica_db_manager):
            super(ConsistentTimestampGenerator.ReplicatedTableGenerator, self).__init__(tables)
            self._meta_db_manager = meta_db_manager
            self._replica_db_manager = replica_db_manager
            self._table_to_upstream_replica_id = {
                str(table): table.attributes["upstream_replica_id"]
                for table in self._replica_db_manager.get_tables_attributes(
                    self.tables, attributes=["upstream_replica_id"]
                )
            }

        def generate_timestamp(self):
            return self._meta_db_manager.yt_client.generate_timestamp()

        def _get_table_with_min_last_replication_timestamp(self, table, tablet_infos, tablet_count):
            last_replication_ts = [
                replica["last_replication_timestamp"]
                for tablet in tablet_infos["tablets"]
                for replica in tablet["replica_infos"]
                if replica["replica_id"] == self._table_to_upstream_replica_id[table]
            ]
            assert len(last_replication_ts) == tablet_count

            return (min(last_replication_ts), table)

        def get_timestamp_right_bound_based_on_replication(self):
            table_to_tablet_count = {
                str(table): table.attributes["tablet_count"]
                for table in self._meta_db_manager.get_tables_attributes(
                    self.tables,
                    attributes=["tablet_count"],
                )
            }
            assert len(table_to_tablet_count) == len(self._table_to_upstream_replica_id)

            def get_tablet_infos(item, client):
                table_path, requested_tablets = item[0], list(range(item[1]))
                return client.get_tablet_infos(table_path, requested_tablets)

            tables_tablet_infos = batch_apply(
                get_tablet_infos,
                [
                    (
                        self._meta_db_manager.get_table_path(table),
                        table_to_tablet_count[table],
                    )
                    for table in self._table_to_upstream_replica_id.keys()
                ],
                self._meta_db_manager.get_yt_client(),
            )

            min_last_replication_ts, last_replication_ts_table = min(
                [
                    self._get_table_with_min_last_replication_timestamp(
                        table, tablet_infos, table_to_tablet_count[table]
                    )
                    for table, tablet_infos in zip(
                        self._table_to_upstream_replica_id.keys(), tables_tablet_infos
                    )
                ],
                key=lambda t: t[0],
            )
            logger.info(
                "Min last replication timestamp over all tables is {} (table: {})".format(
                    min_last_replication_ts, last_replication_ts_table
                )
            )
            return min_last_replication_ts

        def tighten_timestamp_bounds_by_retained_and_unflushed(self, ts_upper_bound):
            return self._tighten_timestamp_bounds_by_retained_and_unflushed(
                self._replica_db_manager, ts_upper_bound
            )

    class ChaosTableGenerator(CommonGenerator):
        def __init__(self, tables, meta_db_manager, db_manager):
            super(ConsistentTimestampGenerator.ChaosTableGenerator, self).__init__(tables)
            self._meta_db_manager = meta_db_manager
            self._db_manager = db_manager
            self._table_to_upstream_replica_id = {
                str(table): table.attributes["upstream_replica_id"]
                for table in self._db_manager.get_tables_attributes(
                    self.tables, attributes=["upstream_replica_id"]
                )
            }

        def generate_timestamp(self):
            return self._meta_db_manager.yt_client.generate_timestamp()

        def get_timestamp_right_bound_based_on_replication(self):
            tables = self._meta_db_manager.get_tables_attributes(
                self.tables,
                attributes=["replicas"],
            )
            assert len(tables) == len(self._table_to_upstream_replica_id)
            min_replication_lag, replication_lag_table = None, None
            for table in tables:
                replication_lag_timestamp = table.attributes["replicas"][
                    self._table_to_upstream_replica_id[str(table)]
                ]["replication_lag_timestamp"]
                if min_replication_lag is None or replication_lag_timestamp < min_replication_lag:
                    min_replication_lag = replication_lag_timestamp
                    replication_lag_table = str(table)

            logger.info(
                "Min replication lag over all tables is {} (table: {})".format(
                    min_replication_lag, replication_lag_table
                )
            )
            return min_replication_lag

        def tighten_timestamp_bounds_by_retained_and_unflushed(self, ts_upper_bound):
            return self._tighten_timestamp_bounds_by_retained_and_unflushed(
                self._db_manager, ts_upper_bound
            )

    def __init__(self, internal_generator):
        self._internal_generator = internal_generator

    @classmethod
    def create_simple(cls, tables, **kwargs):
        return cls(ConsistentTimestampGenerator.SimpleTableGenerator(tables, **kwargs))

    @classmethod
    def create_replicated(cls, tables, **kwargs):
        return cls(ConsistentTimestampGenerator.ReplicatedTableGenerator(tables, **kwargs))

    @classmethod
    def create_chaos(cls, tables, **kwargs):
        return cls(ConsistentTimestampGenerator.ChaosTableGenerator(tables, **kwargs))

    def get_retained_and_unflushed_timestamp_bounds(self):
        ts_upper_bound = self._internal_generator.generate_timestamp()
        return self._internal_generator.tighten_timestamp_bounds_by_retained_and_unflushed(
            ts_upper_bound
        )

    def check_generated_timestamp_obsoletion(self, generated_ts):
        (
            ts_lower_bound,
            ts_upper_bound,
        ) = self._internal_generator.tighten_timestamp_bounds_by_retained_and_unflushed(
            generated_ts
        )
        if not (ts_lower_bound <= generated_ts <= ts_upper_bound):
            raise ConsistentTimestampGenerator.GenerateTimestampError(
                "Generated timestamp {} became obsolete".format(generated_ts)
            )

    def try_check_generated_timestamp_obsoletion(self, generated_ts):
        try:
            self.check_generated_timestamp_obsoletion(generated_ts)
            return True
        except ConsistentTimestampGenerator.GenerateTimestampError:
            return False

    def _generate(self):
        ts_upper_bound = self._internal_generator.get_timestamp_right_bound_based_on_replication()
        (
            _,
            ts_upper_bound,
        ) = self._internal_generator.tighten_timestamp_bounds_by_retained_and_unflushed(
            ts_upper_bound
        )
        return ts_upper_bound

    # Use in pair with check_generated_timestamp_obsoletion(),
    # which should be called after you froze tables or got snapshot lock on them.
    def generate(self, retry_count=10, sleep_time=10):
        assert retry_count > 0 and sleep_time >= 0
        logger.info(
            "Generating consistent timestamp for {} tables".format(self._internal_generator.tables)
        )
        ts = run_with_retries(
            self._generate,
            retry_count=retry_count,
            backoff=sleep_time,
            exceptions=(ConsistentTimestampGenerator.GenerateTimestampError,),
        )
        logger.info("Generated consistent timestamp - {}".format(ts))
        return ts
