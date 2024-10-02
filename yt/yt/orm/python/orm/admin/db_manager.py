from yt.orm.library.common import wait, ClientError

import yt.wrapper as yt
from yt.wrapper.common import generate_uuid
from yt.wrapper.retries import run_with_retries
from yt.wrapper.ypath import TablePath

import yt.yson as yson

try:
    from yt.packages.six.moves import map as imap
except ImportError:
    from six.moves import map as imap

from yt.common import update
import yt.logger as logger

from contextlib import contextmanager
from itertools import chain
import time


WATCH_LOG_TABLET_COUNT = 6


def batch_apply(function, data, client):
    """Applies function to each element from data in a batch mode and returns result."""
    batch_client = client.create_batch_client(raise_errors=True)
    results = []
    for item in data:
        results.append(function(item, client=batch_client))
    batch_client.commit_batch()
    return [result.get_result() if result is not None else None for result in results]


def get_yt_cluster_url(yt_client):
    return yt_client.config["proxy"]["url"]


def get_yt_cluster_name(yt_client):
    return yt_client.get("//sys/@cluster_name")


def get_local_yt_options(yt_client):
    return getattr(yt_client, "orm_local_yt_options", None)


def get_yt_cluster_proxy(yt_client):
    if yt_client.exists("//sys/@local_mode_fqdn"):
        proxy_url = get_yt_cluster_url(yt_client)
        assert proxy_url is not None, "Proxy address for local mode is unavailable, try setting 'http_proxy_count' > 0"
        return proxy_url
    return get_yt_cluster_name(yt_client)


def batched_insert_rows(yt_client, table_path, rows, batch_size=10000):
    for start_index in range(0, len(rows), batch_size):
        end_index = min(start_index + batch_size, len(rows))
        yt_client.insert_rows(table_path, rows[start_index:end_index])


def set_db_finalization_timestamp(yt_client, path, timestamp):
    path = yt.ypath_join(path, "db")
    logger.info(
        "Writing finalization timestamp {} to database at cluster {} and path {}".format(
            timestamp,
            get_yt_cluster_url(yt_client),
            path,
        ),
    )
    yt_client.set_attribute(path, "finalization_timestamp", timestamp)


def update_db_finalization_timestamp(yt_client, path):
    set_db_finalization_timestamp(yt_client, path, yt_client.generate_timestamp())


def is_alive_db_object(object):
    removal_time = object["meta.removal_time"]
    assert removal_time is None or (type(removal_time) is yson.YsonUint64 and removal_time > 0)
    return removal_time is None


def is_watch_log(table):
    return table.endswith("_watch_log")


def get_schema_acl(type):
    return [{"action": "allow", "permissions": ["read"], "subjects": ["everyone"]}]


class TablePrototype(object):
    """Defines attributes of DB table: e.g. sharding attributes."""

    COPIED_ATTRIBUTES = (
        "auto_compaction_period",
        "desired_partition_data_size",
        "desired_tablet_count",
        "enable_dynamic_store_read",
        "enable_lookup_hash_table",
        "in_memory_mode",
        "max_partition_data_size",
        "min_partition_data_size",
        "min_partitioning_data_size",
        "monitor_table_statistics",
        "optimize_for",
        "periodic_compaction_mode",
        "primary_medium",
        "tablet_balancer_config",
    )

    ATTRIBUTES = COPIED_ATTRIBUTES + ("pivot_keys",)

    def __init__(self, yt_client, path, attributes_override=None):
        self._table = yt_client.get(path, attributes=self.ATTRIBUTES)
        self._table.attributes = update(self._table.attributes, attributes_override)

    def get_attribute(self, attribute):
        assert attribute in self.ATTRIBUTES
        return self._table.attributes.get(attribute, None)


def apply_attributes_by_prototype(yt_client, path, prototype=None):
    """Restores attributes due to prototype."""

    if prototype is None:
        prototype = TablePrototype(yt_client, path)
    batch_client = yt_client.create_batch_client(raise_errors=True)
    for attribute in TablePrototype.COPIED_ATTRIBUTES:
        value = prototype.get_attribute(attribute)
        if value is None:
            continue
        logger.info(
            "Setting attribute {} for table {} to value {}".format(
                attribute,
                path,
                value,
            ),
        )
        batch_client.set_attribute(path, attribute, value)
    batch_client.commit_batch()

    pivot_keys = prototype.get_attribute("pivot_keys")
    logger.info("Resharding table {} to {} tablets".format(path, len(pivot_keys)))
    yt_client.reshard_table(path, pivot_keys, sync=True)


def convert_static_to_dynamic_db_by_prototype(
    target_manager, prototype_manager, attributes_override=None
):
    """Converts all database tables from static to dynamic by corresponding prototypes."""
    for table in target_manager.get_tables_list(skip_watch_logs=True):
        table_path = target_manager.get_table_path(table)
        # Altering precedes attributes setting, because
        # some attributes require table to be dynamic (e.g. tablet_balancer_config).
        target_manager.yt_client.alter_table(table_path, dynamic=True)

        apply_attributes_by_prototype(
            target_manager.yt_client,
            table_path,
            TablePrototype(
                prototype_manager.yt_client,
                prototype_manager.get_table_path(table),
                attributes_override,
            ),
        )
    logger.info("Finished database conversion from static to dynamic")


def generate_db_time():
    return int(time.time()) * 1000000


# Common functionality with ReplicatedDbManager.
class DbManagerBase(object):
    DEFAULT_MOUNT_WAIT_TIME = 200

    def __init__(self, data_model_traits, version, mount_wait_time=None):
        self._data_model_traits = data_model_traits
        self._version = version
        self._tables_to_mount = set()
        self._mount_wait_time = mount_wait_time or self.DEFAULT_MOUNT_WAIT_TIME
        self._versioned_type_to_tables_map = {}

    def get_data_model_traits(self):
        return self._data_model_traits

    def get_snake_case_orm_name(self):
        return self._data_model_traits.get_snake_case_name()

    def get_used_media(self):
        return self._data_model_traits.get_used_media()

    def _get_db_path(self, orm_path):
        return yt.ypath_join(orm_path, "db")

    def _get_table_path(self, orm_path, table):
        return yt.ypath_join(self._get_db_path(orm_path), table)

    def _make_object_rows(self, rows):
        return [
            update(
                {
                    "meta.creation_time": generate_db_time(),
                    "meta.inherit_acl": True,
                    "meta.acl": [],
                    "meta.etc": {"uuid": generate_uuid()},
                    "labels": {},
                },
                row,
            )
            for row in rows
        ]

    def _tables_not_in_state(self, yt_client, path, tables, target_state, log_match=False, log_mismatch=False):
        states = self._batch_apply(
            lambda table, client: client.get(yt.ypath_join(table, "@tablet_state")), path, tables, yt_client
        )

        matches = []
        mismatches = []
        filtered_tables = []
        for table, state in zip(tables, states):
            if state == target_state:
                matches.append(str(table))
            else:
                mismatches.append([str(table), state])
                filtered_tables.append(table)

        if log_match:
            logger.info("Some tables are already {}: {}".format(target_state, matches))
        if log_mismatch:
            logger.info("Some tables are still not {}: {}".format(target_state, mismatches))

        return filtered_tables

    def _is_mounted_and_preloaded(self, yt_client, path, tables, freeze):
        expected_tablet_state = "frozen" if freeze else "mounted"
        expected_preload_state = "complete"
        attributes = list(
            map(
                lambda result: result.attributes,
                self._batch_apply(
                    lambda path, client: client.get(
                        path, attributes=["tablet_state", "preload_state"]
                    ),
                    path,
                    tables,
                    yt_client,
                ),
            )
        )

        def check_mismatch(name, table, actual, expected):
            match = actual == expected
            if not match:
                logger.info(
                    'Expected {} of {} to be equal to "{}", but got "{}"'.format(
                        name,
                        table,
                        expected,
                        actual,
                    )
                )
            return match

        is_mounted_and_preloaded = True
        for table, table_attributes in zip(tables, attributes):
            is_mounted_and_preloaded &= (
                check_mismatch("tablet state", table, table_attributes["tablet_state"], expected_tablet_state) and
                check_mismatch("preload state", table, table_attributes["preload_state"], expected_preload_state)
            )
        return is_mounted_and_preloaded

    def _wait_for_mount_cache_refresh(self, yt_client):
        # Tablet states may be still cached for some time. Especially painful in tests.
        local_yt_options = get_local_yt_options(yt_client)
        if local_yt_options:
            refresh_time = (
                local_yt_options.get("driver_config", {}).get("table_mount_cache", {}).get("refresh_time", None)
            )
            if refresh_time:
                time.sleep(refresh_time * 1.1 / 1000.)

    def _mount_tables(self, yt_client, path, tables, freeze):
        tables = set(self._get_table_path(path, table) for table in tables)
        logger.info(
            "Mounting tables at {} cluster and path {}".format(
                get_yt_cluster_url(yt_client),
                path,
            ),
        )
        self._batch_apply(
            lambda path, client: client.mount_table(path, freeze=freeze),
            path,
            tables,
            yt_client,
        )
        logger.info("Mounting requests were sent")

        def check():
            logger.info("Checking if tables are mounted and preloaded")
            return self._is_mounted_and_preloaded(yt_client, path, tables, freeze=freeze)

        wait(check, iter=int(self._mount_wait_time + 0.5), sleep_backoff=1)
        self._wait_for_mount_cache_refresh(yt_client)
        logger.info("Tables mounted")

    def _check_version_specified(self):
        if self._version is None:
            raise yt.YtError("DB version must be specified")

    def _make_table_attributes(self):
        self._check_version_specified()
        return {
            "dynamic": True,
            "optimize_for": "scan",
            "in_memory_mode": "uncompressed",
            "account": self._data_model_traits.get_account(),
            "version": self._version,
            "chunk_writer": {
                "upload_replication_factor": 3,
                "min_upload_replication_factor": 3,
            },
            "monitor_table_statistics": True,
            "enable_dynamic_store_read": True,
        }

    def _get_tables_list(
        self,
        yt_client,
        path,
        attributes=None,
        absolute=False,
        skip_watch_logs=False,
        exclude_tables=None,
        include_tables=None,
    ):
        assert exclude_tables is None or include_tables is None

        is_type_in_requested_attributes = attributes is not None and "type" in attributes
        if attributes is None:
            attributes = ["type"]
        else:
            attributes.append("type")

        # TODO(dgolear): Drop retries after YT-17826.
        tables = run_with_retries(
            lambda: list(
                yt_client.list(self._get_db_path(path), absolute=absolute, attributes=attributes)
            ),
            additional_retriable_error_codes=720,  # OptimisticLockFailure.
        )

        tables = [table for table in tables if table.attributes["type"] != "replication_log_table"]
        if not is_type_in_requested_attributes:
            for table in tables:
                table.attributes.pop("type")
                if len(table.attributes) == 0:
                    del table.attributes

        if skip_watch_logs:
            tables = [t for t in tables if not is_watch_log(t)]
        if exclude_tables:
            tables = [t for t in tables if t.rsplit("/", 1)[-1] not in exclude_tables]
        elif include_tables:
            tables = [t for t in tables if t.rsplit("/", 1)[-1] in include_tables]
        return tables

    def _update_versions(self):
        self._check_version_specified()
        for yt_client, path in self.iterate_replicas():
            batch_apply(
                lambda path, client: client.set_attribute(path, "version", self._version),
                self._get_tables_list(yt_client, path, absolute=True),
                yt_client,
            )

    def _read_version(self, yt_client, path):
        if not yt_client.exists(path):
            raise ClientError("{} does not exist".format(path))

        table_versions = {}

        tables = self._get_tables_list(yt_client, path, absolute=True)
        table_versions = batch_apply(
            lambda table, client: client.get_attribute(table, "version", default=None),
            tables,
            yt_client,
        )

        if not table_versions:
            raise ClientError(
                "There are no {} tables in {}".format(
                    self._data_model_traits.get_human_readable_name(), path
                )
            )

        db_version = None
        for table_version in table_versions:
            if db_version is None:
                db_version = table_version
            elif db_version != table_version:
                raise ClientError(
                    "Inconsistent database version, all tables versions: {}".format(
                        zip(tables, table_versions)
                    )
                )

        self._version = db_version
        return db_version

    def _batch_apply(self, func, path, tables, yt_client, retry_count=0):
        tables = [self._get_table_path(path, table) for table in tables]
        if retry_count:
            run_with_retries(
                lambda: batch_apply(func, tables, yt_client),
                retry_count=retry_count,
                backoff=2.0,
                exceptions=(Exception,),
            )
        else:
            return batch_apply(func, tables, yt_client)

    def set_version(self, version):
        self._version = version

    def get_version(self):
        return self._version

    @staticmethod
    def _set_default_group(schema):
        for column in schema:
            column.setdefault("group", "default")
        return schema

    def make_table_attributes(self, table):
        if "schema" not in table.attributes:
            raise yt.YtError("Expected schema in table attributes (Table: {})".format(table))
        self._set_default_group(table.attributes["schema"])

        table_name = str(table)
        attributes = self._make_table_attributes()
        if not (
            self._data_model_traits.get_initial_db_version()
            <= self._version
            <= self._data_model_traits.get_actual_db_version()
        ):
            logger.warning(
                "Cannot get {} table attributes from db_schema because of requested db version {} is out of known range [{}, {}]".format(
                    table_name,
                    self._version,
                    self._data_model_traits.get_initial_db_version(),
                    self._data_model_traits.get_actual_db_version(),
                )
            )
        else:
            if table_name not in self._data_model_traits.get_table_schema_verification_exclude_list(self._version):
                attributes = update(attributes, self._data_model_traits.get_table_attributes(self._version, table_name))
        self._data_model_traits.patch_table_attributes(table_name, attributes, self._version)
        table.attributes = update(attributes, table.attributes)

    def create_tables(self, tables, recursive=False):
        for table in tables:
            self.make_table_attributes(table)
            self._tables_to_mount.add(str(table))
        self._create_tables_impl(tables, recursive)

    def initialize_tables(self):
        # If db_manager is equal to ReplicatedDbmanager, then it will create replicated watch_logs tables,
        # which does not coincide with production environment.
        tables = []
        for table_schema in self._data_model_traits.get_table_schemas(self._version).items():
            table = yson.YsonUnicode(table_schema[0])
            table.attributes = {"schema": table_schema[1]["schema"]}
            tables.append(table)
        self.create_tables(tables)

    def _normalize_schema_for_cmp(self, schema):
        for column in schema:
            if column.get("group", None) is None:
                column["group"] = "default"
        keys = [c for c in schema if "sort_order" in c]
        others = sorted([c for c in schema if "sort_order" not in c], key=lambda c: c["name"])
        return keys + others

    def verify_table_schema(self, table, schema=None):
        exclude_list = self._data_model_traits.get_table_schema_verification_exclude_list(self._version)
        if table not in exclude_list:
            if schema is None:
                schema = self.get_table_schema(table)
                for column in schema:
                    if "type_v3" in column:
                        del column["type_v3"]
                    if "required" in column and not column["required"]:
                        del column["required"]

            logger.info("Verifying schema v{} of {}".format(self._version, table))

            manual_schema = self._normalize_schema_for_cmp(schema)
            generated_schema = self._normalize_schema_for_cmp(
                self._data_model_traits.get_table_schema(self._version, table)
            )
            assert manual_schema == generated_schema, "{} ->\n{}\n!=\n{}".format(
                table, manual_schema, generated_schema
            )

    def verify_all_tables_schema(self):
        for table in self.get_tables_list():
            self.verify_table_schema(table.rsplit("/", 1)[-1])

    def create_table(self, table, schema=None, attributes=None, recursive=False):
        if schema is None:
            assert table not in self._data_model_traits.get_table_schema_verification_exclude_list(self._version)
            schema = self._data_model_traits.get_table_schema(self._version, table)
        table = yson.YsonUnicode(table)
        table.attributes = attributes if attributes is not None else {}
        table.attributes["schema"] = schema
        self.create_tables([table], recursive=recursive)

    def create_watch_log(self, table):
        assert table.endswith("_watch_log"), "Invalid watch log table name {}".format(table)
        self.create_table(table)

    def create_object_env(self, object_type):
        if self._version not in self._versioned_type_to_tables_map:
            yt_schema = self._data_model_traits.get_yt_schema(self._version)

            type_to_tables_map = {}
            for type_name in yt_schema["object_types"]:
                type_to_tables_map[type_name] = []

            for table, flags in yt_schema["all_tables"].items():
                if "orm_flags" in flags:
                    table = yson.YsonUnicode(table)
                    table.attributes = {"schema": flags["schema"]}
                    type_to_tables_map[flags["orm_flags"]["type"]].append(table)

            self._versioned_type_to_tables_map[self._version] = type_to_tables_map

        tables = self._versioned_type_to_tables_map[self._version][object_type]
        assert len(tables), "Empty table list for type {} v{}".format(object_type, self._version)
        self.create_tables(tables)
        self.mount_tables(["schemas"])

        self.create_schemas([object_type])

    def create_schemas(self, types=None):
        if types is None:
            types = self._data_model_traits.get_yt_schema(self._version)["object_types"]

        schemas_rows = []
        for type in types:
            if type == "schema":
                continue
            schemas_rows.append(
                {
                    "meta.id": type,
                    "meta.acl": get_schema_acl(type),
                    "meta.inherit_acl": False,
                }
            )

        if schemas_rows:
            self.insert_rows("schemas", schemas_rows)

    def mount_unmounted_tables(self, freeze=False):
        self.mount_tables(self._tables_to_mount, freeze=freeze)
        self._tables_to_mount = set()

    def finalize(self):
        logger.info("Finalizing database to version {}".format(self._version))
        self.mount_unmounted_tables()
        self._update_finalization_timestamp()
        self._update_versions()
        logger.info("Migration finalized")


class DbManager(DbManagerBase):
    def __init__(
        self,
        yt_client,
        orm_path,
        data_model_traits,
        version=None,
        yt_replicas=list(),
        mount_wait_time=None,
        tmp_path=None,
    ):
        super(DbManager, self).__init__(data_model_traits, version, mount_wait_time)
        self.yt_client = yt_client
        self.orm_path = orm_path
        self.tmp_path = tmp_path
        self.yt_replicas = list(yt_replicas)

    def _get_tablet_state(self, path):
        return self.yt_client.get(path + "/@tablet_state")

    def _verify_static_table_interface_applicability(self, path):
        tablet_state = self._get_tablet_state(path)
        assert tablet_state in ("unmounted", "frozen"), (
            "Application of static table interface to the dynamic table in {} "
            "state can cause data loss due to lack of dynamic store processing support".format(
                tablet_state
            )
        )

    def iterate_replicas(self, include_primary=True):
        if include_primary:
            yield (self.yt_client, self.orm_path)
        for yt_replica in self.yt_replicas:
            yield (yt_replica, self.orm_path)

    def get_yt_client(self):
        return self.yt_client

    def get_orm_path(self):
        return self.orm_path

    def get_db_path(self):
        return self._get_db_path(self.orm_path)

    def get_table_path(self, table):
        return self._get_table_path(self.orm_path, table)

    # Use get_backup_list [+ get_tables_attributes] when working with backups.
    def get_tables_list(
        self,
        attributes=None,
        absolute=False,
        skip_watch_logs=False,
        exclude_tables=None,
        include_tables=None,
    ):
        return self._get_tables_list(
            self.yt_client,
            self.orm_path,
            attributes,
            absolute,
            skip_watch_logs,
            exclude_tables,
            include_tables,
        )

    def read_version(self):
        return self._read_version(self.yt_client, self.orm_path)

    # NB! Do not try to add key columns or remove columns using this method.
    def alter_table(self, table, schema):
        self.alter_tables({table: schema})

    def alter_tables(self, tables_to_schema):
        self.unmount_tables(tables_to_schema.keys())
        self.batch_apply(
            lambda path, client: client.alter_table(
                path, schema=tables_to_schema[yt.ypath_split(path)[1]]
            ),
            tables_to_schema,
        )

    def rename_table(self, table, new_name, skip_if_not_exists=False):
        table = yson.YsonUnicode(table)
        table.attributes = {"new_name": new_name}
        self.rename_tables([table], skip_if_not_exists)

    def rename_tables(self, tables, skip_nonexistent_tables=False):
        assert all(map(lambda table: "new_name" in table.attributes, tables))

        if skip_nonexistent_tables:
            tables = [table for table, exists in zip(tables, self.tables_exist(tables)) if exists]
        table_to_new_path = {
            str(table): self._get_table_path(self.orm_path, table.attributes["new_name"])
            for table in tables
        }

        self.unmount_tables(tables)
        self.batch_apply(
            lambda path, client: client.move(path, table_to_new_path[yt.ypath_split(path)[1]]),
            tables,
        )

        for table in tables:
            if str(table) in self._tables_to_mount:
                self._tables_to_mount.remove(str(table))
            self._tables_to_mount.add(table.attributes["new_name"])

    def batch_apply(self, func, tables, retry_count=0):
        return self._batch_apply(func, self.orm_path, tables, self.yt_client, retry_count)

    def lookup_rows(self, table, keys):
        return self.yt_client.lookup_rows(self.get_table_path(table), keys)

    def insert_rows(self, table, rows, object_table=True):
        if object_table:
            rows = self._make_object_rows(rows)
        self.yt_client.insert_rows(self.get_table_path(table), rows)

    def batched_insert_rows(self, table, rows, object_table=True):
        if object_table:
            rows = self._make_object_rows(rows)
        batched_insert_rows(self.yt_client, self.get_table_path(table), rows)

    def _create_tables_impl(self, tables, recursive):
        def _create(table, client):
            client.create(
                "table",
                self.get_table_path(table),
                attributes=table.attributes,
                recursive=recursive,
            )

        batch_apply(_create, tables, self.yt_client)

    def select_rows(self, columns, table):
        return self.yt_client.select_rows(
            "{} from [{}]".format(columns, self.get_table_path(table))
        )

    def read_table(self, table):
        path = self.get_table_path(table)
        self._verify_static_table_interface_applicability(path)
        return self.yt_client.read_table(path)

    def recreate_watch_logs(self):
        for table in self.get_tables_list():
            if is_watch_log(table):
                self.remove_table(table)
                self.create_watch_log(table)

    # TODO: Add support to trim head of the table.
    def trim_table(self, table):
        yt_client = self.yt_client
        path = self.get_table_path(table)
        infos = yt_client.get_tablet_infos(
            path, list(range(yt_client.get(path + "/@tablet_count")))
        )

        for tablet_index, info in enumerate(infos["tablets"]):
            yt_client.trim_rows(path, tablet_index, info["total_row_count"])

    def remove_table(self, table):
        if not self.table_exists(table):
            return

        yt_client = self.yt_client

        path = self.get_table_path(table)
        yt_client.unmount_table(path, sync=True)
        yt_client.remove(path)
        if table in self._tables_to_mount:
            self._tables_to_mount.remove(table)

    def get_table_schema(self, table=None, node_id=None):
        if table is not None:
            assert node_id is None
            path = yt.ypath_join(self.get_table_path(table), "@schema")
        else:
            assert node_id is not None
            path = yt.ypath_join(node_id, "@schema")
        return self.yt_client.get(path)

    def reshard_table(self, table, tablet_count):
        yt_client = self.yt_client

        path = yt.ypath_join(self.get_db_path(), table)
        self.unmount_tables([table])

        logger.info("Resharding table {} to {} tablets".format(path, tablet_count))
        yt_client.reshard_table(path, tablet_count=tablet_count)

        self.mount_tables([path])

    @contextmanager
    def _temp_output_table(self, output_table, output_schema, prototype_table, convert_to_dynamic):
        temp_table_schema = output_schema
        if temp_table_schema is None:
            temp_table_schema = self.get_table_schema(output_table)

        unique_keys = any(map(lambda column: "sort_order" in column, temp_table_schema))
        assert (
            not convert_to_dynamic or unique_keys
        ), "Cannot convert table to dynamic after map operation: destination table has no key columns"
        temp_table_attributes = self._make_table_attributes()
        temp_table_attributes = update(
            temp_table_attributes,
            {
                "dynamic": False,
                "schema": yson.to_yson_type(
                    temp_table_schema, attributes={"unique_keys": unique_keys}
                ),
            },
        )

        yt_client = self.yt_client
        with yt_client.TempTable(attributes=temp_table_attributes, path=self.tmp_path) as temp_table_path:
            had_exception = False
            try:
                yield temp_table_path
            except BaseException:
                had_exception = True
                raise
            finally:
                if not had_exception:
                    if convert_to_dynamic:
                        yt_client.alter_table(temp_table_path, dynamic=True)
                        apply_attributes_by_prototype(
                            yt_client,
                            temp_table_path,
                            TablePrototype(yt_client, prototype_table),
                        )
                    yt_client.move(temp_table_path, self.get_table_path(output_table), force=True)

    def run_map(
        self,
        table,
        mapper,
        output_schema=None,
        output_table=None,
        convert_to_dynamic=True,
        ordered=True,
        memory_limit=None,
    ):
        yt_client = self.yt_client

        path = self.get_table_path(table)
        if output_table is None:
            output_table = table

        output_path = self.get_table_path(output_table)

        self.unmount_tables([table] if output_table == table else [table, output_table])

        # There is no reason to verify after unmounting. Just sanity check.
        self._verify_static_table_interface_applicability(path)

        if yt_client.get(path + "/@chunk_count") == 0 and output_schema is None:
            logger.info("Skipping map for unmounted empty table {}".format(path))
            if output_path != path:
                yt_client.copy(path, output_path, force=True)
            return

        with self._temp_output_table(
            output_table, output_schema, path, convert_to_dynamic
        ) as temp_table_path:
            if yt_client.get(path + "/@data_weight") > 2**20:
                logger.info("Running remote map operation for table {}".format(path))
                spec = (
                    yt.spec_builders.MapSpecBuilder()
                    .begin_mapper()
                    .command(mapper)
                    .memory_limit(memory_limit)
                    .end_mapper()
                    .input_table_paths([path])
                    .output_table_paths([temp_table_path])
                    .ordered(ordered)
                    .begin_job_io()
                    .table_writer({"block_size": 256 * 1024, "desired_chunk_size": 100 * 1024**2})
                    .end_job_io()
                )
                yt_client.run_operation(spec)
            else:
                logger.info("Running local map operation for table {}".format(path))
                input_rows = yt_client.read_table(path)
                output_rows = chain.from_iterable(imap(mapper, input_rows))
                yt_client.write_table(temp_table_path, output_rows)

    def rename_columns(self, table, rename_columns_mapping, convert_to_dynamic=True):
        path = self.get_table_path(table)
        self.unmount_tables([table])

        output_schema = self.get_table_schema(table)
        for column in output_schema:
            if column["name"] in rename_columns_mapping:
                column["name"] = rename_columns_mapping[column["name"]]

        with self._temp_output_table(
            table, output_schema, path, convert_to_dynamic
        ) as temp_table_path:
            logger.info("Running merge operation for table {}".format(path))
            self.yt_client.run_merge(
                TablePath(path, rename_columns=rename_columns_mapping), temp_table_path
            )

    def add_columns(self, table, new_columns_schema):
        schema = self.get_table_schema(table)

        existing_columns = {}
        for column in schema:
            existing_columns[column["name"]] = column
        for new_column in new_columns_schema:
            assert new_column["name"] not in existing_columns

        self._set_default_group(new_columns_schema)
        schema.extend(new_columns_schema)
        self.alter_table(table, schema)

    def unmount_all_tables(self):
        tables = self.get_tables_list()
        self.unmount_tables(tables)

    def tables_not_in_state(self, tables, target_state, log_match=False, log_mismatch=False):
        return self._tables_not_in_state(self.yt_client, self.orm_path, tables, target_state, log_match, log_mismatch)

    def wait_for_tables_state(self, tables, target_state):
        def check():
            logger.info("Checking if tables are {}".format(target_state))
            return not self.tables_not_in_state(tables, target_state, log_mismatch=True)

        wait(check, iter=200, sleep_backoff=2)
        self._wait_for_mount_cache_refresh(self.yt_client)

        logger.info("Tables {}".format(target_state))

    def unmount_tables(self, tables):
        if tables is None:
            tables = self.get_tables_list()

        for table in tables:
            self._tables_to_mount.add(str(table))

        logger.info("Unmounting tables")

        tables = self.tables_not_in_state(tables, "unmounted", log_match=True)
        if not tables:
            return

        self.batch_apply(lambda path, client: client.unmount_table(path), tables)
        logger.info("Unmounting requests were sent")
        self.wait_for_tables_state(tables, "unmounted")

    def freeze_tables(self, tables, verify_previous_state=True):
        if verify_previous_state:
            assert not self.tables_not_in_state(tables, "mounted", log_mismatch=True), (
                "Tables passed to freeze_tables must be mounted, call mount_all_tables beforehand"
            )

        self.batch_apply(lambda path, client: client.freeze_table(path), tables, retry_count=10)
        logger.info("Freezing requests were sent")
        self.wait_for_tables_state(tables, "frozen")

    def unfreeze_tables(self, tables, verify_previous_state=True):
        if verify_previous_state:
            assert not self.tables_not_in_state(tables, "frozen", log_mismatch=True), (
                "Tables passed to unfreeze_tables must be frozen"
            )

        self.batch_apply(lambda path, client: client.unfreeze_table(path), tables, retry_count=10)
        logger.info("Unfreezing requests were sent")
        self.wait_for_tables_state(tables, "mounted")

    # Flush dynamic store (in-memory cache of dynamic tables).
    # Note: freezing too many tables at once may take longer than we could expect.
    def flush_dynamic_store(self, table_list, avoid_unneeded_flush=False):
        if avoid_unneeded_flush:
            dynamic_store_read_enabled = self.batch_apply(
                lambda table, client: client.get(yt.ypath_join(table, "@enable_dynamic_store_read")),
                table_list
            )
            if all(dynamic_store_read_enabled):
                logger.info(
                    "Not flushing tables, bacause 'enable_dynamic_store_read' attribute is set: {}".format(table_list)
                )
                return

        logger.info("Flushing tables: {}".format(table_list))
        self.freeze_tables(table_list)
        self.unfreeze_tables(table_list)

    def mount_all_tables(self, freeze=False):
        for table in self.get_tables_list():
            self._tables_to_mount.add(table)
        self.mount_unmounted_tables(freeze=freeze)

    def mount_tables(self, tables, freeze=False):
        self._mount_tables(
            self.yt_client,
            self.orm_path,
            tables,
            freeze,
        )

    def _update_finalization_timestamp(self):
        update_db_finalization_timestamp(self.yt_client, self.orm_path)

    def table_exists(self, table):
        return self.yt_client.exists(self.get_table_path(table))

    def tables_exist(self, tables):
        return self.batch_apply(
            lambda path, client: client.exists(path),
            tables,
        )

    def get_tablet_infos(self, table, tablet_count):
        return self.yt_client.get_tablet_infos(
            self.get_table_path(table), list(range(tablet_count))
        )

    def get_db_attributes(self, attributes):
        db_path = self.get_db_path()
        get_results = batch_apply(
            lambda attribute, client: client.get_attribute(db_path, attribute),
            attributes,
            self.yt_client,
        )
        return {attribute: result for attribute, result in zip(attributes, get_results)}

    def setup_nightly_compression(
        self,
        enabled,
        pool=None,
        compression_codec=None,
        erasure_codec=None,
        min_table_size=None,
        min_table_age=None,
        desired_chunk_size=512 * 1024**2,
        owners=None,
        force_recompress_to_specified_codecs=None,
        optimize_for=None,
        dynamic_table_select_timestamp=None,
    ):
        nightly_compression_settings = {
            "enabled": enabled,
            "pool": pool,
            "compression_codec": compression_codec,
            "erasure_codec": erasure_codec,
            "min_table_size": min_table_size,
            "min_table_age": min_table_age,
            "owners": owners,
            "desired_chunk_size": desired_chunk_size,
            "force_recompress_to_specified_codecs": force_recompress_to_specified_codecs,
            "dynamic_table_select_timestamp": dynamic_table_select_timestamp,
            "optimize_for": optimize_for,
        }
        self.yt_client.set(
            "{}/@nightly_compression_settings".format(self.get_db_path()),
            {
                key: value
                for key, value in nightly_compression_settings.items()
                if value is not None
            },
        )

    def get_tables_attributes(self, tables, attributes, absolute=False):
        db_tables = self.get_tables_list(attributes=attributes, absolute=absolute)
        return [t for t in db_tables if t.rsplit("/", 1)[-1] in tables]

    def get_tables_for_backup(self, exclude_tables=None, include_tables=None):
        tables_for_backup = self._data_model_traits.get_tables_for_backup()
        assert bool(tables_for_backup) + (exclude_tables is not None) + (include_tables is not None) <= 1

        tables = self.get_tables_list(
            skip_watch_logs=True, exclude_tables=exclude_tables, include_tables=include_tables
        )
        if tables_for_backup is not None:
            tables = [t for t in tables if t.rsplit("/", 1)[-1] in tables_for_backup]
        return tables
