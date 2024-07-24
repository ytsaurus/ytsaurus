import yt.yson as yson
from yt.wrapper import TablePath, ypath_join
from yt.common import YtError

import copy
import logging
import subprocess
import time


DEFAULT_SHARD_COUNT = 100
SYS_ACCOUNT_NAME = "sys"


def _wait_for_predicate(predicate, message, timeout=60, pause=1):
    start = time.time()
    while not predicate():
        if time.time() - start > timeout:
            error = "Timeout while waiting for \"%s\"" % message
            logging.info(error)
            raise YtError(error)
        logging.info("Waiting for \"%s\"" % message)
        time.sleep(pause)


def _unmount_table(client, path):
    logging.info("Unmounting table %s", path)
    client.unmount_table(path, sync=True)


def _mount_table(client, path):
    logging.info("Mounting table %s", path)
    client.mount_table(path, sync=True)


def _swap_table(client, target, source, version):
    backup_path = target + ".bak.{0}".format(version)
    has_target = False
    if client.exists(target):
        has_target = True
        _unmount_table(client, target)

    _unmount_table(client, source)

    logging.info("Swapping tables %s <-> %s", source, target)
    if has_target:
        client.move(target, backup_path, force=True)
    client.move(source, target)

    _mount_table(client, target)


def _make_dynamic_table_attributes(schema, key_columns, optimize_for):
    attributes = {
        "dynamic": True,
        "optimize_for": optimize_for,
        "schema": schema,
    }
    for column in schema:
        assert (column.get("sort_order") == "ascending") == (column["name"] in key_columns)
    return attributes


class TableInfo(object):
    """
    Contains all information about the table structure

    :param key_columns:
    :param value_columns:
        all non-key table columns
    :param in_memory:
        whether in-memory mode used for the table, if true, "compressed" mode is always used
    :param get_pivot_keys:
        function used to get boundary keys by the number of shards
    :param default_lock:
        default lock name (only for non-key columns), it is used for a column if its value is not None and is not redefined in this column
    :param optimize_for:
        table storage format ("scan" or "lookup")
    :param attributes:
        other table attributes
    """

    def __init__(self, key_columns, value_columns, in_memory=False, get_pivot_keys=None,
                 default_lock=None, optimize_for="scan", attributes={}):
        def make_column(name, type_name, attributes={}, key=False):
            result = {
                "name": name,
                "type": type_name,
            }
            lock = attributes.get("lock", default_lock)
            if not key and lock is not None:
                result["lock"] = lock
            if "max_inline_hunk_size" in attributes:
                result["max_inline_hunk_size"] = attributes["max_inline_hunk_size"]
            return result

        def make_key_column(name, type_name, expression=None):
            result = make_column(name, type_name, key=True)
            result["sort_order"] = "ascending"
            if expression:
                result["expression"] = expression
            return result

        self.schema = [make_key_column(*columns) for columns in key_columns]
        self.key_columns = [column["name"] for column in self.schema]
        self.schema += [make_column(*column) for column in value_columns]
        self.user_columns = [column["name"] for column in self.schema if "expression" not in column]
        self.get_pivot_keys = get_pivot_keys
        self.in_memory = in_memory
        self.optimize_for = optimize_for
        self.attributes = attributes

    def create_table(self, client, path, sorted=True):
        if sorted:
            key_columns = self.key_columns
        else:
            key_columns = []

        schema = copy.deepcopy(self.schema)
        if not sorted:
            for column in schema:
                if "sort_order" in column:
                    del column["sort_order"]

        attributes = _make_dynamic_table_attributes(schema, key_columns, self.optimize_for)
        attributes.update(self.attributes)
        attributes["dynamic"] = False

        logging.info("Creating table %s with attributes %s", path, attributes)
        client.create("table", path, recursive=True, attributes=attributes)

    def create_dynamic_table(self, client, path):
        attributes = _make_dynamic_table_attributes(self.schema, self.key_columns, self.optimize_for)
        attributes.update(self.attributes)

        if attributes.get("account") is None :
            attributes["account"] = SYS_ACCOUNT_NAME

        logging.info("Creating dynamic table %s with attributes %s", path, attributes)
        client.create("table", path, recursive=True, attributes=attributes)

    def to_dynamic_table(self, client, path):
        attributes = _make_dynamic_table_attributes(self.schema, self.key_columns, self.optimize_for)

        # add unique_keys to schema
        attributes["schema"] = yson.to_yson_type(attributes["schema"], attributes={"unique_keys": True})

        logging.info("Sorting table %s with attributes %s", path, attributes)
        primary_medium = client.get(path + "/@primary_medium")
        account = client.get(path + "/@account")
        client.run_sort(path, TablePath(path, attributes=attributes), sort_by=self.key_columns, spec={
            "merge_job_io": {"table_writer": {"block_size": 256 * 2**10, "desired_chunk_size": 100 * 2**20}},
            "force_transform": True,
            "intermediate_data_medium": primary_medium,
            "intermediate_data_account": account,
            "use_new_partitions_heuristic": True,
            "max_speculative_job_count_per_task": 20,
        })
        logging.info("Converting table to dynamic %s", path)
        client.alter_table(path, dynamic=True)
        for attr, value in self.attributes.items():
            client.set("{0}/@{1}".format(path, attr), value)

    def alter_table(self, client, path, shard_count, mount=True):
        logging.info("Altering table %s", path)
        _unmount_table(client, path)
        attributes = _make_dynamic_table_attributes(self.schema, self.key_columns, self.optimize_for)

        logging.info("Alter table %s with attributes %s", path, attributes)
        client.alter_table(path, schema=attributes["schema"])
        for attr, value in self.attributes.items():
            client.set("{0}/@{1}".format(path, attr), value)

        if self.get_pivot_keys:
            client.reshard_table(path, self.get_pivot_keys(shard_count), sync=True)

        if mount:
            _mount_table(client, path)

    def get_default_mapper(self):
        column_names = self.user_columns

        def default_mapper(row):
            yield dict([(key, row.get(key)) for key in column_names])

        return default_mapper


class Conversion(object):
    """
    Contains all the information about the table conversion

    :param table:
        result table name
    :param table_info:
        result table TableInfo
    :param mapper:
        mapper for map operations
    :param source:
        name of the table before the conversion, for cases when it differs from the "table" parameter
    :param use_default_mapper:
        whether to use the default mapper for map operations if the "mapper" parameter is not specified
    :param filter_callback:
        a callback called with "client" and "table_path" arguments used to ignore some conversions
        for re-using migration in different environments
    """

    def __init__(self, table, table_info=None, mapper=None, source=None, use_default_mapper=False, filter_callback=None):
        self.table = table
        self.table_info = table_info
        self.mapper = mapper
        self.source = source
        self.use_default_mapper = use_default_mapper
        self.filter_callback = filter_callback

    def __call__(self, client, table_info, target_table, source_table, tables_path, shard_count):
        if self.table_info:
            table_info = self.table_info

        source_table = self.source or source_table
        if source_table:
            source_table = ypath_join(tables_path, source_table)
            old_key_columns = client.get(source_table + "/@key_columns")
            need_sort = old_key_columns != table_info.key_columns
        else:
            need_sort = False

        if not self.use_default_mapper and not self.mapper and not self.source and source_table and not need_sort:
            table_info.alter_table(client, source_table, shard_count, mount=False)
            return True  # in place transformation

        if source_table:
            if client.exists(source_table):
                primary_medium = client.get(source_table + "/@primary_medium")
                # If need_sort == True, we create target table non-sorted to avoid
                # sort order violation error during map.
                table_info.create_table(client, target_table, sorted=not need_sort)
                client.set(target_table + "/@account", client.get(source_table + "/@account"))
                client.set(target_table + "/@tablet_cell_bundle", client.get(source_table + "/@tablet_cell_bundle"))
                client.set(target_table + "/@primary_medium", primary_medium)
                for key, value in client.get(source_table + "/@user_attributes").items():
                    client.set(target_table + "/@" + key, value)

                assert self.use_default_mapper or self.mapper is not None
                mapper = self.mapper if self.mapper else table_info.get_default_mapper()
                _unmount_table(client, source_table)

                logging.info("Run mapper '%s': %s -> %s", mapper.__name__, source_table, target_table)
                # If need_sort == False, we already created target table sorted and we need to run ordered map
                # to avoid sort order violation error during map.
                client.run_map(mapper, source_table, target_table, spec={"data_size_per_job": 2 * 2**30}, ordered=not need_sort)
                table_info.to_dynamic_table(client, target_table)
                client.set(target_table + "/@forced_compaction_revision", 1)
        else:
            logging.info("Creating dynamic table %s", target_table)
            table_info.create_dynamic_table(client, target_table)

        if table_info.in_memory:
            client.set(target_table + "/@in_memory_mode", "compressed")
        table_info.alter_table(client, target_table, shard_count, mount=False)
        return False  # need additional swap


class ExecAction(object):
    def __init__(self, *args):
        self.args = args

    def __call__(self, client):
        logging.info("Executing: %s", self.args)
        subprocess.check_call(self.args)


class Migration(object):
    """
    Migrates tables

    :param initial_table_infos:
        dictionary with TableInfo for all tables at the time of the "initial_version"
    :param initial_version:
        the minimum possible version of the tables
    :param transforms:
        dictionary containing for each version a list of conversions to be performed when switching from the previous version to it
    :param actions:
        dictionary containing for each version a list of actions to be performed when switching from the previous version to it
    :param table_init_callback:
        a function for custom initialization of tables, called after tables creation, but before mount
    """

    def __init__(
        self,
        initial_table_infos,
        initial_version,
        transforms={},
        actions={},
        table_init_callback=None,
    ):
        self.initial_table_infos = initial_table_infos
        self.initial_version = initial_version
        self.table_init_callback = table_init_callback
        self.transforms = transforms
        self.actions = actions

    def _create_table(self, client, table_info, table_path, shard_count):
        logging.info("Creating dynamic table %s", table_path)
        table_info = copy.deepcopy(table_info)

        if table_info.get_pivot_keys is not None:
            table_info.attributes["pivot_keys"] = table_info.get_pivot_keys(shard_count)
        if table_info.in_memory:
            table_info.attributes["in_memory_mode"] = "compressed"
        table_info.create_dynamic_table(client, table_path)

    def _initialize_migration(self, client, tables_path, version=None, tablet_cell_bundle=None, shard_count=1, mount=False):
        if version is None:
            version = self.initial_version

        table_infos = copy.deepcopy(self.initial_table_infos)
        for version in range(self.initial_version + 1, version + 1):
            for conversion in self.transforms.get(version, []):
                if conversion.table_info:
                    table_infos[conversion.table] = conversion.table_info

        for table_name, table_info in table_infos.items():
            if tablet_cell_bundle is not None:
                table_info.attributes["tablet_cell_bundle"] = tablet_cell_bundle
            self._create_table(client, table_info, ypath_join(tables_path, table_name), shard_count=shard_count)

        if self.table_init_callback:
            self.table_init_callback(client, tables_path)

        client.set(tables_path + "/@version", version)

        if mount:
            for table_name in table_infos.keys():
                client.mount_table(ypath_join(tables_path, table_name), sync=False)
            for table_name in table_infos.keys():
                _wait_for_predicate(
                    lambda: client.get(ypath_join(tables_path, table_name) + "/@tablet_state") == "mounted",
                    "table {} becomes mounted".format(table_name))

    def _transform(self, client, transform_begin, transform_end, force, tables_path, shard_count):
        logging.info("Transforming from %s to %s version", transform_begin - 1, transform_end)
        table_infos = copy.deepcopy(self.initial_table_infos)
        for version in range(self.initial_version, transform_begin):
            for conversion in self.transforms.get(version, []):
                if conversion.table_info:
                    table_infos[conversion.table] = conversion.table_info

        for version in range(transform_begin, transform_end + 1):
            logging.info("Transforming to version %d", version)
            swap_tasks = []
            if version in self.transforms:
                for conversion in self.transforms[version]:
                    table = conversion.table
                    table_path = ypath_join(tables_path, table)

                    # Filters out conversions according to their filter_callback, if present.
                    if conversion.filter_callback and not conversion.filter_callback(client=client, table_path=table_path):
                        continue

                    table_exists = client.exists(table_path)

                    shard_count = shard_count
                    if table_exists:
                        shard_count = int(client.get(table_path + "/@tablet_count"))

                    tmp_path = "{0}/{1}.tmp.{2}".format(tables_path, table, version)
                    if force and client.exists(tmp_path):
                        client.remove(tmp_path)
                    in_place = conversion(
                        client=client,
                        table_info=table_infos.get(table),
                        target_table=tmp_path,
                        source_table=table if (table in table_infos and table_exists) else None,
                        tables_path=tables_path,
                        shard_count=shard_count,
                    )
                    if not in_place:
                        swap_tasks.append((table_path, tmp_path))
                    if conversion.table_info:
                        table_infos[table] = conversion.table_info

                for target_path, tmp_path in swap_tasks:
                    _swap_table(client, target_path, tmp_path, version)

            if version in self.actions:
                for action in self.actions[version]:
                    action(client)

            client.set_attribute(tables_path, "version", version)

        for table in table_infos.keys():
            path = ypath_join(tables_path, table)
            if client.get(path + "/@tablet_state") != "mounted":
                _mount_table(client, path)

    def get_latest_version(self):
        latest_version = self.initial_version
        if self.transforms:
            latest_version = max(latest_version, max(self.transforms.keys()))
        if self.actions:
            latest_version = max(latest_version, max(self.actions.keys()))
        return latest_version

    def get_schemas(self, version=None):
        """Returns mapping from table name to its schema"""
        if version is None:
            version = self.get_latest_version()

        # NB: Everything is copied to prevent user from changing
        # initial_table_infos or conversions.
        table_infos = copy.deepcopy(self.initial_table_infos)
        for version in range(self.initial_version + 1, version + 1):
            for conversion in self.transforms.get(version, []):
                if conversion.source:
                    del table_infos[conversion.source]
                if conversion.table_info:
                    table_infos[conversion.table] = copy.deepcopy(conversion.table_info)

        return {table: table_info.schema for table, table_info in table_infos.items()}

    def create_tables(self, client, target_version, tables_path, shard_count, override_tablet_cell_bundle="default", force_initialize=False):
        """Creates tables of given version"""
        assert target_version == self.initial_version or target_version in self.transforms
        assert target_version >= self.initial_version

        if not client.exists(tables_path) or force_initialize:
            self._initialize_migration(
                client,
                tables_path=tables_path,
                version=target_version,
                tablet_cell_bundle=override_tablet_cell_bundle,
                shard_count=shard_count,
                mount=True,
            )
            return

        current_version = client.get("{0}/@".format(tables_path)).get("version", self.initial_version)
        assert current_version >= self.initial_version, \
            "Expected initial version to be >= {}, got {}".format(self.initial_version, current_version)

        table_infos = {}
        for version in range(current_version + 1, target_version + 1):
            for conversion in self.transforms.get(version, []):
                if conversion.table_info:
                    table_infos[conversion.table] = conversion.table_info

        for table, table_info in table_infos.items():
            table_path = ypath_join(tables_path, table)
            if override_tablet_cell_bundle is not None:
                table_info.attributes["tablet_cell_bundle"] = override_tablet_cell_bundle
            if not client.exists(table_path):
                table_info.create_dynamic_table(client, table_path)
            table_info.alter_table(client, table_path, shard_count)

        client.set(tables_path + "/@version", target_version)

    def run(self, client, tables_path, target_version, shard_count, force):
        """Migrate tables to the given version"""
        assert target_version == self.initial_version or target_version in self.transforms
        assert target_version >= self.initial_version

        current_version = None
        if client.exists(tables_path):
            current_version = client.get("{0}/@".format(tables_path)).get("version")

        if current_version is None:
            current_version = target_version
            self._initialize_migration(client, tables_path=tables_path, version=current_version)

        assert current_version >= self.initial_version, \
            "Expected tables version to be >= {}, got {}".format(self.initial_version, current_version)

        next_version = current_version + 1

        self._transform(client, next_version, target_version, force, tables_path, shard_count)
