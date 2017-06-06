#!/usr/bin/python

import yt.yson as yson
import yt.wrapper as yt
from yt.wrapper import YtClient

import argparse
import logging

from yt.tools.dynamic_tables import make_dynamic_table_attributes, mount_table_new, unmount_table_new, DynamicTablesClient

from yt.packages.six.moves import xrange
from yt.packages.six import iteritems


class TableInfo(object):
    def __init__(self, key_columns, value_columns, in_memory=False, get_pivot_keys=None):
        def make_column(name, type_name, expression=None):
            return {
                "name": name,
                "type": type_name
            }

        def make_key_column(name, type_name, expression=None):
            result = make_column(name, type_name)
            if expression:
                result["expression"] = expression
            return result

        self.schema = [make_key_column(*columns) for columns in key_columns]
        self.key_columns = [column["name"] for column in self.schema]
        self.schema += [make_column(*column) for column in value_columns]
        self.user_columns = [column["name"] for column in self.schema if "expression" not in column]
        self.get_pivot_keys = get_pivot_keys
        self.in_memory = in_memory

    def create_table(self, client, path, shards, attributes):
        attrs = make_dynamic_table_attributes(client, self.schema, self.key_columns, "scan")
        for attribute, value in iteritems(attributes):
            attrs[attribute] = value

        logging.info("Creating table %s with attributes %s", path, attrs)
        client.create("table", path, recursive=True, attributes=attrs)

        if self.get_pivot_keys:
            client.reshard_table(path, self.get_pivot_keys(shards))

        client.mount_table(path)

    def alter_table(self, client, path, shards, attributes):
        logging.info("Unmounting table %s", path)
        unmount_table_new(client, path)
        attrs = make_dynamic_table_attributes(client, self.schema, self.key_columns, "scan")
        schema = attrs["schema"]

        logging.info("Altering table %s with schema %s and attributes %s", path, schema, attributes)
        client.alter_table(path, schema=schema)
        for attribute, value in iteritems(attributes):
            if attribute != "schema":
                client.set(path + "/@" + attribute, value)

        if self.get_pivot_keys:
            client.reshard_table(path, self.get_pivot_keys(shards))

        logging.info("Mounting table %s", path)
        mount_table_new(client, path)

    def get_default_mapper(self):
        column_names = self.user_columns

        def default_mapper(row):
            yield {key: row.get(key) for key in column_names}

        return default_mapper

DEFAULT_SHARD_COUNT = 100
class Convert(object):
    def __init__(self, table, table_info=None, mapper=None, source=None, use_default_mapper=False, attributes={}):
        self.table = table
        self.table_info = table_info
        self.mapper = mapper
        self.source = source
        self.use_default_mapper = use_default_mapper
        self.attributes = attributes

    def __call__(self, client, table_info, target_table, source_table, base_path, shard_count=DEFAULT_SHARD_COUNT, **kwargs):
        if self.table_info:
            table_info = self.table_info

        if not self.use_default_mapper and not self.mapper and not self.source and source_table:
            source_table = yt.ypath_join(base_path, source_table)
            table_info.alter_table(client, source_table, shard_count, self.attributes)
            return True  # in place transformation

        table_info.create_table(client, target_table, shard_count, self.attributes)

        source_table = self.source or source_table
        dt_client = DynamicTablesClient(
            client,
            job_count=100,
            job_memory_limit=4*2**30,
            batch_size=10000,
            **kwargs)

        if source_table:
            source_table = yt.ypath_join(base_path, source_table)
            mapper = self.mapper
            if not mapper:
                mapper = table_info.get_default_mapper()

            mount_table_new(client, source_table)
            dt_client.run_map_dynamic(mapper, source_table, target_table)

            unmount_table_new(client, target_table)
            client.set(target_table + "/@forced_compaction_revision", client.get(target_table + "/@revision"))
            if table_info.in_memory:
                client.set(target_table + "/@in_memory_mode", "compressed")
            mount_table_new(client, target_table)
        return False  # need additional swap


def get_default_pivots(shard_count):
    return [[]] + [[yson.YsonUint64((i * 2 ** 64) / shard_count)] for i in xrange(1, shard_count)]

TRANSFORMS = {}

TRANSFORMS[0] = [
    Convert(
        "ordered_by_id",
        table_info=TableInfo([
            ("id_hash", "uint64", "farm_hash(id_hi, id_lo)"),
            ("id_hi", "uint64"),
            ("id_lo", "uint64"),
        ], [
            ("state", "string"),
            ("authenticated_user", "string"),
            ("operation_type", "string"),
            ("progress", "any"),
            ("spec", "any"),
            ("brief_progress", "any"),
            ("brief_spec", "any"),
            ("start_time", "int64"),
            ("finish_time", "int64"),
            ("filter_factors", "string"),
            ("result", "any")
        ],
        in_memory=True,
        get_pivot_keys=get_default_pivots)),
    Convert(
        "ordered_by_start_time",
        table_info=TableInfo([
            ("start_time", "int64"),
            ("id_hi", "uint64"),
            ("id_lo", "uint64")
        ], [
            ("dummy", "int64")
        ],
        in_memory=True))
]

def convert_start_finish_time_mapper(row):
    row['start_time'] = row['start_time'] + 1000000 * 60 * 60 * 3
    row['finish_time'] = row['finish_time'] + 1000000 * 60 * 60 * 3
    yield row

def convert_start_time_mapper(row):
    row['start_time'] = row['start_time'] + 1000000 * 60 * 60 * 3
    yield row

TRANSFORMS[1] = [
    Convert("ordered_by_id", mapper=convert_start_finish_time_mapper),
    Convert("ordered_by_start_time", mapper=convert_start_time_mapper)
]

TRANSFORMS[2] = [
    Convert(
        "ordered_by_start_time",
        table_info=TableInfo([
                ("start_time", "int64"),
                ("id_hi", "uint64"),
                ("id_lo", "uint64"),
            ], [
                ("operation_type", "string"),
                ("state", "string"),
                ("authenticated_user", "string"),
                ("filter_factors", "string")
            ],
            in_memory=True),
        source="ordered_by_id")
]

TRANSFORMS[3] = [
    Convert(
        "stderrs",
        table_info=TableInfo([
                ("operation_id_hi", "uint64"),
                ("operation_id_lo", "uint64"),
                ("job_id_hi", "uint64"),
                ("job_id_lo", "uint64")
            ], [
                ("stderr", "string")
            ],
            get_pivot_keys=get_default_pivots)),
    Convert(
        "jobs",
        table_info=TableInfo([
                ("operation_id_hi", "uint64"),
                ("operation_id_lo", "uint64"),
                ("job_id_hi", "uint64"),
                ("job_id_lo", "uint64")
            ], [
                ("job_type", "string"),
                ("state", "string"),
                ("start_time", "int64"),
                ("finish_time", "int64"),
                ("address", "string"),
                ("error", "any"),
                ("statistics", "any")
            ],
            get_pivot_keys=get_default_pivots))
]

TRANSFORMS[4] = [
    Convert(
        "jobs",
        table_info=TableInfo([
                ("operation_id_hi", "uint64"),
                ("operation_id_lo", "uint64"),
                ("job_id_hi", "uint64"),
                ("job_id_lo", "uint64")
            ], [
                ("job_type", "string"),
                ("state", "string"),
                ("start_time", "int64"),
                ("finish_time", "int64"),
                ("address", "string"),
                ("error", "any"),
                ("statistics", "any"),
                ("stderr_size", "uint64")
            ],
            get_pivot_keys=get_default_pivots))
]

TRANSFORMS[5] = [
    Convert(
        "ordered_by_id",
        table_info=TableInfo([
                ("id_hash", "uint64", "farm_hash(id_hi, id_lo)"),
                ("id_hi", "uint64"),
                ("id_lo", "uint64"),
            ], [
                ("state", "string"),
                ("authenticated_user", "string"),
                ("operation_type", "string"),
                ("progress", "any"),
                ("spec", "any"),
                ("brief_progress", "any"),
                ("brief_spec", "any"),
                ("start_time", "int64"),
                ("finish_time", "int64"),
                ("filter_factors", "string"),
                ("result", "any"),
                ("events", "any")
            ],
            in_memory=True,
            get_pivot_keys=get_default_pivots))
]

def convert_id(id):
    return id >> 32 | (((1 << 32) - 1) & id) << 32

def convert_id_mapper(row):
    row["id_hi"] = convert_id(row["id_hi"])
    row["id_lo"] = convert_id(row["id_lo"])
    yield row

def convert_job_id(row):
    row["operation_id_hi"] = convert_id(row["operation_id_hi"])
    row["operation_id_lo"] = convert_id(row["operation_id_lo"])
    row["job_id_hi"] = convert_id(row["job_id_hi"])
    row["job_id_lo"] = convert_id(row["job_id_lo"])

def convert_job_id_mapper(row):
    convert_job_id(row)
    yield row

def convert_job_id_and_job_type_mapper(row):
    convert_job_id(row)
    row["type"] = row["job_type"]
    del row["job_type"]
    yield row

TRANSFORMS[6] = [
    Convert(
        "ordered_by_id",
        mapper=convert_id_mapper),
    Convert(
        "ordered_by_start_time",
        mapper=convert_id_mapper),
    Convert(
        "jobs",
        table_info=TableInfo([
                ("operation_id_hi", "uint64"),
                ("operation_id_lo", "uint64"),
                ("job_id_hi", "uint64"),
                ("job_id_lo", "uint64")
            ], [
                ("type", "string"),
                ("state", "string"),
                ("start_time", "int64"),
                ("finish_time", "int64"),
                ("address", "string"),
                ("error", "any"),
                ("statistics", "any"),
                ("stderr_size", "uint64")
            ],
            get_pivot_keys=get_default_pivots),
        mapper=convert_job_id_and_job_type_mapper),
    Convert(
        "stderrs",
        mapper=convert_job_id_mapper)
]

def convert_statistics_remove_attributes_mapper(row):
    statistics = row['statistics']
    if hasattr(statistics):
        statistics.attributes = {}
    yield row

TRANSFORMS[7] = [
    Convert(
        "jobs",
        table_info=TableInfo([
                ("operation_id_hash", "uint64", "farm_hash(operation_id_hi, operation_id_lo)"),
                ("operation_id_hi", "uint64"),
                ("operation_id_lo", "uint64"),
                ("job_id_hi", "uint64"),
                ("job_id_lo", "uint64")
            ], [
                ("type", "string"),
                ("state", "string"),
                ("start_time", "int64"),
                ("finish_time", "int64"),
                ("address", "string"),
                ("error", "any"),
                ("statistics", "any"),
                ("stderr_size", "uint64"),
                ("spec", "string"),
                ("spec_version", "int64"),
                ("events", "any")
            ],
            get_pivot_keys=get_default_pivots),
        use_default_mapper=True)
]

TRANSFORMS[8] = [
    Convert(
        "ordered_by_id",
        attributes={"tablet_cell_bundle": "sys"}),
    Convert(
        "ordered_by_start_time",
        attributes={"tablet_cell_bundle": "sys"}),
    Convert(
        "jobs",
        attributes={"tablet_cell_bundle": "sys"}),
    Convert(
        "stderrs",
        attributes={"tablet_cell_bundle": "sys"}),
]

def swap_table(client, target, source):
    backup_path = target + ".bak"
    if client.exists(target):
        unmount_table_new(client, target)
        client.move(target, backup_path)

    unmount_table_new(client, source)
    client.move(source, target)

    if client.exists(backup_path):
        client.move(backup_path, source)

    mount_table_new(client, target)

def transform_archive(
    client,
    transform_begin,
    transform_end,
    force,
    archive_path,
    **kwargs):

    schemas = {}
    for version in xrange(0, transform_begin):
        for convertion in TRANSFORMS[version]:
            table = convertion.table
            if convertion.table_info:
                schemas[table] = convertion.table_info

    for version in xrange(transform_begin, transform_end + 1):
        logging.info("Transforming to version %d", version)
        swap_tasks = []
        for convertion in TRANSFORMS[version]:
            table = convertion.table
            tmp_path = "{}/{}.tmp.{}".format(archive_path, table, version)
            if force and client.exists(tmp_path):
                client.remove(tmp_path)
            in_place = convertion(
                client,
                schemas.get(table),
                tmp_path,
                table if table in schemas else None,
                archive_path,
                **kwargs)
            if not in_place:
                swap_tasks.append((yt.ypath_join(archive_path, table), tmp_path))
            if convertion.table_info:
                schemas[table] = convertion.table_info

        for target_path, tmp_path in swap_tasks:
            swap_table(client, target_path, tmp_path)

        client.set_attribute(archive_path, "version", version)

BASE_PATH = "//sys/operations_archive"

def create_tables_latest_version(client, shard_count=1, base_path=BASE_PATH):
    """ Creates operation archive tables of latest version """
    schemas = {}
    for version in sorted(TRANSFORMS.keys()):
        for convertion in TRANSFORMS[version]:
            if convertion.table_info:
                schemas[convertion.table] = convertion.table_info
    for table in schemas:
        table_path = BASE_PATH + "/" + table
        schemas[table].create_table(client, table_path, shard_count, {})

def main():
    parser = argparse.ArgumentParser(description="Transform operations archive")
    parser.add_argument("--target-version", type=int)
    parser.add_argument("--force", action="store_true", default=False)
    parser.add_argument("--archive-path", type=str, default=BASE_PATH)
    parser.add_argument("--shard-count", type=int, default=DEFAULT_SHARD_COUNT)

    args = parser.parse_args()

    archive_path = args.archive_path

    logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)

    client = YtClient(proxy=yt.config["proxy"]["url"], token=yt.config["token"])

    if client.exists(archive_path):
        current_version = client.get("{}/@".format(archive_path)).get("version", 0)
    else:
        current_version = -1

    next_version = current_version + 1

    target_version = args.target_version
    transform_archive(client, next_version, target_version, args.force, archive_path, shard_count=args.shard_count)

if __name__ == "__main__":
    main()

