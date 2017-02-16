#!/usr/bin/python

import yt.yson as yson
import yt.wrapper as yt

import calendar
import datetime
import argparse
import logging

import yt.tools.dynamic_tables as dynamic_tables
from yt.tools.dynamic_tables import get_dynamic_table_attributes, make_dynamic_table_attributes

class TableInfo(object):
    def __init__(self, key_columns, value_columns, in_memory=False, pivot_keys=None):
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
        self.pivot_keys = pivot_keys
        self.in_memory = in_memory

    def create_table(self, path):
        attributes = make_dynamic_table_attributes(yt, self.schema, self.key_columns, "scan")

        logging.info("Creating table %s with attributes %s", path, attributes)
        yt.create("table", path, recursive=True, attributes=attributes)

        if self.pivot_keys:
            yt.reshard_table(path, self.pivot_keys)

        yt.mount_table(path)

    def alter_table(self, path):
        logging.info("Unmounting table %s", path)
        dynamic_tables.unmount_table(path)
        attributes = make_dynamic_table_attributes(yt, self.schema, self.key_columns, "scan")

        logging.info("Alter table %s with attributes %s", path, attributes)
        yt.alter_table(path, schema=attributes['schema'])

        if self.pivot_keys:
            yt.reshard_table(path, self.pivot_keys)

        logging.info("Mounting table %s", path)
        dynamic_tables.mount_table(path)

    def get_default_mapper(self):
        column_names = self.user_columns

        def default_mapper(row):
            yield {key: row.get(key) for key in column_names}

        return default_mapper

class Convert(object):
    def __init__(self, table, table_info=None, mapper=None, source=None, use_default_mapper=False):
        self.table = table
        self.table_info = table_info
        self.mapper = mapper
        self.source = source
        self.use_default_mapper = use_default_mapper

    def __call__(self, table_info, target_table, source_table, base_path):
        if self.table_info:
            table_info = self.table_info

        if not self.use_default_mapper and not self.mapper and not self.source and source_table:
            source_table = yt.ypath_join(base_path, source_table)
            table_info.alter_table(source_table)
            return True  # in place transformation

        table_info.create_table(target_table)

        source_table = self.source or source_table
        client = dynamic_tables.DynamicTablesClient(job_count=100, job_memory_limit=4*2**30, batch_size=10000)

        if source_table:
            source_table = yt.ypath_join(base_path, source_table)
            mapper = self.mapper
            if not mapper:
                mapper = table_info.get_default_mapper()

            client.mount_table(source_table)
            client.run_map_dynamic(mapper, source_table, target_table)
            client.unmount_table(target_table)
            yt.set(target_table + "/@forced_compaction_revision", yt.get(target_table + "/@revision"))
            if table_info.in_memory:
                yt.set(target_table + "/@in_memory_mode", "compressed")
            client.mount_table(target_table)
        return False  # need additional swap

SHARD_COUNT = 100
DEFAULT_PIVOTS = [[]] + [[yson.YsonUint64((i * 2 ** 64) / SHARD_COUNT)] for i in xrange(1, SHARD_COUNT)]

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
        pivot_keys=DEFAULT_PIVOTS)),
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
            pivot_keys=DEFAULT_PIVOTS)),
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
            pivot_keys=DEFAULT_PIVOTS))
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
            pivot_keys=DEFAULT_PIVOTS))
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
            pivot_keys=DEFAULT_PIVOTS))
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
            pivot_keys=DEFAULT_PIVOTS),
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
            pivot_keys=DEFAULT_PIVOTS),
        use_default_mapper=True)
]

def swap_table(target, source):
    client = dynamic_tables.DynamicTablesClient()

    backup_path = target + ".bak"
    if yt.exists(target):
        client.unmount_table(target)
        yt.move(target, backup_path)

    client.unmount_table(source)
    yt.move(source, target)

    if yt.exists(backup_path):
        yt.move(backup_path, source)

    client.mount_table(target)

def transform_archive(transform_begin, transform_end, force, archive_path):
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
            if force and yt.exists(tmp_path):
                yt.remove(tmp_path)
            in_place = convertion(schemas.get(table), tmp_path, table if table in schemas else None, archive_path)
            if not in_place:
                swap_tasks.append((yt.ypath_join(archive_path, table), tmp_path))

        for target_path, tmp_path in swap_tasks:
            swap_table(target_path, tmp_path)

        yt.set_attribute(archive_path, "version", version)


BASE_PATH = "//sys/operations_archive"

def main():
    parser = argparse.ArgumentParser(description="Transform operations archive")
    parser.add_argument("--target-version", type=int)
    parser.add_argument("--force", action="store_true", default=False)
    parser.add_argument("--archive-path", type=str, default=BASE_PATH)

    args = parser.parse_args()

    archive_path = args.archive_path

    logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)

    if yt.exists(archive_path):
        current_version = yt.get("{}/@".format(archive_path)).get("version", 0)
    else:
        current_version = -1

    next_version = current_version + 1

    target_version = args.target_version
    init_operation_archive.transform_archive(next_version, target_version, args.force, archive_path)

if __name__ == "__main__":
    main()

