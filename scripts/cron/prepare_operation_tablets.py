#!/usr/bin/env python

import yt.yson as yson
import yt.wrapper as yt

SHARD_COUNT = 100
yt.config.VERSION = "v3"
yt.config.http.HEADER_FORMAT = "yson"

def create_ordered_by_id_table():
    path = "//sys/operations_archive/ordered_by_id"
    yt.remove(path, force=True)
    yt.create("table", path, ignore_existing=True, recursive=True)

    yt.set(path + "/@schema", [
        {"name": "id_hash",  "type": "uint64", "expression": "farm_hash(id)"},
        {"name": "id",  "type": "string"},
        {"name": "state", "type": "string"},
        {"name": "authenticated_user", "type": "string"},
        {"name": "operation_type", "type": "string"},
        {"name": "progress", "type": "any"},
        {"name": "spec", "type": "any"},
        {"name": "brief_progress", "type": "any"},
        {"name": "brief_spec", "type": "any"},
        {"name": "start_time", "type": "string"},
        {"name": "finish_time", "type": "string"},
        {"name": "result", "type": "any"}])

    yt.set_attribute(path, "key_columns", ["id_hash", "id"])

    pivot_keys = [[]] + [[yson.YsonUint64((i * 2 ** 63) / SHARD_COUNT)] for i in xrange(1, SHARD_COUNT)]
    yt.reshard_table(path, pivot_keys)
    yt.mount_table(path)

def create_ordered_by_start_time_table():
    path = "//sys/operations_archive/ordered_by_start_time"
    yt.remove(path, force=True)
    yt.create("table", path, ignore_existing=True, recursive=True)

    yt.set(path + "/@schema", [
        {"name": "start_time", "type": "string"},
        {"name": "id_hash", "type": "uint64", "expression": "farm_hash(id)"},
        {"name": "id", "type": "string"},
        {"name": "dummy", "type": "string"}])
    yt.set(path + "/@key_columns", ["start_time", "id_hash", "id"])
    yt.mount_table(path)

def main():
    create_ordered_by_id_table()
    create_ordered_by_start_time_table()

if __name__ == "__main__":
    main()

