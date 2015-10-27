#!/usr/bin/env python

import yt.yson as yson
import yt.wrapper as yt

import argparse

BY_ID_ARCHIVE = "//sys/operations_archive/ordered_by_id"
BY_START_TIME_ARCHIVE = "//sys/operations_archive/ordered_by_start_time"
SHARD_COUNT = 100

def create_ordered_by_id_table():
    path = BY_ID_ARCHIVE
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
        {"name": "filter_factors", "type": "string"},
        {"name": "result", "type": "any"}])

    yt.set_attribute(path, "key_columns", ["id_hash", "id"])

    pivot_keys = [[]] + [[yson.YsonUint64((i * 2 ** 64) / SHARD_COUNT)] for i in xrange(1, SHARD_COUNT)]
    yt.reshard_table(path, pivot_keys)
    yt.mount_table(path)

def create_ordered_by_start_time_table():
    path = BY_START_TIME_ARCHIVE
    yt.remove(path, force=True)
    yt.create("table", path, ignore_existing=True, recursive=True)

    yt.set(path + "/@schema", [
        {"name": "start_time", "type": "string"},
        {"name": "id_hash", "type": "uint64", "expression": "farm_hash(id)"},
        {"name": "id", "type": "string"},
        {"name": "dummy", "type": "string"}])
    yt.set(path + "/@key_columns", ["start_time", "id_hash", "id"])
    yt.mount_table(path)

def prepare_tables(proxy):
    yt.config["proxy"]["url"] = proxy
    yt.config["api_version"] = "v3"
    yt.config["proxy"]["header_format"] = "yson"

    create_ordered_by_id_table()
    create_ordered_by_start_time_table()

def main():
    parser = argparse.ArgumentParser(description="Prepare dynamic tables for operations archive")
    parser.add_argument("--proxy", metavar="PROXY")
    args = parser.parse_args()

    prepare_tables(args.proxy)

if __name__ == "__main__":
    main()

