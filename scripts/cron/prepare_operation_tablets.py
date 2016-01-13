#!/usr/bin/env python

import yt.yson as yson
import yt.wrapper as yt

import argparse

BY_ID_ARCHIVE = "//sys/operations_archive/ordered_by_id"
BY_START_TIME_ARCHIVE = "//sys/operations_archive/ordered_by_start_time"
SHARD_COUNT = 100

def create_ordered_by_id_table(path):
    yt.remove(path, force=True)
    yt.create("table", path, ignore_existing=True, recursive=True)

    yt.set(path + "/@schema", [
        {"name": "id_hash",  "type": "uint64", "expression": "farm_hash(id_hi, id_lo)"},
        {"name": "id_hi",  "type": "uint64"},
        {"name": "id_lo",  "type": "uint64"},
        {"name": "state", "type": "string"},
        {"name": "authenticated_user", "type": "string"},
        {"name": "operation_type", "type": "string"},
        {"name": "progress", "type": "any"},
        {"name": "spec", "type": "any"},
        {"name": "brief_progress", "type": "any"},
        {"name": "brief_spec", "type": "any"},
        {"name": "start_time", "type": "int64"},
        {"name": "finish_time", "type": "int64"},
        {"name": "filter_factors", "type": "string"},
        {"name": "result", "type": "any"}])

    yt.set_attribute(path, "key_columns", ["id_hash", "id_hi", "id_lo"])

    pivot_keys = [[]] + [[yson.YsonUint64((i * 2 ** 64) / SHARD_COUNT)] for i in xrange(1, SHARD_COUNT)]
    yt.reshard_table(path, pivot_keys)
    yt.mount_table(path)

def create_ordered_by_start_time_table(path):
    yt.remove(path, force=True)
    yt.create("table", path, ignore_existing=True, recursive=True)

    yt.set(path + "/@schema", [
        {"name": "start_time", "type": "int64"},
        {"name": "id_hi",  "type": "uint64"},
        {"name": "id_lo",  "type": "uint64"},
        {"name": "dummy", "type": "int64"}])
    yt.set(path + "/@key_columns", ["start_time", "id_hi", "id_lo"])
    yt.mount_table(path)

def prepare_tables(proxy):
    yt.config["proxy"]["url"] = proxy
    yt.config["api_version"] = "v3"
    yt.config["proxy"]["header_format"] = "yson"

    create_ordered_by_id_table(BY_ID_ARCHIVE)
    create_ordered_by_start_time_table(BY_START_TIME_ARCHIVE)

def main():
    parser = argparse.ArgumentParser(description="Prepare dynamic tables for operations archive")
    parser.add_argument("--proxy", metavar="PROXY")
    args = parser.parse_args()

    prepare_tables(args.proxy)

if __name__ == "__main__":
    main()

