#!/usr/bin/env python

import yt.yson as yson
import yt.wrapper as yt

import argparse

BY_ID_ARCHIVE = "//sys/operations_archive/ordered_by_id"
BY_START_TIME_ARCHIVE = "//sys/operations_archive/ordered_by_start_time"
SHARD_COUNT = 100

def get_cluster_version():
    sys_keys = yt.list("//sys")
    if "primary_masters" in sys_keys:
        masters_key = "primary_masters"
    else:
        masters_key = "masters"

    master_name = yt.list("//sys/" + masters_key)[0]
    version = yt.get("//sys/{0}/{1}/orchid/service/version".format(masters_key, master_name)).split(".")[:2]
    return version

def create_ordered_by_id_table(path, scheme_type):
    yt.remove(path, force=True)
    yt.create("table", path, ignore_existing=True, recursive=True)

    if get_cluster_version()[0] == "18":
        yt.set(path + "/@external", False)
        yt.set(path + "/@schema", [
            {"name": "id_hash",  "type": "uint64", "expression": "farm_hash(id_hi, id_lo)", "sort_order": "ascending"},
            {"name": "id_hi",  "type": "uint64", "sort_order": "ascending"},
            {"name": "id_lo",  "type": "uint64", "sort_order": "ascending"},
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

    else:
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

def create_ordered_by_start_time_table(path, scheme_type):
    yt.remove(path, force=True)
    yt.create("table", path, ignore_existing=True, recursive=True)

    if get_cluster_version()[0] == "18":
        yt.set(path + "/@external", False)
        yt.set(path + "/@schema", [
            {"name": "start_time", "type": "int64", },
            {"name": "id_hi",  "type": "uint64"},
            {"name": "id_lo",  "type": "uint64"},
            {"name": "dummy", "type": "int64"}])
    else:
        yt.set(path + "/@schema", [
            {"name": "start_time", "type": "int64", "sort_order": "ascending"},
            {"name": "id_hi",  "type": "uint64", "sort_order": "ascending"},
            {"name": "id_lo",  "type": "uint64", "sort_order": "ascending"},
            {"name": "dummy", "type": "int64"}])
        yt.set(path + "/@key_columns", ["start_time", "id_hi", "id_lo"])

    yt.mount_table(path)

def prepare_tables(proxy, scheme_type):
    if scheme_type == "old":
         raise Exception("Creating old schemes is not supported")

    yt.config["proxy"]["url"] = proxy
    yt.config["api_version"] = "v3"
    yt.config["proxy"]["header_format"] = "yson"

    create_ordered_by_id_table(BY_ID_ARCHIVE, scheme_type)
    create_ordered_by_start_time_table(BY_START_TIME_ARCHIVE, scheme_type)

def main():
    parser = argparse.ArgumentParser(description="Prepare dynamic tables for operations archive")
    parser.add_argument("--proxy", metavar="PROXY")
    args = parser.parse_args()

    prepare_tables(args.proxy)

if __name__ == "__main__":
    main()

