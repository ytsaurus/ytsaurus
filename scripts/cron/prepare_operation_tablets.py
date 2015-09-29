#!/usr/bin/env python

import yt.yson as yson
import yt.wrapper as yt

from copy import deepcopy

SHARD_COUNT = 100

def remove_by_bame(fields, name):
    return filter(lambda item: item["name"] != name, fields)

def move_to_front(fields, name):
    index = -1
    for i, item in enumerate(fields):
        if item["name"] == name:
            index = i
            break
    fields = deepcopy(fields)

    item = fields[index]
    fields.pop(index)
    fields.insert(0, item)

    return fields

def main():
    fields = [
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
        {"name": "result", "type": "any"},
        {"name": "filter_factors", "type": "string"}]

    ordered_by_id = "//sys/operations_archive/ordered_by_id"
    ordered_by_start_time = "//sys/operations_archive/ordered_by_start_time"
 
    for table in (ordered_by_id, ordered_by_start_time):
        yt.remove(table, force=True)
        yt.create("table", table, ignore_existing=True, recursive=True)

    # ordered_by_id
    yt.set_attribute(ordered_by_id, "schema", fields)
    yt.set_attribute(ordered_by_id, "key_columns", ["id_hash", "id"])
    pivot_keys = [[]] + [[yson.YsonUint64((i * 2 ** 63) / SHARD_COUNT)] for i in xrange(1, SHARD_COUNT)]
    yt.reshard_table(ordered_by_id, pivot_keys)
    yt.mount_table(ordered_by_id)

    # ordered_by_start_time
    yt.set_attribute(ordered_by_start_time, "schema", move_to_front(remove_by_bame(fields, "id_hash"), "start_time"))
    yt.set_attribute(ordered_by_start_time, "key_columns", ["start_time"])
    yt.mount_table(ordered_by_start_time)

if __name__ == "__main__":
    main()

