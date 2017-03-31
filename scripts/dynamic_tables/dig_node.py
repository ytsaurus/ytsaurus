#!/usr/bin/env python

"""
Prints static tablet memory consumption for a particular node, grouped by table path.
"""

import argparse
from itertools import groupby

import yt.wrapper as yt

GB = 1024 * 1024 * 1024

def checked(rsp, default=None):
    if "error" in rsp:
        if default:
            return default
        raise RuntimeError(rsp["error"])
    return rsp["output"]

def get_statistics_mode(dig_mode, in_memory_mode):
    if dig_mode == "memory":
        return in_memory_mode
    elif dig_mode == "compressed_disk":
        return "compressed"
    elif dig_mode == "uncompressed_disk":
        return "uncompressed"

def dig(node, dig_mode):
    path_cache = dict()
    mode_cache = dict()

    print "Digging tablet cells from node {}...".format(node)
    cells = yt.list("//sys/nodes/{}:9012/orchid/tablet_cells".format(node))

    reqs = []
    for cell in cells:
        reqs.append({"command": "list", "parameters": {
            "path": "//sys/nodes/{}:9012/orchid/tablet_cells/{}/tablets".format(node, cell)}})

    print "Digging {} tablet cells...".format(len(reqs))
    rsps = yt.execute_batch(reqs)
    tablets = []

    reqs = []
    for cell, rsp in zip(cells, rsps):
        for tablet in checked(rsp):
            tablets.append(tablet)
            reqs.append({"command": "get", "parameters": {
                "path": "//sys/nodes/{}:9012/orchid/tablet_cells/{}/tablets/{}".format(
                    node, cell, tablet)}})

    print "Digging {} tablets...".format(len(reqs))
    rsps = yt.execute_batch(reqs)

    tables = []
    for rsp in rsps:
        tables.append(checked(rsp)["table_id"])

    assert len(tablets) == len(tables)
    unique_tables = list(set(tables))
    print "Digging {} table modes...".format(len(unique_tables))

    reqs = []
    for table in unique_tables:
        reqs.append({"command": "get", "parameters": {"path": "#" + table + "/@in_memory_mode"}})
    rsps = yt.execute_batch(reqs)
    for table, rsp in zip(unique_tables, rsps):
        mode_cache[table] = checked(rsp, "none")

    filtered_tables = []
    filtered_tablets = []
    for table, tablet in zip(tables, tablets):
        if dig_mode != "memory" or mode_cache[table] != "none":
            filtered_tables.append(table)
            filtered_tablets.append(tablet)

    unique_tables = list(set(filtered_tables))
    print "Digging {} table paths...".format(len(unique_tables))

    reqs = []
    for table in unique_tables:
        reqs.append({"command": "get", "parameters": {"path": "#" + table + "/@path"}})
    rsps = yt.execute_batch(reqs)
    for table, rsp in zip(unique_tables, rsps):
        path_cache[table] = checked(rsp)

    reqs = []
    for table, tablet in zip(filtered_tables, filtered_tablets):
        reqs.append({"command": "get", "parameters": {
            "path": "#" + tablet + "/@statistics/" + get_statistics_mode(dig_mode, mode_cache[table]) + "_data_size"
        }})

    print "Digging {} tablet statistics...".format(len(reqs))
    rsps = yt.execute_batch(reqs)

    items = []
    for table, rsp in zip(filtered_tables, rsps):
        items.append((path_cache[table], checked(rsp)))

    result = []
    for key, group in groupby(sorted(items), key=lambda p: p[0]):
        result.append((key, sum(map(lambda p: p[1], group))))

    result = sorted(result, key=lambda p: p[1])

    width = max(len(p[0]) for p in result)
    fmt = "%%%ds | %%.2f GBs" % width
    total = 0
    for key, value in result:
        total += value
        print fmt % (key, float(value) / GB)
    print fmt % ("-" * (width - 1) + ">", float(total) / GB)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("node", type=str, help="node address to dig")
    parser.add_argument("--mode", type=str, default="memory", help="compressed_disk, uncompressed_disk or memory (default: memory)")

    args = parser.parse_args()

    dig(args.node, args.mode)
