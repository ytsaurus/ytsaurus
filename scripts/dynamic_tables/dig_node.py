#!/usr/bin/env python

"""
Prints static tablet memory consumption for a particular node, grouped by table path.
"""

import argparse
from itertools import groupby

import yt.wrapper as yt

GB = 1024 * 1024 * 1024

def dig(node):
    path_cache = dict()
    mode_cache = dict()
    items = []

    print "Digging {}...".format(node)

    cells = yt.list("//sys/nodes/{}:9012/orchid/tablet_cells".format(node))
    for cell in cells:
        tablets = yt.list("//sys/nodes/{}:9012/orchid/tablet_cells/{}/tablets".format(node, cell))
        for tablet in tablets:
            datum = yt.get("//sys/nodes/{}:9012/orchid/tablet_cells/{}/tablets/{}".format(
                node, cell, tablet))
            if datum["table_id"] not in path_cache:
                path_cache[datum["table_id"]] = yt.get_attribute("#" + datum["table_id"], "path", None)
            if datum["table_id"] not in mode_cache:
                mode_cache[datum["table_id"]] = yt.get_attribute("#" + datum["table_id"], "in_memory_mode", "none")
            path = path_cache[datum["table_id"]]
            mode = mode_cache[datum["table_id"]]
            if mode != "none":
                items.append((path, yt.get("#" + tablet + "/@statistics/" + mode + "_data_size")))

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
    parser.add_argument("node", type=str, nargs=1)

    args = parser.parse_args()

    dig(args.node[0])
