#!/usr/bin/env python

import argparse

import yt.wrapper as yt

GB = 1024 * 1024 * 1024

def main(root, fix_options):
    rows = []
    rows.append(["Table", "# Tablets", "Mode", "Total Size (GBs)", "Tablet Size (Min/Med/Max; GBs)"])

    nodes = yt.list("//sys/nodes", attributes=["statistics"])
    memory_statistics = [
        node.attributes.get("statistics", {}).get("memory", {}).get("tablet_static", {})
        for node in nodes]
    memory_used = sum(m.get("used", 0) for m in memory_statistics)
    memory_used = float(memory_used) / GB
    memory_limit = sum(m.get("limit", 0) for m in memory_statistics)
    memory_limit = float(memory_limit) / GB

    print "Cluster Usage: %.2f / %.2f GBs (%.2f%%)" % (
        memory_used, memory_limit, 100.0 * memory_used / memory_limit)

    tables = yt.search(root, node_type="table", attributes=["dynamic", "compressed_data_size",
                                                            "uncompressed_data_size", "in_memory_mode"])
    for n, table in enumerate(tables):
        if not table.attributes.get("dynamic", False):
            continue
        imm = table.attributes.get("in_memory_mode", "none")
        key = None
        if imm == "none":
            continue
        elif imm == "uncompressed":
            key = "uncompressed_data_size"
        elif imm == "compressed":
            key = "compressed_data_size"
        if fix_options:
            yt.set(table + "/@compression_codec", "lz4")
            yt.set(table + "/@min_compaction_store_count", 2)
            yt.remount_table(table)
        tablets = yt.get("%s/@tablets" % table)
        tablet_sizes = list(sorted(t["statistics"][key] for t in tablets))
        table_size = float(table.attributes.get(key)) / GB
        min_tablet_size = float(tablet_sizes[0]) / GB
        med_tablet_size = float(tablet_sizes[len(tablet_sizes) / 2]) / GB
        max_tablet_size = float(tablet_sizes[-1]) / GB
        columns = []
        columns.append(table + (" ^" if fix_options else ""))
        columns.append(len(tablets))
        columns.append(imm)
        columns.append("%.2f" % table_size)
        columns.append("%6.2f .. %6.2f .. %6.2f" % (min_tablet_size, med_tablet_size, max_tablet_size))
        rows.append(map(str,  columns))

    widths = [max(map(len, values)) for values in zip(*rows)]
    fmt = " | ".join("%%%ds" % width for width in widths)
    cnt = sum(widths) + 3 * (len(widths) - 1)

    for n, row in enumerate(rows):
        if n == 0:
            print "-" * cnt
        print fmt % tuple(row)
        if n == 0:
            print "-" * cnt


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=str, default="/")
    parser.add_argument("--fix-options", action="store_true", default=False)

    args = parser.parse_args()

    main(args.root, args.fix_options)
