#!/usr/bin/env python

import yt.wrapper as yt

import argparse
import sys

if __name__ == "__main__":
    parser = argparse.ArgumentParser("Reshard a dynamic table with evenly sampled pivots")

    parser.add_argument("--proxy", type=yt.config.set_proxy)
    parser.add_argument("table", type=str)
    parser.add_argument("tablet_count", type=int)

    args = parser.parse_args()

    if yt.get(args.table + "/@tablet_state") != "unmounted":
        print>>sys.stderr, "Table should be unmounted to use the script"
        exit(1)

    schema = yt.get(args.table + "/@schema")
    key_columns = [col["name"] for col in schema if col.get("sort_order") == "ascending"]

    tmp = yt.create_temp_table()
    yt.run_merge(
        yt.TablePath(args.table, columns=key_columns),
        tmp,
        mode="ordered")

    row_count = yt.get(tmp + "/@row_count")
    sampling_rate = args.tablet_count * 100.0 / row_count

    rows = [[]] + list(yt.read_table(tmp, table_reader={"sampling_rate": sampling_rate}))

    def flatten(key):
        return [key[col] for col in key_columns]

    pivots = [[]]
    for i in range(1, args.tablet_count):
        index = i * len(rows) / args.tablet_count
        pivots.append(flatten(rows[index]))

    for p in pivots:
        print p

    yt.reshard_table(args.table, pivots, sync=True)
