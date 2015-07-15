#!/usr/bin/python

"""
This script rebalances given table, splitting large tablets if necessary.
"""

import argparse
import re
import time
import logging
import json

import yt.wrapper as yt
import yt.yson as yson

yt.config.VERSION = "v3"
yt.config.http.HEADER_FORMAT = "yson"


def get_tablets(table):
    tablets = yt.get(table + "/@tablets")
    ids = map(lambda t: t["tablet_id"], tablets)
    sizes = map(lambda t: t["statistics"]["uncompressed_data_size"], tablets)
    return zip(range(len(tablets)), ids, sizes)


def get_tablet_size(tablet_id):
    return yt.get("//sys/tablets/%s/@statistics/uncompressed_data_size" % tablet_id)


def get_tablet_info(tablet_id):
    tablet_cell_id = yt.get("//sys/tablets/%s/@cell_id" % tablet_id)
    peers = yt.get("//sys/tablet_cells/%s/@peers" % tablet_cell_id)
    leading_peer = None
    for peer in peers:
        if peer["state"] == "leading":
            leading_peer = peer
            break
    assert leading_peer["address"]
    # Due to 0.16-0.17 incompatibilities in YSON we load information in JSON and deserialize it manually.
    info = yt.get("//sys/nodes/%s/orchid/tablet_cells/%s/tablets/%s" % (
        leading_peer["address"],
        tablet_cell_id,
        tablet_id), format=yt.JsonFormat())
    info = json.loads(info)
    return info


def suggest_pivot_keys(tablet_id, desired_tablet_size, number_of_key_columns):
    info = get_tablet_info(tablet_id)
    size = get_tablet_size(tablet_id)
    keys = [info["pivot_key"][0:number_of_key_columns]]
    # We estimate pivot keys by looking on partitions.
    n = int(1 + size / desired_tablet_size)
    for i in range(1, n):
        k = i * len(info["partitions"]) / n
        p = info["partitions"][k]["next_pivot_key"][0:number_of_key_columns]
        if p != keys[-1]:
            keys.append(p)
    return keys


def rebalance(table, key_columns, threshold, yes=False):
    tablets = get_tablets(table)

    unbalanced_tablet_indexes = []
    for tablet_index, _, tablet_size in tablets:
        if tablet_size > threshold:
            unbalanced_tablet_indexes.append(tablet_index)

    logging.info("There are %s / %s unbalanced tablets in %s",
                 len(unbalanced_tablet_indexes), len(tablets), table)

    # Iterate in reverse order to produce correct sequence of operations.
    operations = []
    for unbalanced_tablet_index in reversed(unbalanced_tablet_indexes):
        tablet_id = tablets[unbalanced_tablet_index][1]
        pivot_keys = suggest_pivot_keys(tablet_id, threshold, key_columns)
        logging.info("Gathered %s splits from partition information for tablet %s: %s",
                     len(pivot_keys), unbalanced_tablet_index, pivot_keys)
        operations.append((unbalanced_tablet_index, pivot_keys))

    if not yes:
        fmt = lambda x: "'" + yson.dumps(x)[1:-1] + "'"
        for index, keys in operations:
            print "yt reshard_table --first '%s' --last '%s' '%s' %s" % (
                index, index, table, " ".join(map(fmt, keys)))
        logging.info("`--yes` was not specified; exiting.")
        return

    logging.info("You have 5 seconds to press Ctrl+C to abort...")
    time.sleep(5)

    logging.info("Unmounting %s", table)
    yt.unmount_table(table)
    while not all(x["state"] == "unmounted" for x in yt.get(table + "/@tablets")):
        logging.info("Waiting for tablets to become unmounted...")
        time.sleep(1)

    logging.info("Resharding %s", table)
    for index, keys in operations:
        logging.info("Resharding tablet %s in table %s", index, table)
        yt.reshard_table(table, keys, first_tablet_index=index, last_tablet_index=index)

    logging.info("Mounting %s", table)
    yt.mount_table(table)
    while not all(x["state"] == "mounted" for x in yt.get(table + "/@tablets")):
        logging.info("Waiting for tablets to become mounted...")
        time.sleep(1)

    logging.info("Done")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--table", type=str, required=True, help="Table to rebalance")
    parser.add_argument("--key-columns", type=int, required=True,
                        help="How many key columns to take into account")
    parser.add_argument("--threshold", type=int, required=False, default=80*1024*1024*1024,
                        help="Maximum tablet size")
    parser.add_argument("--silent", action="store_true", help="Do not log anything")
    parser.add_argument("--yes", action="store_true", help="Actually do something (do nothing by default)")
    args = parser.parse_args()
    if args.silent:
        logging.basicConfig(level=logging.ERROR)
    else:
        logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)

    rebalance(args.table, args.key_columns, args.threshold, args.yes)
