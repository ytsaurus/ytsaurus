#!/usr/bin/env python

import argparse
import logging
import time
import sys

import yt.wrapper as yt


def gradually_compact_table(table, tolerance):
    tablets = table.attributes.get("tablets", [])
    if len(tablets) == 0:
        return

    total_store_count = sum(tablet["statistics"]["store_count"] for tablet in tablets)
    total_partition_count = sum(tablet["statistics"]["partition_count"] for tablet in tablets)

    logging.info(
        "[%s] Compacting table with %s tablets, %s stores and %s partitions",
        table, len(tablets), total_store_count, total_partition_count)

    table = str(table)

    last_forced_compaction_time = None
    while True:
        now = time.time()
        if last_forced_compaction_time is None or (now - last_forced_compaction_time) > 300.0:
            logging.info("[%s] Updating @forced_compaction_revision", table)
            yt.set(table + "/@forced_compaction_revision", yt.get(table + "/@revision"))
            yt.set(table + "/@forced_compaction_revision", yt.get(table + "/@revision"))
            yt.remount_table(table)
            last_forced_compaction_time = now
            time.sleep(5.0)

        tablets = yt.get(table + "/@tablets")
        total_store_count = sum(tablet["statistics"]["store_count"] for tablet in tablets)
        total_partition_count = sum(tablet["statistics"]["partition_count"] for tablet in tablets)
        ratio = float(total_store_count) / float(total_partition_count)
        logging.info(
            "[%s] Stores: %s, Partitions: %s, Ratio: %.2f",
            table, total_store_count, total_partition_count, ratio)
        if ratio < tolerance:
            break
        elif ratio > 1.5:
            time.sleep(60.0)
        elif ratio > 1.1:
            time.sleep(30.0)
        else:
            time.sleep(15.0)

    logging.info("[%s] Done", table)


def gradually_compact_tables(tolerance):
    tables = yt.search("/", node_type="table", attributes=["uncompressed_data_size", "tablets"])
    tables = sorted(tables, key=lambda table: table.attributes.get("uncompressed_data_size"), reverse=True)
    for table in tables:
        gradually_compact_table(table, tolerance)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tolerance", type=float, default=1.05,
                        help="Tolerance between number of partitions and stores")
    parser.add_argument("--silent", action="store_true",
                        help="Do not log anything")
    parser.add_argument("--yes", action="count",
                        help="Actually do something (do nothing by default)")
    args = parser.parse_args()

    if args.silent:
        logging.basicConfig(level=logging.ERROR)
    else:
        logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)

    if args.tolerance <= 1.0:
        logging.error("Tolerance must be >= 1.0 (actual: %.2f)", args.tolerance)
        sys.exit(1)

    gradually_compact_tables(args.tolerance)
