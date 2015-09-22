#!/usr/bin/env python

import argparse
import logging
import time
import sys

import yt.wrapper as yt

EPS = 1e-5


def gradually_compact_table(
        cluster_epoch, cluster_size, table, tolerance_before_compaction, tolerance_after_compaction,
        network_bandwidth, sleep_quantum, epochs_to_skip, compaction_timeout):
    tablets = yt.get(table + "/@tablets")

    if len(tablets) == 0:
        logging.warning("[%s] Table has no tablets; skipping", table)
        return

    total_store_count = sum(tablet["statistics"]["store_count"] for tablet in tablets)
    total_partition_count = sum(tablet["statistics"]["partition_count"] for tablet in tablets)
    total_disk_space = sum(tablet["statistics"]["disk_space"] for tablet in tablets)

    if total_store_count == 0 or total_partition_count == 0:
        logging.warning("[%s] Table has no stores or partitions; skipping", table)
        return

    ratio = float(total_store_count) / float(total_partition_count)

    if ratio < tolerance_before_compaction - EPS:
        logging.warning(
            "[%s] Table is already compacted (%s stores and %s partitions); skipping",
            table, total_store_count, total_partition_count)
        return

    table_compaction_revision = yt.get(table + "/@forced_compaction_revision")
    table_compaction_epoch = (table_compaction_revision >> 32)

    if table_compaction_epoch + epochs_to_skip > cluster_epoch:
        logging.warning(
            "[%s] Table was recently compacted (in epoch %s while current epoch is %s); skipping",
            table, table_compaction_epoch, cluster_epoch)
        return

    shuffle_time = float(total_disk_space)
    shuffle_time /= 1024.0 * 1024.0
    shuffle_time /= float(min(cluster_size, len(tablets))) * network_bandwidth

    logging.info(
        "[%s] Compacting table with %s tablets, %s stores and %s partitions (estimated shuffle time is %.0fs)",
        table, len(tablets), total_store_count, total_partition_count, shuffle_time)

    first_compaction_time = None
    last_compaction_time = None

    while True:
        now = time.time()
        if last_compaction_time is None or (now - last_compaction_time) > shuffle_time:
            logging.info("[%s] Updating @forced_compaction_revision", table)
            yt.set(table + "/@forced_compaction_revision", yt.get(table + "/@revision"))
            yt.set(table + "/@forced_compaction_revision", yt.get(table + "/@revision"))
            yt.remount_table(table)
            if first_compaction_time is None:
                first_compaction_time = now
            last_compaction_time = now
            time.sleep(sleep_quantum)
        if first_compaction_time is not None and (now - first_compaction_time) > compaction_timeout:
            logging.warning("[%s] Compaction is taking too long; bailing out", table)
            return

        assert first_compaction_time is not None
        assert last_compaction_time is not None

        tablets = yt.get(table + "/@tablets")
        total_store_count = sum(tablet["statistics"]["store_count"] for tablet in tablets)
        total_partition_count = sum(tablet["statistics"]["partition_count"] for tablet in tablets)
        ratio = float(total_store_count) / float(total_partition_count)

        logging.info(
            "[%s] Stores: %s, Partitions: %s, Ratio: %.2f",
            table, total_store_count, total_partition_count, ratio)

        if ratio < tolerance_after_compaction + EPS:
            if now < first_compaction_time + shuffle_time:
                shuffle_time_remaining = first_compaction_time + shuffle_time - now
                logging.info(
                    "[%s] Awaiting for data shuffle to complete (estimated shuffle time remaining is %.0fs)",
                    table, shuffle_time_remaining)
                time.sleep(shuffle_time_remaining)
            logging.info("[%s] Done", table)
            return
        elif ratio > 1.5:
            time.sleep(12.0 * sleep_quantum)
        elif ratio > 1.2:
            time.sleep(3.0 * sleep_quantum)
        else:
            time.sleep(1.0 * sleep_quantum)


def gradually_compact_tables(
        tolerance_before_compaction, tolerance_after_compaction, network_bandwidth,
        sleep_quantum, epochs_to_skip, compaction_timeout):
    # Compute current epoch.
    yt.set("//tmp/@gradually_compact_tables", None)
    yt.remove("//tmp/@gradually_compact_tables")
    cluster_epoch = (yt.get("//tmp/@revision") >> 32)

    # Compute number of nodes.
    cluster_size = min(yt.get("//sys/tablet_cells/@count"), yt.get("//sys/nodes/@count"))

    tables = yt.search("/", node_type="table", attributes=["uncompressed_data_size"])
    tables = sorted(tables, key=lambda table: table.attributes.get("uncompressed_data_size"), reverse=True)

    for table in tables:
        try:
            gradually_compact_table(
                cluster_epoch=cluster_epoch,
                cluster_size=cluster_size,
                table=str(table),
                tolerance_before_compaction=tolerance_before_compaction,
                tolerance_after_compaction=tolerance_after_compaction,
                network_bandwidth=network_bandwidth,
                sleep_quantum=sleep_quantum,
                epochs_to_skip=epochs_to_skip,
                compaction_timeout=compaction_timeout)
        except yt.errors.YtError:
            logging.error("[%s] Failed", table, exc_info=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tolerance-before-compaction", metavar="E", type=float, default=1.00,
                        help="Minimal stores-to-partitions ratio to start compaction (relative; 1.0+)")
    parser.add_argument("--tolerance-after-compaction", metavar="E", type=float, default=1.05,
                        help="Maximal stores-to-partitions ratio to finish compaction (relative; 1.0+)")
    parser.add_argument("--network-bandwidth", metavar="S", type=float, default=50.0,
                        help="Network bandwidth per node available for compaction (megabytes per second)")
    parser.add_argument("--sleep-quantum", metavar="T", type=float, default=5.0,
                        help="Sleep quantum for polls")
    parser.add_argument("--epochs-to-skip", metavar="N", type=int, default=5,
                        help="Do not compact tablets that were compacted within last N epochs")
    parser.add_argument("--compaction-timeout", metavar="T", type=float, default=3600.0,
                        help="Full compaction timeout (or maximum time per table to be compacted)")
    parser.add_argument("--silent", action="store_true",
                        help="Do not log anything")
    args = parser.parse_args()

    if args.silent:
        logging.basicConfig(level=logging.ERROR)
    else:
        logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)

    if args.tolerance_before_compaction < 1.0:
        logging.error(
            "Tolerance before compaction must be >= 1.0 (actual: %.2f)",
            args.tolerance_before_compaction)
        sys.exit(1)

    if args.tolerance_after_compaction < 1.0:
        logging.error(
            "Tolerance after compaction must be >= 1.0 (actual: %.2f)",
            args.tolerance_after_compaction)
        sys.exit(1)

    gradually_compact_tables(
        tolerance_before_compaction=args.tolerance_before_compaction,
        tolerance_after_compaction=args.tolerance_after_compaction,
        network_bandwidth=args.network_bandwidth,
        sleep_quantum=args.sleep_quantum,
        epochs_to_skip=args.epochs_to_skip,
        compaction_timeout=args.compaction_timeout)
