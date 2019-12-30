#!/usr/bin/python

import argparse
import logging
import sys
import yt.wrapper as yt

logger = logging.getLogger(__name__)

args = None

def get_table_kind(input_table_path):
    return "ordered_by_trace_id" if "ordered_by_trace_id" in input_table_path else "ordered_normally"


def remerge_tables(input_table_paths):
    logger.info("Remerging %d tables", len(input_table_paths))

    to_move = []

    with yt.OperationsTrackerPool(args.pool_size, poll_period=args.pool_poll_period) as tracker:
        for input_table_path in input_table_paths:
            output_table_path = input_table_path + ".static"
            schema = yt.get(input_table_path + "/@schema")
            table_kind = get_table_kind(output_table_path)
            logger.debug("Creating table %s with schema %s and setting log tailer attributes on it as for order kind %s", output_table_path, schema, table_kind)
            if not args.dry_run:
                yt.create("table", output_table_path, attributes={"schema": schema}, force=True)
                yt.clickhouse.set_log_tailer_table_attributes(table_kind, output_table_path, 7 * 24 * 60 * 60 * 1000)

            spec_builder = yt.spec_builders.MergeSpecBuilder() \
                .input_table_paths(input_table_path) \
                .output_table_path(output_table_path) \
                .mode("ordered") \
                .begin_job_io() \
                    .table_writer({"block_size": 256 * 1024, "desired_chunk_size": 100 * 1024**2}) \
                ._end_job_io()

            if not args.dry_run:
                tracker.add(spec_builder)

            input_table_backup_path = input_table_path + args.backup_suffix

            to_move += [(input_table_path, input_table_backup_path),
                        (output_table_path, input_table_path)]

    for src_path, dst_path in to_move:
        logger.debug("Moving %s to %s", src_path, dst_path)
        if not args.dry_run:
            yt.move(src_path, dst_path)


    for input_table_path in input_table_paths:
        logger.debug("Altering table %s to dynamic", input_table_path)
        if not args.dry_run:
            yt.alter_table(input_table_path, dynamic=True)

        logger.debug("Resharding table %s", input_table_path)
        if not args.dry_run:
            yt.clickhouse.set_log_tailer_table_dynamic_attributes(table_kind, input_table_path)

    logger.info("Remerged %d tables", len(input_table_paths))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--pool-size", type=int, default=10, help="Maximum number of concurrently running merge operations")
    parser.add_argument("--pool-poll-period", type=int, default=1000, help="Poll period in ms")
    parser.add_argument("--dry-run", action="store_true", help="Only log what is going to happen")
    parser.add_argument("-v", "--verbose", action="store_true", help="Print lots of debugging information")
    parser.add_argument("--backup-suffix", default=".bak", help="Suffix for backing up input tables")
    parser.add_argument("--remerge-tables", action="store_true", help="Remerge tables")
    args = parser.parse_args()

    if args.verbose:
        logger.setLevel(logging.DEBUG)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter("%(asctime)s %(levelname)s\t%(message)s")
        handler.setFormatter(formatter)
        logger.handlers.append(handler)

    input_table_paths = list(map(str.strip, sys.stdin.readlines()))

    for input_table_path in input_table_paths:
        logger.debug("Unmounting table %s", input_table_path)
        if not args.dry_run:
            yt.unmount_table(input_table_path, sync=True)

    if args.remerge_tables:
        remerge_tables(input_table_paths)
    else:
        for input_table_path in input_table_paths:
            logger.debug("Setting up parameters on %s", input_table_path)
            if not args.dry_run:
                table_kind = get_table_kind(input_table_path)
                yt.clickhouse.set_log_tailer_table_attributes(table_kind, input_table_path, 7 * 24 * 60 * 60 * 1000)
                yt.clickhouse.set_log_tailer_table_dynamic_attributes(table_kind, input_table_paths)

    for input_table_path in input_table_paths:
        logger.debug("Mounting table %s", input_table_path)
        if not args.dry_run:
            yt.mount_table(input_table_path, sync=True)
