#!/usr/bin/python

import argparse
import logging
import sys
import fnmatch
import os.path
import functools
import yt.wrapper as yt
import collections
import time

logger = logging.getLogger(__name__)

args = None

def get_table_kind(input_table_path):
    return "ordered_by_trace_id" if "ordered_by_trace_id" in input_table_path else "ordered_normally"


def remerge_tables(input_table_paths):
    logger.info("Remerging %d tables", len(input_table_paths))

    to_move = []

    attrs = yt.batch_apply(lambda path, client: client.get(path + "/@", attributes=["log_tailer_version", "schema"]), input_table_paths)

    def create_destination_table(input, client=None):
        input_table_path, table_attrs = input
        table_kind = get_table_kind(input_table_path)
        output_table_path = input_table_path + ".static"
        schema = table_attrs.get("schema")
        log_tailer_version = table_attrs.get("log_tailer_version")
        logger.debug("Creating table %s with schema %s and setting log tailer attributes on it as for order kind %s, log tailer version = %s", output_table_path, schema, table_kind, log_tailer_version)
        return client.create("table", output_table_path, attributes={"schema": schema}, force=True)

    logger.info("Creating static tables")

    if not args.dry_run:
        yt.batch_apply(create_destination_table, zip(input_table_paths, attrs))

    batch_client = yt.create_batch_client(raise_errors=True)

    logger.info("Setting attributes on static tables")

    if not args.dry_run:
        for input_table_path, table_attrs in zip(input_table_paths, attrs):
            output_table_path = input_table_path + ".static"
            table_kind = get_table_kind(input_table_path)
            log_tailer_version = table_attrs.get("log_tailer_version")
            yt.clickhouse.set_log_tailer_table_attributes(table_kind, output_table_path, 7 * 24 * 60 * 60 * 1000,
                                                      log_tailer_version=log_tailer_version, client=batch_client)
        batch_client.commit_batch()

    logger.info("Running merge operations")

    with yt.OperationsTrackerPool(args.pool_size, poll_period=args.pool_poll_period) as tracker:
        for input_table_path in input_table_paths:
            output_table_path = input_table_path + ".static"
            spec_builder = yt.spec_builders.MergeSpecBuilder() \
                .input_table_paths(input_table_path) \
                .output_table_path(output_table_path) \
                .mode("ordered") \
                .pool(args.operation_pool) \
                .begin_job_io() \
                    .table_writer({"block_size": 256 * 1024, "desired_chunk_size": 100 * 1024**2}) \
                ._end_job_io()

            if not args.dry_run:
                tracker.add(spec_builder)

    logger.info("Replacing old tables with new ones")

    if not args.dry_run:
        def move_table(input_table_path, client=None):
            output_table_path = input_table_path + ".static"
            return client.move(output_table_path, input_table_path, force=True)

        yt.batch_apply(move_table, input_table_paths)

    logger.info("Altering tables to dynamic")

    if not args.dry_run:
        yt.batch_apply(lambda input_table_path, client: client.alter_table(input_table_path, dynamic=True), input_table_paths)

    batch_reshard_tables(input_table_paths)

    batch_mount(input_table_paths)

    logger.info("Remerged %d tables", len(input_table_paths))


def batch_wait_tablet_state(input_table_paths, target_state):
    logger.info("Waiting for common tablet state '%s'", target_state)

    if not args.dry_run:
        MAX_ITERS = 600
        SLEEP_PERIOD = 1

        for i in xrange(MAX_ITERS):
            def get_tablet_state(path, client=None):
                return yt.get(path + "/@tablet_state", client=client)
            tablet_states = yt.batch_apply(get_tablet_state, input_table_paths)
            state_counter = collections.Counter(tablet_states)
            logger.info("State counters are %s", state_counter)

            if state_counter.keys() == [target_state]:
                break

            for tablet_state, input_table_path in zip(tablet_states, input_table_paths):
                if tablet_state != target_state:
                    logger.debug("Table %s is in state %s", input_table_path, tablet_state)

            time.sleep(SLEEP_PERIOD)

    logger.info("Common tablet state is '%s'", target_state)


def batch_unmount(input_table_paths):
    logger.info("Unmounting tables")

    if not args.dry_run:
        yt.batch_apply(yt.unmount_table, input_table_paths)

    batch_wait_tablet_state(input_table_paths, "unmounted")


def batch_mount(input_table_paths):
    logger.info("Mounting tables")

    if not args.dry_run:
        yt.batch_apply(yt.mount_table, input_table_paths)

    batch_wait_tablet_state(input_table_paths, "mounted")


def batch_reshard_tables(input_table_paths):
    logger.info("Resharding tables")

    if not args.dry_run:
        batch_client = yt.create_batch_client(raise_errors=True)
        for input_table_path in input_table_paths:
            table_kind = get_table_kind(input_table_path)
            yt.clickhouse.reshard_log_tailer_table(table_kind, input_table_path, sync=False, client=batch_client)
        batch_client.commit_batch()

    batch_wait_tablet_state(input_table_paths, "unmounted")


def reset_attributes(input_table_paths):
    logger.info("Resetting attributes on %d tables", len(input_table_paths))

    batch_unmount(input_table_paths)

    attrs = yt.batch_apply(lambda path, client: client.get(path + "/@", attributes=["log_tailer_version"]), input_table_paths)

    batch_client = yt.create_batch_client(raise_errors=True)

    logger.info("Setting table attributes")

    for attr, input_table_path in zip(attrs, input_table_paths):
        log_tailer_version = attr.get("log_tailer_version")
        logger.debug("Setting up parameters on %s, log tailer version = %s", input_table_path, log_tailer_version)

        if not args.dry_run:
            table_kind = get_table_kind(input_table_path)
            yt.clickhouse.set_log_tailer_table_attributes(table_kind, input_table_path, 7 * 24 * 60 * 60 * 1000,
                                                          log_tailer_version=log_tailer_version, client=batch_client)
            yt.clickhouse.set_log_tailer_table_dynamic_attributes(table_kind, input_table_path, client=batch_client)

    batch_client.commit_batch()

    batch_reshard_tables(input_table_paths)

    batch_mount(input_table_paths)

    logger.info("Attributes reset")



def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--from-stdin", action="store_true", help="Read table list from stdin")
    parser.add_argument("--operation-pool", type=str, default="babenko", help="Pool to run operations in")
    parser.add_argument("--pool-size", type=int, default=10, help="Maximum number of concurrently running merge operations")
    parser.add_argument("--pool-poll-period", type=int, default=1000, help="Poll period in ms")
    parser.add_argument("--dry-run", action="store_true", help="Only log what is going to happen")
    parser.add_argument("-v", "--verbose", action="store_true", help="Print lots of debugging information")
    parser.add_argument("--reset-attributes", action="store_true", help="Reset attributes")
    parser.add_argument("--remerge-tables", action="store_true", help="Remerge tables")
    parser.add_argument("--mount-tables", action="store_true", help="Mount all possibly unmounted tables")


    global args
    args = parser.parse_args()

    if args.verbose:
        logger.setLevel(logging.DEBUG)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter("%(asctime)s %(levelname)s\t%(message)s")
        handler.setFormatter(formatter)
        logger.handlers.append(handler)

    if args.from_stdin:
        input_table_paths = map(str.strip, sys.stdin.readlines())
    else:
        input_table_paths = yt.search("//sys/clickhouse/kolkhoz", path_filter=lambda path: fnmatch.fnmatch(os.path.basename(path), "clickhouse*log"))
    input_table_paths = list(input_table_paths)

    if args.mount_tables:
        if yt.get("//sys/clickhouse/kolkhoz/manage_log_tables_lock/@lock_count") > 0:
            logger.info("There is a lock on //sys/clickhouse/kolkhoz/manage_log_tables_lock, doing nothing")
        else:
            batch_mount(input_table_paths)

    if args.reset_attributes or args.remerge_tables:
        lock_tx = yt.Transaction()
        with yt.Transaction(transaction_id=lock_tx.transaction_id):
            logger.info("Locking //sys/clickhouse/kolkhoz/manage_log_tables_lock")
            yt.lock("//sys/clickhouse/kolkhoz/manage_log_tables_lock")

        success = False

        try:
            if args.reset_attributes:
                reset_attributes(input_table_paths)
            if args.remerge_tables:
                remerge_tables(input_table_paths)
            success = True
        finally:
            if success:
                logger.info("Committing lock transaction")
                lock_tx.commit()
            else:
                logger.info("Aborting lock transaction")
                lock_tx.abort()


if __name__ == "__main__":
    main()
