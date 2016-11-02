#!/usr/bin/env python

from yt.wrapper.common import parse_bool
import yt.wrapper as yt

import argparse
import logging
import time
from threading import Thread


def do_action_batched(action, tables, args, **kwargs):
    cmd = None
    if action == "mount":
        cmd = "mount_table"
    elif action == "unmount":
        cmd = "unmount_table"
    elif action == "remount":
        cmd = "remount_table"
    assert cmd is not None

    reqs = []
    for table in tables:
        reqs.append({"command": cmd, "parameters": {"path": table}})

    rsps = yt.execute_batch(reqs)
    for table, rsp in zip(table, rsps):
        if "error" in rsp:
            logging.error("%s -> %r", table, rsp)

def do_action(action, tables, args, **kwargs):
    def _mount(table, **kwargs):
        logging.info("Mounting table %s", table)
        try:
            yt.mount_table(table, **kwargs)
            return True
        except:
            logging.exception("Unable to unmount table %s", table)
            return False

    def _unmount(table, **kwargs):
        logging.info("Unmounting table %s", table)
        try:
            yt.unmount_table(table, **kwargs)
            return True
        except:
            logging.exception("Unable to unmount table %s", table)
            return False

    def _remount(table, **kwargs):
        logging.info("Remounting table %s", table)
        try:
            yt.remount_table(table, **kwargs)
            return True
        except:
            logging.exception("Unable to unmount table %s", table)
            return False

    ops = []
    if action == "mount":
        ops.append([_mount, "mounted"])
    if action == "unmount":
        ops.append([_unmount, "unmounted"])
    if action == "remount":
        ops.append([_remount, "mounted"])
    if action == "unmount_mount":
        ops.append([_unmount, "unmounted"])
        ops.append([_mount, "mounted"])

    skipped_tables = []
    success_tables = []
    failed_tables = []

    check_state = lambda: all(tablet["state"] == state for tablet in yt.get(table + "/@tablets"))
    for fn, state in ops:
        for table in tables:
            if check_state():
                skipped_tables.append(table)
                continue
            if args.read_only:
                yt.set(table + "/@read_only", True)
            if args.read_write:
                yt.remove(table + "/@read_only", force=True)
            ok = fn(table, **kwargs)
            if ok:
                success_tables.append(table)
            else:
                failed_tables.append(table)
        for table in success_tables:
            while not check_state():
                logging.info("Waiting for table %s tablets to become %s", table, state)
                time.sleep(1)

    logging.info("Done; skipped %s tables, succeded in %s tables, failed in %s tables",
                 len(skipped_tables), len(success_tables), len(failed_tables))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=("mount", "unmount", "remount", "unmount_mount"))
    parser.add_argument("--thread-count", type=int, default=20)
    parser.add_argument("--force", action="store_true", default=False,
                        help="Do not wait for transactions and flushes while unmounting")
    parser.add_argument("--silent", action="store_true", default=False,
                        help="Do not log anything")
    parser.add_argument("--read-only", action="store_true", default=False,
                        help="Set read-only mode for table")
    parser.add_argument("--read-write", action="store_true", default=False,
                        help="Set read-write mode for table")
    parser.add_argument("--path", default="/", help="Path to operate on")

    args = parser.parse_args()

    if args.silent:
        logging.basicConfig(level=logging.ERROR)
    else:
        logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)

    if args.action == "unmount" or args.action == "unmount_mount":
        kwargs = {"force": args.force}
    else:
        kwargs = {}

    logging.info("Operating on %s", args.path)

    tables = []
    for table in yt.search(args.path, node_type="table", attributes=["dynamic"]):
        dynamic = parse_bool(table.attributes.get("dynamic", "false"))
        if not dynamic:
            continue
        tables.append(str(table))

    tables_per_thread = 1 + (len(tables) / args.thread_count)

    threads = []
    for thread_index in xrange(args.thread_count):
        start_index = thread_index * tables_per_thread
        end_index = min(len(tables), (thread_index + 1) * tables_per_thread)
        if start_index >= end_index:
            break
        tables_for_thread = tables[start_index:end_index]
        thread = Thread(target=do_action, args=(args.action, tables_for_thread, args), kwargs=kwargs)
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()


if __name__ == "__main__":
    main()
