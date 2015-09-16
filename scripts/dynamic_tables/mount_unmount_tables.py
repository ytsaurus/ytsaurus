#!/usr/bin/env python

from yt.wrapper.common import parse_bool
import yt.wrapper as yt

import argparse
import logging
import time
from threading import Thread

def remove(path):
    try:
        yt.remove(path)
    except yt.errors.YtResponseError as error:
        if error.error["code"] not in (500, 501):
            raise

def do_action(action, tables, args, **kwargs):
    def _mount(table, **kwargs):
        logging.info("Mounting table %s", table)
        yt.mount_table(table, **kwargs)

    def _unmount(table, **kwargs):
        logging.info("Unmounting table %s", table)
        yt.unmount_table(table, **kwargs)

    def _remount(table, **kwargs):
        logging.info("Remounting table %s", table)
        yt.remount_table(table, **kwargs)

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

    for fn, state in ops:
        for table in tables:
            if args.read_only:
                yt.set(table + "/@read_only", True)
            if args.read_write:
                remove(table + "/@read_only")

            fn(table, **kwargs)
        for table in tables:
            while not all(tablet["state"] == state for tablet in yt.get(table + "/@tablets")):
                logging.info("Waiting for table %s tablets to become %s", table, state)
                time.sleep(1)


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

    args = parser.parse_args()

    if args.silent:
        logging.basicConfig(level=logging.ERROR)
    else:
        logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)

    if args.action == "unmount" or args.action == "unmount_mount":
        kwargs = {"force": args.force}
    else:
        kwargs = {}

    tables = []
    for table in yt.search("/", node_type="table", attributes=["dynamic"]):
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
