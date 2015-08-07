#!/usr/bin/env python

from yt.wrapper.common import parse_bool
import yt.wrapper as yt

import argparse
import logging
import time
from threading import Thread
from copy import deepcopy


def do_action(tables, action, **kwargs):
    for table in tables:
        action(table, **kwargs)


def _mount_action(table, **kwargs):
    logging.info("Mounting table %s", table)
    yt.mount_table(table, **kwargs)


def _unmount_action(table, **kwargs):
    logging.info("Unmounting table %s", table)
    yt.unmount_table(table, **kwargs)


def _remount_action(table, **kwargs):
    logging.info("Remounting table %s", table)
    yt.remount_table(table, **kwargs)


def _await_action(table, **kwargs):
    while not all(tablet["state"] == kwargs["state"] for tablet in yt.get(table + "/@tablets")):
        logging.info("Waiting for table %s tablets to become %s", table, kwargs["state"])
        time.sleep(1)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=("mount", "unmount", "remount", "unmount_mount"))
    parser.add_argument("--thread-count", type=int, default=20)
    parser.add_argument("--force", action="store_true", default=False,
                        help="Do not wait for transactions and flushes while unmounting")
    parser.add_argument("--silent", action="store_true", default=False,
                        help="Do not log anything")
    args = parser.parse_args()

    if args.silent:
        logging.basicConfig(level=logging.ERROR)
    else:
        logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)

    if args.action == "mount":
        action = _mount_action
        kwargs = {}
    if args.action == "unmount":
        action = _unmount_action
        kwargs = {"force": args.force}
    if args.action == "remount":
        action = _remount_action
        kwargs = {}
    if args.action == "unmount_mount":
        def func(table, **kwargs):
            _unmount_action(table, **kwargs)
            _await_action(table, state="unmounted")
            _mount_action(table)
        action = func
        kwargs = {"force": args.force}

    tables = []
    for table in yt.search("/", node_type="table", attributes=["dynamic"]):
        dynamic = parse_bool(table.attributes.get("dynamic", "false"))
        if not dynamic:
            continue
        tables.append(str(table))

    tables_per_thread = 1 + (len(tables) / args.thread_count)

    threads = []
    for index in xrange(args.thread_count):
        start_index = index * tables_per_thread
        end_index = min(len(tables), (index + 1) * tables_per_thread)
        if start_index >= end_index:
            break
        tables_for_thread = tables[start_index:end_index]
        thread_kwargs = deepcopy(kwargs)
        thread_kwargs["tables"] = tables_for_thread
        thread_kwargs["action"] = action
        thread = Thread(target=do_action, kwargs=thread_kwargs)
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()


if __name__ == "__main__":
    main()
