#!/usr/bin/env python

from yt.wrapper.common import parse_bool
import yt.wrapper as yt

import sys
import argparse
from threading import Thread
from copy import deepcopy

def do_action(tables, action, **kwargs):
    for table in tables:
        action(table, **kwargs)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("action")
    parser.add_argument("--force", action="store_true", default=False)
    parser.add_argument("--thread-count", type=int, default=20)
    args = parser.parse_args()

    if args.action not in ["mount", "unmount", "unmount_mount"]:
        print >>sys.stderr, "Usage: {} {{mount|unmount|unmount_mount}}".format(sys.args[0])
        sys.exit(1)

    if args.action == "mount":
        action = yt.mount_table
        kwargs = {}
    if args.action == "unmount":
        action = yt.unmount_table
        kwargs = {"force": args.force}
    if args.action == "unmount_mount":
        def func(table, **kwargs):
            yt.unmount_table(table, **kwargs)
            yt.mount_table(table)
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
