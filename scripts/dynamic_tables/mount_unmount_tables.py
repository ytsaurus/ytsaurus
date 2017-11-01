#!/usr/bin/python
import yt.wrapper as yt
import sys
import logging
import argparse

def execute_batch(reqs):
    rsps = []
    step = 100
    for i in xrange(0, len(reqs), step):
        rsps += yt.execute_batch(reqs[i:i+step])
    return rsps

def get_mounted_tables(bundle=None):
    if args.bundle is None:
        tablet_cells_path = "//sys/tablet_cells"
    else:
        tablet_cells_path = "//sys/tablet_cell_bundles/{0}/@tablet_cell_ids".format(bundle)
    tables = set()
    reqs = []
    for tablet_cell in yt.get(tablet_cells_path):
        reqs.append({"command": "get", "parameters": {"path": "#" + tablet_cell + "/@tablet_ids"}})
    rsps = execute_batch(reqs)
    tablets = []
    for rsp in rsps:
        if "error" in rsp:
            logging.error("%r", rsp)
        else:
            tablets += rsp["output"]
    reqs = []
    for tablet in tablets:
        reqs.append({"command": "get", "parameters": {"path": "#" + tablet + "/@table_id"}})
    rsps = execute_batch(reqs)
    tables = set()
    for rsp in rsps:
        if "error" in rsp:
            logging.error("%r", rsp)
        else:
            tables.add(rsp["output"])
    return list(tables)

def get_table_paths(table_ids):
    reqs = []
    for table in table_ids:
        reqs.append({"command": "get", "parameters": {"path": "#" + table + "/@path"}})
    rsps = execute_batch(reqs)
    result = []
    for rsp in rsps:
        if "error" in rsp:
            logging.error("%r", rsp)
        else:
            result.append(rsp["output"])
    return result

def action(tables, command):
    reqs = []
    for table in tables:
        reqs.append({"command": command, "parameters": {"path": "#" + table}})
    rsps = execute_batch(reqs)
    for table, rsp in zip(tables, rsps):
        if "error" in rsp:
            logging.error("%s -> %r", table, rsp)

def unmount(tables):
    action(tables, "unmount_table")

def mount(tables):
    action(tables, "mount_table")

def remount(tables):
    action(tables, "remount_table")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Mount-unmount dynamic tables.")
    parser.add_argument("command", type=str, help="Action to perform (mount, unmount or remount)")
    parser.add_argument("--tables", type=str, help="File with tables")
    parser.add_argument("--bundle", type=str, default=None, help="Unmount tables from specific bunle")
    args = parser.parse_args()

    if args.command == "remount":
        tables = []
        if args.tables is None:
            tables = get_mounted_tables(args.bundle)
        else:
            with open(args.tables, "r") as f:
                for table in f.readlines():
                    tables.append(table.strip())
        remount(tables)
    elif args.command == "list":
        tables = get_mounted_tables(args.bundle)
        paths = get_table_paths(tables)
        for path in paths:
            print path
    elif args.command == "unmount":
        if args.tables is None:
            raise Exception("--tables argument is required")
        with open(args.tables, "a") as f:
            tables = get_mounted_tables(args.bundle)
            for table in tables:
                f.write(table + "\n")
            f.close()
            unmount(tables)
    elif args.command == "mount":
        tables = []
        with open(args.tables, "r") as f:
            for table in f.readlines():
                tables.append(table.strip())
        mount(tables)
    else:
        raise Exception("Unknown command: %s" % (args.command))

