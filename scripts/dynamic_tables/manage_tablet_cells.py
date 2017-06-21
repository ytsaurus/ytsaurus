#!/usr/bin/env python

import yt.wrapper as yt

import argparse
import logging
import time
import sys
from threading import Thread


def do_create(count, attributes):
    for _ in xrange(count):
        tablet_cell = yt.create("tablet_cell", attributes=attributes)
        logging.info("Created tablet cell %s", tablet_cell)


def do_remove(tablet_cells):
    for tablet_cell in tablet_cells:
        logging.info("Removing tablet cell %s", tablet_cell)
        yt.remove("//sys/tablet_cells/%s" % tablet_cell)

def execute_batch(reqs):
    rsps = []
    step = 100
    for i in xrange(0, len(reqs), step):
        rsps += yt.execute_batch(reqs[i:i+step])
    return rsps

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=("create", "remove"))
    parser.add_argument("--count", type=int, default=None,
                        help="How many tablet cells to create?")
    parser.add_argument("--replication-factor", "--RF", type=int, default=5,
                        help="Changelog replication factor for newly created tablet cells")
    parser.add_argument("--read-quorum", "--RQ", type=int, default=3,
                        help="Changelog read quorum for newly created tablet cells")
    parser.add_argument("--write-quorum", "--WQ", type=int, default=3,
                        help="Changelog write quorum for newly created tablet cells")
    parser.add_argument("--bundle", "--B", type=str, default="default",
                        help="Assigns to specified tablet cell bundle")
    parser.add_argument("--thread-count", type=int, default=20,
                        help="Number of worker threads")
    parser.add_argument("--silent", action="store_true", default=False,
                        help="Do not log anything")
    args = parser.parse_args()

    if args.silent:
        logging.basicConfig(level=logging.ERROR)
    else:
        logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)

    threads = []

    if args.action == "create":
        if args.count is None:
            logging.error("You must specify `--count` when creating tablet cells")
            sys.exit(1)

        logging.info("Will create %s tablet cells; sleeping for 5 seconds...", args.count)
        time.sleep(5)

        attributes = {
            "changelog_replication_factor": args.replication_factor,
            "changelog_read_quorum": args.read_quorum,
            "changelog_write_quorum": args.write_quorum,
            "tablet_cell_bundle": args.bundle,
        }

        tablet_cells_per_thread = 1 + args.count / args.thread_count
        for thread_index in xrange(args.thread_count):
            start_index = thread_index * tablet_cells_per_thread
            end_index = min(args.count, (thread_index + 1) * tablet_cells_per_thread)
            if start_index >= end_index:
                break
            tablet_cells_for_thread = end_index - start_index
            thread = Thread(target=do_create, args=(tablet_cells_for_thread, attributes))
            thread.start()
            threads.append(thread)
    elif args.action == "remove":
        tablet_cells = yt.list("//sys/tablet_cells")

        if args.bundle:
            reqs = []
            for cell in tablet_cells:
                reqs.append({"command": "get", "parameters": {"path": "#" + cell + "/@tablet_cell_bundle"}})
            rsps = execute_batch(reqs)
            filtered = []
            for index in xrange(len(reqs)):
                rsp = rsps[index]
                if "error" in rsp:
                    logging.error("%r", rsp)
                else:
                    if rsp["output"] == args.bundle:
                        filtered.append(tablet_cells[index])
            tablet_cells = filtered

        logging.info("Will remove %s tablet cells; sleeping for 5 seconds...", len(tablet_cells))
        time.sleep(5)

        tablet_cells_per_thread = 1 + (len(tablet_cells) / args.thread_count)
        for thread_index in xrange(args.thread_count):
            start_index = thread_index * tablet_cells_per_thread
            end_index = min(len(tablet_cells), (thread_index + 1) * tablet_cells_per_thread)
            if start_index >= end_index:
                break
            tablet_cells_for_thread = tablet_cells[start_index:end_index]
            thread = Thread(target=do_remove, args=(tablet_cells_for_thread,))
            thread.start()
            threads.append(thread)

    for thread in threads:
        thread.join()

if __name__ == "__main__":
    main()
