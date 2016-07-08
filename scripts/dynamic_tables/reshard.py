#!/usr/bin/python

"""
This script reshards given table into a particular number of tablets, taking into
account several important factors:

    * _Column type_. In particular, script distinguishes between signed and unsigned
    integers.
    * _Column expression_. In particular, script is capable of devising shards
    for modulo columns.
"""

import argparse
import re
import time
import logging

import yt.wrapper as yt
import yt.yson as yson

yt.config.VERSION = "v3"
yt.config.http.HEADER_FORMAT = "yson"


GB = 1024 * 1024 * 1024


def make_pivot_keys(ty, expr, shards=1):
    logging.info("Type: %s ; Expression: %s ; Shards: %s", ty, expr, shards)
    modulo = None
    if expr is not None:
        match = re.match(r"^.*%\s*(\d+)u?$", expr)
        if match:
            modulo = long(match.group(1))
    if modulo is None:
        if ty == "int64":
            modulo = 2L**63L
        elif ty == "uint64":
            modulo = 2L**64L
        else:
            raise RuntimeError("Unsupported type %s for sharding column" % ty)
    if ty == "int64":
        cls = yson.YsonInt64
        shift = long(modulo)
        span = 2L * long(modulo)
    elif ty == "uint64":
        cls = yson.YsonUint64
        shift = 0L
        span = long(modulo)
    else:
        raise RuntimeError("Unsupported type %s for sharding column" % ty)
    logging.info("Modulo: %s ; Shift: %s ; Span: %s", modulo, shift, span)
    return [[]] + [[cls(i * span / shards - shift)] for i in xrange(1, shards)]


def reshard(table, shards=None, yes=False):
    pivot_schema = yt.get(table + "/@schema/0")
    pivot_keys = make_pivot_keys(
        pivot_schema.get("type", None),
        pivot_schema.get("expression", None),
        shards)

    logging.info("Resharding into %s tablets", len(pivot_keys))

    if not yes:
        fmt = lambda x: "'" + yson.dumps(x)[1:-1] + "'"
        print "yt reshard_table '%s' %s" % (table, " ".join(map(fmt, pivot_keys)))
        logging.info("`--yes` was not specified; exiting.")
        return

    logging.info("You have 5 seconds to press Ctrl+C to abort...")
    time.sleep(5)

    has_tablets = "tablets" in yt.list(table + "/@")
    if has_tablets:
        logging.info("Unmounting %s", table)
        yt.unmount_table(table)
        while not all(x["state"] == "unmounted" for x in yt.get(table + "/@tablets")):
            logging.info("Waiting for tablets to become unmounted...")
            time.sleep(1)

    logging.info("Resharding %s", table)
    yt.reshard_table(table, pivot_keys)

    logging.info("Mounting %s", table)
    yt.mount_table(table)
    while not all(x["state"] == "mounted" for x in yt.get(table + "/@tablets")):
        logging.info("Waiting for tablets to become mounted...")
        time.sleep(1)

    logging.info("Done")


def main(args):
    if args.action == "one":
        process_one(args)
    elif args.action == "many":
        process_many(args)


def process_one(args):
    reshard(args.table, args.shards, args.yes)


def process_many(args):
    tables = []
    for table in args.table:
        tables.append(table)

    if len(args.include) + len(args.exclude) > 0:
        for table in yt.search("/", node_type="table", attributes=["dynamic", "in_memory_mode"]):
            dynamic = table.attributes.get("dynamic", False)
            in_memory_mode = table.attributes.get("in_memory_mode", "none")
            if not dynamic:
                continue
            if args.in_memory_only and in_memory_mode == "none":
                continue
            tables.append(str(table))

    if len(tables) == 0:
        logging.error("You must specify at least one table with either `--table` or `--include`/`--exclude`")
        return

    for table in tables:
        logging.info("Processing %s", table)

        table_attributes = yt.get(table + "/@")
        tablets = yt.get(table + "/@tablets")

        in_memory_mode = table_attributes.get("in_memory_mode", "none")

        desired_size_gbs = args.desired_size_gbs
        if desired_size_gbs == 0:
            if in_memory_mode == "none":
                desired_size_gbs = 80
            else:
                desired_size_gbs = 1

        desired_size_kind = "uncompressed"
        if in_memory_mode == "compressed":
            desired_size_kind = "compressed"

        sizes = [tablet["statistics"]["%s_data_size" % desired_size_kind] for tablet in tablets]

        if all(size < 3 * desired_size_gbs * GB / 2 for size in sizes):
            logging.info("Table %s is well-enough balanced; skipping", table)
            continue

        shards = 1 + sum(sizes) / (desired_size_gbs * GB)

        try:
            logging.info("Resharding table %s into %d shards", table, shards)
            reshard(table, shards, args.yes)
        except KeyboardInterrupt:
            raise
        except:
            import traceback
            traceback.print_exc()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--silent", action="store_true", help="Do not log anything")
    parser.add_argument("--yes", action="store_true", help="Actually do something (do nothing by default)")

    subparsers = parser.add_subparsers()

    one_parser = subparsers.add_parser("one", help="reshard one table")
    one_parser.add_argument("--table", type=str, required=True, help="Table to reshard")
    one_parser.add_argument("--shards", type=int, required=True, help="Target number of shards")
    one_parser.set_defaults(action="one")

    many_parser = subparsers.add_parser("many", help="reshard many tables")
    many_parser.add_argument("--table", action="append", type=str,
                             help="Add a single table to task list")
    many_parser.add_argument("--include", metavar="REGEXP", action="append", type=str,
                             help="Add tables matching regular expression to task list")
    many_parser.add_argument("--exclude", metavar="REGEXP", action="append", type=str,
                             help="Remove tables matching regular expression from task list")
    many_parser.add_argument("--in-memory-only", action="store_true", default=False,
                             help="Process only in-memory tables")
    many_parser.add_argument("--desired-size-gbs", metavar="N", type=long, required=False, default=0,
                             help="Desired tablet size (in GBs)")
    many_parser.set_defaults(table=[], include=[], exclude=[], action="many")

    args = parser.parse_args()

    if args.silent:
        logging.basicConfig(level=logging.ERROR)
    else:
        logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)

    main(args)
