#!/usr/bin/python

import argparse
import copy
import re
import time
import logging

import yt.wrapper as yt
import yt.yson as yson


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

    try:
        has_tablets = "tablets" in yt.list(table + "/@")
        if has_tablets:
            logging.info("Unmounting %s", table)
            yt.unmount_table(table)
            while not all(x["state"] == "unmounted" for x in yt.get(table + "/@tablets")):
                logging.info("Waiting for tablets to become unmounted...")
                time.sleep(1)

        logging.info("Resharding %s", table)
        yt.reshard_table(table, pivot_keys)
    finally:
        logging.info("Mounting %s", table)
        yt.mount_table(table)
        while not all(x["state"] == "mounted" for x in yt.get(table + "/@tablets")):
            logging.info("Waiting for tablets to become mounted...")
            time.sleep(1)

    logging.info("Done")


def main(args):
    if args.action == "manual":
        process_manual(args.table, args.shards, args.yes)
    elif args.action == "auto":
        process_auto(args.table, args.include, args.exclude, args.root,
                     args.desired_size_gbs, args.desired_slack, args.in_memory_only, args.yes)


def process_manual(table, shards, yes=False):
    reshard(table, shards, yes)


def process_auto(tables, include_regexps=[], exclude_regexps=[], root="/",
                 desired_size_gbs=None, desired_slack=1.15, in_memory_only=False, yes=False):
    tables = copy.deepcopy(tables)

    if root != "/" and len(tables) == 0 and len(include_regexps) == 0 and len(exclude_regexps) == 0:
        include_regexps.append(".*")

    if len(include_regexps) + len(exclude_regexps) > 0:
        for table in yt.search(root, node_type="table", attributes=["dynamic", "in_memory_mode"]):
            dynamic = table.attributes.get("dynamic", False)
            in_memory_mode = table.attributes.get("in_memory_mode", "none")
            if not dynamic:
                continue
            if in_memory_only and in_memory_mode == "none":
                continue
            tables.append(str(table))

    if len(tables) == 0:
        logging.error("You must specify at least one table with either `--table` or `--include`/`--exclude` or `--root`")
        logging.info("Root: %r", root)
        logging.info("Include Regexps: %r", include_regexps)
        logging.info("Exclude Regexps: %r", exclude_regexps)
        return

    for table in tables:
        logging.info("Processing %s", table)

        table_attributes = yt.get(table + "/@")
        tablets = yt.get(table + "/@tablets")

        in_memory_mode = table_attributes.get("in_memory_mode", "none")

        if not desired_size_gbs:
            if in_memory_mode == "none":
                desired_size_gbs = 80.0
            else:
                desired_size_gbs = 1.0

        if in_memory_mode == "compressed":
            desired_size_kind = "compressed"
        else:
            desired_size_kind = "uncompressed"

        sizes = [tablet["statistics"]["%s_data_size" % desired_size_kind] for tablet in tablets]
        slacks = [float(size) / float(desired_size_gbs * GB) for size in sizes]

        if all(slack < desired_slack for slack in slacks):
            logging.info("Table %s is well-enough balanced (slack: %.2f .. %.2f); skipping",
                         table, min(slacks), max(slacks))
            continue

        shards = int(1.0 + float(sum(sizes)) / float(desired_size_gbs * GB))

        try:
            if shards > len(tablets):
                logging.info("Resharding table %s from %d shards to %d shards (slack: %.2f .. %.2f)",
                             table, len(tablets), shards, min(slacks), max(slacks))
                reshard(table, shards, yes)
            else:
                logging.info("Skipping table %s due to natural disbalance (slack: %.3f .. %.2f)",
                             table, min(slacks), max(slacks))
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

    manual_parser = subparsers.add_parser("manual", help="reshard table manually")
    manual_parser.add_argument("--table", type=str, required=True, help="Table to reshard")
    manual_parser.add_argument("--shards", type=int, required=True, help="Target number of shards")
    manual_parser.set_defaults(action="manual")

    auto_parser = subparsers.add_parser("auto", help="reshard tables automatically")
    auto_parser.add_argument("--table", action="append", type=str,
                             help="Add a single table to task list")
    auto_parser.add_argument("--include", metavar="REGEXP", action="append", type=str,
                             help="Add tables matching regular expression to task list")
    auto_parser.add_argument("--exclude", metavar="REGEXP", action="append", type=str,
                             help="Remove tables matching regular expression from task list")
    auto_parser.add_argument("--root", metavar="ROOT", type=str,
                             help="Root path to use for --include/--exclude search")
    auto_parser.add_argument("--in-memory-only", action="store_true", default=False,
                             help="Process only in-memory tables")
    auto_parser.add_argument("--size-gbs", dest="desired_size_gbs", metavar="N", type=float, required=False, default=None,
                             help="Desired tablet size (in GBs)")
    auto_parser.add_argument("--slack", dest="desired_slack", metavar="S", type=float, required=False, default=1.15,
                             help="Allowed slack (1 = no slack; 1.15 = default)")
    auto_parser.set_defaults(table=[], include=[], exclude=[], root="/", action="auto")

    args = parser.parse_args()

    if args.silent:
        logging.basicConfig(level=logging.ERROR)
    else:
        logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)

    main(args)
