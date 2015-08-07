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
            raise RuntimeError("Cannot infer modulo")
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


def reshard(table, shards, yes=False):
    pivot_schema = yt.get(table + "/@schema/0")
    pivot_keys = make_pivot_keys(pivot_schema.get("type", None), pivot_schema.get("expression", None), shards)

    if not yes:
        fmt = lambda x: "'" + yson.dumps(x)[1:-1] + "'"
        print "yt reshard_table '%s' %s" % (table, " ".join(map(fmt, pivot_keys)))
        logging.info("`--yes` was not specified; exiting.")
        return

    logging.info("You have 5 seconds to press Ctrl+C to abort...")
    time.sleep(5)

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

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--table", type=str, required=True, help="Table to reshard")
    parser.add_argument("--shards", type=int, required=True, help="Target number of shards")
    parser.add_argument("--silent", action="store_true", help="Do not log anything")
    parser.add_argument("--yes", action="store_true", help="Actually do something (do nothing by default)")
    args = parser.parse_args()
    if args.silent:
        logging.basicConfig(level=logging.ERROR)
    else:
        logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)
    reshard(args.table, args.shards, args.yes)
