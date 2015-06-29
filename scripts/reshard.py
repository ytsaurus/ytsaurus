#!/usr/bin/python

import argparse
import re
import time

import yt.wrapper as yt
import yt.yson as yson

yt.config.VERSION = "v3"
yt.config.http.HEADER_FORMAT = "yson"


def make_pivot_keys(ty, expr, shards=1):
    print "Type: %s ; Expression: %s ; Shards: %s" % (ty, expr, shards)
    if expr is not None:
        match = re.match(r"^.*%\s*(\d+)u?$", expr)
        if match:
            modulo = long(match.group(1))
        elif ty == "int64":
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
    print "Modulo: %s ; Shift: %s ; Span: %s" % (modulo, shift, span)
    return [[]] + [[cls(i * span / shards - shift)] for i in xrange(1, shards)]

def reshard(table=None, shards=1, dump=False):
    pivot_schema = yt.get(table + "/@schema/0")
    pivot_keys = make_pivot_keys(pivot_schema.get("type", None), pivot_schema.get("expression", None), shards)

    if table is None or dump:
        print " ".join("'%s'" % yson.dumps(key)[1:-1] for key in pivot_keys)
        return

    print "Unmounting..."
    yt.unmount_table(table)
    while not all(x["state"] == "unmounted" for x in yt.get(table + "/@tablets")):
        time.sleep(1)

    print "Resharding..."
    yt.reshard_table(table, pivot_keys)

    print "Mounting..."
    yt.mount_table(table)
    while not all(x["state"] == "mounted" for x in yt.get(table + "/@tablets")):
        time.sleep(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Copy table with tablets.")
    parser.add_argument("--proxy", type=yt.config.set_proxy, help="YT proxy")
    parser.add_argument("--table", type=str, required=True, help="Table to reshard")
    parser.add_argument("--shards", type=int, required=True, help="Number of shards")
    parser.add_argument("--dump", action="store_true", help="Only dump pivots, don't reshard")
    args = parser.parse_args()
    reshard(args.table, args.shards, args.dump)
