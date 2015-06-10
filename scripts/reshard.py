#!/usr/bin/python

import yt.wrapper as yt
import yt.yson as yson

import argparse
from time import sleep

yt.config.VERSION = "v3"
yt.config.HEADER_FORMAT = "yson"

def reshard(path, shard_count, is_unsigned=False):
    delta = 0 if is_unsigned else 2**63
    cls = yson.YsonUint64 if is_unsigned else yson.YsonInt64
    pivot_keys = [[]] + [[cls(long((i * 2**64) / shard_count - delta))] for i in xrange(1, shard_count)]
    print "Unmounting..."
    yt.unmount_table(path)
    while not all(x["state"] == "unmounted" for x in yt.get(path + "/@tablets")):
        sleep(1)
    print "Resharding..."
    yt.reshard_table(path, pivot_keys)
    print "Mounting..."
    yt.mount_table(path)
    while not all(x["state"] == "mounted" for x in yt.get(path + "/@tablets")):
        sleep(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Copy table with tablets.")
    parser.add_argument("--proxy", type=yt.config.set_proxy, help="YT proxy")    
    parser.add_argument("--table", type=str, required=True, help="Table to reshard")
    parser.add_argument("--shard_count", type=int, required=True, help="Number of shards")
    parser.add_argument("--unsigned", type=bool, help="Make unsigned pivots")
    args = parser.parse_args()
    reshard(args.table, args.shard_count, args.unsigned)
