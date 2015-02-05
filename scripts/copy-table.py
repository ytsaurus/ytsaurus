#!/usr/bin/python
import yt.wrapper as yt
import yt.yson as yson
from yt.common import YtError
import argparse
import subprocess as sp
import sys
import tempfile
import os

yt.config.VERSION="v3"

# Config is passed to mapper.
config_file_name="copy-table.config"

def prepare(value, is_raw=False):
    if not is_raw:
        if not isinstance(value, list):
            value = [value]
        value = yson.dumps(value)
        # remove surrounding [ ]
        value = value[1:-1]
    return value

# Map task - get tablet partition pivot keys.
def tablets_mapper(tablet):
    yield {"pivot_key":tablet["pivot_key"]}
    tablet_id = tablet["tablet_id"]
    cell_id = tablet["cell_id"]
    p = sp.Popen(["yt", "get", "#" + cell_id + "/@peers/0/address", "--format <format=text>yson"], stdout=sp.PIPE)
    node = yson.loads(p.communicate()[0])
    p = sp.Popen(["yt", "get", "//sys/nodes/%s/orchid/tablet_cells/%s/tablets/%s/partitions" %(node, cell_id, tablet_id), "--format <format=text>yson"], stdout=sp.PIPE)
    partitions = p.communicate()[0]
    if partitions != None:
        for partition in yson.loads(partitions):
            yield {"pivot_key":partition["pivot_key"]}

# Map task - copy content with keys between r["left"] and r["right"].
def regions_mapper(r):
    f = open(config_file_name, 'r')
    config = yson.loads(f.read())
    
    # Get something like ((key1, key2, key3), (bound1, bound2, bound3)) from a bound.
    S = lambda l : ",".join(['"' + x + '"' if isinstance(x, str) else str(x) for x in l])
    SS = lambda w : ",".join([str(x) for x in config["key_columns"][:w]])
    SSS = lambda l : (SS(len(l)), S(l))
    
    # Get records from source table.
    def query(left, right):
        if left == None: left = None
        if right == None: right = None
        if left: left = " (%s) >= (%s)" % SSS(left)
        if right: right = " (%s) < (%s)" % SSS(right)
        query = "* from [%s]" % config["source"]
        if left or right: query += " where"
        if left: query += left
        if left and right: query += " and"
        if right: query += right
        p = sp.Popen(["yt", "select", query, "--format <format=text>yson"], stdout=sp.PIPE)
        data, err = p.communicate()
        return data
    raw_data = query(r["left"], r["right"])

    # Process data.
    #
    #data = yson.loads("[%s]" % raw_data)
    #new_data = []
    #for row in data:
    #    #
    #    # possible data transformation here
    #    #
    #    new_data.append(row)
    #raw_data = prepare(new_data)

    # Write data to destination table.
    p = sp.Popen(["yt", "insert", config["destination"], "--format <format=text>yson"], stdin=sp.PIPE)
    p.communicate(raw_data)
    yield r

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Copy and alter table.")
    parser.add_argument("--input")
    parser.add_argument("--output")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--proxy")
    args = parser.parse_args()
    
    if args.proxy != None:
        yt.config.set_proxy(args.proxy)

    src = args.input
    dst = args.output
    
    if yt.exists(dst):
        if args.force:
            yt.remove(dst)
        else:
            raise Exception("Destination table exists. Use --force")

    schema = yt.get(src + "/@schema")
    key_columns = yt.get(src + "/@key_columns")
    tablets = yt.get(src + "/@tablets")

    # Get pivot keys. For a large number of tablets use map-reduce version.
    # Tablet pivots are merget with partition pivots
    print "Prepare partition keys:"
    partition_keys = []
    if len(tablets) < 10:
        tablet_idx = 0
        for tablet in tablets:
            partition_keys.append(tablet["pivot_key"])
            tablet_idx += 1
            sys.stdout.write("\rTablet %s of %s" % (tablet_idx, len(tablets)))
            sys.stdout.flush()
            tablet_id = tablet["tablet_id"]
            cell_id = tablet["cell_id"]
            node = yt.get("#" + cell_id + "/@peers/0/address")
            for partition in yt.get("//sys/nodes/%s/orchid/tablet_cells/%s/tablets/%s/partitions" %(node, cell_id, tablet_id)):
                partition_keys.append(partition["pivot_key"])
        print ""
    else:
        tablets_table = yt.create_temp_table()
        partitions_table = yt.create_temp_table()
        yt.write_table(tablets_table, prepare(tablets), format=yt.YsonFormat(format="text"))
        try:
            yt.run_map(
                tablets_mapper,
                tablets_table,
                partitions_table,
                spec={"job_count": 100, "max_failed_job_count":10},
                format=yt.YsonFormat(format="text"))
        except YtError as e:
            print yt.errors.format_error(e)
        partition_keys = yt.read_table(partitions_table, format=yt.YsonFormat(format="text"))
        partition_keys = yson.loads("[%s]" % "".join(partition_keys))
        partition_keys = [p["pivot_key"] for p in partition_keys]
        yt.remove(tablets_table)   
        yt.remove(partitions_table)

    partition_keys = sorted([key for key in partition_keys if len(key) > 0])
    print "Total %s partitions" % len(partition_keys)

    regions_table = yt.create_temp_table()
    out_regions_table = yt.create_temp_table()

    # Write partition bounds into regions_table.
    regions = zip([None] + partition_keys, partition_keys + [None])
    yt.write_table(regions_table, prepare([{"left":r[0], "right":r[1]} for r in regions]), format=yt.YsonFormat(format="text"))

    config = {"key_columns" : key_columns, "source": src, "destination" : dst}
    config_file = open(config_file_name, "w")
    config_file.write(yson.dumps(config))
    config_file.close()
    
    # Copy tablet pivot keys from source table.
    pivot_keys = []
    for tablet in tablets:
        key = tablet["pivot_key"]
        pivot_keys.append(key)
    pivot_keys = sorted(pivot_keys)

    yt.create_table(dst)
    yt.set_attribute(dst, "schema", schema)
    yt.set_attribute(dst, "key_columns", key_columns)
    yt.reshard_table(dst, pivot_keys)
    yt.mount_table(dst)

    # FIXME: fix job count and memory limit
    #job_count=min(100, len(pivot_keys))
    job_count=10

    # Copy table. Each mapper task copies a single partition.
    try:
        yt.run_map(
            regions_mapper,
            regions_table,
            out_regions_table,
            spec={
                "job_count": job_count,
                "max_failed_job_count":10,
                "job_proxy_memory_control":False,
                "mapper": { "memory_limit":1024*1024*1024*8}
            },
            format=yt.YsonFormat(format="text"),
            local_files=config_file_name)
    except YtError as e:
        print yt.errors.format_error(e)

    yt.remove(regions_table)   
    yt.remove(out_regions_table)

