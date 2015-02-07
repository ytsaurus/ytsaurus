#!/usr/bin/python

import yt.wrapper as yt
import yt.yson as yson
from yt.common import YtError

import argparse
import subprocess as sp
import sys

yt.config.VERSION = "v3"

# Config is passed to mapper.
CONFIG_FILE_NAME = "copy-table.config"
# File with mapper program.
HELPER_FILE_NAME = "./copy-table-helper.py"
# Maximum number or rows passed to yt insert.
MAX_ROWS_PER_INSERT = 20000
# Mapper job options.
JOB_COUNT = 20
JOB_MEMORY_LIMIT = 1024*1024*1024*2

def parse_size(size):
    try:
        return int(size)
    except:
        scale = {"kb": 2**10, "mb": 2**20, "gb": 2**30}
        try:
            numeric = int(size[:-2])
            return (numeric if numeric > 0 else 1) * scale[size[-2:].lower()]
        except:
            raise ValueError("Invalid size: '%s'. Valid suffixes are: %s." %
                (size, ", ".join(["'%s'" % key for key in scale.keys()])))

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
    node = yson.loads(sp.check_output(["yt", "get", "#" + cell_id + "/@peers/0/address", "--format <format=text>yson"]))
    partitions_path = "//sys/nodes/%s/orchid/tablet_cells/%s/tablets/%s/partitions" % (node, cell_id, tablet_id)
    partitions = sp.check_output(["yt", "get", partitions_path, "--format <format=text>yson"])
    if partitions != None:
        for partition in yson.loads(partitions):
            yield {"pivot_key": partition["pivot_key"]}

# Map task - copy table.
def regions_mapper(r):
    f = open(CONFIG_FILE_NAME, 'r')
    config = yson.load(f)
    sp.check_output([HELPER_FILE_NAME, yson.dumps(config), yson.dumps(r)])
    yield r

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Copy table.")
    parser.add_argument("--input", required=True, help="Source table")
    parser.add_argument("--output", required=True, help="Destination table")
    parser.add_argument("--force", action="store_true", help="Overwrite destination table if it exists")
    parser.add_argument("--proxy", type=yt.config.set_proxy, help="YT proxy")
    parser.add_argument("--job_count", type=int, default=JOB_COUNT, help="Numbser of jobs in copy task")
    parser.add_argument("--memory_limit", type=parse_size, default=JOB_MEMORY_LIMIT, help="Memory limit for a copy task")
    parser.add_argument("--insert_size", type=int, default=MAX_ROWS_PER_INSERT, help="Number of rows passed to 'yt insert' call")
    args = parser.parse_args()

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
            for partition in yt.get("//sys/nodes/%s/orchid/tablet_cells/%s/tablets/%s/partitions" % (node, cell_id, tablet_id)):
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
        partition_keys = yt.read_table(partitions_table, format=yt.YsonFormat(), raw=False)
        partition_keys = [p["pivot_key"] for p in partition_keys]
        yt.remove(tablets_table)   
        yt.remove(partitions_table)

    partition_keys = sorted([key for key in partition_keys if len(key) > 0])
    print "Total %s partitions" % len(partition_keys)

    regions_table = yt.create_temp_table()
    out_regions_table = yt.create_temp_table()

    # Write partition bounds into regions_table.
    regions = zip([None] + partition_keys, partition_keys + [None])
    regions = [{"left":r[0], "right":r[1]} for r in regions]
    yt.write_table(
        regions_table,
        prepare(regions),
        format=yt.YsonFormat(format="text"))

    config = {
        "key_columns": key_columns,
        "source": src,
        "destination": dst,
        "rows_per_insert": args.insert_size}
    config_file = open(CONFIG_FILE_NAME, "w")
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

    # Copy table. Each mapper task copies a single partition.
    try:
        yt.run_map(
            regions_mapper,
            regions_table,
            out_regions_table,
            spec={
                "job_count": args.job_count,
                "max_failed_job_count": 10,
                "job_proxy_memory_control": False,
                "mapper": { "memory_limit": args.memory_limit }},
            format=yt.YsonFormat(format="text"),
            local_files=[CONFIG_FILE_NAME, HELPER_FILE_NAME])
    except YtError as e:
        print yt.errors.format_error(e)

    yt.remove(regions_table)   
    yt.remove(out_regions_table)

