#!/usr/bin/python

import yt.wrapper as yt
import yt.yson as yson
from yt.common import YtError

import argparse
import subprocess as sp
import sys
from StringIO import StringIO

yt.config.VERSION = "v3"

# Config is passed to mapper.
CONFIG_FILE_NAME = "copy-table.config"
# Maximum number or rows passed to yt insert.
MAX_ROWS_PER_INSERT = 20000
# Mapper job options.
JOB_COUNT = 20
JOB_MEMORY_LIMIT = "2GB"

# Parse human-writable size.
def parse_size(size):
    scale = {"kb": 2**10, "mb": 2**20, "gb": 2**30}
    try:
        return int(float(size[:-2]) * scale[size[-2:].lower()]) if size[-2:].lower() in scale.keys() else int(size)
    except:
        raise ValueError("Invalid size: '%s'. Valid suffixes are: %s." %
            (size, ", ".join(["'%s'" % key for key in scale.keys()])))

# Prepare rows data.
def prepare(value):
    if not isinstance(value, list):
        value = [value]
    # remove surrounding [ ]
    return yson.dumps(value)[1:-1]

# Mapper - get tablet partition pivot keys.
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

# Mapper - copy content with keys between r["left"] and r["right"].
def regions_mapper(r):
    # Read config.
    f = open(CONFIG_FILE_NAME, 'r')
    config = yson.load(f)

    # Get something like ((key1, key2, key3), (bound1, bound2, bound3)) from a bound.
    get_bound_value = lambda bound : ",".join(['"' + x + '"' if isinstance(x, str) else str(x) for x in bound])
    get_bound_key = lambda width : ",".join([str(x) for x in config["key_columns"][:width]])
    expand_bound = lambda bound : (get_bound_key(len(bound)), get_bound_value(bound))

    # Get records from source table.
    def query(left, right):
        left = "(%s) >= (%s)" % expand_bound(left) if left != None else None
        right = "(%s) < (%s)" % expand_bound(right) if right != None else None
        bounds = [x for x in [left, right] if x is not None]
        where = (" where " + " and ".join(bounds)) if len(bounds) > 0 else ""
        query = "* from [%s]%s" % (config["source"], where)
        return sp.check_output(["yt", "select", query, "--format <format=text>yson"])
    raw_data = StringIO(query(r["left"], r["right"]))

    # Insert data into destination table.
    insert_cmd = ["yt", "insert", config["destination"], "--format <format=text>yson"]
    new_data = []
    def dump_data():
        p = sp.Popen(insert_cmd, stdin=sp.PIPE)
        p.communicate(prepare(new_data))
        if p.returncode != 0:
            raise sp.CalledProcessError(p.returncode, insert_cmd, p.stdout)
        del new_data[:]

    # Process data.
    for row in yson.load(raw_data, yson_type="list_fragment"):
        #
        # Possible data transformation here.
        #
        new_data.append(row)
        if len(new_data) > config["rows_per_insert"]: dump_data()
    dump_data()

    # YT Wrapper doesn't like empty output.
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

    # Config passed to mapper.
    config = {
        "key_columns": key_columns,
        "source": src,
        "destination": dst,
        "rows_per_insert": args.insert_size}
    with open(CONFIG_FILE_NAME, "w") as config_file: config_file.write(yson.dumps(config))
    
    # Copy tablet pivot keys from source table.
    pivot_keys = sorted([tablet["pivot_key"] for tablet in tablets])

    # Create destination table.
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
            local_files=CONFIG_FILE_NAME)
    except YtError as e:
        print yt.errors.format_error(e)

    yt.remove(regions_table)   
    yt.remove(out_regions_table)

