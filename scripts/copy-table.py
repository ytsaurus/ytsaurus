#!/usr/bin/python

import yt.wrapper as yt
import yt.yson as yson
from yt.common import YtError

import sys
import argparse
import subprocess as sp
import itertools as it
from StringIO import StringIO
from time import sleep
from copy import deepcopy

yt.config.VERSION = "v3"

# Config is passed to mapper.
CONFIG_FILE_NAME = "copy-table.config"
# Maximum number or rows passed to yt insert.
MAX_ROWS_PER_INSERT = 20000
# Maximum number of output rows
OUTPUT_ROW_LIMIT = 100000000
# Mapper job options.
JOB_COUNT = 20
JOB_MEMORY_LIMIT = "4GB"
MAX_FAILDED_JOB_COUNT = 10

# Simple transformation class - just copy everything.
class Copy:
    # Get schema for the new table
    @staticmethod
    def schema(schema):
        return schema

    # Get key columns for the new table
    @staticmethod
    def key_columns(key_columns):
        return key_columns

    # Get list of columns to select
    @staticmethod
    def select_columns(schema):
        return ",".join([x["name"] for x in schema if "expression" not in x.keys()])

    # Get pivot keys for the new table
    @staticmethod
    def pivot_keys(pivot_keys):
        return pivot_keys

    # Get row for the new table
    @staticmethod
    def row(row):
        return row

# Rehash transformation class: make 'hash' column to be a computed column.
class Rehash:
    @staticmethod
    def schema(schema):
        assert schema[0]["name"] == "hash"
        assert schema[1]["name"] == "date"
        schema[0]["type"] = "int64"
        schema[0]["expression"] = "int64(farm_hash(date))"
        return schema
    @staticmethod
    def key_columns(key_columns):
        return key_columns
    @staticmethod
    def select_columns(schema):
        return ",".join([x["name"] for x in schema if "expression" not in x.keys()])
    @staticmethod
    def pivot_keys(pivot_keys):
        shard_count = 50
        pivot_keys = [[]] + [[int((i * 2**64) / shard_count - 2**63)] for i in xrange(1, shard_count)]
        return pivot_keys
    @staticmethod
    def row(row):
        return row

# Transformation object.
Transform = Copy()

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
    node = yson.loads(sp.check_output(["yt", "get", "--format", "<format=text>yson", "#" + cell_id + "/@peers/0/address"]))
    partitions_path = "//sys/nodes/%s/orchid/tablet_cells/%s/tablets/%s/partitions" % (node, cell_id, tablet_id)
    partitions = sp.check_output(["yt", "get", "--format", "<format=text>yson", partitions_path])
    if partitions != None:
        for partition in yson.loads(partitions):
            yield {"pivot_key": partition["pivot_key"]}

# Mapper - copy content with keys between r["left"] and r["right"].
def regions_mapper(r):
    # Read config.
    f = open(CONFIG_FILE_NAME, 'r')
    config = yson.load(f)

    # Get something like ((key1, key2, key3), (bound1, bound2, bound3)) from a bound.
    get_bound_value = lambda bound : ",".join([yson.dumps(x, yson_format="text") for x in bound])
    get_bound_key = lambda width : ",".join([str(x) for x in config["key_columns"][:width]])
    expand_bound = lambda bound : (get_bound_key(len(bound)), get_bound_value(bound))

    # Get records from source table.
    def query(left, right):
        left = "(%s) >= (%s)" % expand_bound(left) if left != None else None
        right = "(%s) < (%s)" % expand_bound(right) if right != None else None
        bounds = [x for x in [left, right, config.get("select_where")] if x is not None]
        where = (" where " + " and ".join(bounds)) if len(bounds) > 0 else ""
        query = "%s from [%s]%s" % (config["select_columns"], config["source"], where)
        return sp.check_output(["yt", "select_rows", "--output_row_limit", str(config["output_row_limit"]),"--format", "<format=text>yson",  query]) 
    raw_data = StringIO(query(r["left"], r["right"]))

    # Insert data into destination table.
    insert_cmd = ["yt", config["command"], "--format", "<format=text>yson", config["destination"]]
    new_data = []
    def dump_data():
        p = sp.Popen(insert_cmd, stdin=sp.PIPE)
        p.communicate(prepare(new_data))
        if p.returncode != 0:
            raise sp.CalledProcessError(p.returncode, insert_cmd, p.stdout)
        del new_data[:]

    # Process data.
    for row in yson.load(raw_data, yson_type="list_fragment"):
        new_data.append(Transform.row(row))
        if len(new_data) > config["rows_per_insert"]: dump_data()
    dump_data()

    # Mapper has to be a generator.
    if False: yield None

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Map-Reduce table manipulator.")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--copy", nargs=2, metavar=('SOURCE', 'DESTINATION'), help="Copy table")
    mode.add_argument("--delete", metavar=('SOURCE'), help="Delete rows from table")
    parser.add_argument("--force", action="store_true", help="Overwrite destination table if it exists")
    parser.add_argument("--proxy", type=yt.config.set_proxy, help="YT proxy")
    parser.add_argument("--job_count", type=int, default=JOB_COUNT, help="Numbser of jobs in copy task")
    parser.add_argument("--max_failed_job_count", type=int, default=MAX_FAILDED_JOB_COUNT, help="Maximum number of failed jobs")
    parser.add_argument("--memory_limit", type=parse_size, default=JOB_MEMORY_LIMIT, help="Memory limit for a copy task")
    parser.add_argument("--insert_size", type=int, default=MAX_ROWS_PER_INSERT, help="Number of rows passed to 'yt insert' call")
    parser.add_argument("--output_row_limit", type=int, default=OUTPUT_ROW_LIMIT, help="Limit the output of 'yt select' call")
    parser.add_argument("--where", type=str, help="Additional predicate for 'yt select'")
    args = parser.parse_args()

    src = args.copy[0] if args.copy else args.delete
    dst = args.copy[1] if args.copy else None
    
    if dst and yt.exists(dst):
        if args.force:
            yt.remove(dst)
        else:
            raise Exception("Destination table exists. Use --force")

    yt.mount_table(src)
    while not all(x["state"] == "mounted" for x in yt.get(src + "/@tablets")):
        sleep(1)

    tablets = yt.get(src + "/@tablets")

    # Get pivot keys. For a large number of tablets use map-reduce version.
    # Tablet pivots are merged with partition pivots
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
        yt.write_table(tablets_table, tablets, format=yt.YsonFormat(format="text"), raw=False)
        try:
            yt.run_map(
                tablets_mapper,
                tablets_table,
                partitions_table,
                spec={"job_count": 100, "max_failed_job_count":10},
                format=yt.YsonFormat(format="text"))
        except YtError as e:
            print yt.errors.format_error(e)
            raise e
        partition_keys = yt.read_table(partitions_table, format=yt.YsonFormat(format="text"), raw=False)
        partition_keys = [p["pivot_key"] for p in partition_keys]
        yt.remove(tablets_table)   
        yt.remove(partitions_table)

    partition_keys = [list(it.takewhile(lambda x : x is not None, key)) for key in partition_keys]
    partition_keys = [key for key in partition_keys if len(key) > 0]
    partition_keys = sorted(partition_keys)
    print "Total %s partitions" % len(partition_keys)

    regions_table = yt.create_temp_table()
    out_regions_table = yt.create_temp_table()

    # Write partition bounds into regions_table.
    regions = zip([None] + partition_keys, partition_keys + [None])
    regions = [{"left":r[0], "right":r[1]} for r in regions]
    yt.write_table(
        regions_table,
        regions,
        format=yt.YsonFormat(format="text"), raw=False)

    schema = yt.get(src + "/@schema")
    key_columns = yt.get(src + "/@key_columns")

    # Config passed to mapper.
    config = {
        "key_columns": deepcopy(key_columns),
        "source": src,
        "rows_per_insert": args.insert_size,
        "output_row_limit": args.output_row_limit}
    if (args.where): config["select_where"] = args.where
    def write_config(config):
        with open(CONFIG_FILE_NAME, "w") as config_file: config_file.write(yson.dumps(config))

    spec={
        "job_count": args.job_count,
        "max_failed_job_count": args.max_failed_job_count,
        "job_proxy_memory_control": False,
        "mapper": { "memory_limit": args.memory_limit }}
    
    # Copy tablet pivot keys from source table.
    pivot_keys = sorted([tablet["pivot_key"] for tablet in tablets])
    #if len(pivot_keys) > 1 and pivot_keys[1][0] == -2**63: pivot_keys = pivot_keys[0] + pivot_keys[2:]

    if args.copy:
        # Create destination table.
        yt.create_table(dst)
        yt.set_attribute(dst, "schema", Transform.schema(schema))
        yt.set_attribute(dst, "key_columns", Transform.key_columns(key_columns))
        yt.reshard_table(dst, Transform.pivot_keys(pivot_keys))
        yt.mount_table(dst)
        while not all(x["state"] == "mounted" for x in yt.get(dst + "/@tablets")):
            sleep(1)

        config["destination"] = dst
        config["command"] = "insert_rows"
        config["select_columns"] = Transform.select_columns(schema)
        write_config(config)

        # Copy table. Each mapper task copies a single partition.
        try:
            yt.run_map(
                regions_mapper,
                regions_table,
                out_regions_table,
                spec=spec,
                format=yt.YsonFormat(format="text"),
                local_files=CONFIG_FILE_NAME)
        except YtError as e:
            print yt.errors.format_error(e)
            raise e
    else:
        config["destination"] = src
        config["command"] = "delete_rows"
        config["select_columns"] = ",".join([x["name"] for x in schema if "expression" not in x.keys() and x["name"] in key_columns])
        write_config(config)

        # Delete rows from table. Each mapper task deletes from a single partition.
        try:
            yt.run_map(
                regions_mapper,
                regions_table,
                out_regions_table,
                spec=spec,
                format=yt.YsonFormat(format="text"),
                local_files=CONFIG_FILE_NAME)
        except YtError as e:
            print yt.errors.format_error(e)
            raise e

    yt.remove(regions_table)   
    yt.remove(out_regions_table)

