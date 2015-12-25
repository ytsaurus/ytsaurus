#!/usr/bin/python

import yt.wrapper as yt
import yt.yson as yson
from yt.common import YtError
from yt.wrapper.client import Yt
from yt.wrapper.http import get_token
from yt.wrapper.native_driver import make_request

import os
import sys
import argparse
import subprocess as sp
import itertools as it
from StringIO import StringIO
from time import sleep
from copy import deepcopy
from random import randint, shuffle

yt.config.VERSION = "v3"
yt.config.http.HEADER_FORMAT="yson"
#yt.config.http.TOKEN = "your token"

####################################################################################################

# Maximum number or rows passed to yt insert.
MAX_ROWS_PER_INSERT = 20000
# Maximum number of output rows
OUTPUT_ROW_LIMIT = 100000000
# Maximum number of input rows
INPUT_ROW_LIMIT = 100000000
# Mapper job options.
JOB_COUNT = 100
# Maximum number of simultaneously running jobs.
USER_SLOTS = 100
# Maximum amount of memory allowed for a job
JOB_MEMORY_LIMIT = "4GB"
# Maximum number of failed jobs which doensn't imply operation failure.
MAX_FAILDED_JOB_COUNT = 10
# Maximum number of retries to call YT command
MAX_RETRY_COUNT = 10
# Sleep interval between retries
SLEEP_INTERVAL = 120
# Attribute prefix
ATTRIBUTE_PREFIX = "_yt_dump_restore_"

####################################################################################################

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
    return yson.dumps(value, boolean_as_string=False)[1:-1]

# Check that table is mounted
def mounted(path):
    return all(x["state"] == "mounted" for x in yt.get(path + "/@tablets"))

# Get nice yson format
def yson_format():
    return yt.YsonFormat(format="text", boolean_as_string=False, process_table_index=False)

####################################################################################################

# Schema altering helper.
class Schematizer:
    def __init__(self, schema):
        self.type_map = {}
        for column in schema:
            self.type_map[column["name"]] = column["type"]

    def schematize(self, row):
        for key, value in row.iteritems():
            if row[key] is None:
                pass
            elif self.type_map[key] == "int64":
                row[key] = yson.YsonInt64(value)
            elif self.type_map[key] == "uint64":
                row[key] = yson.YsonUint64(value)
            elif self.type_map[key] == "double":
                row[key] = yson.YsonDouble(value)
            elif self.type_map[key] == "boolean":
                row[key] = yson.YsonBoolean(value)
            else:
                row[key] = yson.YsonString(value)
        return row

####################################################################################################

# Mapper - get tablet partition pivot keys.
def collect_pivot_keys_mapper(tablet):
    yield {"pivot_key":tablet["pivot_key"]}
    tablet_id = tablet["tablet_id"]
    cell_id = tablet["cell_id"]
    node = yson.loads(sp.check_output(["yt", "get", "#" + cell_id + "/@peers/0/address", "--format <format=text>yson"]))
    partitions_path = "//sys/nodes/%s/orchid/tablet_cells/%s/tablets/%s/partitions" % (node, cell_id, tablet_id)
    partitions = sp.check_output(["yt", "get", partitions_path, "--format <format=text>yson"])
    if partitions != None:
        for partition in yson.loads(partitions):
            yield {"pivot_key": partition["pivot_key"]}

# Base mapper class to hold command line arguments 
class ArgsMapper(object):
    def __init__(self, args):
        self.input_row_limit = args.input_row_limit
        self.output_row_limit = args.output_row_limit
        self.sleep_interval = args.sleep_interval
        self.max_retry_count = args.max_retry_count
        self.rows_per_insert = args.insert_size

    def make_request(self, command, params, data, client):
        errors = []
        attempt = 0
        while attempt < self.max_retry_count:
            try:
                return make_request(command, params, data=data, client=client)
            except YtError as error:
                errors.append((attempt, str(error)))
                sleep(randint(1, self.sleep_interval))
        errors = ["try: %s\nerror:%s\n" % (attempt, err) for attempt, err in errors]
        errors = [e +  "\n\n===================================================================\n\n" for e in errors]
        stderr = "".join(errors)
        print >> sys.stderr, stderr
        raise Exception(" ".join(("Failed to execute command (%s attempts):" % attempt, command, str(params))))

# Mapper - output rows with keys between r["left"] and r["right"].
class DumpMapper(ArgsMapper):
    def __init__(self, args, key_columns, schema, source):
        super(DumpMapper, self).__init__(args)
        self.key_columns = key_columns
        self.select_columns = ",".join([x["name"] for x in schema if "expression" not in x.keys()])
        self.source = source

    def __call__(self, r):
        config = {"driver_config_path": "/etc/ytdriver.conf", "api_version": "v3"}
        client = Yt(config=config)

        # Get something like ((key1, key2, key3), (bound1, bound2, bound3)) from a bound.
        get_bound_value = lambda bound : ",".join([yson.dumps(x, yson_format="text") for x in bound])
        get_bound_key = lambda width : ",".join([str(x) for x in self.key_columns[:width]])
        expand_bound = lambda bound : (get_bound_key(len(bound)), get_bound_value(bound))

        # Get records from source table.
        def query(left, right):
            left = "(%s) >= (%s)" % expand_bound(left) if left != None else None
            right = "(%s) < (%s)" % expand_bound(right) if right != None else None
            bounds = [x for x in [left, right] if x is not None]
            where = (" where " + " and ".join(bounds)) if len(bounds) > 0 else ""
            query = "%s from [%s]%s" % (self.select_columns, self.source, where)
            params = {
                "query": query,
                "input_row_limit": self.input_row_limit,
                "output_row_limit": self.output_row_limit,
                "output_format": "yson"
            }
            return self.make_request("select_rows", params, None, client)

        count = 0
        raw_data = StringIO(query(r["left"], r["right"]))
        for row in yson.load(raw_data, yson_type="list_fragment"):
            ++count
            yield row
        yt.write_statistics({"processed_rows": count})

# Mapper - insert input rows into the destination table.
@yt.aggregator
class RestoreMapper(ArgsMapper):
    def __init__(self, args, destination, schema):
        super(RestoreMapper, self).__init__(args)
        self.destination = destination
        self.schematizer = Schematizer(schema)

    def __call__(self, rows):
        config = {"driver_config_path": "/etc/ytdriver.conf", "api_version": "v3"}
        client = Yt(config=config)

        # Insert data into the destination table.
        new_data = []
        def dump_data():
            params = {
                "path": self.destination,
                "input_format": "yson"
            }
            self.make_request("insert_rows", params, prepare(new_data), client)
            yt.write_statistics({"processed_rows": len(new_data)})             
            del new_data[:]

        # Process data.
        for row in rows:
            # FIXME for now we use schematization because TM may change uint64 to int64.
            row = self.schematizer.schematize(row)

            new_data.append(row)
            if len(new_data) > self.rows_per_insert:
                dump_data()
        dump_data()

        # Dummy yield to disguise self as a generator object.
        if False:
            yield None

####################################################################################################

# Save dynamic table attributes.
def save_attributes(dst, schema, key_columns, pivot_keys):
    yt.set(dst + "/@" + ATTRIBUTE_PREFIX + "schema", schema)
    yt.set(dst + "/@" + ATTRIBUTE_PREFIX + "key_columns", key_columns)
    yt.set(dst + "/@" + ATTRIBUTE_PREFIX + "pivot_keys", pivot_keys)

# Restore dynamic table attributes.
def restore_attributes(src):
    schema = yt.get(src + "/@" + ATTRIBUTE_PREFIX + "schema")
    key_columns = yt.get(src + "/@" + ATTRIBUTE_PREFIX + "key_columns")
    pivot_keys = yt.get(src + "/@" + ATTRIBUTE_PREFIX + "pivot_keys")
    return schema, key_columns, pivot_keys

####################################################################################################

# Write source table partition bounds into partition_bounds_table
def get_partition_bounds(tablets, partition_bounds_table):
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
        yt.write_table(tablets_table, tablets, yson_format(), raw=False)
        try:
            yt.run_map(
                collect_pivot_keys_mapper,
                tablets_table,
                partitions_table,
                spec={"job_count": 100, "max_failed_job_count":10},
                format=yson_format())
        except YtError as e:
            print str(e)
            raise e
        partition_keys = yt.read_table(partitions_table, format=yson_format(), raw=False)
        partition_keys = [p["pivot_key"] for p in partition_keys]
        yt.remove(tablets_table)
        yt.remove(partitions_table)

    partition_keys = [list(it.takewhile(lambda x : x is not None, key)) for key in partition_keys]
    partition_keys = [key for key in partition_keys if len(key) > 0]
    partition_keys = sorted(partition_keys)
    print "Total %s partitions" % (len(partition_keys) + 1)

    # Write partition bounds into partition_bounds_table.
    regions = zip([None] + partition_keys, partition_keys + [None])
    regions = [{"left":r[0], "right":r[1]} for r in regions]
    shuffle(regions)
    yt.write_table(
        partition_bounds_table,
        regions,
        format=yson_format(),
        raw=False)

####################################################################################################

# Build map operation spec from command line args.
def build_spec_from_args(args):
    spec = {
        "enable_job_proxy_memory_control": False,
        "job_count": args.job_count,
        "max_failed_job_count": args.max_failed_job_count,
        "job_proxy_memory_control": False,
        "mapper": {"memory_limit": args.memory_limit},
        "resource_limits": {"user_slots": args.user_slots}}
    return spec

# Create destination table.
def create_destination(args, dst):
    if yt.exists(dst):
        if args.force:
            yt.remove(dst)
        else:
            raise Exception("Destination table exists. Use --force")
    yt.create_table(dst)

####################################################################################################

# Dump dynamic table rows into a static table
def dump_table(args):
    # Source and destination table paths.
    src = args.dump[0]
    dst = args.dump[1]

    # Source table schema.
    schema = yt.get(src + "/@schema")
    key_columns = yt.get(src + "/@key_columns")
    tablets = yt.get(src + "/@tablets")    
    pivot_keys = sorted([tablet["pivot_key"] for tablet in tablets])

    create_destination(args, dst)
    save_attributes(dst, schema, key_columns, pivot_keys)

    partition_bounds_table = yt.create_temp_table()
    get_partition_bounds(tablets, partition_bounds_table)

    yt.run_map(
        DumpMapper(args, key_columns, schema, src),
        partition_bounds_table,
        dst,
        spec=build_spec_from_args(args),
        format=yson_format())

    yt.remove(partition_bounds_table)

# Insert static table rows into a dynamic table
def restore_table(args):
    # Source and destination table paths.
    src = args.restore[0]
    dst = args.restore[1]

    schema, key_columns, pivot_keys = restore_attributes(src)

    create_destination(args, dst)
    yt.set(dst + "/@schema", schema)
    yt.set(dst + "/@key_columns", key_columns)
    yt.reshard_table(dst, pivot_keys)
    yt.mount_table(dst)
    while not mounted(dst):
        sleep(1)

    out_table = yt.create_temp_table()

    yt.run_map(
        RestoreMapper(args, dst, schema),
        src,
        out_table,
        spec=build_spec_from_args(args),
        format=yson_format())

    yt.remove(out_table)

####################################################################################################

def main():
    parser = argparse.ArgumentParser(description="Map-Reduce table manipulator.")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dump", nargs=2, metavar=('SOURCE', 'DESTINATION'), help="Copy table")
    mode.add_argument("--restore", nargs=2, metavar=('SOURCE', 'DESTINATION'), help="Copy table")
    parser.add_argument("--force", action="store_true", help="Overwrite destination table if it exists")
    parser.add_argument("--proxy", type=yt.config.set_proxy, help="YT proxy")
    parser.add_argument("--job_count", type=int, default=JOB_COUNT, help="Numbser of jobs in copy task")
    parser.add_argument("--user_slots", type=int, default=USER_SLOTS, help="Maximum number of simultaneous jobs running")
    parser.add_argument("--max_failed_job_count", type=int, default=MAX_FAILDED_JOB_COUNT, help="Maximum number of failed jobs")
    parser.add_argument("--memory_limit", type=parse_size, default=JOB_MEMORY_LIMIT, help="Memory limit for a copy task")
    parser.add_argument("--insert_size", type=int, default=MAX_ROWS_PER_INSERT, help="Number of rows passed to 'yt insert' call")
    parser.add_argument("--input_row_limit", type=int, default=INPUT_ROW_LIMIT, help="Limit the input of 'yt select' call")
    parser.add_argument("--output_row_limit", type=int, default=OUTPUT_ROW_LIMIT, help="Limit the output of 'yt select' call")
    parser.add_argument("--max_retry_count", type=int, default=MAX_RETRY_COUNT, help="Maximum number of 'yt' call retries")
    parser.add_argument("--sleep_interval", type=int, default=SLEEP_INTERVAL, help="Sleep interval between 'yt' call retries")
    args = parser.parse_args()

    if args.dump:
        dump_table(args)
    elif args.restore:
        restore_table(args)

if __name__ == "__main__":
    main()

