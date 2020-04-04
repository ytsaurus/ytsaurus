#!/usr/bin/python

import yt.wrapper as yt
import yt.yson as yson
from yt.common import YtError
from yt.wrapper.client import Yt
from yt.wrapper.http import get_token

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
#yt.config.http.TOKEN = "your token"

# Config is passed to mapper.
CONFIG_FILE_NAME = "copy-table.config"
# Maximum number or rows passed to yt insert.
MAX_ROWS_PER_INSERT = 20000
# Maximum number of output rows
OUTPUT_ROW_LIMIT = 100000000
# Maximum number of input rows
INPUT_ROW_LIMIT = 100000000
# Mapper job options.
JOB_COUNT = 20
JOB_MEMORY_LIMIT = "4GB"
MAX_FAILDED_JOB_COUNT = 10
# Maximum number of retries to call YT command
MAX_RETRY_COUNT = 10
# Sleep interval between retries
SLEEP_INTERVAL = 120

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

# Check that table is mounted
def mounted(yt_client, path):
    return all(x["state"] == "mounted" for x in yt_client.get(path + "/@tablets"))

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

# Rehash transformation class: make 'hash' column to be a modulo computed column.
class ModuloRehash:
    def __init__(self):
        self._mod = 100
    def schema(self, schema):
        assert schema[0]["name"] == "YTHash"
        assert schema[1]["name"] == "UpdateTime"
        schema[0]["type"] = "int64"
        schema[0]["expression"] = "int64(farm_hash(UpdateTime, PageID)) %% %s" % self._mod
        return schema
    def key_columns(self, key_columns):
        return key_columns
    def select_columns(self, schema):
        return ",".join([x["name"] for x in schema if "expression" not in x.keys()])
    def pivot_keys(self, pivot_keys):
        shard_count = 100
        pivot_keys = [[]] + [[int(i * (self._mod * 2 + 1) / shard_count - self._mod)] for i in xrange(1, shard_count)]
        return pivot_keys
    def row(self, row):
        return row

# Transformation object.
Transform = Copy()

# Call subprocess with retries
def call_subprocess(command, stdin, max_retry_count, sleep_interval, env):
    errors = []
    def single_call():
        p = sp.Popen(command, stdin=sp.PIPE if stdin is not None else None, stdout=sp.PIPE, stderr=sp.PIPE, env=env)
        out, err = p.communicate(stdin)
        return p.returncode, out, err
    attempt = 0
    ret, out, err = single_call()
    errors.append((attempt, ret, out, err))
    while attempt < max_retry_count and ret != 0:
        attempt +=1
        sleep(randint(1, sleep_interval))
        ret, out, err = single_call()
        errors.append((attempt, ret, out, err))
    if ret == 0:
        return out
    else:
        errors = ["try: %s\nreturn code: %s\nstdout:\n%s\n\nstderr:\n%s\n" % (attempt_index, return_code, output, error) for attempt_index, return_code, output, error in errors]
        errors = [e +  "\n\n===================================================================\n\n" for e in errors]
        stderr = "".join(errors)
        #print >> sys.stderr, "stdin:\n%s\n" % stdin
        print >> sys.stderr, stderr
        raise sp.CalledProcessError(ret, " ".join(command), errors)

class Driver:
    def __init__(self, data):
        self._data = data
    def execute(self, command, stdin=None):
        env = deepcopy(os.environ)
        if not self._data["token"] == None:
            env["YT_TOKEN"] = str(self._data["token"])
        env["YT_VERSION"] = "v3"
        if self._data.get("disabled"):
            print >> sys.stderr, self._data["command"] + command
            print >> sys.stderr, env
            print >> sys.stderr, " ".join(self._data["command"] + command)
        else:
            return call_subprocess(
                self._data["command"] + command,
                stdin,
                self._data["max_retry_count"],
                self._data["sleep_interval"],
                env)
    def execute_select(self, query):
        return self.execute(self._data["select"] + [query])
    def execute_command(self, command, destination, rows):
        return self.execute(self._data[command] + [destination], rows)
    def disable(self):
        self._data["disabled"] = True
    def serialize(self):
        return self._data
    @staticmethod
    def deserialize(data):
        return Driver(data)
    @staticmethod
    def create(max_retry_count, sleep_interval, input_row_limit, output_row_limit, proxy=None, token=None):
        select_cmd = "select_rows" if not proxy else "select-rows"
        input_opt = "--input_row_limit" if not proxy else "--input-row-limit"
        output_opt = "--output_row_limit" if not proxy else "--output-row-limit"
        data = {
            "max_retry_count" : max_retry_count,
            "sleep_interval" : sleep_interval,
            "select": [select_cmd, input_opt, str(input_row_limit), output_opt, str(output_row_limit), "--format", "<format=text>yson"],
            "insert": ["insert_rows" if not proxy else "insert-rows", "--format", "<format=text>yson"],
            "delete": ["delete_rows" if not proxy else "delete-rows", "--format", "<format=text>yson"],
            "command": ["yt"] if not proxy else ["yt2", "--proxy", proxy],
            "token": token}
        return Driver(data)

# Mapper - get tablet partition pivot keys.
def tablets_mapper(tablet):
    # Read config.
    f = open(CONFIG_FILE_NAME, 'r')
    config = yson.load(f)
    driver = Driver.deserialize(config["driver_src"])

    yield {"pivot_key":tablet["pivot_key"]}
    tablet_id = tablet["tablet_id"]
    cell_id = tablet["cell_id"]
    node = yson.loads(driver.execute(["get", "--format", "<format=text>yson", "#" + cell_id + "/@peers/0/address"]))
    partitions_path = "//sys/nodes/%s/orchid/tablet_cells/%s/tablets/%s/partitions" % (node, cell_id, tablet_id)
    partitions = driver.execute(["get", "--format", "<format=text>yson", partitions_path])
    if partitions != None:
        for partition in yson.loads(partitions):
            yield {"pivot_key": partition["pivot_key"]}

# Mapper - copy content with keys between r["left"] and r["right"].
def regions_mapper(r):
    # Read config.
    f = open(CONFIG_FILE_NAME, 'r')
    config = yson.load(f)
    driver_src = Driver.deserialize(config["driver_src"])
    driver_dst = Driver.deserialize(config["driver_dst"])

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
        return driver_src.execute_select(query)
    raw_data = StringIO(query(r["left"], r["right"]))

    if config["mode"] == "write":
        # Yield data.
        for row in yson.load(raw_data, yson_type="list_fragment"):
            yield row
    else:
        # Insert data into destination table.
        new_data = []
        def dump_data():
            driver_dst.execute_command(config["mode"], config["destination"], prepare(new_data))
            yt.write_statistics({"processed_rows": len(new_data)})
            del new_data[:]

        # Process data.
        for row in yson.load(raw_data, yson_type="list_fragment"):
            new_data.append(Transform.row(row))
            if len(new_data) > config["rows_per_insert"]: dump_data()
        dump_data()

        # Yield processed range.
        yield r

def main():
    parser = argparse.ArgumentParser(description="Map-Reduce table manipulator.")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--copy", nargs=2, metavar=('SOURCE', 'DESTINATION'), help="Copy table")
    mode.add_argument("--delete", metavar=('SOURCE'), help="Delete rows from table")
    parser.add_argument("--force", action="store_true", help="Overwrite destination table if it exists")
    parser.add_argument("--proxy", type=str, help="YT proxy")
    parser.add_argument("--proxy_src", type=str, help="source YT proxy")
    parser.add_argument("--proxy_dst", type=str, help="destination YT proxy")
    parser.add_argument("--token_src", type=str, help="source YT token")
    parser.add_argument("--token_dst", type=str, help="destination YT token")
    parser.add_argument("--job_count", type=int, default=JOB_COUNT, help="Numbser of jobs in copy task")
    parser.add_argument("--user_slots", type=int, help="Maximum number of simultaneous jobs running")
    parser.add_argument("--max_failed_job_count", type=int, default=MAX_FAILDED_JOB_COUNT, help="Maximum number of failed jobs")
    parser.add_argument("--memory_limit", type=parse_size, default=JOB_MEMORY_LIMIT, help="Memory limit for a copy task")
    parser.add_argument("--insert_size", type=int, default=MAX_ROWS_PER_INSERT, help="Number of rows passed to 'yt insert' call")
    parser.add_argument("--input_row_limit", type=int, default=INPUT_ROW_LIMIT, help="Limit the input of 'yt select' call")
    parser.add_argument("--output_row_limit", type=int, default=OUTPUT_ROW_LIMIT, help="Limit the output of 'yt select' call")
    parser.add_argument("--max_retry_count", type=int, default=MAX_RETRY_COUNT, help="Maximum number of 'yt' call retries")
    parser.add_argument("--sleep_interval", type=int, default=SLEEP_INTERVAL, help="Sleep interval between 'yt' call retries")
    parser.add_argument("--where", type=str, help="Additional predicate for 'yt select'")
    parser.add_argument("--static_dst", action='store_true', help="Destination table is a static table")
    args = parser.parse_args()

    # Source and destination table paths.
    src = args.copy[0] if args.copy else args.delete
    dst = args.copy[1] if args.copy else None

    if args.proxy_dst and args.static_dst:
        raise Exception("static_dst is not supported for external claster")

    # YT proxies connections.
    existent = lambda x: None if len(x) == 0 else x[0] if x[0] is not None else existent(x[1:])
    yt.config.set_proxy(args.proxy)
    yt_src = Yt(existent([args.proxy_src, args.proxy]), existent([args.token_src, get_token()]), config={"api_version": "v3"})
    yt_dst = Yt(existent([args.proxy_dst, args.proxy]), existent([args.token_dst, get_token()]), config={"api_version": "v3"})

    # YT drivers to use inside operations.
    driver_src = Driver.create(
        args.max_retry_count,
        args.sleep_interval,
        args.input_row_limit,
        args.output_row_limit,
        args.proxy_src,
        args.token_src)
    driver_dst = Driver.create(
        args.max_retry_count,
        args.sleep_interval,
        args.input_row_limit,
        args.output_row_limit,
        args.proxy_dst,
        args.token_dst)

    # Only print commands to stderr, without execution.
    #driver_src.disable()
    #driver_dst.disable()

    # Source table schema.
    schema = yt_src.get(src + "/@schema")
    key_columns = yt_src.get(src + "/@key_columns")

    # Config passed to mapper.
    config = {
        "key_columns": deepcopy(key_columns),
        "source": src,
        "rows_per_insert": args.insert_size,
        "driver_src": driver_src.serialize(),
        "driver_dst": driver_dst.serialize()}
    if (args.where): config["select_where"] = args.where
    def write_config(config):
        with open(CONFIG_FILE_NAME, "w") as config_file: config_file.write(yson.dumps(config))
    write_config(config)

    # Prepare tables.
    if dst and yt_dst.exists(dst):
        if args.force:
            yt_dst.remove(dst)
        else:
            raise Exception("Destination table exists. Use --force")
    if not mounted(yt_src, src):
        yt_src.mount_table(src)
        while not mounted(yt_src, src):
            sleep(1)

    tablets = yt_src.get(src + "/@tablets")

    # Get pivot keys. For a large number of tablets use map-reduce version.
    # Tablet pivots are merged with partition pivots
    print "Prepare partition keys:"
    partition_keys = []

    tablets_table = yt.create_temp_table()
    partitions_table = yt.create_temp_table()
    yt.write_table(tablets_table, tablets, format=yt.YsonFormat(format="text"), raw=False)
    try:
        yt.run_map(
            tablets_mapper,
            tablets_table,
            partitions_table,
            spec={"job_count": 100, "max_failed_job_count":10, "resource_limits": {"user_slots": 50}},
            format=yt.YsonFormat(format="text"),
            local_files=CONFIG_FILE_NAME)
    except YtError as e:
        print str(e)
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
    shuffle(regions)
    yt.write_table(
        regions_table,
        regions,
        format=yt.YsonFormat(format="text"), raw=False)

    # Only print commands to stderr, without execution.
    #driver_src.disable()
    #driver_dst.disable()
    #config["driver_src"] = driver_src.serialize()
    #config["driver_dst"] = driver_dst.serialize()

    # Map operation spec.
    spec={
        "enable_job_proxy_memory_control": False,
        "job_count": args.job_count,
        "max_failed_job_count": args.max_failed_job_count,
        "job_proxy_memory_control": False,
        "mapper": {"memory_limit": args.memory_limit}}
    if args.user_slots: spec["resource_limits"] = {"user_slots": args.user_slots}

    # Copy tablet pivot keys from source table.
    pivot_keys = sorted([tablet["pivot_key"] for tablet in tablets])
    #if len(pivot_keys) > 1 and pivot_keys[1][0] == -2**63: pivot_keys = pivot_keys[0] + pivot_keys[2:]

    if args.copy:
        # Create destination table.
        yt_dst.create_table(dst)
        yt_dst.set_attribute(dst, "schema", Transform.schema(schema))
        yt_dst.set_attribute(dst, "key_columns", Transform.key_columns(key_columns))

        out_table = ""
        if args.static_dst:
            out_table = dst
            config["mode"] = "write"
        else:
            out_table = out_regions_table
            config["mode"] = "insert"
            yt_dst.reshard_table(dst, Transform.pivot_keys(pivot_keys))
            yt_dst.mount_table(dst)
            while not mounted(yt_dst, dst):
                sleep(1)

        config["destination"] = dst
        config["select_columns"] = Transform.select_columns(schema)
        write_config(config)

        # Copy table. Each mapper task copies a single partition.
        try:
            yt.run_map(
                regions_mapper,
                regions_table,
                out_table,
                spec=spec,
                format=yt.YsonFormat(format="text"),
                local_files=CONFIG_FILE_NAME)
        except YtError as e:
            print str(e)
            raise e
    else:
        config["destination"] = src
        config["mode"] = "delete"
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
            print str(e)
            raise e

    yt.remove(regions_table)
    yt.remove(out_regions_table)

if __name__ == "__main__":
    main()
