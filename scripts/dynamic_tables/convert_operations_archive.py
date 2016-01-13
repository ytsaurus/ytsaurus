#!/usr/bin/env python

import yt.wrapper as yt
import yt.yson as yson
from yt.common import YtError, update, set_pdeathsig
from yt.wrapper.client import Yt
from yt.wrapper.native_driver import make_request

import os
import sys
import itertools as it
from random import randint, shuffle
from StringIO import StringIO
from time import sleep

import sys
import time
import datetime

prepare_archive = __import__("../cron/prepare_operation_tablets.py")

def retry(fun, log=lambda message: None, retry_count=10):
    while retry_count > 0:
        try:
            return fun()
        except BaseException as e:
            log('Execution failed. Retrying...')
            log(str(e))
            time.sleep(5)
            retry_count -= 1
    raise Exception('Retry limit exceeded')

def std_err_log(message):
    sys.stderr.write(message)
    sys.stderr.flush()

def make_driver_request(command, params, data):
    config = {"driver_config_path": "/etc/ytdriver.conf", "api_version": "v3"}
    client = Yt(config=config)
    return retry(lambda: make_request(command, params, data=data, client=client), log=std_err_log)

# Get nice yson format
def yson_format():
    return yt.YsonFormat(format="text", boolean_as_string=False, process_table_index=False)

# Mapper - get tablet partition pivot keys.
def collect_pivot_keys_mapper(tablet):
        yield {"pivot_key":tablet["pivot_key"]}
        tablet_id = tablet["tablet_id"]
        cell_id = tablet["cell_id"]
        node = yson.loads(make_driver_request("get", {"output_format": "yson", "path": "#" + cell_id + "/@peers/0/address"}, None))
        partitions_path = "//sys/nodes/%s/orchid/tablet_cells/%s/tablets/%s/partitions" % (node, cell_id, tablet_id)
        partitions = make_driver_request("get", {"output_format": "yson", "path": partitions_path}, None)
        if partitions != None:
            for partition in yson.loads(partitions):
                yield {"pivot_key": partition["pivot_key"]}

def wait_for_state(table, state):
    while not all(tablet["state"] == state for tablet in yt.get(table + "/@tablets")):
        #logging.info("Waiting for table %s tablets to become %s", table, state)
        time.sleep(1)

def unmount_table(table):
    yt.unmount_table(table)
    wait_for_state(table, "unmounted")

def mount_table(table):
    yt.mount_table(table)
    wait_for_state(table, "mounted")

# Write source table partition bounds into partition_bounds_table
def get_partition_bounds(tablets, partition_bounds_table):
    # Get pivot keys. For a large number of tablets use map-reduce version.
    # Tablet pivots are merged with partition pivots
    print "Prepare partition keys:"
    partition_keys = []

    print len(tablets)

    if len(tablets) < 10:
        print "via get"
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
        print "via map"
        tablets_table = yt.create_temp_table()
        partitions_table = yt.create_temp_table()
        yt.write_table(tablets_table, tablets, yson_format(), raw=False)
        try:
            yt.run_map(
                collect_pivot_keys_mapper,
                tablets_table,
                partitions_table,
                spec={"job_count": 100, "max_failed_job_count":10, "resource_limits": {"user_slots": 50}},
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

# Build map operation spec from command line args.
def build_spec_from_options(args):
    spec = {
        "enable_job_proxy_memory_control": False,
        "job_count": args["job_count"],
        "max_failed_job_count": args["max_failed_job_count"],
        "job_proxy_memory_control": False,
        "mapper": {"memory_limit": args["memory_limit"]},
        "resource_limits": {"user_slots": args["user_slots"]}}
    return spec

# Mapper job options.
JOB_COUNT = 100
# Maximum number of simultaneously running jobs.
USER_SLOTS = 100
# Maximum amount of memory allowed for a job
JOB_MEMORY_LIMIT = 2*1024*1024*1024
# Maximum number of failed jobs which doesn't imply operation failure.
MAX_FAILDED_JOB_COUNT = 20
# Maximum number of output rows
OUTPUT_ROW_LIMIT = 100000000
# Maximum number of input rows
INPUT_ROW_LIMIT = 100000000

def run_map_dynamic(mapper, src_table, dst_table, options={}):
    options = update({
        "job_count": JOB_COUNT,
        "user_slots": USER_SLOTS,
        "max_failed_job_count": MAX_FAILDED_JOB_COUNT,
        "memory_limit": JOB_MEMORY_LIMIT
    }, options);

    mount_table(src_table)
    mount_table(dst_table)

    tablets = yt.get(src_table + "/@tablets")
    partition_bounds_table = yt.create_temp_table()
    get_partition_bounds(tablets, partition_bounds_table)

    schema = yt.get(src_table + "/@schema")
    key_columns = yt.get(src_table + "/@key_columns")

    select_columns = ",".join([x["name"] for x in schema if "expression" not in x.keys()])

    def dump_mapper(bound):
        #config = {"driver_config_path": "/etc/ytdriver.conf", "api_version": "v3"}
        #client = Yt(config=config)

        # Get something like ((key1, key2, key3), (bound1, bound2, bound3)) from a bound.
        get_bound_value = lambda bound : ",".join([yson.dumps(x, yson_format="text") for x in bound])
        get_bound_key = lambda width : ",".join([str(x) for x in key_columns[:width]])
        expand_bound = lambda bound : (get_bound_key(len(bound)), get_bound_value(bound))

        # Get records from source table.
        def query(left, right):
            left = "(%s) >= (%s)" % expand_bound(left) if left != None else None
            right = "(%s) < (%s)" % expand_bound(right) if right != None else None
            bounds = [x for x in [left, right] if x is not None]
            where = (" where " + " and ".join(bounds)) if len(bounds) > 0 else ""
            query = "%s from [%s]%s" % (select_columns, src_table, where)
            params = {
                "query": query,
                "input_row_limit": INPUT_ROW_LIMIT,
                "output_row_limit": OUTPUT_ROW_LIMIT,
                "output_format": "yson"
            }
            return make_driver_request("select_rows", params, None)

        count = 0
        src_rows = yson.load(StringIO(query(bound["left"], bound["right"])), yson_type="list_fragment")

        #std_err_log("mapping rows")

        insert_params = {
            "path": dst_table,
            "input_format": "yson"
        }

        result_data = []
        for row in src_rows:
            for result_row in mapper(row):
                count += 1
                result_data.append(yson.dumps(result_row))

            if len(result_data) > 10000:
                result_data = ";".join(result_data)
                make_driver_request("insert_rows", insert_params, result_data)
                result_data = []

        if len(result_data) > 0:
            result_data = ";".join(result_data)
            make_driver_request("insert_rows", insert_params, result_data)

        #std_err_log("mapped {} rows".format(len(result_data)))

        yt.write_statistics({"write_rows": count})

        if False:
            yield

    yt.run_map(
        dump_mapper,
        partition_bounds_table,
        yt.create_temp_table(),
        spec=build_spec_from_options(options),
        format=yson_format())

    yt.remove(partition_bounds_table)

def run_convert(mapper, src_table, dst_table):
    run_map_dynamic(mapper, src_table, dst_table)

    unmount_table(dst_table)
    yt.set(dst_table + "/@forced_compaction_revision", yt.get(dst_table + "/@revision"))
    mount_table(dst_table)

    unmount_table(src_table)
    unmount_table(dst_table)

    yt.move(src_table, src_table + ".bak")
    yt.move(dst_table, src_table)

    mount_table(src_table)

def datestr2int(time_str):
    return int(time.mktime(datetime.datetime.strptime(time_str, "%Y-%m-%dT%H:%M:%S.%fZ").timetuple()))


def convert_key(row):
    parts = row["id"].split("-")

    id_hi = long(parts[3], 16) << 32 | int(parts[2], 16)
    id_lo = long(parts[1], 16) << 32 | int(parts[0], 16)

    return {
        'id_hi': yson.YsonUint64(id_hi),
        'id_lo': yson.YsonUint64(id_lo),
        'start_time': datestr2int(row["start_time"])
    }

def mapper_by_id(row):
    result = convert_key(row)

    result['finish_time'] = datestr2int(row["finish_time"])

    other_columns = [
        'authenticated_user',
        'brief_progress',
        'brief_spec',
        'filter_factors',
        'operation_type',
        'progress',
        'result',
        'spec',
        'state'
    ]

    for column in other_columns:
        result[column] = row[column]

    yield result


def mapper_by_start_time(row):
    result = convert_key(row)
    result['dummy'] = 0

    yield result

def main():
    new_by_start_time_path = prepare_archive.BY_START_TIME_ARCHIVE + ".new"
    prepare_archive.create_ordered_by_start_time_table(new_by_start_time_path);
    run_convert(mapper_by_start_time, prepare_archive.BY_START_TIME_ARCHIVE, new_by_start_time_path)

    new_by_id_path = prepare_archive.BY_ID_ARCHIVE + ".new"
    prepare_archive.create_ordered_by_id_table(new_by_id_path);
    run_convert(mapper_by_id, prepare_archive.BY_ID_ARCHIVE, new_by_id_path)

if __name__ == "__main__":
    main()

