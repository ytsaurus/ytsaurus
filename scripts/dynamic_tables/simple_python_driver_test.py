#!/usr/bin/python
import yt.wrapper as yt
import yt.yson as yson
from yt.common import YtError
from yt.wrapper.table import TablePath
from yt.wrapper.client import Yt
from yt.wrapper.native_driver import make_request
from time import sleep
import argparse
import sys

schema = [
    {"name": "key", "type": "int64", "sort_order": "ascending"},
    {"name": "value", "type": "int64"}
]
key_columns = [column["name"] for column in schema if "sort_order" in column]
data_rows = [{"key": i, "value": i} for i in xrange(1000)]

def wait_until(path, state):
    while not all(x["state"] == state for x in yt.get(path + "/@tablets")):
        sleep(1)
def mount_table(path):
    sys.stdout.write("Mounting table %s... " % (path))
    yt.mount_table(path)
    wait_until(path, "mounted")
    print "done"

def prepare(value):
    if not isinstance(value, list):
        value = [value]
    # remove surrounding [ ]
    return yson.dumps(value)[1:-1]

@yt.aggregator
class InsertMapper():
    def __init__(self, table):
        self.table = table
    def __call__(self, records):
        rows = []
        for record in records:
            rows.append(record)

        config = {"driver_config_path": "/etc/ytdriver.conf", "api_version": "v3"}
        client = Yt(config=config)
        params = {
            "path": self.table,
            "input_format": "yson",
        }
        make_request("insert_rows", params, data=prepare(rows), client=client)

        if False:
            yield None

@yt.aggregator
class LookupMapper():
    def __init__(self, table):
        self.table = table
    def __call__(self, records):
        rows = []
        keys = []
        for record in records:
            rows.append(record)
            keys.append({"key": record["key"]})

        config = {"driver_config_path": "/etc/ytdriver.conf", "api_version": "v3"}
        client = Yt(config=config)
        params = {
            "path": self.table,
            "input_format": "yson",
            "output_format": "yson",
            "keep_missing_rows": True
        }
        data = make_request("lookup_rows", params, data=prepare(keys), client=client)
        results = list(yson.loads(data, yson_type="list_fragment"))
        for i in xrange(len(rows)):
            if results[i] == None:
                raise Exception("Lookup for key %s returned none" % (keys[i]))
            if not all(x[0] == x[1] for x in zip(sorted(rows[i].iteritems()), sorted(results[i].iteritems()))):
                raise Exception("Lookup for key %s expected %s actual %s" % (keys[i], rows[i], results[i]))

        if False:
            yield None

@yt.aggregator
class SelectMapper():
    def __init__(self, table):
        self.table = table
    def __call__(self, records):
        rows = []
        keys = []
        select_keys = []
        for record in records:
            rows.append(record)
            keys.append({"key": record["key"]})
            select_keys.append("(%s)" % (",".join("null" if record.get(k, None) == None else yson.dumps(record[k]) for k in key_columns)))
        query = "* from [%s] where (%s) in (%s)" % (self.table, ",".join(key_columns), ",".join(select_keys))

        config = {"driver_config_path": "/etc/ytdriver.conf", "api_version": "v3"}
        client = Yt(config=config)
        params = {
            "query": query,
            "output_format": "yson",
        }
        data = make_request("select_rows", params, data=None, client=client)
        results = list(yson.loads(data, yson_type="list_fragment"))

        def sorting(row):
            return [row.get(k, None) for k in key_columns]

        rows.sort(key=sorting)
        keys.sort(key=sorting)
        results.sort(key=sorting)

        if len(rows) != len(results):
            raise Exception("Select for keys %s expected %s actual %s" % (keys, rows, results))

        for i in xrange(len(rows)):
            if not all(x[0] == x[1] for x in zip(sorted(rows[i].iteritems()), sorted(results[i].iteritems()))):
                raise Exception("Select for keys %s expected %s actual %s" % (keys, rows, results))

        if False:
            yield None

def main(args):
    static_table = args.table + ".s"
    dynamic_table = args.table + ".d"
    yt.remove(static_table, force=True)
    yt.remove(dynamic_table, force=True)
    temp_table = yt.create_temp_table()
    yt.create_table(static_table)
    yt.write_table(static_table, data_rows)
    yt.create_table(dynamic_table, attributes={"dynamic": True, "schema": schema})
    mount_table(dynamic_table)
    spec = {"max_failed_job_count": 1, "job_count": len(data_rows)}
    yt.run_map(
        InsertMapper(dynamic_table),
        static_table,
        temp_table,
        spec=spec)
    yt.run_map(
        LookupMapper(dynamic_table),
        static_table,
        temp_table,
        spec=spec)
    yt.run_map(
        SelectMapper(dynamic_table),
        static_table,
        temp_table,
        spec=spec)
    yt.remove(temp_table)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Map-Reduce table manipulator.")
    parser.add_argument("--table", type=str, help="Table path", required=True)
    parser.add_argument("--proxy", type=yt.config.set_proxy, help="YT proxy")
    args = parser.parse_args()
    main(args)
