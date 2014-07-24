#!/usr/bin/env python

import yt.yson as yson
import yt.wrapper as yt

import argparse
from datetime import datetime
from copy import deepcopy
        
MOD = 2 ** 60
SHARD_COUNT = 500

def date_to_timestamp(date_str):
    #2000-11-01 00:00:00
    return int((datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S") - datetime(year=1970, month=1, day=1)).total_seconds())

def get_python_type(schema_type):
    if schema_type == "int64":
        return int
    elif schema_type == "double":
        return float
    elif schema_type == "string":
        return str
    elif schema_type == "date":
        return date_to_timestamp
    else:
        raise yt.YtError("Unknown schema type: " + schema_type)

def to_yt_schema(schema):
    result = deepcopy(schema)
    for elem in result:
        if elem["type"] == "date":
            elem["type"] = "int64"
    return result


class AddHash(object):
    def __init__(self, key_columns):
        self._key_columns = key_columns

    def __call__(self, rec):
        rec["hash"] = hash(tuple(rec.get(key) for key in self._key_columns)) % MOD
        yield rec

class ApplySchemaTypes(object):
    def __init__(self, schema):
        self._schema = schema
    
    def __call__(self, rec):
        new_rec = {}
        for elem in self._schema:
            name = elem["name"]
            type = get_python_type(elem["type"])
            if name in rec:
                if rec[name] == "":
                    continue
                new_rec[name] = type(rec[name])
        yield new_rec

def unique(key, recs):
    yield recs.next()

class ParseYsonArgument(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, yson.loads(values))

def main():
    parser = argparse.ArgumentParser(description="Create new table with tablets and schema and push into it data from input table.")
    parser.add_argument("--input")
    parser.add_argument("--output")
    parser.add_argument("--schema", action=ParseYsonArgument,
                        help="Description of table schema in yson")
    parser.add_argument("--key-columns",  action=ParseYsonArgument,
                        help="Names of key columns (as yson list)")
    parser.add_argument("--hash-columns",  action=ParseYsonArgument,
                        help="Names of hash columns (as yson list)")
    args = parser.parse_args()

    assert(len(args.hash_columns) <= len(args.key_columns) and 
           args.key_columns[:len(args.hash_columns)] == args.hash_columns)

    yt.config.format.TABULAR_DATA_FORMAT = yt.YsonFormat()
    
    yt.remove(args.output, force=True)
    yt.create_table(args.output)
    yt.set_attribute(args.output, "schema", [{"name": "hash", "type": "int64"}] + to_yt_schema(args.schema))
    yt.set_attribute(args.output, "key_columns", ["hash"] + args.key_columns)
    yt.reshard_table(args.output, [[]] + [[(i * MOD) / SHARD_COUNT] for i in xrange(1, SHARD_COUNT)])
    yt.mount_table(args.output)

    temp = "//tmp/intermediate" #yt.create_temp_table()
    yt.run_map(ApplySchemaTypes(args.schema), args.input, temp)
    yt.run_map(AddHash(args.hash_columns), temp, temp)
    yt.run_map_reduce(None, unique, temp, temp, reduce_by=["hash"] + args.key_columns)
    yt.run_map("./upload.sh " + args.output, temp, yt.create_temp_table(),
               spec={"data_size_per_job": 16 * 1024 * 1024,
                     "mapper": {"enable_input_table_index": "false"}},
               local_files="upload.sh")
    #yt.remove(temp)

if __name__ == "__main__":
    main()
