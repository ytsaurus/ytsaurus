#!/usr/bin/python

import yt.yson as yson

import subprocess as sp
import sys
from StringIO import StringIO

def prepare(value, is_raw=False):
    if not is_raw:
        if not isinstance(value, list):
            value = [value]
        value = yson.dumps(value)
        # remove surrounding [ ]
        value = value[1:-1]
    return value

# Map task - copy content with keys between r["left"] and r["right"].
def regions_mapper(config, r):
    # Get something like ((key1, key2, key3), (bound1, bound2, bound3)) from a bound.
    get_bound_value = lambda bound : ",".join(['"' + x + '"' if isinstance(x, str) else str(x) for x in bound])
    get_bound_key = lambda width : ",".join([str(x) for x in config["key_columns"][:width]])
    expand_bound = lambda bound : (get_bound_key(len(bound)), get_bound_value(bound))
    
    # Get records from source table.
    def query(left, right):
        left = "(%s) >= (%s)" % expand_bound(left) if type(left) != yson.YsonEntity else None
        right = "(%s) < (%s)" % expand_bound(right) if type(right) != yson.YsonEntity else None
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

if __name__ == "__main__":
    regions_mapper(yson.loads(sys.argv[1]), yson.loads(sys.argv[2]))
