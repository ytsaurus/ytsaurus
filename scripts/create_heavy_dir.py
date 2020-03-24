#!/usr/bin/env python2

import yt.wrapper as yt

GROWTH_FACTOR = 7
DIR_PREFIX="//tmp/create_heavy_dir.py"
TABLES_PER_DIR = 10
DEPTH_LIMIT = 5

def multiply_dir(level):
    yt.create("map_node", "{}/d{}".format(DIR_PREFIX, level+1), attributes={"opaque": True})

    for i in xrange(GROWTH_FACTOR):
        yt.copy("{}/d{}".format(DIR_PREFIX, level), "{}/d{}/d{}".format(DIR_PREFIX, level+1, i))

def main():
    yt.create("map_node", "{}/d0".format(DIR_PREFIX))
    for i in xrange(TABLES_PER_DIR):
        yt.create("table", "{}/d0/tbl{}".format(DIR_PREFIX, i))

    for i in xrange(DEPTH_LIMIT):
        multiply_dir(i)

    for i in xrange(DEPTH_LIMIT):
        yt.remove("{}/d{}".format(DIR_PREFIX, i), recursive=True)

    total_node_count = yt.get("{}/d{}/@recursive_resource_usage/node_count".format(DIR_PREFIX, DEPTH_LIMIT))
    print "Created directory \"{}/d{}\" with {} nodes".format(DIR_PREFIX, DEPTH_LIMIT, total_node_count)

if __name__ == "__main__":
    main()
