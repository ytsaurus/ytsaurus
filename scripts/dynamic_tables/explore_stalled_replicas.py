#!/usr/bin/python

import yt.wrapper as yt
import yt.yson as yson
import sys
import time
import datetime
from yt.wrapper.driver import make_request

def execute_batch(reqs):
    rsps = []
    for i in xrange(0, len(reqs), 1000):
        rsps += yt.execute_batch(reqs[i:i+1000], concurrency=50)
    return rsps

def ask_for_each(items, request):
    rsps = []
    for item in items:
        rsps.append(request(item))
    rsps = execute_batch(rsps)
    res = []
    for rsp in rsps:
        if "error" in rsp:
            #raise RuntimeError(rsp["error"])
            res.append(None)
        else:
            res.append(rsp["output"])
    return res

def ask_attr_for_each(items, path):
    return ask_for_each(items, lambda x: {"command": "get", "parameters": {"path": "#{0}/@{1}".format(x, path)}})

def ask_get_for_each(prefix, items, suffix):
    return ask_for_each(items, lambda x: {"command": "get", "parameters": {"path": "{0}{1}{2}".format(prefix, x, suffix)}})

def main():
    cells = list(yt.get("//sys/tablet_cells"))
    print "cells", len(cells)
    tablets = ask_attr_for_each(cells, "tablet_ids")
    print "tablets", len(tablets)
    peers = ask_attr_for_each(cells, "peers/0/address") 
    print "peers", len(peers)
    cells = [[cells[i]] * len(tablets[i]) for i in range(len(cells))]
    peers = [[peers[i]] * len(tablets[i]) for i in range(len(cells))]
    tablets = sum(tablets, [])
    cells = sum(cells, [])
    peers = sum(peers, [])

    tables = ask_attr_for_each(tablets, "table_id")
    print "tables", len(tables)
    tablet_tables = dict(zip(tablets, tables))

    tables = list(set(tables))
    paths = ask_attr_for_each(tables, "path")
    print "paths", len(paths)
    table_paths = dict(zip(tables, paths))

    sources = zip(peers, cells, tablets)

    total_row_counts = ask_for_each(sources, lambda x: {"command": "get", "parameters": {"path": "//sys/nodes/{0}/orchid/tablet_cells/{1}/tablets/{2}/total_row_count".format(x[0], x[1], x[2])}})
    print "total_row_counts", len(total_row_counts)

    replicas = ask_for_each(sources, lambda x: {"command": "get", "parameters": {"path": "//sys/nodes/{0}/orchid/tablet_cells/{1}/tablets/{2}/replicas".format(x[0], x[1], x[2])}})
    print "replicas", len(replicas)

    rsources = [[sources[i]] * len(replicas[i]) for i in xrange(len(replicas)) if replicas[i] is not None]
    total_row_counts = [[total_row_counts[i]] * len(replicas[i]) for i in xrange(len(replicas)) if replicas[i] is not None]
    replicas = [list(replica) for replica in replicas if replica is not None]
    rsources = sum(rsources, [])
    replicas = sum(replicas, [])
    total_row_counts = sum(total_row_counts, [])

    rsources = [list(s) + [r] for s, r in zip(rsources, replicas)]

    print "Will ask for", len(rsources)

    replication_row_indexes = ask_for_each(rsources, lambda x: {"command": "get", "parameters": {"path": "//sys/nodes/{0}/orchid/tablet_cells/{1}/tablets/{2}/replicas/{3}/current_replication_row_index".format(x[0], x[1], x[2], x[3])}})
    print "replication_row_indexes", len(replication_row_indexes)

    assert len(total_row_counts) == len(replication_row_indexes)

    for i in xrange(len(replicas)):
        if total_row_counts[i] < replication_row_indexes[i]:
            print replication_row_indexes[i] - total_row_counts[i], total_row_counts[i], replication_row_indexes[i], rsources[i], table_paths[tablet_tables[rsources[i][2]]] 

if __name__ == "__main__":
    main()
