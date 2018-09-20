#!/usr/bin/python

import yt.wrapper as yt
import yt.yson as yson
import sys
import time
import datetime
import argparse
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

def check_replica(table, replica):
    tablets = yt.get(table + "/@tablets")
    cells = [t["cell_id"] for t in tablets]
    tablets = [t["tablet_id"] for t in tablets]
    peers = ask_attr_for_each(cells, "peers/0/address")

    sources = zip(peers, cells, tablets)

    total_row_counts = ask_for_each(sources, lambda x: {"command": "get", "parameters": {"path": "//sys/nodes/{0}/orchid/tablet_cells/{1}/tablets/{2}/total_row_count".format(x[0], x[1], x[2])}})
    replication_row_indexes = ask_for_each(sources, lambda x: {"command": "get", "parameters": {"path": "//sys/nodes/{0}/orchid/tablet_cells/{1}/tablets/{2}/replicas/{3}/current_replication_row_index".format(x[0], x[1], x[2], replica)}})
    for count, index in zip(total_row_counts, replication_row_indexes):
        if index > count:
            return False
    return True

def fix_replica(args, table, replica_id, replica):
    tablets = yt.get(table + "/@tablets")
    cells = [t["cell_id"] for t in tablets]
    tablets = [t["tablet_id"] for t in tablets]
    peers = ask_attr_for_each(cells, "peers/0/address")

    sources = zip(peers, cells, tablets)

    total_row_counts = ask_for_each(sources, lambda x: {"command": "get", "parameters": {"path": "//sys/nodes/{0}/orchid/tablet_cells/{1}/tablets/{2}/total_row_count".format(x[0], x[1], x[2])}})
    
    replica_cluster = replica["cluster_name"]
    replica_path = replica["replica_path"]

    attributes={
        "table_path": table,
        "cluster_name": replica_cluster,
        "replica_path": replica_path,
        "mode": replica["mode"],
        "start_replication_row_indexes": total_row_counts}
    yt_destination = yt.YtClient(proxy=replica_cluster)
    print "Creating new replica", yson.dumps(attributes)
    
    yt.remove("#" + replica_id)
    replica_id = yt.create("table_replica", attributes=attributes)
    if replica["state"] != "disabled":
        yt.alter_table_replica(replica_id, True)
    print "Unmounting replica table", replica_path
    yt_destination.unmount_table(replica_path, sync=True)
    make_request("alter_table", {"path": replica_path, "upstream_replica_id": replica_id}, client=yt_destination)
    yt_destination.mount_table(replica_path, sync=True)

def main(args):
    table = args.table
    yt.freeze_table(table, sync=True)

    #print "Wait 15 seconds for statistics"
    print "Checking", table
    time.sleep(5)

    replicas = yt.get(table + "/@replicas")

    for replica in replicas.keys():
        if not check_replica(table, replica):
            print "Bad replica", replica, replicas[replica]
            if args.yes and replicas[replica]["mode"] == "sync":
                print "Will fix replica"
                fix_replica(args, table, replica, replicas[replica])

    print "Checked all replicas"

    yt.unfreeze_table(table, sync=True)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Recreate table replica.")
    parser.add_argument("--yes", action="store_true", default=False, help="Do the job")
    parser.add_argument("table", type=str, help="Table path")
    args = parser.parse_args()
    
    main(args)
