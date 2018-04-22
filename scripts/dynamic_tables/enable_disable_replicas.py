#!/usr/bin/python
import yt.wrapper as yt
import sys
import logging
import argparse

def ask_for_each(items, request):
    rsps = []
    for item in items:
        rsps.append(request(item))
    rsps = yt.execute_batch(rsps, concurrency=100)
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

def get_enabled_replicas(cluster):
    tablet_cells_path = "//sys/tablet_cells"
    tablet_cells = yt.get(tablet_cells_path)
    tablet_ids = ask_attr_for_each(tablet_cells, "tablet_ids")
    tablet_ids = sum(tablet_ids, [])
    table_ids = ask_attr_for_each(tablet_ids, "table_id")
    table_ids = list(set(table_ids))
    replicas = ask_attr_for_each(table_ids, "replicas")
    result = []
    for replica in replicas:
        if replica == None: continue
        for r, rr in replica.items():
            if rr["state"] == "enabled" and (cluster == None or rr["cluster_name"] == cluster):
                result.append(r)
    return result

def alter_table_replicas(replicas, enable):
    reqs = []
    for replica in replicas:
        reqs.append({"command": "alter_table_replica", "parameters": {"replica_id": replica, "enabled": enable}})
    rsps = execute_batch(reqs)
    for replica, rsp in zip(replicas, rsps):
        if "error" in rsp:
            logging.error("%s -> %r", replica, rsp)

def disable(replicas):
    alter_table_replicas(replicas, enable=False)

def enable(replicas):
    alter_table_replicas(replicas, enable=True)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Mount-unmount dynamic tables.")
    parser.add_argument("command", type=str, help="Action to perform (enable, disable)")
    parser.add_argument("--replicas", type=str, help="File with replicas")
    parser.add_argument("--cluster", type=str, default=None, help="Disable replicas for specific cluster")
    args = parser.parse_args()

    if args.command == "list":
        replicas = get_enabled_replicas(args.cluster)
        for replica in replicas:
            print replica
    elif args.command == "disable":
        if args.replicas is None:
            raise Exception("--replicas argument is required")
        with open(args.replicas, "a") as f:
            replicas = get_enabled_replicas(args.cluster)
            for replica in replicas:
                f.write(replica + "\n")
            f.close()
            disable(replicas)
    elif args.command == "enable":
        replicas = []
        with open(args.replicas, "r") as f:
            for replica in f.readlines():
                replicas.append(replica.strip())
        enable(replicas)
    else:
        raise Exception("Unknown command: %s" % (args.command))

