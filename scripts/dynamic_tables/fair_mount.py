#!/usr/bin/python

import yt.wrapper as yt
import sys

def checked(rsp, default=None):
    if "error" in rsp:
        if default is not None:
            return default
        raise RuntimeError(rsp["error"])
    return rsp["output"]

if __name__ == "__main__":
    table = sys.argv[1]
    tablet_count = yt.get(table + "/@tablet_count")
    bundle = yt.get(table + "/@tablet_cell_bundle")
    cells = yt.get("//sys/tablet_cell_bundles/{0}/@tablet_cell_ids".format(bundle))

    reqs = []
    for cell in cells:
        reqs.append({"command": "get", "parameters": {
            "path": "#" + cell + "/@peers/0/address"}})
    rsps = yt.execute_batch(reqs)

    nodes = {}
    for index in xrange(len(cells)):
        cell = cells[index]
        node = checked(rsps[index])
        if node not in nodes:
            nodes[node] = [cell]
        else:
            nodes[node].append(cell)
        
    tabular = []
    for node in nodes.values():
        for i in range(len(node)):
            tabular.append([i, node[i]])
    tabular = sorted(tabular)

    reqs = []
    for i in range(tablet_count):
        reqs.append({"command": "mount_table", "parameters": {
            "path": table,
            "first_tablet_index": i,
            "last_tablet_index": i,
            "cell_id": tabular[i % len(tabular)][1]}})
    rsps = yt.execute_batch(reqs)
    for rsp in rsps:
        if "error" in rsp:
            raise RuntimeError(rsp["error"])
