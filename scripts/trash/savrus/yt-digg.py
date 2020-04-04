#!/usr/bin/python
#
# YT Dynamic tables digging scripts
#

import yt.wrapper as yt
import yt.yson as yson
import sys
import time
import datetime
import subprocess as sp

suffixes = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
def humansize(nbytes):
    i = 0
    while nbytes >= 1024 and i < len(suffixes)-1:
        nbytes /= 1024.
        i += 1
    f = ('%.2f' % nbytes).rstrip('0').rstrip('.')
    return '%s %s' % (f, suffixes[i])

def count_tablets_per_table():
    #node="n0046h.seneca-sas.yt.yandex.net"
    node = sys.argv[1]
    path="//sys/nodes/{0}:9012/orchid/tablet_cells".format(node)
    d = {}
    for cell in yt.get(path):
        print "processing cell", cell
        ppath = path + "/" + cell + "/tablets"
        for tablet in yt.get(ppath):
            print "processing tablet", tablet
            try:
                table = yt.get("#" + tablet + "/@table_id")
                if table in d:
                    d[table] += 1
                else:
                    d[table] = 1
            except:
                pass
    l = []
    for k, v in d.iteritems():
        try:
            p = yt.get("#" + k + "/@path")
            l.append((v, p))
        except:
            l.append((v, k))
    l = sorted(l)
    for x in l:
        print x[1], x[0]   

def f():
    #node="n0046h.seneca-sas.yt.yandex.net"
    node = sys.argv[1]
    path="//sys/nodes/{0}:9012/orchid/tablet_cells".format(node)
    xx = []
    for cell in yt.get(path):
        print "processing cell", cell
        ppath = path + "/" + cell + "/tablets"
        for tablet in yt.get(ppath):
            print "processing tablet", tablet
            pppath = ppath + "/" + tablet + "/eden/stores"
            try:
                for store in yt.get(pppath):
                    qpath = pppath + "/" + store
                    data = yt.get(qpath)
                    #if data["store_state"] == "passive_dynamic":
                    if data["store_state"] != "persistent":
                        print data
                        xx.append((data["pool_capacity"], tablet, data))
            except:
                pass
    d = {}
    #xx = sorted(xx)
    for x in xx:
        table = yt.get("#" + x[1]  + "/@table_id")
        path = yt.get("#" + table + "/@path")
        #print x[0], x[2], path
        #if path in d:
        #    d[path] += (x[0], 1)
        #else:
        #    d[path] = (x[0], 1)
        d[path] = d.get(path, 0) + x[0]

    l = []
    for k, v in d.iteritems():
        l.append((v, k))
    for ll in sorted(l):
        print ll[1], ll[0]

def tc(node):
    count = 0;
    state = yt.get("//sys/nodes/{0}/@state".format(node))
    if state == "online":
        path="//sys/nodes/{0}/orchid/tablet_cells".format(node)
        for cell in yt.get(path):
            count += yt.get("#" + cell + "/@tablet_count")
    return count 

def t():
    for node in yt.list("//sys/nodes"):
        print node, tc(node)

def ff():
    node = sys.argv[1]
    path="//sys/nodes/{0}:9012/orchid/tablet_cells".format(node)
    d = {}
    for cell in yt.get(path):
        #print "processing cell", cell
        ppath = path + "/" + cell + "/tablets"
        for tablet in yt.get(ppath):
            #print "processing tablet", tablet
            attrs = yt.get("#" + tablet  + "/@")
            tpath = yt.get("#" + attrs["table_id"] + "/@path")
            if tpath in d:
                d[tpath].append(attrs)
            else:
                d[tpath] = [attrs]
    #for k,v in d.items():
    #    print k
    #    print v
    xx = []
    for k,v in d.items():
        a = {"tablet_count": 1, "performance_counters": v[0]["performance_counters"]}
        for i in range(1, len(v)):
            a["tablet_count"] += 1
            for p in a["performance_counters"].keys():
                a["performance_counters"][p] += v[i]["performance_counters"][p]
        for p in a["performance_counters"].keys():
            if a["performance_counters"][p] == 0 or str(a["performance_counters"][p]) == "0.0":
                a["performance_counters"].pop(p)

        if len(a["performance_counters"].keys()) > 0:
            xx.append((k, a))
    for x in xx:
        print x[0]
        print x[1]

def tt():
    node = sys.argv[1]
    path="//sys/nodes/{0}:9012".format(node)
    xx =[]
    for slot in yt.get(path + "/@tablet_slots"):
        if "cell_id" not in slot.keys():
            continue
        #print slot["cell_id"], yt.get("#" + slot["cell_id"] + "/@total_statistics/memory_size")

        for tablet_id in yt.get("#" + slot["cell_id"] + "/@tablet_ids"):
            tablet = yt.get("#" + tablet_id + "/@")
            memory = tablet["statistics"]["memory_size"]
            if memory == 0:
                continue
            table = yt.get("#" + tablet["table_id"] + "/@path")
            #print table, tablet_id
            xx.append((memory, table, tablet_id))

    for x in reversed(sorted(xx)):
        print x

def ttbundle():
    bundle = sys.argv[1]
    xx =[]
    for cell_id in yt.get("//sys/tablet_cell_bundles/{0}/@tablet_cell_ids".format(bundle)):
        for tablet_id in yt.get("#" + cell_id + "/@tablet_ids"):
            tablet = yt.get("#" + tablet_id + "/@")
            memory = tablet["statistics"]["memory_size"]
            if memory == 0:
                continue
            table = yt.get("#" + tablet["table_id"] + "/@path")
            #print table, tablet_id
            xx.append((memory, table, tablet_id))

    for x in reversed(sorted(xx)):
        print x

def ttt():
    table = sys.argv[1]
    nodes = {}
    for tablet in yt.get(table + "/@tablets"):
        node = yt.get("#" + tablet["cell_id"] + "/@peers/0/address")
        if node not in nodes:
            nodes[node] = []
        path="//sys/nodes/{0}/orchid/tablet_cells/{1}/tablets/{2}/eden/stores".format(node, tablet["cell_id"], tablet["tablet_id"])
        for store in yt.get(path):
            data = yt.get(path + "/" + store)
            if data["store_state"] != "persistent":
                print data
                nodes[node].append((tablet, data))
            
    for k, v in nodes.items():
        pool_size = 0
        tablet_count = len(v)
        compressed_size = 0
        for pair in v:
            pool_size += pair[1]["pool_capacity"]
            compressed_size += pair[0]["statistics"]["compressed_data_size"]

        print k, tablet_count, pool_size, compressed_size

def fair_mount():
    table = sys.argv[1]
    tablet_count = yt.get(table + "/@tablet_count")
    bundle = yt.get(table + "/@tablet_cell_bundle")
    cells = yt.get("//sys/tablet_cell_bundles/{0}/@tablet_cell_ids".format(bundle))
    nodes = {}
    for cell in cells:
        node = yt.get("#" + cell + "/@peers/0/address")
        if node == "n3922-sas.hahn.yt.yandex.net:9012":
            continue
        if node not in nodes:
            nodes[node] = [cell]
        else:
            nodes[node].append(cell)
        
    tabular = []
    for node in nodes.values():
        for i in range(len(node)):
            tabular.append([i, node[i]])
    tabular = sorted(tabular)
    print tabular

    for i in range(tablet_count):
        yt.mount_table(table, first_tablet_index=i, last_tablet_index=i, cell_id=tabular[i % len(tabular)][1])

def checked(rsp, default=None):
    if "error" in rsp:
        if default is not None:
            return default
        raise RuntimeError(rsp["error"])
    return rsp["output"]

def list_tablet_nodes():
    nodes = list(yt.list("//sys/nodes"))
    reqs = []
    for node in nodes:
        reqs.append({"command": "get", "parameters": {
            "path": "//sys/nodes/{}/@".format(node)}})
    rsps = []
    for i in xrange(0, len(reqs), 100):
        rsps += yt.execute_batch(reqs[i:i+100])
    for i in xrange(len(nodes)):
        data = checked(rsps[i], default={})
        if not "tablet_slots" in data:
            continue
        slots = data["tablet_slots"]
        if len(slots) > 0:
            print nodes[i].split(":")[0], len(slots), data["tags"]

def misconfigured():
    for node in yt.list("//sys/nodes"):
        data = yt.get("//sys/nodes/{0}/@".format(node))
        if "not_bsyeti" in data["tags"] and data["statistics"]["memory"]["tablet_static"]["limit"] != 171798691840:
            print node

def reshard():
    table = "//home/metrika/webvisor"
    pivots = yt.get(table + "/@pivot_keys")
    yt.reshard_table(table, pivots[::9])
    yt.set(table + "/@disable_tablet_balancer", True)

def du_journals():
    reqs = []
    cell_ids = yt.list("//sys/tablet_cells")
    for cell_id in cell_ids:
        reqs.append({"command": "get", "parameters": {
            "path": "//sys/tablet_cells/{}".format(cell_id)}})
    rsps = yt.execute_batch(reqs, concurrency=100)
    reqs = []
    cells = []
    for i in xrange(len(rsps)):
        cell = cell_ids[i]
        rsp = rsps[i]
        if "error" in rsp:
            continue
        rsp = rsp["output"]
        #for s in rsp["snapshots"]:
        #    reqs.append({"command": "get", "parameters": {
        #        "path": "//sys/tablet_cells/{}/snapshots/{}/@resource_usage/disk_space".format(cell, s)}})
        #    cells.append(cell)            
        for c in rsp["changelogs"]:
            reqs.append({"command": "get", "parameters": {
                "path": "//sys/tablet_cells/{}/changelogs/{}/@resource_usage/disk_space".format(cell, c)}})
            cells.append(cell)            

    rsps = yt.execute_batch(reqs, concurrency=100)
    cell_sizes = {cell: 0 for cell in cell_ids}
    for i in xrange(len(rsps)):
        cell = cells[i]
        rsp = rsps[i] 
        if "error" in rsp:
            continue
        rsp = rsp["output"]
        cell_sizes[cell] += rsp

    cell_sizes = [(size, cell_id) for cell_id, size in cell_sizes.iteritems()]
    cell_sizes = sorted(cell_sizes)
    for size, cell_id in cell_sizes:
        print cell_id, size
    

def get_table_ids_for_tablet_ids(tablet_ids):
    table_ids = {}
    reqs = []
    for tablet_id in tablet_ids:
        reqs.append({"command": "get", "parameters": {
            "path": "#" + tablet_id + "/@table_id"}})
    rsps = yt.execute_batch(reqs, concurrency=100)
    for i in xrange(len(rsps)):
        tablet_id = tablet_ids[i]
        rsp = rsps[i] 
        if "error" in rsp:
            raise RuntimeError(rsp["error"])
        rsp = rsp["output"]
        table_ids[tablet_id] = rsp
    return table_ids

def get_table_paths_for_table_ids(table_ids):
    paths = {}
    reqs = []
    for table_id in table_ids:
        reqs.append({"command": "get", "parameters": {
            "path": "#" + table_id + "/@path"}})
    rsps = yt.execute_batch(reqs, concurrency=100)
    for i in xrange(len(rsps)):
        table_id = table_ids[i]
        rsp = rsps[i] 
        if "error" in rsp:
            raise RuntimeError({"table_id": table_id, "error": rsp["error"]})
        rsp = rsp["output"]
        paths[table_id] = rsp
    return paths

def cell_tablet_dyncmic_statistics(cell_id):
    peer = yt.get("#" + cell_id + "/@peers/0/address")
    tablet_ids = yt.get("#" + cell_id + "/@tablet_ids")
    table_ids = get_table_ids_for_tablet_ids(tablet_ids)
    paths = get_table_paths_for_table_ids(table_ids.values())

    reqs = []
    for tablet_id in tablet_ids:
        reqs.append({"command": "get", "parameters": {
            "path": "//sys/nodes/{0}/orchid/tablet_cells/{1}/tablets/{2}".format(peer, cell_id, tablet_id)}})
    rsps = yt.execute_batch(reqs, concurrency=100)
    reqs = []
    tablets = []
    for i in xrange(len(rsps)):
        tablet_id = tablet_ids[i]
        rsp = rsps[i] 
        if "error" in rsp:
            continue
        rsp = rsp["output"]
        if not "eden" in rsp:
            continue
        for store in rsp["eden"]["stores"]:
            reqs.append({"command": "get", "parameters": {
                "path": "//sys/nodes/{0}/orchid/tablet_cells/{1}/tablets/{2}/eden/stores/{3}".format(peer, cell_id, tablet_id, store)}})
            tablets.append(tablet_id)
        #for i in range(len(rsp["partitions"])):
        #    for store in rsp["partitions"][i]["stores"]:
        #        reqs.append({"command": "get", "parameters": {
        #            "path": "//sys/nodes/{0}/orchid/tablet_cells/{1}/tablets/{2}/partitions/{3}/stores/{4}".format(peer, cell_id, tablet_id, i, store)}})
        #        tablets.append(tablet_id)

    statistics = {table_id: {
            "active_dynamic": {"pool_size": 0, "row_count": 0},
            "passive_dynamic": {"pool_size": 0, "row_count": 0}
        } for table_id in table_ids.values()}
    rsps = yt.execute_batch(reqs, concurrency=100)
    for i in xrange(len(rsps)):
        tablet_id = tablets[i]
        table_id = table_ids[tablet_id]
        rsp = rsps[i] 
        if "error" in rsp:
            continue
        rsp = rsp["output"]
        if rsp["store_state"] not in ("active_dynamic", "passive_dynamic"):
            continue
        for stat in ("pool_size", "row_count"):
            statistics[table_id][rsp["store_state"]][stat] += rsp[stat]

    keys = sorted(statistics.keys(), key=lambda x: statistics[x]["active_dynamic"]["pool_size"] + statistics[x]["passive_dynamic"]["pool_size"])
    for key in keys:
         print paths[key], statistics[key]

def ask_for_each(items, request):
    rsps = []
    for item in items:
        rsps.append(request(item))
    rsps = yt.execute_batch(rsps, concurrency=50)
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


def overlapping_stores_statistics():
    cells = yt.list("//sys/tablet_cells")
    peers = ask_for_each(cells, lambda cell: {"command": "get", "parameters": {"path": "#{0}/@peers/0/address".format(cell)}})
    tablets = ask_for_each(cells, lambda cell: {"command": "get", "parameters": {"path": "#{0}/@tablet_ids".format(cell)}})
    exts = []
    for i in xrange(len(cells)):
        for tablet in tablets[i]:
            exts.append({"tablet": tablet, "cell": cells[i], "peer": peers[i]})
    overlapping = ask_for_each(exts, lambda x: {"command": "get", "parameters": {"path": "//sys/nodes/{0}/orchid/tablet_cells/{1}/tablets/{2}/overlapping_store_count".format(x["peer"], x["cell"], x["tablet"])}})
    tables = ask_for_each(exts, lambda x: {"command": "get", "parameters": {"path": "#{0}/@table_id".format(x["tablet"])}})
    d = {}
    for i in xrange(len(tables)):
        path = tables[i]
        if path not in d:
            d[path] = list()
        d[path].append(overlapping[i])
    for table in d.keys():
        d[table] = list(reversed(sorted(d[table])))[:5]
    result = list(d.items())
    result = sorted(result, key=lambda x: x[1][0])
    paths = ask_for_each(result, lambda x: {"command": "get", "parameters": {"path": "#{0}/@path".format(x[0])}})
    for i in xrange(len(result)):
        print paths[i], result[i][1]

def investigate():
    tablets = yt.get("//sys/tablet_cells/11c3e-d50f4-3fc02bc-f2369d29/@tablet_ids")
    attrs = ask_for_each(tablets, lambda x: {"command": "get", "parameters": {"path": "#{0}/@".format(x)}})
    print yson.dumps(attrs, yson_format="pretty")

def dynamic_store_size():
    cell_id = sys.argv[1]
    tablets = yt.get("#{0}/@tablet_ids".format(cell_id))
    peer = yt.get("#{0}/@peers/0/address".format(cell_id))
    rsps = ask_for_each(tablets, lambda tablet: {"command": "get", "parameters": {"path": "//sys/nodes/{0}/orchid/tablet_cells/{1}/tablets/{2}/eden/stores".format(peer, cell_id, tablet)}})
    #rsps = ask_for_each(tablets, lambda tablet: {"command": "get", "parameters": {"path": "//sys/nodes/{0}/orchid/tablet_cells/{1}/tablets/{2}/stores".format(peer, cell_id, tablet)}})
    reqs = []
    for tablet, stores in zip(tablets, rsps):
        if stores is None:
            continue
        for store in stores:
            reqs.append((tablet, store))
    rsps = ask_for_each(reqs, lambda (t, s): {"command": "get", "parameters": {"path": "//sys/nodes/{0}/orchid/tablet_cells/{1}/tablets/{2}/eden/stores/{3}".format(peer, cell_id, t, s)}})
    #rsps = ask_for_each(reqs, lambda (t, s): {"command": "get", "parameters": {"path": "//sys/nodes/{0}/orchid/tablet_cells/{1}/tablets/{2}/stores/{3}".format(peer, cell_id, t, s)}})
    d={}
    for req, rsp in zip(reqs, rsps):
        if rsp["store_state"] == "persistent":
            continue
        tablet = req[0]
        size = rsp["pool_size"]
        d[tablet] = d.get(tablet, 0) + size
    result = list(d.items())
    result = sorted(result, key=lambda x: x[1])
    tables = ask_for_each(result, lambda x: {"command": "get", "parameters": {"path": "#{0}/@table_id".format(x[0])}})
    paths = ask_for_each(tables, lambda x: {"command": "get", "parameters": {"path": "#{0}/@path".format(x)}})
    for i in xrange(len(result)):
        print result[i], paths[i]


def do_free_cell(cell_id, forbidden_cells=None):
    bundle = yt.get("#{0}/@tablet_cell_bundle".format(cell_id))
    tablets = yt.get("#{0}/@tablet_ids".format(cell_id))
    cells = [cell for cell in yt.get("//sys/tablet_cell_bundles/{0}/@tablet_cell_ids".format(bundle)) if cell != cell_id]
    if forbidden_cells != None:
        cells = [cell for cell in cells if cell not in forbidden_cells]
    health = ask_attr_for_each(cells, "health")
    cells = [x[0] for x in zip(cells, health) if x[1] == "good"]
    print len(cells), "healthy cells"
    i = 0
    for tablet in tablets:
        yt.create("tablet_action", attributes={"kind": "move", "tablet_ids": [tablet], "cell_ids": [cells[i]]})
        i += 1
        if i >= len(cells): i = 0

def do_remove_cell(cell, forbidden_cells=None):
    do_free_cell(cell, forbidden_cells)
    print "Removing cell {0}".format(cell)
    while yt.get("#{0}/@tablet_count".format(cell)) != 0:
        time.sleep(1)
        print cell, yt.get("#{0}/@tablet_count".format(cell))
    yt.remove("#{0}".format(cell))
    print "Removed cell {0}".format(cell)

def free_cell():
    cell_id = sys.argv[1]
    do_free_cell(cell_id)

def remove_cell():
    cell_id = sys.argv[1]
    do_remove_cell(cell_id)

def remove_cell_list2(cells):
    for cell in cells:
        while True:
            try:
                do_remove_cell(cell, cells)
                break
            except Exception:
                pass

def remove_cell_list(cells):
    for cell in cells:
        while True:
            try:
                do_remove_cell(cell, cells)
                break
            except Exception:
                pass

def remove_cells():
    count = int(sys.argv[1])
    bundle = sys.argv[2]
    #cells = yt.list("//sys/tablet_cells")
    cells = yt.get("//sys/tablet_cell_bundles/{0}/@tablet_cell_ids".format(bundle))
    health = ask_attr_for_each(cells, "health")
    cells = [cell for cell, health in zip(cells, health) if health == "good"]
    tablet_counts = ask_attr_for_each(cells, "tablet_count")
    candidates = sorted(zip(cells, tablet_counts), key=lambda x: x[1])
    candidates = [x[0] for x in candidates[:count]]
    remove_cell_list(candidates)

def decommission_cells():
    count = int(sys.argv[1])
    bundle = sys.argv[2]
    cells = yt.get("//sys/tablet_cell_bundles/{0}/@tablet_cell_ids".format(bundle))
    health = ask_attr_for_each(cells, "health")
    cells = [cell for cell, health in zip(cells, health) if health == "good"]
    cells = cells[:count]
    print "Decommission cells", cells

    batch = yt.create_batch_client()
    for cell in cells:
        batch.set("#{0}/@decommissioned".format(cell), True)
    batch.commit_batch()
    #return ask_for_each(cells, lambda x: {"command": "set", "parameters": {"path": "#{0}/@decommissioned".format(x)}, "input": yson.dumps(True)})

def remove_decommissioned_cells():
    cells = yt.get("//sys/tablet_cells")
    decommissioned = ask_attr_for_each(cells, "decommissioned")
    tablet_counts = ask_attr_for_each(cells, "tablet_count")
    batch = yt.create_batch_client()
    for cell, remove, tc in zip(cells, decommissioned, tablet_counts):
        if remove:
            if tc > 0:
                print "Cell", cell, "has", tc, "tablets"
                continue
            batch.remove("#{0}".format(cell))
    batch.commit_batch()


def remove_cells_from_node():
    node = sys.argv[1]
    cells = [x["cell_id"] for x in yt.get("//sys/nodes/{0}:9012/@tablet_slots".format(node)) if "cell_id" in x]
    remove_cell_list(cells)

def table_distribution():
    table = sys.argv[1]
    tablets = yt.get("{0}/@tablets".format(table))
    cells = [t["cell_id"] for t in tablets]
    nodes = ask_for_each(cells, lambda x: {"command": "get", "parameters": {"path": "#{0}/@peers/0/address".format(x)}})
    #print cells
    node_map = {}
    for node in nodes:
        node_map[node] = node_map.get(node, 0) + 1
    result = sorted(node_map.items(), key = lambda x: x[1])
    for res in result:
        print res

    print ""
    node_map = {}
    for i in xrange(len(tablets)):
        node = nodes[i]
        tablet = tablets[i]
        node_map[node] = node_map.get(node, 0) + tablet["statistics"]["uncompressed_data_size"]
    result = sorted(node_map.items(), key = lambda x: x[1])
    for res in result:
        print res


def list_tablets_on_node():
    table = sys.argv[1]
    node = sys.argv[2]
    tablets = yt.get("{0}/@tablets".format(table))
    cells = [t["cell_id"] for t in tablets]
    nodes = ask_for_each(cells, lambda x: {"command": "get", "parameters": {"path": "#{0}/@peers/0/address".format(x)}})
    for i in xrange(len(tablets)):
        if nodes[i].startswith(node):
            print tablets[i]["tablet_id"]

def write_rate():
    table = sys.argv[1]
    tablets = yt.get("{0}/@tablets".format(table))
    #for tablet in tablets:
    #    print tablet["tablet_id"], tablet["performance_counters"]["dynamic_row_write_data_weight_10m_rate"]
    cells = [t["cell_id"] for t in tablets]
    nodes = ask_for_each(cells, lambda x: {"command": "get", "parameters": {"path": "#{0}/@peers/0/address".format(x)}})
    node_map = {}
    for i in xrange(len(tablets)):
        node = nodes[i]
        tablet = tablets[i]
        node_map[node] = node_map.get(node, 0) + tablet["performance_counters"]["dynamic_row_write_data_weight_1h_rate"]
    result = sorted(node_map.items(), key = lambda x: x[1])
    for res in result:
        print res



def ee():
    node = sys.argv[1]
    cells = [c["cell_id"] for c in yt.get("//sys/nodes/{0}:9012/@tablet_slots".format(node)) if "cell_id" in c]
    cell = cells[0]
    result = {}
    total = 0
    for cell in cells:
    #for cell in ["16352-6b096-3fc02bc-99cbbf4a"]:
        tablets = yt.get("#{0}/@tablet_ids".format(cell))
        tables = ask_attr_for_each(tablets, "table_id")
        paths = ask_attr_for_each(tables, "path")
        pool_size = ask_attr_for_each(tablets, "statistics/dynamic_memory_pool_size")

        for tablet, path, pool in zip(tablets, paths, pool_size):
            #print tablet, path, pool
            result[path] = result.get(path, 0) + pool
            total += pool
   
        #print cell, sum(pool_size) 

    result = sorted(result.items(), key = lambda x: x[1])
    for res in result:
        print res
    print total

def uu():
    #path = "//sys/nodes/p1388i.banach.yt.yandex.net:9012/orchid/tablet_cells/16352-6b096-3fc02bc-99cbbf4a/tablets/170a2-11369b-3fc02be-a2834694/stores"
    path="//sys/nodes/p1388i.banach.yt.yandex.net:9012/orchid/tablet_cells/16352-66eb3-3fc02bc-ee5b34a7/tablets/170a2-11369b-3fc02be-c12c033b/stores"
    stores = yt.list(path)
    rows = ask_get_for_each(path + "/", stores, "/row_count")
    for store, row in zip(stores, rows):
        print store, row
    print sum(rows)

def ww():
    #chunklist = sys.argv[1]
    #chunks = yt.get("#{0}/@child_ids".format(chunklist))
    #logical = ask_attr_for_each(chunks, "statistics/logical_row_count")
    #print sum(logical), yt.get("#{0}/@statistics/logical_row_count".format(chunklist))

    table = sys.argv[1]
    tablets = [tablet["tablet_id"] for tablet in yt.get(table + "/@tablets")]
    trimmed = ask_attr_for_each(tablets, "trimmed_row_count")
    chunk_list_ids = ask_attr_for_each(tablets, "chunk_list_id")
    logical = ask_attr_for_each(chunk_list_ids, "statistics/logical_row_count")
    for x in zip(tablets, trimmed, logical):
        #if x[1] != x[2]:
            print x

def list_top_tables():
    bundle = sys.argv[1]
    counter = sys.argv[2]
    cells = yt.get("//sys/tablet_cell_bundles/{0}/@tablet_cell_ids".format(bundle))
    tablets = sum(ask_attr_for_each(cells, "tablet_ids"), [])
    paths = ask_attr_for_each(ask_attr_for_each(tablets, "table_id"), "path")
    performance = ask_attr_for_each(tablets, "performance_counters/{0}".format(counter))
    result = {}
    for path, count in zip(paths, performance):
        result[path] = result.get(path, 0) + count
    for res in sorted(result.items(), key = lambda x: x[1]):
        print res[0], res[1]

def list_unpreloaded_tables():
    bundle = sys.argv[1]
    cells = yt.get("//sys/tablet_cell_bundles/{0}/@tablet_cell_ids".format(bundle))
    tablets = sum(ask_attr_for_each(cells, "tablet_ids"), [])
    tables = list(set(ask_attr_for_each(tablets, "table_id")))
    memories = ask_attr_for_each(tables, "in_memory_mode")
    paths = ask_attr_for_each(tables, "path")
    fails = ask_attr_for_each(tables, "tablet_statistics/preload_pending_store_count")
    for path, inm, fail in zip(paths, memories, fails):
        if inm == "compressed" or inm == "uncompressed":
            print path, fail

def aggr_chunks():
    chunkfile = open(sys.argv[1], "r")
    chunks = []
    for chunk in chunkfile:
        chunks.append(chunk.strip())
    sizes = ask_attr_for_each(chunks, "compressed_data_size")
    owners = ask_attr_for_each(chunks, "owning_nodes")
    tables = {}
    for owner, size in zip(owners, sizes):
        if size is None: continue
        owner = str(owner[0]) if owner is not None else "none"
        tables[owner] = tables.get(owner, 0) + size
    for res in sorted(tables.items(), key = lambda x: x[1]):
        print res[0], res[1]

def heal_replica():
    parent="//home/yabs/stat/replica"
    for table in yt.list(parent):
        yt.mount_table(parent + "/" + table)
        replicas = yt.get(parent + "/" + table + "/@replicas")
        for replica, data in replicas.items():
        #    #print replica, data["state"]
        #    #if data["mode"] == "sync":
            yt.alter_table_replica(replica, True)

def list_memory_tablets():
    cell = sys.argv[1]
    tablets = yt.get("#{0}/@tablet_ids".format(cell))
    memory = ask_attr_for_each(tablets, "statistics/memory_size")
    for tablet, size in zip(tablets, memory):
        if size != 0:
            print tablet, size

def list_memory_tables():
    bundle = sys.argv[1]
    cells = yt.get("//sys/tablet_cell_bundles/{0}/@tablet_cell_ids".format(bundle))
    tablets = sum(ask_attr_for_each(cells, "tablet_ids"), [])
    memory = ask_attr_for_each(tablets, "statistics/memory_size")
    table_ids = ask_attr_for_each(tablets, "table_id")
    tables = list(set(table_ids))
    table_paths = ask_attr_for_each(tables, "path")
    paths = dict(zip(tables, table_paths))
    r = {}
    for tablet, table_id, size in zip(tablets, table_ids, memory):
        if size and size != 0:
            path = paths[table_id]
            r[path] = r.get(path, 0) + size
    for t in sorted(r.items(), key=lambda x: x[1]):
        print t[0], humansize(t[1])

def list_memory_cells():
    node = sys.argv[1]
    path="//sys/nodes/{0}:9012".format(node)
    cells = [s["cell_id"] for s in yt.get(path + "/@tablet_slots") if "cell_id" in s.keys()]
    memory = ask_attr_for_each(cells, "total_statistics/memory_size")
    bundles = ask_attr_for_each(cells, "tablet_cell_bundle")
    for cell, bundle, size in zip(cells, bundles, memory):
        print cell, bundle, size

def bundle_distribution():
    cells = yt.get("//sys/tablet_cells")
    bundles = ask_attr_for_each(cells, "tablet_cell_bundle")
    memory_sizes = ask_attr_for_each(cells, "total_statistics/memory_size")
    cell_to_bundle = dict(zip(cells, bundles))
    cell_to_memory = dict(zip(cells, memory_sizes))
    #print cell_to_bundle
    nodes = yt.get("//sys/nodes")
    slots = ask_get_for_each("//sys/nodes/", nodes, "/@tablet_slots")
    for node, slots in zip(nodes, slots):
        if not slots:
            continue
        d = {}
        busy = 0
        for slot in slots:
            if "cell_id" in slot:
                busy += 1
                bundle = cell_to_bundle[slot["cell_id"]]
                dd = d.setdefault(bundle, [0, []])
                dd[0] += 1
                dd[1].append(humansize(cell_to_memory[slot["cell_id"]]))
        print node, "{0}/{1}".format(busy, len(slots)), d

def bundle_memory():
    cells = yt.get("//sys/tablet_cells")
    bundles = ask_attr_for_each(cells, "tablet_cell_bundle")
    memory_sizes = ask_attr_for_each(cells, "total_statistics/memory_size")
    cell_to_bundle = dict(zip(cells, bundles))
    cell_to_memory = dict(zip(cells, memory_sizes))
    memory = {}
    for cell in cells:
        bundle = cell_to_bundle[cell]
        memory[bundle] = memory.get(bundle, 0) + cell_to_memory[cell];
    for bundle, size in memory.items():
        print bundle, size

def journal_distribution():
    bundle = sys.argv[1]
    cells = yt.get("//sys/tablet_cell_bundles/{0}/@tablet_cell_ids".format(bundle))
    changelogs = ask_get_for_each("//sys/tablet_cells/", cells, "/changelogs")
    changelogs = [max(log) for log in changelogs]
    changelogs = ["{0}/changelogs/{1}".format(x[0],x[1]) for x in zip(cells,changelogs)]
    chunks = ask_get_for_each("//sys/tablet_cells/", changelogs, "/@chunk_ids")
    chunks = [c[-1] for c in chunks]
    chunk_replicas = ask_attr_for_each(chunks, "stored_replicas")
    count = {}
    for replicas in chunk_replicas:
        for replica in replicas:
            replica = str(replica)
            count[replica] = count.get(replica, 0) + 1

    #for res in reversed(sorted(count.items(), key = lambda x: x[1])):
    #    print res[0], res[1]

    nodes = count.keys()
    sessions = ask_get_for_each("//sys/nodes/", nodes, "/@statistics/total_session_count")
    io_weights = ask_get_for_each("//sys/nodes/", nodes, "/@io_weights")
    tags = ask_get_for_each("//sys/nodes/", nodes, "/@tags")

    for node, weights in zip(nodes, io_weights):
        count[node] /= weights["ssd_journals_xurma"]
    for res in reversed(sorted(count.items(), key = lambda x: x[1])):
        print res[0], res[1]

    print "\n\n"
    for res in reversed(sorted(zip(nodes, sessions, io_weights, tags), key = lambda x: x[1])):
        print res[0], res[1], res[2]

    racks = {}
    for node, tags in zip(nodes, tags):
        for tag in tags:
            if tag.startswith("MAN5"):
                if tag not in racks:
                    racks[tag] = []
                racks[tag].append(node)

    for rack, nodes in racks.items():
        print rack, "|".join([node[:6] for node in nodes])

def list_banned_nodes():
    nodes = yt.get("//sys/nodes", attributes=["banned", "ban_message"])
    for node, attributes in nodes.items():
        if attributes.attributes.get("banned", False):
            message = attributes.attributes.get("ban_message", "")
            print node, message

def force_compact_table(table):
    yt.set(table + "/@forced_compaction_revision", yt.get(table + "/@revision"))
    yt.set(table + "/@forced_compaction_revision", yt.get(table + "/@revision"))
    yt.remount_table(table)

def force_compact():
    table = sys.argv[1]
    force_compact_table(table)

def compact_all_tables():
    path = sys.argv[1]
    tables = yt.search(path, node_type="table", depth_bound=1)
    for table in tables:
        force_compact_table(table)

def prepare_balance():
    def p1():
        bundles = ["default", "modadvert", "turborss", "news-storage", "news-queue", "images-ultra"]
        tag = "tablet_common"
        for bundle in bundles:
            yt.set("//sys/tablet_cell_bundles/{0}/@node_tag_filter".format(bundle), tag + "/" + bundle)
            print yt.get("//sys/tablet_cell_bundles/{0}/@node_tag_filter".format(bundle))
    def p2():
        bundles = yt.get("//sys/tablet_cell_bundles")
        for bundle in bundles:
            yt.set("//sys/tablet_cell_bundles/{0}/@enable_bundle_balancer".format(bundle), False)
    p2()

def list_bundles():
    attrs = ["node_tag_filter", "enable_bundle_balancer", "enable_resource_reclaim"]
    bundles = yt.get("//sys/tablet_cell_bundles", attributes=attrs)
    for bundle, attributes in bundles.items():
        aa = [attributes.attributes.get(a, None) for a in attrs]
        print bundle, aa

def repair_bundles():
    attrs = ["node_tag_filter", "enable_bundle_balancer", "enable_resource_reclaim"]
    bundles = yt.get("//sys/tablet_cell_bundles", attributes=attrs)
    invalid_tag = "&invalid_tag"

    for bundle, attributes in bundles.items():
        aa = [attributes.attributes.get(a, None) for a in attrs]
        f = "(" +  aa[0] + ")" + invalid_tag if aa[0] else "invalid_tag"
        print bundle, aa, "new tag:", f
        yt.set("//sys/tablet_cell_bundles/{0}/@node_tag_filter".format(bundle), f)

    time.sleep(5)
    for bundle, attributes in bundles.items():
        aa = [attributes.attributes.get(a, None) for a in attrs]
        #print aa
        f = aa[0][1:-len(invalid_tag)-1] if aa[0] is not "invalid_tag" else ""
        print bundle, "restore tag: ", f
        yt.set("//sys/tablet_cell_bundles/{0}/@node_tag_filter".format(bundle), f)

def list_nodes():
    #tag = sys.argv[1]
    #all_cells = []
    nodes = yt.get("//sys/nodes", attributes=["tags", "user_tags", "tablet_slots", "banned", "ban_message", "decommissioned"])
    for node, attributes in nodes.items():
        print node, attributes.attributes.get("user_tags", [])
        #yt.set("//sys/nodes/{0}/@user_tags".format(node), [])
        #if tag in attributes.attributes.get("tags",{}):
        #    cells = attributes.attributes.get("tablet_slots", [])
        #    #print node, cells
        #    count = 0
        #    for cell in cells:
        #        if "cell_id" in cell:
        #            all_cells.append(cell["cell_id"])
        #            count += 1
        #    print node, count, len(cells),  attributes.attributes.get("banned", False),  attributes.attributes.get("decommissioned", False),  attributes.attributes.get("ban_message", "")

    #bundles = ask_attr_for_each(all_cells, "tablet_cell_bundle")
    #for cell, bundle in zip(all_cells, bundles):
    #    print cell, bundle

def find_tablets_on_node():
    table = sys.argv[1]
    node = sys.argv[2]
    tablets = yt.get(table + "/@tablets")
    #print tablets[0]
    cells = [t["cell_id"] for t in tablets if "cell_id" in t]
    peers = ask_attr_for_each(cells, "peers/0/address")
    for t, p in zip(tablets, peers):
        if p.startswith(node):
            print t["tablet_id"], t["performance_counters"]["dynamic_row_lookup_rate"], t["performance_counters"]["static_chunk_row_lookup_rate"]


def list_tablet_transactions():
    tablet_cells = yt.get("//sys/tablet_cells")
    peers = ask_attr_for_each(tablet_cells, "peers/0/address")
    transactions = ask_for_each(zip(peers, tablet_cells), lambda x: {"command": "get", "parameters": {"path": "//sys/nodes/{0}/orchid/tablet_cells/{1}/transactions".format(*x)}})
    for txses, cell, peer in zip(transactions, tablet_cells, peers):
        for key, tx in txses.items():
            print peer, cell, key

def old_transactions():
    #bundle = sys.argv[1]
    #tablet_cells = yt.get("//sys/tablet_cell_bundles/{0}/@tablet_cell_ids".format(bundle))
    tablet_cells = yt.get("//sys/tablet_cells")
    #tablet_cells = ["69c5-69f3-40402bc-2eabee2c", "69c5-676e-40402bc-e0893f07", "69c5-67e8-40402bc-7811ee0", "69c5-69c0-40402bc-76a64afa"]
    peers = ask_attr_for_each(tablet_cells, "peers/0/address")
    transactions = ask_for_each(zip(peers, tablet_cells), lambda x: {"command": "get", "parameters": {"path": "//sys/nodes/{0}/orchid/tablet_cells/{1}/transactions".format(*x)}})
    old_txses = []
    for txses, cell, peer in zip(transactions, tablet_cells, peers):
        for key, tx in txses.items():
            import datetime
            #date = datetime.datetime.fromtimestamp(tx["prepare_timestamp"] >> 30)
            date = datetime.datetime.fromtimestamp(tx["start_timestamp"] >> 30)
            if date < datetime.datetime.now() - datetime.timedelta(0,2*3600):
            #if True:
                old_txses.append({
                    "transaction_id": key,
                    "prepare_date": date.strftime("%Y-%m-%d %H:%M"),
                    "info": tx,
                    "cell_id": cell,
                    "peer": peer})

    for tx in old_txses:
        for k, v in tx.items():
            print k, v

def old_cells():
    bundle = sys.argv[1]
    cells = yt.get("//sys/tablet_cell_bundles/{0}/@tablet_cell_ids".format(bundle))
    times = ask_get_for_each("//sys/tablet_cells/", cells, "/snapshots/@creation_time")
    for c, t in zip(cells, times):
        print c, t

def slow_replicas():
    cells = yt.get("//sys/tablet_cells")
    replicated_tables = []
    i = 0
    for cell in cells:
        sys.stdout.write("\rCell {0} of {1}".format(i, len(cells)))
        i += 1
        tablets = yt.get("#" + cell + "/@tablet_ids")
        tables = ask_attr_for_each(tablets, "table_id")
        tables = list(set(tables))
        types = ask_attr_for_each(tables, "type")
        for table, typ in zip(tables, types):
            if typ == "replicated_table":
                replicated_tables.append(table)
    print
    replicas = ask_attr_for_each(replicated_tables, "replicas")
    slow_tables = []
    slow_replicas = []
    for replica, table in zip(replicas, replicated_tables):
        for replica_id, r in replica.items():
            lag = r["replication_lag_time"]
            if lag > 1000*3600*2 and lag < 1000*3600*20:
               slow_replicas.append([replica_id, r])
               slow_tables.append(table)

    paths = ask_attr_for_each(slow_tables, "path")
    for path, replica in zip(paths, slow_replicas):
        print path
        print yson.dumps(replica, yson_format="pretty")

def abort_transactions():
    txses = yt.get("//sys/topmost_transactions")
    res = ask_for_each(txses, lambda x: {"command": "abort_tx", "parameters": {"transaction_id": "#{0}".format(x)}})
    print res

def investigate_large_partition():
    table = "//home/js_tracer/day_by_day/29-05-2018"
    tablets = yt.get(table + "/@tablets")
    chunk_list = "1dbcd-13948f-3fc0065-869aac17"
    childs = yt.get("#" + chunk_list + "/@child_ids")
    sizes = ask_attr_for_each(childs, "compressed_data_size")
    sizes = map(humansize, sizes)
    min_keys = ask_attr_for_each(childs, "min_key")
    max_keys = ask_attr_for_each(childs, "max_key")
    ctimes = ask_attr_for_each(childs, "creation_time")
    s = list(zip(ctimes, min_keys, max_keys, childs, sizes))
    for r in sorted(s):
        print r
    pivot_keys = [tablets[8]["pivot_key"]] + sorted(min_keys)
    print pivot_keys
    print sorted(min_keys)
    yt.reshard_table(table, first_tablet_index=8, last_tablet_index=8, pivot_keys=sorted(min_keys))

def build_cell_snapshots():
    import threading
    from yt.wrapper.native_driver import get_driver_instance

    def build_snapshot(cell):
        get_driver_instance(None).build_snapshot(cell_id=cell)
        print cell
        return

    cells = yt.get("//sys/tablet_cells")
    threads = [] 
    for cell in cells:
        threads.append(threading.Thread(target=build_snapshot, args=(cell,)))
        threads[-1].start()

    for t in threads:
        t.join()

def unflushed_statistics():
    table = sys.argv[1]
    tablets = yt.get(table + "/@tablets")
    unflushed_tses = ask_attr_for_each(tablets, "unflushed_timestamp")
    retained_tses = ask_attr_for_each(tablets, "retained_timestamp")

    for t, u, r in zip(tablets, unflushed_tses, retained_tses):
        def pts(ts):
            print datetime.datetime.fromtimestamp(ts>>30).strftime("%Y-%m-%d %H:%M:%S")
        print t, pts(u), pts(r)

def markov_unmounted():
    filename = sys.argv[1]
    tables= []
    with open(filename, "r") as f:
        for table in f.readlines():
            tables.append(table.strip())
    print tables
    tables_tablets = ask_attr_for_each(tables, "tablets")
    unmounting = []
    cells = []
    for tablets in tables_tablets:
        for tablet in tablets:
            if tablet["state"] == "unmounting":
                unmounting.append[tablet["tablet_id"]]
                cells.append[tablet["cell_id"]]
    peers = ask_attr_for_each(cells, "peers/0/address")
    xxx = zip(peers, cells, unmounting)
    xxx_stores = ask_for_each(exts, lambda x: {"command": "get", "parameters": {"path": "//sys/nodes/{0}/orchid/tablet_cells/{1}/tablets/{2}/stores".format(x[0], x[1], x[2])}})
    yyy = []
    for stores in zip(xxx, xxx_stores):
        for store in stores[1]:
            yyy.append(list(stores[0]) + [store])
    states = ask_for_each(yyy, lambda x: {"command": "get", "parameters": {"path": "//sys/nodes/{0}/orchid/tablet_cells/{1}/tablets/{2}/stores/{3}".format(x[0], x[1], x[2], x[3])}})
    for state in zip(yyy, states):
        if state["store_state"] != "persistent":
            print state

def force_commit_tx():
    bundle = sys.argv[1]
    tx = sys.argv[2]
    cells = yt.get("//sys/tablet_cell_bundles/{0}/@tablet_cell_ids".format(bundle))
    peers = ask_attr_for_each(cells, "peers/0/address")
    for cell, peer in zip(cells, peers):
        #sp.call(["./force_abort_tx", peer, cell, tx])
        print ["./force_abort_tx", peer, cell, tx]
        


if __name__ == "__main__":
    #tt() 
    #ttbundle() 
    #table_distribution()
    #count_tablets_per_table()
    #f()
    #fair_mount()
    #list_tablet_nodes()
    #misconfigured()
    #reshard()
    #du_journals()
    #cell_tablet_dyncmic_statistics(sys.argv[1])
    #overlapping_stores_statistics()
    #investigate()
    #dynamic_store_size()
    #free_cell()
    #remove_cell()
    #remove_cells()
    #decommission_cells()
    #remove_decommissioned_cells()
    #list_tablets_on_node()
    #write_rate()
    #ee()
    #uu()
    #ww()
    #list_top_tables()
    #aggr_chunks()
    #heal_replica()
    #list_memory_tablets()
    #list_memory_tables()
    #list_memory_cells()
    #remove_cells_from_node()
    #bundle_distribution()
    #bundle_memory()
    #journal_distribution()
    #list_banned_nodes()
    #compact_all_tables()
    #force_compact()
    #list_bundle_nodes()
    #prepare_balance()
    #list_bundles()
    #repair_bundles()
    #list_nodes()
    #find_tablets_on_node()
    #list_unpreloaded_tables()
    #old_transactions()
    #old_cells()
    #list_tablet_transactions()
    #abort_transactions()
    #slow_replicas()
    #investigate_large_partition()
    #build_cell_snapshots()
    #unflushed_statistics()
    #markov_unmounted()
    #force_commit_tx()
