#!/usr/bin/python

import yt.wrapper as yt
import yt.transfer_manager.client as tm
import yt.yson as yson
from yt.wrapper.driver import make_request

import argparse
from datetime import datetime
import time

def transfer(src_cluster, src_path, dst_cluster, dst_path):
    args = {
        "source_cluster": src_cluster,
        "source_table": src_path,
        "destination_cluster": dst_cluster,
        "destination_table": dst_path,
    }

    task = tm.add_task(params={"copy_spec": {"network_name": "default"}}, **args)
    print "Add transer manager task", args, task

    info = tm.get_task_info(task)
    while info["state"] != "completed" and info["state"] != "failed":
        print "Wait until task is completed", info
        time.sleep(5)
        info = tm.get_task_info(task)

    assert info["state"] == "completed"

def safe_add_new_replica(args, freezes, tmp_objects):
    replicated_table = args.table
    replica_cluster = args.replica_cluster
    replica_path = args.replica_path
    source_replica_id = args.source_replica
    force = args.force
    temp_prefix = args.temp_prefix

    def get_src_replica():
        return  yt.get(replicated_table + "/@replicas/" + source_replica_id)

    source_replica = get_src_replica()

    yt_source = yt.YtClient(proxy=source_replica["cluster_name"])
    yt_destination = yt.YtClient(proxy=replica_cluster)

    print "Freezing replica", source_replica["replica_path"]
    yt_source.freeze_table(source_replica["replica_path"], sync=True)
    freezes.append([source_replica["cluster_name"], source_replica["replica_path"]])

    dump_table = yt_source.create_temp_table(prefix=temp_prefix)
    assert dump_table != None
    tmp_objects["dump_table"] = [source_replica["cluster_name"], dump_table]

    assert yt_source.get(source_replica["replica_path"] + "/@tablet_state") == "frozen"

    print "Dump replica", source_replica["replica_path"], "to", dump_table
    if yt_source.exists(source_replica["replica_path"] + "/@optimze_for"):
        yt_source.set(dump_table + "/@optimize_for", yt_source.get_attribute(source_replica["replica_path"], "optimize_for"))
    yt_source.alter_table(dump_table, schema=yt_source.get_attribute(source_replica["replica_path"], "schema"))

    # TODO: start transaction and take lock here. After lock is aquired replica can be unfrozen.
    op = yt_source.run_merge(source_replica["replica_path"], dump_table, mode="ordered", sync=False)

    state = op.get_state()
    while not state.is_finished() and not state.is_unsuccessfully_finished() and not state.is_running():
        print "Wait until operation is started", str(op), state
        time.sleep(1)
        state = op.get_state()

    # Alternative: get current_replication_row_indexes from orchid.
    # NB: This should be enough for us to get correct current_replication_row_indexes.
    # If error "Replication log row index mismatch" occur one should get these indeces from orchid.
    print "Wait for replica statistics (current_replication_row_index)"
    time.sleep(5)

    tablets = yt.get("#" + source_replica_id + "/@tablets")
    current_replication_row_indexes = [tablet["current_replication_row_index"] for tablet in tablets]
    print "Got replication row indexes: ", current_replication_row_indexes

    replica_id = yt.create("table_replica", attributes={
        "table_path": replicated_table,
        "cluster_name": replica_cluster,
        "replica_path": replica_path,
        "start_replication_row_indexes": current_replication_row_indexes})
    tmp_objects["replica"] = [args.proxy, "#" + replica_id]
    print "Created new replica", replica_id

    print "Unfreeze replica", source_replica["replica_path"]
    yt_source.unfreeze_table(source_replica["replica_path"])
    freezes.pop(0)

    print "Waiting for dump to complete"
    op.wait()

    state = op.get_state()
    print "Operation finished", op, state
    assert state.is_finished()

    print "Copying dumped replica to new place"

    if yt_destination.exists(replica_path):
        if not force:
            raise "Table exists", replica_cluster, replica_path
        print "Remove dst table if exists", replica_cluster, replica_path
        yt_destination.remove(replica_path, force=True)
    tmp_objects["replica_table"] = [replica_cluster, replica_path]

    print "Set new attributes for", replica_cluster, replica_path

    yt_destination.create("table", replica_path, attributes={"external": False})
    transfer(source_replica["cluster_name"], dump_table, replica_cluster, replica_path)

    builtin_attributes = [
        "account",
        "optimize_for",
        "in_memory_mode",
        "atomicity",
        "commit_ordering",
        "tablet_cell_bundle",
        "min_tablet_size",
        "max_tablet_size",
        "desired_tablet_size",
        "desired_tablet_count", 
        "enable_tablet_balancer"]
    user_attributes = yt_source.get(source_replica["replica_path"] + "/@user_attribute_keys")
    user_attributes.pop("forced_compaction_revision", None)
    for attr in builtin_attributes + user_attributes:
        if yt_source.exists(source_replica["replica_path"] + "/@" + attr):
            value = yt_source.get(source_replica["replica_path"] + "/@" + attr)
            yt_destination.set(replica_path + "/@" + attr, value)
            print "Set attribute", attr, "value", value

    print "Run merge to fix chunk and block sizes"
    yt_destination.run_merge(replica_path, replica_path, mode="ordered", spec={
        "force_transform": True,
        "job_io": {"table_writer": {"block_size": 256 * 2**10, "desired_chunk_size": 100 * 2**20}}})

    # Check because there were some problems with this attribute.
    assert yt_destination.get(replica_path + "/@optimize_for") == yt_source.get(source_replica["replica_path"] + "/@optimize_for")

    print "Table copied, alter and reshard table"
    yt_destination.alter_table(replica_path, dynamic=True)
    pivots = yt_source.get(source_replica["replica_path"] + "/@pivot_keys")
    yt_destination.reshard_table(replica_path, pivot_keys=pivots)

    print "Adding upstream_replica_id for new replica table"
    make_request("alter_table", {"path": replica_path, "upstream_replica_id": replica_id}, client=yt_destination)

    print "Mounting new replica table", replica_path
    yt_destination.mount_table(replica_path, sync=True)

    print "Enabling new replica", replica_id
    yt.alter_table_replica(replica_id, True)

    tmp_objects.pop("replica")
    tmp_objects.pop("replica_table")

    print "SUCCESS!!!!!!"

def add_new_replica(args):
    replicated_table = args.table
    replica_cluster = args.replica_cluster
    replica_path = args.replica_path
    source_replica_id = args.source_replica
    force = args.force

    freezes = []
    tmp_objects = {}
    success = True
    try:
        safe_add_new_replica(args, freezes, tmp_objects)
    except Exception as exc:
        print "Failed"
        print exc
        success = False
        pass
    except:
        pass

    if len(freezes) > 0:
        print "Executed abnormally, unfreeze tables: ", freezes
        for cluster, table in freezes:
            ytc = yt.YtClient(proxy=cluster)
            ytc.unfreeze_table(table)

    if len(tmp_objects) > 0:
        print "Remove temporary tables:", tmp_objects
        for cluster, obj in tmp_objects.values():
            ytc = yt.YtClient(proxy=cluster)
            while True:
                try:
                    ytc.remove(obj)
                    break
                except Exception:
                    time.sleep(10)
                    pass

    return success

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--proxy", type=str, required=True, help="YT proxy")
    parser.add_argument("--table", type=str, required=True, help="Replicated table path")
    parser.add_argument("--replica-cluster", type=str, required=True, help="Replica cluster name")
    parser.add_argument("--replica-path", type=str, required=True, help="Replica cluster path")
    parser.add_argument("--source-replica", type=str, required=True, help="Use specific replica")
    parser.add_argument("--temp-prefix", type=str, default="//tmp/", help="Use specific replica")
    parser.add_argument("--force", action="store_true", default=False, help="Remove replica table if exists")

    args = parser.parse_args()
    yt.config.set_proxy(args.proxy)

    add_new_replica(args)

