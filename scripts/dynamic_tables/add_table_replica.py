#!/usr/bin/python

# Wiki: https://wiki.yandex-team.ru/yt/userdoc/dynamicreplicatedtables/#kopirovaniesushhestvujushhejjrepliki

from datetime import datetime
import argparse
import logging
import sys
import time
import traceback


try:
    import yt.wrapper as yt
    import yt.yson as yson
    from yt.wrapper.driver import make_request
except:
    print>>sys.stderr, "Please install YT python client: https://wiki.yandex-team.ru/yt/userdoc/pythonwrapper"
    exit(1)

try:
    import yt.transfer_manager.client as tm
except:
    print>>sys.stderr, "Please install transfer manager python client: https://wiki.yandex-team.ru/yt/userdoc/transfer_manager/client/"
    exit(1)


EPILOG="""Example: given a replica from markov to banach, make its copy pointing to arnold.

{} --proxy markov \\
    --table //path/on/markov \\
    --replica-cluster arnold \\
    --replica-path //path/on/arnold \\
    --source-replica-cluster banach
""".format(sys.argv[0])


def transfer(src_cluster, src_path, dst_cluster, dst_path):
    args = {
        "source_cluster": src_cluster,
        "source_table": src_path,
        "destination_cluster": dst_cluster,
        "destination_table": dst_path,
    }

    task = tm.add_task(params={"copy_spec": {"network_name": "default"}}, **args)
    logging.info("Add transfer manager task (Id: %s)\n%s", task, args)

    info = tm.get_task_info(task)
    while info["state"] != "completed" and info["state"] != "failed":
        logging.debug("Wait until task is completed\n%s\n", info)
        time.sleep(5)
        info = tm.get_task_info(task)

    assert info["state"] == "completed"


def get_replica_id(table, source_replica_cluster):
    try:
        replicas = yt.get(table + "/@replicas")
    except:
        logging.error("Failed to fetch replicas (Table: %s)", table)
        exit(1)

    replicas_to_cluster = [r for r in replicas if replicas[r]["cluster_name"] == source_replica_cluster]
    if len(replicas_to_cluster) == 0:
        logging.error("No replicas from table to cluster (Table: %s, Cluster: %s)", table, source_replica_cluster)
        exit(1)
    if len(replicas_to_cluster) > 1:
        logging.error("Too many replicas from table to cluster, unable to choose a single one "
            "(Table: %s, Cluster: %s)", table, source_replica_cluster)
        exit(1)
    return replicas_to_cluster[0]


def copy_attributes(yt_source, src_table, yt_destination, dst_table):
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
    user_attributes = yt_source.get(src_table + "/@user_attribute_keys")
    if "forced_compaction_revision" in user_attributes:
        user_attributes.remove("forced_compaction_revision")

    for attr in builtin_attributes + user_attributes:
        if yt_source.exists(src_table + "/@" + attr):
            value = yt_source.get(src_table + "/@" + attr)
            logging.debug("Set attribute to new replica (Attrubute: %s, Value: %s)", attr, value)
            try:
                yt_destination.set(dst_table + "/@" + attr, value)
            except:
                logging.warn("Failed to set attribute (Table: %s, Attribute: %s, Value: %s)", dst_table, attr, value)


def safe_add_new_replica(args, freezes, tmp_objects):
    replicated_table = args.table
    replica_cluster = args.replica_cluster
    replica_path = args.replica_path
    force = args.force
    temp_prefix = args.temp_prefix
    preserve_timestamp_order = args.preserve_timestamp_order

    if args.source_replica is not None:
        source_replica_id = args.source_replica
    else:
        source_replica_id = get_replica_id(replicated_table, args.source_replica_cluster)
    source_replica = yt.get(replicated_table + "/@replicas/" + source_replica_id)

    yt_source = yt.YtClient(proxy=source_replica["cluster_name"])
    yt_destination = yt.YtClient(proxy=replica_cluster)


    if preserve_timestamp_order:
        freezes["replicated_table"] = [None, replicated_table]
        yt.freeze_table(replicated_table, sync=True)

        logging.info("Wait for all data to be replicated")
        while True:
            tablets = yt.get("#" + source_replica_id + "/@tablets")
            if all(tablet["flushed_row_count"] == tablet["current_replication_row_index"] for tablet in tablets):
                break
            else:
                time.sleep(1)

    logging.info("Freeze replica %s", source_replica["replica_path"])
    freezes["source_replica"] = [source_replica["cluster_name"], source_replica["replica_path"]]
    yt_source.freeze_table(source_replica["replica_path"], sync=True)
    assert yt_source.get(source_replica["replica_path"] + "/@tablet_state") == "frozen"

    dump_table = yt_source.create_temp_table(prefix=temp_prefix)
    assert dump_table != None
    tmp_objects["dump_table"] = [source_replica["cluster_name"], dump_table]


    logging.info("Dump replica %s to %s", source_replica["replica_path"], dump_table)
    if yt_source.exists(source_replica["replica_path"] + "/@optimize_for"):
        yt_source.set(
            dump_table + "/@optimize_for",
            yt_source.get(source_replica["replica_path"] + "/@optimize_for"))
    yt_source.alter_table(dump_table, schema=yt_source.get(source_replica["replica_path"] + "/@schema"))

    # TODO: start transaction and take lock here. After lock is aquired replica can be unfrozen.
    op = yt_source.run_merge(source_replica["replica_path"], dump_table, mode="ordered", sync=False)

    state = op.get_state()
    while not state.is_finished() and not state.is_unsuccessfully_finished() and not state.is_running():
        logging.debug("Wait until operation is started (OperationId: %s, State: %s)", op.id, state)
        time.sleep(1)
        state = op.get_state()


    # Alternative: get current_replication_row_indexes from orchid.
    # NB: This should be enough for us to get correct current_replication_row_indexes.
    # If error "Replication log row index mismatch" occur one should get these indexes from orchid.
    logging.info("Wait for replica statistics (current_replication_row_index)")
    time.sleep(5)

    tablets = yt.get("#" + source_replica_id + "/@tablets")
    current_replication_row_indexes = [tablet["current_replication_row_index"] for tablet in tablets]
    logging.debug("Got replication row indexes: %s", current_replication_row_indexes)

    replica_id = yt.create("table_replica", attributes={
        "table_path": replicated_table,
        "cluster_name": replica_cluster,
        "replica_path": replica_path,
        "start_replication_row_indexes": current_replication_row_indexes})
    tmp_objects["replica"] = [args.proxy, "#" + replica_id]
    logging.info("Created new replica (ReplicaId: %s)", replica_id)


    logging.info("Unfreeze replica %s", source_replica["replica_path"])
    yt_source.unfreeze_table(source_replica["replica_path"])
    freezes.pop("source_replica")


    logging.info("Wait for dump to complete")
    op.wait()

    state = op.get_state()
    logging.info("Operation finished (OperationId: %s, State: %s)", op.id, state)
    assert state.is_finished()


    logging.info("Copy dumped replica to new place")
    if yt_destination.exists(replica_path):
        if not force:
            logging.error("Table exists, use --force to override (Cluster: %s, Path: %s)", replica_cluster, replica_path)
            raise yt.YtError("Table exists")
        logging.info("Remove dst table if exists (Cluster: %s, Path: %s)", replica_cluster, replica_path)
        yt_destination.remove(replica_path, force=True)
    tmp_objects["replica_table"] = [replica_cluster, replica_path]

    yt_destination.create("table", replica_path, attributes={"external": False})
    if source_replica["cluster_name"] == replica_cluster:
        logging.debug("Same cluster, run yt.move")
        yt_source.move(dump_table, replica_path, force=args.force)
        tmp_objects.pop("dump_table")
    else:
        logging.debug("Run transfer manager")
        transfer(source_replica["cluster_name"], dump_table, replica_cluster, replica_path)


    logging.info("Set new attributes for new replica (Cluster: %s, Table: %s)", replica_cluster, replica_path)
    copy_attributes(yt_source, source_replica["replica_path"], yt_destination, replica_path)

    logging.info("Run merge to fix chunk and block sizes")
    yt_destination.run_merge(replica_path, replica_path, mode="ordered", spec={
        "force_transform": True,
        "job_io": {"table_writer": {"block_size": 256 * 2**10, "desired_chunk_size": 100 * 2**20}}})

    # Check because there were some problems with this attribute.
    assert yt_destination.get(replica_path + "/@optimize_for") == yt_source.get(source_replica["replica_path"] + "/@optimize_for")

    logging.info("Table copied, alter and reshard table")
    yt_destination.alter_table(replica_path, dynamic=True)
    pivots = yt_source.get(source_replica["replica_path"] + "/@pivot_keys")
    yt_destination.reshard_table(replica_path, pivot_keys=pivots, sync=True)


    print yt_destination.get(replica_path + "/@")
    make_request("alter_table", {"path": replica_path, "upstream_replica_id": replica_id}, client=yt_destination)

    logging.info("Mount new replica table (Cluster: %s, Path: %s)", replica_cluster, replica_path)
    yt_destination.mount_table(replica_path, sync=True)

    logging.info("Enable new replica (ReplicaId: %s)", replica_id)
    yt.alter_table_replica(replica_id, True)


    if "replicated_table" in freezes:
        yt.unfreeze_table(replicated_table, sync=True)
        freezes.pop("replicated_table")

    tmp_objects.pop("replica")
    tmp_objects.pop("replica_table")

    logging.info("Success!")

def add_new_replica(args):
    freezes = {}
    tmp_objects = {}
    success = True
    try:
        safe_add_new_replica(args, freezes, tmp_objects)
    except Exception as exc:
        logging.error("Failed to add new replica")
        traceback.print_exc()
        success = False
    except:
        pass

    if len(freezes) > 0:
        logging.warn("Executed abnormally, unfreeze tables: %s", freezes)
        for cluster, table in freezes.values():
            ytc = yt.YtClient(proxy=cluster) if cluster is not None else yt
            ytc.unfreeze_table(table)

    if len(tmp_objects) > 0:
        logging.info("Remove temporary objects: %s", tmp_objects)
        for cluster, obj in tmp_objects.values():
            ytc = yt.YtClient(proxy=cluster) if cluster is not None else yt
            while True:
                try:
                    ytc.remove(obj)
                    break
                except Exception:
                    time.sleep(10)

    return success

if __name__ == "__main__":
    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter, epilog=EPILOG)
    parser.add_argument("--proxy", type=str, required=True, help="YT proxy")
    parser.add_argument("--table", type=str, required=True, help="path to the replicated table")
    parser.add_argument("--replica-cluster", type=str, required=True,
        help="new replica cluster name", metavar="CLUSTER")
    parser.add_argument("--replica-path", type=str, required=True,
        help="path to the new replica table", metavar="PATH")

    source_replica_desc = parser.add_mutually_exclusive_group(
        required=True)
    source_replica_desc.add_argument("--source-replica", type=str,
            help="id of the source replica", metavar="ID")
    source_replica_desc.add_argument("--source-replica-cluster", type=str,
            help="source replica cluster name", metavar="CLUSTER")

    parser.add_argument("--temp-prefix", type=str, default="//tmp/", help="directory for temp files")
    parser.add_argument("--force", action="store_true", default=False, help="overwrite destination path")
    parser.add_argument(
        "--no-preserve-timestamp-order",
        action="store_false",
        default=True,
        dest="preserve_timestamp_order",
        help="do not preserve strict timestamp order (do not use it unless you fully understand what are you doing)")

    parser.add_argument("-v", "--verbose", action="store_true", help="enable debug output")

    args = parser.parse_args()
    yt.config.set_proxy(args.proxy)

    logging.basicConfig(
        format="%(asctime)s - %(levelname)s - %(message)s",
        level=logging.DEBUG if args.verbose else logging.INFO)

    add_new_replica(args)

