import argparse
import functools
import logging
import sys
import time

from collections import defaultdict

import yt.wrapper as yt
from yt.wrapper import yson
from yt.wrapper.default_config import get_default_config, update_config_from_env

from yt.ypath.rich import RichYPath

EPILOG = """Examples:

{0} --proxy zeno \\
    --pipeline-path //path/on/zeno

# Everything at once: the pipeline tables (data replicas + replication logs) plus the
# replication logs of external state tables:
{0} --proxy pythia \\
    --pipeline-path //home/project/pipeline \\
    --also-chaos-replication-logs \\
    --external-table //home/project/profiles \\
    --external-table //home/project/counters
""".format(sys.argv[0])


def get_args():
    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter, epilog=EPILOG)

    parser.add_argument("--proxy", type=str, required=False, default=None, help="YT proxy")
    parser.add_argument("--pipeline-path", type=str, required=False, default=None, help="path to the flow pipeline")
    parser.add_argument(
        "--tablet-count",
        type=int,
        required=False,
        default=10,
        help="tablets per computation for pipeline tables; the total tablet count for an"
        " --external-table (which has no computations)",
    )
    parser.add_argument(
        "--table",
        choices=[
            "input_messages",
            "compact_input_messages",
            "compact_output_messages",
            "compact_partition_output_messages",
            "timers",
            "states",
            "partition_states",
            "partition_transactions",
        ],
        default=None,
        help="table to reshard",
    )
    parser.add_argument(
        "--also-chaos-replication-logs",
        action="store_true",
        help="also recreate the chaos replication log replicas with a width matching the data"
        " replica: freeze the log (further writes are rejected, but data replicas keep pulling),"
        " wait until every data replica has applied it, then drop and recreate it with the same"
        " attributes and re-attach it to the replication card. Writers see retryable commit errors"
        " for the duration of the swap; the pipeline does not have to be paused",
    )
    parser.add_argument(
        "--external-table",
        action="append",
        default=[],
        metavar="PATH",
        help="also reshard this table outside the pipeline directory (e.g. a user state table),"
        " to --tablet-count tablets, uniformly over its leading hash key column; repeatable."
        " Handled exactly like a pipeline data table: a plain dynamic table is resharded in"
        " place, a chaos table's data replicas are resharded, and with"
        " --also-chaos-replication-logs its replication log is recreated too",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="enable debug output")

    args = parser.parse_args()

    if args.pipeline_path is None and not args.external_table:
        parser.error("at least one of --pipeline-path and --external-table is required")

    def parse_rich_path(raw):
        path, path_attributes = RichYPath().parse(raw)
        if cluster := path_attributes.get("cluster"):
            assert args.proxy is None or args.proxy == cluster, "conflicting clusters in path attributes"
            args.proxy = cluster
        return path

    if args.pipeline_path is not None:
        args.pipeline_path = parse_rich_path(args.pipeline_path)
    args.external_table = [parse_rich_path(raw) for raw in args.external_table]

    return args


def build_compact_input_message_pivot_key(computation_id, key_hash=None):
    result = computation_id.encode("utf-8") + b"\0"
    if key_hash is not None:
        result += key_hash.to_bytes(8, "big")
    return [yson.YsonString(result)]


def key_sort_value(key):
    """Sort value ordering key tuples the way YT orders composite key values (see
    composite_compare.cpp): lexicographically, where a shorter prefix sorts first and columns of
    different types compare by the EValueType order - Int64 < Uint64 < Double < Boolean < String.
    Plain sorted() fails on this: a pipeline mid-release holds keys of both the old and the new
    layout at once, and int meets str at the same position."""

    def column_sort_value(column):
        if isinstance(column, bool):
            return (3, column)
        if isinstance(column, yson.YsonUint64):
            return (1, column)
        if isinstance(column, int):
            return (0, column)
        if isinstance(column, float):
            return (2, column)
        if isinstance(column, str):
            return (4, column.encode("utf-8"))
        if isinstance(column, bytes):
            return (4, column)
        raise TypeError(f"Unsupported key column type: {type(column)}")

    return [column_sort_value(column) for column in key]


def uniform_uint64_pivot_keys(tablet_count):
    """Explicit uniform pivots over a leading uint64 (hash) key column: reshard-by-count is
    rejected for replication logs, they demand pivot keys."""
    step = 2**64 // tablet_count
    return [[]] + [[yson.YsonUint64(i)] for i in range(step, 2**64 - step + 1, step)][: tablet_count - 1]


def _make_client_config():
    # Sync tablet operations (unmount/reshard/mount/freeze) poll for tablet readiness with a 60 s
    # default; under live pipeline load freezing or unmounting a hot table takes longer. Raise the
    # default before applying the environment, so an operator can still override it (along with
    # any other client setting) via YT_CONFIG_PATCHES.
    config = get_default_config()
    config["tablets_ready_timeout"] = 30 * 60 * 1000
    update_config_from_env(config)
    return config


@functools.lru_cache
def _replica_cluster_client(proxy):
    return yt.YtClient(proxy=proxy, config=_make_client_config())


def get_reshard_targets(client, table, make_client=None, warn_logs=True):
    """A chaos pipeline stores its internal tables as chaos_replicated_table nodes, which own no
    tablets — the physical tables are the replicas. Returns the (client, path) pairs to reshard:
    the table itself for a plain dynamic table, or every data replica on its own cluster for a CRT.
    Replication log replicas are skipped: a written-to replication log cannot be
    resharded in place — it has to be recreated and re-attached to the CRT (see
    recreate_replication_log / --also-chaos-replication-logs)."""
    make_client = make_client or _replica_cluster_client
    if client.get(f"{table}/@type") != "chaos_replicated_table":
        return [(client, table)]
    targets = []
    for replica in client.get(f"{table}/@replicas").values():
        if replica["content_type"] != "data":
            if warn_logs:
                logging.warning(
                    f"Skipping {replica['content_type']} replica"
                    f" {replica['cluster_name']}:{replica['replica_path']} of {table}: a written-to"
                    f" replication log cannot be resharded in place; rerun with"
                    f" --also-chaos-replication-logs to recreate it"
                )
            continue
        targets.append((make_client(str(replica["cluster_name"])), str(replica["replica_path"])))
    return targets


def get_replication_log_replicas(client, table):
    """(replica_id, replica_attributes) pairs of the replication log replicas of a CRT (their
    chaos replica has content_type=queue); empty for a plain dynamic table."""
    if client.get(f"{table}/@type") != "chaos_replicated_table":
        return []
    return [
        (replica_id, replica)
        for replica_id, replica in client.get(f"{table}/@replicas").items()
        if replica["content_type"] == "queue"
    ]


def wait_until(predicate, description, timeout=600.0, period=2.0, sleep=time.sleep):
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() >= deadline:
            raise TimeoutError(f"Timed out waiting for {description}")
        sleep(period)


def data_replicas_past_barrier(client, table, barrier_timestamp):
    replicas = client.get(f"{table}/@replicas").values()
    # The upper 34 bits of a YT timestamp are unix seconds — report the lag humanly.
    lagging = {
        str(r["cluster_name"]): (barrier_timestamp >> 30) - (r.get("replication_lag_timestamp", 0) >> 30)
        for r in replicas
        if r["content_type"] == "data" and r.get("replication_lag_timestamp", 0) < barrier_timestamp
    }
    if not lagging:
        return True
    behind = ", ".join(f"{cluster} {seconds}s behind" for cluster, seconds in lagging.items())
    logging.info(f"Waiting for data replicas of {table} to reach the barrier: {behind}")
    return False


# Attributes carried over to the recreated replication log verbatim (when present).
COPIED_LOG_ATTRIBUTES = [
    "account",
    "tablet_cell_bundle",
    "primary_medium",
    "media",
    "compression_codec",
    "erasure_codec",
    "hunk_erasure_codec",
    "optimize_for",
    "in_memory_mode",
    "mount_config",
    "tablet_balancer_config",
]


def recreate_replication_log(
    client,
    table,
    replica_id,
    replica,
    log_pivot_keys,
    make_client=None,
    sleep=time.sleep,
    confirm_timeout=240.0,
    attach_attempts=5,
):
    """Replace the replication log of a CRT with a fresh one of `tablet_count` tablets.

    A written-to replication log cannot be resharded in place, and the only sync log of a
    replication card can be neither disabled nor removed (a new era would leave the card with
    no sync log), so the swap goes through a temporary log: attach a second log, retire the old one,
    attach the final log at the original path, retire the temporary one. Writes stay available the
    whole time — they simply land in whichever sync log is active; retiring a log freezes
    it first and waits until every data replica passes a barrier timestamp taken after the
    freeze, so nothing is lost."""
    make_client = make_client or _replica_cluster_client
    log_cluster = str(replica["cluster_name"])
    log_path = str(replica["replica_path"])
    log_client = make_client(log_cluster)
    tmp_path = f"{log_path}.reshard_tmp"

    schema = log_client.get(f"{log_path}/@schema")
    attributes = {"dynamic": True, "schema": schema}
    for name in COPIED_LOG_ATTRIBUTES:
        if log_client.exists(f"{log_path}/@{name}"):
            attributes[name] = log_client.get(f"{log_path}/@{name}")

    def replica_state(some_replica_id):
        return client.get(f"{table}/@replicas").get(some_replica_id, {}).get("state")

    def attach_log(path):
        logging.info(f"Creating log {log_cluster}:{path} with {len(log_pivot_keys)} tablets...")
        log_client.create("replication_log_table", path, attributes=attributes)
        # A replication log can only be resharded while it is still empty, and — being a sorted
        # table — only by explicit pivot keys; reshard between create and mount.
        log_client.reshard_table(path, pivot_keys=log_pivot_keys, sync=True)
        # catchup=False: the log starts at the current timestamp — history lives in the data
        # replicas already. The log's chaos replica must be sync (content_type queue). The replica
        # must exist BEFORE the table is mounted, else the tablets come up unbound to the
        # replication card and get stuck "identifying replication era", rejecting writes.
        new_replica_id = str(
            client.create(
                "chaos_table_replica",
                attributes={
                    "table_path": table,
                    "cluster_name": log_cluster,
                    "replica_path": path,
                    "content_type": "queue",
                    "mode": "sync",
                    "enabled": True,
                    "catchup": False,
                    # Keep RTT away from the newborn: an empty log looks "lagging" to it,
                    # and the sync->async->sync flip in the first seconds of life races the
                    # tablet attach and freezes the replica progress for good.
                    "enable_replicated_table_tracker": False,
                },
            )
        )
        # Bind the table to its replica: without upstream_replica_id the tablets never
        # attach to the replication card (yt_sync flags such tables as having an invalid
        # upstream_replica_id and repairs them the same way).
        log_client.alter_table(path, upstream_replica_id=new_replica_id)
        log_client.mount_table(path, sync=True)
        wait_until(
            lambda: replica_state(new_replica_id) == "enabled",
            f"log replica {log_cluster}:{path} of {table} to enable",
            sleep=sleep,
        )
        return new_replica_id

    def retire_log(some_replica_id, path):
        logging.info(f"Retiring log {log_cluster}:{path}...")
        # Under live write load freezing takes longer than the wrapper's built-in sync wait
        # allows, so wait ourselves with a generous timeout.
        log_client.freeze_table(path)
        wait_until(
            lambda: log_client.get(f"{path}/@tablet_state") == "frozen",
            f"log {log_cluster}:{path} to freeze",
            timeout=1800.0,
            sleep=sleep,
        )
        barrier_timestamp = client.generate_timestamp()
        wait_until(
            lambda: data_replicas_past_barrier(client, table, barrier_timestamp),
            f"data replicas of {table} to apply log {path}",
            sleep=sleep,
        )
        client.alter_table_replica(some_replica_id, enabled=False)
        wait_until(
            lambda: replica_state(some_replica_id) == "disabled",
            f"log replica {log_cluster}:{path} of {table} to disable",
            sleep=sleep,
        )
        client.remove(f"#{some_replica_id}")
        log_client.unmount_table(path, sync=True)
        log_client.remove(path)

    def attach_healthy_log(path):
        # A newborn sync log non-deterministically (~50%) loses the race between its internal
        # async->sync promotion and the tablet attach: the replica progress freezes at creation
        # and replica_reached_last_own_era never confirms, so clients reject writes with
        # SyncReplicaNotInSync. The newborn is empty — just retire it and roll the dice again.
        for attempt in range(attach_attempts):
            new_replica_id = attach_log(path)
            try:
                wait_until(
                    lambda: bool(
                        client.get(f"{table}/@replicas").get(new_replica_id, {}).get("replica_reached_last_own_era")
                    ),
                    f"log replica {log_cluster}:{path} of {table} to confirm its era",
                    timeout=confirm_timeout,
                    sleep=sleep,
                )
                return new_replica_id
            except TimeoutError:
                logging.warning(
                    f"Newborn log {log_cluster}:{path} did not confirm its era"
                    f" (attempt {attempt + 1}/{attach_attempts}), retiring and retrying"
                )
                retire_log(new_replica_id, path)
        raise RuntimeError(f"log {log_cluster}:{path} failed to confirm its era after {attach_attempts} attempts")

    # A crashed previous run may have left the temporary log behind — retire it first.
    for stale_replica_id, stale in get_replication_log_replicas(client, table):
        if str(stale["replica_path"]) == tmp_path:
            retire_log(stale_replica_id, tmp_path)
    if log_client.exists(tmp_path):
        # A tmp table without a replica: the previous run died between create and attach,
        # nothing has been written to it — safe to drop.
        logging.info(f"Removing stale {log_cluster}:{tmp_path}...")
        log_client.remove(tmp_path)

    tmp_replica_id = attach_healthy_log(tmp_path)
    retire_log(replica_id, log_path)
    attach_healthy_log(log_path)
    retire_log(tmp_replica_id, tmp_path)
    logging.info(f"Recreated {log_cluster}:{log_path}")


def reshard_mounted_table(client, table, also_chaos_replication_logs=False, make_client=None, **reshard_kwargs):
    for target_client, target_table in get_reshard_targets(
        client, table, make_client=make_client, warn_logs=not also_chaos_replication_logs
    ):
        logging.info(f"Resharding {target_table}...")
        target_client.unmount_table(target_table, sync=True)
        target_client.reshard_table(target_table, sync=True, **reshard_kwargs)
        target_client.mount_table(target_table, sync=True)
        logging.info(f"Finished resharding {target_table}")
    if not also_chaos_replication_logs:
        return
    # The log shares the key space of its data table; give it half the data width (the same
    # rule of thumb yt_sync applies when creating pipeline logs).
    pivot_keys = reshard_kwargs.get("pivot_keys")
    if pivot_keys:
        log_pivot_keys = pivot_keys[::2]
    else:
        log_pivot_keys = uniform_uint64_pivot_keys(max(1, reshard_kwargs.get("tablet_count", 1) // 2))
    for replica_id, replica in get_replication_log_replicas(client, table):
        if str(replica["replica_path"]).endswith(".reshard_tmp"):
            # A leftover of a crashed swap; the cleanup inside recreate_replication_log retires
            # it while handling the real log, so it must not be swapped on its own.
            continue
        recreate_replication_log(client, table, replica_id, replica, log_pivot_keys, make_client=make_client)


def reshard_computation_key_table(
    client, computations, source_keys, table, tablet_count, compact_key=False, also_chaos_replication_logs=False
):
    if len(computations) == 0:
        logging.info(f"Skip {table} because there is no computations")
        return

    hash_step = 2**64 // tablet_count

    pivot_keys = []
    for computation_id in sorted(computations):
        if pivot_keys:
            if compact_key:
                pivot_keys.append(build_compact_input_message_pivot_key(computation_id))
            else:
                pivot_keys.append([computation_id])
        else:
            pivot_keys.append([])
        if computation_id in source_keys:
            keys = [[computation_id, key] for key in source_keys[computation_id]]
            source_step = max(1, len(keys) // tablet_count)
            keys.sort(key=lambda item: (item[0], key_sort_value(item[1])))
            pivot_keys.extend(keys[source_step::source_step])
        else:
            for i in range(hash_step, 2**64, hash_step):
                if compact_key:
                    pivot_keys.append(build_compact_input_message_pivot_key(computation_id, i))
                else:
                    pivot_keys.append([computation_id, yson.YsonList([yson.YsonUint64(i)])])
    reshard_mounted_table(client, table, also_chaos_replication_logs=also_chaos_replication_logs, pivot_keys=pivot_keys)


def reshard_partition_table(client, computations, table, tablet_count, also_chaos_replication_logs=False):
    reshard_mounted_table(
        client,
        table,
        also_chaos_replication_logs=also_chaos_replication_logs,
        tablet_count=tablet_count * len(computations),
        uniform=True,
    )


def reshard_input_table(client, computations, path, tablet_count, also_chaos_replication_logs=False):
    table = f"{path}/input_messages"
    reshard_computation_key_table(
        client, computations, {}, table, tablet_count, also_chaos_replication_logs=also_chaos_replication_logs
    )


def reshard_compact_input_table(client, computations, path, tablet_count, also_chaos_replication_logs=False):
    table = f"{path}/compact_input_messages"
    reshard_computation_key_table(
        client,
        computations,
        {},
        table,
        tablet_count,
        compact_key=True,
        also_chaos_replication_logs=also_chaos_replication_logs,
    )


def reshard_timer_table(client, computations, path, tablet_count, also_chaos_replication_logs=False):
    table = f"{path}/timers"
    reshard_computation_key_table(
        client, computations, {}, table, tablet_count, also_chaos_replication_logs=also_chaos_replication_logs
    )


def reshard_compact_partition_output_table(client, computations, path, tablet_count, also_chaos_replication_logs=False):
    table = f"{path}/compact_partition_output_messages"
    reshard_partition_table(
        client, computations, table, tablet_count, also_chaos_replication_logs=also_chaos_replication_logs
    )


def reshard_compact_output_table(
    client, computations, source_keys, path, tablet_count, also_chaos_replication_logs=False
):
    table = f"{path}/compact_output_messages"
    reshard_computation_key_table(
        client, computations, source_keys, table, tablet_count, also_chaos_replication_logs=also_chaos_replication_logs
    )


def reshard_state_table(client, computations, source_keys, path, tablet_count, also_chaos_replication_logs=False):
    table = f"{path}/states"
    reshard_computation_key_table(
        client, computations, source_keys, table, tablet_count, also_chaos_replication_logs=also_chaos_replication_logs
    )


def reshard_partition_state_table(client, computations, path, tablet_count, also_chaos_replication_logs=False):
    table = f"{path}/partition_states"
    reshard_partition_table(
        client, computations, table, tablet_count, also_chaos_replication_logs=also_chaos_replication_logs
    )


def reshard_partition_transactions_table(client, computations, path, tablet_count, also_chaos_replication_logs=False):
    table = f"{path}/partition_transactions"
    reshard_partition_table(
        client, computations, table, tablet_count, also_chaos_replication_logs=also_chaos_replication_logs
    )


def reshard_tables(args):
    logging.basicConfig(
        format="%(asctime)s - %(levelname)s - %(message)s", level=logging.DEBUG if args.verbose else logging.INFO
    )

    client = yt.YtClient(proxy=args.proxy, config=_make_client_config())

    if args.pipeline_path is not None:
        reshard_pipeline_tables(client, args)
    for external_table in args.external_table:
        # An external table has no computations, so it is resharded to --tablet-count tablets
        # uniformly (over its leading hash key column), just like a pipeline partition table.
        reshard_mounted_table(
            client,
            external_table,
            also_chaos_replication_logs=args.also_chaos_replication_logs,
            tablet_count=args.tablet_count,
            uniform=True,
        )


def reshard_pipeline_tables(client, args):
    spec = client.get_pipeline_spec(args.pipeline_path)
    partitions = client.get_flow_view(args.pipeline_path, "/state/execution_spec/layout/partitions", cache=True)

    inputs = []
    outputs = []
    timers = []
    sources = []
    computations = []

    for computation_id, computation in spec["spec"]["computations"].items():
        if computation["input_stream_ids"]:
            assert computation[
                "group_by_schema"
            ], "Computation with input_stream_ids should have non-empty group_by_schema"
            assert (
                computation["group_by_schema"][0]["type"] == "uint64"
            ), "First column in group_by_schema should have type equal to 'uint64' with hash value"
            inputs.append(computation_id)
        if computation["output_stream_ids"]:
            outputs.append(computation_id)
        if computation["timer_streams"]:
            timers.append(computation_id)
        if computation["source_streams"]:
            sources.append(computation_id)
        computations.append(computation_id)

    source_keys = defaultdict(list)
    for partition in partitions.values():
        if "source_key" in partition:
            source_keys[partition["computation_id"]].append(partition["source_key"])

    logging.info(args.table)
    if args.table is None or args.table == "input_messages":
        reshard_input_table(client, inputs, args.pipeline_path, args.tablet_count, args.also_chaos_replication_logs)
    if args.table is None or args.table == "compact_input_messages":
        reshard_compact_input_table(
            client, inputs, args.pipeline_path, args.tablet_count, args.also_chaos_replication_logs
        )
    if args.table is None or args.table == "compact_output_messages":
        reshard_compact_output_table(
            client, sources, source_keys, args.pipeline_path, args.tablet_count, args.also_chaos_replication_logs
        )
    if args.table is None or args.table == "compact_partition_output_messages":
        reshard_compact_partition_output_table(
            client, outputs, args.pipeline_path, args.tablet_count, args.also_chaos_replication_logs
        )
    if args.table is None or args.table == "timers":
        reshard_timer_table(client, timers, args.pipeline_path, args.tablet_count, args.also_chaos_replication_logs)
    if args.table is None or args.table == "states":
        reshard_state_table(
            client, computations, source_keys, args.pipeline_path, args.tablet_count, args.also_chaos_replication_logs
        )
    if args.table is None or args.table == "partition_states":
        reshard_partition_state_table(
            client, computations, args.pipeline_path, args.tablet_count, args.also_chaos_replication_logs
        )
    if args.table is None or args.table == "partition_transactions":
        reshard_partition_transactions_table(
            client, computations, args.pipeline_path, args.tablet_count, args.also_chaos_replication_logs
        )


if __name__ == "__main__":
    args = get_args()
    reshard_tables(args)
