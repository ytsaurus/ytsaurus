import argparse
import functools
import logging
import sys
import time

from bisect import bisect_left, bisect_right
from collections import defaultdict
from dataclasses import dataclass

import yt.wrapper as yt
from yt.wrapper import yson
from yt.wrapper.default_config import get_default_config, update_config_from_env

from yt.ypath.rich import RichYPath

EPILOG = """The plan reports old and new tablet counts for each physical table. Tables with
identical pivot keys are left untouched, even without --dry-run. Tables keyed by
computation_id also report how many tablets intersect each computation's key range;
a shared tablet counts toward every computation it covers. Compact input keys are
handled in the same way.

Pass --commit to apply changes or --dry-run to preview them. For compatibility,
running without either flag still applies changes and emits a warning; a future
version will default to dry-run.

Examples:

{0} --proxy zeno \\
    --pipeline-path //path/on/zeno --commit

# Preview the same changes without modifying tables:
{0} --proxy zeno \\
    --pipeline-path //path/on/zeno \\
    --dry-run

# Everything at once: the pipeline tables (data replicas + replication logs) plus the
# replication logs of external state tables:
{0} --proxy pythia \\
    --pipeline-path //home/project/pipeline \\
    --also-chaos-replication-logs \\
    --external-table //home/project/profiles \\
    --external-table //home/project/counters --commit
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
            "leases",
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
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--commit",
        action="store_true",
        help="apply the planned changes explicitly (currently also the default)",
    )
    mode.add_argument(
        "--dry-run",
        action="store_true",
        help="show tablet count changes for each physical table and selected replication log without modifying tables",
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
        # A non-UTF-8 string column arrives as a YsonStringProxy, so unwrap every string kind
        # through get_bytes.
        if isinstance(column, (str, bytes, yson.YsonStringProxy)):
            return (4, yson.get_bytes(column))
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


# The temporary log a swap goes through lives beside the canonical one, at its path plus this
# suffix. Telling a leftover of a crashed run from a real log is the whole basis of resuming, so
# the suffix is one constant rather than a literal repeated at each site.
TMP_SUFFIX = ".reshard_tmp"


@dataclass
class ReplicationLogPlan:
    client: object
    table: str
    cluster: str
    path: str
    log_client: object
    pivots: list
    attached: dict
    canonical_replica: dict
    states: dict
    attributes: dict
    canonical: "TableReshardPlan | None"
    temporary: "TableReshardPlan | None"

    @functools.cached_property
    def canonical_unchanged(self):
        return (
            self.canonical is not None
            and self.canonical.unchanged
            and self.path in self.attached
            and self.states[self.path] == "mounted"
            and replica_ready(self.canonical_replica)
        )

    @functools.cached_property
    def unchanged(self):
        return self.canonical_unchanged and self.temporary is None and f"{self.path}{TMP_SUFFIX}" not in self.attached


def prepare_replication_log(
    client, table, log_cluster, log_path, log_pivot_keys, make_client=None, replicas=None, computation_ids=()
):
    log_client = (make_client or _replica_cluster_client)(log_cluster)
    tmp_path = f"{log_path}{TMP_SUFFIX}"
    if replicas is None:
        replicas = dict(get_replication_log_replicas(client, table))
    attached = {
        str(replica["replica_path"]): replica_id
        for replica_id, replica in replicas.items()
        if replica["content_type"] == "queue"
        and str(replica["cluster_name"]) == log_cluster
        and str(replica["replica_path"]) in (log_path, tmp_path)
    }
    if not attached:
        raise RuntimeError(f"replication log {log_cluster}:{log_path} of {table} has no attached replica to recreate")
    states = {}
    layouts = {}
    for path in (log_path, tmp_path):
        states[path] = log_client.get(f"{path}/@tablet_state") if log_client.exists(f"{path}/@tablet_state") else None
        if states[path] is not None:
            layouts[path] = read_layout(log_client, path)
    source_path = log_path if states[log_path] is not None else tmp_path
    schema = log_client.get(f"{source_path}/@schema")
    attributes = {"dynamic": True, "schema": schema}
    for name in COPIED_LOG_ATTRIBUTES:
        if log_client.exists(f"{source_path}/@{name}"):
            attributes[name] = log_client.get(f"{source_path}/@{name}")
    table_plans = {
        path: TableReshardPlan(log_client, path, layout, {"pivot_keys": log_pivot_keys}, schema, computation_ids)
        for path, layout in layouts.items()
    }
    return ReplicationLogPlan(
        client,
        table,
        log_cluster,
        log_path,
        log_client,
        log_pivot_keys,
        attached,
        dict(replicas.get(attached.get(log_path), {})),
        states,
        attributes,
        table_plans.get(log_path),
        table_plans.get(tmp_path),
    )


def replica_ready(replica):
    return (
        replica.get("state") == "enabled"
        and replica.get("mode") == "sync"
        and bool(replica.get("replica_reached_last_own_era"))
    )


def log_replication_plan(plan):
    if plan.canonical is not None:
        log_table_plan(
            plan.canonical,
            "recreate" if not plan.canonical.unchanged else "finish interrupted log swap",
            unchanged=plan.canonical_unchanged,
        )
    else:
        logging.info(f"{plan.cluster}:{plan.path}: absent => {len(plan.pivots)} tablets (recreate)")
        creation = TableReshardPlan(
            plan.log_client,
            plan.path,
            {"pivot_keys": []},
            {"pivot_keys": plan.pivots},
            plan.attributes["schema"],
            plan.temporary.computation_ids,
        )
        log_computation_diffs(creation, f"{plan.cluster}:{plan.path}")
    if plan.temporary is not None:
        current = planned_tablet_count(plan.temporary.previous_layout)
        name = f"{plan.cluster}:{plan.temporary.table}"
        logging.info(f"{name}: {current} => 0 tablets (remove temporary log)")
        removal = TableReshardPlan(
            plan.log_client,
            plan.temporary.table,
            plan.temporary.previous_layout,
            {"pivot_keys": []},
            plan.temporary.schema,
            plan.temporary.computation_ids,
        )
        log_computation_diffs(removal, name)


def recreate_replication_log(
    client,
    table,
    log_cluster,
    log_path,
    log_pivot_keys,
    make_client=None,
    sleep=time.sleep,
    confirm_timeout=240.0,
    attach_attempts=5,
    dry_run=False,
):
    """Analyze a log swap once and optionally execute it, retaining interrupted-swap recovery."""
    plan = prepare_replication_log(client, table, log_cluster, log_path, log_pivot_keys, make_client)
    log_replication_plan(plan)
    if not dry_run:
        execute_replication_log(plan, sleep, confirm_timeout, attach_attempts)


def execute_replication_log(plan, sleep=time.sleep, confirm_timeout=240.0, attach_attempts=5):
    client, table = plan.client, plan.table
    log_cluster, log_path, log_client = plan.cluster, plan.path, plan.log_client
    log_pivot_keys, attributes = plan.pivots, plan.attributes
    tmp_path = f"{log_path}{TMP_SUFFIX}"
    canonical_replica_id = plan.attached.get(log_path)
    tmp_replica_id = plan.attached.get(tmp_path)
    states = dict(plan.states)

    def tablet_state(path):
        return states.get(path)

    def replica_state(some_replica_id):
        return client.get(f"{table}/@replicas").get(some_replica_id, {}).get("state")

    def attach_log(path):
        logging.info(f"Creating log {log_cluster}:{path} with {len(log_pivot_keys)} tablets...")
        log_client.create("replication_log_table", path, attributes=attributes)
        states[path] = "unmounted"
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
        states[path] = "mounted"
        wait_until(
            lambda: replica_state(new_replica_id) == "enabled",
            f"log replica {log_cluster}:{path} of {table} to enable",
            sleep=sleep,
        )
        return new_replica_id

    def drop_table(path):
        # A retire that died between removing the chaos replica and removing the table leaves an
        # unattached table behind. Nothing writes to it (writes go through the card), and attach_log
        # cannot create over it, so drop it; one left frozen has to be unmounted first.
        state = tablet_state(path)
        if state is None:
            return
        logging.info(f"Removing {log_cluster}:{path}...")
        if state != "unmounted":
            log_client.unmount_table(path, sync=True)
        log_client.remove(path)
        states[path] = None

    def detach_log(some_replica_id, path):
        client.alter_table_replica(some_replica_id, enabled=False)
        wait_until(
            lambda: replica_state(some_replica_id) == "disabled",
            f"log replica {log_cluster}:{path} of {table} to disable",
            sleep=sleep,
        )
        client.remove(f"#{some_replica_id}")
        drop_table(path)

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
        states[path] = "frozen"
        barrier_timestamp = client.generate_timestamp()
        wait_until(
            lambda: data_replicas_past_barrier(client, table, barrier_timestamp),
            f"data replicas of {table} to apply log {path}",
            sleep=sleep,
        )
        detach_log(some_replica_id, path)

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

    def ready(replica_id, path):
        if replica_id is None:
            return False
        replica = client.get(f"{table}/@replicas").get(replica_id, {})
        states[path] = log_client.get(f"{path}/@tablet_state") if log_client.exists(f"{path}/@tablet_state") else None
        return states[path] == "mounted" and replica_ready(replica)

    canonical_ready = ready(canonical_replica_id, log_path)
    if plan.canonical is not None and plan.canonical.unchanged and canonical_ready:
        if tmp_replica_id is not None:
            retire_log(tmp_replica_id, tmp_path)
        else:
            drop_table(tmp_path)
        return

    if canonical_replica_id is not None and not canonical_ready:
        logging.warning(f"Log {log_cluster}:{log_path} is not ready; recovering it before retiring the temporary log")
        if not ready(tmp_replica_id, tmp_path):
            if tmp_replica_id is not None:
                if tablet_state(tmp_path) in (None, "unmounted"):
                    detach_log(tmp_replica_id, tmp_path)
                else:
                    retire_log(tmp_replica_id, tmp_path)
            else:
                drop_table(tmp_path)
            tmp_replica_id = attach_healthy_log(tmp_path)
        if tablet_state(log_path) in (None, "unmounted"):
            detach_log(canonical_replica_id, log_path)
        else:
            retire_log(canonical_replica_id, log_path)
        canonical_replica_id = None

    if canonical_replica_id is None:
        # Resuming a run that died after retiring the canonical log: writers are on the temporary
        # log, and moving them back is all that is left. This must happen unconditionally — treating
        # the temporary log as a mere leftover to skip is what used to strand the card on it.
        logging.info(f"Resuming the swap of {log_cluster}:{log_path} from {tmp_path}...")
        drop_table(log_path)
        attach_healthy_log(log_path)
        retire_log(tmp_replica_id, tmp_path)
        logging.info(f"Recreated {log_cluster}:{log_path}")
        return

    # Creating the temporary log is the one step that has to account for a leftover of a crashed
    # run: retire it if the card still carries it, drop it if only the table survived.
    if tmp_replica_id is not None:
        retire_log(tmp_replica_id, tmp_path)
    else:
        drop_table(tmp_path)

    tmp_replica_id = attach_healthy_log(tmp_path)
    retire_log(canonical_replica_id, log_path)
    attach_healthy_log(log_path)
    retire_log(tmp_replica_id, tmp_path)
    logging.info(f"Recreated {log_cluster}:{log_path}")


def restore(client, table, previous_layout):
    """Best-effort return of |table| to |previous_layout|, mounted.

    Reshards unconditionally rather than only when the layout is known to have changed: a failed
    `reshard_table(sync=True)` says nothing about whether the mutation went through. It is
    make_request plus a separate _waiting_for_tablet_transition (dynamic_table_commands.py), so a
    timeout waiting for the tablets raises over an applied reshard. Resharding an unmounted table
    to the layout it already has is cheap and idempotent, so paying for it always beats guessing.

    Never raises: it runs while another error is propagating, and that error is the one worth
    reporting."""
    try:
        client.unmount_table(table, sync=True)
        client.reshard_table(table, sync=True, **previous_layout)
        client.mount_table(table, sync=True)
    except BaseException:
        logging.exception(
            f"Failed to restore {table}: it is left unmounted, and the pipeline will reject every"
            " commit against it until it is mounted by hand"
        )


def read_layout(client, table):
    """The reshard arguments that reproduce the current tablet layout of |table|: explicit pivot
    keys for a sorted table, a plain tablet count for an ordered one."""
    if client.get(f"{table}/@sorted"):
        return {"pivot_keys": client.get(f"{table}/@pivot_keys")}
    return {"tablet_count": client.get(f"{table}/@tablet_count")}


@dataclass
class ReshardRequest:
    table: str
    parameters: dict
    computation_ids: tuple = ()


@dataclass
class TableReshardPlan:
    client: object
    table: str
    previous_layout: dict
    layout: dict
    schema: list
    computation_ids: tuple = ()

    @functools.cached_property
    def unchanged(self):
        return layout_identity(self.previous_layout) == layout_identity(self.layout)

    @functools.cached_property
    def delta(self):
        return planned_tablet_count(self.layout) - planned_tablet_count(self.previous_layout)


def layout_identity(layout):
    # Preserve signed/unsigned key types, but ignore YSON attributes on values.
    return yson.dumps(dict(layout), yson_format="binary", ignore_inner_attributes=True)


def uniform_pivot_keys(tablet_count, schema):
    column = schema[0]
    column_type = column.get("type", column.get("type_v3"))
    if column.get("sort_order") != "ascending" or column_type not in (
        "int8",
        "int16",
        "int32",
        "int64",
        "uint8",
        "uint16",
        "uint32",
        "uint64",
    ):
        raise ValueError("Uniform reshard requires an ascending integral first key column")
    unsigned = column_type.startswith("u")
    bits = int(column_type.removeprefix("u").removeprefix("int"))
    lower = 0 if unsigned else -(2 ** (bits - 1))
    value_type = yson.YsonUint64 if unsigned else int
    # Match TClient::PickUniformPivotKeys: divide after multiplying, without rounding the step.
    return [[]] + [[value_type(lower + (2**bits * i) // tablet_count)] for i in range(1, tablet_count)]


def prepare_table_reshard(client, request):
    previous_layout = read_layout(client, request.table)
    schema = client.get(f"{request.table}/@schema")
    layout = dict(request.parameters)
    if "pivot_keys" not in layout:
        count = layout["tablet_count"]
        if count <= 0:
            raise ValueError("Tablet count must be positive")
        if "pivot_keys" in previous_layout:
            if not layout.get("uniform"):
                raise ValueError("Sorted tables require explicit pivot keys or uniform resharding")
            layout = {"pivot_keys": uniform_pivot_keys(count, schema)}
        else:
            layout = {"tablet_count": count}
    return TableReshardPlan(client, request.table, previous_layout, layout, schema, request.computation_ids)


def computation_tablet_counts(pivots, computations, compact=False):
    # A pivot with trailing columns lies after the computation's prefix boundary.
    boundaries = [(bool(key), yson.get_bytes(key[0]) if key else b"", len(key) > 1) for key in pivots]
    counts = {}
    for computation in computations:
        encoded = yson.get_bytes(computation)
        lower = encoded + b"\0" if compact else encoded
        upper = encoded + (b"\1" if compact else b"\0")
        first = max(0, bisect_right(boundaries, (True, lower, False)) - 1)
        last = bisect_left(boundaries, (True, upper, False))
        counts[computation] = last - first
    return counts


def log_table_plan(plan, action="reshard", unchanged=None):
    proxy = plan.client.config["proxy"]["url"]
    name = f"{proxy}:{plan.table}" if proxy else plan.table
    current = planned_tablet_count(plan.previous_layout)
    target = planned_tablet_count(plan.layout)
    if unchanged is None:
        unchanged = plan.unchanged
    status = "already OK, boundaries unchanged" if unchanged else action
    if not plan.unchanged and "pivot_keys" in plan.layout:
        status += ", boundaries changed"
    logging.info(f"{name}: {current} => {target} tablets ({target - current:+d}, {status})")
    log_computation_diffs(plan, name)


def log_computation_diffs(plan, name):
    if "pivot_keys" not in plan.layout or not plan.schema:
        return
    column = plan.schema[0]
    compact = column["name"] == "deduplication_message_key"
    if column["name"] != "computation_id" and not compact:
        return
    computations = set(plan.computation_ids)
    for layout in (plan.previous_layout, plan.layout):
        for key in layout["pivot_keys"]:
            if key:
                value = yson.get_bytes(key[0])
                computations.add(value.split(b"\0", 1)[0].decode("utf-8") if compact else value.decode("utf-8"))
    computations = sorted(computations)
    before = computation_tablet_counts(plan.previous_layout["pivot_keys"], computations, compact)
    after = computation_tablet_counts(plan.layout["pivot_keys"], computations, compact)
    for computation in computations:
        logging.info(f"{name}: computation_id={computation!r}: {before[computation]} => {after[computation]} tablets")


def execute_table_reshard(plan):
    if plan.unchanged:
        return
    client, table = plan.client, plan.table
    try:
        client.unmount_table(table, sync=True)
        client.reshard_table(table, sync=True, **plan.layout)
        client.mount_table(table, sync=True)
    except BaseException:
        # A failed synchronous operation may already have applied its mutation.
        logging.error(f"Resharding {table} failed, restoring its previous layout")
        restore(client, table, plan.previous_layout)
        raise


def reshard_one_table(client, table, dry_run=False, **reshard_kwargs):
    plan = prepare_table_reshard(client, ReshardRequest(table, reshard_kwargs))
    log_table_plan(plan)
    if not dry_run:
        execute_table_reshard(plan)


def reshard_mounted_table(
    client, table, also_chaos_replication_logs=False, make_client=None, dry_run=False, **reshard_kwargs
):
    apply_reshard_plans(client, [(table, reshard_kwargs)], also_chaos_replication_logs, make_client, dry_run)


def planned_tablet_count(reshard_kwargs):
    pivot_keys = reshard_kwargs.get("pivot_keys")
    return len(pivot_keys) if pivot_keys is not None else reshard_kwargs["tablet_count"]


def current_tablet_count(client, table, make_client=None):
    return max(
        (
            target_client.get(f"{target_table}/@tablet_count")
            for target_client, target_table in get_reshard_targets(
                client, table, make_client=make_client, warn_logs=False
            )
        ),
        default=0,
    )


def apply_reshard_plans(client, plans, also_chaos_replication_logs=False, make_client=None, dry_run=False):
    """Analyze once, log the resulting layouts, then optionally apply them, smallest growth first."""
    make_client = make_client or _replica_cluster_client
    data_plans = []
    log_plans = []
    for request in plans:
        if not isinstance(request, ReshardRequest):
            request = ReshardRequest(*request)
        table = request.table
        if client.get(f"{table}/@type") != "chaos_replicated_table":
            data_plans.append(prepare_table_reshard(client, request))
            continue
        replicas = client.get(f"{table}/@replicas")
        logs = set()
        for replica in replicas.values():
            cluster, path = str(replica["cluster_name"]), str(replica["replica_path"])
            if replica["content_type"] == "data":
                data_plans.append(
                    prepare_table_reshard(
                        make_client(cluster), ReshardRequest(path, request.parameters, request.computation_ids)
                    )
                )
            elif also_chaos_replication_logs:
                logs.add((cluster, path.removesuffix(TMP_SUFFIX)))
            else:
                logging.warning(
                    f"Skipping replication log {cluster}:{path}; use --also-chaos-replication-logs to recreate it"
                )
        pivots = request.parameters.get("pivot_keys")
        log_pivots = (
            pivots[::2] if pivots else uniform_uint64_pivot_keys(max(1, request.parameters.get("tablet_count", 1) // 2))
        )
        for cluster, path in sorted(logs):
            log_plans.append(
                prepare_replication_log(
                    client,
                    table,
                    cluster,
                    path,
                    log_pivots,
                    make_client=make_client,
                    replicas=replicas,
                    computation_ids=request.computation_ids,
                )
            )
    data_plans.sort(key=lambda plan: plan.delta)
    for plan in data_plans:
        log_table_plan(plan)
    for plan in log_plans:
        log_replication_plan(plan)
    if dry_run:
        logging.info("Dry run: no tables will be modified")
        return
    for plan in data_plans:
        execute_table_reshard(plan)
    for plan in log_plans:
        execute_replication_log(plan)


def plan_computation_key_table(computations, source_keys, table, tablet_count, compact_key=False):
    if len(computations) == 0:
        logging.info(f"Skip {table} because there is no computations")
        return None

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
    return ReshardRequest(table, {"pivot_keys": pivot_keys}, tuple(computations))


def plan_partition_table(computations, table, tablet_count):
    return table, {"tablet_count": tablet_count * len(computations), "uniform": True}


def plan_input_table(computations, path, tablet_count):
    return plan_computation_key_table(computations, {}, f"{path}/input_messages", tablet_count)


def plan_compact_input_table(computations, path, tablet_count):
    return plan_computation_key_table(
        computations, {}, f"{path}/compact_input_messages", tablet_count, compact_key=True
    )


def plan_timer_table(computations, path, tablet_count):
    return plan_computation_key_table(computations, {}, f"{path}/timers", tablet_count)


def reshard_timer_table(client, computations, path, tablet_count, also_chaos_replication_logs=False):
    """Plan and apply the timers reshard in one call.

    The tool itself plans every table before applying anything, so that the plans can be ordered by
    how much each one grows. This entry point reshards the one table on its own and keeps its
    original signature for the caller outside the tool
    (alice/wonderlogs/flow/rt_dwh/tools/ensure_flow_sharding)."""
    plan = plan_timer_table(computations, path, tablet_count)
    if plan is None:
        return
    apply_reshard_plans(client, [plan], also_chaos_replication_logs=also_chaos_replication_logs)


def plan_compact_partition_output_table(computations, path, tablet_count):
    return plan_partition_table(computations, f"{path}/compact_partition_output_messages", tablet_count)


def plan_compact_output_table(computations, source_keys, path, tablet_count):
    return plan_computation_key_table(computations, source_keys, f"{path}/compact_output_messages", tablet_count)


def plan_state_table(computations, source_keys, path, tablet_count):
    return plan_computation_key_table(computations, source_keys, f"{path}/states", tablet_count)


def plan_partition_state_table(computations, path, tablet_count):
    return plan_partition_table(computations, f"{path}/partition_states", tablet_count)


def plan_partition_transactions_table(computations, path, tablet_count):
    return plan_partition_table(computations, f"{path}/partition_transactions", tablet_count)


def plan_leases_table(client, computations, path, tablet_count, infer_unused=True):
    table = f"{path}/leases"
    # Same write profile as partition_transactions -- tiny rows rewritten at a high rate, keyed by
    # farm_hash(key) -- so spread it over the same width. A single tablet cannot compact the lease
    # churn of every partition and hits "too many overlapping stores, writes disabled" (code 1703).
    full_width = plan_partition_table(computations, table, tablet_count)
    if not infer_unused:
        return full_width

    # The election backend belongs to the controller process config and is not part of the
    # pipeline spec available to this tool, so whether the table is in use is inferred from its
    # contents. A dyntable-backed controller rewrites the pipeline-wide deadline row
    # ("", "expiration") every cycle (TDyntableLeases::TouchLeaseDeadline) and nothing deletes it,
    # so once such a controller has led the pipeline the table never becomes empty again, not even
    # when the pipeline is stopped and every partition row has been revoked. Until then a single
    # tablet is enough, and the tablets an earlier run spread over an unused table are taken back.
    # After switching the backend rerun the tool: the table stays empty until the first controller
    # with the dyntable backend leads the pipeline. `--table leases` bypasses this inference and
    # always plans the full width.
    try:
        empty = not list(client.select_rows(f"* FROM [{table}] LIMIT 1"))
    except Exception as ex:
        # Fail open: an unmounted table, or a chaos table with no in-sync data replica right now,
        # only means the table is not known to be empty, and shrinking is cheap only when we are
        # sure. Aborting here would leave every other table of the run un-resharded too.
        logging.warning(f"Cannot tell whether {table} is empty, planning the full width: {ex}")
        return full_width
    if empty:
        logging.info(f"Plan a single tablet for {table} because it is empty")
        return table, {"tablet_count": 1, "uniform": True}
    return full_width


def reshard_tables(args):
    logging.basicConfig(
        format="%(asctime)s - %(levelname)s - %(message)s", level=logging.DEBUG if args.verbose else logging.INFO
    )

    if not args.commit and not args.dry_run:
        logging.warning(
            "Running without --commit or --dry-run currently applies changes. "
            "In a future version, it will only show the plan. "
            "Pass --commit to keep applying changes, or --dry-run to preview them."
        )

    client = yt.YtClient(proxy=args.proxy, config=_make_client_config())

    plans = []
    if args.pipeline_path is not None:
        plans.extend(plan_pipeline_tables(client, args))
    for external_table in args.external_table:
        # An external table has no computations, so it is resharded to --tablet-count tablets
        # uniformly (over its leading hash key column), just like a pipeline partition table.
        plans.append((external_table, {"tablet_count": args.tablet_count, "uniform": True}))
    apply_reshard_plans(
        client,
        plans,
        also_chaos_replication_logs=args.also_chaos_replication_logs,
        dry_run=args.dry_run,
    )


def plan_pipeline_tables(client, args):
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

    path = args.pipeline_path
    tablet_count = args.tablet_count
    plans = []
    if args.table is None or args.table == "input_messages":
        plans.append(plan_input_table(inputs, path, tablet_count))
    if args.table is None or args.table == "compact_input_messages":
        plans.append(plan_compact_input_table(inputs, path, tablet_count))
    if args.table is None or args.table == "compact_output_messages":
        plans.append(plan_compact_output_table(sources, source_keys, path, tablet_count))
    if args.table is None or args.table == "compact_partition_output_messages":
        plans.append(plan_compact_partition_output_table(outputs, path, tablet_count))
    if args.table is None or args.table == "timers":
        plans.append(plan_timer_table(timers, path, tablet_count))
    if args.table is None or args.table == "states":
        plans.append(plan_state_table(computations, source_keys, path, tablet_count))
    if args.table is None or args.table == "partition_states":
        plans.append(plan_partition_state_table(computations, path, tablet_count))
    if args.table is None or args.table == "partition_transactions":
        plans.append(plan_partition_transactions_table(computations, path, tablet_count))
    if args.table is None or args.table == "leases":
        plans.append(plan_leases_table(client, computations, path, tablet_count, infer_unused=args.table is None))
    return [plan for plan in plans if plan is not None]


if __name__ == "__main__":
    args = get_args()
    reshard_tables(args)
