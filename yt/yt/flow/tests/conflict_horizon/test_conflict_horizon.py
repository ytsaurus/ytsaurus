"""Reproduces the tablet-side commit lifetime limit behind error 1729
(CannotCheckConflictsAgainstChunkStore): a write transaction whose start timestamp is older than
the tablet's last flushed store cannot commit once the backing store grace elapses — regardless of
which rows it touches, because the conflict horizon is per-tablet, not per-row.

The matrix discriminates the mount-config knobs: min_data_ttl / merge_rows_on_flush (version
retention) do not participate in the conflict check at all; backing_store_retention_time is the
knob that actually widens the window. Everything runs on a REGULAR dynamic table: the mechanism
is not chaos-specific, chaos merely makes transactions long enough to hit it.
"""

import logging
import os
import time

import pytest

import yt.wrapper
from yt.common import wait, YtError

##################################################################

TABLET_CELL_BUNDLE = "flow-bundle"

SCHEMA = [
    {"name": "key", "type": "string", "sort_order": "ascending"},
    {"name": "value", "type": "any"},
]

# Frequent periodic flushes so the scenario does not depend on store size thresholds.
FLUSH_MOUNT_CONFIG = {
    "dynamic_store_auto_flush_period": 3000,
    "dynamic_store_flush_period_splay": 0,
}

CANNOT_CHECK_CONFLICTS_AGAINST_CHUNK_STORE = 1729

##################################################################


def make_client():
    config = yt.wrapper.default_config.get_config_from_env()
    config["backend"] = "rpc"
    return yt.wrapper.YtClient(proxy=os.environ["YT_PROXY"], config=config)


@pytest.fixture(scope="session", autouse=True)
def tablet_cell():
    client = make_client()
    cell_id = client.create("tablet_cell", attributes={"tablet_cell_bundle": TABLET_CELL_BUNDLE})
    wait(lambda: client.get("#{}/@health".format(cell_id)) == "good")


def create_table(client, path, extra_mount_config):
    client.create(
        "table",
        path,
        attributes={
            "dynamic": True,
            "schema": SCHEMA,
            "tablet_cell_bundle": TABLET_CELL_BUNDLE,
            "mount_config": dict(FLUSH_MOUNT_CONFIG, **extra_mount_config),
        },
    )
    client.mount_table(path)
    wait(lambda: client.get(path + "/@tablet_state") == "mounted")


def flush_dynamic_stores(client, path):
    """Turns every dynamic store of |path| into a chunk store, synchronously. Freezing a tablet
    flushes it, and coming back from frozen leaves nothing in memory that a conflict check could
    still consult. A periodic flush is not equivalent here: its only visible signal is the new
    chunk, which appears before the flushed store is actually gone from the tablet."""
    client.freeze_table(path, sync=True)
    client.unfreeze_table(path, sync=True)


def run_long_transaction_over_flush(path, extra_mount_config, sleep_after_flush=0, force_flush=False):
    """Opens a tablet transaction, lets an INDEPENDENT commit to another key of the same tablet
    get flushed (and optionally waits |sleep_after_flush| more), then writes its own key and
    commits. |force_flush| picks the deterministic flush; the periodic one is for the cases that
    need the flushed store to stay in memory as a backing store."""
    client = make_client()
    create_table(client, path, extra_mount_config)

    writer = make_client()
    with client.Transaction(type="tablet", attributes={"timeout": 180000}):
        # A concurrent, completely independent commit to ANOTHER key of the same tablet.
        writer.insert_rows(path, [{"key": "other", "value": 1}])
        if force_flush:
            flush_dynamic_stores(writer, path)
        else:
            # Wait for the periodic flush to turn it into a chunk store.
            wait(lambda: writer.get(path + "/@chunk_count") >= 1, timeout=60)
        if sleep_after_flush:
            logging.info("flush observed, sleeping %s seconds", sleep_after_flush)
            time.sleep(sleep_after_flush)
        # The key "mine" was never written by anybody; only the tablet-wide horizon matters.
        client.insert_rows(path, [{"key": "mine", "value": 2}])
    # The commit happens on context exit.


##################################################################


@pytest.mark.authors(["thenewone"])
class TestConflictHorizon:
    def test_dies_after_flush_without_backing_grace(self):
        # (a) The pure mechanism: with no backing store the horizon advances right at flush, and
        # the transaction dies even though its key was never touched by anybody else.
        with pytest.raises(YtError) as err:
            run_long_transaction_over_flush(
                "//tmp/horizon_a",
                {"backing_store_retention_time": 0},
                force_flush=True,
            )
        assert err.value.contains_code(CANNOT_CHECK_CONFLICTS_AGAINST_CHUNK_STORE), str(err.value)

    def test_version_retention_knobs_do_not_help(self):
        # (b) min_data_ttl / merge_rows_on_flush govern version retention in the data, which the
        # conflict check never consults; the transaction dies exactly as in (a).
        with pytest.raises(YtError) as err:
            run_long_transaction_over_flush(
                "//tmp/horizon_b",
                {
                    "backing_store_retention_time": 0,
                    "min_data_ttl": 300000,
                    "merge_rows_on_flush": False,
                },
                force_flush=True,
            )
        assert err.value.contains_code(CANNOT_CHECK_CONFLICTS_AGAINST_CHUNK_STORE), str(err.value)

    def test_dies_with_default_backing_grace(self):
        # (c) Default backing_store_retention_time (60s): the transaction only needs to outlive
        # flush + grace — the realistic timing of a long catch-up commit.
        with pytest.raises(YtError) as err:
            run_long_transaction_over_flush(
                "//tmp/horizon_c",
                {},
                sleep_after_flush=75,
            )
        assert err.value.contains_code(CANNOT_CHECK_CONFLICTS_AGAINST_CHUNK_STORE), str(err.value)

    def test_backing_store_retention_widens_the_window(self):
        # (d) The knob that actually matters: a live backing store keeps the flushed versions
        # checkable and the same scenario commits fine.
        run_long_transaction_over_flush(
            "//tmp/horizon_d",
            {"backing_store_retention_time": 300000},
        )


##################################################################


def measure_transaction_windows(path, count):
    """Runs |count| single-row tablet transactions and measures, per transaction, the wall time
    from the transaction start (timestamp acquisition) to the commit completion — the window
    during which a concurrent flush makes the transaction vulnerable to the conflict horizon —
    and the commit call alone."""
    client = make_client()
    windows = []
    commits = []
    for index in range(count):
        row = [{"key": "k%03d" % index, "value": {"i": index}}]
        window_start = time.monotonic()
        with client.Transaction(type="tablet"):
            client.insert_rows(path, row)
            commit_start = time.monotonic()
        finish = time.monotonic()
        windows.append(finish - window_start)
        commits.append(finish - commit_start)
    return windows, commits


def log_quantiles(name, samples):
    ordered = sorted(samples)
    logging.info(
        "%s: p50=%.1fms p90=%.1fms max=%.1fms",
        name,
        ordered[len(ordered) // 2] * 1000,
        ordered[int(len(ordered) * 0.9)] * 1000,
        ordered[-1] * 1000,
    )
    return ordered[len(ordered) // 2]


@pytest.mark.authors(["thenewone"])
class TestChaosTransactionWindow:
    def test_chaos_commit_window_is_measured(self):
        # The conflict-horizon exposure of a transaction is its start-to-commit window. On an
        # idle single-host cluster the window of a chaos replicated table equals the regular one
        # (~140ms p50 for both when this was written): the chaos commit protocol itself adds no
        # intrinsic latency in the degenerate one-cluster topology. The production stretching
        # comes from cross-cluster round trips and long catch-up commits, which a local cluster
        # cannot produce — the next test emulates the stretch explicitly.
        client = make_client()
        create_table(client, "//tmp/window_regular", {})
        create_chaos_table_with_mount_config(client, "//tmp/window_chaos", {})

        count = 25
        regular_windows, regular_commits = measure_transaction_windows("//tmp/window_regular", count)
        chaos_windows, chaos_commits = measure_transaction_windows("//tmp/window_chaos", count)

        regular_median = log_quantiles("regular: start-to-commit window", regular_windows)
        chaos_median = log_quantiles("chaos: start-to-commit window", chaos_windows)
        log_quantiles("regular: commit call", regular_commits)
        log_quantiles("chaos: commit call", chaos_commits)

        # Sanity only: the numbers are logged for the record, no ordering is asserted.
        assert chaos_median < 100 * regular_median


##################################################################


CHAOS_CELL_BUNDLE = "test-chaos"


def wait_chaos_writable(client, path):
    """Waits until the chaos table accepts writes: replicas need a moment to become writable both
    after creation and after a data replica leaves the frozen state."""

    def probe():
        try:
            client.insert_rows(path, [{"key": "probe", "value": None}])
            client.delete_rows(path, [{"key": "probe"}])
            return True
        except YtError as err:
            logging.info("Chaos table is not ready yet: %s", err)
            return False

    wait(probe, timeout=120, sleep_backoff=1)


def create_chaos_table_with_mount_config(client, path, data_mount_config):
    """A chaos replicated table with a single sync data replica and a replication log, with
    |data_mount_config| applied to the data replica — the tablet that runs the conflict checks."""
    data_path = path + "_data"
    log_path = path + "_log"

    client.create(
        "chaos_replicated_table",
        path,
        attributes={
            "chaos_cell_bundle": CHAOS_CELL_BUNDLE,
            "schema": SCHEMA,
        },
    )

    def create_replica(replica_path, content_type):
        return client.create(
            "chaos_table_replica",
            attributes={
                "table_path": path,
                "cluster_name": "primary",
                "replica_path": replica_path,
                "mode": "sync",
                "enabled": True,
                "content_type": content_type,
            },
        )

    data_replica_id = create_replica(data_path, "data")
    log_replica_id = create_replica(log_path, "queue")

    client.create(
        "table",
        data_path,
        attributes={
            "dynamic": True,
            "schema": SCHEMA,
            "upstream_replica_id": data_replica_id,
            "tablet_cell_bundle": TABLET_CELL_BUNDLE,
            "mount_config": dict(FLUSH_MOUNT_CONFIG, **data_mount_config),
        },
    )
    client.create(
        "replication_log_table",
        log_path,
        attributes={
            "dynamic": True,
            "schema": SCHEMA,
            "upstream_replica_id": log_replica_id,
            "tablet_cell_bundle": TABLET_CELL_BUNDLE,
        },
    )
    for table in (data_path, log_path):
        client.mount_table(table)
        wait(lambda: client.get(table + "/@tablet_state") == "mounted")

    wait_chaos_writable(client, path)


@pytest.mark.authors(["thenewone"])
class TestChaosHorizonDegradation:
    def test_stretched_chaos_transaction_dies_the_same_way(self):
        # Emulates what production chaos does to a transaction: the start-to-commit window is
        # stretched (there — by cross-cluster 2PC and catch-up commits; here — by holding the
        # transaction open across a flush of the sync data replica). The stretched transaction
        # dies on exactly the same tablet-side limit as on a regular table, with the same
        # nonretryable 1729 surfacing through the chaos write path. As in the regular-table
        # matrix, the transaction's own key is never touched by anybody else.
        client = make_client()
        path = "//tmp/horizon_chaos"
        create_chaos_table_with_mount_config(
            client,
            path,
            {"backing_store_retention_time": 0},
        )

        writer = make_client()
        with pytest.raises(YtError) as err:
            with client.Transaction(type="tablet", attributes={"timeout": 180000}):
                # An independent commit to ANOTHER key, through the chaos table.
                writer.insert_rows(path, [{"key": "other", "value": 1}])
                # The flush that matters happens on the sync DATA replica.
                flush_dynamic_stores(writer, path + "_data")
                wait_chaos_writable(writer, path)
                client.insert_rows(path, [{"key": "mine", "value": 2}])
        assert err.value.contains_code(CANNOT_CHECK_CONFLICTS_AGAINST_CHUNK_STORE), str(err.value)
