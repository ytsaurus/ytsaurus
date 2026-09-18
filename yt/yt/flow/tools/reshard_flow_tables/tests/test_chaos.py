import logging
import sys

import pytest
import yt.wrapper as yt

from yt.wrapper import yson

from yt.yt.flow.tools.reshard_flow_tables.lib import (
    TMP_SUFFIX,
    ReshardRequest,
    build_compact_input_message_pivot_key,
    planned_tablet_count,
    uniform_pivot_keys,
    apply_reshard_plans,
    current_tablet_count,
    get_args,
    reshard_timer_table,
    get_reshard_targets,
    recreate_replication_log,
    reshard_mounted_table,
    reshard_tables,
)

HASH_SCHEMA = [{"name": "hash", "type": "uint64", "sort_order": "ascending"}]


def read_back(pivot_keys):
    """Pivot keys the way @pivot_keys hands them back: the client parses YSON with an encoding, and
    a key column that does not decode as UTF-8 becomes a YsonStringProxy rather than str or bytes."""
    return yson.loads(yson.dumps(pivot_keys, yson_format="binary"), encoding="utf-8")


class FakeClient:
    def __init__(self, attributes=None, proxy=None):
        self.config = {"proxy": {"url": proxy}}
        self.attributes = attributes or {}
        for path, value in self.attributes.items():
            if path.endswith("/@replicas"):
                for replica in value.values():
                    replica.setdefault("state", "enabled")
                    replica.setdefault("mode", "sync")
                    replica.setdefault("replica_reached_last_own_era", True)
        self.calls = []
        self.created_replicas = 0

    # Attributes every dynamic table carries. Spelling them out at each call site would only add
    # noise to tests that are not about the tablet layout.
    DEFAULT_ATTRIBUTES = {"@sorted": True, "@schema": HASH_SCHEMA, "@tablet_count": 1}

    def get(self, path):
        if path not in self.attributes:
            if path.endswith("/@pivot_keys"):
                count = self.attributes.get(
                    path.removesuffix("pivot_keys") + "tablet_count", 2 if "_log" in path else 1
                )
                return uniform_pivot_keys(count, HASH_SCHEMA)
            default = self.DEFAULT_ATTRIBUTES.get(path.rpartition("/")[2])
            if default is not None:
                return default
        return self.attributes[path]

    def exists(self, path):
        return path in self.attributes

    def unmount_table(self, table, sync):
        self.calls.append(("unmount", table))
        self.attributes[f"{table}/@tablet_state"] = "unmounted"

    def reshard_table(self, table, sync, **kwargs):
        self.calls.append(("reshard", table, kwargs))

    def mount_table(self, table, sync):
        self.calls.append(("mount", table))
        self.attributes[f"{table}/@tablet_state"] = "mounted"

    def freeze_table(self, table, sync=False):
        self.calls.append(("freeze", table))
        self.attributes[f"{table}/@tablet_state"] = "frozen"

    def remove(self, path):
        self.calls.append(("remove", path))
        if path.startswith("#"):
            replica_id = path[1:]
            for key, value in self.attributes.items():
                if key.endswith("/@replicas") and replica_id in value:
                    del value[replica_id]
            return
        # A removed table takes its attributes with it: telling "no table here" from "a table that
        # is merely unmounted" is exactly what the resume logic keys on.
        for key in [key for key in self.attributes if key == path or key.startswith(f"{path}/@")]:
            del self.attributes[key]

    def create(self, object_type, path=None, attributes=None):
        self.calls.append(("create", object_type, path, attributes))
        if object_type != "chaos_table_replica":
            self.attributes[f"{path}/@tablet_state"] = "unmounted"
            return
        replicas = self.attributes.get(f"{attributes['table_path']}/@replicas")
        if replicas is not None:
            # Ids are never reused, as in YT: a fake that hands the same id to a later replica
            # would hide exactly the kind of mix-up these tests are here to catch.
            replica_id = f"created-{self.created_replicas}"
            self.created_replicas += 1
            replicas[replica_id] = {
                "cluster_name": attributes["cluster_name"],
                "replica_path": attributes["replica_path"],
                "content_type": attributes["content_type"],
                "mode": attributes["mode"],
                "state": "enabled" if attributes.get("enabled") else "disabled",
                "replica_reached_last_own_era": self.newborn_confirms_era(),
            }
            return replica_id

    def newborn_confirms_era(self):
        return True

    def generate_timestamp(self):
        return 0

    def alter_table(self, table, upstream_replica_id=None):
        self.calls.append(("alter_table", table, upstream_replica_id))

    def alter_table_replica(self, replica_id, enabled):
        self.calls.append(("alter_table_replica", replica_id, enabled))
        for key, value in self.attributes.items():
            if key.endswith("/@replicas") and replica_id in value:
                value[replica_id]["state"] = "enabled" if enabled else "disabled"


def test_plain_table_is_resharded_in_place():
    client = FakeClient({"//pipeline/states/@type": "table"})

    assert get_reshard_targets(client, "//pipeline/states") == [(client, "//pipeline/states")]


def test_chaos_table_expands_to_data_replicas_on_their_clusters():
    client = FakeClient(
        {
            "//pipeline/states/@type": "chaos_replicated_table",
            "//pipeline/states/@replicas": {
                "id-1": {"cluster_name": "pythia", "replica_path": "//pipeline/states", "content_type": "data"},
                "id-2": {"cluster_name": "zeno", "replica_path": "//replica/states", "content_type": "data"},
            },
        }
    )
    clients = {}

    def make_client(proxy):
        return clients.setdefault(proxy, FakeClient())

    targets = get_reshard_targets(client, "//pipeline/states", make_client)

    assert sorted(clients) == ["pythia", "zeno"]
    assert (clients["pythia"], "//pipeline/states") in targets
    assert (clients["zeno"], "//replica/states") in targets
    assert len(targets) == 2


def test_chaos_replication_log_replicas_are_skipped():
    # A written-to replication log cannot be resharded in place, so the tool must not touch it.
    client = FakeClient(
        {
            "//pipeline/states/@type": "chaos_replicated_table",
            "//pipeline/states/@replicas": {
                "id-1": {"cluster_name": "zeno", "replica_path": "//pipeline/states_log", "content_type": "queue"},
                "id-2": {"cluster_name": "zeno", "replica_path": "//pipeline/states", "content_type": "data"},
            },
        }
    )
    clients = {}

    def make_client(proxy):
        return clients.setdefault(proxy, FakeClient())

    targets = get_reshard_targets(client, "//pipeline/states", make_client)

    assert targets == [(clients["zeno"], "//pipeline/states")]


def test_reshard_mounted_table_unmounts_reshards_and_mounts():
    client = FakeClient({"//pipeline/states/@type": "table"})

    reshard_mounted_table(client, "//pipeline/states", tablet_count=5, uniform=True)

    assert client.calls == [
        ("unmount", "//pipeline/states"),
        ("reshard", "//pipeline/states", {"pivot_keys": uniform_pivot_keys(5, HASH_SCHEMA)}),
        ("mount", "//pipeline/states"),
    ]


class FailingReshardClient(FakeClient):
    """Fails the reshard of the table it is asked to fail, as the bundle running out of tablets
    does."""

    def __init__(self, attributes=None, failing_table=None):
        super().__init__(attributes)
        self.failing_table = failing_table

    def reshard_table(self, table, sync, **kwargs):
        super().reshard_table(table, sync, **kwargs)
        if self.failing_table in (None, table) and planned_tablet_count(kwargs) == 5:
            raise RuntimeError('Tablet cell bundle "yacs-prestable" is over tablet count limit')


def test_failed_reshard_restores_the_previous_layout():
    # An unmounted internal table is not a failed release but a dead pipeline: workers get "has no
    # mounted tablets" (code 1702) on every commit, and a stop issued afterwards drains forever.
    client = FailingReshardClient(
        {
            "//pipeline/states/@type": "table",
            "//pipeline/states/@sorted": True,
            "//pipeline/states/@pivot_keys": [[], ["b"]],
        }
    )

    with pytest.raises(RuntimeError):
        reshard_mounted_table(client, "//pipeline/states", tablet_count=5, uniform=True)

    assert client.calls == [
        ("unmount", "//pipeline/states"),
        ("reshard", "//pipeline/states", {"pivot_keys": uniform_pivot_keys(5, HASH_SCHEMA)}),
        # A failed reshard_table(sync=True) does not mean the mutation was rejected -- it also
        # raises when the wait that follows it times out -- so the rollback reshards regardless.
        ("unmount", "//pipeline/states"),
        ("reshard", "//pipeline/states", {"pivot_keys": [[], ["b"]]}),
        ("mount", "//pipeline/states"),
    ]
    assert client.attributes["//pipeline/states/@tablet_state"] == "mounted"


def test_an_ordered_table_is_restored_by_its_tablet_count():
    # An ordered table has no pivot keys; its layout is the plain tablet count.
    client = FailingReshardClient(
        {
            "//pipeline/queue/@type": "table",
            "//pipeline/queue/@sorted": False,
            "//pipeline/queue/@tablet_count": 3,
        }
    )

    with pytest.raises(RuntimeError):
        reshard_mounted_table(client, "//pipeline/queue", tablet_count=5, uniform=True)

    assert ("reshard", "//pipeline/queue", {"tablet_count": 3}) in client.calls
    assert client.attributes["//pipeline/queue/@tablet_state"] == "mounted"


def test_an_interrupted_reshard_restores_the_table():
    # Every call here blocks for up to tablets_ready_timeout (30 minutes), so an operator giving up
    # and hitting Ctrl+C is a real way into the window where the table sits unmounted.
    # KeyboardInterrupt is a BaseException and slips past a bare `except Exception`.
    class InterruptedClient(FakeClient):
        def reshard_table(self, table, sync, **kwargs):
            super().reshard_table(table, sync, **kwargs)
            if planned_tablet_count(kwargs) == 5:
                raise KeyboardInterrupt

    client = InterruptedClient(
        {
            "//pipeline/states/@type": "table",
            "//pipeline/states/@sorted": True,
            "//pipeline/states/@pivot_keys": [[], ["b"]],
        }
    )

    with pytest.raises(KeyboardInterrupt):
        reshard_mounted_table(client, "//pipeline/states", tablet_count=5, uniform=True)

    assert ("reshard", "//pipeline/states", {"pivot_keys": [[], ["b"]]}) in client.calls
    assert client.attributes["//pipeline/states/@tablet_state"] == "mounted"


def test_a_failed_unmount_mounts_the_table_back():
    # The unmount is a mutation like any other: unmount_table(sync=True) is make_request plus a
    # separate wait for the tablets, so a timeout raises over a table that is already unmounted.
    # Nothing has been resharded at that point, but the table is down, which is what kills the
    # pipeline -- so the rollback has to cover this window too. The second unmount succeeds: by
    # then the tablets the first call gave up waiting for have settled.
    class FailingUnmountClient(FakeClient):
        def __init__(self, attributes=None):
            super().__init__(attributes)
            self.unmounts_left_to_fail = 1

        def unmount_table(self, table, sync):
            super().unmount_table(table, sync)
            if self.unmounts_left_to_fail:
                self.unmounts_left_to_fail -= 1
                raise RuntimeError(f"Timed out while waiting for tablets of {table} to unmount")

    client = FailingUnmountClient(
        {
            "//pipeline/states/@type": "table",
            "//pipeline/states/@sorted": True,
            "//pipeline/states/@pivot_keys": [[], ["b"]],
        }
    )

    with pytest.raises(RuntimeError, match="Timed out"):
        reshard_mounted_table(client, "//pipeline/states", tablet_count=5, uniform=True)

    assert client.calls == [
        ("unmount", "//pipeline/states"),
        ("unmount", "//pipeline/states"),
        ("reshard", "//pipeline/states", {"pivot_keys": [[], ["b"]]}),
        ("mount", "//pipeline/states"),
    ]
    assert client.attributes["//pipeline/states/@tablet_state"] == "mounted"


def test_a_failing_chaos_replica_is_restored_on_its_own_cluster():
    # A CRT owns no tablets: the reshard runs against every data replica, each on its own cluster
    # and its own bundle. A replica that fails must be put back where it stands, and the failure
    # must still reach the caller rather than being swallowed on the way out.
    crt = "//pipeline/states"
    client = FakeClient(
        {
            f"{crt}/@type": "chaos_replicated_table",
            f"{crt}/@replicas": {
                "id-1": {"cluster_name": "pythia", "replica_path": crt, "content_type": "data"},
                "id-2": {"cluster_name": "zeno", "replica_path": "//replica/states", "content_type": "data"},
            },
        }
    )
    clients = {
        "pythia": FakeClient({f"{crt}/@type": "table", f"{crt}/@sorted": True, f"{crt}/@pivot_keys": [[], ["p"]]}),
        "zeno": FailingReshardClient(
            {
                "//replica/states/@type": "table",
                "//replica/states/@sorted": True,
                "//replica/states/@pivot_keys": [[], ["z"]],
            },
            failing_table="//replica/states",
        ),
    }

    with pytest.raises(RuntimeError):
        reshard_mounted_table(client, crt, make_client=clients.get, tablet_count=5, uniform=True)

    # The healthy replica took the new layout and stayed mounted...
    assert clients["pythia"].calls == [
        ("unmount", crt),
        ("reshard", crt, {"pivot_keys": uniform_pivot_keys(5, HASH_SCHEMA)}),
        ("mount", crt),
    ]
    assert clients["pythia"].attributes[f"{crt}/@tablet_state"] == "mounted"
    # ...and the failing one went back to the pivot keys it had, on its own cluster.
    assert ("reshard", "//replica/states", {"pivot_keys": [[], ["z"]]}) in clients["zeno"].calls
    assert clients["zeno"].attributes["//replica/states/@tablet_state"] == "mounted"


def test_chaos_table_width_is_read_from_its_replicas():
    # A CRT node has no @tablet_count of its own, so ordering by delta has to look at the replicas.
    crt = "//pipeline/states"
    client = FakeClient(
        {
            f"{crt}/@type": "chaos_replicated_table",
            f"{crt}/@replicas": {
                "id-1": {"cluster_name": "pythia", "replica_path": crt, "content_type": "data"},
                "id-2": {"cluster_name": "zeno", "replica_path": "//replica/states", "content_type": "data"},
            },
        }
    )
    clients = {
        "pythia": FakeClient({f"{crt}/@type": "table", f"{crt}/@tablet_count": 8}),
        "zeno": FakeClient({"//replica/states/@type": "table", "//replica/states/@tablet_count": 40}),
    }

    assert current_tablet_count(client, crt, make_client=clients.get) == 40


def test_failed_mount_restores_the_previous_layout():
    # The new layout is in place but will not come up; the previous one did, so go back to it.
    class FailingMountClient(FakeClient):
        def __init__(self, attributes=None):
            super().__init__(attributes)
            self.mounts_left_to_fail = 1

        def mount_table(self, table, sync):
            if self.mounts_left_to_fail:
                self.mounts_left_to_fail -= 1
                self.calls.append(("failed mount", table))
                raise RuntimeError("Not enough tablet static memory")
            super().mount_table(table, sync)

    client = FailingMountClient(
        {
            "//pipeline/states/@type": "table",
            "//pipeline/states/@sorted": True,
            "//pipeline/states/@pivot_keys": [[], ["b"]],
        }
    )

    with pytest.raises(RuntimeError):
        reshard_mounted_table(client, "//pipeline/states", tablet_count=5, uniform=True)

    assert client.calls == [
        ("unmount", "//pipeline/states"),
        ("reshard", "//pipeline/states", {"pivot_keys": uniform_pivot_keys(5, HASH_SCHEMA)}),
        ("failed mount", "//pipeline/states"),
        ("unmount", "//pipeline/states"),
        ("reshard", "//pipeline/states", {"pivot_keys": [[], ["b"]]}),
        ("mount", "//pipeline/states"),
    ]
    assert client.attributes["//pipeline/states/@tablet_state"] == "mounted"


def test_a_failed_rollback_still_reports_the_original_error():
    # Restoring runs while another error propagates, and that first error is the one worth
    # reporting -- the rollback failure must not shadow it.
    class DoomedClient(FakeClient):
        def reshard_table(self, table, sync, **kwargs):
            raise RuntimeError("over tablet count limit" if planned_tablet_count(kwargs) == 5 else "rollback is broken")

        def mount_table(self, table, sync):
            raise RuntimeError("mount is broken too")

    client = DoomedClient({"//pipeline/states/@type": "table"})

    with pytest.raises(RuntimeError, match="over tablet count limit"):
        reshard_mounted_table(client, "//pipeline/states", tablet_count=5, uniform=True)


def test_plans_run_smallest_growth_first():
    # Every table of a pipeline shares one bundle, and its tablet count is a hard limit. Running a
    # fixed order lets a growing table eat the budget a later one needs, while the tables that
    # would have released tablets never get their turn.
    client = FakeClient(
        {
            "//pipeline/grows_a_lot/@type": "table",
            "//pipeline/grows_a_lot/@tablet_count": 10,
            "//pipeline/grows_a_bit/@type": "table",
            "//pipeline/grows_a_bit/@tablet_count": 10,
            "//pipeline/shrinks/@type": "table",
            "//pipeline/shrinks/@tablet_count": 100,
        }
    )
    plans = [
        ("//pipeline/grows_a_lot", {"pivot_keys": uniform_pivot_keys(50, HASH_SCHEMA)}),
        ("//pipeline/grows_a_bit", {"pivot_keys": uniform_pivot_keys(20, HASH_SCHEMA)}),
        ("//pipeline/shrinks", {"pivot_keys": uniform_pivot_keys(10, HASH_SCHEMA)}),
    ]

    apply_reshard_plans(client, plans)

    assert [table for kind, table, *_ in client.calls if kind == "reshard"] == [
        "//pipeline/shrinks",
        "//pipeline/grows_a_bit",
        "//pipeline/grows_a_lot",
    ]


def test_dry_run_logs_before_after_without_mutating_tables(caplog, monkeypatch):
    client = FakeClient(
        {
            "//pipeline/states/@type": "table",
            "//pipeline/states/@tablet_count": 3,
        }
    )
    monkeypatch.setattr(yt, "YtClient", lambda **kwargs: client)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "reshard_flow_tables",
            "--external-table",
            "//pipeline/states",
            "--tablet-count",
            "5",
            "--dry-run",
        ],
    )

    with caplog.at_level(logging.INFO):
        reshard_tables(get_args())

    assert client.calls == []
    assert [
        record.getMessage() for record in caplog.records if record.funcName in ("apply_reshard_plans", "log_table_plan")
    ] == [
        "//pipeline/states: 3 => 5 tablets (+2, reshard, boundaries changed)",
        "Dry run: no tables will be modified",
    ]


def test_plan_width_comes_from_the_pivot_keys_when_they_are_given():
    client = FakeClient({"//pipeline/states/@type": "table", "//pipeline/states/@tablet_count": 3})

    apply_reshard_plans(client, [("//pipeline/states", {"pivot_keys": [[], ["a"]]})])

    assert ("reshard", "//pipeline/states", {"pivot_keys": [[], ["a"]]}) in client.calls


def test_reshard_timer_table_plans_and_applies_in_one_call():
    # Kept for alice/wonderlogs/flow/rt_dwh/tools/ensure_flow_sharding, which reshards the timers
    # table on its own and imports this entry point rather than running the tool.
    client = FakeClient({"//pipeline/timers/@type": "table", "//pipeline/timers/@tablet_count": 1})

    reshard_timer_table(client, ["computation"], "//pipeline", 2)

    assert [kind for kind, *_ in client.calls] == ["unmount", "reshard", "mount"]
    assert client.calls[1][1] == "//pipeline/timers"
    assert len(client.calls[1][2]["pivot_keys"]) == 2


def test_recreate_replication_log_swaps_it():
    crt = "//pipeline/states"
    log = "//pipeline/states_log"
    tmp = f"{log}.reshard_tmp"
    client = FakeClient(
        {
            f"{crt}/@type": "chaos_replicated_table",
            f"{crt}/@replicas": {
                "data-id": {
                    "cluster_name": "zeno",
                    "replica_path": "//replica/states",
                    "content_type": "data",
                    "replication_lag_timestamp": 10**18,
                },
                "queue-id": {
                    "cluster_name": "zeno",
                    "replica_path": log,
                    "content_type": "queue",
                    "state": "enabled",
                },
            },
        }
    )
    log_client = FakeClient(
        {
            f"{log}/@schema": [{"name": "key", "type": "string"}],
            f"{log}/@tablet_cell_bundle": "bigb",
            f"{log}/@primary_medium": "ssd_blobs",
            f"{log}/@tablet_state": "mounted",
        }
    )

    recreate_replication_log(
        client,
        crt,
        "zeno",
        log,
        log_pivot_keys=[[], [123]],
        make_client=lambda proxy: log_client,
        sleep=lambda seconds: None,
    )

    expected_attributes = {
        "dynamic": True,
        "schema": [{"name": "key", "type": "string"}],
        "tablet_cell_bundle": "bigb",
        "primary_medium": "ssd_blobs",
    }
    # Swap through a temporary log: attach tmp, retire the old log, attach the final log,
    # retire tmp. Writes always have a sync log to land in.
    assert log_client.calls == [
        ("create", "replication_log_table", tmp, expected_attributes),
        ("reshard", tmp, {"pivot_keys": [[], [123]]}),
        ("alter_table", tmp, "created-0"),
        ("mount", tmp),
        ("freeze", log),
        ("unmount", log),
        ("remove", log),
        ("create", "replication_log_table", log, expected_attributes),
        ("reshard", log, {"pivot_keys": [[], [123]]}),
        ("alter_table", log, "created-1"),
        ("mount", log),
        ("freeze", tmp),
        ("unmount", tmp),
        ("remove", tmp),
    ]

    def replica_attributes(path):
        return {
            "table_path": crt,
            "cluster_name": "zeno",
            "replica_path": path,
            "content_type": "queue",
            "mode": "sync",
            "enabled": True,
            "catchup": False,
            "enable_replicated_table_tracker": False,
        }

    assert client.calls == [
        ("create", "chaos_table_replica", None, replica_attributes(tmp)),
        ("alter_table_replica", "queue-id", False),
        ("remove", "#queue-id"),
        ("create", "chaos_table_replica", None, replica_attributes(log)),
        ("alter_table_replica", "created-0", False),
        ("remove", "#created-0"),
    ]


def test_also_chaos_replication_logs_recreates_the_log_after_data_reshard():
    crt = "//pipeline/states"
    log = "//pipeline/states_log"
    data = "//replica/states"
    clients = {}

    def make_client(proxy):
        return clients.setdefault(
            proxy,
            FakeClient(
                {
                    f"{log}/@schema": [],
                    f"{log}/@tablet_state": "mounted",
                }
            ),
        )

    client = FakeClient(
        {
            f"{crt}/@type": "chaos_replicated_table",
            f"{crt}/@replicas": {
                "data-id": {
                    "cluster_name": "zeno",
                    "replica_path": data,
                    "content_type": "data",
                    "replication_lag_timestamp": 10**18,
                },
                "queue-id": {
                    "cluster_name": "zeno",
                    "replica_path": log,
                    "content_type": "queue",
                    "state": "enabled",
                },
            },
        }
    )

    reshard_mounted_table(
        client, crt, also_chaos_replication_logs=True, make_client=make_client, tablet_count=50, uniform=True
    )

    log_ops = [call[0] for call in clients["zeno"].calls]
    # data replica resharded first, then the log swapped through the temporary log:
    # 50 data tablets -> 25 log tablets
    assert log_ops == [
        "unmount",
        "reshard",
        "mount",
        "create",
        "reshard",
        "alter_table",
        "mount",
        "freeze",
        "unmount",
        "remove",
        "create",
        "reshard",
        "alter_table",
        "mount",
        "freeze",
        "unmount",
        "remove",
    ]
    log_reshards = [call for call in clients["zeno"].calls if call[0] == "reshard"][1:]
    assert all(len(call[2]["pivot_keys"]) == 25 for call in log_reshards)


def test_stale_tmp_table_without_replica_is_removed():
    crt = "//pipeline/states"
    log = "//pipeline/states_log"
    tmp = f"{log}.reshard_tmp"
    client = FakeClient(
        {
            f"{crt}/@type": "chaos_replicated_table",
            f"{crt}/@replicas": {
                "data-id": {
                    "cluster_name": "zeno",
                    "replica_path": "//replica/states",
                    "content_type": "data",
                    "replication_lag_timestamp": 10**18,
                },
                "queue-id": {
                    "cluster_name": "zeno",
                    "replica_path": log,
                    "content_type": "queue",
                    "state": "enabled",
                },
            },
        }
    )
    # the tmp table exists (a previous run died between create and attach), no replica for it
    log_client = FakeClient(
        {
            f"{log}/@schema": [],
            f"{log}/@tablet_state": "mounted",
            f"{tmp}/@tablet_state": "frozen",
        }
    )

    recreate_replication_log(
        client,
        crt,
        "zeno",
        log,
        log_pivot_keys=[[]],
        make_client=lambda proxy: log_client,
        sleep=lambda seconds: None,
    )

    # It is dropped before the swap starts; a leftover left frozen by an interrupted retire has to
    # be unmounted first, so that a table can be created at the path again.
    assert log_client.calls[0] == ("unmount", tmp)
    assert log_client.calls[1] == ("remove", tmp)
    assert log_client.calls[2][0] == "create"


def test_swap_resumes_when_only_the_tmp_log_is_attached():
    # A previous run died after retiring the canonical log, so the card carries nothing but
    # *.reshard_tmp. Moving the log back to its canonical path is the whole remaining job -- a rerun
    # that dismissed the temporary log as a leftover used to strand the card on it forever.
    crt = "//pipeline/states"
    log = "//pipeline/states_log"
    tmp = f"{log}.reshard_tmp"
    client = FakeClient(
        {
            f"{crt}/@type": "chaos_replicated_table",
            f"{crt}/@replicas": {
                "data-id": {
                    "cluster_name": "zeno",
                    "replica_path": "//replica/states",
                    "content_type": "data",
                    "replication_lag_timestamp": 10**18,
                },
                "tmp-id": {
                    "cluster_name": "zeno",
                    "replica_path": tmp,
                    "content_type": "queue",
                    "state": "enabled",
                },
            },
        }
    )
    # Only the temporary log exists, so its attributes are the ones to carry over.
    log_client = FakeClient(
        {
            f"{tmp}/@schema": [{"name": "key", "type": "string"}],
            f"{tmp}/@tablet_cell_bundle": "bigb",
            f"{tmp}/@tablet_state": "frozen",
        }
    )

    recreate_replication_log(
        client,
        crt,
        "zeno",
        log,
        log_pivot_keys=[[]],
        make_client=lambda proxy: log_client,
        sleep=lambda seconds: None,
    )

    # The canonical log is attached first (writes keep landing in tmp meanwhile), and only then is
    # tmp retired -- the card is never left without a sync log.
    assert [call[0] for call in log_client.calls] == [
        "create",
        "reshard",
        "alter_table",
        "mount",
        "freeze",
        "unmount",
        "remove",
    ]
    assert log_client.calls[0][2] == log
    assert log_client.calls[0][3] == {
        "dynamic": True,
        "schema": [{"name": "key", "type": "string"}],
        "tablet_cell_bundle": "bigb",
    }
    assert log_client.calls[-1] == ("remove", tmp)
    assert ("alter_table_replica", "tmp-id", False) in client.calls


def test_birth_race_retries_the_newborn_log():
    crt = "//pipeline/states"
    log = "//pipeline/states_log"

    class RacyClient(FakeClient):
        # The first newborn loses the promotion race, every following one wins.
        def __init__(self, attributes=None):
            super().__init__(attributes)
            self.births = 0

        def newborn_confirms_era(self):
            self.births += 1
            return self.births > 1

    client = RacyClient(
        {
            f"{crt}/@type": "chaos_replicated_table",
            f"{crt}/@replicas": {
                "data-id": {
                    "cluster_name": "zeno",
                    "replica_path": "//replica/states",
                    "content_type": "data",
                    "replication_lag_timestamp": 10**18,
                },
                "queue-id": {
                    "cluster_name": "zeno",
                    "replica_path": log,
                    "content_type": "queue",
                    "state": "enabled",
                },
            },
        }
    )
    log_client = FakeClient(
        {
            f"{log}/@schema": [],
            f"{log}/@tablet_state": "mounted",
        }
    )

    recreate_replication_log(
        client,
        crt,
        "zeno",
        log,
        log_pivot_keys=[[]],
        make_client=lambda proxy: log_client,
        sleep=lambda seconds: None,
        confirm_timeout=0,
    )

    # the wedged first tmp is retired (freeze/unmount/remove) and a second one attached
    creates = [call for call in log_client.calls if call[0] == "create"]
    assert len(creates) == 3  # wedged tmp, healthy tmp, healthy final
    assert client.births == 3


def test_plain_external_table_is_resharded_without_a_log():
    # A non-chaos external table with --also-chaos-replication-logs: reshard the table in place,
    # and quietly do nothing about logs (it has none). This is the "user has no chaos" case.
    table = "//home/user/counters"
    client = FakeClient({f"{table}/@type": "table"})

    reshard_mounted_table(client, table, also_chaos_replication_logs=True, tablet_count=7, uniform=True)

    assert client.calls == [
        ("unmount", table),
        ("reshard", table, {"pivot_keys": uniform_pivot_keys(7, HASH_SCHEMA)}),
        ("mount", table),
    ]


def test_every_replica_cluster_gets_its_log_recreated():
    # yt_sync gives a chaos table one replication log per replica cluster, and they all live at the
    # same path -- so a log is identified by (cluster, path). Keying by the path alone recreates an
    # arbitrary one of them and leaves the rest un-resharded without a word.
    crt = "//pipeline/states"
    log = "//pipeline/states_log"
    data = "//replica/states"
    clients = {}

    def make_client(proxy):
        return clients.setdefault(
            proxy,
            FakeClient(
                {
                    f"{log}/@schema": [],
                    f"{log}/@tablet_count": 3,
                    f"{log}/@tablet_state": "mounted",
                }
            ),
        )

    client = FakeClient(
        {
            f"{crt}/@type": "chaos_replicated_table",
            f"{crt}/@replicas": {
                f"data-{cluster}": {
                    "cluster_name": cluster,
                    "replica_path": data,
                    "content_type": "data",
                    "replication_lag_timestamp": 10**18,
                }
                for cluster in ("pythia", "zeno")
            }
            | {
                f"queue-{cluster}": {
                    "cluster_name": cluster,
                    "replica_path": log,
                    "content_type": "queue",
                    "state": "enabled",
                }
                for cluster in ("pythia", "zeno")
            },
        }
    )

    reshard_mounted_table(
        client, crt, also_chaos_replication_logs=True, make_client=make_client, tablet_count=4, uniform=True
    )

    assert sorted(clients) == ["pythia", "zeno"]
    for cluster in ("pythia", "zeno"):
        calls = clients[cluster].calls
        # Both clusters get the full swap: tmp created, the old log removed, the log recreated, tmp
        # removed. Neither is left carrying its original log.
        assert [call[2] for call in calls if call[0] == "create"] == [f"{log}{TMP_SUFFIX}", log], cluster
        assert [call[1] for call in calls if call[0] == "remove"] == [log, f"{log}{TMP_SUFFIX}"], cluster
    # Both of the original log replicas were retired -- neither cluster was skipped.
    retired = [call[1] for call in client.calls if call[0] == "alter_table_replica"]
    assert [replica_id for replica_id in retired if replica_id.startswith("queue-")] == [
        "queue-pythia",
        "queue-zeno",
    ], retired


def test_unmounted_canonical_log_is_discarded_before_the_serving_tmp():
    # A previous run died inside the attach of the canonical log, between creating its chaos replica
    # and mounting the table: the card carries a replica whose table never attached to it. Such a
    # log serves nothing and can never freeze, so retiring it the normal way blocks on the freeze --
    # and does so after retiring the temporary log, which is the one still taking writes.
    crt = "//pipeline/states"
    log = "//pipeline/states_log"
    tmp = f"{log}{TMP_SUFFIX}"
    client = FakeClient(
        {
            f"{crt}/@type": "chaos_replicated_table",
            f"{crt}/@replicas": {
                "data-id": {
                    "cluster_name": "zeno",
                    "replica_path": "//replica/states",
                    "content_type": "data",
                    "replication_lag_timestamp": 10**18,
                },
                "canonical-id": {
                    "cluster_name": "zeno",
                    "replica_path": log,
                    "content_type": "queue",
                    "state": "enabled",
                },
                "tmp-id": {
                    "cluster_name": "zeno",
                    "replica_path": tmp,
                    "content_type": "queue",
                    "state": "enabled",
                },
            },
        }
    )
    log_client = FakeClient(
        {
            f"{log}/@schema": [{"name": "key", "type": "string"}],
            f"{log}/@tablet_state": "unmounted",
            f"{tmp}/@tablet_state": "mounted",
        }
    )

    recreate_replication_log(
        client,
        crt,
        "zeno",
        log,
        log_pivot_keys=[[]],
        make_client=lambda proxy: log_client,
        sleep=lambda seconds: None,
    )

    # The broken log is dropped outright -- no freeze, and no unmount either, it is unmounted
    # already -- and the tmp is retired only once a healthy log stands at the canonical path.
    assert [call[0] for call in log_client.calls] == [
        "remove",
        "create",
        "reshard",
        "alter_table",
        "mount",
        "freeze",
        "unmount",
        "remove",
    ]
    assert log_client.calls[0] == ("remove", log)
    assert ("freeze", log) not in log_client.calls
    assert [call for call in client.calls if call[0] == "alter_table_replica"] == [
        ("alter_table_replica", "canonical-id", False),
        ("alter_table_replica", "tmp-id", False),
    ]


def test_unmounted_canonical_log_without_a_tmp_brings_one_up_first():
    # The same broken canonical, but nothing else is attached. The card may not be left without a
    # sync log, and a log that never attached is not one, so the temporary log has to come up before
    # the broken one can go away.
    crt = "//pipeline/states"
    log = "//pipeline/states_log"
    tmp = f"{log}{TMP_SUFFIX}"
    client = FakeClient(
        {
            f"{crt}/@type": "chaos_replicated_table",
            f"{crt}/@replicas": {
                "data-id": {
                    "cluster_name": "zeno",
                    "replica_path": "//replica/states",
                    "content_type": "data",
                    "replication_lag_timestamp": 10**18,
                },
                "canonical-id": {
                    "cluster_name": "zeno",
                    "replica_path": log,
                    "content_type": "queue",
                    "state": "enabled",
                },
            },
        }
    )
    log_client = FakeClient(
        {
            f"{log}/@schema": [],
            f"{log}/@tablet_state": "unmounted",
        }
    )

    recreate_replication_log(
        client,
        crt,
        "zeno",
        log,
        log_pivot_keys=[[]],
        make_client=lambda proxy: log_client,
        sleep=lambda seconds: None,
    )

    assert [(call[0], call[1] if call[0] != "create" else call[2]) for call in log_client.calls] == [
        ("create", tmp),
        ("reshard", tmp),
        ("alter_table", tmp),
        ("mount", tmp),
        ("remove", log),
        ("create", log),
        ("reshard", log),
        ("alter_table", log),
        ("mount", log),
        ("freeze", tmp),
        ("unmount", tmp),
        ("remove", tmp),
    ]


@pytest.mark.parametrize("dry_run", [False, True])
def test_plain_table_plan_logs_growth_shrink_and_unchanged(caplog, dry_run):
    client = FakeClient(
        {
            f"//pipeline/{name}/@{attribute}": value
            for name, count in (("grow", 1), ("shrink", 5), ("same", 3))
            for attribute, value in (("type", "table"), ("tablet_count", count))
        },
        proxy="zeno",
    )
    plans = [(f"//pipeline/{name}", {"pivot_keys": [[], ["a"], ["b"]]}) for name in ("grow", "shrink", "same")]

    with caplog.at_level(logging.INFO):
        apply_reshard_plans(client, plans, dry_run=dry_run)

    assert [
        record.getMessage()
        for record in caplog.records
        if record.funcName in ("log_table_plan", "log_replication_plan")
    ] == [
        "zeno://pipeline/shrink: 5 => 3 tablets (-2, reshard, boundaries changed)",
        "zeno://pipeline/same: 3 => 3 tablets (+0, reshard, boundaries changed)",
        "zeno://pipeline/grow: 1 => 3 tablets (+2, reshard, boundaries changed)",
    ]
    if dry_run:
        assert client.calls == []
    else:
        assert [(kind, path) for kind, path, *_ in client.calls] == [
            (kind, f"//pipeline/{name}")
            for name in ("shrink", "same", "grow")
            for kind in ("unmount", "reshard", "mount")
        ]


@pytest.mark.parametrize("also_logs", [False, True])
@pytest.mark.parametrize("log_state", ["canonical", "temporary", "both"])
def test_chaos_dry_run_logs_each_cluster_without_mutations(caplog, monkeypatch, also_logs, log_state):
    table = "//pipeline/states"
    log = f"{table}_log"
    replicas = {}
    clients = {}
    for cluster, data_count, log_count in (("pythia", 2, 7), ("zeno", 8, 3)):
        attributes = {f"{table}/@tablet_count": data_count}
        replicas[f"data-{cluster}"] = {
            "cluster_name": cluster,
            "replica_path": table,
            "content_type": "data",
        }
        for suffix in ("", TMP_SUFFIX):
            if (suffix == "" and log_state == "temporary") or (suffix and log_state == "canonical"):
                continue
            path = f"{log}{suffix}"
            attributes[f"{path}/@tablet_count"] = log_count
            attributes[f"{path}/@tablet_state"] = "mounted"
            replicas[f"queue-{cluster}{suffix}"] = {
                "cluster_name": cluster,
                "replica_path": path,
                "content_type": "queue",
            }
        clients[cluster] = FakeClient(attributes, proxy=cluster)
    client = FakeClient({f"{table}/@type": "chaos_replicated_table", f"{table}/@replicas": replicas})

    def fail_timestamp():
        pytest.fail("Dry-run must not generate a replication barrier timestamp")

    monkeypatch.setattr(client, "generate_timestamp", fail_timestamp)
    with caplog.at_level(logging.INFO):
        apply_reshard_plans(
            client,
            [(table, {"pivot_keys": [[], [1], [2], [3], [4]]})],
            also_chaos_replication_logs=also_logs,
            make_client=clients.__getitem__,
            dry_run=True,
        )

    messages = [
        record.getMessage()
        for record in caplog.records
        if record.funcName in ("log_table_plan", "log_replication_plan")
    ]
    expected = [
        "zeno://pipeline/states: 8 => 5 tablets (-3, reshard, boundaries changed)",
        "pythia://pipeline/states: 2 => 5 tablets (+3, reshard, boundaries changed)",
    ]
    if also_logs:
        for cluster, count in (("pythia", 7), ("zeno", 3)):
            if log_state == "temporary":
                expected.append(f"{cluster}:{log}: absent => 3 tablets (recreate)")
            else:
                expected.append(f"{cluster}:{log}: {count} => 3 tablets ({3 - count:+d}, recreate, boundaries changed)")
            if log_state != "canonical":
                expected.append(f"{cluster}:{log}{TMP_SUFFIX}: {count} => 0 tablets (remove temporary log)")
    assert messages == expected
    assert client.calls == []
    assert all(replica_client.calls == [] for replica_client in clients.values())


@pytest.mark.parametrize("dry_run", [False, True])
@pytest.mark.parametrize("same_boundaries", [False, True])
def test_analyze_layout_once_and_skip_exact_matches(caplog, dry_run, same_boundaries):
    from collections import Counter

    table = "//pipeline/states"
    old = [[], [10]]
    target = old if same_boundaries else [[], [20]]

    class ReadOnceClient(FakeClient):
        def __init__(self):
            super().__init__({f"{table}/@type": "table", f"{table}/@pivot_keys": old})
            self.reads = Counter()

        def get(self, path):
            self.reads[path] += 1
            assert self.reads[path] == 1, f"Repeated analysis: {path}"
            assert not self.calls, f"Analysis after mutation: {path}"
            return super().get(path)

    client = ReadOnceClient()
    with caplog.at_level(logging.INFO):
        apply_reshard_plans(client, [(table, {"pivot_keys": target})], dry_run=dry_run)

    if dry_run or same_boundaries:
        assert client.calls == []
    else:
        assert client.calls == [("unmount", table), ("reshard", table, {"pivot_keys": target}), ("mount", table)]
    assert ("already OK, boundaries unchanged" in caplog.text) == same_boundaries
    assert ("boundaries changed" in caplog.text) == (not same_boundaries)
    assert "2 => 2 tablets" in caplog.text


@pytest.mark.parametrize(
    "column_type, expected",
    [
        ("uint64", [[], [2**64 // 3], [2**65 // 3]]),
        ("int8", [[], [-43], [42]]),
    ],
)
def test_uniform_boundaries_match_native_rounding(column_type, expected):
    schema = [{"name": "hash", "type": column_type, "sort_order": "ascending"}]
    assert uniform_pivot_keys(3, schema) == expected


@pytest.mark.parametrize("dry_run", [False, True])
def test_uniform_reshard_skips_matching_boundaries(caplog, dry_run):
    table = "//pipeline/partition_states"
    client = FakeClient({f"{table}/@type": "table", f"{table}/@pivot_keys": uniform_pivot_keys(3, HASH_SCHEMA)})
    with caplog.at_level(logging.INFO):
        apply_reshard_plans(client, [(table, {"tablet_count": 3, "uniform": True})], dry_run=dry_run)
    assert client.calls == []
    assert "already OK" in caplog.text


@pytest.mark.parametrize("compact", [False, True])
def test_computation_counts_include_shared_tablets_and_first_computation(caplog, compact):
    from yt.yt.flow.tools.reshard_flow_tables.lib import plan_computation_key_table

    table = "//pipeline/states"
    schema = [
        {
            "name": "deduplication_message_key" if compact else "computation_id",
            "type": "string",
            "sort_order": "ascending",
        }
    ]
    client = FakeClient({f"{table}/@type": "table", f"{table}/@schema": schema, f"{table}/@pivot_keys": [[]]})
    request = plan_computation_key_table(["a", "b"], {}, table, 2, compact_key=compact)
    with caplog.at_level(logging.INFO):
        apply_reshard_plans(client, [request], dry_run=True)
    assert "1 => 4 tablets" in caplog.text
    assert "computation_id='a': 1 => 2 tablets" in caplog.text
    assert "computation_id='b': 1 => 2 tablets" in caplog.text
    assert client.calls == []


def test_binary_compact_pivot_keys_are_counted(caplog):
    from yt.yt.flow.tools.reshard_flow_tables.lib import plan_compact_input_table

    table = "//pipeline/compact_input_messages"
    schema = [{"name": "deduplication_message_key", "type": "string", "sort_order": "ascending"}]
    previous = [[], build_compact_input_message_pivot_key("a", 2**63), build_compact_input_message_pivot_key("b")]
    client = FakeClient(
        {f"{table}/@type": "table", f"{table}/@schema": schema, f"{table}/@pivot_keys": read_back(previous)}
    )
    request = plan_compact_input_table(["a", "b"], "//pipeline", 2)
    with caplog.at_level(logging.INFO):
        apply_reshard_plans(client, [request], dry_run=True)
    assert "3 => 4 tablets" in caplog.text
    assert "computation_id='a': 2 => 2 tablets" in caplog.text
    assert "computation_id='b': 1 => 2 tablets" in caplog.text
    assert client.calls == []


def test_removed_computation_reports_remaining_shared_shard(caplog):
    table = "//pipeline/states"
    schema = [{"name": "computation_id", "type": "string", "sort_order": "ascending"}]
    client = FakeClient(
        {
            f"{table}/@type": "table",
            f"{table}/@schema": schema,
            f"{table}/@pivot_keys": [[], ["a", [1]], ["old"], ["old", [1]]],
        }
    )
    request = ReshardRequest(table, {"pivot_keys": [[]]}, ("a",))
    with caplog.at_level(logging.INFO):
        apply_reshard_plans(client, [request], dry_run=True)
    assert "computation_id='a': 2 => 1 tablets" in caplog.text
    assert "computation_id='old': 2 => 1 tablets" in caplog.text


@pytest.mark.parametrize("dry_run", [False, True])
def test_matching_chaos_logs_are_not_recreated(caplog, dry_run):
    from collections import Counter

    table, log = "//pipeline/states", "//pipeline/states_log"
    client = FakeClient(
        {
            f"{table}/@type": "chaos_replicated_table",
            f"{table}/@replicas": {
                "data": {"cluster_name": "zeno", "replica_path": table, "content_type": "data"},
                "queue": {"cluster_name": "zeno", "replica_path": log, "content_type": "queue"},
            },
        }
    )

    class ReadOnceClient(FakeClient):
        def __init__(self):
            super().__init__(
                {
                    f"{table}/@pivot_keys": [[], [10]],
                    f"{log}/@pivot_keys": [[]],
                    f"{log}/@tablet_state": "mounted",
                    f"{log}/@schema": HASH_SCHEMA,
                },
                proxy="zeno",
            )
            self.reads = Counter()

        def get(self, path):
            self.reads[path] += 1
            if not path.endswith(("/@replicas", "/@tablet_state")):
                assert self.reads[path] == 1, path
            return super().get(path)

    replica = ReadOnceClient()
    with caplog.at_level(logging.INFO):
        apply_reshard_plans(
            client,
            [(table, {"pivot_keys": [[], [10]]})],
            also_chaos_replication_logs=True,
            make_client=lambda cluster: replica,
            dry_run=dry_run,
        )
    assert client.calls == replica.calls == []
    assert caplog.text.count("already OK") == 2


@pytest.mark.parametrize("target_key, unchanged", [(b"a", True), ("b", False)])
def test_boundary_comparison_normalizes_string_encoding(caplog, target_key, unchanged):
    table = "//pipeline/states"
    client = FakeClient({f"{table}/@type": "table", f"{table}/@pivot_keys": [[], ["a"]]})
    with caplog.at_level(logging.INFO):
        apply_reshard_plans(client, [(table, {"pivot_keys": [[], [target_key]]})])
    assert (client.calls == []) == unchanged


def test_boundary_comparison_preserves_signedness():
    from yt.wrapper import yson

    table = "//pipeline/states"
    client = FakeClient({f"{table}/@type": "table", f"{table}/@pivot_keys": [[], [1]]})
    apply_reshard_plans(client, [(table, {"pivot_keys": [[], [yson.YsonUint64(1)]]})])
    assert len(client.calls) == 3


@pytest.mark.parametrize("dry_run", [False, True])
def test_matching_canonical_log_only_retires_temporary_log(caplog, dry_run):
    table, log = "//pipeline/states", "//pipeline/states_log"
    tmp = f"{log}{TMP_SUFFIX}"
    client = FakeClient(
        {
            f"{table}/@type": "chaos_replicated_table",
            f"{table}/@replicas": {
                "data": {
                    "cluster_name": "zeno",
                    "replica_path": table,
                    "content_type": "data",
                    "replication_lag_timestamp": 10**18,
                },
                "queue": {"cluster_name": "zeno", "replica_path": log, "content_type": "queue", "state": "enabled"},
                "tmp": {"cluster_name": "zeno", "replica_path": tmp, "content_type": "queue", "state": "enabled"},
            },
        }
    )
    replica = FakeClient(
        {
            f"{table}/@pivot_keys": [[], [10]],
            f"{log}/@pivot_keys": [[]],
            f"{log}/@tablet_state": "mounted",
            f"{log}/@schema": HASH_SCHEMA,
            f"{tmp}/@tablet_state": "frozen",
        },
        proxy="zeno",
    )
    with caplog.at_level(logging.INFO):
        apply_reshard_plans(
            client,
            [(table, {"pivot_keys": [[], [10]]})],
            also_chaos_replication_logs=True,
            make_client=lambda cluster: replica,
            dry_run=dry_run,
        )
    if dry_run:
        assert client.calls == replica.calls == []
    else:
        assert replica.calls == [("freeze", tmp), ("unmount", tmp), ("remove", tmp)]
        assert client.calls == [("alter_table_replica", "tmp", False), ("remove", "#tmp")]
    assert caplog.text.count("already OK") == 2


def test_chaos_execution_does_not_repeat_layout_analysis():
    from collections import Counter

    table, log = "//pipeline/states", "//pipeline/states_log"
    clients = []

    class StrictClient(FakeClient):
        def __init__(self, attributes):
            super().__init__(attributes)
            self.reads = Counter()
            clients.append(self)

        def get(self, path):
            executing = any(client.calls for client in clients)
            if executing:
                assert path.endswith(("/@replicas", "/@tablet_state")), f"Repeated layout analysis: {path}"
            else:
                self.reads[path] += 1
                if not path.endswith(("/@replicas", "/@tablet_state")):
                    assert self.reads[path] == 1, f"Repeated planning read: {path}"
            return super().get(path)

    client = StrictClient(
        {
            f"{table}/@type": "chaos_replicated_table",
            f"{table}/@replicas": {
                "data": {
                    "cluster_name": "zeno",
                    "replica_path": table,
                    "content_type": "data",
                    "replication_lag_timestamp": 10**18,
                },
                "queue": {"cluster_name": "zeno", "replica_path": log, "content_type": "queue", "state": "enabled"},
            },
        }
    )
    replica = StrictClient(
        {
            f"{table}/@pivot_keys": [[]],
            f"{log}/@pivot_keys": [[], [15]],
            f"{log}/@tablet_state": "mounted",
            f"{log}/@schema": HASH_SCHEMA,
        }
    )
    target = [[], [10], [20], [30]]
    apply_reshard_plans(
        client, [(table, {"pivot_keys": target})], also_chaos_replication_logs=True, make_client=lambda cluster: replica
    )
    assert replica.calls[:3] == [("unmount", table), ("reshard", table, {"pivot_keys": target}), ("mount", table)]
    assert [call[2] for call in replica.calls if call[0] == "create"] == [f"{log}{TMP_SUFFIX}", log]


@pytest.mark.parametrize("with_temporary", [False, True])
@pytest.mark.parametrize("failure", ["frozen", "disabled", "unconfirmed", "async", "changed_after_plan"])
def test_exact_log_boundaries_do_not_hide_unhealthy_canonical(caplog, with_temporary, failure):
    from yt.yt.flow.tools.reshard_flow_tables.lib import prepare_replication_log, execute_replication_log

    table, log = "//pipeline/states", "//pipeline/states_log"
    tmp = f"{log}{TMP_SUFFIX}"
    replicas = {
        "data": {
            "cluster_name": "zeno",
            "replica_path": table,
            "content_type": "data",
            "replication_lag_timestamp": 10**18,
        },
        "queue": {
            "cluster_name": "zeno",
            "replica_path": log,
            "content_type": "queue",
            "state": "disabled" if failure == "disabled" else "enabled",
            "mode": "async" if failure == "async" else "sync",
            "replica_reached_last_own_era": failure != "unconfirmed",
        },
    }
    attributes = {
        f"{log}/@schema": HASH_SCHEMA,
        f"{log}/@pivot_keys": [[]],
        f"{log}/@tablet_state": "frozen" if failure == "frozen" else "mounted",
    }
    if with_temporary:
        replicas["tmp"] = {"cluster_name": "zeno", "replica_path": tmp, "content_type": "queue"}
        attributes[f"{tmp}/@tablet_state"] = "mounted"
    client, replica = FakeClient(
        {f"{table}/@type": "chaos_replicated_table", f"{table}/@replicas": replicas}
    ), FakeClient(attributes)
    plan = prepare_replication_log(client, table, "zeno", log, [[]], make_client=lambda cluster: replica)
    if failure == "changed_after_plan":
        replica.attributes[f"{log}/@tablet_state"] = "frozen"
    with caplog.at_level(logging.INFO):
        execute_replication_log(plan, sleep=lambda seconds: None)
    canonical_mount = replica.calls.index(("mount", log))
    temporary_freeze = replica.calls.index(("freeze", tmp))
    assert canonical_mount < temporary_freeze
    assert ("create", "replication_log_table", log, {"dynamic": True, "schema": HASH_SCHEMA}) in replica.calls
    assert replica.attributes[f"{log}/@tablet_state"] == "mounted"
    assert "not ready; recovering" in caplog.text


def test_temporary_only_log_reports_per_computation_creation_and_removal(caplog):
    table, log = "//pipeline/states", "//pipeline/states_log"
    tmp = f"{log}{TMP_SUFFIX}"
    client = FakeClient(
        {
            f"{table}/@type": "chaos_replicated_table",
            f"{table}/@replicas": {
                "tmp": {"cluster_name": "zeno", "replica_path": tmp, "content_type": "queue"},
            },
        }
    )
    schema = [{"name": "computation_id", "type": "string", "sort_order": "ascending"}]
    replica = FakeClient(
        {f"{tmp}/@tablet_state": "mounted", f"{tmp}/@schema": schema, f"{tmp}/@pivot_keys": [[], ["a", [1]], ["b"]]},
        proxy="zeno",
    )
    request = ReshardRequest(table, {"pivot_keys": [[], ["a", [1]], ["b"], ["b", [1]]]}, ("a", "b"))
    with caplog.at_level(logging.INFO):
        apply_reshard_plans(
            client, [request], also_chaos_replication_logs=True, make_client=lambda cluster: replica, dry_run=True
        )
    assert "zeno://pipeline/states_log: computation_id='a': 0 => 1 tablets" in caplog.text
    assert "zeno://pipeline/states_log: computation_id='b': 0 => 1 tablets" in caplog.text
    assert "zeno://pipeline/states_log.reshard_tmp: computation_id='a': 2 => 0 tablets" in caplog.text
    assert "zeno://pipeline/states_log.reshard_tmp: computation_id='b': 1 => 0 tablets" in caplog.text
    assert client.calls == replica.calls == []


@pytest.mark.parametrize(
    "flags, applies, warns", [([], True, True), (["--commit"], True, False), (["--dry-run"], False, False)]
)
def test_cli_commit_transition(caplog, monkeypatch, flags, applies, warns):
    client = FakeClient({"//pipeline/states/@type": "table", "//pipeline/states/@tablet_count": 3})
    monkeypatch.setattr(yt, "YtClient", lambda **kwargs: client)
    monkeypatch.setattr(
        sys, "argv", ["reshard_flow_tables", "--external-table", "//pipeline/states", "--tablet-count", "5"] + flags
    )
    with caplog.at_level(logging.WARNING):
        reshard_tables(get_args())
    assert bool(client.calls) == applies
    warnings = [record.getMessage() for record in caplog.records if "Running without --commit" in record.getMessage()]
    assert bool(warnings) == warns
    if warns:
        assert "future version" in warnings[0]
        assert "--dry-run" in warnings[0]


def test_cli_commit_and_dry_run_are_mutually_exclusive(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["reshard_flow_tables", "--external-table", "//table", "--commit", "--dry-run"])
    with pytest.raises(SystemExit) as error:
        get_args()
    assert error.value.code == 2
