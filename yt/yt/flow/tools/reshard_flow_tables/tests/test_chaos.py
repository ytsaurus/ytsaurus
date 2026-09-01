import pytest

from yt.yt.flow.tools.reshard_flow_tables.lib import (
    TMP_SUFFIX,
    apply_reshard_plans,
    current_tablet_count,
    reshard_timer_table,
    get_reshard_targets,
    recreate_replication_log,
    reshard_mounted_table,
)


class FakeClient:
    def __init__(self, attributes=None):
        self.attributes = attributes or {}
        self.calls = []
        self.created_replicas = 0

    # Attributes every dynamic table carries. Spelling them out at each call site would only add
    # noise to tests that are not about the tablet layout.
    DEFAULT_ATTRIBUTES = {"@sorted": True, "@pivot_keys": [[]], "@tablet_count": 1}

    def get(self, path):
        if path not in self.attributes:
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
        ("reshard", "//pipeline/states", {"tablet_count": 5, "uniform": True}),
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
        if self.failing_table in (None, table) and kwargs.get("tablet_count") == 5:
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
        ("reshard", "//pipeline/states", {"tablet_count": 5, "uniform": True}),
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
            if kwargs.get("tablet_count") == 5:
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
        ("reshard", crt, {"tablet_count": 5, "uniform": True}),
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
        ("reshard", "//pipeline/states", {"tablet_count": 5, "uniform": True}),
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
            raise RuntimeError("over tablet count limit" if kwargs.get("tablet_count") == 5 else "rollback is broken")

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
        ("//pipeline/grows_a_lot", {"tablet_count": 50, "uniform": True}),
        ("//pipeline/grows_a_bit", {"tablet_count": 20, "uniform": True}),
        ("//pipeline/shrinks", {"tablet_count": 10, "uniform": True}),
    ]

    apply_reshard_plans(client, plans)

    assert [table for kind, table, *_ in client.calls if kind == "reshard"] == [
        "//pipeline/shrinks",
        "//pipeline/grows_a_bit",
        "//pipeline/grows_a_lot",
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
            f"{log}/@tablet_state": "frozen",
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
                    f"{log}/@tablet_state": "frozen",
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
            f"{log}/@tablet_state": "frozen",
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
            f"{log}/@tablet_state": "frozen",
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
        ("reshard", table, {"tablet_count": 7, "uniform": True}),
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
                    f"{log}/@tablet_state": "frozen",
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
