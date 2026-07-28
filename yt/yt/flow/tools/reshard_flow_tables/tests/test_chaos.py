from yt.yt.flow.tools.reshard_flow_tables.lib import (
    get_reshard_targets,
    recreate_replication_log,
    reshard_mounted_table,
)


class FakeClient:
    def __init__(self, attributes=None):
        self.attributes = attributes or {}
        self.calls = []

    def get(self, path):
        return self.attributes[path]

    def exists(self, path):
        return path in self.attributes

    def unmount_table(self, table, sync):
        self.calls.append(("unmount", table))

    def reshard_table(self, table, sync, **kwargs):
        self.calls.append(("reshard", table, kwargs))

    def mount_table(self, table, sync):
        self.calls.append(("mount", table))

    def freeze_table(self, table, sync=False):
        self.calls.append(("freeze", table))

    def remove(self, path):
        self.calls.append(("remove", path))
        if path.startswith("#"):
            replica_id = path[1:]
            for key, value in self.attributes.items():
                if key.endswith("/@replicas") and replica_id in value:
                    del value[replica_id]

    def create(self, object_type, path=None, attributes=None):
        self.calls.append(("create", object_type, path, attributes))
        if object_type == "chaos_table_replica":
            replicas = self.attributes.get(f"{attributes['table_path']}/@replicas")
            if replicas is not None:
                replica_id = f"created-{len(replicas)}"
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
            f"{tmp}/@tablet_state": "frozen",
        }
    )

    recreate_replication_log(
        client,
        crt,
        "queue-id",
        client.get(f"{crt}/@replicas")["queue-id"],
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
        ("alter_table", tmp, "created-2"),
        ("mount", tmp),
        ("freeze", log),
        ("unmount", log),
        ("remove", log),
        ("create", "replication_log_table", log, expected_attributes),
        ("reshard", log, {"pivot_keys": [[], [123]]}),
        ("alter_table", log, "created-2"),
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
        ("alter_table_replica", "created-2", False),
        ("remove", "#created-2"),
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
                    f"{log}.reshard_tmp/@tablet_state": "frozen",
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
            tmp: True,
            f"{log}/@tablet_state": "frozen",
            f"{tmp}/@tablet_state": "frozen",
        }
    )

    recreate_replication_log(
        client,
        crt,
        "queue-id",
        client.get(f"{crt}/@replicas")["queue-id"],
        log_pivot_keys=[[]],
        make_client=lambda proxy: log_client,
        sleep=lambda seconds: None,
    )

    assert log_client.calls[0] == ("remove", tmp)
    assert log_client.calls[1][0] == "create"


def test_birth_race_retries_the_newborn_log():
    crt = "//pipeline/states"
    log = "//pipeline/states_log"
    tmp = f"{log}.reshard_tmp"

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
            f"{tmp}/@tablet_state": "frozen",
        }
    )

    recreate_replication_log(
        client,
        crt,
        "queue-id",
        client.get(f"{crt}/@replicas")["queue-id"],
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
