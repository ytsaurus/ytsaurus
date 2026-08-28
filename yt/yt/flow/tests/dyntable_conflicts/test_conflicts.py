"""Verifies the transactional primitive the flow dyntable-lease design relies on: two overlapping
tablet transactions writing the same key must not both commit — the later commit fails with a lock
conflict. The guarantee must hold identically for regular dynamic tables and for chaos replicated
tables, since the lease protocol is meant to be agnostic of the table kind.
"""

import logging
import os

import pytest

import yt.wrapper
from yt.common import wait, YtError

##################################################################

# Created by the chaos recipe (yt/recipe/chaos/lib/chaos.py).
CHAOS_CELL_BUNDLE = "test-chaos"
# Created by the recipe per YT_TABLET_CELL_BUNDLE_NAME; carries the clock_cluster_tag required
# for tablet cells serving chaos replicas.
TABLET_CELL_BUNDLE = "flow-bundle"

SCHEMA = [
    {"name": "key", "type": "string", "sort_order": "ascending"},
    {"name": "subkey", "type": "string", "sort_order": "ascending"},
    {"name": "value", "type": "any"},
]

##################################################################


def make_client():
    config = yt.wrapper.default_config.get_config_from_env()
    config["backend"] = "rpc"
    return yt.wrapper.YtClient(proxy=os.environ["YT_PROXY"], config=config)


@pytest.fixture(scope="session", autouse=True)
def tablet_cell():
    # The recipe creates the bundle (with the chaos clock tag) but no tablet cells in it.
    client = make_client()
    cell_id = client.create("tablet_cell", attributes={"tablet_cell_bundle": TABLET_CELL_BUNDLE})
    wait(lambda: client.get("#{}/@health".format(cell_id)) == "good")


def wait_mounted(client, path):
    wait(lambda: client.get(path + "/@tablet_state") == "mounted")


def create_regular_table(client, path):
    client.create(
        "table",
        path,
        attributes={
            "dynamic": True,
            "schema": SCHEMA,
            "tablet_cell_bundle": TABLET_CELL_BUNDLE,
        },
    )
    client.mount_table(path)
    wait_mounted(client, path)


def create_chaos_table(client, path):
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
        wait_mounted(client, table)

    # The replication card needs a moment to converge; the table is usable once a probe write
    # goes through.
    def probe():
        try:
            client.insert_rows(path, [{"key": "probe", "subkey": "", "value": None}])
            client.delete_rows(path, [{"key": "probe", "subkey": ""}])
            return True
        except YtError as err:
            logging.info("Chaos table is not ready yet: %s", err)
            return False

    wait(probe, timeout=120, sleep_backoff=1)


##################################################################


def run_write_write_conflict_scenario(path):
    """Two overlapping tablet transactions write the same key; the one committing second must fail."""
    client1 = make_client()
    client2 = make_client()

    row = [{"key": "lease", "subkey": "", "value": {"owner": "tx"}}]

    with pytest.raises(YtError) as error:
        with client1.Transaction(type="tablet"):
            client1.insert_rows(path, row)
            # The inner transaction starts after the outer wrote the key locally, and commits
            # first; the outer transaction must then fail on commit.
            with client2.Transaction(type="tablet"):
                client2.insert_rows(path, row)

    assert error.value.contains_text("lock conflict") or error.value.contains_text("Row lock conflict"), (
        "Expected a row lock conflict, got: %s" % error.value
    )


def run_read_check_write_race_scenario(path):
    """The lease-capture pattern: both contenders read the key, see the same state and try to
    write it; exactly one commit must succeed."""
    client1 = make_client()
    client2 = make_client()

    key = [{"key": "leader", "subkey": ""}]

    def capture(client, name):
        with client.Transaction(type="tablet"):
            list(client.lookup_rows(path, key))
            client.insert_rows(path, [{"key": "leader", "subkey": "", "value": {"owner": name}}])

    # Interleave: both transactions are open and both have read before either writes-commits.
    with pytest.raises(YtError) as error:
        with client1.Transaction(type="tablet"):
            list(client1.lookup_rows(path, key))
            client1.insert_rows(path, [{"key": "leader", "subkey": "", "value": {"owner": "one"}}])
            capture(client2, "two")

    assert error.value.contains_text("lock conflict") or error.value.contains_text("Row lock conflict")

    # The committed value must be the inner (successful) contender's.
    rows = list(make_client().lookup_rows(path, key))
    assert len(rows) == 1
    assert rows[0]["value"] == {"owner": "two"}


##################################################################


@pytest.mark.authors(["thenewone"])
class TestRegularDyntableConflicts:
    def test_write_write_conflict(self):
        client = make_client()
        path = "//tmp/regular_ww"
        create_regular_table(client, path)
        run_write_write_conflict_scenario(path)

    def test_read_check_write_race(self):
        client = make_client()
        path = "//tmp/regular_rcw"
        create_regular_table(client, path)
        run_read_check_write_race_scenario(path)


@pytest.mark.authors(["thenewone"])
class TestChaosDyntableConflicts:
    def test_write_write_conflict(self):
        client = make_client()
        path = "//tmp/chaos_ww"
        create_chaos_table(client, path)
        run_write_write_conflict_scenario(path)

    def test_read_check_write_race(self):
        client = make_client()
        path = "//tmp/chaos_rcw"
        create_chaos_table(client, path)
        run_read_check_write_race_scenario(path)
