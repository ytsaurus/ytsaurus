from yt_env_setup import YTEnvSetup

from yt_commands import authors, wait, get, set, ls, create, write_table, start_transaction, build_snapshot

from yt_helpers import get_current_time, parse_yt_time

import pytest

import time

import os

##################################################################


@pytest.mark.enabled_multidaemon
class TestRff(YTEnvSetup):
    ENABLE_MULTIDAEMON = True
    NUM_MASTERS = 5
    NUM_NONVOTING_MASTERS = 2
    NUM_NODES = 3

    DELTA_MASTER_CONFIG = {
        "hydra_manager": {
            # COMPAT(babenko): drop once Hydra1 is no more
            "max_commit_batch_delay": 1000,
            "mutation_flush_period": 500
        }
    }

    NUM_ITERATIONS = 20

    @authors("babenko")
    def test_plain_read_table(self):
        set("//tmp/x", 123)
        for i in range(self.NUM_ITERATIONS):
            assert get("//tmp/x", read_from="follower") == 123

    @authors("babenko")
    def test_sync(self):
        for i in range(self.NUM_ITERATIONS):
            set("//tmp/x", i)
            assert get("//tmp/x", read_from="follower") == i

    @authors("babenko")
    def test_access_stat(self):
        time.sleep(1.0)
        c0 = get("//tmp/@access_counter")
        for i in range(self.NUM_ITERATIONS):
            assert ls("//tmp", read_from="follower") == []
        wait(lambda: get("//tmp/@access_counter") == c0 + self.NUM_ITERATIONS)

    @authors("babenko")
    def test_leader_fallback(self):
        create("table", "//tmp/t")
        write_table("//tmp/t", {"a": "b"})

        assert ls("//sys/lost_vital_chunks", read_from="follower") == []

        assert all(
            not c.attributes["replication_status"]["default"]["overreplicated"]
            for c in ls("//sys/chunks", attributes=["replication_status"], read_from="follower")
        )

        assert all(
            n.attributes["state"] == "online"
            for n in ls("//sys/cluster_nodes", attributes=["state"], read_from="follower")
        )

        assert get("//sys/@chunk_replicator_enabled", read_from="follower")

        tx = start_transaction()
        last_ping_time = parse_yt_time(get("#" + tx + "/@last_ping_time", read_from="follower"))
        now = get_current_time()
        assert last_ping_time < now
        assert (now - last_ping_time).total_seconds() < 3

    @authors("aleksandra-zh")
    def test_snapshots(self):
        cell_id = get("//sys/@cell_id")
        snapshot_id = build_snapshot(cell_id=cell_id, set_read_only=False)

        for peer_index in range(self.NUM_MASTERS):
            result_dir = os.path.join(self.path_to_run, "runtime_data", "master", str(peer_index))
            assert os.path.exists(result_dir)

            snapshot_dir = os.path.join(result_dir, "snapshots")

            snapshot_ids = [int(name.split(".")[0]) for name in os.listdir(snapshot_dir) if name.endswith("snapshot")]

            assert snapshot_id in snapshot_ids


##################################################################


@pytest.mark.enabled_multidaemon
class TestRffMulticell(TestRff):
    ENABLE_MULTIDAEMON = True
    NUM_SECONDARY_MASTER_CELLS = 2
