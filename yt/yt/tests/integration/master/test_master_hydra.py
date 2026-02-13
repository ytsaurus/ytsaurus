from yt_env_setup import (
    YTEnvSetup, Restarter,  MASTERS_SERVICE, with_additional_threads)

from yt_commands import (
    authors, raises_yt_error, wait, get, set, ls, switch_leader, build_snapshot,
    is_active_primary_master_leader, is_active_primary_master_follower, create,
    get_active_primary_master_leader_address, get_master_consistent_state,
    reset_state_hash, build_master_snapshots, discombobulate_nonvoting_peers,
    get_active_primary_master_follower_address, start_transaction, remove)

from yt_helpers import master_exit_read_only_sync

import os
import pytest
import time

##################################################################


@pytest.mark.enabled_multidaemon
class TestMasterLeaderSwitch(YTEnvSetup):
    ENABLE_MULTIDAEMON = True
    NUM_MASTERS = 7
    NUM_NODES = 0
    DELTA_MASTER_CONFIG = {
        "hydra_manager": {
            "leader_lease_grace_delay": 6000,
            "leader_lease_timeout": 5000,
            "disable_leader_lease_grace_delay": False,
        },
    }

    @authors("babenko")
    def test_invalid_params(self):
        cell_id = get("//sys/@cell_id")
        with raises_yt_error("is not a valid cell id"):
            switch_leader("1-2-3-4", get_active_primary_master_follower_address(self))
        with raises_yt_error("resolve failed for"):
            switch_leader(cell_id, "foo.bar:9012")
        with raises_yt_error("Invalid peer state"):
            switch_leader(cell_id, get_active_primary_master_leader_address(self))

    @authors("babenko")
    def test_switch(self):
        def _try_get_master_grace_delay_status(rpc_address):
            return get("//sys/primary_masters/{}/orchid/monitoring/hydra/grace_delay_status".format(rpc_address), None)

        def _get_master_grace_delay_status(rpc_address):
            wait(lambda: _try_get_master_grace_delay_status(rpc_address) is not None)
            return _try_get_master_grace_delay_status(rpc_address)

        old_leader_rpc_address = get_active_primary_master_leader_address(self)
        new_leader_rpc_address = get_active_primary_master_follower_address(self)

        assert _get_master_grace_delay_status(old_leader_rpc_address) == "grace_delay_executed"

        cell_id = get("//sys/@cell_id")
        switch_leader(cell_id, new_leader_rpc_address)

        wait(lambda: is_active_primary_master_leader(new_leader_rpc_address))
        wait(lambda: is_active_primary_master_follower(old_leader_rpc_address))

        assert _get_master_grace_delay_status(new_leader_rpc_address) == "previous_lease_abandoned"


##################################################################

@pytest.mark.enabled_multidaemon
class TestMasterResetStateHash(YTEnvSetup):
    ENABLE_MULTIDAEMON = True
    NUM_MASTERS = 3
    NUM_NODES = 0

    @authors("gritukan")
    def test_reset_state_hash(self):
        cell_id = get("//sys/@cell_id")

        def _test(new_state_hash):
            reset_state_hash(cell_id, new_state_hash)
            # Do something and do not crash.
            for i in range(10):
                set("//tmp/@foo", i)

        _test(new_state_hash=None)
        _test(new_state_hash=0xbebebebe)


##################################################################

class TestDiscombobulate(YTEnvSetup):
    ENABLE_MULTIDAEMON = False  # There are component restarts.
    NUM_MASTERS = 5
    NUM_NONVOTING_MASTERS = 2
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True

    DELTA_MASTER_CONFIG = {
        "election_manager": {
            "follower_ping_period": 200,
            "leader_ping_timeout": 500,
        }
    }

    DELTA_CYPRESS_PROXY_CONFIG = {
        "heartbeat_period": 1000,
    }

    @authors("danilalexeev")
    def test_discombobulate_nonvoting_peers(self):
        set("//tmp/hello", "world")

        primary_master_config = self.Env.configs["master"][0]["primary_master"]

        build_snapshot(cell_id=primary_master_config["cell_id"], set_read_only=True)

        discombobulate_nonvoting_peers(primary_master_config["cell_id"])

        def wait_active(master_ids):
            for idx in master_ids:
                address = primary_master_config["addresses"][idx]
                wait(lambda: get(
                    "{}/{}/orchid/monitoring/hydra/active".format("//sys/primary_masters", address),
                    default=False), ignore_exceptions=True)

        def restart_check_masters(master_ids, voting):
            self.Env.kill_service("master", indexes=master_ids)
            time.sleep(3)

            assert get("//tmp/hello") == "world"

            if voting:
                with raises_yt_error("Read-only mode is active"):
                    set("//tmp/hello", "hello")

            self.Env.start_master_cell(set_config=False)
            wait_active(master_ids)

            assert get("//tmp/hello") == "world"

        voting_ids = [0, 1, 2]
        nonvoting_ids = [3, 4]

        for address in primary_master_config["addresses"][3:]:
            wait(lambda: get(
                "{}/{}/orchid/monitoring/hydra/discombobulated".format("//sys/primary_masters", address),
                default=False))

        restart_check_masters(voting_ids, True)

        restart_check_masters(nonvoting_ids, False)

        for address in primary_master_config["addresses"][3:]:
            assert not get(
                "{}/{}/orchid/monitoring/hydra/discombobulated".format("//sys/primary_masters", address),
                default=True)


##################################################################

@pytest.mark.enabled_multidaemon
class TestLamportClock(YTEnvSetup):
    ENABLE_MULTIDAEMON = True
    NUM_SECONDARY_MASTER_CELLS = 3

    @authors("danilalexeev")
    def test_get_consistent_state(self):
        def convert_to_map(yson_map):
            result = {}
            for m in yson_map:
                result[m["cell_id"]] = int(m["sequence_number"])
            return result

        state = convert_to_map(get_master_consistent_state())
        assert len(state) == len(ls("//sys/secondary_masters")) + 1

        new_state = convert_to_map(get_master_consistent_state())
        assert len(state) == len(new_state)
        for cell_id in new_state:
            assert new_state[cell_id] >= state[cell_id]


##################################################################

@pytest.mark.enabled_multidaemon
class TestHydraLogicalTime(YTEnvSetup):
    ENABLE_MULTIDAEMON = False  # There are component restarts.

    # Hydra logical time is measured in microseconds, and 1 second is 1kk microseconds.
    SECOND = 1000000

    @authors("h0pless")
    def test_hydra_logical_time(self):
        initial_time = get("//sys/@hydra_logical_time")
        time.sleep(1.0)
        build_master_snapshots(set_read_only=True)

        read_only_time = get("//sys/@hydra_logical_time")
        assert read_only_time - initial_time > 0.5 * self.SECOND

        time.sleep(5.0)
        assert read_only_time == get("//sys/@hydra_logical_time")

        master_exit_read_only_sync()
        time.sleep(5.0)

        time_passed_since_read_only = get("//sys/@hydra_logical_time") - read_only_time
        assert 4 * self.SECOND < time_passed_since_read_only
        assert time_passed_since_read_only < 9 * self.SECOND

    @authors("h0pless")
    def test_hydra_logical_time_restart(self):
        time.sleep(0.5)
        build_master_snapshots(set_read_only=False)
        time.sleep(0.5)

        with Restarter(self.Env, MASTERS_SERVICE):
            pass

        time.sleep(2.0)  # Just don't crash...


##################################################################

@pytest.mark.enabled_multidaemon
class TestLocalJanitor(YTEnvSetup):
    ENABLE_MULTIDAEMON = True
    DELTA_MASTER_CONFIG = {
        "hydra_manager": {
            "enable_local_janitor": False,
            "max_snapshot_count_to_keep": 2,
            "cleanup_period": 100,
        }
    }

    @authors("danilalexeev")
    def test_janitor_dynamic_config(self):
        def build_n_snapshots(n):
            for _ in range(n):
                build_master_snapshots()

        snapshot_dir = os.path.join(self.path_to_run, "runtime_data", "master", "0", "snapshots")
        assert os.path.exists(snapshot_dir)

        assert len(os.listdir(snapshot_dir)) == 0

        build_n_snapshots(3)
        assert len(os.listdir(snapshot_dir)) == 3

        set('//sys/@config/hydra_manager/enable_local_janitor', True)
        wait(lambda: len(os.listdir(snapshot_dir)) == 2)

        set('//sys/@config/hydra_manager/max_snapshot_count_to_keep', 0)
        wait(lambda: len(os.listdir(snapshot_dir)) == 1)

        set('//sys/@config/hydra_manager/cleanup_period', 100500)
        build_n_snapshots(3)
        time.sleep(1)
        assert len(os.listdir(snapshot_dir)) == 4


##################################################################

class TestChangelogRecovery(YTEnvSetup):
    ENABLE_MULTIDAEMON = False  # There are component restarts.
    NUM_MASTERS = 5
    DELTA_MASTER_CONFIG = {
        "hydra_manager": {
            "max_sequence_number_gap_for_changelog_only_recovery": 100000,
        },
    }

    @authors("grphil")
    @pytest.mark.parametrize("read_only", [True, False])
    def test_changelog_recovery(self, read_only):
        self.Env.kill_service("master", indexes=[1])

        set("//tmp/a", "b")

        primary_master_config = self.Env.configs["master"][0]["primary_master"]

        build_snapshot(cell_id=primary_master_config["cell_id"], set_read_only=read_only)

        time.sleep(3)

        self.Env.start_master_cell(set_config=False)
        address = primary_master_config["addresses"][1]

        def get_monitoring_param(param, default=None):
            return get("{}/{}/orchid/monitoring/hydra/{}".format("//sys/primary_masters", address, param), default=default)

        wait(lambda: get_monitoring_param("active", default=False), ignore_exceptions=True)

        assert get_monitoring_param("last_snapshot_id_used_for_recovery", 1) == -1
        assert get_monitoring_param("read_only", 2) == read_only


##################################################################


class TestHydraLogicalVersion(YTEnvSetup):
    ENABLE_MULTIDAEMON = False  # There are component restarts.
    NUM_MASTERS = 3
    NUM_SECONDARY_MASTER_CELLS = 2
    MASTER_CELL_DESCRIPTORS = {
        "10": {"roles": ["cypress_node_host"]},
        "11": {"roles": ["chunk_host"]},
        "12": {"roles": ["transaction_coordinator"]},
    }

    @authors("h0pless")
    @with_additional_threads
    def test_recovery(self):
        time.sleep(5)  # Let some more mutations accumulate.

        src_cell_tag = 12
        dst_cell_tag = 10

        set("//sys/@config/multicell_manager/testing/frozen_hive_edges", [[src_cell_tag, dst_cell_tag]])

        tx1 = start_transaction()
        tx2 = start_transaction()

        def wait_for_counter_to_change_1():
            wait(lambda: create("table", "//tmp/table_1", tx=tx1), ignore_exceptions=True)

        def wait_for_counter_to_change_2():
            wait(lambda: create("table", "//tmp/table_2", tx=tx2), ignore_exceptions=True)

        first_request = self.spawn_additional_thread(name="create first table",
                                                     target=wait_for_counter_to_change_1)

        second_request = self.spawn_additional_thread(name="create second table",
                                                      target=wait_for_counter_to_change_2)

        time.sleep(1)  # Let some more mutations accumulate.

        remove("//sys/@config/multicell_manager/testing/frozen_hive_edges",
               suppress_transaction_coordinator_sync=True)

        first_request.join()
        second_request.join()

        table_1_revision = get("//tmp/table_1/@revision", tx=tx1)
        table_2_revision = get("//tmp/table_2/@revision", tx=tx2)
        assert table_1_revision != table_2_revision

        with Restarter(self.Env, MASTERS_SERVICE):
            pass

        assert table_1_revision == get("//tmp/table_1/@revision", tx=tx1)
        assert table_2_revision == get("//tmp/table_2/@revision", tx=tx2)
