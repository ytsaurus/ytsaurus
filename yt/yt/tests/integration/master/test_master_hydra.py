from yt_env_setup import YTEnvSetup

from yt_commands import (
    authors, wait, get, set, ls, switch_leader, is_active_primary_master_leader, is_active_primary_master_follower,
    get_active_primary_master_leader_address, get_active_primary_master_follower_address, build_snapshot,
    reset_state_hash, discombobulate_nonvoting_peers, get_master_consistent_state)

from yt.common import YtError

import pytest
import time

##################################################################


class TestMasterLeaderSwitch(YTEnvSetup):
    NUM_MASTERS = 7
    NUM_NODES = 0
    DELTA_MASTER_CONFIG = {
        "hydra_manager": {
            "leader_lease_grace_delay": 6000,
            "leader_lease_timeout": 5000,
            "disable_leader_lease_grace_delay": False,
        }
    }

    @authors("babenko")
    def test_invalid_params(self):
        cell_id = get("//sys/@cell_id")
        with pytest.raises(YtError):
            switch_leader("1-2-3-4", get_active_primary_master_follower_address(self))
        with pytest.raises(YtError):
            switch_leader(cell_id, "foo.bar:9012")
        with pytest.raises(YtError):
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


class TestMasterResetStateHash(YTEnvSetup):
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


class TestDiscombobulate(YTEnvSetup):
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
                with pytest.raises(YtError, match="Read-only mode is active"):
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


class TestLamportClock(YTEnvSetup):
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
