from yt_env_setup import YTEnvSetup

from yt_commands import authors, wait, get, switch_leader

from yt.common import YtError

import pytest

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

    def _is_active_leader(self, rpc_address):
        try:
            return get("//sys/primary_masters/{}/orchid/monitoring/hydra/active_leader".format(rpc_address))
        except YtError:
            return False

    def _is_active_follower(self, rpc_address):
        try:
            return get("//sys/primary_masters/{}/orchid/monitoring/hydra/active_follower".format(rpc_address))
        except YtError:
            return False

    def _get_active_leader_address(self):
        while True:
            for rpc_address in self.Env.configs["master"][0]["primary_master"]["addresses"]:
                if self._is_active_leader(rpc_address):
                    return rpc_address

    def _get_active_follower_address(self):
        while True:
            for rpc_address in self.Env.configs["master"][0]["primary_master"]["addresses"]:
                if self._is_active_follower(rpc_address):
                    return rpc_address

    @authors("babenko")
    def test_invalid_params(self):
        cell_id = get("//sys/@cell_id")
        with pytest.raises(YtError):
            switch_leader("1-2-3-4", self._get_active_follower_address())
        with pytest.raises(YtError):
            switch_leader(cell_id, "foo.bar:9012")
        with pytest.raises(YtError):
            switch_leader(cell_id, self._get_active_leader_address())

    @authors("babenko")
    def test_switch(self):
        def _get_master_grace_delay_status(rpc_address):
            return get("//sys/primary_masters/{}/orchid/monitoring/hydra/grace_delay_status".format(rpc_address))

        old_leader_rpc_address = self._get_active_leader_address()
        new_leader_rpc_address = self._get_active_follower_address()

        assert _get_master_grace_delay_status(old_leader_rpc_address) == "grace_delay_executed"

        cell_id = get("//sys/@cell_id")
        switch_leader(cell_id, new_leader_rpc_address)

        wait(lambda: self._is_active_leader(new_leader_rpc_address))
        wait(lambda: self._is_active_follower(old_leader_rpc_address))

        assert _get_master_grace_delay_status(new_leader_rpc_address) == "previous_lease_abandoned"
