from yt_env_setup import YTEnvSetup
from yt_commands import *

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

    @authors("babenko")
    def test_invalid_params(self):
        cell_id = get("//sys/@cell_id")
        with pytest.raises(YtError):
            switch_leader("1-2-3-4", 0)
        with pytest.raises(YtError):
            switch_leader(cell_id, -1)
        with pytest.raises(YtError):
            switch_leader(cell_id, 7)

    @authors("babenko")
    def test_switch(self):
        def _is_active_leader(rpc_address):
            try:
                return get("//sys/primary_masters/{}/orchid/monitoring/hydra/active_leader".format(rpc_address))
            except:
                return False

        def _get_master_grace_delay_status(rpc_address):
            return get("//sys/primary_masters/{}/orchid/monitoring/hydra/grace_delay_status".format(rpc_address))

        current_leader_id = None
        current_leader_rpc_address = None
        while current_leader_id is None:
            for id, rpc_address in enumerate(self.Env.configs["master"][0]["primary_master"]["addresses"]):
                if _is_active_leader(rpc_address):
                    current_leader_id = id
                    current_leader_rpc_address = rpc_address
                    break

        assert _get_master_grace_delay_status(current_leader_rpc_address) == "grace_delay_executed"

        new_leader_id = (current_leader_id + 1) % 5
        new_leader_rpc_address = self.Env.configs["master"][0]["primary_master"]["addresses"][new_leader_id]

        cell_id = get("//sys/@cell_id")
        switch_leader(cell_id, new_leader_id)

        wait(lambda: _is_active_leader(new_leader_rpc_address))

        assert _get_master_grace_delay_status(new_leader_rpc_address) == "previous_lease_abandoned"
