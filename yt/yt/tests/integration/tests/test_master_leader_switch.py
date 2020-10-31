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
        def _get_master_state(rpc_address):
            try:
                return get("//sys/primary_masters/{}/orchid/monitoring/hydra/state".format(rpc_address))
            except:
                return None

        current_leader_id = None
        while current_leader_id is None:
            for id, rpc_address in enumerate(self.Env.configs["master"][0]["primary_master"]["addresses"]):
                if _get_master_state(rpc_address) == "leading":
                    current_leader_id = id
                    break

        new_leader_id = (current_leader_id + 1) % 5
        new_leader_rpc_address = self.Env.configs["master"][0]["primary_master"]["addresses"][new_leader_id]

        cell_id = get("//sys/@cell_id")
        switch_leader(cell_id, new_leader_id)

        def _check():
            try:
                return _get_master_state(new_leader_rpc_address) == "leading"
            except:
                return False

        wait(_check)
