from copy import deepcopy

from yt_env_setup import YTEnvSetup
from yt_commands import *  # noqa

##################################################################


class TestClockServer(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_CLOCKS = 3
    NUM_NODES = 3
    NUM_SCHEDULERS = 1
    NUM_CONTROLLER_AGENTS = 1

    @authors("savrus")
    def test_generate_timestamp(self):
        config = deepcopy(self.Env.configs["clock_driver"])
        config["api_version"] = 4
        driver = Driver(config=config)
        t1 = generate_timestamp()
        t2 = generate_timestamp(driver=driver)
        assert abs((t1 >> 30) - (t2 >> 30)) < 2

    @authors("babenko")
    def test_tx(self):
        tx = start_transaction()
        commit_transaction(tx)

    @authors("gritukan")
    def test_sys_timestamp_providers(self):
        assert len(ls("//sys/timestamp_providers")) == self.NUM_CLOCKS
        for timestamp_provider in ls("//sys/timestamp_providers"):
            assert "monitoring" in get("//sys/timestamp_providers/{}/orchid".format(timestamp_provider))

    def _wait_for_hydra(self, rpc_address):
        wait(lambda: exists("//sys/timestamp_providers/{}/orchid/monitoring/hydra".format(rpc_address)))

    @authors("aleksandra-zh")
    def test_leader_switch(self):
        timestamp_providers = ls("//sys/timestamp_providers")
        timestamp_provider_rpc_addresses = get("//sys/timestamp_providers/{}/orchid/config/clock_cell/addresses".format(timestamp_providers[0]))
        cell_id = get("//sys/timestamp_providers/{}/orchid/config/clock_cell/cell_id".format(timestamp_providers[0]))

        old_leader_rpc_address = None
        new_leader_rpc_address = None

        def get_peer_state(rpc_address):
            try:
                return get("//sys/timestamp_providers/{}/orchid/monitoring/hydra/state".format(rpc_address))
            except:
                return None

        while old_leader_rpc_address is None or new_leader_rpc_address is None:
            for rpc_address in timestamp_provider_rpc_addresses:
                self._wait_for_hydra(rpc_address)
                peer_state = get_peer_state(rpc_address)
                if peer_state == "leading":
                    old_leader_rpc_address = rpc_address
                elif peer_state == "following":
                    new_leader_rpc_address = rpc_address

        switch_leader(cell_id, new_leader_rpc_address)

        wait(lambda: get_peer_state(new_leader_rpc_address) == "leading")
        wait(lambda: get_peer_state(old_leader_rpc_address) == "following")

    @authors("aleksandra-zh")
    def test_build_snapshot(self):
        timestamp_providers = ls("//sys/timestamp_providers")
        ts = timestamp_providers[0]
        cell_id = get("//sys/timestamp_providers/{}/orchid/config/clock_cell/cell_id".format(ts))

        def get_last_snapshot_id():
            self._wait_for_hydra(ts)
            return int(get("//sys/timestamp_providers/{}/orchid/monitoring/hydra/committed_version".format(ts)).split(":")[0])

        last_snapshot_id = get_last_snapshot_id()
        build_snapshot(cell_id=cell_id)

        wait(lambda: get_last_snapshot_id() == last_snapshot_id + 1)

##################################################################


class TestClockServerMulticell(TestClockServer):
    NUM_SECONDARY_MASTER_CELLS = 1
