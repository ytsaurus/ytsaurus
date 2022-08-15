from yt_env_setup import YTEnvSetup

from yt_commands import (
    authors, wait, ls, get,
    build_snapshot, switch_leader,
    generate_timestamp, start_transaction, commit_transaction)

from yt_driver_bindings import Driver

from copy import deepcopy

##################################################################


class TestClockServer(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_CLOCKS = 3
    NUM_NODES = 3
    NUM_SCHEDULERS = 0
    NUM_CONTROLLER_AGENTS = 0

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

    @authors("aleksandra-zh")
    def test_leader_switch(self):
        timestamp_providers = ls("//sys/timestamp_providers")
        timestamp_provider_rpc_addresses = get("//sys/timestamp_providers/{}/orchid/config/clock_cell/addresses"
                                               .format(timestamp_providers[0]))
        cell_id = get("//sys/timestamp_providers/{}/orchid/config/clock_cell/cell_id".format(timestamp_providers[0]))

        old_leader_rpc_address = None
        new_leader_rpc_address = None

        def try_get_peer_state(rpc_address):
            return get("//sys/timestamp_providers/{}/orchid/monitoring/hydra/state".format(rpc_address), None)

        while old_leader_rpc_address is None or new_leader_rpc_address is None:
            for rpc_address in timestamp_provider_rpc_addresses:
                peer_state = try_get_peer_state(rpc_address)
                if peer_state == "leading":
                    old_leader_rpc_address = rpc_address
                elif peer_state == "following":
                    new_leader_rpc_address = rpc_address

        switch_leader(cell_id, new_leader_rpc_address)

        wait(lambda: try_get_peer_state(new_leader_rpc_address) == "leading")
        wait(lambda: try_get_peer_state(old_leader_rpc_address) == "following")

    @authors("aleksandra-zh")
    def test_build_snapshot(self):
        timestamp_providers = ls("//sys/timestamp_providers")
        rpc_address = timestamp_providers[0]
        cell_id = get("//sys/timestamp_providers/{}/orchid/config/clock_cell/cell_id".format(rpc_address))

        def try_get_last_snapshot_id():
            return get("//sys/timestamp_providers/{}/orchid/monitoring/hydra/last_snapshot_id".format(rpc_address), default=None)

        def get_last_snapshot_id():
            wait(lambda: try_get_last_snapshot_id() is not None)
            return try_get_last_snapshot_id()

        last_snapshot_id = get_last_snapshot_id()

        build_snapshot(cell_id=cell_id)

        wait(lambda: get_last_snapshot_id() > last_snapshot_id)

##################################################################


class TestClockServerMulticell(TestClockServer):
    NUM_SECONDARY_MASTER_CELLS = 1
