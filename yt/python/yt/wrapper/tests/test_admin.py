from .conftest import authors
from .helpers import get_operation_path, wait, yatest_common

from yt.wrapper.driver import get_api_version
from yt.wrapper.native_driver import get_driver_instance
from yt.wrapper.spec_builders import VanillaSpecBuilder

import yt.wrapper as yt

import os
import pytest
from concurrent.futures import ThreadPoolExecutor


@pytest.mark.usefixtures("yt_env_with_rpc_v3")
class TestAdminCommands(object):
    @authors("gritukan")
    def test_write_operation_controller_core_dump(self):
        # NB(gritukan): This test should also work with v3, but it's quite heavy, so
        # we don't want to run it in too many parametrizations.
        if yt.config["api_version"] != "v4":
            pytest.skip()

        if yatest_common.context.sanitize == "address":
            pytest.skip("core dumps are not supported under asan")

        op = yt.run_operation(VanillaSpecBuilder().task("sample", {"command": "sleep 1000", "job_count": 1}), sync=False)

        def get_controller_agent_address():
            return yt.get_attribute(get_operation_path(op.id), "controller_agent_address", default=None)
        wait(lambda: get_controller_agent_address() is not None)

        core_path = get_driver_instance(None).write_operation_controller_core_dump(operation_id=op.id)

        controller_agent_address = get_controller_agent_address()

        def get_core_dumper_count():
            return yt.get("//sys/controller_agents/instances/{}/orchid/core_dumper/active_count".format(controller_agent_address))

        wait(lambda: get_core_dumper_count() == 1)
        wait(lambda: get_core_dumper_count() == 0, iter=200, sleep_backoff=5)

        assert os.path.exists(core_path)
        assert os.path.getsize(core_path) >= 5 * 10**6
        os.remove(core_path)

    @authors("kvk1920")
    def test_maintenance_requests(self):
        if get_api_version() != "v4":
            pytest.skip("Maintenance requests are supported for API v4 only")

        node = yt.list("//sys/cluster_nodes")[0]
        path = "//sys/cluster_nodes/" + node
        maintenance_id = yt.add_maintenance("cluster_node", node, "decommission", "1234")[node]
        assert list(yt.get(path + "/@maintenance_requests").keys()) == [maintenance_id]
        assert yt.get(path + "/@decommissioned")
        assert yt.remove_maintenance("cluster_node", node, id=maintenance_id) == {
            node: {"decommission": 1},
        }
        assert not yt.get(path + "/@decommissioned")


@pytest.mark.usefixtures("yt_env_hydra")
class TestHydraCommands(object):
    @authors("danilalexeev")
    def test_hydra_freeze(self):
        addrs = yt.list("//sys/primary_masters")
        cell_id = yt.get("//sys/@cell_id")

        def get_hydra_monitoring(addr):
            return yt.get(
                f"//sys/primary_masters/{addr}/orchid/monitoring/hydra",
                suppress_upstream_sync=True,
                suppress_transaction_coordinator_sync=True)

        follower_addrs = [addr for addr in addrs if get_hydra_monitoring(addr)["active_follower"]]
        leader_addr = [addr for addr in addrs if addr not in follower_addrs][0]
        assert len(follower_addrs) + 1 == len(addrs)

        term = get_hydra_monitoring(addrs[0])["term"]

        def freeze_hydra_peer(addr):
            get_driver_instance(None).freeze_hydra_peer(addr, cell_id=cell_id, term=term)

        with ThreadPoolExecutor() as executor:
            list(executor.map(freeze_hydra_peer, follower_addrs))

        for addr in follower_addrs:
            wait(lambda: get_hydra_monitoring(addr).get("frozen", False), f"{addr} is not frozen")

        for addr in follower_addrs:
            wait(
                lambda a=addr: (
                    lambda m=get_hydra_monitoring(a): m["logged_sequence_number"] > m["committed_sequence_number"]
                )(),
                f"{addr} has no uncommitted records to truncate",
            )

        get_driver_instance(None).schedule_restart(leader_addr, cell_id=cell_id)

        seq_nums = [get_hydra_monitoring(addr)["committed_sequence_number"] for addr in follower_addrs]

        def truncate_changelog(addr, seq_num):
            def try_truncate():
                try:
                    get_driver_instance(None).truncate_changelog(addr, cell_id=cell_id, last_sequence_number=seq_num)
                    return True
                except yt.errors.YtError:
                    # Cannot truncate changelog while logging mutations.
                    return False
            wait(try_truncate, f"failed to truncate changelog on {addr}")

        with ThreadPoolExecutor() as executor:
            list(executor.map(truncate_changelog, follower_addrs, seq_nums))

        def check_available():
            try:
                yt.set("//sys/@test", 42)
                return True
            except yt.errors.YtError:
                return False

        wait(check_available)

        assert yt.get("//sys/@test") == 42
