from .conftest import authors
from .helpers import get_operation_path, wait, yatest_common

from yt.wrapper.driver import get_api_version
from yt.wrapper.native_driver import get_driver_instance
from yt.wrapper.spec_builders import VanillaSpecBuilder

import yt.wrapper as yt

import pytest
import os


@pytest.mark.usefixtures("yt_env_with_rpc")
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
