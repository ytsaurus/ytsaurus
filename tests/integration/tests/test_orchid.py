import pytest

from yt_env_setup import YTEnvSetup
from yt_commands import *

##################################################################

class TestOrchid(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 5
    NUM_SCHEDULERS = 1

    def _check_service(self, path_to_orchid, service_name):
        path_to_value = path_to_orchid + "/value"

        assert get(path_to_orchid + "/service/name") == service_name

        some_map = {"a": 1, "b": 2}

        set(path_to_value, some_map)
        assert get(path_to_value) == some_map

        self.assertItemsEqual(ls(path_to_value), ["a", "b"])
        remove(path_to_value)
        with pytest.raises(YtError): get(path_to_value)


    def _check_orchid(self, path, num_services, service_name):
        services = ls(path)
        assert len(services) == num_services
        for service in services:
            path_to_orchid = path + "/" + service + "/orchid"
            self._check_service(path_to_orchid, service_name)

    def test_at_masters(self):
        for i in range(10):
            self._check_orchid("//sys/masters", self.NUM_MASTERS, "master")

    def test_at_nodes(self):
        for i in range(10):
            self._check_orchid("//sys/nodes", self.NUM_NODES, "node")

    def test_at_scheduler(self):
        for i in range(10):
            self._check_service("//sys/scheduler/orchid", "scheduler")
