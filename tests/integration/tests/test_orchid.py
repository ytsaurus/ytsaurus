import pytest

from yt_env_setup import YTEnvSetup
from yt_commands import *
from yt.environment.helpers import assert_items_equal

##################################################################

class TestOrchid(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 5
    NUM_SCHEDULERS = 1

    def _check_service(self, path_to_orchid, service_name):
        assert get(path_to_orchid + "/service/name") == service_name
        keys = list(get(path_to_orchid + "/"))
        for key in keys:
            get(path_to_orchid + "/" + key, verbose=False)
        with pytest.raises(YtError): set(path_to_orchid + "/key", "value")

    def _check_orchid(self, path, num_services, service_name):
        services = ls(path)
        assert len(services) == num_services
        for service in services:
            path_to_orchid = path + "/" + service + "/orchid"
            self._check_service(path_to_orchid, service_name)

    def test_at_primary_masters(self):
        self._check_orchid("//sys/primary_masters", self.NUM_MASTERS, "master")

    def test_at_nodes(self):
        self._check_orchid("//sys/nodes", self.NUM_NODES, "node")

    def test_at_scheduler(self):
        self._check_service("//sys/scheduler/orchid", "scheduler")

    def test_at_tablet_cells(self):
        sync_create_cells(1)
        cells = ls("//sys/tablet_cells")
        assert len(cells) == 1
        for cell in cells:
            peers = get("//sys/tablet_cells/" + cell + "/@peers")
            for peer in peers:
                address = peer["address"]
                peer_cells = ls("//sys/nodes/" + address + "/orchid/tablet_cells")
                assert cell in peer_cells

##################################################################

class TestOrchidMulticell(TestOrchid):
    NUM_SECONDARY_MASTER_CELLS = 2

    def test_at_secondary_masters(self):
        for tag in range(1, self.NUM_SECONDARY_MASTER_CELLS + 1):
            self._check_orchid("//sys/secondary_masters/" + str(tag) , self.NUM_MASTERS, "master")
