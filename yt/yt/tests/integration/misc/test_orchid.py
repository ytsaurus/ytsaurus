from yt_env_setup import YTEnvSetup

from yt_commands import authors, get, ls, set, sync_create_cells, create, raises_yt_error, exists

from yt.common import YtError
import yt.yson as yson

import pytest

##################################################################


class TestOrchid(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 5
    NUM_SCHEDULERS = 1

    def _check_service(self, path_to_orchid, service_name):
        assert get(path_to_orchid + "/service/name") == service_name
        keys = list(get(path_to_orchid))
        for key in keys:
            get(path_to_orchid + "/" + key, verbose=False)
        with pytest.raises(YtError):
            set(path_to_orchid + "/key", "value")

    def _check_orchid(self, path, num_services, service_name):
        services = ls(path)
        assert len(services) == num_services
        for service in services:
            path_to_orchid = path + "/" + service + "/orchid"
            self._check_service(path_to_orchid, service_name)

    @authors("babenko")
    def test_at_primary_masters(self):
        self._check_orchid("//sys/primary_masters", self.NUM_MASTERS, "master")

    @authors("rebenkoy")
    def test_at_cluster_masters(self):
        self._check_orchid("//sys/cluster_masters", self.NUM_MASTERS, "master")

    @authors("babenko")
    def test_at_nodes(self):
        self._check_orchid("//sys/cluster_nodes", self.NUM_NODES, "node")

    @authors("babenko")
    def test_at_scheduler(self):
        self._check_service("//sys/scheduler/orchid", "scheduler")

    @authors("savrus", "babenko")
    def test_at_tablet_cells(self):
        sync_create_cells(1)
        cells = ls("//sys/tablet_cells")
        assert len(cells) == 1
        for cell in cells:
            peers = get("//sys/tablet_cells/" + cell + "/@peers")
            for peer in peers:
                address = peer["address"]
                peer_cells = ls("//sys/cluster_nodes/" + address + "/orchid/tablet_cells")
                assert cell in peer_cells

    @authors("ifsmirnov")
    def test_master_reign(self):
        peer = ls("//sys/primary_masters")[0]
        assert type(get("//sys/primary_masters/{}/orchid/reign".format(peer))) == yson.YsonInt64

    @authors("max42")
    def test_invalid_orchid(self):
        # Missing remote_addresses attribute.
        create("orchid", "//tmp/orchid")

        # These requests should work fine.
        assert "orchid" in ls("//tmp")
        assert get("//tmp")["orchid"] == yson.YsonEntity()
        assert get("//tmp", attributes=["type"])["orchid"].attributes["type"] == "orchid"

        # TODO(max42): somehow this does not work.
        # assert exists("//tmp/orchid")

        # These must trigger error.
        with raises_yt_error("Missing required parameter /remote_addresses"):
            ls("//tmp/orchid")
        with raises_yt_error("Missing required parameter /remote_addresses"):
            get("//tmp/orchid")
        with raises_yt_error("Missing required parameter /remote_addresses"):
            exists("//tmp/orchid/foo")

    @authors("shakurov")
    def test_missing_default_address(self):
        create("orchid", "//tmp/orchid", attributes={"remote_addresses": {"hello": "127.0.0.1"}})

        assert get("//tmp")["orchid"] == yson.YsonEntity()
        with raises_yt_error("Cannot select address"):
            # Must not crash.
            get("//tmp/orchid")


##################################################################


class TestOrchidMulticell(TestOrchid):
    NUM_SECONDARY_MASTER_CELLS = 2

    @authors("babenko")
    def test_at_secondary_masters(self):
        for tag in range(1, self.NUM_SECONDARY_MASTER_CELLS + 1):
            self._check_orchid("//sys/secondary_masters/" + str(tag + 10), self.NUM_MASTERS, "master")

    @authors("rebenkoy")
    def test_at_cluster_masters(self):
        self._check_orchid("//sys/cluster_masters", self.NUM_MASTERS * (self.NUM_SECONDARY_MASTER_CELLS + 1), "master")
