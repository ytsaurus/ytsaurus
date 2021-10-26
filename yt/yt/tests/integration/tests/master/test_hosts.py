
from yt_env_setup import YTEnvSetup

from yt_commands import (
    authors, ls, get, set, remove, create_rack, exists, create_host, remove_host)

from yt.common import YtError

import pytest

##################################################################


class TestHosts(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3

    def teardown(self):
        for node in ls("//sys/cluster_nodes"):
            self._set_host(node, node)

        for host in ls("//sys/hosts"):
            try:
                remove_host(host)
            except:
                pass

    def _get_custom_hosts(self):
        nodes = ls("//sys/cluster_nodes")
        hosts = ls("//sys/hosts")
        for node in nodes:
            assert node in hosts
        return [host for host in hosts if host not in nodes]

    def _get_host(self, node):
        return get("//sys/cluster_nodes/{}/@host".format(node))

    def _set_host(self, node, host):
        set("//sys/cluster_nodes/{}/@host".format(node), host)

    def _get_nodes(self, host):
        return get("//sys/hosts/{}/@nodes".format(host))

    def _get_rack(self, host):
        return get("//sys/hosts/{}/@rack".format(host))

    def _set_rack(self, host, rack):
        set("//sys/hosts/{}/@rack".format(host), rack)

    def _reset_rack(self, host):
        remove("//sys/hosts/{}/@rack".format(host))

    def _get_hosts(self, rack):
        return get("//sys/racks/{}/@hosts".format(rack))

    @authors("gritukan")
    def test_create(self):
        create_host("h")
        assert self._get_custom_hosts() == ["h"]
        assert get("//sys/hosts/h/@name") == "h"
        assert get("//sys/hosts/h/@nodes") == []
        assert not exists("//sys/hosts/h/@rack")

    @authors("gritukan")
    def test_empty_name_fail(self):
        with pytest.raises(YtError):
            create_host("")
        assert self._get_custom_hosts() == []

    @authors("gritukan")
    def test_duplicate_name_fail(self):
        create_host("h")
        with pytest.raises(YtError):
            create_host("h")
        assert self._get_custom_hosts() == ["h"]

    @authors("gritukan")
    def test_rename_forbidden(self):
        create_host("h")
        with pytest.raises(YtError):
            set("//sys/hosts/h/@name", "g")
        assert self._get_custom_hosts() == ["h"]

    @authors("gritukan")
    def test_default_node_host(self):
        for node in ls("//sys/cluster_nodes"):
            assert self._get_host(node) == node
            assert self._get_nodes(node) == [node]

    @authors("gritukan")
    def test_set_host(self):
        node = ls("//sys/cluster_nodes")[0]
        create_host("h")

        assert self._get_host(node) == node
        assert self._get_nodes(node) == [node]
        assert self._get_nodes("h") == []
        self._set_host(node, "h")
        assert self._get_host(node) == "h"
        assert self._get_nodes(node) == []
        assert self._get_nodes("h") == [node]

    @authors("gritukan")
    def test_set_invalid_host(self):
        node = ls("//sys/cluster_nodes")[0]
        with pytest.raises(YtError):
            self._set_host(node, "h")
        assert self._get_host(node) == node
        assert self._get_nodes(node) == [node]

    @authors("gritukan")
    def test_remove_host(self):
        create_host("h")
        remove_host("h")

    @authors("gritukan")
    def test_remove_forbidden(self):
        create_host("h")
        node = ls("//sys/cluster_nodes")[0]
        self._set_host(node, "h")
        with pytest.raises(YtError):
            remove_host("h")

    @authors("gritukan")
    def test_set_rack(self):
        create_host("h")
        create_rack("r")

        assert self._get_hosts("r") == []
        self._set_rack("h", "r")
        assert self._get_rack("h") == "r"
        assert self._get_hosts("r") == ["h"]
        self._reset_rack("h")
        assert not exists("//sys/hosts/h/@rack")
        assert self._get_hosts("r") == []

    @authors("gritukan")
    def test_set_rack_fail(self):
        create_host("h")
        with pytest.raises(YtError):
            self._set_rack("h", "r")

##################################################################


class TestHostsMulticell(TestHosts):
    NUM_SECONDARY_MASTER_CELLS = 2
