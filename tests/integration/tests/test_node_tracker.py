import pytest

from yt_env_setup import YTEnvSetup
from yt_commands import *
from time import sleep

##################################################################

class TestNodeTracker(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3

    def test_ban(self):
        nodes = ls("//sys/nodes")
        assert len(nodes) == 3

        test_node = nodes[0]
        assert get("//sys/nodes/%s/@state" % test_node) == "online"

        self.set_node_banned(test_node, True)
        self.set_node_banned(test_node, False)

    def test_resource_limits_overrides_defaults(self):
        node = ls("//sys/nodes")[0]
        assert get("//sys/nodes/{0}/@resource_limits_overrides".format(node)) == {}

    def test_resource_limits_overrides_valiation(self):
        node = ls("//sys/nodes")[0]
        with pytest.raises(YtError): remove("//sys/nodes/{0}/@resource_limits_overrides".format(node))

##################################################################

class TestNodeTrackerMulticell(TestNodeTracker):
    NUM_SECONDARY_MASTER_CELLS = 2
