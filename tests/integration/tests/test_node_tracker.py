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

##################################################################

class TestNodeTrackerMulticell(TestNodeTracker):
    NUM_SECONDARY_MASTER_CELLS = 2
