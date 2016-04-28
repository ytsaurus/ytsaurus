import pytest

from yt_env_setup import YTEnvSetup
from yt_commands import *
from time import sleep

##################################################################

class TestNodeTracker(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3

    DELTA_NODE_CONFIG = {
        "tags" : [ "config_tag1", "config_tag2" ]
    }

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

    def test_user_tags_validation(self):
        node = ls("//sys/nodes")[0]
        with pytest.raises(YtError): set("//sys/nodes/{0}/@user_tags".format(node), 123)

    def test_user_tags_update(self):
        node = ls("//sys/nodes")[0]
        set("//sys/nodes/{0}/@user_tags".format(node), ["user_tag"])
        assert get("//sys/nodes/{0}/@user_tags".format(node)) == ["user_tag"]       
        assert "user_tag" in get("//sys/nodes/{0}/@tags".format(node))       

    def test_config_tags(self):
        for node in ls("//sys/nodes"):
            tags = get("//sys/nodes/{0}/@tags".format(node))
            assert "config_tag1" in tags
            assert "config_tag2" in tags

    def test_rack_tags(self):
        create_rack("r")
        node = ls("//sys/nodes")[0]
        assert "r" not in get("//sys/nodes/{0}/@tags".format(node))
        set("//sys/nodes/{0}/@rack".format(node), "r")
        assert "r" in get("//sys/nodes/{0}/@tags".format(node))
        remove("//sys/nodes/{0}/@rack".format(node))
        assert "r" not in get("//sys/nodes/{0}/@tags".format(node))
        
##################################################################

class TestNodeTrackerMulticell(TestNodeTracker):
    NUM_SECONDARY_MASTER_CELLS = 2
