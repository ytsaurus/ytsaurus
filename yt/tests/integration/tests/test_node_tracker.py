import pytest

from yt_env_setup import YTEnvSetup, Restarter, NODES_SERVICE, MASTERS_SERVICE
from yt_commands import *
from time import sleep

##################################################################

class TestNodeTracker(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3

    DELTA_NODE_CONFIG = {
        "tags" : [ "config_tag1", "config_tag2" ]
    }

    @authors("babenko")
    def test_ban(self):
        nodes = ls("//sys/cluster_nodes")
        assert len(nodes) == 3

        test_node = nodes[0]
        assert get("//sys/cluster_nodes/%s/@state" % test_node) == "online"

        set_node_banned(test_node, True)
        set_node_banned(test_node, False)

    @authors("babenko")
    def test_resource_limits_overrides_defaults(self):
        node = ls("//sys/cluster_nodes")[0]
        assert get("//sys/cluster_nodes/{0}/@resource_limits_overrides".format(node)) == {}

    @authors("psushin")
    def test_disable_write_sessions(self):
        nodes = ls("//sys/cluster_nodes")
        assert len(nodes) == 3

        create("table", "//tmp/t")
        write_table("//tmp/t", {"a" : "b"})

        for node in nodes:
            set("//sys/cluster_nodes/{0}/@disable_write_sessions".format(node), True)

        def can_write():
            try:
                write_table("//tmp/t", {"a" : "b"})
                return True
            except:
                return False

        assert not can_write()

        for node in nodes:
            set("//sys/cluster_nodes/{0}/@disable_write_sessions".format(node), False)

        wait(lambda: can_write())

    @authors("psushin", "babenko")
    def test_disable_scheduler_jobs(self):
        nodes = ls("//sys/cluster_nodes")
        assert len(nodes) == 3

        test_node = nodes[0]
        assert get("//sys/cluster_nodes/{0}/@resource_limits/user_slots".format(test_node)) > 0
        set("//sys/cluster_nodes/{0}/@disable_scheduler_jobs".format(test_node), True)

        wait(lambda: get("//sys/cluster_nodes/{0}/@resource_limits/user_slots".format(test_node)) == 0)

    @authors("babenko")
    def test_resource_limits_overrides_valiation(self):
        node = ls("//sys/cluster_nodes")[0]
        with pytest.raises(YtError): remove("//sys/cluster_nodes/{0}/@resource_limits_overrides".format(node))

    @authors("babenko")
    def test_user_tags_validation(self):
        node = ls("//sys/cluster_nodes")[0]
        with pytest.raises(YtError): set("//sys/cluster_nodes/{0}/@user_tags".format(node), 123)

    @authors("babenko")
    def test_user_tags_update(self):
        node = ls("//sys/cluster_nodes")[0]
        set("//sys/cluster_nodes/{0}/@user_tags".format(node), ["user_tag"])
        assert get("//sys/cluster_nodes/{0}/@user_tags".format(node)) == ["user_tag"]
        assert "user_tag" in get("//sys/cluster_nodes/{0}/@tags".format(node))

    @authors("babenko")
    def test_config_tags(self):
        for node in ls("//sys/cluster_nodes"):
            tags = get("//sys/cluster_nodes/{0}/@tags".format(node))
            assert "config_tag1" in tags
            assert "config_tag2" in tags

    @authors("babenko")
    def test_rack_tags(self):
        create_rack("r")
        node = ls("//sys/cluster_nodes")[0]
        assert "r" not in get("//sys/cluster_nodes/{0}/@tags".format(node))
        set("//sys/cluster_nodes/{0}/@rack".format(node), "r")
        assert "r" in get("//sys/cluster_nodes/{0}/@tags".format(node))
        remove("//sys/cluster_nodes/{0}/@rack".format(node))
        assert "r" not in get("//sys/cluster_nodes/{0}/@tags".format(node))

    @authors("babenko", "shakurov")
    def test_create_cluster_node(self):
        kwargs = {"type": "cluster_node"}
        with pytest.raises(YtError): execute_command("create", kwargs)

    @authors("gritukan")
    def test_node_decommissioned(self):
        nodes = ls("//sys/cluster_nodes")
        assert len(nodes) == 3

        create("table", "//tmp/t")

        def can_write():
            try:
                write_table("//tmp/t", {"a" : "b"})
                return True
            except:
                return False

        for node in nodes:
            wait(lambda: get("//sys/cluster_nodes/{0}/@resource_limits/user_slots".format(node)) > 0)
        wait(lambda: can_write())

        for node in nodes:
            set("//sys/cluster_nodes/{0}/@decommissioned".format(node), True)

        for node in nodes:
           wait(lambda: get("//sys/cluster_nodes/{0}/@resource_limits/user_slots".format(node)) == 0)
        wait(lambda: not can_write())

        for node in nodes:
            set("//sys/cluster_nodes/{0}/@decommissioned".format(node), False)

        for node in nodes:
            wait(lambda: get("//sys/cluster_nodes/{0}/@resource_limits/user_slots".format(node)) > 0)
        wait(lambda: can_write())

##################################################################

class TestNodeTrackerMulticell(TestNodeTracker):
    NUM_SECONDARY_MASTER_CELLS = 2

################################################################################

class TestRemoveClusterNodes(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SECONDARY_MASTER_CELLS = 2

    DELTA_NODE_CONFIG = {
        "data_node": {
            "lease_transaction_timeout": 2000,
            "lease_transaction_ping_period": 1000,
            "register_timeout": 1000,
            "incremental_heartbeat_timeout": 1000,
            "full_heartbeat_timeout": 1000,
            "job_heartbeat_timeout": 1000,
        }
    }

    @authors("babenko")
    def test_remove_nodes(self):
        for _ in xrange(10):
            with Restarter(self.Env, NODES_SERVICE):
                for node in ls("//sys/cluster_nodes"):
                    wait(lambda: get("//sys/cluster_nodes/{}/@state".format(node)) == "offline")
                    id = get("//sys/cluster_nodes/{}/@id".format(node))
                    remove("//sys/cluster_nodes/" + node)
                    wait(lambda: not exists("#" + id))

            build_snapshot(cell_id=None)

            with Restarter(self.Env, MASTERS_SERVICE):
                pass


################################################################################

class TestNodesCreatedBanned(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    DEFER_NODE_START = True

    @authors("shakurov")
    def test_new_nodes_created_banned(self):
        assert get("//sys/cluster_nodes/@count") == 0
        assert get("//sys/cluster_nodes/@online_node_count") == 0

        assert not get("//sys/@config/node_tracker/ban_new_nodes")
        set("//sys/@config/node_tracker/ban_new_nodes", True)

        self.Env.start_nodes(sync=False)

        wait(lambda: get("//sys/cluster_nodes/@count") == 3)
        wait(lambda: get("//sys/cluster_nodes/@online_node_count") == 0)

        nodes = ls("//sys/cluster_nodes")
        assert len(nodes) == 3

        for node in nodes:
            assert get("//sys/cluster_nodes/{0}/@banned".format(node))
            ban_message = get("//sys/cluster_nodes/{0}/@ban_message".format(node))
            assert "banned" in ban_message and "provisionally" in ban_message
            assert get("//sys/cluster_nodes/{0}/@state".format(node)) == "offline"

        for node in nodes:
            set("//sys/cluster_nodes/{0}/@banned".format(node), False)

        self.Env.synchronize()

        wait(lambda: get("//sys/cluster_nodes/@online_node_count") == 3)

        create("table", "//tmp/t")

        write_table("//tmp/t", {"a" : "b"})
        read_table("//tmp/t")

class TestNodesCreatedBannedMulticell(TestNodesCreatedBanned):
    NUM_SECONDARY_MASTER_CELLS = 2

################################################################################

class TestNodeUnrecognizedOptionsAlert(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 1
    NUM_SCHEDULERS = 1

    DELTA_NODE_CONFIG = {
        "enable_unrecognized_options_alert": True,
        "some_nonexistent_option": 42
    }

    @authors("gritukan")
    def test_node_unrecognized_options_alert(self):
        nodes = ls("//sys/cluster_nodes")
        alerts = get("//sys/cluster_nodes/{}/@alerts".format(nodes[0]))
        assert alerts[0]["code"] == UnrecognizedConfigOption
