from yt_env_setup import YTEnvSetup, wait, Restarter, NODES_SERVICE
from yt_commands import *
from yt.yson import YsonEntity

import time

#################################################################

class TestNodeDynamicConfig(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_NODE_CONFIG = {
        "dynamic_config_manager": {
            "enabled": True,
            "update_period": 50,
            "enable_unrecognized_options_alert": True,
        },
    }

    def get_dynamic_config(self, node):
        return get("//sys/cluster_nodes/{}/orchid/dynamic_config_manager/config".format(node))

    def get_dynamic_config_annotation(self, node):
        dynamic_config = self.get_dynamic_config(node)
        if type(dynamic_config) == YsonEntity:
            return ""

        return dynamic_config.get("config_annotation", "")

    def get_dynamic_config_last_update_time(self, node):
        return get("//sys/cluster_nodes/{}/orchid/dynamic_config_manager/last_config_update_time".format(node))

    @authors("gritukan")
    def test_simple(self):
        for node in ls("//sys/cluster_nodes"):
            assert self.get_dynamic_config_annotation(node) == "default"

        config = {
            "%true": {
                "config_annotation": "foo",
            },
        }
        set("//sys/cluster_nodes/@config", config)

        for node in ls("//sys/cluster_nodes"):
            wait(lambda: self.get_dynamic_config_annotation(node) == "foo")

        set("//sys/cluster_nodes/@config", {})

        for node in ls("//sys/cluster_nodes"):
            wait(lambda: self.get_dynamic_config_annotation(node) == "")

        set("//sys/cluster_nodes/@config", config)

    @authors("gritukan")
    def test_cleanup(self):
        # Previous test sets non-trivial config.
        # Let's check whether it's removed.
        for node in ls("//sys/cluster_nodes"):
            assert self.get_dynamic_config_annotation(node) == "default"

    @authors("gritukan")
    def test_config_tag_filter(self):
        nodes = ls("//sys/cluster_nodes")
        assert len(nodes) == 3

        set("//sys/cluster_nodes/{0}/@user_tags".format(nodes[0]), ["nodeA"])
        set("//sys/cluster_nodes/{0}/@user_tags".format(nodes[1]), ["nodeB"])
        set("//sys/cluster_nodes/{0}/@user_tags".format(nodes[2]), ["nodeC"])

        config = {
            "nodeA": {
                "config_annotation": "configA",
            },
            "nodeB": {
                "config_annotation": "configB",
            },
            "nodeC": {
                "config_annotation": "configC",
            },
        }

        set("//sys/cluster_nodes/@config", config)
        wait(lambda: self.get_dynamic_config_annotation(nodes[0]) == "configA")
        wait(lambda: self.get_dynamic_config_annotation(nodes[1]) == "configB")
        wait(lambda: self.get_dynamic_config_annotation(nodes[2]) == "configC")

    @authors("gritukan")
    def test_dynamic_node_tag_list(self):
        nodes = ls("//sys/cluster_nodes")

        set("//sys/cluster_nodes/{0}/@user_tags".format(nodes[0]), ["nodeA"])
        config = {
            "nodeA": {
                "config_annotation": "configA",
            },
            "nodeB": {
                "config_annotation": "configB",
            },
        }

        set("//sys/cluster_nodes/@config", config)
        wait(lambda: self.get_dynamic_config_annotation(nodes[0]) == "configA")

        set("//sys/cluster_nodes/{0}/@user_tags".format(nodes[0]), ["nodeB"])
        wait(lambda: self.get_dynamic_config_annotation(nodes[0]) == "configB")

    @authors("gritukan")
    def test_unrecognized_dynamic_config_options_alert(self):
        nodes = ls("//sys/cluster_nodes")

        set("//sys/cluster_nodes/{0}/@user_tags".format(nodes[0]), ["nodeA"])
        set("//sys/cluster_nodes/{0}/@user_tags".format(nodes[1]), ["nodeB"])

        config = {
            "nodeA": {
                "config_annotation": "foo",
            },
            "nodeB": {
                "config_annotation": "fooB",
            }
        }
        set("//sys/cluster_nodes/@config", config)

        wait(lambda: self.get_dynamic_config_annotation(nodes[0]) == "foo")
        wait(lambda: self.get_dynamic_config_annotation(nodes[1]) == "fooB")

        config = {
            "nodeA": {
                "config_annotation": "boo",
                "some_unrecognized_option": 42,
            },
            "nodeB": {
                "config_annotation": "foo",
            },
        }
        set("//sys/cluster_nodes/@config", config)

        def check_alert():
            alerts = get("//sys/cluster_nodes/{0}/@alerts".format(nodes[0]))
            return len(alerts) == 1 and alerts[0]["code"] == UnrecognizedDynamicConfigOption
        wait(check_alert)
        wait(lambda: len(get("//sys/cluster_nodes/{0}/@alerts".format(nodes[1]))) == 0)

        wait(lambda: self.get_dynamic_config_annotation(nodes[0]) == "boo")
        assert self.get_dynamic_config(nodes[0])["some_unrecognized_option"] == 42
        wait(lambda: self.get_dynamic_config_annotation(nodes[1]) == "foo")

    @authors("gritukan")
    def test_invalid_config(self):
        nodes = ls("//sys/cluster_nodes")

        set("//sys/cluster_nodes/{0}/@user_tags".format(nodes[0]), ["nodeA"])
        set("//sys/cluster_nodes/{0}/@user_tags".format(nodes[1]), ["nodeB"])

        config = {
            "nodeA": {
                "config_annotation": "foo",
            },
            "nodeB": {
                "config_annotation": "fooB",
            }
        }
        set("//sys/cluster_nodes/@config", config)

        wait(lambda: self.get_dynamic_config_annotation(nodes[0]) == "foo")
        wait(lambda: self.get_dynamic_config_annotation(nodes[1]) == "fooB")

        config = {
            "nodeA": {
                "config_annotation": 42,
            },
            "nodeB": {
                "config_annotation": "foo",
            },
        }
        set("//sys/cluster_nodes/@config", config)

        def check_alert():
            alerts = get("//sys/cluster_nodes/{0}/@alerts".format(nodes[0]))
            return len(alerts) == 1 and alerts[0]["code"] == InvalidDynamicConfig
        wait(check_alert)
        wait(lambda: len(get("//sys/cluster_nodes/{0}/@alerts".format(nodes[1]))) == 0)

        wait(lambda: self.get_dynamic_config_annotation(nodes[1]) == "foo")
        assert self.get_dynamic_config_annotation(nodes[0]) == "foo"

    @authors("gritukan")
    def test_multiple_suitable_configs(self):
        nodes = ls("//sys/cluster_nodes")

        config = {
            "nodeA": {
                "config_annotation": "foo",
            },
        }
        set("//sys/cluster_nodes/@config", config)

        set("//sys/cluster_nodes/{0}/@user_tags".format(nodes[0]), ["nodeA"])
        set("//sys/cluster_nodes/{0}/@user_tags".format(nodes[1]), ["nodeB"])
        set("//sys/cluster_nodes/{0}/@user_tags".format(nodes[2]), ["nodeA", "nodeB"])

        wait(lambda: self.get_dynamic_config_annotation(nodes[0]) == "foo")
        wait(lambda: self.get_dynamic_config_annotation(nodes[1]) == "")
        wait(lambda: self.get_dynamic_config_annotation(nodes[2]) == "foo")

        config = {
            "nodeA": {
                "config_annotation": "configA",
            },
            "nodeB": {
                "config_annotation": "configB",
            },
        }
        set("//sys/cluster_nodes/@config", config)

        def check_alert():
            alerts = get("//sys/cluster_nodes/{0}/@alerts".format(nodes[2]))
            return len(alerts) == 1 and alerts[0]["code"] == DuplicateMatchingDynamicConfigs
        wait(check_alert)
        wait(lambda: self.get_dynamic_config_annotation(nodes[2]) == "foo")

        wait(lambda: self.get_dynamic_config_annotation(nodes[0]) == "configA")
        wait(lambda: len(get("//sys/cluster_nodes/{0}/@alerts".format(nodes[0]))) == 0)

        wait(lambda: self.get_dynamic_config_annotation(nodes[1]) == "configB")
        wait(lambda: len(get("//sys/cluster_nodes/{0}/@alerts".format(nodes[1]))) == 0)

    @authors("gritukan")
    def test_boolean_formula(self):
        nodes = ls("//sys/cluster_nodes")

        set("//sys/cluster_nodes/{0}/@user_tags".format(nodes[0]), ["nodeA"])
        set("//sys/cluster_nodes/{0}/@user_tags".format(nodes[1]), ["nodeB"])

        config = {
            "nodeA": {
                "config_annotation": "configA",
            },
            "nodeB": {
                "config_annotation": "configB",
            },
            "!nodeA & !nodeB": {
                "config_annotation": "configC",
            },
        }
        set("//sys/cluster_nodes/@config", config)

        wait(lambda: self.get_dynamic_config_annotation(nodes[0]) == "configA")
        wait(lambda: len(get("//sys/cluster_nodes/{0}/@alerts".format(nodes[0]))) == 0)

        wait(lambda: self.get_dynamic_config_annotation(nodes[1]) == "configB")
        wait(lambda: len(get("//sys/cluster_nodes/{0}/@alerts".format(nodes[1]))) == 0)

        wait(lambda: self.get_dynamic_config_annotation(nodes[2]) == "configC")
        wait(lambda: len(get("//sys/cluster_nodes/{0}/@alerts".format(nodes[2]))) == 0)

    @authors("gritukan")
    def test_last_config_update_time(self):
        nodes = ls("//sys/cluster_nodes")

        set("//sys/cluster_nodes/{0}/@user_tags".format(nodes[0]), ["nodeA"])
        set("//sys/cluster_nodes/{0}/@user_tags".format(nodes[1]), ["nodeB"])

        config = {
            "nodeA": {
                "config_annotation": "configA",
            },
            "nodeB": {
                "config_annotation": "configB",
            },
        }
        set("//sys/cluster_nodes/@config", config)

        wait(lambda: self.get_dynamic_config_annotation(nodes[0]) == "configA")
        wait(lambda: self.get_dynamic_config_annotation(nodes[1]) == "configB")

        node_a_config_last_update_time = self.get_dynamic_config_last_update_time(nodes[0])
        node_b_config_last_update_time = self.get_dynamic_config_last_update_time(nodes[1])

        config = {
            "nodeA": {
                "config_annotation": "configA",
            },
            "nodeB": {
                "config_annotation": "configB2",
            },
        }
        set("//sys/cluster_nodes/@config", config)

        wait(lambda: self.get_dynamic_config_annotation(nodes[0]) == "configA")
        wait(lambda: self.get_dynamic_config_annotation(nodes[1]) == "configB2")

        assert self.get_dynamic_config_last_update_time(nodes[0]) == node_a_config_last_update_time
        assert self.get_dynamic_config_last_update_time(nodes[1]) > node_b_config_last_update_time

    @authors("gritukan")
    def test_node_read_only(self):
        create("table", "//tmp/t")
        write_table("//tmp/t", [{"x": "y"}])

        sync_create_cells(1)
        cell_id = ls("//sys/tablet_cells")[0]
        assert get("#{0}/@health".format(cell_id)) == "good"

        with Restarter(self.Env, NODES_SERVICE):
            remove("//sys/cluster_nodes/@config")

        def check_reads():
            return read_table("//tmp/t") == [{"x": "y"}]

        def check_writes():
            try:
                write_table("//tmp/t", [{"x": "y"}])
                return True
            except:
                return False

        def check_jobs():
            try:
                op = run_test_vanilla("sleep 0.1")
                op.track()
                return True
            except:
                return False

        def check_tablet_cells():
            return get("#{0}/@health".format(cell_id)) == "good"

        assert check_reads()
        assert not check_writes()
        assert not check_jobs()
        assert not check_tablet_cells()

        set("//sys/cluster_nodes/@config", {"%true": {}})

        wait(lambda: check_writes())
        wait(lambda: check_jobs())
        wait(lambda: check_tablet_cells())