from yt_env_setup import YTEnvSetup, wait, Restarter, NODES_SERVICE
from yt_commands import *  # noqa
import yt_error_codes
from yt.yson import YsonEntity

#################################################################


class TestNodeDynamicConfig(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_NODE_CONFIG = {
        "dynamic_config_manager": {
            "enable_unrecognized_options_alert": True,
        },
    }

    def get_dynamic_config_annotation(self, node):
        dynamic_config = get_applied_node_dynamic_config(node)
        if type(dynamic_config) == YsonEntity:
            return ""

        return dynamic_config.get("config_annotation", "")

    def get_dynamic_config_last_change_time(self, node):
        return get("//sys/cluster_nodes/{}/orchid/dynamic_config_manager/last_config_change_time".format(node))

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

        set("//sys/cluster_nodes/@config", {"%true": {}})

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
            },
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
            return len(alerts) == 1 and alerts[0]["code"] == yt_error_codes.UnrecognizedDynamicConfigOption

        wait(check_alert)
        wait(lambda: len(get("//sys/cluster_nodes/{0}/@alerts".format(nodes[1]))) == 0)

        wait(lambda: self.get_dynamic_config_annotation(nodes[0]) == "boo")
        assert get_applied_node_dynamic_config(nodes[0])["some_unrecognized_option"] == 42
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
            },
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
            return len(alerts) == 1 and alerts[0]["code"] == yt_error_codes.InvalidDynamicConfig

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
            return len(alerts) == 1 and alerts[0]["code"] == yt_error_codes.DuplicateMatchingDynamicConfigs

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
    def test_last_config_change_time(self):
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

        node_a_config_last_change_time = self.get_dynamic_config_last_change_time(nodes[0])
        node_b_config_last_change_time = self.get_dynamic_config_last_change_time(nodes[1])

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

        assert self.get_dynamic_config_last_change_time(nodes[0]) == node_a_config_last_change_time
        assert self.get_dynamic_config_last_change_time(nodes[1]) > node_b_config_last_change_time

    @authors("gritukan")
    def test_no_config_node(self):
        create("table", "//tmp/r")
        create("table", "//tmp/w")

        write_table("//tmp/r", [{"x": "y"}])

        sync_create_cells(1)
        cell_id = ls("//sys/tablet_cells")[0]
        assert get("#{0}/@health".format(cell_id)) == "good"

        def check_reads():
            return read_table("//tmp/r") == [{"x": "y"}]

        def check_writes():
            try:
                write_table("//tmp/w", [{"x": "y"}])
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
        assert check_writes()
        assert check_jobs()
        assert check_tablet_cells()

        with Restarter(self.Env, NODES_SERVICE):
            remove("//sys/cluster_nodes/@config")

        wait(lambda: check_reads())
        wait(lambda: check_writes())
        wait(lambda: check_jobs())
        wait(lambda: check_tablet_cells())
        for node in ls("//sys/cluster_nodes"):
            assert not exists("//sys/cluster_nodes/dynamic_config_manager/applied_config")
            assert len(get("//sys/cluster_nodes/{0}/@alerts".format(node))) == 0

    @authors("gritukan", "savrus")
    @pytest.mark.parametrize("config_node", ["tablet", "cellar"])
    def test_dynamic_tablet_slot_count(self, config_node):
        set("//sys/@config/tablet_manager/tablet_cell_balancer/rebalance_wait_time", 100)
        set(
            "//sys/@config/tablet_manager/tablet_cell_balancer/enable_verbose_logging",
            True,
        )
        set("//sys/@config/tablet_manager/peer_revocation_timeout", 3000)

        def healthy_cell_count():
            result = 0
            for cell in ls("//sys/tablet_cells", attributes=["health"]):
                if cell.attributes["health"] == "good":
                    result += 1
            return result

        sync_create_cells(5)
        assert healthy_cell_count() == 5

        node = ls("//sys/cluster_nodes")[0]
        set("//sys/cluster_nodes/{0}/@user_tags".format(node), ["nodeA"])

        # All nodes are non-tablet.
        config = {
            "nodeA": {
                "config_annotation": "nodeA",
                "tablet_node": {
                    "slots": 0,
                },
            },
            "!nodeA": {
                "config_annotation": "notNodeA",
                "tablet_node": {
                    "slots": 0,
                },
            },
        }
        set("//sys/cluster_nodes/@config", config)

        wait(lambda: healthy_cell_count() == 0)

        if config_node == "cellar":
            config["nodeA"]["cellar_node"] = {
                "cellar_manager": {
                    "cellars": {
                        "tablet" : {
                            "type": "tablet",
                            "size": 0,
                        }
                    }
                }
            }

        for slot_count in [2, 0, 7, 3, 1, 0, 2, 4]:
            if config_node == "cellar":
                config["nodeA"]["cellar_node"]["cellar_manager"]["cellars"]["tablet"]["size"] = slot_count
            else:
                config["nodeA"]["tablet_node"]["slots"] = slot_count
            set("//sys/cluster_nodes/@config", config)
            wait(lambda: healthy_cell_count() == min(5, slot_count))

    @authors("starodub")
    def test_versioned_chunk_meta_cache(self):
        nodes = ls("//sys/cluster_nodes")

        set("//sys/cluster_nodes/{0}/@user_tags".format(nodes[0]), ["nodeA"])
        config = {
            "nodeA": {
                "tablet_node": {
                    "versioned_chunk_meta_cache": {
                        "capacity": 300,
                    }
                }
            }
        }
        set("//sys/cluster_nodes/@config", config)
        key = "//sys/cluster_nodes/{0}/orchid/dynamic_config_manager/effective_config/tablet_node/versioned_chunk_meta_cache/capacity"
        wait(lambda: get(key.format(nodes[0])) == 300, ignore_exceptions=True)
