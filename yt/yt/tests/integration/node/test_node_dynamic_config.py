from yt_env_setup import YTEnvSetup, wait, Restarter, NODES_SERVICE

from yt_commands import (
    authors,
    exists,
    get,
    set,
    ls,
    create,
    remove,
    read_table,
    write_table,
    run_test_vanilla,
    get_applied_node_dynamic_config,
    sync_create_cells,
    wait_for_node_alive_object_counts
)

from yt_helpers import profiler_factory

import yt_error_codes

import yt.yson as yson
from yt.common import YtError

from collections import namedtuple

import pytest

#################################################################


class TestNodeDynamicConfig(YTEnvSetup):
    NUM_TEST_PARTITIONS = 2

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
        if type(dynamic_config) == yson.YsonEntity:
            return ""

        return dynamic_config.get("config_annotation", "")

    def get_dynamic_config_last_change_time(self, node):
        return get("//sys/cluster_nodes/{}/orchid/dynamic_config_manager/last_config_change_time".format(node))

    def get_dynamic_config_errors(self, node):
        return get("//sys/cluster_nodes/{}/orchid/dynamic_config_manager/errors".format(node))

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
        assert len(self.get_dynamic_config_errors(nodes[0])) > 0
        assert len(self.get_dynamic_config_errors(nodes[1])) == 0

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
            except YtError:
                return False

        def check_jobs():
            try:
                op = run_test_vanilla("sleep 0.1")
                op.track()
                return True
            except YtError:
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

    def _get_used_cellar_slot_count(self, address):
        cellars = get("//sys/cluster_nodes/{}/@cellars".format(address))
        used_slot_count = 0
        for _, cellar in cellars.items():
            for slot in cellar:
                if slot["state"] != "none":
                    used_slot_count += 1
        return used_slot_count

    def _check_tablet_node_rct(self, address):
        used_slot_count = self._get_used_cellar_slot_count(address)
        wait_for_node_alive_object_counts(
            address,
            {
                "NYT::NTabletNode::TTabletSlot" : used_slot_count,
                "NYT::NHydra::TDistributedHydraManager" : used_slot_count,
                "NYT::NHydra::TDecoratedAutomaton" : used_slot_count,
                "NYT::NHydra::TLeaderCommitter" : used_slot_count,
                "NYT::NHydra::TRecovery" : used_slot_count,
                "NYT::NHydra::TLeaseTracker" : used_slot_count,
                "NYT::NHydra::TEpochContext" : used_slot_count,
                "NYT::NHydra::TRemoteChangelogStore" : used_slot_count,
                "NYT::NHydra::TRemoteSnapshotStore" : used_slot_count,
                "NYT::NApi::NNative::TJournalWriter" : used_slot_count,
            })

    def _check_tablet_nodes_rct(self):
        for address in ls("//sys/cluster_nodes"):
            self._check_tablet_node_rct(address)

    def _healthy_cell_count(self):
        result = 0
        for cell in ls("//sys/tablet_cells", attributes=["health"]):
            if cell.attributes["health"] == "good":
                result += 1
        return result

    def _init_zero_slot_nodes(self, config_node):
        set("//sys/tablet_cell_bundles/@config", {})
        set("//sys/@config/tablet_manager/tablet_cell_balancer/rebalance_wait_time", 100)
        set(
            "//sys/@config/tablet_manager/tablet_cell_balancer/enable_verbose_logging",
            True,
        )
        set("//sys/@config/tablet_manager/peer_revocation_timeout", 3000)

        sync_create_cells(5)
        assert self._healthy_cell_count() == 5

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

        wait(lambda: self._healthy_cell_count() == 0)

        if config_node == "cellar":
            config["nodeA"]["cellar_node"] = {
                "cellar_manager": {
                    "cellars": {
                        "tablet": {
                            "type": "tablet",
                            "size": 0,
                        }
                    }
                }
            }
        return node, config

    @authors("gritukan", "savrus")
    @pytest.mark.parametrize("config_node", ["tablet", "cellar"])
    def test_dynamic_tablet_slot_count(self, config_node):
        _, config = self._init_zero_slot_nodes(config_node)

        for slot_count in [2, 0, 7, 3, 1, 0, 2, 4]:
            if config_node == "cellar":
                config["nodeA"]["cellar_node"]["cellar_manager"]["cellars"]["tablet"]["size"] = slot_count
            else:
                config["nodeA"]["tablet_node"]["slots"] = slot_count
            set("//sys/cluster_nodes/@config", config)
            wait(lambda: self._healthy_cell_count() == min(5, slot_count))
            self._check_tablet_nodes_rct()

    @authors("capone212")
    @pytest.mark.parametrize("config_node", ["tablet", "cellar"])
    def test_bundle_dynamic_config(self, config_node):
        node_with_tag_NodeA, _ = self._init_zero_slot_nodes(config_node)

        def get_orchid_memory_limits(category):
            return get(
                "//sys/tablet_nodes/{0}/orchid/node_resource_manager/memory_limit_per_category/{1}"
                .format(node_with_tag_NodeA, category))

        def get_thread_pool_size(pool_name):
            return get(
                "//sys/tablet_nodes/{0}/orchid/tablet_node_thread_pools/{1}_thread_pool_size"
                .format(node_with_tag_NodeA, pool_name))

        bundle_dynamic_config = {
            "nodeA": {
                "cpu_limits": {},
                "memory_limits": {},
            },
            "!nodeA": {
                "cpu_limits": {},
            },
        }

        set("//sys/tablet_cell_bundles/@config", bundle_dynamic_config)

        ConfigPatch = namedtuple("ConfigPatch", ["slot_count", "lookup_threads", "query_threads", "tablet_static", "tablet_dynamic"])
        patches = [
            ConfigPatch(2, 1, 3, 2048, 4096),
            ConfigPatch(7, 3, 1, 4096, 2048),
            ConfigPatch(3, 7, 2, 1024, 8192),
            ConfigPatch(4, 2, 7, 8192, 2048),
        ]

        for patch in patches:
            bundle_dynamic_config["nodeA"]["cpu_limits"]["write_thread_pool_size"] = patch.slot_count
            bundle_dynamic_config["nodeA"]["cpu_limits"]["lookup_thread_pool_size"] = patch.lookup_threads
            bundle_dynamic_config["nodeA"]["cpu_limits"]["query_thread_pool_size"] = patch.query_threads
            bundle_dynamic_config["nodeA"]["memory_limits"]["tablet_static"] = patch.tablet_static
            bundle_dynamic_config["nodeA"]["memory_limits"]["tablet_dynamic"] = patch.tablet_dynamic

            set("//sys/tablet_cell_bundles/@config", bundle_dynamic_config)
            wait(lambda: self._healthy_cell_count() == min(5, patch.slot_count))
            self._check_tablet_nodes_rct()
            wait(lambda: get_orchid_memory_limits("tablet_static") == patch.tablet_static)
            wait(lambda: get_orchid_memory_limits("tablet_dynamic") == patch.tablet_dynamic)
            wait(lambda: get_thread_pool_size("tablet_lookup") == patch.lookup_threads)
            wait(lambda: get_thread_pool_size("query") == patch.query_threads)

    @authors("capone212")
    @pytest.mark.parametrize("config_node", ["tablet", "cellar"])
    def test_bundle_dynamic_config_caches(self, config_node):
        node_with_tag_NodeA, _ = self._init_zero_slot_nodes(config_node)

        def get_orchid_memory_limits(category):
            return get(
                "//sys/tablet_nodes/{0}/orchid/node_resource_manager/memory_limit_per_category/{1}"
                .format(node_with_tag_NodeA, category))

        bundle_dynamic_config = {
            "nodeA": {
                "cpu_limits": {},
                "memory_limits": {},
            },
            "!nodeA": {
                "cpu_limits": {},
            },
        }

        set("//sys/tablet_cell_bundles/@config", bundle_dynamic_config)
        bundle_dynamic_config["nodeA"]["memory_limits"]["uncompressed_block_cache"] = 4096
        bundle_dynamic_config["nodeA"]["memory_limits"]["compressed_block_cache"] = 8192
        bundle_dynamic_config["nodeA"]["memory_limits"]["versioned_chunk_meta"] = 16384

        set("//sys/tablet_cell_bundles/@config", bundle_dynamic_config)
        self._check_tablet_nodes_rct()
        wait(lambda: get_orchid_memory_limits("versioned_chunk_meta") == 16384)
        wait(lambda: get_orchid_memory_limits("block_cache") == 4096 + 8192)

    @authors("starodub")
    def test_versioned_chunk_meta_cache(self):
        set("//sys/tablet_cell_bundles/@config", {})
        nodes = ls("//sys/cluster_nodes")

        def get_orchid_memory_limits(node, category):
            return get(
                "//sys/cluster_nodes/{0}/orchid/node_resource_manager/memory_limit_per_category/{1}"
                .format(node, category))

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
        key = (
            "//sys/cluster_nodes/{0}/orchid/dynamic_config_manager/"
            "effective_config/tablet_node/versioned_chunk_meta_cache/capacity"
        )
        wait(lambda: get(key.format(nodes[0])) == 300, ignore_exceptions=True)
        wait(lambda: get_orchid_memory_limits(nodes[0], "versioned_chunk_meta") == 300)

    @authors("orlovorlov")
    def test_security_manager_config_in_dynamic_config_manager(self):
        nodes = ls("//sys/cluster_nodes")
        set("//sys/cluster_nodes/{0}/@user_tags".format(nodes[0]), ["nodeA"])

        config = {
            "nodeA": {
                "tablet_node": {
                    "security_manager": {
                        "resource_limits_cache": {
                            "expire_after_access_time": 10,
                        },
                    }
                }
            }
        }
        set("//sys/cluster_nodes/@config", config)

        key = (
            "//sys/cluster_nodes/{0}/orchid/dynamic_config_manager/"
            "effective_config/tablet_node/security_manager/resource_limits_cache/expire_after_access_time"
        )
        wait(lambda: get(key.format(nodes[0])) == 10, ignore_exceptions=True)

    @authors("achulkov2")
    def test_alert_solomon_export(self):
        set("//sys/tablet_cell_bundles/@config", {})
        nodes = ls("//sys/cluster_nodes")

        set("//sys/cluster_nodes/{0}/@user_tags".format(nodes[0]), ["nodeA"])
        set("//sys/cluster_nodes/{0}/@user_tags".format(nodes[1]), ["nodeB"])

        config = {
            "nodeA": {
                "config_annotation": 55,
            },
            "nodeB": {
                "some_unrecognized_option": 42,
            }
        }
        set("//sys/cluster_nodes/@config", config)

        def check():
            profiler_a = profiler_factory().at_node(nodes[0])
            gauge1 = profiler_a.get(
                "cluster_node/alerts",
                {"error_code": "NYT::EErrorCode::Generic"})
            gauge2 = profiler_a.get(
                "cluster_node/alerts",
                {"error_code": "NYT::NDynamicConfig::EErrorCode::InvalidDynamicConfig"})
            profiler_b = profiler_factory().at_node(nodes[1])
            gauge3 = profiler_b.get(
                "cluster_node/alerts",
                {"error_code": "NYT::NDynamicConfig::EErrorCode::UnrecognizedDynamicConfigOption"})
            return gauge1 == gauge2 == gauge3 == 1.0

        wait(check)

        config2 = {
            "nodeA": {
                "config_annotation": 55,
            },
            "nodeB": {
            }
        }

        set("//sys/cluster_nodes/@config", config2)

        def check2():
            profiler_a = profiler_factory().at_node(nodes[0])
            gauge1 = profiler_a.get(
                "cluster_node/alerts",
                {"error_code": "NYT::EErrorCode::Generic"})
            gauge2 = profiler_a.get(
                "cluster_node/alerts",
                {"error_code": "NYT::NDynamicConfig::EErrorCode::InvalidDynamicConfig"})
            profiler_b = profiler_factory().at_node(nodes[1])
            return gauge1 == gauge2 == 1.0 and not profiler_b.get_all("cluster_node/alerts")

        wait(check2)

    @authors("danilalexeev")
    def test_dynamic_hydra_manager_config(self):
        nodes = ls("//sys/cluster_nodes")
        set("//sys/cluster_nodes/{0}/@user_tags".format(nodes[0]), ["nodeA"])

        config = {
            "nodeA": {
                "cellar_node": {
                    "cellar_manager": {
                        "cellars": {
                            "tablet": {
                                "type": "tablet",
                                "hydra_manager": {
                                    "restart_backoff_time": 100
                                },
                            }
                        }
                    }
                }
            }
        }

        set("//sys/cluster_nodes/@config", config)

        key = (
            "//sys/cluster_nodes/{0}/orchid/dynamic_config_manager/effective_config/"
            "cellar_node/cellar_manager/cellars/tablet/hydra_manager/restart_backoff_time"
        )
        wait(lambda: get(key.format(nodes[0])) == 100, ignore_exceptions=True)
