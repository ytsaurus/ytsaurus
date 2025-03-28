from yt_env_setup import YTEnvSetup
import yt.yson as yson
import yt.packages.requests as requests
from yt_commands import authors, ls, exists, set, get, create, create_tablet_cell_bundle, create_area, execute_command

import time
from typing import Tuple

##################################################################


class TestBundleController(YTEnvSetup):
    ENABLE_MULTIDAEMON = False  # Cell balancer crashes in multidaemon mode.
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_CELL_BALANCERS = 3
    NUM_HTTP_PROXIES = 1
    NUM_RPC_PROXIES = 1
    ENABLE_BUNDLE_CONTROLLER = True
    USE_DYNAMIC_TABLES = True
    ENABLE_HTTP_PROXY = True
    ENABLE_RPC_PROXY = True

    DELTA_CELL_BALANCER_CONFIG = {
        "election_manager": {
            "transaction_timeout": "5s",
            "transaction_ping_period": "100ms",
            "lock_acquisition_period": "100ms",
            "leader_cache_update_period": "100ms",
        },
    }

    def _get_proxy_address(self):
        return "http://" + self.Env.get_proxy_address()

    def _get_bundle_config_url(self):
        return self._get_proxy_address() + "/api/v4/get_bundle_config"

    def _set_bundle_config_url(self):
        return self._get_proxy_address() + "/api/v4/set_bundle_config"

    def _get_bundle_config(self, bundle_name):
        params = {
            "bundle_name": bundle_name,
        }
        headers = {
            "X-YT-Parameters": yson.dumps(params),
            "X-YT-Header-Format": "<format=text>yson",
            "X-YT-Output-Format": "<format=text>yson",
        }

        rsp = requests.post(self._get_bundle_config_url(), headers=headers)
        rsp.raise_for_status()

        return yson.loads(rsp.content)

    def _set_bundle_config(self, bundle_config):
        params = bundle_config
        headers = {
            "X-YT-Parameters": yson.dumps(params),
            "X-YT-Header-Format": "<format=text>yson",
            "X-YT-Output-Format": "<format=text>yson",
        }

        rsp = requests.post(self._set_bundle_config_url(), headers=headers)
        rsp.raise_for_status()

    def _initialize_zone_default(self):
        create("map_node", "//sys/bundle_controller/controller/zones/zone_default", recursive=True, force=True)
        set("//sys/bundle_controller/controller/zones/zone_default/@rpc_proxy_sizes", {
            "medium": {
                "resource_guarantee": {
                    "memory": 21474836480,
                    "net": 1090519040,
                    "net_bytes": 1090519040 // 8,
                    "vcpu": 10000
                }
            },
            "small": {
                "resource_guarantee": {
                    "memory": 21474836480,
                    "net": 545259520,
                    "net_bytes": 545259520 // 8,
                    "vcpu": 4000
                }
            }
        })
        set("//sys/bundle_controller/controller/zones/zone_default/@tablet_node_sizes", {
            "cpu_intensive": {
                "default_config": {
                    "cpu_limits": {
                        "lookup_thread_pool_size": 12,
                        "query_thread_pool_size": 12,
                        "write_thread_pool_size": 10
                    },
                    "memory_limits": {
                        "compressed_block_cache": 8589934592,
                        "lookup_row_cache": 1024,
                        "tablet_dynamic": 21474836480,
                        "tablet_static": 10737418240,
                        "uncompressed_block_cache": 8589934592,
                        "versioned_chunk_meta": 21474836480
                    }
                },
                "resource_guarantee": {
                    "memory": 107374182400,
                    "net": 5368709120,
                    "vcpu": 28000
                }
            },
            "medium": {
                "default_config": {
                    "cpu_limits": {
                        "lookup_thread_pool_size": 4,
                        "query_thread_pool_size": 4,
                        "write_thread_pool_size": 10
                    },
                    "memory_limits": {
                        "compressed_block_cache": 8589934592,
                        "lookup_row_cache": 0,
                        "reserved": 21474836480,
                        "tablet_dynamic": 15032385536,
                        "tablet_static": 42949672960,
                        "uncompressed_block_cache": 8589934592,
                        "versioned_chunk_meta": 10737418240
                    }
                },
                "resource_guarantee": {
                    "memory": 107374182400,
                    "net": 2684354560,
                    "vcpu": 14000
                }
            },
            "small": {
                "default_config": {
                    "cpu_limits": {
                        "lookup_thread_pool_size": 2,
                        "query_thread_pool_size": 2,
                        "write_thread_pool_size": 5
                    },
                    "memory_limits": {
                        "compressed_block_cache": 4294967296,
                        "lookup_row_cache": 1024,
                        "reserved": 10737418240,
                        "tablet_dynamic": 7516192768,
                        "tablet_static": 21474836480,
                        "uncompressed_block_cache": 4294967296,
                        "versioned_chunk_meta": 5368709120
                    }
                },
                "resource_guarantee": {
                    "memory": 53687091200,
                    "net": 1342177280,
                    "vcpu": 7000
                }
            },
            "tiny": {
                "default_config": {
                    "cpu_limits": {
                        "lookup_thread_pool_size": 1,
                        "query_thread_pool_size": 1,
                        "write_thread_pool_size": 1
                    },
                    "memory_limits": {
                        "compressed_block_cache": 1073741824,
                        "lookup_row_cache": 1073741824,
                        "reserved": 8589934592,
                        "tablet_dynamic": 2147483648,
                        "tablet_static": 4294967296,
                        "uncompressed_block_cache": 1073741824,
                        "versioned_chunk_meta": 2147483648
                    }
                },
                "resource_guarantee": {
                    "memory": 21474836480,
                    "net": 104857600,
                    "vcpu": 4000
                }
            }
        }, recursive=True)

    def _set_bundle_system_quotas_account_for_bundle(self, bundle):
        account_name = f"{bundle}_bundle_system_quotas"
        execute_command(
            "create",
            {
                "type": "account",
                "attributes": {
                    "name": account_name,
                    "parent_name": "bundle_system_quotas",
                },
            },
        )
        set(
            f"//sys/tablet_cell_bundles/{bundle}/@options/changelog_account",
            account_name,
        )
        set(
            f"//sys/tablet_cell_bundles/{bundle}/@options/snapshot_account",
            account_name,
        )

    def _fill_default_bundle(self):
        self._initialize_zone_default()

        set("//sys/tablet_cell_bundles/default/@zone", "zone_default")

        set("//sys/tablet_cell_bundles/default/@bundle_controller_target_config", {})
        set("//sys/tablet_cell_bundles/default/@bundle_controller_target_config/cpu_limits", {
            "lookup_thread_pool_size": 16,
            "query_thread_pool_size": 4,
            "write_thread_pool_size": 10})

        set("//sys/tablet_cell_bundles/default/@bundle_controller_target_config/memory_limits", {
            "compressed_block_cache": 17179869184,
            "key_filter_block_cache": 1024,
            "lookup_row_cache": 1024,
            "reserved": 1024,
            "tablet_dynamic": 10737418240,
            "tablet_static": 10737418240,
            "uncompressed_block_cache": 17179869184,
            "versioned_chunk_meta": 10737418240})

        set("//sys/tablet_cell_bundles/default/@bundle_controller_target_config/rpc_proxy_count", 1)
        set("//sys/tablet_cell_bundles/default/@bundle_controller_target_config/tablet_node_count", 1)

        set("//sys/tablet_cell_bundles/default/@bundle_controller_target_config/rpc_proxy_resource_guarantee", {
            "memory": 21474836480,
            "net": 1090519040,
            "net_bytes": 1090519040 // 8,
            "type": "medium",
            "vcpu": 10000})

        set("//sys/tablet_cell_bundles/default/@bundle_controller_target_config/tablet_node_resource_guarantee", {
            "memory": 107374182400,
            "net": 5368709120,
            "net_bytes": 5368709120 // 8,
            "type": "cpu_intensive",
            "vcpu": 28000})

        set("//sys/tablet_cell_bundles/default/@resource_quota", {
            "cpu": 100,
            "network": 5*1024*1024*1024,
            "memory": 750323855360})

        self._set_bundle_system_quotas_account_for_bundle("default")

    def _create_bundle(self, bundle, enable_bundle_controller=True, zone="zone_default", enable_instance_allocation=False, bundle_controller_target_config={}, **kwargs):
        user_attributes = {
            "skip_spare_nodes_monitoring": True,
            "resource_quota": {
                "cpu": 860,
                "memory": 6175089229824,
            },
        }
        create_tablet_cell_bundle(bundle, attributes={
            "enable_bundle_controller": enable_bundle_controller,
            "zone": zone,
            "bundle_controller_target_config": bundle_controller_target_config,
            "enable_instance_allocation": enable_instance_allocation,
            **user_attributes,
            **kwargs,
        })
        self._set_bundle_system_quotas_account_for_bundle(bundle)

    def _get_cypress_config(self, bundle_name):
        config = {}
        config["bundle_config"] = {}

        config["bundle_name"] = bundle_name

        config["bundle_config"]["rpc_proxy_count"] = get("//sys/tablet_cell_bundles/default/@bundle_controller_target_config/rpc_proxy_count")
        config["bundle_config"]["tablet_node_count"] = get("//sys/tablet_cell_bundles/default/@bundle_controller_target_config/tablet_node_count")

        config["bundle_config"]["cpu_limits"] = {
            "lookup_thread_pool_size": get("//sys/tablet_cell_bundles/default/@bundle_controller_target_config/cpu_limits/lookup_thread_pool_size"),
            "query_thread_pool_size": get("//sys/tablet_cell_bundles/default/@bundle_controller_target_config/cpu_limits/query_thread_pool_size"),
            "write_thread_pool_size": get("//sys/tablet_cell_bundles/default/@bundle_controller_target_config/cpu_limits/write_thread_pool_size"),
        }

        config["bundle_config"]["memory_limits"] = {
            "compressed_block_cache": get("//sys/tablet_cell_bundles/default/@bundle_controller_target_config/memory_limits/compressed_block_cache"),
            "key_filter_block_cache": get("//sys/tablet_cell_bundles/default/@bundle_controller_target_config/memory_limits/key_filter_block_cache"),
            "reserved": get("//sys/tablet_cell_bundles/default/@bundle_controller_target_config/memory_limits/reserved"),
            "lookup_row_cache": get("//sys/tablet_cell_bundles/default/@bundle_controller_target_config/memory_limits/lookup_row_cache"),
            "tablet_dynamic": get("//sys/tablet_cell_bundles/default/@bundle_controller_target_config/memory_limits/tablet_dynamic"),
            "tablet_static": get("//sys/tablet_cell_bundles/default/@bundle_controller_target_config/memory_limits/tablet_static"),
            "uncompressed_block_cache": get("//sys/tablet_cell_bundles/default/@bundle_controller_target_config/memory_limits/uncompressed_block_cache"),
            "versioned_chunk_meta": get("//sys/tablet_cell_bundles/default/@bundle_controller_target_config/memory_limits/versioned_chunk_meta"),
        }

        config["bundle_config"]["rpc_proxy_resource_guarantee"] = {
            "memory": get("//sys/tablet_cell_bundles/default/@bundle_controller_target_config/rpc_proxy_resource_guarantee/memory"),
            "net": get("//sys/tablet_cell_bundles/default/@bundle_controller_target_config/rpc_proxy_resource_guarantee/net"),
            "net_bytes": get("//sys/tablet_cell_bundles/default/@bundle_controller_target_config/rpc_proxy_resource_guarantee/net_bytes"),
            "type": get("//sys/tablet_cell_bundles/default/@bundle_controller_target_config/rpc_proxy_resource_guarantee/type"),
            "vcpu": get("//sys/tablet_cell_bundles/default/@bundle_controller_target_config/rpc_proxy_resource_guarantee/vcpu"),
        }

        config["bundle_config"]["tablet_node_resource_guarantee"] = {
            "memory": get("//sys/tablet_cell_bundles/default/@bundle_controller_target_config/tablet_node_resource_guarantee/memory"),
            "net": get("//sys/tablet_cell_bundles/default/@bundle_controller_target_config/tablet_node_resource_guarantee/net"),
            "net_bytes": get("//sys/tablet_cell_bundles/default/@bundle_controller_target_config/tablet_node_resource_guarantee/net_bytes"),
            "type": get("//sys/tablet_cell_bundles/default/@bundle_controller_target_config/tablet_node_resource_guarantee/type"),
            "vcpu": get("//sys/tablet_cell_bundles/default/@bundle_controller_target_config/tablet_node_resource_guarantee/vcpu"),
        }

        return config

    def _check_configs(self, expected, current):
        assert expected["bundle_name"] == current["bundle_name"]
        assert expected["bundle_config"]["rpc_proxy_count"] == current["bundle_config"]["rpc_proxy_count"]
        assert expected["bundle_config"]["tablet_node_count"] == current["bundle_config"]["tablet_node_count"]

        assert expected["bundle_config"]["cpu_limits"] == current["bundle_config"]["cpu_limits"]
        assert expected["bundle_config"]["memory_limits"] == current["bundle_config"]["memory_limits"]

        assert expected["bundle_config"]["rpc_proxy_resource_guarantee"] == current["bundle_config"]["rpc_proxy_resource_guarantee"]
        assert expected["bundle_config"]["tablet_node_resource_guarantee"] == current["bundle_config"]["tablet_node_resource_guarantee"]

    def _wait_for_bundle_controller_iterations(self, iterations=(1, 0), start: Tuple[int, int] | None = None, sleep_duration=0.1, fail_on_error=False):
        """
        Waits for `iterations` to be observed.
        """
        iterations_observed = (0, 0)
        if start is None:
            start = self._get_bundle_controller_iteration_count()
        while True:
            current = self._get_bundle_controller_iteration_count()
            diff = tuple(max(c - s, 0) for c, s in zip(current, start))
            iterations_observed = tuple(io + d for io, d in zip(iterations_observed, diff))
            if fail_on_error and current[1] > start[1]:
                raise Exception("Bundle controller iterations failed")
            start = current
            if all(actual >= need for actual, need in zip(iterations_observed, iterations)):
                break
            time.sleep(sleep_duration)
        pass

    def _get_bundle_controller_iteration_count(self):
        """
        Returns (success_iterations, error_iterations)
        """
        orchid_path = "//sys/bundle_controller/orchid/bundle_controller"
        iteration_count = get(f"{orchid_path}/state/iteration_count")
        successful = iteration_count["successful"]
        failed = iteration_count["failed"]
        return successful, failed

    def _move_nodes_to_spare_bundle(self):
        def get_node_config(bundle):
            return {
                "allocated": True,
                "allocated_for_bundle": bundle,
                "data_center": "default",
                "nanny_service": "yt_local_is_not_a_service_service",
                "resources": {},
                "yp_cluster": "dev_vm_is_not_a_cluster_cluster",
            }

        nodes = ls("//sys/cluster_nodes")
        for node in nodes:
            set(f"//sys/cluster_nodes/{node}/@bundle_controller_annotations", get_node_config("spare"))

    @authors("grachevkirill")
    def test_bundle_controller_just_works(self):
        assert exists("//sys/bundle_controller/controller/zones")
        self._fill_default_bundle()
        self._wait_for_bundle_controller_iterations((5, 0), sleep_duration=0.5)

    @authors("alexmipt")
    def test_bundle_controller_api_set_default_check(self):
        assert len(ls("//sys/cell_balancers/instances")) == self.NUM_CELL_BALANCERS
        assert exists("//sys/bundle_controller")
        self._fill_default_bundle()
        expected_config = self._get_cypress_config("default")

        # check solo field ("rpc_proxy_count")
        expected_config["bundle_config"]["rpc_proxy_count"] = 2
        update_config = {
            "bundle_name": "default",
            "bundle_config": {
                "rpc_proxy_count": 2,
            }
        }
        self._set_bundle_config(update_config)
        config = self._get_cypress_config("default")
        self._check_configs(expected_config, config)

        # check all fields set query
        expected_config["bundle_config"]["rpc_proxy_count"] = 1
        expected_config["bundle_config"]["tablet_node_count"] = 1
        expected_config["bundle_config"]["cpu_limits"] = {
            "lookup_thread_pool_size": 1,
            "query_thread_pool_size": 1,
            "write_thread_pool_size": 1,
        }
        expected_config["bundle_config"]["memory_limits"] = {
            "compressed_block_cache": 1073741824,
            "lookup_row_cache": 1073741824,
            "key_filter_block_cache": 1024,
            "reserved": 8589934592,
            "tablet_dynamic": 2147483648,
            "tablet_static": 4294967296,
            "uncompressed_block_cache": 1073741824,
            "versioned_chunk_meta": 2147483648,
        }
        expected_config["bundle_config"]["rpc_proxy_resource_guarantee"] = {
            "memory": 21474836480,
            "net": 545259520,
            "net_bytes": 545259520 // 8,
            "type": "small",
            "vcpu": 4000,
        }
        expected_config["bundle_config"]["tablet_node_resource_guarantee"] = {
            "memory": 21474836480,
            "net": 104857600,
            "net_bytes": 104857600 // 8,
            "vcpu": 4000,
            "type": "tiny",
        }
        self._set_bundle_config(expected_config)
        config = self._get_cypress_config("default")
        self._check_configs(expected_config, config)

    @authors("alexmipt")
    def test_bundle_controller_api_set_half_structured_check(self):
        assert len(ls("//sys/cell_balancers/instances")) == self.NUM_CELL_BALANCERS
        assert exists("//sys/bundle_controller")
        self._fill_default_bundle()
        expected_config = self._get_cypress_config("default")

        # check half-structed set query (for cpu_limits, memory_limits, rpc_proxy_resource_guarantee, tablet_node_resource_guarantee)
        update_config = {
            "bundle_name": "default",
            "bundle_config": {
                "cpu_limits": {
                    "query_thread_pool_size": 4,
                },
                "memory_limits": {
                    "compressed_block_cache": 8589934592,
                    "lookup_row_cache": 0,
                    "tablet_static": 42949672960,
                },
                "rpc_proxy_resource_guarantee": {
                    "memory": 21474836480,
                    "net": 545259520,
                    # "net_bytes" is not specified explicitly
                    "type": "small",
                    "vcpu": 4000
                },
                "tablet_node_resource_guarantee": {
                    "memory": 107374182400,
                    "net": 2684354560,
                    # "net_bytes" is not specified explicitly
                    "type": "medium",
                    "vcpu": 14000
                }
            }
        }
        expected_config["bundle_config"]["cpu_limits"]["query_thread_pool_size"] = 4
        expected_config["bundle_config"]["memory_limits"]["compressed_block_cache"] = 8589934592
        expected_config["bundle_config"]["memory_limits"]["lookup_row_cache"] = 0
        expected_config["bundle_config"]["memory_limits"]["tablet_static"] = 42949672960

        expected_config["bundle_config"]["rpc_proxy_resource_guarantee"]["memory"] = 21474836480
        expected_config["bundle_config"]["rpc_proxy_resource_guarantee"]["net"] = 545259520
        expected_config["bundle_config"]["rpc_proxy_resource_guarantee"]["net_bytes"] = 545259520 // 8
        expected_config["bundle_config"]["rpc_proxy_resource_guarantee"]["type"] = "small"
        expected_config["bundle_config"]["rpc_proxy_resource_guarantee"]["vcpu"] = 4000

        expected_config["bundle_config"]["tablet_node_resource_guarantee"]["memory"] = 107374182400
        expected_config["bundle_config"]["tablet_node_resource_guarantee"]["net"] = 2684354560
        expected_config["bundle_config"]["tablet_node_resource_guarantee"]["net_bytes"] = 2684354560 // 8
        expected_config["bundle_config"]["tablet_node_resource_guarantee"]["type"] = "medium"
        expected_config["bundle_config"]["tablet_node_resource_guarantee"]["vcpu"] = 14000

        self._set_bundle_config(update_config)
        config = self._get_cypress_config("default")
        self._check_configs(expected_config, config)

        update_config = {
            "bundle_name": "default",
            "bundle_config": {
                "cpu_limits": {
                    "query_thread_pool_size": 4,
                },
                "memory_limits": {
                    "compressed_block_cache": 8589934592,
                    "lookup_row_cache": 0,
                    "tablet_static": 42949672960,
                },
                "rpc_proxy_resource_guarantee": {
                    "memory": 21474836480,
                    "net_bytes": 198029589,
                    # "net": 198029589 * 8,
                    "type": "small",
                    "vcpu": 4000
                },
                "tablet_node_resource_guarantee": {
                    "memory": 107374182400,
                    "net_bytes": 92947848,
                    # "net": 92947848 * 8,
                    "type": "medium",
                    "vcpu": 14000
                }
            }
        }

        expected_config["bundle_config"]["rpc_proxy_resource_guarantee"]["net_bytes"] = 198029589
        expected_config["bundle_config"]["rpc_proxy_resource_guarantee"]["net"] = 198029589 * 8
        expected_config["bundle_config"]["tablet_node_resource_guarantee"]["net_bytes"] = 92947848
        expected_config["bundle_config"]["tablet_node_resource_guarantee"]["net"] = 92947848 * 8

        self._set_bundle_config(update_config)
        config = self._get_cypress_config("default")
        self._check_configs(expected_config, config)

    @authors("capone212")
    def test_bundle_controller_api_get(self):
        assert len(ls("//sys/cell_balancers/instances")) == self.NUM_CELL_BALANCERS
        assert exists("//sys/bundle_controller")
        self._fill_default_bundle()
        expected_config = self._get_cypress_config("default")
        expected_config["bundle_constraints"] = {
            "rpc_proxy_sizes": [
                {
                    "default_config": {
                        "cpu_limits": {},
                        "memory_limits": {}
                    },
                    "resource_guarantee": {
                        "memory": 21474836480,
                        "net": 545259520,
                        "net_bytes": 545259520 // 8,
                        "type": "small",
                        "vcpu": 4000
                    }
                }, {
                    "default_config": {
                        "cpu_limits": {},
                        "memory_limits": {}
                    },
                    "resource_guarantee": {
                        "memory": 21474836480,
                        "net": 1090519040,
                        "net_bytes": 1090519040 // 8,
                        "type": "medium",
                        "vcpu": 10000
                    }
                }
            ],
            "tablet_node_sizes": [
                {
                    "default_config": {
                        "cpu_limits": {
                            "lookup_thread_pool_size": 2,
                            "query_thread_pool_size": 2,
                            "write_thread_pool_size": 5
                        },
                        "memory_limits": {
                            "compressed_block_cache": 4294967296,
                            "lookup_row_cache": 1024,
                            "reserved": 10737418240,
                            "tablet_dynamic": 7516192768,
                            "tablet_static": 21474836480,
                            "uncompressed_block_cache": 4294967296,
                            "versioned_chunk_meta": 5368709120
                        }
                    },
                    "resource_guarantee": {
                        "memory": 53687091200,
                        "net": 1342177280,
                        "net_bytes": 1342177280 // 8,
                        "type": "small",
                        "vcpu": 7000
                    }
                }, {
                    "default_config": {
                        "cpu_limits": {
                            "lookup_thread_pool_size": 1,
                            "query_thread_pool_size": 1,
                            "write_thread_pool_size": 1
                        },
                        "memory_limits": {
                            "compressed_block_cache": 1073741824,
                            "lookup_row_cache": 1073741824,
                            "tablet_dynamic": 2147483648,
                            "reserved": 8589934592,
                            "tablet_static": 4294967296,
                            "uncompressed_block_cache": 1073741824,
                            "versioned_chunk_meta": 2147483648
                        }
                    },
                    "resource_guarantee": {
                        "memory": 21474836480,
                        "net": 104857600,
                        "net_bytes": 104857600 // 8,
                        "type": "tiny",
                        "vcpu": 4000
                    }
                }, {
                    "default_config": {
                        "cpu_limits": {
                            "lookup_thread_pool_size": 4,
                            "query_thread_pool_size": 4,
                            "write_thread_pool_size": 10
                        },
                        "memory_limits": {
                            "compressed_block_cache": 8589934592,
                            "lookup_row_cache": 0,
                            "reserved": 21474836480,
                            "tablet_dynamic": 15032385536,
                            "tablet_static": 42949672960,
                            "uncompressed_block_cache": 8589934592,
                            "versioned_chunk_meta": 10737418240
                        }
                    },
                    "resource_guarantee": {
                        "memory": 107374182400,
                        "net": 2684354560,
                        "net_bytes": 2684354560 // 8,
                        "type": "medium",
                        "vcpu": 14000
                    }
                }, {
                    "default_config": {
                        "cpu_limits": {
                            "lookup_thread_pool_size": 12,
                            "query_thread_pool_size": 12,
                            "write_thread_pool_size": 10
                        },
                        "memory_limits": {
                            "compressed_block_cache": 8589934592,
                            "lookup_row_cache": 1024,
                            "tablet_dynamic": 21474836480,
                            "tablet_static": 10737418240,
                            "uncompressed_block_cache": 8589934592,
                            "versioned_chunk_meta": 21474836480
                        }
                    },
                    "resource_guarantee": {
                        "memory": 107374182400,
                        "net": 5368709120,
                        "net_bytes": 5368709120 // 8,
                        "type": "cpu_intensive",
                        "vcpu": 28000
                    }
                }
            ]
        }
        expected_config["resource_quota"] = {
            "vcpu": 100000,
            "memory": 750323855360,
            "network": 0,
            "network_bytes": 0,
        }

        # check get query
        config = self._get_bundle_config("default")
        self._check_configs(expected_config, config)
        assert expected_config["bundle_constraints"]["rpc_proxy_sizes"] == config["bundle_constraints"]["rpc_proxy_sizes"]
        assert expected_config["bundle_constraints"]["tablet_node_sizes"] == config["bundle_constraints"]["tablet_node_sizes"]
        assert expected_config["resource_quota"] == config["resource_quota"]

    @authors("grachevkirill")
    def test_bundle_controller_skips_faulty_bundles_with_multiple_zones(self):
        bundle = "chaplin"

        self._move_nodes_to_spare_bundle()
        self._fill_default_bundle()
        self._create_bundle(bundle, bundle_controller_target_config={
            "tablet_node_count": 1,
            "cpu_limits": {
                "write_thread_pool_size": 3,
            },
        })
        self._wait_for_bundle_controller_iterations((5, 0))
        bundle_id = get(f"//sys/tablet_cell_bundles/{bundle}/@id")
        # Areas is a new mechanism for Chaos protocol. If a bundle has one area,
        # backward compatible mechanism is used. Assigning more than one area to
        # the bundle causes new mechanism (which is not handled by bundle
        # controller), and bundle contorller fails due to unexpected absence
        # node_tag_filter attribute.
        create_area(f"{bundle}-area", cell_bundle_id=bundle_id)
        self._wait_for_bundle_controller_iterations((2, 2))

    @authors("grachevkirill")
    def test_bundle_controller_skips_faulty_bundles_with_invalid_medium(self):
        BUNDLE = "chaplin"
        tablet_node_count = 1
        write_thread_pool_size = 3

        self._wait_for_bundle_controller_iterations((2, 0))

        self._move_nodes_to_spare_bundle()
        self._fill_default_bundle()
        self._create_bundle(BUNDLE, bundle_controller_target_config={
            "tablet_node_count": tablet_node_count,
            "cpu_limits": {
                "write_thread_pool_size": write_thread_pool_size,
            },
        })
        self._wait_for_bundle_controller_iterations((5, 0), fail_on_error=True)
        set(f"//sys/tablet_cell_bundles/{BUNDLE}/@options/snapshot_primary_medium", "invalid")
        set(f"//sys/tablet_cell_bundles/{BUNDLE}/@options/changelog_pirmary_medium", "invalid")
        self._wait_for_bundle_controller_iterations((3, 1))
