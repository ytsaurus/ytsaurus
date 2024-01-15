from yt_env_setup import YTEnvSetup
import yt.yson as yson
import yt.packages.requests as requests
from yt_commands import authors, ls, exists, set, get


##################################################################


class TestBundleController(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_CELL_BALANCERS = 3
    NUM_HTTP_PROXIES = 1
    NUM_RPC_PROXIES = 1
    ENABLE_BUNDLE_CONTROLLER = True
    USE_DYNAMIC_TABLES = True
    ENABLE_HTTP_PROXY = True
    ENABLE_RPC_PROXY = True

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

    def _fill_default_bundle(self):
        set("//sys/tablet_cell_bundles/default/@bundle_controller_target_config", {})
        set("//sys/tablet_cell_bundles/default/@bundle_controller_target_config/cpu_limits", {
            "lookup_thread_pool_size": 16,
            "query_thread_pool_size": 4,
            "write_thread_pool_size": 10})

        set("//sys/tablet_cell_bundles/default/@bundle_controller_target_config/memory_limits", {
            "compressed_block_cache": 17179869184,
            "key_filter_block_cache": 1024,
            "lookup_row_cache": 1024,
            "tablet_dynamic": 10737418240,
            "tablet_static": 10737418240,
            "uncompressed_block_cache": 17179869184,
            "versioned_chunk_meta": 10737418240})

        set("//sys/tablet_cell_bundles/default/@bundle_controller_target_config/rpc_proxy_count", 6)
        set("//sys/tablet_cell_bundles/default/@bundle_controller_target_config/tablet_node_count", 1)

        set("//sys/tablet_cell_bundles/default/@bundle_controller_target_config/rpc_proxy_resource_guarantee", {
            "memory": 21474836480,
            "net": 1090519040,
            "type": "medium",
            "vcpu": 10000})

        set("//sys/tablet_cell_bundles/default/@bundle_controller_target_config/tablet_node_resource_guarantee", {
            "memory": 107374182400,
            "net": 5368709120,
            "type": "cpu_intensive",
            "vcpu": 28000})

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
            "lookup_row_cache": get("//sys/tablet_cell_bundles/default/@bundle_controller_target_config/memory_limits/lookup_row_cache"),
            "tablet_dynamic": get("//sys/tablet_cell_bundles/default/@bundle_controller_target_config/memory_limits/tablet_dynamic"),
            "tablet_static": get("//sys/tablet_cell_bundles/default/@bundle_controller_target_config/memory_limits/tablet_static"),
            "uncompressed_block_cache": get("//sys/tablet_cell_bundles/default/@bundle_controller_target_config/memory_limits/uncompressed_block_cache"),
            "versioned_chunk_meta": get("//sys/tablet_cell_bundles/default/@bundle_controller_target_config/memory_limits/versioned_chunk_meta"),
        }

        config["bundle_config"]["rpc_proxy_resource_guarantee"] = {
            "memory": get("//sys/tablet_cell_bundles/default/@bundle_controller_target_config/rpc_proxy_resource_guarantee/memory"),
            "net": get("//sys/tablet_cell_bundles/default/@bundle_controller_target_config/rpc_proxy_resource_guarantee/net"),
            "type": get("//sys/tablet_cell_bundles/default/@bundle_controller_target_config/rpc_proxy_resource_guarantee/type"),
            "vcpu": get("//sys/tablet_cell_bundles/default/@bundle_controller_target_config/rpc_proxy_resource_guarantee/vcpu"),
        }

        config["bundle_config"]["tablet_node_resource_guarantee"] = {
            "memory": get("//sys/tablet_cell_bundles/default/@bundle_controller_target_config/tablet_node_resource_guarantee/memory"),
            "net": get("//sys/tablet_cell_bundles/default/@bundle_controller_target_config/tablet_node_resource_guarantee/net"),
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
        expected_config["bundle_config"]["rpc_proxy_count"] = 5
        expected_config["bundle_config"]["tablet_node_count"] = 5
        expected_config["bundle_config"]["cpu_limits"] = {
            "lookup_thread_pool_size": 5,
            "query_thread_pool_size": 5,
            "write_thread_pool_size": 5,
        }
        expected_config["bundle_config"]["memory_limits"] = {
            "compressed_block_cache": 5,
            "key_filter_block_cache": 5,
            "lookup_row_cache": 5,
            "tablet_dynamic": 5,
            "tablet_static": 5,
            "uncompressed_block_cache": 5,
            "versioned_chunk_meta": 5,
        }
        expected_config["bundle_config"]["rpc_proxy_resource_guarantee"] = {
            "memory": 5,
            "net": 5,
            "type": "kek",
            "vcpu": 5,
        }
        expected_config["bundle_config"]["tablet_node_resource_guarantee"] = {
            "memory": 5,
            "net": 5,
            "type": "kek",
            "vcpu": 5,
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
                    "query_thread_pool_size": 15,
                },
                "memory_limits": {
                    "compressed_block_cache": 15,
                    "lookup_row_cache": 15,
                    "tablet_static": 15,
                },
                "rpc_proxy_resource_guarantee": {
                    "memory": 15,
                    "type": "lol",
                    "vcpu": 15,
                },
                "tablet_node_resource_guarantee": {
                    "memory": 15,
                    "type": "lol",
                    "vcpu": 15,
                }
            }
        }
        expected_config["bundle_config"]["cpu_limits"]["query_thread_pool_size"] = 15
        expected_config["bundle_config"]["memory_limits"]["compressed_block_cache"] = 15
        expected_config["bundle_config"]["memory_limits"]["lookup_row_cache"] = 15
        expected_config["bundle_config"]["memory_limits"]["tablet_static"] = 15

        expected_config["bundle_config"]["rpc_proxy_resource_guarantee"]["memory"] = 15
        expected_config["bundle_config"]["rpc_proxy_resource_guarantee"]["type"] = "lol"
        expected_config["bundle_config"]["rpc_proxy_resource_guarantee"]["vcpu"] = 15

        expected_config["bundle_config"]["tablet_node_resource_guarantee"]["memory"] = 15
        expected_config["bundle_config"]["tablet_node_resource_guarantee"]["type"] = "lol"
        expected_config["bundle_config"]["tablet_node_resource_guarantee"]["vcpu"] = 15

        self._set_bundle_config(update_config)
        config = self._get_cypress_config("default")
        self._check_configs(expected_config, config)

    @authors("capone212")
    def test_bundle_controller_api_get(self):
        assert len(ls("//sys/cell_balancers/instances")) == self.NUM_CELL_BALANCERS
        assert exists("//sys/bundle_controller")
        self._fill_default_bundle()
        expected_config = self._get_cypress_config("default")

        # check get query
        config = self._get_bundle_config("default")
        self._check_configs(expected_config, config)
