from yt_env_setup import YTEnvSetup
import yt.yson as yson
import yt.packages.requests as requests
from yt_commands import authors, ls, exists, set


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

    def _set_default_bundle(self):
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

    @authors("capone212")
    def test_bundle_controller_api(self):
        assert len(ls("//sys/cell_balancers/instances")) == self.NUM_CELL_BALANCERS
        assert exists("//sys/bundle_controller")
        self._set_default_bundle()

        config = self._get_bundle_config("default")
        assert config["bundle_name"] == "default"
        assert config["rpc_proxy_count"] == 6
        assert config["tablet_node_count"] == 1

        assert config["cpu_limits"]["lookup_thread_pool_size"] == 16
        assert config["cpu_limits"]["query_thread_pool_size"] == 4
        assert config["cpu_limits"]["write_thread_pool_size"] == 10

        assert config["memory_limits"]["compressed_block_cache"] == 17179869184
        assert config["memory_limits"]["key_filter_block_cache"] == 1024
        assert config["memory_limits"]["lookup_row_cache"] == 1024
        assert config["memory_limits"]["tablet_dynamic"] == 10737418240
        assert config["memory_limits"]["tablet_static"] == 10737418240
        assert config["memory_limits"]["uncompressed_block_cache"] == 17179869184
        assert config["memory_limits"]["versioned_chunk_meta"] == 10737418240

        assert config["rpc_proxy_resource_guarantee"]["memory"] == 21474836480
        assert config["rpc_proxy_resource_guarantee"]["net"] == 1090519040
        assert config["rpc_proxy_resource_guarantee"]["type"] == "medium"
        assert config["rpc_proxy_resource_guarantee"]["vcpu"] == 10000

        assert config["tablet_node_resource_guarantee"]["memory"] == 107374182400
        assert config["tablet_node_resource_guarantee"]["net"] == 5368709120
        assert config["tablet_node_resource_guarantee"]["type"] == "cpu_intensive"
        assert config["tablet_node_resource_guarantee"]["vcpu"] == 28000
