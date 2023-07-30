from yt_env_setup import YTEnvSetup

from yt_commands import authors, wait, get, ls, set

from time import sleep

##################################################################


class TestClusterConnectionDynamicConfig(YTEnvSetup):
    ENABLE_HTTP_PROXY = True

    @authors("max42")
    def test_cluster_connection_dynamic_config(self):
        # Unfortunately current implementation relies on //sys/clusters rather than on //sys/@cluster_connection.
        set("//sys/clusters/primary/default_list_operations_timeout", 424242)

        http_proxy = ls("//sys/http_proxies")[0]
        wait(lambda:
             get(f"//sys/http_proxies/{http_proxy}/orchid/cluster_connection"
                 f"/dynamic_config/default_list_operations_timeout") == 424242)

        # For now, node is not configured to automatically update cluster connection.
        node = ls("//sys/cluster_nodes")[0]
        sleep(2.0)
        assert get(f"//sys/cluster_nodes/{node}/orchid/cluster_connection"
                   f"/dynamic_config/default_list_operations_timeout") != 424242
