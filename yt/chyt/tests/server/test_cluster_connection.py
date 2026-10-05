import time

from yt_commands import authors, set

from base import ClickHouseTestBase, Clique

from yt.common import wait


class TestClusterConnectionDynamicConfig(ClickHouseTestBase):
    @authors("iharbychyk")
    def test_cluster_connection_dynamic_config_static_patch(self):
        with Clique(
            1,
            config_patch={
                "cluster_connection_dynamic_config_mode": "from_cluster_directory_with_static_patch",
                "cluster_connection": {"default_list_operations_timeout": 111111},
            },
        ) as clique:
            instance = clique.get_active_instances()[0]

            set("//sys/clusters/primary/default_list_operations_timeout", 222222)
            set("//sys/clusters/primary/default_get_tablet_errors_limit", 777)

            # A field with no static override picks up the cluster directory update as usual.
            wait(lambda: clique.get_orchid(
                instance, "/cluster_connection/dynamic_config/default_get_tablet_errors_limit") == 777)

            # A field statically pinned in the clique's own config wins over the cluster
            # directory update: PatchNode(dynamicConfigNode, staticClusterConnectionNode)
            # applies the static config on top, so the static value must persist.
            assert clique.get_orchid(
                instance, "/cluster_connection/dynamic_config/default_list_operations_timeout") == 111111

    @authors("iharbychyk")
    def test_cluster_connection_dynamic_config_disabled_by_default(self):
        with Clique(1) as clique:
            instance = clique.get_active_instances()[0]

            set("//sys/clusters/primary/default_list_operations_timeout", 434343)

            # Default policy is "from_static_config": no subscription is set up,
            # so the cluster directory update must never reach clique's orchid.
            time.sleep(2.0)
            assert clique.get_orchid(
                instance, "/cluster_connection/dynamic_config/default_list_operations_timeout") != 434343
