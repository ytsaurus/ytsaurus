from yt.wrapper import yson

from deepmerge import always_merger
import copy
import os

from . import run_concurrent


_GB = 1024 * 1024 * 1024
_DEFAULT_CONFIG = {
    "enable_debug_logging": True,
    "wait_tablet_cell_initialization": True,
    "node_count": 2,
    "node_config": {
        "tablet_node": {
            "resource_limits": {
                "tablet_dynamic_memory": 2 * _GB,
                "tablet_static_memory": 8 * _GB,
            }
        }
    },
}


def _merge(base, patch):
    return always_merger.merge(copy.deepcopy(base), patch)


def start(
    yt_cluster_factory,
    cluster_names,
    package_dir,
    default_config=_DEFAULT_CONFIG,
    cluster_config_patches=None,
    config_patches=None,
    work_dir=None,
):
    """start a local yt clusters group"""

    config = default_config
    if config_patches is not None:
        for config_patch in config_patches:
            config = _merge(config, config_patch)

    cluster_configs = {x: config for x in cluster_names}
    if cluster_config_patches is not None:
        for cluster_name, patch in cluster_config_patches.items():
            cluster_configs[cluster_name] = _merge(cluster_configs[cluster_name], patch)

    clusters = {}
    for index, cluster_name in enumerate(sorted(cluster_configs, reverse=True)):
        cluster = yt_cluster_factory(cluster_name, index, work_dir, cluster_configs[cluster_name], package_dir)
        cluster.prepare_local_yt()
        clusters[cluster_name] = cluster

    run_concurrent(lambda c: c.start_local_yt(), list(clusters.values()))

    url_aliasing_config = yson.loads(os.environ.get("YT_PROXY_URL_ALIASING_CONFIG", "{}").encode("utf-8"))
    url_aliasing_config.update({cluster.yt_id: cluster.get_proxy_address() for cluster in clusters.values()})
    os.environ["YT_PROXY_URL_ALIASING_CONFIG"] = yson.dumps(url_aliasing_config).decode("utf-8")

    # TODO(nadya73): move this logic out of recipe.
    # Take first cluster as main cluster for queue_agent.
    queue_agent_cluster_name = next(iter(clusters.values())).yt_id if clusters else None

    def get_cluster_connection(cluster):
        client = cluster.get_yt_client()
        return client.get("//sys/@cluster_connection")

    def fill_queue_agent_state(cluster):
        cluster_connection = get_cluster_connection(cluster)

        consumer_registrations_path = "//sys/queue_agents/consumer_registrations"
        queue_consumer_registration_manager_config = cluster_connection \
            .setdefault("queue_agent", {}) \
            .setdefault("queue_consumer_registration_manager", {})

        queue_consumer_registration_manager_config.update({
            "state_write_path": "{cluster}:{path}".format(
                cluster=queue_agent_cluster_name,
                path=consumer_registrations_path,
            ),
            "state_read_path": '<clusters=["{cluster}"]>{path}'.format(
                cluster=queue_agent_cluster_name,
                path=consumer_registrations_path,
            ),
        })

        client = cluster.get_yt_client()
        client.set("//sys/@cluster_connection", cluster_connection)

    for cluster in clusters.values():
        fill_queue_agent_state(cluster)

    # Update //sys/clusters to union all cluster.
    cluster_connections = {
        cluster.yt_id : get_cluster_connection(cluster) for cluster in clusters.values()
    }

    for cluster in clusters.values():
        client = cluster.get_yt_client()
        client.set("//sys/clusters", cluster_connections)

    return clusters
