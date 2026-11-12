from yt.wrapper import yson

from deepmerge import always_merger
import copy
import os
import yatest.common

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


def _merge_cluster_config(base, patch):
    """Deep-merge cluster configs, matching component patches by component name."""
    base = copy.deepcopy(base)
    patch = copy.deepcopy(patch)
    if isinstance(base.get("components"), list) and isinstance(patch.get("components"), list):
        component_patches = patch.pop("components")
        if not component_patches:
            # An explicit empty list disables all components for this cluster.
            base["components"] = []
        else:
            # Patch existing components by name, preserving the other services.
            components = base["components"]
            for component_patch in component_patches:
                name = component_patch.get("name")
                component = next((item for item in components if name is not None and item.get("name") == name), None)
                if component is None:
                    components.append(component_patch)
                else:
                    always_merger.merge(component, component_patch)
    return always_merger.merge(base, patch)


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
            config = _merge_cluster_config(config, config_patch)

    cluster_configs = {x: config for x in cluster_names}
    if cluster_config_patches is not None:
        for cluster_name, patch in cluster_config_patches.items():
            cluster_configs[cluster_name] = _merge_cluster_config(cluster_configs[cluster_name], patch)

    for cluster_name in sorted(cluster_configs, reverse=True):
        for component in cluster_configs[cluster_name].get("components", []):
            if component.get("name") != "yql-agent":
                continue

            yqla_path = yatest.common.runtime.work_path("yql_agent_artifacts")
            component_config = component.setdefault("config", {})
            component_config.setdefault("path", yqla_path)
            component_config.setdefault("artifacts_path", yqla_path)
            component_config.setdefault("enable_qtworker", True)
            if component_config["enable_qtworker"]:
                component_config.setdefault("qtworker_path", os.path.join(yqla_path, "qtworker"))
                component_config.setdefault("qtworker_udf_resolver_path", os.path.join(yqla_path, "udf_resolver"))
                component_config.setdefault("qtworker_udf_dep_stub_path", os.path.join(yqla_path, "libyql_udf_dep_stub.so"))

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

    # Update //sys/clusters to unite all clusters.
    cluster_connections = {
        cluster.yt_id : get_cluster_connection(cluster) for cluster in clusters.values()
    }

    for cluster in clusters.values():
        client = cluster.get_yt_client()
        client.set("//sys/clusters", cluster_connections)

    return clusters
