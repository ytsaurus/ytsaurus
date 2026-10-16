from yt.common import wait

import random
import copy

DEFAULT_CHAOS_CONFIG = {
    "node_count": 3,
    "chaos_node_count": 1,
    "rpc_proxy_count": 1,
    "master_cache_count": 1,
    "clock_count": 1,
    "timestamp_provider_count": 1,
}

CHAOS_BUNDLE_NAME = "test-chaos"

_current_chaos_cell_tag = 100


def _generate_chaos_cell_tag():
    global _current_chaos_cell_tag
    _current_chaos_cell_tag += 1
    assert _current_chaos_cell_tag <= 10000
    return _current_chaos_cell_tag


def _format_uuid_part(value):
    return hex(value)[2:].rstrip("L")


def _generate_uuid_part(limit=2 ** 32):
    return _format_uuid_part(random.randint(0, limit - 1))


def generate_uuid():
    return "-".join([_generate_uuid_part() for _ in range(4)])


def _format_chaos_cell_id(cell_tag):
    # See IsWellKnownId.
    # EObjectType::ChaosCell == 1200
    return _generate_uuid_part(2 ** 16) + "-" + \
        _generate_uuid_part() + "-" + \
        _format_uuid_part(2 ** 16 * cell_tag + 1200) + "-" + \
        _generate_uuid_part()


def generate_chaos_cell_id():
    return _format_chaos_cell_id(_generate_chaos_cell_tag())


def _create_chaos_objects(
    object_type,
    attributes_template,
    peer_cluster_names,
    meta_cluster_names,
    clusters,
):
    object_ids = []

    def _create(cluster_name, attributes):
        yt_client = clusters[cluster_name].get_yt_client()
        object_id = yt_client.create(object_type, attributes=attributes)
        wait(
            lambda: yt_client.exists("#{}".format(object_id)) and yt_client.get("#{}/@life_stage".format(object_id)) == "creation_committed"
        )
        object_ids.append(object_id)

    for peer_id, cluster_name in enumerate(peer_cluster_names):
        attributes = copy.deepcopy(attributes_template)
        del attributes["chaos_options"]["peers"][peer_id]["alien_cluster"]
        _create(cluster_name, attributes)

    for cluster_name in meta_cluster_names:
        _create(cluster_name, attributes_template)

    return object_ids


def _create_chaos_cells(clusters):
    cell_id = generate_chaos_cell_id()

    cell_attributes = {
        "id": cell_id,
        "cell_bundle": CHAOS_BUNDLE_NAME,
        "area": "default",
    }

    for cluster in clusters.values():
        cluster.get_yt_client().create("chaos_cell", attributes=cell_attributes)

    def _are_cells_healthy():
        for cluster in clusters.values():
            health = cluster.get_yt_client().get("#{0}/@health".format(cell_id))
            if health != "good":
                return False
        return True

    wait(_are_cells_healthy, timeout=60)
    return cell_id


def patch_timestamp_providers_configs_and_restart(clusters):
    """Configure timestamp providers to use clock from primary cluster."""

    primary_cluster = clusters["primary"].get_yt_instance()
    primary_cluster_clock_config = primary_cluster.get_cluster_configuration()["clock"]

    alien_clock_configs = primary_cluster_clock_config[primary_cluster_clock_config["cell_tag"]]
    alien_clock_addresses = ["localhost:{}".format(clock_config["rpc_port"]) for clock_config in alien_clock_configs]

    for cluster_name, cluster in clusters.items():
        yt_instance = cluster.get_yt_instance()
        timestamp_provider_configs = yt_instance.get_cluster_configuration()["timestamp_provider"]
        for config in timestamp_provider_configs:
            config["clock_cluster_tag"] = yt_instance.yt_config.primary_cell_tag
            if cluster_name != "primary":
                config["alien_timestamp_providers"] = [
                    {
                        "clock_cluster_tag": primary_cluster.yt_config.primary_cell_tag,
                        "timestamp_provider": {
                            "addresses": alien_clock_addresses
                        },
                    }
                ]
        yt_instance.kill_service("timestamp_provider")
        yt_instance.rewrite_timestamp_provider_configs()
        yt_instance.start_timestamp_providers()

    def _are_tablet_cells_healthy():
        for cluster in clusters.values():
            tablet_cells = cluster.get_yt_client().list(
                "//sys/tablet_cells",
                attributes=["health"],
            )
            if not tablet_cells:
                return False
            for tablet_cell in tablet_cells:
                if tablet_cell.attributes["health"] != "good":
                    return False
        return True

    wait(_are_tablet_cells_healthy, timeout=60)


def configure_chaos(clusters):
    """Create and configure chaos entities."""

    for cluster in clusters.values():
        synchronizer_config = {
            "enable": True,
            "sync_period": 100,
            "full_sync_period": 200,
        }
        cluster.get_yt_client().set("//sys/@config/chaos_manager/alien_cell_synchronizer", synchronizer_config)
        discovery_config = {
            "peer_count": 1,
            "update_period": 100,
            "node_tag_filter": "master_cache"
        }
        cluster.get_yt_client().set("//sys/@config/node_tracker/master_cache_manager", discovery_config)
        nodes = cluster.get_yt_client().list("//sys/cluster_nodes", attributes=["flavors"])
        chaos_nodes = [node for node in nodes if "chaos" in node.attributes["flavors"]]
        for node in chaos_nodes:
            cluster.get_yt_client().set("//sys/cluster_nodes/{0}/@user_tags/end".format(node), "chaos_cache")

    cluster_names = clusters.keys()

    primary_client = clusters["primary"].get_yt_client()
    primary_cell_tag = primary_client.get("//sys/@primary_cell_tag")

    bundle_attributes_template = {
        "name": CHAOS_BUNDLE_NAME,
        "chaos_options": {
            "peers": [{"alien_cluster": cluster_name} for cluster_name in cluster_names],
        },
        "options": {
            "changelog_account": "sys",
            "snapshot_account": "sys",
            "peer_count": len(cluster_names),
            "independent_peers": True,
            "clock_cluster_tag": primary_cell_tag
        }
    }
    _create_chaos_objects("chaos_cell_bundle", bundle_attributes_template, cluster_names, [], clusters)
    chaos_cell_id = _create_chaos_cells(clusters)

    primary_client.set("//sys/chaos_cell_bundles/{}/@metadata_cell_id".format(CHAOS_BUNDLE_NAME), chaos_cell_id)
