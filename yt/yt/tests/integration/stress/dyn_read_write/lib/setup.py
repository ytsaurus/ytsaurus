from time import sleep

import yt.wrapper as yt

from .log import logger
from .common import create_client


def setup_cluster(client):
    client.set("//sys/@config/tablet_manager/use_avenues", True)

    if not client.exists("//sys/cluster_nodes/@config/%true/tablet_node/smooth_movement_tracker"):
        client.set(
            "//sys/cluster_nodes/@config/%true/tablet_node",
            {"smooth_movement_tracker": {"testing": {
                "delay_after_stage_at_source": {
                    "target_allocated": 500,
                    "servant_switched": 0,
                    "waiting_for_locks_before_activation": 200,
                },
                "delay_after_stage_at_target": {
                    "target_activated": 1000,
                },
            }}}
        )
    client.set("//sys/@cluster_connection/queue_agent/queue_consumer_registration_manager/cache_refresh_period", 10**16)
    if client.get("//sys/tablet_cell_bundles/default/@options/snapshot_replication_factor") != 2:
        client.set("//sys/tablet_cell_bundles/default/@options/snapshot_replication_factor", 2)

    ns = client.list("//sys/cluster_nodes", attributes=["disable_tablet_cells"])
    for n in ns:
        if n.attributes["disable_tablet_cells"]:
            logger.info(f"Enabled tablet cells at node {n}")
            client.set(f"//sys/cluster_nodes/{n}/@disable_tablet_cells", False)


def setup_cells(cell_count, client):
    current_cell_count = client.get("//sys/tablet_cells/@count")
    if current_cell_count <= cell_count:
        logger.info("Creating tablet cells")
        for i in range(cell_count - current_cell_count):
            client.create("tablet_cell", attributes={"tablet_cell_bundle": "default"})
    else:
        logger.error("You probably want to start without --start-over flag")
        exit(1)

    while True:
        if all(c.attributes["health"] == "good" for c in client.list("//sys/tablet_cells", attributes=["health"])):
            break
        sleep(0.5)
    logger.info("Tablet cells OK")


def setup_sys_clusters(clusters: list[(str, yt.YtClient)]):
    for src_name, src_client in clusters:
        for dst_name, dst_client in clusters:
            if src_name == dst_name:
                continue
            dst_client.set(f"//sys/clusters/{src_name}", src_client.get("//sys/@cluster_connection"))
            dst_client.set(f"//sys/clusters/{src_name}/queue_agent/queue_consumer_registration_manager/cache_refresh_period", 10**16)
            logger.info(f'Registered cluster "{src_name}" at "{dst_name}"')


def setup_chaos_cluster(client):
    for node in client.list("//sys/chaos_nodes"):
        client.set(f"//sys/cluster_nodes/{node}/@user_tags/end", "chaos_cache")

    client.set("//sys/@config/chaos_manager/alien_cell_synchronizer", {
        "enable": True,
        "sync_period": 100,
        "full_sync_period": 200,
    })

    logger.info("Chaos cluster setup done")


def setup_long_cache_at_rpc_proxies(client):
    table_mount_cache_config = {
        "expire_after_successful_update_time": 60000,
        "refresh_time": 60000,
        "expiration_period": 60000,
        "expire_after_failed_update_time": 20000,
        "expire_after_access_time": 300000,
    }
    replication_card_cache = {
        "expire_after_successful_update_time": 60000,
        "expire_after_failed_update_time": 60000,
        "expire_after_access_time": 60000,
        "refresh_time": 100000,
        "expiration_period": 100000,
        "soft_backoff_time": 20000,
        "hard_backoff_time":  20000,
    }
    client.set("//sys/rpc_proxies/@config/cluster_connection/table_mount_cache", table_mount_cache_config)
    client.set("//sys/rpc_proxies/@config/cluster_connection/replication_card_cache", replication_card_cache)
    client.set("//sys/rpc_proxies/@config/cluster_connection/local_tablet_write_retry_count", 3)
    client.set("//sys/rpc_proxies/@config/cluster_connection/use_uniform_prepare_signatures", True)


def setup_clusters(mode, proxy, replica_proxy, local_cell_count, replica_cell_count):
    client = create_client(proxy=proxy)
    cluster_name = client.get("//sys/@cluster_connection/cluster_name")

    logger.info("Setting up cluster {cluster_name}")
    setup_cluster(client)
    setup_cells(local_cell_count, client)
    setup_long_cache_at_rpc_proxies(client)

    if mode == "chaos":
        logger.info("Setting up chaos for cluster {cluster_name}")
        setup_chaos_cluster(client)

    if mode == "replicated":
        replica_client = create_client(proxy=replica_proxy)
        replica_cluster_name = replica_client.get("//sys/@cluster_connection/cluster_name")

        logger.info("Setting up cluster {replica_cluster_name}")

        setup_cluster(replica_client)
        setup_cells(replica_cell_count, replica_client)
        setup_long_cache_at_rpc_proxies(replica_client)
        setup_sys_clusters((
            (cluster_name, client),
            (replica_cluster_name, replica_client)))
