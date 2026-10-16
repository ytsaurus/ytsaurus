from .create import create_sorted_table, make_schema
from .log import get_logger

logger = get_logger()


def create_replicated_table_with_replicas(
    replicated_client, replica_client, replica_cluster_name, table_path
):
    replicated_client.create("replicated_table", table_path, attributes={
        "dynamic": True,
        "schema": make_schema(with_hash=True),
        "enable_replication_logging": True,
        "replicated_table_options": {
            "enable_replicated_table_tracker": False,
        },
    }, force=True)

    for mode in ["sync", "async"]:
        replica_path = f"{table_path}_{mode}"

        replica_id = replicated_client.create("table_replica", attributes={
            "table_path": table_path,
            "cluster_name": replica_cluster_name,
            "replica_path": replica_path,
            "mode": mode,
        })

        create_sorted_table(
            replica_path,
            attributes={"upstream_replica_id": replica_id},
            schema_attributes={"with_hash": True},
            client=replica_client, force=True, ignore_existing=True)

        replica_client.reshard_table(replica_path, tablet_count=10, uniform=True)
        replica_client.mount_table(replica_path, sync=True)

        replicated_client.alter_table_replica(replica_id, enabled=True)

    replicated_client.reshard_table(table_path, tablet_count=10, uniform=True)
    replicated_client.mount_table(table_path, sync=True)

    logger.info(f"Created replicated table {table_path} with sync/async replicas on {replica_cluster_name}")
