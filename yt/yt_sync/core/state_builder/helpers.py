from yt.yt_sync.core.client import YtClientFactory
from yt.yt_sync.core.model import YtCluster


def get_cluster_name(yt_client_factory: YtClientFactory, cluster_name: str) -> str:
    name, address = YtClientFactory.split_name(cluster_name)
    if address:
        yt_client_factory.add_alias(name, address)
    return name


def link_chaos_tables(desired_cluster: YtCluster, yt_cluster: YtCluster):
    for table_key, table in yt_cluster.tables.items():
        src_table = desired_cluster.tables[table_key]
        if src_table.is_chaos_replication_log_required:
            assert src_table.chaos_replication_log, f"No chaos replication log for {src_table.rich_path}"
            table.chaos_replication_log = src_table.chaos_replication_log
            replication_log = yt_cluster.tables[src_table.chaos_replication_log]
            replication_log.chaos_data_table = table_key
