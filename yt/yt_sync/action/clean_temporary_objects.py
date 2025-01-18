import logging

from yt.yt_sync.core.client import YtClientProxy
from yt.yt_sync.core.model import Types
from yt.yt_sync.core.model import YtCluster

from .base import ActionBase

LOG = logging.getLogger("yt_sync")


class CleanTemporaryObjectsAction(ActionBase):
    def __init__(self, yt_cluster: YtCluster):
        super().__init__()
        self._cluster: YtCluster = yt_cluster

    def schedule_next(self, _: YtClientProxy) -> bool:
        removed_tables: set[str] = set()
        for table_key, table in self._cluster.tables.items():
            if table.is_temporary and not table.exists:
                removed_tables.add(table_key)

            if table.is_replicated:
                removed_replicas: set[Types.ReplicaKey] = set()
                for replica_key, replica in table.replicas.items():
                    if replica.is_temporary and not replica.exists:
                        removed_replicas.add(replica_key)
                for replica_key in removed_replicas:
                    replica = table.replicas.pop(replica_key, None)
                    if replica:
                        LOG.debug(
                            "Remove from model temporary replica: #%s (%s:%s) for %s:%s",
                            replica.replica_id,
                            replica.cluster_name,
                            replica.replica_path,
                            table.cluster_name,
                            table.path,
                        )
        for table_key in removed_tables:
            table = self._cluster.tables.pop(table_key)
            LOG.debug(
                "Remove from model temporary table %s:%s",
                table.cluster_name,
                table.path,
            )

        return False

    def process(self):
        pass
