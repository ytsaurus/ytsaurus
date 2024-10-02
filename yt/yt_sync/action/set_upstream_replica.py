import logging
from typing import Any

from yt.yt_sync.core.client import YtClientProxy
from yt.yt_sync.core.model import YtReplica
from yt.yt_sync.core.model import YtTable

from .base import ActionBase

LOG = logging.getLogger("yt_sync")


class SetUpstreamReplicaAction(ActionBase):
    def __init__(self, actual_table: YtTable, actual_replicated_table: YtTable):
        super().__init__()
        self._table = actual_table
        self._replicated_table = actual_replicated_table
        self._result: Any | None = None
        self._scheduled: bool = False

    def schedule_next(self, batch_client: YtClientProxy) -> bool:
        assert not self._scheduled, "Can't call schedule_next() more than one time"
        assert not self._table.is_replicated
        assert self._replicated_table.is_replicated
        self._scheduled = True
        replica = self._get_replica()
        assert replica and replica.replica_id, f"Can't set upstream_replica_id for unexisting replica={replica}"
        LOG.warning(
            "Alter %s %s:%s: upstream_replica_id=%s",
            self._table.table_type,
            self._table.cluster_name,
            self._table.path,
            replica.replica_id,
        )
        self._result = batch_client.alter_table(self._table.path, upstream_replica_id=replica.replica_id)
        return False

    def process(self):
        assert self._scheduled, "Can't call process before schedule"
        self.assert_response(self._result)
        replica = self._get_replica()
        assert replica
        self._table.attributes["upstream_replica_id"] = replica.replica_id

    def _get_replica(self) -> YtReplica | None:
        return self._replicated_table.replicas.get(self._table.replica_key)
