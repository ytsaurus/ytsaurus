import logging
from typing import Any

from yt.yt_sync.core.client import YtClientProxy
from yt.yt_sync.core.model import YtTable

from .base import ActionBase

LOG = logging.getLogger("yt_sync")


class RemoveReplicaAction(ActionBase):
    def __init__(self, replicated_table: YtTable, table: YtTable):
        assert replicated_table.is_replicated
        assert not table.is_replicated

        super().__init__()
        self._replicated_table: YtTable = replicated_table
        self._table: YtTable = table

        self._scheduled: bool = False
        self._response: Any | None = None

    def schedule_next(self, batch_client: YtClientProxy) -> bool:
        assert not self._scheduled
        self._scheduled = True
        replica = self._replicated_table.replicas.get(self._table.replica_key, None)
        assert replica
        assert replica.exists
        assert replica.replica_id
        assert not replica.enabled

        LOG.warning(
            "Remove replica #%s (%s:%s) for %s %s:%s",
            replica.replica_id,
            replica.cluster_name,
            replica.replica_path,
            self._replicated_table.table_type,
            self._replicated_table.cluster_name,
            self._replicated_table.path,
        )
        self._response = batch_client.remove(f"#{replica.replica_id}")

        return False

    def process(self):
        assert self._scheduled
        self.assert_response(self._response)
        replica = self._replicated_table.replicas[self._table.replica_key]
        replica.exists = False
        replica.attributes.pop("replication_lag_timestamp", None)
        replica.attributes.pop("replication_lag_time", None)
