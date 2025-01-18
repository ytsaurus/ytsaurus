import logging
from typing import Any

from yt.yt_sync.core.client import YtClientProxy
from yt.yt_sync.core.model import YtReplica
from yt.yt_sync.core.model import YtTable

from .base import ActionBase

LOG = logging.getLogger("yt_sync")


class WaitChaosReplicationLagAction(ActionBase):
    def __init__(self, replicated_table: YtTable, table: YtTable):
        super().__init__()
        self._replicated: YtTable = replicated_table
        self._table: YtTable = table

        self._timestamp: int | None = None
        self._scheduled: bool = False
        self._done: bool = False
        self._response: Any | None = None

    @property
    def is_awaiting(self) -> bool:
        return True

    def schedule_next(self, batch_client: YtClientProxy) -> bool:
        if self._done:
            return False
        if not self._scheduled:
            assert self._replicated.is_chaos_replicated
            assert not self._table.is_replicated
            self._scheduled = True
        replica = self._replicated.replicas.get(self._table.replica_key)
        assert replica
        assert replica.exists
        assert replica.replica_id
        assert YtReplica.ContentType.DATA == replica.content_type

        self._generate_timestamp(batch_client)
        self._response = batch_client.get(
            f"{self._replicated.path}/@replicas/{replica.replica_id}/replication_lag_timestamp"
        )
        return True

    def process(self):
        assert self._scheduled, "Can't call 'process' before 'schedule_next"
        if self.is_retryable_error(self._response):
            return
        self.assert_response(self._response)
        self._done = self._is_required_state()

    def _generate_timestamp(self, batch_client: YtClientProxy):
        if self._timestamp is not None:
            return
        yt_client = batch_client.underlying_client_proxy
        assert yt_client
        self._timestamp = yt_client.generate_timestamp()

    def _is_required_state(self) -> bool:
        if self.dry_run:
            return True
        assert self._timestamp
        assert self._response

        # debug output
        replica = self._replicated.replicas.get(self._table.replica_key)
        assert replica
        assert replica.replica_id

        LOG.debug(
            "WaitChaosReplicationLagAction: replica #%s (%s:%s) replication_lag_timestamp actual=%s, desired=%s",
            replica.replica_id,
            replica.cluster_name,
            replica.replica_path,
            self._response.get_result(),
            self._timestamp,
        )
        return int(self._response.get_result()) >= self._timestamp
