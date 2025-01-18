from typing import Any

from yt.yt_sync.core.client import YtClientProxy
from yt.yt_sync.core.model import YtTable

from .base import ActionBase


class WaitReplicasFlushedAction(ActionBase):
    def __init__(self, table: YtTable):
        assert table.is_replicated

        super().__init__()
        self._table: YtTable = table
        self._scheduled: bool = False
        self._done: bool = False
        self._responses: dict[str, Any] = dict()

    def schedule_next(self, batch_client: YtClientProxy) -> bool:
        if self._done:
            return False
        self._scheduled = True
        for replica in self._table.replicas.values():
            assert replica.replica_id
            self._responses[replica.replica_id] = batch_client.get(f"#{replica.replica_id}/@tablets")
        return True

    def process(self):
        assert self._scheduled, "Can't call 'process' before 'schedule_next"
        flushed: bool = True
        for response in self._responses.values():
            if self.is_retryable_error(response):
                flushed = False
                break
            self.assert_response(response)
            if not self._is_required_state(response.get_result()):
                flushed = False
                break
        if flushed:
            self._done = True
        self._responses.clear()

    @property
    def is_awaiting(self) -> bool:
        return True

    def _is_required_state(self, response: Any) -> bool:
        if self.dry_run:
            return True
        replica_tablets = list(response)
        for tablet in replica_tablets:
            if tablet["flushed_row_count"] != tablet["current_replication_row_index"]:
                return False
        return True
