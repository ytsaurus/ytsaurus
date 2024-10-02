import logging
from typing import Any

from yt.yt_sync.core.client import YtClientProxy
from yt.yt_sync.core.model import YtTable

from .base import ActionBase

LOG = logging.getLogger("yt_sync")


class WaitInMemoryPreloadAction(ActionBase):
    def __init__(self, table: YtTable):
        super().__init__()
        self._table: YtTable = table

        self._scheduled: bool = False
        self._done: bool = False
        self._response: Any | None = None

    @property
    def is_awaiting(self) -> bool:
        return True

    def schedule_next(self, batch_client: YtClientProxy) -> bool:
        if self._done:
            return False
        self._scheduled = True
        if not self._table.is_in_memory:
            LOG.debug(
                "Check preload state for %s %s:%s: not in memory, nothing to wait",
                self._table.table_type,
                self._table.cluster_name,
                self._table.path,
            )
            self._done = True
            return False
        assert self._table.exists, "Can't perform checks for non-existing table"
        self._response = batch_client.get(f"{self._table.path}/@preload_state")
        return True

    def process(self):
        if self._done:
            return
        assert self._scheduled, "Can't process before schedule_next() call"
        if self.is_retryable_error(self._response):
            return
        self.assert_response(self._response)
        assert self._response
        result = str(self._response.get_result())
        LOG.debug(
            "Check preload state for %s %s:%s: expected=complete, actual=%s",
            self._table.table_type,
            self._table.cluster_name,
            self._table.path,
            result,
        )
        if "complete" == result:
            self._done = True
