import logging
from typing import Any

from yt.yt_sync.core.client import YtClientProxy
from yt.yt_sync.core.model import YtTable

from .base import ActionBase

LOG = logging.getLogger("yt_sync")


class ReadReplicationProgressAction(ActionBase):
    def __init__(self, table: YtTable):
        assert not table.is_replicated, f"Can't fetch replication progress from replicated table {table.rich_path}"
        super().__init__()
        self._table: YtTable = table

        self._scheduled: bool = False
        self._done: bool = False
        self._response: Any | None = None

    def schedule_next(self, batch_client: YtClientProxy) -> bool:
        if self._done:
            return False
        assert not self._scheduled, "Can't call schedule_next more than one time"
        self._scheduled = True

        assert self._table.exists, f"Can't read replication_progress from non-existing table {self._table.rich_path}"
        assert (
            self._table.tablet_state.is_unmounted
        ), f"No use for reading replication_progress for mounted table {self._table.rich_path}"

        self._response = batch_client.get(f"{self._table.path}/@replication_progress")

        return False

    def process(self):
        assert self._scheduled
        assert self._response
        self.assert_response(self._response)
        self._table.chaos_replication_progress = self._response.get_result()
        LOG.info("Got %s/@replication_progress=%s", self._table.rich_path, self._table.chaos_replication_progress)
