import logging
from typing import Any

from yt.yt_sync.core.client import YtClientProxy
from yt.yt_sync.core.model import YtTable

from .base import ActionBase

LOG = logging.getLogger("yt_sync")


class MoveTableAction(ActionBase):
    def __init__(self, from_table: YtTable, to_table: YtTable):
        assert from_table.cluster_name == to_table.cluster_name
        assert from_table.path != to_table.path

        super().__init__()
        self._from_table: YtTable = from_table
        self._to_table: YtTable = to_table

        self._scheduled: bool = False
        self._done: bool = False
        self._response: Any | None = None

    def schedule_next(self, batch_client: YtClientProxy) -> bool:
        if self._done:
            return False
        assert not self._scheduled, "Can't call schedule_next more than one time"
        self._scheduled = True
        assert (
            self._from_table.exists
        ), f"Can't move unexisting table {self._from_table.cluster_name}:{self._from_table.path}"
        LOG.warning(
            "Move %s %s:%s to new place %s:%s",
            self._from_table.table_type,
            self._from_table.cluster_name,
            self._from_table.path,
            self._to_table.cluster_name,
            self._to_table.path,
        )
        self._response = batch_client.move(self._from_table.path, self._to_table.path, force=True)
        return False

    def process(self):
        assert self._scheduled
        self.assert_response(self._response)
        self._from_table.exists = False
        self._to_table.exists = True
        self._to_table.tablet_state.set(None)
