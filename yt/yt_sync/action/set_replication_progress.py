from copy import deepcopy
import logging
from typing import Any

from yt.yt_sync.core.client import YtClientProxy
from yt.yt_sync.core.model import YtTable

from .base import ActionBase

LOG = logging.getLogger("yt_sync")


class SetReplicationProgressAction(ActionBase):
    def __init__(self, src_table: YtTable, dst_table: YtTable):
        assert not src_table.is_replicated, f"Source table {src_table.rich_path} can't be replicated"
        assert not dst_table.is_replicated, f"Destination table {dst_table.rich_path} can't be replicated"
        super().__init__()

        self._src_table: YtTable = src_table
        self._dst_table: YtTable = dst_table

        self._scheduled: bool = False
        self._done: bool = False
        self._response: Any | None = None

    def schedule_next(self, batch_client: YtClientProxy) -> bool:
        if self._done:
            return False
        assert not self._scheduled, "Can't call schedule_next more than one time"
        self._scheduled = True

        assert (
            self._src_table.chaos_replication_progress is not None
        ), f"'replication_progress' is empty in model of source table {self._src_table.rich_path}"
        assert (
            self._dst_table.exists
        ), f"Can't set replication_progress to non-existing table {self._dst_table.rich_path}"

        LOG.warning(
            "Alter %s %s: replication_progress=%s",
            self._dst_table.table_type,
            self._dst_table.rich_path,
            self._src_table.chaos_replication_progress,
        )
        self._response = batch_client.alter_table(
            self._dst_table.path, replication_progress=self._src_table.chaos_replication_progress
        )
        return False

    def process(self):
        assert self._scheduled
        assert self._response
        self.assert_response(self._response)
        self._dst_table.chaos_replication_progress = deepcopy(self._src_table.chaos_replication_progress)
