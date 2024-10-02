import logging

from yt.yt_sync.core.client import YtClientProxy
from yt.yt_sync.core.model import YtTable

from .base import ActionBase

LOG = logging.getLogger("yt_sync")


class CopyReplicationProgressAction(ActionBase):
    def __init__(self, from_table: YtTable, to_table: YtTable):
        super().__init__()

        self._from_table: YtTable = from_table
        self._to_table: YtTable = to_table

        self._scheduled: bool = False
        self._done: bool = False

    def schedule_next(self, batch_client: YtClientProxy) -> bool:
        if self._done:
            return False
        assert not self._scheduled
        self._scheduled = True
        assert self._from_table.exists
        assert self._to_table.exists

        yt_client = batch_client.underlying_client_proxy
        assert yt_client
        replication_progress = yt_client.get(f"{self._from_table.path}/@replication_progress")
        LOG.warning(
            "Alter %s %s:%s: replication_progress=%s",
            self._to_table.table_type,
            self._to_table.cluster_name,
            self._to_table.path,
            replication_progress,
        )
        yt_client.alter_table(self._to_table.path, replication_progress=replication_progress)
        return False

    def process(self):
        assert self._scheduled
        self._done = True
