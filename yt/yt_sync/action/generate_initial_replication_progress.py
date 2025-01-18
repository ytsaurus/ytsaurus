import logging

import yt.yson as yson
from yt.yt_sync.core.client import YtClientProxy
from yt.yt_sync.core.model import YtTable

from .base import ActionBase

LOG = logging.getLogger("yt_sync")
MAX_KEY = [yson.to_yson_type(None, attributes={"type": "max"})]


class GenerateInitialReplicationProgressAction(ActionBase):
    def __init__(self, table: YtTable):
        self._table: YtTable = table

        self._scheduled: bool = False
        self._done: bool = False

    def schedule_next(self, batch_client: YtClientProxy) -> bool:
        if self._done:
            return False
        assert not self._scheduled, "Can't call schedule_next more than one time"
        self._scheduled = True

        if self._table.chaos_replication_progress is not None:
            LOG.debug("Table %s already has replication progress, skip", self._table.rich_path)
            self._done = True
            return False

        yt_client = batch_client.underlying_client_proxy
        assert yt_client
        ts = yt_client.generate_timestamp()

        self._table.chaos_replication_progress = {
            "segments": [{"lower_key": [], "timestamp": ts}],
            "upper_key": MAX_KEY,
        }
        LOG.debug(
            "Generate replication progress for %s: progress=%s",
            self._table.rich_path,
            self._table.chaos_replication_progress,
        )
        return False

    def process(self):
        assert self._scheduled
        self._done = True
