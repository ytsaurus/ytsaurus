import logging
from typing import Any

from yt.yt_sync.core.client import YtClientProxy
from yt.yt_sync.core.model import YtTable

from .base import ActionBase

LOG = logging.getLogger("yt_sync")


class RemountTableAction(ActionBase):
    def __init__(self, table: YtTable):
        super().__init__()
        self._table: YtTable = table
        self._result: Any | None = None
        self._scheduled: bool = False

    def schedule_next(self, batch_client: YtClientProxy) -> bool:
        assert not self._scheduled, "Can't call schedule_next() more than one time"
        self._scheduled = True
        assert self._table.exists
        if self._table.need_remount is False:
            LOG.debug("Remount of table %s explicitly disabled, skip", self._table.rich_path)
            return False

        LOG.warning("Remount %s %s", self._table.table_type, self._table.rich_path)
        self._result = batch_client.remount_table(self._table.path)
        return False

    def process(self):
        assert self._scheduled, "Can't call process before schedule"
        if self._result is not None:
            self.assert_response(self._result)
        self._table.need_remount = None
