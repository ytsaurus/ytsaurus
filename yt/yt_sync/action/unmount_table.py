import logging
from typing import Any

from yt.yt_sync.core.client import YtClientProxy
from yt.yt_sync.core.model import YtTable
from yt.yt_sync.core.model import YtTabletState

from .base import AwaitingResultAction

LOG = logging.getLogger("yt_sync")


class UnmountTableAction(AwaitingResultAction):
    def __init__(self, table: YtTable):
        super().__init__()
        self._table: YtTable = table

    def do_modify_request(self, batch_client: YtClientProxy) -> Any:
        if self._table.tablet_state.is_unmounted:
            LOG.info("Skip unmount table %s, already unmounted", self._table.rich_path)
            return None
        if self._table.table_type == YtTable.Type.CHAOS_REPLICATED_TABLE:
            LOG.info("Skip unmount table %s, chaotic one", self._table.rich_path)
            return None
        LOG.warning("Unmount table %s", self._table.rich_path)
        return batch_client.unmount_table(self._table.path)

    def do_await_request(self, batch_client: YtClientProxy) -> Any:
        return batch_client.get(f"{self._table.path}/@tablet_state")

    def is_required_state(self, result: Any) -> bool:
        LOG.debug(
            "%s %s tablet state: expected=%s, actual=%s",
            self._table.table_type,
            self._table.rich_path,
            YtTabletState.UNMOUNTED,
            str(result),
        )
        return YtTabletState.UNMOUNTED == str(result)

    def postprocess(self):
        self._table.tablet_state.set(YtTabletState.UNMOUNTED)
