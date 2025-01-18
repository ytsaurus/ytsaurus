import logging
from typing import Any

from yt.yt_sync.core.client import YtClientProxy
from yt.yt_sync.core.model import YtTable
from yt.yt_sync.core.model import YtTabletState

from .base import AwaitingResultAction

LOG = logging.getLogger("yt_sync")


class FreezeTableAction(AwaitingResultAction):
    def __init__(self, table: YtTable):
        super().__init__()
        self._table: YtTable = table

    def do_modify_request(self, batch_client: YtClientProxy) -> Any:
        if not self._table.tablet_state.is_mounted:
            LOG.info(
                "Skip freeze table %s, can freeze table only in state '%s' but got '%s'",
                self._table.rich_path,
                YtTabletState.MOUNTED,
                self._table.tablet_state.state,
            )
            return None
        LOG.warning("Freeze table %s:%s", self._table.cluster_name, self._table.path)
        return batch_client.freeze_table(self._table.path)

    def do_await_request(self, batch_client: YtClientProxy) -> Any:
        return batch_client.get(f"{self._table.path}/@tablet_state")

    def is_required_state(self, result: Any) -> bool:
        LOG.debug(
            "%s %s tablet state: expected=%s, actual=%s",
            self._table.table_type,
            self._table.rich_path,
            YtTabletState.FROZEN,
            str(result),
        )
        return YtTabletState.FROZEN == str(result)

    def postprocess(self):
        self._table.tablet_state.set(YtTabletState.FROZEN)
