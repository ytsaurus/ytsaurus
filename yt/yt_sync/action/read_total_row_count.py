from typing import Any

from yt.yt_sync.core.client import YtClientProxy
from yt.yt_sync.core.model import YtTable

from .base import ActionBase
from .helpers import parse_tablet_infos


class ReadTotalRowCountAction(ActionBase):
    def __init__(self, table: YtTable):
        assert table.is_ordered, f"Table {table.rich_path} must be ordered"
        assert (
            table.table_type == YtTable.Type.TABLE
        ), f"Table type {table.table_type} for {table.rich_path} is not supported"
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

        assert self._table.exists, f"Can't read total_row_count from non-existing table {self._table.rich_path}"
        assert (
            not self._table.tablet_state.is_mounted
        ), f"No use for reading total_row_count for mounted table {self._table.rich_path}"

        self._response = batch_client.get_tablet_infos(
            self._table.path, [i for i in range(self._table.effective_tablet_count)]
        )

        return False

    def process(self):
        assert self._scheduled
        tablet_infos = parse_tablet_infos(self._response)
        total_row_count: list[int] = list()
        for tablet_info in tablet_infos:
            total_row_count.append(int(tablet_info["total_row_count"]))
        assert len(total_row_count) == self._table.effective_tablet_count
        self._table.total_row_count = total_row_count
