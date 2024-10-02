from copy import deepcopy
import logging
from typing import Any

from yt.yt_sync.core.client import YtClientProxy
from yt.yt_sync.core.model import YtTable

from .base import ActionBase
from .helpers import reset_actual_table_after_create

LOG = logging.getLogger("yt_sync")


class CreateOrderedTableFromAction(ActionBase):
    def __init__(self, desired_table: YtTable, actual_table: YtTable, source_table: YtTable):
        assert desired_table.table_type == YtTable.Type.TABLE
        assert desired_table.is_ordered
        assert source_table.table_type == YtTable.Type.TABLE
        assert source_table.is_ordered
        assert desired_table.rich_path != source_table.rich_path
        super().__init__()

        self._desired_table: YtTable = desired_table
        self._actual_table: YtTable = actual_table
        self._source_table: YtTable = source_table

        self._response: Any | None = None
        self._scheduled: bool = False

    def schedule_next(self, batch_client: YtClientProxy) -> bool:
        assert not self._scheduled, "Can't call schedule_next() more than one time"
        self._scheduled = True

        assert (
            not self._actual_table.exists
        ), f"{self._actual_table.table_type} {self._actual_table.rich_path} already exists"
        assert (
            self._source_table.exists
        ), f"Can't fetch total_row_counts from unexisting table {self._source_table.rich_path}"
        assert (
            self._source_table.total_row_count
        ), f"No total row counts in source table model {self._source_table.rich_path}"

        table_attributes = self._desired_table.yt_attributes
        table_attributes["tablet_count"] = self._source_table.effective_tablet_count
        table_attributes["trimmed_row_counts"] = self._source_table.total_row_count

        LOG.warning(
            "Create %s %s with attributes=%s",
            self._desired_table.table_type,
            self._desired_table.rich_path,
            table_attributes,
        )
        self._response = batch_client.create(
            self._desired_table.table_type,
            self._desired_table.path,
            attributes=table_attributes,
        )

        return False

    def process(self):
        assert self._scheduled, "Can't call process before schedule"
        if self._response is not None:
            self.assert_response(self._response)
            reset_actual_table_after_create(self._actual_table, self._desired_table)
            self._actual_table.tablet_info = deepcopy(self._desired_table.tablet_info)
            self._actual_table.total_row_count = deepcopy(self._source_table.total_row_count)
