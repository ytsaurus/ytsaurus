from copy import deepcopy
import logging
from typing import Any

from yt.yt_sync.core.client import YtClientProxy
from yt.yt_sync.core.model import YtTable

from .base import ActionBase

LOG = logging.getLogger("yt_sync")


class AlterTableSchemaAction(ActionBase):
    def __init__(self, desired_table: YtTable, actual_table: YtTable):
        assert desired_table.cluster_name == actual_table.cluster_name
        assert desired_table.path == actual_table.path
        assert actual_table.exists

        super().__init__()
        self._desired_table = desired_table
        self._actual_table = actual_table
        self._scheduled: bool = False
        self._response: Any | None = None

    def schedule_next(self, batch_client: YtClientProxy) -> bool:
        assert not self._scheduled, "Can't call schedule_next() more than one time"
        self._scheduled = True
        if self._actual_table.schema == self._desired_table.schema:
            LOG.info(
                "Skip alter schema, %s %s:%s already has desired schema=%s",
                self._actual_table.table_type,
                self._actual_table.cluster_name,
                self._actual_table.path,
                self._actual_table.schema.yt_schema,
            )
            return False

        LOG.warning(
            "Alter %s %s:%s schema: actual=%s, desired=%s",
            self._actual_table.table_type,
            self._actual_table.cluster_name,
            self._actual_table.path,
            self._actual_table.schema.yt_schema,
            self._desired_table.schema.yt_schema,
        )
        self._response = batch_client.alter_table(self._actual_table.path, schema=self._desired_table.schema.yt_schema)
        return False

    def process(self):
        assert self._scheduled, "Can't call process before schedule"
        if self._response is not None:
            self.assert_response(self._response)
        self._actual_table.schema = deepcopy(self._desired_table.schema)
