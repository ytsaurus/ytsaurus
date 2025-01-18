from copy import deepcopy
import logging
from typing import Any

from yt.yt_sync.core.client import YtClientProxy
from yt.yt_sync.core.model import Types
from yt.yt_sync.core.model import YtReplica
from yt.yt_sync.core.model import YtTable
from yt.yt_sync.core.model import YtTableAttributes

from .base import ActionBase
from .helpers import reset_actual_table_after_create

LOG = logging.getLogger("yt_sync")


class CreateTableAction(ActionBase):
    def __init__(self, desired_table: YtTable, actual_table: YtTable, replicated_table: YtTable | None = None):
        super().__init__()
        self._desired_table: YtTable = desired_table
        self._actual_table: YtTable = actual_table
        self._replicated_table: YtTable | None = replicated_table
        self._result: Any | None = None
        self._scheduled: bool = False

    def schedule_next(self, batch_client: YtClientProxy) -> bool:
        assert not self._scheduled, "Can't call schedule_next() more than one time"
        self._scheduled = True

        assert (
            not self._actual_table.exists
        ), f"{self._actual_table.table_type} {self._actual_table.rich_path} already exists"

        table_attributes = self._patch(self._desired_table.yt_attributes)
        LOG.warning(
            "Create %s %s:%s with attributes=%s",
            self._desired_table.table_type,
            self._desired_table.cluster_name,
            self._desired_table.path,
            table_attributes,
        )
        self._result = batch_client.create(
            self._desired_table.table_type,
            self._desired_table.path,
            attributes=table_attributes,
        )
        return False

    def process(self):
        assert self._scheduled, "Can't call process before schedule"
        if self._result is not None:
            self.assert_response(self._result)
            reset_actual_table_after_create(
                self._actual_table, self._desired_table, self._patch(self._desired_table.attributes)
            )

    def _get_replica(self) -> YtReplica | None:
        if not self._replicated_table:
            return None
        return self._replicated_table.replicas.get(self._desired_table.replica_key)

    def _patch(self, table_attributes: Types.Attributes | YtTableAttributes) -> Types.Attributes | YtTableAttributes:
        if self._replicated_table is None:
            return table_attributes
        assert self._replicated_table.is_replicated, "Wrong type of replicated table"
        assert self._replicated_table.exists, "Can't link to unexisting replicated table"
        result = deepcopy(table_attributes)
        replica = self._get_replica()
        if replica and replica.replica_id:
            result["upstream_replica_id"] = replica.replica_id
        for system_attr in ["data_weight"]:
            if system_attr in result:
                result.pop(system_attr, None)
        return result
