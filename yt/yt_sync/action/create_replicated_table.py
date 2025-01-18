from copy import deepcopy
import logging
from typing import Any
from typing import ClassVar

from yt.yt_sync.core.client import YtClientProxy
from yt.yt_sync.core.model import Types
from yt.yt_sync.core.model import YtTable
from yt.yt_sync.core.model.table import RttOptions

from .base import ActionBase
from .helpers import reset_actual_table_after_create

LOG = logging.getLogger("yt_sync")


class CreateReplicatedTableAction(ActionBase):
    _START: ClassVar[int] = 0
    _CREATE_REPLICATED_TABLE: ClassVar[int] = 1
    _CREATE_REPLICAS: ClassVar[int] = 2
    _FETCH_REPLICATED_TABLE_STUFF: ClassVar[int] = 3
    _DONE: ClassVar[int] = 4

    def __init__(self, desired_table: YtTable, actual_table: YtTable):
        assert desired_table.is_replicated, "Replicated table expected"
        super().__init__()

        self._desired_table: YtTable = desired_table
        self._actual_table: YtTable = actual_table
        self._state: int = self._START
        self._table_result: Any | None = None
        self._rtt_options_result: Any | None = None
        self._replicas_result: dict[Types.ReplicaKey, Any] = dict()

    def schedule_next(self, batch_client: YtClientProxy) -> bool:
        assert self._state != self._DONE, "Unexpected state"
        if self._START == self._state:
            self._state = self._CREATE_REPLICATED_TABLE
            if self._actual_table.exists:
                LOG.info(
                    "Skip create %s %s, already exists",
                    self._actual_table.table_type,
                    self._actual_table.rich_path,
                )
            else:
                LOG.warning(
                    "Create %s %s with spec %s",
                    self._desired_table.table_type,
                    self._desired_table.rich_path,
                    self._desired_table.yt_attributes,
                )
                self._table_result = batch_client.create(
                    self._desired_table.table_type,
                    self._desired_table.path,
                    attributes=self._desired_table.yt_attributes,
                )
            return True
        if self._CREATE_REPLICATED_TABLE == self._state:
            self._state = self._CREATE_REPLICAS
            for replica_key, replica in self._desired_table.replicas.items():
                actual_replica = self._actual_table.replicas.get(replica_key)
                if actual_replica and actual_replica.replica_id:
                    LOG.info(
                        "Skip create %s (table_path=%s:%s, cluster_name=%s, replica_path=%s), already exists",
                        self._desired_table.replica_type,
                        self._desired_table.cluster_name,
                        self._desired_table.path,
                        replica.cluster_name,
                        replica.replica_path,
                    )
                else:
                    LOG.warning(
                        "Create %s (table_path=%s, cluster_name=%s, replica_path=%s)",
                        self._desired_table.replica_type,
                        self._desired_table.rich_path,
                        replica.cluster_name,
                        replica.replica_path,
                    )
                    self._replicas_result[replica_key] = batch_client.create(
                        self._desired_table.replica_type,
                        attributes=self._patch_replica_attributes(replica.yt_attributes),
                    )
            return True
        if self._CREATE_REPLICAS == self._state:
            self._state = self._FETCH_REPLICATED_TABLE_STUFF
            self._table_result = None
            if not self.dry_run:
                LOG.debug("Fetch %s %s stuff", self._actual_table.table_type, self._actual_table.rich_path)
                if self._actual_table.is_chaos_replicated:
                    self._table_result = batch_client.get(f"{self._actual_table.path}/@replication_card_id")
                self._rtt_options_result = batch_client.get(f"{self._actual_table.path}/@replicated_table_options")
            return False
        return False

    def process(self):
        assert self._state not in (self._START, self._DONE), "Unexpected state"
        if self._CREATE_REPLICATED_TABLE == self._state:
            if self._table_result is not None:
                self.assert_response(self._table_result)
                reset_actual_table_after_create(self._actual_table, self._desired_table)
        if self._CREATE_REPLICAS == self._state:
            for replica_key, response in self._replicas_result.items():
                self.assert_response(response)
                self._actual_table.replicas[replica_key] = deepcopy(self._desired_table.replicas[replica_key])
                self._actual_table.replicas[replica_key].replica_id = str(response.get_result())
        if self._FETCH_REPLICATED_TABLE_STUFF == self._state:
            if self.dry_run:
                if self._actual_table.is_chaos_replicated:
                    self._actual_table.chaos_replication_card_id = "0-0-0-0"
            else:
                self.assert_response(self._rtt_options_result)
                assert self._rtt_options_result
                self._actual_table.rtt_options = RttOptions.make(dict(self._rtt_options_result.get_result()))
                if self._actual_table.rtt_options.enabled:
                    self._actual_table.set_rtt_enabled_from_replicas()
                if self._table_result is not None:
                    self.assert_response(self._table_result)
                    self._actual_table.chaos_replication_card_id = str(self._table_result.get_result())
            self._state = self._DONE

    def _patch_replica_attributes(self, attrs: Types.Attributes) -> Types.Attributes:
        result = deepcopy(attrs)
        result["table_path"] = self._desired_table.path
        return result
