from enum import Enum
import logging
from typing import Any

from yt.yt_sync.core.client import YtClientProxy
from yt.yt_sync.core.model import YtReplica
from yt.yt_sync.core.model import YtTable

from .base import ActionBase
from .helpers import create_replica_from
from .helpers import replica_attributes_for_create

LOG = logging.getLogger("yt_sync")


class _State(Enum):
    INIT = 0
    READ_REPLICATION_PROGRESS = 1
    CREATE_REPLICA = 2
    DONE = 3


class CloneChaosReplicaAction(ActionBase):
    def __init__(
        self,
        desired_replicated_table: YtTable,
        actual_replicated_table: YtTable,
        source_table: YtTable,
        destination_table: YtTable,
        destination_as_source: bool = False,
    ):
        assert (
            desired_replicated_table.is_chaos_replicated
        ), f"Desired table {desired_replicated_table.rich_path} should be chaos replicated"
        assert (
            actual_replicated_table.is_chaos_replicated
        ), f"Actual table {actual_replicated_table} should be chaos replicated"
        assert not source_table.is_replicated, f"Can't use source replicated table {source_table.rich_path} as replica"
        assert (
            not destination_table.is_replicated
        ), f"Can't use destination replicated table {destination_table.rich_path} as replica"
        assert (
            source_table.rich_path != destination_table.rich_path
        ), f"Can't clone replica of self {source_table.rich_path}"

        super().__init__()
        self._actual_replicated_table: YtTable = actual_replicated_table
        self._desired_replicated_table: YtTable = desired_replicated_table
        self._source_table: YtTable = source_table
        self._destination_table: YtTable = destination_table
        self._destination_as_source: bool = destination_as_source

        self._state: _State = _State.INIT
        self._get_response: Any | None = None
        self._create_response: Any | None = None
        self._replication_progress: Any | None = None
        self._created_replica: YtReplica | None = None

    def schedule_next(self, batch_client: YtClientProxy) -> bool:
        if self._state == _State.DONE:
            return False
        match self._state:
            case _State.INIT:
                assert self._actual_replicated_table.exists, (
                    "Can't fetch replication progress from non-existing chaos replicated table "
                    + f" {self._actual_replicated_table.rich_path}"
                )
                assert (
                    self._source_table.exists
                ), f"Can't fetch replication progress for non-existing replica table {self._source_table.rich_path}"
                self._get_response = batch_client.get(f"#{self._get_source_replica_id()}/@replication_progress")
                self._state = _State.READ_REPLICATION_PROGRESS
                return True
            case _State.READ_REPLICATION_PROGRESS:
                assert False, "unreachable"
            case _State.CREATE_REPLICA:
                self._create_replica(batch_client)
                return False
            case _State.DONE:
                return False

    def process(self):
        match self._state:
            case _State.INIT:
                assert False, "Can't call process before schedule"
            case _State.READ_REPLICATION_PROGRESS:
                self._process_replication_progress()
                self._state = _State.CREATE_REPLICA
                return
            case _State.CREATE_REPLICA:
                self._process_created_replica()
                self._state = _State.DONE
                return
            case _State.DONE:
                return

    def _get_source_replica_id(self) -> str:
        source_replica = self._actual_replicated_table.replicas.get(self._source_table.replica_key)
        assert source_replica, (
            f"No replica for source table {self._source_table.rich_path} in chaos replicated table "
            + f"{self._actual_replicated_table.rich_path}"
        )
        assert source_replica.replica_id
        return source_replica.replica_id

    def _process_replication_progress(self):
        assert self._get_response
        self.assert_response(self._get_response)
        self._replication_progress = self._get_response.get_result()

    def _create_replica(self, batch_client: YtClientProxy):
        destination_replica: YtReplica | None = self._actual_replicated_table.replicas.get(
            self._destination_table.replica_key
        )
        assert (
            destination_replica is None or destination_replica.exists is False
        ), f"Replica {self._destination_table.rich_path} for {self._actual_replicated_table.rich_path} already exists"

        desired_replica_key = (
            self._destination_table.replica_key if self._destination_as_source else self._source_table.replica_key
        )
        assert (
            desired_replica_key in self._desired_replicated_table.replicas
        ), f"No desired replica for {desired_replica_key}"

        desired_replica: YtReplica = self._desired_replicated_table.replicas[desired_replica_key]
        self._created_replica = create_replica_from(
            self._destination_table,
            desired_replica,
            {"replication_progress": self._replication_progress, "catchup": True, "enabled": True},
        )
        replica_attributes = replica_attributes_for_create(self._desired_replicated_table, self._created_replica)
        LOG.warning(
            "Create %s (%s:%s) for %s %s with attributes %s",
            self._desired_replicated_table.replica_type,
            self._created_replica.cluster_name,
            self._created_replica.replica_path,
            self._desired_replicated_table.table_type,
            self._desired_replicated_table.rich_path,
            replica_attributes,
        )
        self._create_response = batch_client.create(
            self._desired_replicated_table.replica_type, attributes=replica_attributes
        )

    def _process_created_replica(self):
        self.assert_response(self._create_response)
        assert self._created_replica
        self._created_replica.replica_id = str(self._create_response.get_result())
        self._created_replica.exists = True
        LOG.debug(
            "Replica [type=%s] created: id=%s, cluster_name=%s, replica_path=%s, mode=%s, enabled=%s, rtt_enabled=%s, content_type=%s",
            self._desired_replicated_table.replica_type,
            self._created_replica.replica_id,
            self._created_replica.cluster_name,
            self._created_replica.replica_path,
            self._created_replica.mode,
            self._created_replica.enabled,
            self._created_replica.enable_replicated_table_tracker,
            self._created_replica.content_type,
        )
        self._actual_replicated_table.add_replica(self._created_replica)
