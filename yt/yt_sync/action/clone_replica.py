from enum import Enum
import logging
from typing import Any

from yt.yt_sync.core.client import YtClientProxy
from yt.yt_sync.core.model import YtReplica
from yt.yt_sync.core.model import YtTable

from .base import ActionBase
from .helpers import create_replica_from
from .helpers import parse_tablet_infos
from .helpers import replica_attributes_for_create

LOG = logging.getLogger("yt_sync")


class _State(Enum):
    INIT = 0
    READ_REPLICATION_INDEXES = 1
    CREATE_REPLICA = 2
    DONE = 3


class CloneReplicaAction(ActionBase):
    def __init__(
        self,
        desired_replicated_table: YtTable,
        actual_replicated_table: YtTable,
        source_table: YtTable,
        destination_table: YtTable,
    ):
        assert (
            desired_replicated_table.is_replicated
        ), f"Table {desired_replicated_table.rich_path} should be replicated"
        assert (
            not desired_replicated_table.is_chaos_replicated
        ), f"Table {desired_replicated_table.rich_path} should not be chaos replicated"
        assert actual_replicated_table.is_replicated, f"Table {actual_replicated_table.rich_path} should be replicated"
        assert (
            not actual_replicated_table.is_chaos_replicated
        ), f"Table {actual_replicated_table.rich_path} should be chaos replicated"

        assert not source_table.is_replicated, f"Can't use replicated table {source_table.rich_path} as replica"
        assert (
            not destination_table.is_replicated
        ), f"Can't use replicated table {destination_table.rich_path} as replica"
        assert (
            not source_table.rich_path == destination_table.rich_path
        ), f"Can't clone replica to self {source_table.rich_path}"
        super().__init__()
        self._desired_replicated_table: YtTable = desired_replicated_table
        self._actual_replicated_table: YtTable = actual_replicated_table
        self._source: YtTable = source_table
        self._destination: YtTable = destination_table

        self._state: _State = _State.INIT
        self._get_response: Any | None = None
        self._replication_row_indexes: list = list()
        self._created_replica: YtReplica | None = None
        self._create_response: Any | None = None

    def schedule_next(self, batch_client: YtClientProxy) -> bool:
        if self._state == _State.DONE:
            return False
        match self._state:
            case _State.INIT:
                assert (
                    self._actual_replicated_table.exists
                ), f"Can't fetch replicas for non-existing table {self._actual_replicated_table.rich_path}"
                assert self._source.exists, f"Can't continue with non-existing replica table {self._source.rich_path}"
                self._get_response = batch_client.get_tablet_infos(
                    self._actual_replicated_table.path,
                    [i for i in range(self._actual_replicated_table.effective_tablet_count)],
                )
                self._state = _State.READ_REPLICATION_INDEXES
                return True
            case _State.READ_REPLICATION_INDEXES:
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
            case _State.READ_REPLICATION_INDEXES:
                self._collect_replication_row_indexes()
                self._state = _State.CREATE_REPLICA
                return
            case _State.CREATE_REPLICA:
                self._process_created_replica()
                self._state = _State.DONE
                return
            case _State.DONE:
                return

    def _get_source_replica_id(self) -> str:
        source_replica = self._actual_replicated_table.replicas.get(self._source.replica_key)
        assert source_replica
        return source_replica.replica_id

    def _collect_replication_row_indexes(self):
        source_replica_id = self._get_source_replica_id()
        tablet_infos = parse_tablet_infos(self._get_response)
        for tablet_info in tablet_infos:
            replica_info = None
            for entry in tablet_info["replica_infos"]:
                if entry["replica_id"] == source_replica_id:
                    replica_info = entry
                    break
            else:
                assert False, f"Source replica=#{source_replica_id} for {self._source.rich_path} not found"
            assert (
                replica_info["current_replication_row_index"] == replica_info["committed_replication_row_index"]
            ), f"Has replication lag for {self._source.rich_path}"
            self._replication_row_indexes.append(replica_info["current_replication_row_index"])

    def _create_replica(self, batch_client: YtClientProxy):
        assert (
            self._destination.replica_key not in self._actual_replicated_table.replicas
        ), f"Replica for {self._destination.rich_path} already exists"
        assert (
            self._source.replica_key in self._desired_replicated_table.replicas
        ), f"No desired replica for {self._source.rich_path}"

        desired_replica: YtReplica = self._desired_replicated_table.replicas[self._source.replica_key]
        self._created_replica: YtReplica = create_replica_from(
            self._destination,
            desired_replica,
            {"start_replication_row_indexes": self._replication_row_indexes, "enabled": False},
        )
        replica_attributes = replica_attributes_for_create(self._desired_replicated_table, self._created_replica)
        LOG.warning(
            "Create %s (%s:%s) for %s %s:%s with attributes %s",
            self._desired_replicated_table.replica_type,
            self._created_replica.cluster_name,
            self._created_replica.replica_path,
            self._desired_replicated_table.table_type,
            self._desired_replicated_table.cluster_name,
            self._desired_replicated_table.path,
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
            "Replica [type=%s] created: id=%s, cluster_name=%s, replica_path=%s, mode=%s, enabled=%s, rtt_enabled=%s",
            self._desired_replicated_table.replica_type,
            self._created_replica.replica_id,
            self._created_replica.cluster_name,
            self._created_replica.replica_path,
            self._created_replica.mode,
            self._created_replica.enabled,
            self._created_replica.enable_replicated_table_tracker,
        )
        self._actual_replicated_table.add_replica(self._created_replica)
