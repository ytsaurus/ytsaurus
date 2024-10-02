from copy import deepcopy
import logging
from typing import Any

from yt.yt_sync.core.client import YtClientProxy
from yt.yt_sync.core.model import Types
from yt.yt_sync.core.model import YtReplica
from yt.yt_sync.core.model import YtTable

from .base import ActionBase
from .helpers import create_replica_from
from .helpers import replica_attributes_for_create

LOG = logging.getLogger("yt_sync")


class CreateReplicaAction(ActionBase):
    def __init__(
        self,
        replicated_table: YtTable,
        desired_replica: YtReplica,
        replica_for: YtTable,
        replica_attrs: Types.Attributes = {},
    ):
        assert replicated_table.is_replicated
        assert not replica_for.is_replicated

        super().__init__()
        self._replicated_table: YtTable = replicated_table
        self._desired_replica: YtReplica = desired_replica
        self._table: YtTable = replica_for
        self._replica_attrs = deepcopy(replica_attrs)
        self._created_replica: YtReplica | None = None

        self._scheduled: bool = False
        self._response: Any | None = None

    def schedule_next(self, batch_client: YtClientProxy) -> bool:
        assert not self._scheduled
        self._scheduled = True
        self._created_replica = self._get_replica_for_create()
        actual_replica = self._replicated_table.replicas.get(self._created_replica.key)
        if actual_replica:
            assert not actual_replica.exists, (
                f"Replica #{actual_replica.replica_id} "
                + f"({actual_replica.cluster_name}:{actual_replica.replica_path}) for "
                + f"{self._replicated_table.cluster_name}:{self._replicated_table.path} already exists"
            )

        replica_attributes = replica_attributes_for_create(self._replicated_table, self._created_replica)
        LOG.warning(
            "Create %s (%s:%s) for %s %s:%s with attributes %s",
            self._replicated_table.replica_type,
            self._created_replica.cluster_name,
            self._created_replica.replica_path,
            self._replicated_table.table_type,
            self._replicated_table.cluster_name,
            self._replicated_table.path,
            replica_attributes,
        )
        self._response = batch_client.create(self._replicated_table.replica_type, attributes=replica_attributes)
        return False

    def process(self):
        assert self._scheduled
        if self._response is None:
            return
        self.assert_response(self._response)
        assert self._created_replica
        self._created_replica.replica_id = str(self._response.get_result())
        self._created_replica.exists = True
        LOG.debug(
            "Replica [type=%s] created: id=%s, cluster_name=%s, replica_path=%s, mode=%s, enabled=%s, rtt_enabled=%s",
            self._replicated_table.replica_type,
            self._created_replica.replica_id,
            self._created_replica.cluster_name,
            self._created_replica.replica_path,
            self._created_replica.mode,
            self._created_replica.enabled,
            self._created_replica.enable_replicated_table_tracker,
        )
        self._replicated_table.add_replica(self._created_replica)

    def _get_replica_for_create(self) -> YtReplica:
        return create_replica_from(self._table, self._desired_replica, self._replica_attrs)
