import logging
from typing import Any
from typing import ClassVar

from yt.yt_sync.core.client import YtClientProxy
from yt.yt_sync.core.model import YtReplica
from yt.yt_sync.core.model import YtTable

from .base import ActionBase

LOG = logging.getLogger("yt_sync")


class SwitchReplicaStateAction(ActionBase):
    _STATE_INIT: ClassVar[int] = 0
    _STATE_MODIFY: ClassVar[int] = 1
    _STATE_WAIT: ClassVar[int] = 2
    _STATE_DONE: ClassVar[int] = 3

    def __init__(self, replicated_table: YtTable, enabled: bool, for_table: YtTable | None = None):
        assert replicated_table.is_replicated
        if for_table is not None:
            assert not for_table.is_replicated

        super().__init__()
        self._replicated_table: YtTable = replicated_table
        self._enabled: bool = enabled
        self._for_table: YtTable | None = for_table
        self._replica_desired_state: str = YtReplica.State.ENABLED if self._enabled else YtReplica.State.DISABLED

        self._modify_responses: dict[str, Any] = dict()
        self._wait_responses: dict[str, Any] = dict()
        self._state = self._STATE_INIT

    def schedule_next(self, batch_client: YtClientProxy) -> bool:
        if self._STATE_DONE == self._state:
            return False
        elif self._STATE_INIT == self._state:
            for replica in self._select_replicas():
                self._modify_responses[replica.key] = batch_client.alter_table_replica(
                    replica.replica_id, enabled=self._enabled
                )
                LOG.warning(
                    "Change replica [type=%s] #%s (%s:%s) for %s:%s state to %s",
                    self._replicated_table.replica_type,
                    replica.replica_id,
                    replica.cluster_name,
                    replica.replica_path,
                    self._replicated_table.cluster_name,
                    self._replicated_table.path,
                    self._replica_desired_state,
                )
            if not self._modify_responses:
                LOG.info(
                    "All replicas of %s:%s in desired state %s",
                    self._replicated_table.cluster_name,
                    self._replicated_table.path,
                    self._replica_desired_state,
                )
                self._state = self._STATE_DONE
                return False
            self._state = self._STATE_MODIFY
        elif self._STATE_WAIT == self._state:
            self._wait_responses.clear()
            for replica_key in self._modify_responses:
                replica = self._replicated_table.replicas[replica_key]
                assert replica.replica_id
                self._wait_responses[replica_key] = batch_client.get(f"#{replica.replica_id}/@state")
        return True

    def process(self):
        assert self._STATE_INIT != self._state
        if self._STATE_MODIFY == self._state:
            for response in self._modify_responses.values():
                self.assert_response(response)
            self._state = self._STATE_WAIT
            return
        elif self._STATE_WAIT == self._state:
            if self.dry_run:
                self._finalize()
                return

            done = True
            for replica_key, response in self._wait_responses.items():
                self.assert_response(response)
                replica = self._replicated_table.replicas[replica_key]
                result = str(response.get_result())
                LOG.debug(
                    "Replica [type=%s] #%s (%s:%s) for %s:%s state: desired=%s, actual=%s",
                    self._replicated_table.replica_type,
                    replica.replica_id,
                    replica.cluster_name,
                    replica.replica_path,
                    self._replicated_table.cluster_name,
                    self._replicated_table.path,
                    self._replica_desired_state,
                    result,
                )
                if self._replica_desired_state != result:
                    done = False
                    break
            if done:
                self._finalize()

    @property
    def is_awaiting(self) -> bool:
        return True

    @property
    def recommended_iteration_delay(self) -> float:
        return 1.0

    def _finalize(self):
        self._state = self._STATE_DONE
        for replica in self._select_replicas():
            replica.enabled = self._enabled

    def _select_replicas(self) -> list[YtReplica]:
        result: list[YtReplica] = []

        def _add_replica(replica: YtReplica):
            if replica.exists:
                assert replica.replica_id
                result.append(replica)

        if self._for_table is not None:
            replica = self._replicated_table.replicas.get(self._for_table.replica_key, None)
            assert replica
            if self._enabled != replica.enabled:
                _add_replica(replica)
            return result

        for replica in self._replicated_table.replicas.values():
            if self._enabled == replica.enabled:
                continue
            _add_replica(replica)
        return result
