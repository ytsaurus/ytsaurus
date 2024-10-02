from collections import defaultdict
import logging
from typing import Any
from typing import Callable
from typing import ClassVar

from yt.yt_sync.core.client import YtClientProxy
from yt.yt_sync.core.model import Types
from yt.yt_sync.core.model import YtReplica
from yt.yt_sync.core.model import YtTable
from yt.yt_sync.core.yt_commands import make_yt_commands

from .base import ActionBase

LOG = logging.getLogger("yt_sync")


class SwitchReplicaModeAction(ActionBase):
    _PREFERRED_SYNC_REPLICAS_ATTR: ClassVar[str] = "replicated_table_options/preferred_sync_replica_clusters"
    _MIN_SYNC_REPLICA_COUNT_ATTR: ClassVar[str] = "replicated_table_options/min_sync_replica_count"
    _MAX_SYNC_REPLICA_COUNT_ATTR: ClassVar[str] = "replicated_table_options/max_sync_replica_count"

    def __init__(
        self,
        table: YtTable,
        sync_replicas: set[str],
        content_types: set[str] | None = None,
        strict_preferred: bool = True,
        force_fix_preferred: bool = False,
        min_sync_replica_count: int | None = None,
        max_sync_replica_count: int | None = None,
    ):
        assert table.is_replicated, "Replica mode switching supported only for replicated tables"
        if min_sync_replica_count is not None and max_sync_replica_count is not None:
            assert min_sync_replica_count >= 0
            assert max_sync_replica_count >= 0
            assert max_sync_replica_count >= min_sync_replica_count
        super().__init__()

        self._table: YtTable = table
        self._sync_replicas: frozenset[str] = frozenset(sync_replicas)
        self._content_types: frozenset[str] = frozenset() if content_types is None else frozenset(content_types)
        self._strict_preferred: bool = strict_preferred
        self._force_fix_preferred: bool = force_fix_preferred
        self._min_sync_replica_count: int | None = min_sync_replica_count
        self._max_sync_replica_count: int | None = max_sync_replica_count

        self._replicas: dict[Types.ReplicaKey, YtReplica] = dict()
        self._switch_requests: dict[Types.ReplicaKey, Any] = defaultdict(list)
        self._preferred_sync_replicas: list[str] | None = None
        self._scheduled: bool = False
        self._done: bool = False

    def _desired_mode(self, replica: YtReplica) -> str:
        return YtReplica.Mode.SYNC if replica.cluster_name in self._sync_replicas else YtReplica.Mode.ASYNC

    def _fill_replicas_to_switch(self):
        def _iterate_replicas(is_desired_mode: Callable[[YtReplica], tuple[str, bool]]):
            self._replicas.clear()
            for replica_key, replica in self._table.replicas.items():
                if not replica.exists:
                    continue
                if self._content_types and replica.content_type not in self._content_types:
                    continue
                desired_mode, is_same = is_desired_mode(replica)
                if is_same:
                    LOG.debug(
                        "Replica #%s (%s:%s) already in desired mode %s",
                        replica.replica_id,
                        replica.cluster_name,
                        replica.replica_path,
                        desired_mode,
                    )
                else:
                    LOG.debug(
                        "Request switch replica #%s (%s:%s) mode, actual=%s, desired=%s",
                        replica.replica_id,
                        replica.cluster_name,
                        replica.replica_path,
                        replica.mode,
                        desired_mode,
                    )
                    self._replicas[replica_key] = replica

        def _replica_mode_check(replica: YtReplica) -> tuple[str, bool]:
            desired_mode = self._desired_mode(replica)
            return (desired_mode, replica.mode == desired_mode)

        def _preferred_replicas_check(replica: YtReplica) -> tuple[str, bool]:
            actual_preferred: set[str] = self._table.rtt_options.preferred_sync_replica_clusters or set()

            if replica.cluster_name in actual_preferred and replica.cluster_name in self._sync_replicas:
                return (YtReplica.Mode.SYNC, True)
            elif replica.cluster_name in actual_preferred:
                return (YtReplica.Mode.ASYNC, False)
            else:
                return (YtReplica.Mode.SYNC, False)

        _iterate_replicas(_replica_mode_check)
        if not self._replicas and self._force_fix_preferred and self._is_rtt():
            LOG.debug("Force fix RTT preferred_sync_replicas")
            _iterate_replicas(_preferred_replicas_check)

    def _is_rtt(self) -> bool:
        return self._table.is_rtt_enabled and self._table.has_rtt_enabled_replicas

    def _switch(self, batch_client: YtClientProxy):
        def _replicas_match(actual_sync_replicas: set[str]) -> bool:
            return (
                actual_sync_replicas == self._sync_replicas
                if self._strict_preferred
                else actual_sync_replicas >= self._sync_replicas
            )

        is_rtt = self._is_rtt()
        actual_sync_replicas: set[str] = self._table.preferred_sync_replicas
        if is_rtt and actual_sync_replicas:
            replicas_match = _replicas_match(actual_sync_replicas)
            if replicas_match and self._force_fix_preferred:
                actual_sync_replicas: set[str] = self._table.rtt_options.preferred_sync_replica_clusters or set()
                replicas_match = _replicas_match(actual_sync_replicas)
            if replicas_match:
                LOG.debug(
                    "Already actual state of %s %s/@%s = %s",
                    self._table.table_type,
                    self._table.rich_path,
                    self._PREFERRED_SYNC_REPLICAS_ATTR,
                    sorted(actual_sync_replicas),
                )
                return
            desired_sync_replicas = sorted(self._sync_replicas)
            LOG.warning(
                "Set %s %s/@%s, actual=%s, desired=%s",
                self._table.table_type,
                self._table.rich_path,
                self._PREFERRED_SYNC_REPLICAS_ATTR,
                sorted(actual_sync_replicas),
                desired_sync_replicas,
            )
            if (
                self._min_sync_replica_count is not None
                and self._table.min_sync_replica_count != self._min_sync_replica_count
            ):
                LOG.warning(
                    "Set %s %s/@%s: actual=%s, desired=%s",
                    self._table.table_type,
                    self._table.rich_path,
                    self._MIN_SYNC_REPLICA_COUNT_ATTR,
                    self._table.min_sync_replica_count,
                    self._min_sync_replica_count,
                )

            if (
                self._max_sync_replica_count is not None
                and self._table.max_sync_replica_count != self._max_sync_replica_count
            ):
                LOG.warning(
                    "Set %s %s/@%s: actual=%s, desired=%s",
                    self._table.table_type,
                    self._table.rich_path,
                    self._MAX_SYNC_REPLICA_COUNT_ATTR,
                    self._table.max_sync_replica_count,
                    self._max_sync_replica_count,
                )

            self._preferred_sync_replicas = desired_sync_replicas
            yt_commands = make_yt_commands(self._table.is_chaos_replicated)
            self._switch_requests[self._table.key].extend(
                yt_commands.set_preferred_sync_replicas(
                    batch_client=batch_client,
                    table=self._table,
                    preferred_sync_replicas=desired_sync_replicas,
                    min_sync_replicas_count=self._min_sync_replica_count,
                    max_sync_replicas_count=self._max_sync_replica_count,
                )
            )
        else:
            for replica_key, replica in self._replicas.items():
                desired_mode = self._desired_mode(replica)
                LOG.warning(
                    "Switch replica #%s (%s:%s) mode: actual=%s, desired=%s",
                    replica.replica_id,
                    replica.cluster_name,
                    replica.replica_path,
                    replica.mode,
                    desired_mode,
                )
                self._switch_requests[replica_key].append(
                    batch_client.alter_table_replica(replica.replica_id, mode=desired_mode)
                )

    def schedule_next(self, batch_client: YtClientProxy) -> bool:
        if self._done:
            return False
        self._scheduled = True
        self._fill_replicas_to_switch()
        if not self._replicas:
            LOG.debug(
                "All replicas for %s %s in desired mode, nothing to switch",
                self._table.table_type,
                self._table.rich_path,
            )
            self._done = True
            return False
        self._switch(batch_client)
        return False

    def process(self):
        if self._done:
            return
        assert self._scheduled, "Can't process before schedule"
        for responses in self._switch_requests.values():
            for response in responses:
                self.assert_response(response)
        for replica_key, replica in self._replicas.items():
            self._table.replicas[replica_key].mode = self._desired_mode(replica)
        if self._preferred_sync_replicas is not None:
            self._table.rtt_options.preferred_sync_replica_clusters = set(self._preferred_sync_replicas)
            if self._min_sync_replica_count is not None:
                self._table.rtt_options.min_sync_replica_count = self._min_sync_replica_count
            if self._max_sync_replica_count is not None:
                self._table.rtt_options.max_sync_replica_count = self._max_sync_replica_count
        self._done = True
