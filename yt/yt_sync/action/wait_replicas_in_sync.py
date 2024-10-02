import logging
from typing import Any

from yt.yt_sync.core.client import YtClientProxy
from yt.yt_sync.core.model import Types
from yt.yt_sync.core.model import YtReplica
from yt.yt_sync.core.model import YtTable

from .base import ActionBase
from .helpers import parse_tablet_infos

LOG = logging.getLogger("yt_sync")


class WaitReplicasInSyncAction(ActionBase):
    def __init__(self, table: YtTable, sync_clusters: set[str]):
        assert table.is_replicated

        super().__init__()
        self._table: YtTable = table
        self._sync_clusters: frozenset[str] = frozenset(sync_clusters)
        self._scheduled: bool = False
        self._done: bool = False
        self._timestamp: int | None = None
        self._response: Any | None = None

    def schedule_next(self, batch_client: YtClientProxy) -> bool:
        if not self._table.exists or self.dry_run:
            self._done = True
            return False
        if self._done:
            return False
        self._scheduled = True
        if self._table.is_ordered:
            if self._table.is_chaos_replicated:
                self._response = batch_client.get(f"{self._table.path}/@replicas")
            else:
                self._response = batch_client.get_tablet_infos(
                    self._table.path, [i for i in range(self._table.tablet_count or 1)]
                )
        else:
            timestamp = self._get_timestamp(batch_client)
            self._response = batch_client.get_in_sync_replicas(self._table.path, timestamp, [], all_keys=True)
        return True

    def process(self):
        if self._done:
            return
        assert self._scheduled, "Can't call 'process' before 'schedule_next"
        if self.is_retryable_error(self._response):
            return
        self.assert_response(self._response)
        assert self._response
        if self._is_required_state(self._response.get_result()):
            self._done = True

    @property
    def is_awaiting(self) -> bool:
        return True

    def _get_timestamp(self, batch_client: YtClientProxy) -> int:
        if self._timestamp is None:
            yt_client = batch_client.underlying_client_proxy
            assert yt_client
            self._timestamp = yt_client.generate_timestamp()
        return self._timestamp

    def _is_required_state(self, result: Any) -> bool:
        if self.dry_run:
            return True

        replica_id_to_key: dict[str, Types.ReplicaKey] = dict()
        desired_sync_replicas: set[str] = set()
        actual_sync_replicas: set[str] = set()
        for replica in self._table.replicas.values():
            if not replica.exists:
                continue
            assert replica.replica_id
            replica_id_to_key[replica.replica_id] = replica.key
            if not self._table.is_ordered and YtReplica.ContentType.QUEUE == replica.content_type:
                continue
            if replica.cluster_name in self._sync_clusters:
                desired_sync_replicas.add(replica.replica_id)

        if self._table.is_ordered:
            if self._table.is_chaos_replicated:
                for replica_id, replica in dict(result).items():
                    if replica.get("mode") == YtReplica.Mode.SYNC and replica.get("state") == YtReplica.State.ENABLED:
                        actual_sync_replicas.add(replica_id)
            else:
                tablet_infos = parse_tablet_infos(self._response)
                sync_replica_tablets: dict[int] = dict()
                for tablet_info in tablet_infos:
                    total_row_count = int(tablet_info["total_row_count"])
                    for replica_info in tablet_info.get("replica_infos", []):
                        if replica_info.get("mode") != YtReplica.Mode.SYNC:
                            continue
                        committed_replication_row_index = int(replica_info["committed_replication_row_index"])
                        if total_row_count != committed_replication_row_index:
                            continue
                        replica_id = replica_info["replica_id"]
                        if replica_id in sync_replica_tablets:
                            sync_replica_tablets[replica_id] = sync_replica_tablets[replica_id] + 1
                        else:
                            sync_replica_tablets[replica_id] = 1
                for replica_id, sync_tablet_count in sync_replica_tablets.items():
                    if sync_tablet_count == len(tablet_infos):
                        actual_sync_replicas.add(replica_id)

        else:
            actual_sync_replicas: set[str] = set(result)  # type:ignore

        def _format_replicas(replica_ids: set[str]) -> list[str]:
            result = list()
            for replica_id in replica_ids:
                if replica_id in replica_id_to_key:
                    key = replica_id_to_key[replica_id]
                    result.append(f"{key[0]}:{key[1]}#{replica_id}")
                else:
                    result.append(f"#{replica_id}")
            return result

        result = True
        for replica in desired_sync_replicas:
            if replica not in actual_sync_replicas:
                result = False
                break
        LOG.debug(
            "WaitReplicasInSyncAction(%s %s) sync replicas: in_sync=%s, desired=%s, actual=%s",
            self._table.table_type,
            self._table.rich_path,
            result,
            sorted(_format_replicas(desired_sync_replicas)),
            sorted(_format_replicas(actual_sync_replicas)),
        )
        return result
