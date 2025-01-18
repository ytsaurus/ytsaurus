import logging
from typing import Any

from yt.yt_sync.core.client import YtClientProxy
from yt.yt_sync.core.model import YtReplica
from yt.yt_sync.core.model import YtTable

from .base import ActionBase

LOG = logging.getLogger("yt_sync")


class AlterReplicaAttributesAction(ActionBase):
    def __init__(self, desired: YtReplica, actual_replicated_table: YtTable):
        assert (
            actual_replicated_table.is_replicated
        ), f"Can't alter replicas of non-replicated table {actual_replicated_table.rich_path}"
        super().__init__()

        self._desired: YtReplica = desired
        self._actual_replicated_table: YtTable = actual_replicated_table

        self._actual: YtReplica | None = None
        self._scheduled: bool = False
        self._changes: dict[str, Any] = dict()
        self._response: Any | None = None

    def schedule_next(self, batch_client: YtClientProxy) -> bool:
        assert not self._scheduled, "Can't call schedule_next() more than one time"
        self._scheduled = True

        self._fetch_actual_replica()

        for attr_path, desired_value, actual_value in self._desired.changed_attributes(self._actual):
            if attr_path in ("mode"):
                continue
            self._apply_change(attr_path, desired_value, actual_value)

        if self._actual.enabled != self._desired.enabled:
            self._apply_change("enabled", self._desired.enabled, self._actual.enabled)
        # COMPAT(dgolear): Drop after 24.2 is released.
        if (
            len(self._changes) == 1
            and "enable_replicated_table_tracker" in self._changes
            and not self._actual_replicated_table.is_chaos_replicated
        ):
            LOG.warning("Updating unchanged attribute `enabled` as a workaround for YT-21258 bug")
            self._apply_change("enabled", self._desired.enabled, self._actual.enabled)
        if self._changes:
            self._response = batch_client.alter_table_replica(self._actual.replica_id, **self._changes)
        return False

    def process(self):
        assert self._scheduled, "Can't call process before schedule"
        if self._response is not None:
            self.assert_response(self._response)
        self._actual.update_from_attributes(self._changes)

    def _apply_change(self, attr_path: str, desired_value: Any, actual_value: Any):
        LOG.warning(
            "Change replica #%s (%s:%s) attribute '%s': actual=%s, desired=%s",
            self._actual.replica_id,
            self._actual.cluster_name,
            self._actual.replica_path,
            attr_path,
            actual_value,
            desired_value,
        )
        self._changes[attr_path] = desired_value

    def _fetch_actual_replica(self) -> YtReplica:
        assert self._desired.key in self._actual_replicated_table.replicas, (
            f"No desired replica for {self._desired.cluster_name}:{self._desired.replica_path} "
            + f" in {self._actual_replicated_table.rich_path}"
        )

        self._actual = self._actual_replicated_table.replicas[self._desired.key]
        assert (
            self._actual.cluster_name == self._desired.cluster_name
        ), f"Replica cluster mismatch: desired={self._desired.cluster_name}, actual={self._actual.cluster_name}"
        assert (
            self._actual.replica_path == self._desired.replica_path
        ), f"Replica path mismatch: desired={self._desired.replica_path}, actual={self._actual.replica_path}"
        assert (
            self._actual.content_type == self._desired.content_type
        ), f"Replica content type mismatch: desired={self._desired.content_type}, actual={self._actual.content_type}"
        assert self._actual.replica_id, f"No id for replica {self._actual.cluster_name}:{self._actual.replica_path}"
        assert (
            self._actual.exists
        ), f"Can't alter non-existing replica {self._actual.cluster_name}:{self._actual.replica_path}"
