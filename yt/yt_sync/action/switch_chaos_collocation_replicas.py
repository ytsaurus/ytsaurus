import logging
from typing import Any

from yt.yt_sync.core.client.proxy import YtClientProxy
from yt.yt_sync.core.model import YtTable

from .base import ActionBase

LOG = logging.getLogger("yt_sync")


class SwitchChaosCollocationReplicasAction(ActionBase):
    def __init__(self, source: YtTable, sync_replicas: set[str]):
        super().__init__()

        self.source_table: YtTable = source
        self.sync_replicas: set[str] = sync_replicas

        self._scheduled: bool = False
        self._done: bool = False
        self._response: Any | None = None

    def schedule_next(self, batch_client: YtClientProxy) -> bool:
        if self._done:
            return False
        assert not self._scheduled, "Can't call schedule_next more than one time"
        self._scheduled = True
        assert (
            self.source_table.chaos_replication_card_id
        ), f"Can't use table {self.source_table.rich_path} without 'replication_card_id' property"

        collocation_options = {"preferred_sync_replica_clusters": sorted(self.sync_replicas)}

        LOG.warning(
            "Update replication card #%s (for %s) collocation_options to: %s",
            self.source_table.chaos_replication_card_id,
            self.source_table.rich_path,
            collocation_options,
        )
        self._response = batch_client.alter_replication_card(
            self.source_table.chaos_replication_card_id, collocation_options=collocation_options
        )

        return False

    def process(self):
        if self._done:
            return
        assert self._scheduled, "Can't process before schedule"

        if not self.dry_run:
            self.assert_response(self._response)
        self._done = True
