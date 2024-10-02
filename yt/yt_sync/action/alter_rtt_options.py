import logging
from typing import Any
from typing import ClassVar

from yt.yt_sync.core.client import YtClientProxy
from yt.yt_sync.core.model import RttOptions
from yt.yt_sync.core.model import YtTable
from yt.yt_sync.core.yt_commands import make_yt_commands

from .base import ActionBase

LOG = logging.getLogger("yt_sync")


class AlterRttOptionsAction(ActionBase):
    _PREFERRED_SYNC_ATTR: ClassVar[str] = "replicated_table_options/preferred_sync_replica_clusters"

    def __init__(self, desired_table: YtTable, actual_table: YtTable):
        assert desired_table.is_replicated, f"Desired table {desired_table.rich_path} must be replicated"
        assert desired_table.is_rtt_enabled, f"Desired table {desired_table.rich_path} must be under RTT"
        assert actual_table.is_replicated, f"Actual table {actual_table.rich_path} must be replicated"
        super().__init__()

        self._desired: YtTable = desired_table
        self._actual: YtTable = actual_table
        self._scheduled: bool = False
        self._responses: list[Any] | None = None
        self._rtt_options: dict[str, Any] = {}

    def schedule_next(self, batch_client: YtClientProxy) -> bool:
        assert not self._scheduled, "Can't call schedule_next() more than one time"
        self._scheduled = True

        assert self._actual.exists, f"Can't process non-existing table {self._actual.rich_path}"

        has_diff: bool = False
        desired_sync_replicas: set[str] = self._desired.sync_replicas
        actual_sync_replicas: set[str] = set(self._actual.rtt_options.preferred_sync_replica_clusters or [])
        if desired_sync_replicas != actual_sync_replicas:
            LOG.warning(
                "Update attribute %s/@%s: actual=%s, desired=%s",
                self._desired.rich_path,
                self._PREFERRED_SYNC_ATTR,
                sorted(actual_sync_replicas),
                sorted(desired_sync_replicas),
            )
            has_diff = True

        for attribute_path, desired_value, actual_value in self._desired.rtt_options.changed_attributes(
            self._actual.rtt_options
        ):
            if attribute_path == self._PREFERRED_SYNC_ATTR:
                continue

            if desired_value != actual_value:
                LOG.warning(
                    "Update attribute %s/@%s: actual=%s, desired=%s",
                    self._desired.rich_path,
                    attribute_path,
                    actual_value,
                    desired_value,
                )
                has_diff = True

        if has_diff:
            self._rtt_options = self._actual.rtt_options.yt_attributes
            self._rtt_options.update(self._desired.rtt_options.yt_attributes)
            self._rtt_options["preferred_sync_replica_clusters"] = sorted(desired_sync_replicas)
            yt_commands = make_yt_commands(self._desired.is_chaos_replicated)
            self._responses = yt_commands.update_rtt_options(batch_client, self._actual, self._rtt_options)
        return False

    def process(self):
        assert self._scheduled, "Can't call process before schedule"
        if not self._responses:
            self._actual.need_remount = False
            return
        for response in self._responses:
            self.assert_response(response)
        self._actual.rtt_options = RttOptions.make(self._rtt_options)
        self._actual.need_remount = True
