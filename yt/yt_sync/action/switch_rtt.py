import logging
from typing import Any

from yt.yt_sync.core.client import YtClientProxy
from yt.yt_sync.core.model import YtTable
from yt.yt_sync.core.yt_commands import make_yt_commands

from .base import ActionBase

LOG = logging.getLogger("yt_sync")


class SwitchRttAction(ActionBase):
    def __init__(self, actual_table: YtTable, desired_table: YtTable, enable: bool):
        assert actual_table.is_replicated
        assert desired_table.is_replicated

        super().__init__()
        self._actual_table: YtTable = actual_table
        self._desired_table: YtTable = desired_table
        self._enable: bool = enable

        self._scheduled: bool = False
        self._table_response: Any | None = None

    def schedule_next(self, batch_client: YtClientProxy) -> bool:
        assert not self._scheduled
        self._scheduled = True
        assert self._actual_table.exists
        if self._actual_table.is_rtt_enabled == self._enable:
            LOG.info(
                "Table %s:%s RTT is already at desired state: enabled=%s",
                self._actual_table.cluster_name,
                self._actual_table.path,
                self._actual_table.is_rtt_enabled,
            )
            return False

        if self._enable:
            if not self._desired_table.is_rtt_enabled:
                LOG.info("No need to enable RTT for %s:%s", self._actual_table.cluster_name, self._actual_table.path)
                return False
            self._switch_rtt(batch_client)
        else:
            if not self._actual_table.is_rtt_enabled:
                LOG.info("RTT already disabled for %s:%s", self._actual_table.cluster_name, self._actual_table.path)
                return False
            self._switch_rtt(batch_client)
        return False

    def process(self):
        assert self._scheduled
        if self._table_response is None:
            return
        self.assert_response(self._table_response)
        self._actual_table.is_rtt_enabled = self._enable

    def _switch_rtt(self, batch_client: YtClientProxy):
        LOG.warning(
            "Set %s %s:%s attribute 'replicated_table_options/enable_replicated_table_tracker': actual=%s, desired=%s",
            self._actual_table.table_type,
            self._actual_table.cluster_name,
            self._actual_table.path,
            self._actual_table.is_rtt_enabled,
            self._enable,
        )

        yt_commands = make_yt_commands(self._actual_table.is_chaos_replicated)
        self._table_response = yt_commands.set_table_rtt_enabled(batch_client, self._actual_table, self._enable)
