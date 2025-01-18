import logging
from typing import ClassVar

from yt.yt_sync.action import ActionBatch
from yt.yt_sync.action import SwitchReplicaModeAction
from yt.yt_sync.action import TableActionCollector
from yt.yt_sync.action import WaitReplicasInSyncAction
from yt.yt_sync.core.client import YtClientFactory
from yt.yt_sync.core.model import YtDatabase
from yt.yt_sync.core.settings import Settings

from .base import ScenarioBase
from .helpers import CollocationSwitcher
from .registry import scenario

LOG = logging.getLogger("yt_sync")


@scenario
class SwitchReplicaScenario(ScenarioBase):
    SCENARIO_NAME: ClassVar[str] = "switch_replica"
    SCENARIO_DESCRIPTION: ClassVar[str] = "Set tables sync replicas"

    def __init__(
        self,
        desired: YtDatabase,
        actual: YtDatabase,
        settings: Settings,
        yt_client_factory: YtClientFactory,
    ):
        super().__init__(desired, actual, settings, yt_client_factory)
        self._desired_sync_replicas: set[str] = set()
        self._force_fix_preferred: bool = False
        self._update_max_sync_replica_count: bool = False

    def setup(self, **kwargs):
        self._desired_sync_replicas = set(kwargs.get("desired_sync_replicas", []))
        self._force_fix_preferred = kwargs.get("force_fix_preferred", False)
        self._update_max_sync_replica_count = kwargs.get("update_max_sync_replica_count", False)

    def pre_action(self):
        super().pre_action()
        assert not (
            self._desired_sync_replicas & self.settings.always_async
        ), f"Desired sync {self._desired_sync_replicas} intersects with always async {self.settings.always_async}"

    def generate_actions(self) -> list[ActionBatch]:
        actual_main = self.actual.main
        action_collector = TableActionCollector(actual_main.name)
        collocation_switcher = CollocationSwitcher()
        for table in actual_main.tables_sorted:
            if table.is_replicated and table.exists:
                collocation_switcher.use(table, self._desired_sync_replicas)
                chaotic_rtt_queue = (
                    table.is_chaos_replicated
                    and table.is_ordered
                    and table.is_rtt_enabled
                    and table.has_rtt_enabled_replicas
                )
                action_collector.add(
                    table.key,
                    SwitchReplicaModeAction(
                        table,
                        self._desired_sync_replicas,
                        strict_preferred=not chaotic_rtt_queue,
                        force_fix_preferred=chaotic_rtt_queue or self._force_fix_preferred,
                        max_sync_replica_count=(
                            len(self._desired_sync_replicas) if self._update_max_sync_replica_count else None
                        ),
                    ),
                )
                action_collector.add(table.key, WaitReplicasInSyncAction(table, self._desired_sync_replicas))
            else:
                LOG.info("Skip table %s because it is not replicated or does not exist", table.rich_path)
        collocation_switcher.add_actions(action_collector)
        return action_collector.dump()
