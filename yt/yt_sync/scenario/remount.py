import logging
from typing import ClassVar

from yt.yt_sync.action import ActionBatch
from yt.yt_sync.action import DbTableActionCollector
from yt.yt_sync.action import MountTableAction
from yt.yt_sync.action import SleepAction
from yt.yt_sync.action import UnmountTableAction
from yt.yt_sync.core.client import YtClientFactory
from yt.yt_sync.core.model import YtCluster
from yt.yt_sync.core.model import YtDatabase
from yt.yt_sync.core.settings import Settings

from .base import ScenarioBase
from .registry import scenario

LOG = logging.getLogger("yt_sync")


@scenario
class RemountScenario(ScenarioBase):
    SCENARIO_NAME: ClassVar[str] = "remount"
    SCENARIO_DESCRIPTION: ClassVar[str] = "Do tables unmount/mount"

    def __init__(self, desired: YtDatabase, actual: YtDatabase, settings: Settings, yt_client_factory: YtClientFactory):
        super().__init__(desired, actual, settings, yt_client_factory)

        self._only_cluster: str | None = None
        self._parallel: bool = False
        self._parallel_batch_size: int | None = None
        self._between_batches_delay: int | None = None

    def setup(self, **kwargs):
        self._only_cluster = kwargs.get("cluster", None)
        self._parallel = kwargs.get("parallel", False)
        self._parallel_batch_size = kwargs.get("parallel_batch_size", None)
        self._between_batches_delay = kwargs.get("batch_delay", None)

    def generate_actions(self) -> list[ActionBatch]:
        affected_clusters = [c.name for c in self.actual.all_clusters]
        if self._only_cluster:
            assert self._only_cluster in self.actual.clusters, f"Unknown cluster {self._only_cluster}"
            affected_clusters = [self._only_cluster]
        if not affected_clusters:
            LOG.info("No clusters to process")
            return []

        LOG.info(
            "Unmount/mount tables for: clusters = %s, parallel = %s, batch_size = %s",
            affected_clusters,
            self._parallel,
            self._parallel_batch_size,
        )

        action_collector = DbTableActionCollector()

        for cluster_name in affected_clusters:
            cluster: YtCluster = self.actual.clusters[cluster_name]
            for table in cluster.tables_sorted:
                if not table.exists or table.is_chaos_replicated:
                    continue
                action_collector.add(cluster.name, table.key, UnmountTableAction(table))
                action_collector.add(cluster.name, table.key, MountTableAction(table))
                if self._between_batches_delay:
                    action_collector.add(cluster.name, table.key, SleepAction(self._between_batches_delay))

        return action_collector.dump(self._parallel, self._parallel_batch_size)
