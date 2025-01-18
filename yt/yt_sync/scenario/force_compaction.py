from dataclasses import dataclass
import logging
from typing import ClassVar

from yt.yt_sync.action import ActionBatch
from yt.yt_sync.action import CleanupForceCompactionAttributesAction
from yt.yt_sync.action import DbTableActionCollector
from yt.yt_sync.action import FORCED_COMPACTION_REVISION
from yt.yt_sync.action import GradualRemountActon
from yt.yt_sync.action import SetForceCompactionRevisionAction
from yt.yt_sync.action import SLEEP_AFTER_TABLET
from yt.yt_sync.action import TABLETS_REMOUNTED
from yt.yt_sync.core import DefaultTableFilter
from yt.yt_sync.core import YtClientProxy
from yt.yt_sync.core.model import YtCluster
from yt.yt_sync.core.model import YtTable

from .base import ScenarioBase
from .helpers import ReplicaSwitcher
from .registry import scenario

LOG = logging.getLogger("yt_sync")

_COMPACTION_RATE_PER_NODE = 100 * 1024 * 1024


@dataclass
class ForceCompactOptions:
    sleep_after_tablet: float = 1
    forced_compaction_revision: int | None = None
    tablet_count: int = 1
    tablets_remounted: int = 0


@scenario
class ForceCompaction(ScenarioBase):
    SCENARIO_NAME: ClassVar[str] = "force_compaction"
    SCENARIO_DESCRIPTION: ClassVar[str] = "Gradually force-compact tables"

    def setup(self, **kwargs):
        self.use_clients_cache = True
        # TODO(hitsedesen): Get tablet node count from bundle controller settings.
        self._tablet_node_count: float = kwargs.get("tablet_node_count")
        self._restart_compaction = kwargs.get("restart_compaction", False)
        self._force_compact_filter = kwargs.get("force_compact_filter", DefaultTableFilter())
        self._force_compact_clusters: list[str] | None = kwargs.get("force_compact_clusters", None)
        self._tables_to_compact_per_cluster: dict[str, dict[str, ForceCompactOptions]] = {}
        assert self._tablet_node_count, "tablet_node_count must be provided to calculate remount time"

    def _get_in_progress_tablets_remounted(self, yt_client: YtClientProxy, table: YtTable) -> int | None:
        existing_last_remounted_tablet = yt_client.get(f"{table.path}/@{TABLETS_REMOUNTED}")
        if existing_last_remounted_tablet is not None:
            LOG.info(
                "Found remount in progress for table %s:%s%s",
                table.cluster_name,
                table.path,
                ", removing" if self._restart_compaction else "",
            )
            if self._restart_compaction:
                yt_client.remove(f"{table.path}/@{TABLETS_REMOUNTED}")
                return None
        return existing_last_remounted_tablet

    def _get_existing_sleep_after_tablet(self, yt_client: YtClientProxy, table: YtTable) -> float | None:
        if self._restart_compaction:
            return None
        return yt_client.get(f"{table.path}/@{SLEEP_AFTER_TABLET}")

    def _get_existing_forced_compaction_revision(self, yt_client: YtClientProxy, table: YtTable) -> int | None:
        return yt_client.get(f"{table.path}/@{FORCED_COMPACTION_REVISION}")

    def _prepare_compaction_for_table(self, yt_client: YtClientProxy, table: YtTable) -> ForceCompactOptions | None:
        if not table.exists:
            return None
        if not self._force_compact_filter.is_ok(table.key):
            return None
        if table.table_type != YtTable.Type.TABLE or table.is_ordered:
            return None
        tablet_count: int = table.tablet_count
        tablets_remounted = self._get_in_progress_tablets_remounted(yt_client, table) or 0
        existing_sleep_after_tablet = self._get_existing_sleep_after_tablet(yt_client, table)
        data_weight: int = table.attributes.get("data_weight")
        if existing_sleep_after_tablet:
            estimated_time = (tablet_count - tablets_remounted) * existing_sleep_after_tablet
            LOG.info(
                "Table %s:%s will be force compacted using existing settings. "
                "Data weight: %d, estimated compaction time: %.2f seconds. "
                "Tablet remount wait: %.2f seconds",
                table.cluster_name,
                table.path,
                data_weight,
                estimated_time,
                existing_sleep_after_tablet,
            )
            sleep_after_tablet = existing_sleep_after_tablet
        else:
            table_remount_time = data_weight / self._tablet_node_count / _COMPACTION_RATE_PER_NODE
            sleep_after_tablet = max(table_remount_time / tablet_count, 1)
            yt_client.set(f"{table.path}/@{SLEEP_AFTER_TABLET}", sleep_after_tablet)
            estimated_time = sleep_after_tablet * tablet_count
            LOG.info(
                "Table %s:%s will be force compacted. "
                "Data weight: %d, estimated compaction time: %.2f seconds. "
                "Tablet remount wait: %.2f seconds",
                table.cluster_name,
                table.path,
                data_weight,
                estimated_time,
                sleep_after_tablet,
            )
        return ForceCompactOptions(
            sleep_after_tablet=sleep_after_tablet,
            forced_compaction_revision=self._get_existing_forced_compaction_revision(yt_client, table),
            tablet_count=table.tablet_count or 1,
            tablets_remounted=tablets_remounted,
        )

    def _prepare_compaction_for_cluster(self, cluster: YtCluster):
        if self._force_compact_clusters is not None and cluster.name not in self._force_compact_clusters:
            return
        yt_client = self.yt_client_factory(cluster.name)
        self._tables_to_compact_per_cluster[cluster.name] = {}
        for key, table in cluster.tables.items():
            options = self._prepare_compaction_for_table(yt_client, table)
            if options is not None:
                self._tables_to_compact_per_cluster[cluster.name][key] = options
        total_time = 0
        for cluster_name, tables in self._tables_to_compact_per_cluster.items():
            cluster_time = 0
            for key, options in tables.items():
                table_time = options.sleep_after_tablet * options.tablet_count
                cluster_time += table_time
            LOG.info("Estimated time to force compact cluster %s: %.2f seconds", cluster_name, cluster_time)
            total_time += cluster_time
        LOG.info("Total estimated compaction time: %.2f seconds", total_time)

    def pre_action(self):
        LOG.info("Preparing tables for force compaction")
        for cluster in self.actual.data_clusters:
            self._prepare_compaction_for_cluster(cluster)

    def generate_actions(self) -> list[ActionBatch]:
        result: list[ActionBatch] = list()
        switcher = ReplicaSwitcher(self.settings, self.actual)
        for cluster, _ in switcher.clusters():
            to_compact = self._tables_to_compact_per_cluster.get(cluster.name, {})
            if not to_compact:
                continue
            if self.settings.switch_replicas_mode_always:
                result.extend(switcher.make_async(cluster.name))
            for table_key, options in to_compact.items():
                table = cluster.tables[table_key]
                if self._restart_compaction:
                    compaction_revision = options.forced_compaction_revision
                    if options.forced_compaction_revision is None:
                        compaction_revision = 0
                    compaction_revision += 1
                    result.append(
                        ActionBatch(
                            cluster_name=cluster.name,
                            actions=[SetForceCompactionRevisionAction(table, compaction_revision)],
                        )
                    )
                elif options.tablets_remounted >= options.tablet_count:
                    continue
                elif options.forced_compaction_revision is None:
                    result.append(
                        ActionBatch(
                            cluster_name=cluster.name,
                            actions=[SetForceCompactionRevisionAction(table, 1)],
                        )
                    )
                result.append(
                    ActionBatch(
                        cluster_name=cluster.name,
                        actions=[GradualRemountActon(table, options.sleep_after_tablet)],
                    )
                )
        if self.settings.switch_replicas_mode_always:
            result.extend(switcher.ensure_sync_mode(self.desired))
        cleanup_actions = DbTableActionCollector()
        for cluster in self.actual.data_clusters:
            to_compact = self._tables_to_compact_per_cluster.get(cluster.name, {})
            for table_key, options in to_compact.items():
                table = cluster.tables[table_key]
                cleanup_actions.add(cluster.name, table_key, CleanupForceCompactionAttributesAction(table))
        result.extend(cleanup_actions.dump(parallel=True))
        return result
