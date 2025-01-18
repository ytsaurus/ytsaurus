import logging
from typing import ClassVar

from yt.yt_sync.action import ActionBatch
from yt.yt_sync.action import CleanTemporaryObjectsAction
from yt.yt_sync.action import CreateReplicatedTableAction
from yt.yt_sync.action import DbTableActionCollector
from yt.yt_sync.action import FreezeTableAction
from yt.yt_sync.action import MountTableAction
from yt.yt_sync.action import RemoveTableAction
from yt.yt_sync.action import ReshardTableAction
from yt.yt_sync.action import SetUpstreamReplicaAction
from yt.yt_sync.action import UnmountTableAction
from yt.yt_sync.action import WaitReplicasFlushedAction
from yt.yt_sync.core.client import YtClientFactory
from yt.yt_sync.core.diff import DbDiff
from yt.yt_sync.core.diff import TableDiffSet
from yt.yt_sync.core.diff import TableDiffType
from yt.yt_sync.core.diff import TabletCountChange
from yt.yt_sync.core.model import YtCluster
from yt.yt_sync.core.model import YtDatabase
from yt.yt_sync.core.model import YtTable
from yt.yt_sync.core.settings import Settings

from .base import ScenarioBase
from .helpers import ChaosTableRttController
from .helpers import ChaosToolbox
from .helpers import check_and_log_db_diff
from .helpers import ensure_collocation
from .helpers import generate_wait_until_in_memory_preloaded
from .helpers import ReplicaSwitcher
from .helpers import should_switch_replicas
from .registry import scenario

LOG = logging.getLogger("yt_sync")


@scenario
class ReshardScenario(ScenarioBase):
    SCENARIO_NAME: ClassVar[str] = "reshard"
    SCENARIO_DESCRIPTION: ClassVar[str] = "Reshard tables"

    def __init__(
        self,
        desired: YtDatabase,
        actual: YtDatabase,
        settings: Settings,
        yt_client_factory: YtClientFactory,
    ):
        super().__init__(desired, actual, settings, yt_client_factory)
        self.db_diff: DbDiff = DbDiff.generate(settings, self.desired, self.actual)
        self.affected_table_keys: set[str] = set()
        self.skipped_table_keys: set[str] = set()

    @property
    def diff_types(self) -> set[int]:
        return set([TableDiffType.TABLET_COUNT_CHANGE])

    def pre_action(self):
        assert check_and_log_db_diff(self.settings, self.desired, self.db_diff, self.diff_types).is_valid, "Bad diff"

        self.affected_table_keys: set[str] = set(
            [diff.table_key for diff in self.db_diff.table_diff_for(cluster_name=None, diff_types=self.diff_types)]
        )

        main_actual_cluster = self.actual.main
        for table_key in self.affected_table_keys:
            table = main_actual_cluster.tables[table_key]
            # TODO: remove after https://st.yandex-team.ru/YTSYNC-13
            if table.table_type == YtTable.Type.REPLICATED_TABLE and table.is_ordered:
                self.skipped_table_keys.add(table_key)
                table = main_actual_cluster.tables[table_key]
                LOG.warning("Skip resharding replicated ordered table %s, consider recreating one", table.rich_path)

    def post_action(self):
        super().post_action()
        if self.settings.ensure_collocation:
            self.has_diff |= ensure_collocation(
                self.desired.main, self.actual.main, self.yt_client_factory, self.settings
            )

    def generate_actions(self) -> list[ActionBatch]:
        if not (self.affected_table_keys ^ self.skipped_table_keys):
            LOG.info("No effective diff")
            return []

        result: list[ActionBatch] = list()

        rtt_controller = ChaosTableRttController(self.desired.main, self.actual.main)
        sync_replicas = set([r.name for r in self.actual.replicas]) - self.settings.always_async
        if self.settings.is_chaos:
            result.extend(rtt_controller.switch_rtt_off(sync_replicas, self.affected_table_keys))

        switcher = ReplicaSwitcher(self.settings, self.actual)
        main_actual_cluster = self.actual.main
        for actual_cluster, _ in switcher.clusters():
            desired_cluster = self.desired.clusters[actual_cluster.name]
            unmounted_tables: set[str] = set()
            action_collector: DbTableActionCollector = DbTableActionCollector()
            for diff_set in self.db_diff.table_diff_for(cluster_name=actual_cluster.name, diff_types=self.diff_types):
                if not self._has_diff_for(actual_cluster, diff_set):
                    continue
                if should_switch_replicas(self.settings, is_unmount_required=True, is_data_modification_required=False):
                    result.extend(switcher.make_async(actual_cluster.name))
                for table_diff in diff_set.diffs:
                    assert isinstance(table_diff, TabletCountChange)
                    for desired_table, actual_table in table_diff.get_diff_for(actual_cluster.name):
                        if actual_table.key in self.skipped_table_keys:
                            continue
                        match desired_table.table_type:
                            case YtTable.Type.CHAOS_REPLICATED_TABLE:
                                # nothing to reshard
                                pass
                            case YtTable.Type.REPLICATED_TABLE:
                                if actual_table.is_ordered:
                                    self._make_actions_for_replicated_ordered(
                                        action_collector, actual_cluster, desired_table, actual_table
                                    )
                                else:
                                    self._make_actions_for_replicated(
                                        action_collector, actual_cluster, desired_table, actual_table
                                    )
                            case YtTable.Type.REPLICATION_LOG:
                                self._make_actions_for_replication_log(
                                    action_collector,
                                    main_actual_cluster,
                                    desired_cluster,
                                    actual_cluster,
                                    desired_table,
                                    actual_table,
                                )
                            case YtTable.Type.TABLE:
                                unmounted_tables.add(actual_table.key)
                                self._make_actions_for_generic_table(
                                    action_collector, actual_cluster, desired_table, actual_table
                                )
            if self.settings.is_chaos:
                action_collector.add(main_actual_cluster.name, "_", CleanTemporaryObjectsAction(main_actual_cluster))
                action_collector.add(actual_cluster.name, "_", CleanTemporaryObjectsAction(actual_cluster))
                action_collector.add(actual_cluster.name, "_", CleanTemporaryObjectsAction(desired_cluster))

            result.extend(
                action_collector.dump(
                    self.settings.parallel_processing_for_unmounted,
                    self.settings.get_batch_size_for_parallel(self.SCENARIO_NAME),
                )
            )
            result.extend(generate_wait_until_in_memory_preloaded(self.settings, actual_cluster, unmounted_tables))

        if self.settings.is_chaos:
            result.extend(rtt_controller.switch_rtt_on(switcher.get_sync_clusters(), self.affected_table_keys))
        result.extend(switcher.ensure_sync_mode(self.desired))

        return result

    def _make_actions_for_replicated(
        self,
        action_collector: DbTableActionCollector,
        actual_cluster: YtCluster,
        desired_table: YtTable,
        actual_table: YtTable,
    ):
        action_collector.add(actual_cluster.name, actual_table.key, FreezeTableAction(actual_table))
        action_collector.add(actual_cluster.name, actual_table.key, WaitReplicasFlushedAction(actual_table))
        action_collector.add(actual_cluster.name, actual_table.key, RemoveTableAction(actual_table))
        action_collector.add(
            actual_cluster.name,
            actual_table.key,
            CreateReplicatedTableAction(desired_table, actual_table),
        )
        action_collector.add(
            actual_cluster.name,
            actual_table.key,
            ReshardTableAction(desired_table, actual_table),
        )

        for replica_cluster in self.actual.replicas:
            replica_table = replica_cluster.tables[actual_table.key]
            action_collector.add(replica_cluster.name, actual_table.key, UnmountTableAction(replica_table))
            action_collector.add(
                replica_cluster.name,
                actual_table.key,
                SetUpstreamReplicaAction(replica_table, actual_table),
            )
            action_collector.add(replica_cluster.name, actual_table.key, MountTableAction(replica_table))

        action_collector.add(
            actual_cluster.name,
            actual_table.key,
            MountTableAction(actual_table),
        )

    def _make_actions_for_replicated_ordered(
        self,
        action_collector: DbTableActionCollector,
        actual_cluster: YtCluster,
        desired_table: YtTable,
        actual_table: YtTable,
    ):
        assert False, "Can't be used until https://st.yandex-team.ru/YTSYNC-13"
        action_collector.add(actual_cluster.name, actual_table.key, FreezeTableAction(actual_table))
        action_collector.add(actual_cluster.name, actual_table.key, WaitReplicasFlushedAction(actual_table))

        action_collector.add(
            actual_cluster.name,
            actual_table.key,
            UnmountTableAction(actual_table),
        )

        # TODO: change actions instead of reshard:
        #  - recreate replicated ordered table with initial non-zero trimmed_row_count
        #  - set upstream replica id for replicas
        action_collector.add(
            actual_cluster.name,
            actual_table.key,
            ReshardTableAction(desired_table, actual_table),
        )

        action_collector.add(
            actual_cluster.name,
            actual_table.key,
            MountTableAction(actual_table),
        )

    def _make_actions_for_replication_log(
        self,
        action_collector: DbTableActionCollector,
        main_actual_cluster: YtCluster,
        desired_cluster: YtCluster,
        actual_cluster: YtCluster,
        desired_log: YtTable,
        actual_log: YtTable,
    ):
        desired_data_table = desired_cluster.tables[desired_log.chaos_data_table]
        actual_data_table = actual_cluster.tables[actual_log.chaos_data_table]

        replicated_table = self.actual.get_replicated_table_for(actual_data_table)
        desired_log_replica = self.desired.get_replicated_table_for(desired_data_table).replicas[
            desired_log.replica_key
        ]

        actual_tmp_log = actual_cluster.get_or_add_temporary_table_for(actual_log, exists=False)
        desired_tmp_log = desired_cluster.get_or_add_temporary_table_for(desired_log, exists=False)

        chaos_toolbox: ChaosToolbox = ChaosToolbox(
            self.settings,
            action_collector,
            replicated_table,
            main_actual_cluster.name,
            actual_cluster.name,
            actual_log.key,
        )

        desired_tmp_log.sync_user_attributes(actual_log)
        chaos_toolbox.create_replicated_log(
            desired_log_replica, desired_tmp_log, actual_tmp_log, actual_create_source=True, actual_reshard_source=False
        )
        chaos_toolbox.mount_replication_log(actual_tmp_log)
        chaos_toolbox.wait_replication(actual_data_table)
        chaos_toolbox.remove_replicated_log(actual_log)
        chaos_toolbox.create_replicated_log(
            desired_log_replica, desired_log, actual_log, actual_create_source=True, actual_reshard_source=False
        )
        chaos_toolbox.mount_replication_log(actual_log)
        chaos_toolbox.wait_replication(actual_data_table)
        chaos_toolbox.remove_replicated_log(actual_tmp_log)

    def _make_actions_for_generic_table(
        self,
        action_collector: DbTableActionCollector,
        actual_cluster: YtCluster,
        desired_table: YtTable,
        actual_table: YtTable,
    ):
        action_collector.add(actual_cluster.name, actual_table.key, UnmountTableAction(actual_table))
        action_collector.add(actual_cluster.name, actual_table.key, ReshardTableAction(desired_table, actual_table))
        action_collector.add(actual_cluster.name, actual_table.key, MountTableAction(actual_table))

    def _has_diff_for(self, actual_cluster: YtCluster, diff_set: TableDiffSet) -> bool:
        assert len(diff_set.diffs) == 1, "Unexpected diff count"
        table_diff = diff_set.diffs[0]
        assert (
            table_diff.diff_type == TableDiffType.TABLET_COUNT_CHANGE
        ), f"Unexpected diff type {table_diff.__class__.__name__}"
        return table_diff.has_diff_for(actual_cluster.name)
