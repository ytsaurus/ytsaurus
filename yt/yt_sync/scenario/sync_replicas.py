import logging
from typing import ClassVar

from yt.yt_sync.action import ActionBatch
from yt.yt_sync.action import CloneChaosReplicaAction
from yt.yt_sync.action import CloneReplicaAction
from yt.yt_sync.action import CreateOrderedTableFromAction
from yt.yt_sync.action import CreateTableAction
from yt.yt_sync.action import DbTableActionCollector
from yt.yt_sync.action import EliminateChunkViews
from yt.yt_sync.action import FreezeTableAction
from yt.yt_sync.action import MountTableAction
from yt.yt_sync.action import ReadReplicationProgressAction
from yt.yt_sync.action import ReadTotalRowCountAction
from yt.yt_sync.action import RemoteCopyAction
from yt.yt_sync.action import RemoveReplicaAction
from yt.yt_sync.action import RemoveTableAction
from yt.yt_sync.action import ReshardTableAction
from yt.yt_sync.action import SetReplicationProgressAction
from yt.yt_sync.action import SetUpstreamReplicaAction
from yt.yt_sync.action import SwitchReplicaStateAction
from yt.yt_sync.action import UnfreezeTableAction
from yt.yt_sync.action import UnmountTableAction
from yt.yt_sync.core.client import YtClientFactory
from yt.yt_sync.core.diff import DbDiff
from yt.yt_sync.core.diff import NodeDiffType
from yt.yt_sync.core.diff import ReplicasChange
from yt.yt_sync.core.diff import TableDiffType
from yt.yt_sync.core.model import YtDatabase
from yt.yt_sync.core.model import YtReplica
from yt.yt_sync.core.model import YtTable
from yt.yt_sync.core.settings import Settings

from .base import ScenarioBase
from .helpers import ChaosToolbox
from .helpers import check_and_log_db_diff
from .helpers import NodeManager
from .registry import scenario

LOG = logging.getLogger("yt_sync")


@scenario
class SyncReplicasScenario(ScenarioBase):
    SCENARIO_NAME: ClassVar[str] = "sync_replicas"
    SCENARIO_DESCRIPTION: ClassVar[str] = "Add/remove replica clusters"

    def __init__(
        self,
        desired: YtDatabase,
        actual: YtDatabase,
        settings: Settings,
        yt_client_factory: YtClientFactory,
    ):
        super().__init__(desired, actual, settings, yt_client_factory)
        self.source_cluster_name: str | None = None
        self.remove_tables: bool = False
        self.db_diff: DbDiff = DbDiff.generate(settings, self.desired, self.actual)
        self.node_manager: NodeManager = NodeManager(self.yt_client_factory, settings)

    def setup(self, **kwargs):
        super().setup(**kwargs)
        self.source_cluster_name = kwargs.get("source_cluster", None)
        self.remove_tables = bool(kwargs.get("remove_tables", False))

    @property
    def diff_types(self) -> set[int]:
        return set([TableDiffType.REPLICAS_CHANGE]) | NodeDiffType.all()

    def pre_action(self):
        super().pre_action()
        if self._has_missing_replicas():
            unavailable_source_clusters = self._get_clusters_with_missing_replicas()
            available_async_source_clusters = (
                self.settings.always_async | set(map(lambda cluster: cluster.name, self.actual.async_replicas_relaxed))
            ) - unavailable_source_clusters

            assert self.source_cluster_name, (
                "Required scenario parameter `source_cluster` is missing; "
                f"to prevent downtime, you may use one of current async replicas ({available_async_source_clusters})"
            )
            assert (
                self.source_cluster_name in self.actual.clusters
            ), f"Unknown source cluster {self._source_cluster_name}"
            assert (
                self.source_cluster_name not in unavailable_source_clusters
            ), f"Can't use same cluster '{self.source_cluster_name}' for missing replica and source"
        assert check_and_log_db_diff(self.settings, self.desired, self.db_diff, self.diff_types).is_valid, "Bad diff"
        if self.settings.ensure_folders:
            self.has_diff |= self.node_manager.ensure_nodes_from_diff(self.db_diff, self.actual)

    def post_action(self):
        super().post_action()
        if self.settings.ensure_folders:
            self.has_diff |= self.node_manager.ensure_links_from_diff(self.db_diff, self.actual)

    def generate_actions(self) -> list[ActionBatch]:
        has_changes: bool = (
            next(self.db_diff.table_diff_for(cluster_name=None, diff_types=self.diff_types), None) is not None
        )
        if not has_changes:
            LOG.info("No diff")
            return []

        action_collector: DbTableActionCollector = DbTableActionCollector()

        # remove replicas
        for diff_set in self.db_diff.table_diff_for(cluster_name=None, diff_types=self.diff_types):
            for table_diff in diff_set.diffs:
                assert isinstance(table_diff, ReplicasChange)
                table_key: str = table_diff.actual_main_table.key
                actual_table: YtTable = table_diff.actual_main_table
                main_cluster_name: str = actual_table.cluster_name
                if table_diff.has_deleted_replicas():
                    for replica in table_diff.deleted_replicas():
                        replica_table = self._get_replica_table(replica, table_key, actual_table)
                        action_collector.add(
                            main_cluster_name,
                            table_key,
                            SwitchReplicaStateAction(
                                table_diff.actual_main_table, enabled=False, for_table=replica_table
                            ),
                        )
                        action_collector.add(
                            main_cluster_name, table_key, RemoveReplicaAction(actual_table, replica_table)
                        )
                        if self.remove_tables:
                            action_collector.add(
                                replica_table.cluster_name, table_key, RemoveTableAction(replica_table)
                            )
                        else:
                            LOG.info("Skipping tables removal since `remove_tables` flag is not set")
        # add replicas
        if self._has_missing_replicas():
            for replica_cluster in self.desired.replicas:
                for diff_set in self.db_diff.table_diff_for(cluster_name=None, diff_types=self.diff_types):
                    for table_diff in diff_set.diffs:
                        assert isinstance(table_diff, ReplicasChange)
                        for missing_actual_table in table_diff.missing_replicas_for(replica_cluster.name):
                            missing_desired_table: YtTable = self.desired.clusters[
                                missing_actual_table.cluster_name
                            ].tables[missing_actual_table.key]
                            if missing_desired_table.is_ordered:
                                self._add_replica_for_ordered(
                                    action_collector, table_diff, missing_actual_table, missing_desired_table
                                )
                            else:
                                self._add_replica_for_sorted(
                                    action_collector, table_diff, missing_actual_table, missing_desired_table
                                )

        return action_collector.dump(
            parallel=self.settings.parallel_processing_for_unmounted,
            limit=self.settings.get_batch_size_for_parallel(self.SCENARIO_NAME),
        )

    def _has_missing_replicas(self) -> bool:
        for diff_set in self.db_diff.table_diff_for(cluster_name=None, diff_types=self.diff_types):
            for table_diff in diff_set.diffs:
                assert isinstance(table_diff, ReplicasChange)
                if table_diff.has_missing_replicas():
                    return True
        return False

    def _get_clusters_with_missing_replicas(self) -> set[str]:
        clusters_with_missing_replicas = set()
        for diff_set in self.db_diff.table_diff_for(cluster_name=None, diff_types=self.diff_types):
            for table_diff in diff_set.diffs:
                assert isinstance(table_diff, ReplicasChange)
                for replica_table in table_diff.missing_replicas.values():
                    clusters_with_missing_replicas.add(replica_table.cluster_name)
        return clusters_with_missing_replicas

    def _get_replica_table(self, replica: YtReplica, table_key: str, src_table: YtTable) -> YtTable:
        if replica.cluster_name in self.actual.clusters:
            return self.actual.clusters[replica.cluster_name].tables[table_key]
        return YtTable.make(
            table_key,
            replica.cluster_name,
            YtTable.Type.TABLE,
            replica.replica_path,
            True,
            {"schema": src_table.schema.yt_schema},
        )

    def _add_replica_for_ordered_replicated(
        self,
        action_collector: DbTableActionCollector,
        table_diff: ReplicasChange,
        missing_actual_table: YtTable,
        missing_desired_table: YtTable,
    ):
        table_key: str = table_diff.actual_main_table.key
        source_table: YtTable = self.actual.clusters[self.source_cluster_name].tables[table_key]
        main_cluster_name = self.actual.main.name

        action_collector.add(source_table.cluster_name, table_key, FreezeTableAction(source_table))

        action_collector.add(source_table.cluster_name, table_key, ReadTotalRowCountAction(source_table))

        action_collector.add(
            missing_actual_table.cluster_name,
            table_key,
            CreateOrderedTableFromAction(missing_desired_table, missing_actual_table, source_table),
        )

        action_collector.add(
            main_cluster_name,
            table_key,
            CloneReplicaAction(
                table_diff.desired_main_table,
                table_diff.actual_main_table,
                source_table,
                missing_actual_table,
            ),
        )
        action_collector.add(
            missing_actual_table.cluster_name,
            table_key,
            SetUpstreamReplicaAction(missing_actual_table, table_diff.actual_main_table),
        )
        action_collector.add(
            main_cluster_name,
            table_key,
            SwitchReplicaStateAction(table_diff.actual_main_table, enabled=True, for_table=missing_actual_table),
        )
        action_collector.add(missing_actual_table.cluster_name, table_key, MountTableAction(missing_actual_table))

        action_collector.add(source_table.cluster_name, table_key, UnfreezeTableAction(source_table))

    def _add_replica_for_ordered_chaos(
        self,
        action_collector: DbTableActionCollector,
        table_diff: ReplicasChange,
        missing_actual_table: YtTable,
        missing_desired_table: YtTable,
    ):
        table_key: str = table_diff.actual_main_table.key
        source_table: YtTable = self.actual.clusters[self.source_cluster_name].tables[table_key]
        main_cluster_name = self.actual.main.name

        action_collector.add(source_table.cluster_name, table_key, FreezeTableAction(source_table))
        action_collector.add(source_table.cluster_name, table_key, ReadTotalRowCountAction(source_table))
        action_collector.add(source_table.cluster_name, table_key, UnmountTableAction(source_table))

        action_collector.add(
            missing_actual_table.cluster_name,
            table_key,
            CreateOrderedTableFromAction(missing_desired_table, missing_actual_table, source_table),
        )

        action_collector.add(
            main_cluster_name,
            table_key,
            CloneChaosReplicaAction(
                table_diff.desired_main_table,
                table_diff.actual_main_table,
                source_table,
                missing_actual_table,
            ),
        )
        action_collector.add(
            missing_actual_table.cluster_name,
            table_key,
            SetUpstreamReplicaAction(missing_actual_table, table_diff.actual_main_table),
        )
        action_collector.add(source_table.cluster_name, table_key, ReadReplicationProgressAction(source_table))
        action_collector.add(
            missing_actual_table.cluster_name,
            table_key,
            SetReplicationProgressAction(source_table, missing_actual_table),
        )

        action_collector.add(
            main_cluster_name,
            table_key,
            SwitchReplicaStateAction(table_diff.actual_main_table, enabled=True, for_table=missing_actual_table),
        )
        action_collector.add(missing_actual_table.cluster_name, table_key, MountTableAction(missing_actual_table))

        action_collector.add(source_table.cluster_name, table_key, MountTableAction(source_table))

    def _add_replica_for_ordered(
        self,
        action_collector: DbTableActionCollector,
        table_diff: ReplicasChange,
        missing_actual_table: YtTable,
        missing_desired_table: YtTable,
    ):
        if self.settings.is_chaos:
            self._add_replica_for_ordered_chaos(
                action_collector, table_diff, missing_actual_table, missing_desired_table
            )
        else:
            self._add_replica_for_ordered_replicated(
                action_collector, table_diff, missing_actual_table, missing_desired_table
            )

    def _add_replica_for_sorted_replicated(
        self,
        action_collector: DbTableActionCollector,
        table_diff: ReplicasChange,
        missing_actual_table: YtTable,
        missing_desired_table: YtTable,
    ):
        table_key: str = table_diff.actual_main_table.key
        source_table: YtTable = self.actual.clusters[self.source_cluster_name].tables[table_key]
        main_cluster_name = self.actual.main.name

        action_collector.add(
            missing_actual_table.cluster_name,
            table_key,
            CreateTableAction(missing_desired_table, missing_actual_table),
        )
        action_collector.add(
            missing_actual_table.cluster_name,
            table_key,
            ReshardTableAction(source_table, missing_actual_table),
        )
        action_collector.add(source_table.cluster_name, table_key, EliminateChunkViews(source_table))
        action_collector.add(source_table.cluster_name, table_key, FreezeTableAction(source_table))
        action_collector.add(
            missing_actual_table.cluster_name,
            table_key,
            RemoteCopyAction(self.settings, source_table, missing_actual_table),
        )
        action_collector.add(
            main_cluster_name,
            table_key,
            CloneReplicaAction(
                table_diff.desired_main_table,
                table_diff.actual_main_table,
                source_table,
                missing_actual_table,
            ),
        )
        action_collector.add(
            missing_actual_table.cluster_name,
            table_key,
            SetUpstreamReplicaAction(missing_actual_table, table_diff.actual_main_table),
        )
        action_collector.add(
            main_cluster_name,
            table_key,
            SwitchReplicaStateAction(table_diff.actual_main_table, enabled=True, for_table=missing_actual_table),
        )
        action_collector.add(missing_actual_table.cluster_name, table_key, MountTableAction(missing_actual_table))
        action_collector.add(source_table.cluster_name, table_key, UnfreezeTableAction(source_table))

    def _add_replica_for_sorted_chaos(
        self,
        action_collector: DbTableActionCollector,
        table_diff: ReplicasChange,
        missing_actual_table: YtTable,
        missing_desired_table: YtTable,
    ):
        table_key: str = table_diff.actual_main_table.key
        main_cluster_name = self.actual.main.name

        if missing_desired_table.table_type == YtTable.Type.REPLICATION_LOG:
            replicated_table = self.actual.get_replicated_table_for(missing_actual_table)
            desired_log_replica = self.desired.get_replicated_table_for(missing_desired_table).replicas[
                missing_desired_table.replica_key
            ]

            chaos_toolbox: ChaosToolbox = ChaosToolbox(
                self.settings,
                action_collector,
                replicated_table,
                main_cluster_name,
                missing_desired_table.cluster_name,
                table_key,
            )
            chaos_toolbox.create_replicated_log(
                desired_log_replica, missing_desired_table, missing_actual_table, YtReplica.Mode.ASYNC
            )
            chaos_toolbox.mount_replication_log(missing_actual_table)
        else:
            source_table: YtTable = self.actual.clusters[self.source_cluster_name].tables[table_key]

            action_collector.add(
                missing_actual_table.cluster_name,
                table_key,
                CreateTableAction(missing_desired_table, missing_actual_table),
            )
            action_collector.add(
                missing_actual_table.cluster_name,
                table_key,
                ReshardTableAction(source_table, missing_actual_table),
            )
            action_collector.add(source_table.cluster_name, table_key, EliminateChunkViews(source_table))
            action_collector.add(source_table.cluster_name, table_key, UnmountTableAction(source_table))
            action_collector.add(
                missing_actual_table.cluster_name,
                table_key,
                RemoteCopyAction(self.settings, source_table, missing_actual_table),
            )
            action_collector.add(source_table.cluster_name, table_key, ReadReplicationProgressAction(source_table))
            action_collector.add(
                source_table.cluster_name, table_key, SetReplicationProgressAction(source_table, missing_actual_table)
            )
            action_collector.add(
                main_cluster_name,
                table_key,
                CloneChaosReplicaAction(
                    table_diff.desired_main_table,
                    table_diff.actual_main_table,
                    source_table,
                    missing_actual_table,
                ),
            )
            action_collector.add(
                missing_actual_table.cluster_name,
                table_key,
                SetUpstreamReplicaAction(missing_actual_table, table_diff.actual_main_table),
            )
            action_collector.add(
                main_cluster_name,
                table_key,
                SwitchReplicaStateAction(table_diff.actual_main_table, enabled=True, for_table=missing_actual_table),
            )
            action_collector.add(missing_actual_table.cluster_name, table_key, MountTableAction(missing_actual_table))
            action_collector.add(source_table.cluster_name, table_key, MountTableAction(source_table))

    def _add_replica_for_sorted(
        self,
        action_collector: DbTableActionCollector,
        table_diff: ReplicasChange,
        missing_actual_table: YtTable,
        missing_desired_table: YtTable,
    ):
        if self.settings.is_chaos:
            self._add_replica_for_sorted_chaos(
                action_collector, table_diff, missing_actual_table, missing_desired_table
            )
        else:
            self._add_replica_for_sorted_replicated(
                action_collector, table_diff, missing_actual_table, missing_desired_table
            )
