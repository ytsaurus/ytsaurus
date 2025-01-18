from __future__ import annotations

from abc import ABC
from abc import abstractmethod
from collections import defaultdict
from copy import deepcopy
import logging
from typing import ClassVar
from typing import Generator

from yt.yt_sync.action import ActionBase
from yt.yt_sync.action import ActionBatch
from yt.yt_sync.action import AlterReplicaAttributesAction
from yt.yt_sync.action import AlterTableAttributesAction
from yt.yt_sync.action import AlterTableSchemaAction
from yt.yt_sync.action import CleanTemporaryObjectsAction
from yt.yt_sync.action import CloneChaosReplicaAction
from yt.yt_sync.action import CreateReplicatedTableAction
from yt.yt_sync.action import CreateTableAction
from yt.yt_sync.action import DbTableActionCollector
from yt.yt_sync.action import FreezeTableAction
from yt.yt_sync.action import MountTableAction
from yt.yt_sync.action import RegisterQueueExportAction
from yt.yt_sync.action import RemountTableAction
from yt.yt_sync.action import RemoveTableAction
from yt.yt_sync.action import ReshardTableAction
from yt.yt_sync.action import SetUpstreamReplicaAction
from yt.yt_sync.action import SleepAction
from yt.yt_sync.action import SwitchReplicaStateAction
from yt.yt_sync.action import TransformTableSchemaAction
from yt.yt_sync.action import UnfreezeTableAction
from yt.yt_sync.action import UnmountTableAction
from yt.yt_sync.action import WaitChaosReplicationLagAction
from yt.yt_sync.action import WaitReplicasFlushedAction
from yt.yt_sync.action import WaitReplicasInSyncAction
from yt.yt_sync.core.client import YtClientFactory
from yt.yt_sync.core.diff import DbDiff
from yt.yt_sync.core.diff import MissingTable
from yt.yt_sync.core.diff import NodeDiffType
from yt.yt_sync.core.diff import OrphanedTable
from yt.yt_sync.core.diff import SchemaChange
from yt.yt_sync.core.diff import TableAttributesChange
from yt.yt_sync.core.diff import TableDiffType
from yt.yt_sync.core.model import YtCluster
from yt.yt_sync.core.model import YtDatabase
from yt.yt_sync.core.model import YtReplica
from yt.yt_sync.core.model import YtTable
from yt.yt_sync.core.settings import Settings

from .base import ScenarioBase
from .helpers import ChaosTableRttController
from .helpers import ChaosToolbox
from .helpers import check_and_log_db_diff
from .helpers import ensure_collocation
from .helpers import ensure_db_consumers
from .helpers import generate_wait_until_in_memory_preloaded
from .helpers import NodeManager
from .helpers import ReplicaSwitcher
from .helpers import should_switch_replicas
from .registry import scenario

LOG = logging.getLogger("yt_sync")


class _ActionGeneratorBase(ABC):
    """
    Generates actions for given table and cluster if needed for ensure scenario.
    Assumes that table_diff for given cluster is not empty.
    Respects cluster iteration order.
    """

    def __init__(
        self,
        scenario: EnsureBase,
        actual_cluster: YtCluster,
        table_key: str,
        after_main: bool,
    ):
        self.scenario: EnsureBase = scenario
        self.actual_cluster: YtCluster = actual_cluster
        self.table_key: str = table_key
        self.after_main: bool = after_main

    @property
    def actual_db(self):
        return self.scenario.actual

    @property
    def desired_db(self):
        return self.scenario.desired

    @property
    def settings(self):
        return self.scenario.settings

    @property
    def has_actions(self) -> bool:
        """Returns True if any actions should be generated for given diff, cluster, and iteration order."""
        return False

    @property
    def is_unmount_required(self) -> bool:
        """
        Returns True if table should be unmounted before applying generated actions.
        """
        return False

    @abstractmethod
    def fill(self, action_collector: DbTableActionCollector):
        raise NotImplementedError()

    def get_actual_replicated_table_for(self, table: YtTable) -> YtTable:
        return self.scenario.actual.get_replicated_table_for(table)

    def add_action_to(self, action_collector: DbTableActionCollector, action: ActionBase, key: str | None = None):
        action_key = key or self.table_key
        action_collector.add(self.actual_cluster.name, action_key, action)


class _MissingTableActionGenerator(_ActionGeneratorBase):
    def __init__(
        self,
        scenario: EnsureBase,
        actual_cluster: YtCluster,
        table_key: str,
        after_main: bool,
        table_diff: MissingTable,
    ):
        super().__init__(scenario, actual_cluster, table_key, after_main)
        self.table_diff: MissingTable = table_diff

    @property
    def has_actions(self) -> bool:
        return self.actual_cluster.is_main or self.after_main

    def fill(self, action_collector: DbTableActionCollector):
        for missing_table in self.table_diff.missing_tables_for(self.actual_cluster.name):
            actual_table: YtTable = self.actual_cluster.tables[missing_table.key]
            desired_table: YtTable = self.desired_db.clusters[self.actual_cluster.name].tables[missing_table.key]
            orphaned_table: YtTable | None = self.table_diff.orphaned.get(missing_table.replica_key)
            if orphaned_table is not None:
                self.add_action_to(action_collector, UnmountTableAction(orphaned_table))
                self.add_action_to(action_collector, RemoveTableAction(orphaned_table))
            if self.actual_cluster.is_main:
                self._fill_main(action_collector, desired_table, actual_table)
            else:
                self._fill_replica(action_collector, desired_table, actual_table)
            self._fill_post(action_collector, desired_table, actual_table)

    def _fill_main(self, action_collector: DbTableActionCollector, desired_table: YtTable, actual_table: YtTable):
        if desired_table.is_replicated:
            self.add_action_to(action_collector, CreateReplicatedTableAction(desired_table, actual_table))
        else:
            self.add_action_to(action_collector, CreateTableAction(desired_table, actual_table))

        if "static_export_config" in desired_table.attributes or "static_export_config" in actual_table.attributes:
            old_actual_table = YtTable(**actual_table.__dict__)
            self.add_action_to(action_collector, RegisterQueueExportAction(desired_table, old_actual_table))

    def _fill_replica(self, action_collector: DbTableActionCollector, desired_table: YtTable, actual_table: YtTable):
        replicated_table = self.get_actual_replicated_table_for(desired_table)
        self.add_action_to(action_collector, CreateTableAction(desired_table, actual_table, replicated_table))

        if "static_export_config" in desired_table.attributes or "static_export_config" in actual_table.attributes:
            old_actual_table = YtTable(**actual_table.__dict__)
            self.add_action_to(action_collector, RegisterQueueExportAction(desired_table, old_actual_table))

    def _fill_post(self, action_collector: DbTableActionCollector, desired_table: YtTable, actual_table: YtTable):
        if self.actual_cluster.is_mountable:
            self.add_action_to(action_collector, ReshardTableAction(desired_table, actual_table))
            self.add_action_to(action_collector, MountTableAction(actual_table))


class _OrphanedTableActionGenerator(_ActionGeneratorBase):
    def __init__(
        self,
        scenario: EnsureBase,
        actual_cluster: YtCluster,
        table_key: str,
        after_main: bool,
        table_diff: OrphanedTable,
    ):
        super().__init__(scenario, actual_cluster, table_key, after_main)
        self.table_diff: OrphanedTable = table_diff

    @property
    def has_actions(self) -> bool:
        # main, replicas...
        return self.actual_cluster.is_main or self.after_main

    @property
    def is_unmount_required(self) -> bool:
        # unmount processed by fill()
        return False

    def fill(self, action_collector: DbTableActionCollector):
        if self.actual_cluster.name == self.table_diff.main_table.cluster_name:
            self._fill_main(action_collector)
        else:
            self._fill_orphaned(action_collector)

    def _fill_main(self, action_collector: DbTableActionCollector):
        # create replicated table
        desired_table = self.table_diff.main_table
        actual_table = self.actual_cluster.tables[self.table_diff.main_table.key]
        self.add_action_to(action_collector, CreateReplicatedTableAction(desired_table, actual_table))
        if self.actual_cluster.is_mountable:
            self.add_action_to(action_collector, ReshardTableAction(desired_table, actual_table))
            self.add_action_to(action_collector, MountTableAction(actual_table))

    def _fill_orphaned(self, action_collector: DbTableActionCollector):
        # set upstream replica id for data tables
        for actual_table in self.table_diff.orphaned_tables_for(self.actual_cluster.name):
            replicated_table = self.get_actual_replicated_table_for(actual_table)
            self.add_action_to(action_collector, UnmountTableAction(actual_table), actual_table.key)
            self.add_action_to(
                action_collector, SetUpstreamReplicaAction(actual_table, replicated_table), actual_table.key
            )
            self.add_action_to(action_collector, MountTableAction(actual_table), actual_table.key)
        # create missing replication logs (if any)
        for desired_log in self.table_diff.replication_logs_for(self.actual_cluster.name):
            replicated_table = self.get_actual_replicated_table_for(actual_table)
            actual_log = self.actual_cluster.tables[desired_log.key]
            self.add_action_to(
                action_collector, CreateTableAction(desired_log, actual_log, replicated_table), actual_log.key
            )
            self.add_action_to(action_collector, ReshardTableAction(desired_log, actual_log), actual_log.key)
            self.add_action_to(action_collector, MountTableAction(actual_log), actual_log.key)


class _SchemaChangeActionGenerator(_ActionGeneratorBase):
    def __init__(
        self,
        scenario: EnsureBase,
        actual_cluster: YtCluster,
        table_key: str,
        after_main: bool,
        table_diff: SchemaChange,
        current_sync_clusters: set[str],
    ):
        super().__init__(scenario, actual_cluster, table_key, after_main)
        self.table_diff: SchemaChange = table_diff
        self.is_data_modification_required = self.table_diff.is_data_modification_required(self.actual_cluster.name)
        self.is_key_changed = self.table_diff.is_key_changed(self.actual_cluster.name)
        self.current_sync_clusters: set[str] = current_sync_clusters

    @property
    def has_actions(self) -> bool:
        if self.is_data_modification_required:
            # main, replicas...
            return self.actual_cluster.is_main or self.after_main
        else:
            # replicas..., main
            return self.actual_cluster.is_main or not self.after_main

    @property
    def is_unmount_required(self) -> bool:
        return self.has_actions

    @property
    def is_full_downtime_required(self) -> bool:
        return self.table_diff.is_change_with_downtime()

    def fill(self, action_collector: DbTableActionCollector):
        if self.settings.is_chaos:
            self._fill_chaos(action_collector)
        else:
            self._fill_replicated(action_collector)

    def _fill_chaos(self, action_collector: DbTableActionCollector):
        main_actual_cluster = self.actual_db.main
        actual_cluster = self.actual_cluster
        desired_cluster = self.desired_db.clusters[self.actual_cluster.name]

        actual_table = actual_cluster.tables[self.table_key]
        desired_table = desired_cluster.tables[self.table_key]

        if actual_table.is_chaos_replicated:
            self.add_action_to(action_collector, AlterTableSchemaAction(desired_table, actual_table))
            return

        if actual_table.is_ordered:
            self._fill_chaos_ordered_table(action_collector, actual_table, desired_table)
        else:
            self._fill_chaos_sorted_table(
                action_collector, main_actual_cluster, actual_cluster, desired_cluster, actual_table, desired_table
            )

    def _fill_chaos_ordered_table(
        self, action_collector: DbTableActionCollector, actual_table: YtTable, desired_table: YtTable
    ):
        assert not self.is_data_modification_required, "Not supported for chaos queues"
        # TODO(ashishkin): make replicas async before alter?
        self.add_action_to(action_collector, UnmountTableAction(actual_table))
        self.add_action_to(action_collector, AlterTableSchemaAction(desired_table, actual_table))
        self.add_action_to(action_collector, MountTableAction(actual_table))

    def _fill_chaos_sorted_table(
        self,
        action_collector: DbTableActionCollector,
        main_actual_cluster: YtCluster,
        actual_cluster: YtCluster,
        desired_cluster: YtCluster,
        actual_table: YtTable,
        desired_table: YtTable,
    ):
        replicated_table = self.actual_db.get_replicated_table_for(actual_table)
        actual_log = actual_cluster.get_replication_log_for(actual_table)
        assert actual_log
        desired_log = desired_cluster.get_replication_log_for(desired_table)
        assert desired_log
        desired_log_replica = self.desired_db.get_replicated_table_for(desired_table).replicas[desired_log.replica_key]

        actual_tmp_log = actual_cluster.get_or_add_temporary_table_for(actual_log, exists=False)
        desired_tmp_log = desired_cluster.get_or_add_temporary_table_for(desired_log, exists=False)
        desired_tmp_log.sync_user_attributes(actual_log)

        chaos_toolbox: ChaosToolbox = ChaosToolbox(
            self.settings,
            action_collector,
            replicated_table,
            main_actual_cluster.name,
            actual_cluster.name,
            self.table_key,
        )
        if self.is_data_modification_required:
            actual_tmp_table = actual_cluster.get_or_add_temporary_table_for(actual_table, exists=False)
            desired_replicated_table = self.desired_db.get_replicated_table_for(desired_table)

            if self.is_full_downtime_required:
                self._fill_chaos_sorted_full_downtime(
                    action_collector=action_collector,
                    chaos_toolbox=chaos_toolbox,
                    desired_replicated_table=desired_replicated_table,
                    actual_replicated_table=replicated_table,
                    desired_table=desired_table,
                    actual_table=actual_table,
                    actual_tmp_table=actual_tmp_table,
                    desired_log=desired_log,
                    actual_log=actual_log,
                )
            else:
                self._fill_chaos_sorted_data_modification(
                    action_collector=action_collector,
                    chaos_toolbox=chaos_toolbox,
                    desired_replicated_table=desired_replicated_table,
                    actual_replicated_table=replicated_table,
                    desired_table=desired_table,
                    actual_table=actual_table,
                    actual_tmp_table=actual_tmp_table,
                    desired_log_replica=desired_log_replica,
                    desired_log=desired_log,
                    actual_log=actual_log,
                    desired_tmp_log=desired_tmp_log,
                    actual_tmp_log=actual_tmp_log,
                )

        else:
            self._fill_chaos_sorted_light_change(
                action_collector=action_collector,
                chaos_toolbox=chaos_toolbox,
                desired_table=desired_table,
                actual_table=actual_table,
                desired_log_replica=desired_log_replica,
                desired_log=desired_log,
                actual_log=actual_log,
                desired_tmp_log=desired_tmp_log,
                actual_tmp_log=actual_tmp_log,
            )

        # clean temporary stuff
        action_collector.add(main_actual_cluster.name, self.table_key, CleanTemporaryObjectsAction(main_actual_cluster))
        self.add_action_to(action_collector, CleanTemporaryObjectsAction(actual_cluster))
        self.add_action_to(action_collector, CleanTemporaryObjectsAction(desired_cluster))

    def _fill_chaos_sorted_full_downtime(
        self,
        action_collector: DbTableActionCollector,
        chaos_toolbox: ChaosToolbox,
        desired_replicated_table: YtTable,
        actual_replicated_table: YtTable,
        desired_table: YtTable,
        actual_table: YtTable,
        actual_tmp_table: YtTable,
        desired_log: YtTable,
        actual_log: YtTable,
    ):
        main_actual_cluster: YtCluster = self.actual_db.main

        chaos_toolbox.remove_replicated_log(actual_log, with_replica=False)

        # create chaos_table_replica for temporary table with data
        action_collector.add(
            main_actual_cluster.name,
            self.table_key,
            CloneChaosReplicaAction(
                desired_replicated_table,
                actual_replicated_table,
                actual_table,
                actual_tmp_table,
            ),
        )

        chaos_toolbox.transform_table_schema(
            desired_table=desired_table, actual_table=actual_table, actual_tmp_table=actual_tmp_table, enable=False
        )

        # remove old data table
        chaos_toolbox.remove_replica_table(actual_table)

        # add chaos_table_replica for new table
        action_collector.add(
            main_actual_cluster.name,
            self.table_key,
            CloneChaosReplicaAction(
                desired_replicated_table,
                actual_replicated_table,
                actual_tmp_table,
                actual_table,
                destination_as_source=True,
            ),
        )

        # move tmp table to old place
        chaos_toolbox.move_table(source=actual_tmp_table, destination=actual_table, wait_lag=False)

        # remove temporary chaos_table_replica for data table
        chaos_toolbox.remove_chaos_replica(actual_tmp_table)

        # create replication log with new schema
        chaos_toolbox.create_replica_table(
            desired_log, actual_log, actual_create_source=False, actual_reshard_source=True
        )

        # enable log
        action_collector.add(
            self.actual_db.main.name,
            self.table_key,
            SwitchReplicaStateAction(actual_replicated_table, enabled=True, for_table=actual_log),
        )

    def _fill_chaos_sorted_data_modification(
        self,
        action_collector: DbTableActionCollector,
        chaos_toolbox: ChaosToolbox,
        desired_replicated_table: YtTable,
        actual_replicated_table: YtTable,
        desired_table: YtTable,
        actual_table: YtTable,
        actual_tmp_table: YtTable,
        desired_log_replica: YtReplica,
        desired_log: YtTable,
        actual_log: YtTable,
        desired_tmp_log: YtTable,
        actual_tmp_log: YtTable,
    ):
        main_actual_cluster: YtCluster = self.actual_db.main
        # stop replication to actual table by unmounting
        self.add_action_to(action_collector, UnmountTableAction(actual_table))

        # create chaos_table_replica for temporary table with data
        action_collector.add(
            main_actual_cluster.name,
            self.table_key,
            CloneChaosReplicaAction(
                desired_replicated_table,
                actual_replicated_table,
                actual_table,
                actual_tmp_table,
            ),
        )
        # mount table back to restore replication
        self.add_action_to(action_collector, MountTableAction(actual_table))

        # create temporary replication log with new schema
        chaos_toolbox.create_and_mount_replicated_log(
            desired_log_replica,
            desired_tmp_log,
            actual_tmp_log,
            actual_table,
            actual_create_source=False,
            actual_reshard_source=True,
        )

        # transform table schema
        chaos_toolbox.transform_table_schema(
            desired_table=desired_table, actual_table=actual_table, actual_tmp_table=actual_tmp_table, enable=True
        )

        # remove replication log with old schema
        chaos_toolbox.remove_replicated_log(actual_log)

        # remove old data table
        chaos_toolbox.remove_replica_table(actual_table)

        # wait lag for new table
        action_collector.add(
            main_actual_cluster.name,
            self.table_key,
            WaitChaosReplicationLagAction(actual_replicated_table, actual_tmp_table),
        )

        # disable replication to tmp table by unmounting
        self.add_action_to(action_collector, UnmountTableAction(actual_tmp_table))

        # add chaos_table_replica for new table
        action_collector.add(
            main_actual_cluster.name,
            self.table_key,
            CloneChaosReplicaAction(
                desired_replicated_table,
                actual_replicated_table,
                actual_tmp_table,
                actual_table,
                destination_as_source=True,
            ),
        )

        # move tmp table to old place
        chaos_toolbox.move_table(source=actual_tmp_table, destination=actual_table, wait_lag=True)

        # remove temporary chaos_table_replica for temporary data table
        chaos_toolbox.remove_chaos_replica(actual_tmp_table)

        # create replication log with new schema
        chaos_toolbox.create_and_mount_replicated_log(
            desired_log_replica,
            desired_log,
            actual_log,
            actual_table,
            actual_create_source=False,
            actual_reshard_source=True,
        )

        # remove temporary replication log
        chaos_toolbox.remove_replicated_log(actual_tmp_log)

    def _fill_chaos_sorted_light_change(
        self,
        action_collector: DbTableActionCollector,
        chaos_toolbox: ChaosToolbox,
        desired_table: YtTable,
        actual_table: YtTable,
        desired_log_replica: YtReplica,
        desired_log: YtTable,
        actual_log: YtTable,
        desired_tmp_log: YtTable,
        actual_tmp_log: YtTable,
    ):
        # create temporary replication log with new schema
        chaos_toolbox.create_and_mount_replicated_log(
            desired_log_replica,
            desired_tmp_log,
            actual_tmp_log,
            actual_table,
            actual_create_source=False,
            actual_reshard_source=True,
        )

        # alter table schema
        self.add_action_to(action_collector, UnmountTableAction(actual_table))
        self.add_action_to(action_collector, AlterTableSchemaAction(desired_table, actual_table))
        self.add_action_to(action_collector, MountTableAction(actual_table))

        # remove replication log with old schema
        chaos_toolbox.remove_replicated_log(actual_log)

        # create replication log with new schema
        chaos_toolbox.create_and_mount_replicated_log(
            desired_log_replica,
            desired_log,
            actual_log,
            actual_table,
            actual_create_source=False,
            actual_reshard_source=True,
        )

        # remove temporary replication log
        chaos_toolbox.remove_replicated_log(actual_tmp_log)

    def _fill_replicated(self, action_collector: DbTableActionCollector):
        actual_table = self.actual_cluster.tables[self.table_key]
        desired_table = self.desired_db.clusters[self.actual_cluster.name].tables[self.table_key]

        if self.is_data_modification_required:
            if actual_table.is_replicated:
                self.add_action_to(action_collector, RemoveTableAction(actual_table))
                self.add_action_to(
                    action_collector,
                    CreateReplicatedTableAction(
                        self._patched_replicated_table(actual_table, desired_table), actual_table
                    ),
                )
                if not self.is_full_downtime_required:
                    for replica_cluster in self.actual_db.replicas:
                        replica_table = replica_cluster.tables[actual_table.key]
                        if replica_table.exists:
                            action_collector.add(
                                replica_cluster.name, replica_table.key, UnmountTableAction(replica_table)
                            )
                            action_collector.add(
                                replica_cluster.name,
                                replica_table.key,
                                SetUpstreamReplicaAction(replica_table, actual_table),
                            )
                            action_collector.add(
                                replica_cluster.name, replica_table.key, MountTableAction(replica_table)
                            )
            else:
                self.add_action_to(
                    action_collector, TransformTableSchemaAction(desired_table, actual_table, self.settings)
                )
                if not self.actual_cluster.is_main:
                    replicated_table = self.get_actual_replicated_table_for(actual_table)
                    self.add_action_to(action_collector, SetUpstreamReplicaAction(actual_table, replicated_table))

            if self.actual_cluster.is_mountable:
                self.add_action_to(action_collector, ReshardTableAction(desired_table, actual_table))
        else:
            self.add_action_to(action_collector, AlterTableSchemaAction(desired_table, actual_table))

    def _patched_replicated_table(self, actual_table: YtTable, desired_table: YtTable) -> YtTable:
        patched_desired_table = deepcopy(desired_table)
        patched_desired_table.sync_replicas_mode(self.current_sync_clusters)
        patched_desired_table.replication_collocation_id = actual_table.replication_collocation_id

        # missing attributes
        for key in actual_table.attributes.user_attribute_keys - desired_table.attributes.user_attribute_keys:
            value = actual_table.attributes.get_filtered(key)
            if isinstance(value, bool) or value:
                patched_desired_table.attributes[key] = value
        return patched_desired_table


class _AttributeChangeActionGenerator(_ActionGeneratorBase):
    def __init__(
        self,
        scenario: EnsureBase,
        actual_cluster: YtCluster,
        table_key: str,
        after_main: bool,
        table_diff: TableAttributesChange,
    ):
        super().__init__(scenario, actual_cluster, table_key, after_main)
        self.table_diff: TableAttributesChange = table_diff

    @property
    def has_actions(self) -> bool:
        has_attr_changes = False
        has_replica_changes = False
        if self.actual_cluster.is_main:
            has_replica_changes = self.table_diff.has_replica_changes()
        if self.actual_cluster.is_main or self.after_main:
            # main, replicas...
            has_attr_changes = next(self.table_diff.attribute_changes_for(self.actual_cluster.name), None) is not None
        return has_replica_changes or has_attr_changes

    @property
    def is_unmount_required(self) -> bool:
        return self.has_actions and self.table_diff.is_unmount_required(self.actual_cluster.name)

    def fill(self, action_collector: DbTableActionCollector):
        for _ in self.table_diff.attribute_changes_for(self.actual_cluster.name):
            desired_table = self.desired_db.clusters[self.actual_cluster.name].tables[self.table_key]
            actual_table = self.actual_cluster.tables[self.table_key]

            skip_by_chaos = self.settings.is_chaos and self.actual_cluster.is_main
            unmount_required = not skip_by_chaos and self.is_unmount_required and self.settings.is_chaos
            if unmount_required:
                self.add_action_to(action_collector, UnmountTableAction(actual_table))

            if "static_export_config" in desired_table.attributes or "static_export_config" in actual_table.attributes:
                self.add_action_to(action_collector, RegisterQueueExportAction(desired_table, actual_table))

            self.add_action_to(action_collector, AlterTableAttributesAction(desired_table, actual_table))

            remount_required = not self.is_unmount_required and self.table_diff.is_remount_required(
                self.actual_cluster.name
            )
            if remount_required and not skip_by_chaos:
                self.add_action_to(action_collector, RemountTableAction(actual_table))

            if unmount_required:
                self.add_action_to(action_collector, MountTableAction(actual_table))

        if self.actual_cluster.is_main and self.table_diff.has_replica_changes():
            actual_table = self.actual_cluster.tables[self.table_key]
            for change in self.table_diff.changes.values():
                if not change.replica_changes:
                    continue
                for desired, _ in change.replica_changes:
                    self.add_action_to(action_collector, AlterReplicaAttributesAction(desired, actual_table))


class _WrappingActionGeneratorBase(_ActionGeneratorBase):
    def __init__(
        self,
        scenario: EnsureBase,
        actual_cluster: YtCluster,
        table_key: str,
        after_main: bool,
        has_actions: bool,
        unmount_required: bool,
        data_modification_required: bool,
        full_downtime_required: bool,
    ):
        super().__init__(scenario, actual_cluster, table_key, after_main)
        self._has_actions: bool = has_actions
        self._unmount_required: bool = unmount_required
        self._data_modification_required: bool = data_modification_required
        self._full_downtime_required: bool = full_downtime_required

    @property
    def has_actions(self) -> bool:
        return self._has_actions and self._unmount_required

    @property
    def is_unmount_required(self) -> bool:
        return self._unmount_required

    @property
    def is_full_downtime_required(self) -> bool:
        return self._full_downtime_required


class _PrepareActionGenerator(_WrappingActionGeneratorBase):
    def __init__(
        self,
        scenario: EnsureBase,
        actual_cluster: YtCluster,
        table_key: str,
        after_main: bool,
        has_actions: bool,
        unmount_required: bool,
        data_modification_required: bool,
        full_downtime_required: bool,
    ):
        super().__init__(
            scenario,
            actual_cluster,
            table_key,
            after_main,
            has_actions,
            unmount_required,
            data_modification_required,
            full_downtime_required,
        )

    def fill(self, action_collector: DbTableActionCollector):
        if self.settings.is_chaos:
            if self.is_full_downtime_required and self.actual_cluster.is_main:
                replicated_table: YtTable = self.actual_cluster.tables[self.table_key]
                for replica_cluster in self.actual_db.replicas:
                    replica_table: YtTable = replica_cluster.tables[self.table_key]
                    replication_log: YtTable | None = replica_cluster.get_replication_log_for(replica_table)
                    assert replication_log
                    action_collector.add(replica_cluster.name, self.table_key, FreezeTableAction(replication_log))
                    action_collector.add(
                        self.actual_db.main.name,
                        self.table_key,
                        WaitChaosReplicationLagAction(replicated_table, replica_table),
                    )
        else:
            actual_table = self.actual_cluster.tables[self.table_key]
            self.add_action_to(action_collector, FreezeTableAction(actual_table))
            if actual_table.is_replicated and self._data_modification_required:
                # Can't alter replicated table if data modification is required so we need to recreate it.
                # Before removing replicated table we should wait until all data flushed to data replicas,
                # then switch replicas to disabled state.
                self.add_action_to(action_collector, WaitReplicasFlushedAction(actual_table))
                self.add_action_to(action_collector, SwitchReplicaStateAction(actual_table, False))
            self.add_action_to(action_collector, UnmountTableAction(actual_table))


class _PostprocessActionGenerator(_WrappingActionGeneratorBase):
    def __init__(
        self,
        scenario: EnsureBase,
        actual_cluster: YtCluster,
        table_key: str,
        after_main: bool,
        has_actions: bool,
        unmount_required: bool,
        data_modification_required: bool,
        full_downtime_required: bool,
        is_last_cluster: bool,
        replica_switcher: ReplicaSwitcher,
    ):
        super().__init__(
            scenario,
            actual_cluster,
            table_key,
            after_main,
            has_actions,
            unmount_required,
            data_modification_required,
            full_downtime_required,
        )
        self._is_last_cluster: bool = is_last_cluster
        self._replica_switcher: ReplicaSwitcher = replica_switcher

    @property
    def has_actions(self) -> bool:
        return super().has_actions or self._full_downtime_required

    def fill(self, action_collector: DbTableActionCollector):
        if self.settings.is_chaos:
            if self.is_full_downtime_required and self._is_last_cluster:
                replicated_table: YtTable = self.actual_db.main.tables[self.table_key]
                for replica_cluster in self.actual_db.replicas:
                    replica_table: YtTable = replica_cluster.tables[self.table_key]
                    replication_log: YtTable | None = replica_cluster.get_replication_log_for(replica_table)
                    assert replication_log
                    action_collector.add(replica_cluster.name, self.table_key, MountTableAction(replication_log))
                action_collector.add(
                    self.actual_db.main.name,
                    self.table_key,
                    WaitReplicasInSyncAction(replicated_table, self._replica_switcher.get_sync_clusters()),
                )
        else:
            actual_table = self.actual_cluster.tables[self.table_key]
            if not actual_table.is_replicated or not self.is_full_downtime_required:
                self.add_action_to(action_collector, MountTableAction(actual_table))

            if self._is_last_cluster and self._full_downtime_required:
                replicated_table = self.actual_db.get_replicated_table_for(actual_table)
                action_collector.add(self.actual_db.main.name, actual_table.key, MountTableAction(replicated_table))


class EnsureBase(ScenarioBase):
    Generators = dict[str, list[_ActionGeneratorBase]]

    def __init__(
        self,
        desired: YtDatabase,
        actual: YtDatabase,
        settings: Settings,
        yt_client_factory: YtClientFactory,
    ):
        super().__init__(desired, actual, settings, yt_client_factory)
        self.db_diff: DbDiff = DbDiff.generate(settings, self.desired, self.actual)
        self.node_manager = NodeManager(self.yt_client_factory, self.settings)

    def pre_action(self):
        assert check_and_log_db_diff(self.settings, self.desired, self.db_diff, self.diff_types).is_valid, "Bad diff"
        is_unmount_required = False
        for cluster_name in sorted(self.actual.clusters):
            is_unmount_required |= self.db_diff.is_unmount_required(cluster_name)
        is_data_modification_required = self.db_diff.is_data_modification_required()
        if self._should_switch_replicas(is_unmount_required, is_data_modification_required):
            sync_replicas = set([r.name for r in self.actual.sync_replicas_relaxed])
            async_replicas = set([r.name for r in self.actual.async_replicas_relaxed])
            assert ReplicaSwitcher.is_all_switchable(self.settings, self.actual, self.desired), (
                f"Replicas configuration mismatch: sync={sync_replicas}, async={async_replicas}, "
                + f"always_async={self.settings.always_async}, min_sync_count={self.settings.min_sync_clusters}"
            )

    @property
    def diff_types(self) -> set[int]:
        raise NotImplementedError

    def generate_actions(self) -> list[ActionBatch]:
        result: list[ActionBatch] = list()
        if self.db_diff.is_empty():
            LOG.info("No diff")
            return result

        result.extend(self._pre_ensure_actions())
        full_downtime_tables: set[str] = self._collect_full_downtime_tables()
        switcher = ReplicaSwitcher(
            self.settings,
            self.actual,
            skip_wait=full_downtime_tables,
        )
        after_main: bool = False
        for cluster, is_last in switcher.clusters(True):
            if not after_main and cluster.is_main:
                after_main = True
            if not self.db_diff.has_diff_for(cluster.name, self.diff_types):
                LOG.info("No changes for cluster %s", cluster.name)
                continue

            generators = self._create_action_generators(
                cluster,
                after_main,
                full_downtime_tables,
                is_last,
                switcher,
            )
            if not self._has_actions(generators):
                LOG.info("No actions for cluster %s", cluster.name)
                continue

            is_unmount_required = self._is_unmount_required(generators)
            is_data_modification_required = self._is_data_modification_required(generators)

            if self._should_switch_replicas(is_unmount_required, is_data_modification_required):
                result.extend(switcher.make_async(cluster.name))

            unmounted_tables: set[str] = set()
            result.extend(self._generate_actions_for_cluster(generators, unmounted_tables))

            # wait for in-memory tables to preload before processing next cluster
            result.extend(self._wait_until_in_memory_preloaded(cluster, unmounted_tables))

            if is_unmount_required and not cluster.is_main:
                # sleep if needed after replica cluster tables modification
                result.extend(SleepAction.make_sleep_batch(cluster.name, self.settings))

            if not after_main and cluster.is_main:
                after_main = True

        result.extend(self._post_ensure_actions(switcher.get_sync_clusters()))
        result.extend(switcher.ensure_sync_mode(self.desired, update_rtt=True))

        return result

    def _pre_ensure_actions(self) -> list[ActionBatch]:
        action_collector = DbTableActionCollector()
        if self.settings.ensure_tables_mounted:
            # ensure all existing tables are mounted
            for cluster in self.actual.mountable_clusters:
                for table_key in sorted(cluster.tables):
                    actual_table = cluster.tables[table_key]
                    if not actual_table.exists:
                        continue
                    if actual_table.tablet_state.is_unmounted or actual_table.tablet_state.is_not_set:
                        action_collector.add(cluster.name, table_key, MountTableAction(actual_table))
                    elif actual_table.tablet_state.is_frozen:
                        action_collector.add(cluster.name, table_key, UnfreezeTableAction(actual_table))
        result = action_collector.dump(True)
        if self.settings.is_chaos:
            # toggle off RTT and make all replication logs sync
            rtt_controller = ChaosTableRttController(self.desired.main, self.actual.main)
            sync_replicas = set([r.name for r in self.actual.replicas]) - self.settings.always_async
            result.extend(
                rtt_controller.switch_rtt_off(
                    sync_replicas, [diff.table_key for diff in self.db_diff.schema_diff_for()]
                )
            )
        return result

    def _collect_full_downtime_tables(self) -> set[str]:
        result: set[str] = set()
        for diff_set in self.db_diff.schema_diff_for():
            for schema_diff in diff_set.diffs:
                assert isinstance(schema_diff, SchemaChange)
                if schema_diff.is_change_with_downtime():
                    result.add(diff_set.table_key)
        return result

    def _post_ensure_actions(self, sync_replicas: set[str]) -> list[ActionBatch]:
        if self.settings.is_chaos:
            rtt_controller = ChaosTableRttController(self.desired.main, self.actual.main)
            return rtt_controller.switch_rtt_on(
                sync_replicas, [diff.table_key for diff in self.db_diff.schema_diff_for()]
            )
        return []

    def _is_chaos_schema_change_required(self) -> bool:
        return self.settings.is_chaos and self.db_diff.has_schema_changes()

    def _should_switch_replicas(self, is_unmount_required: bool, is_data_modification_required: bool) -> bool:
        return should_switch_replicas(self.settings, is_unmount_required, is_data_modification_required)

    def _create_action_generators(
        self,
        actual_cluster: YtCluster,
        after_main: bool,
        full_downtime_tables: set[str],
        is_last_cluster: bool,
        replica_switcher: ReplicaSwitcher,
    ) -> EnsureBase.Generators:
        result: dict[str, list[_ActionGeneratorBase]] = defaultdict(list)

        for diff_set in self.db_diff.table_diff_for(actual_cluster.name, self.diff_types):
            generators = result[diff_set.table_key]
            for table_diff in sorted(diff_set.diffs, key=lambda x: x.diff_type):
                if not table_diff.has_diff_for(actual_cluster.name):
                    continue
                skip_attributes = False
                if TableDiffType.MISSING_TABLE == table_diff.diff_type:
                    assert isinstance(table_diff, MissingTable)
                    generators.append(
                        _MissingTableActionGenerator(self, actual_cluster, diff_set.table_key, after_main, table_diff)
                    )
                elif TableDiffType.ORPHANED_TABLE == table_diff.diff_type:
                    assert isinstance(table_diff, OrphanedTable)
                    generators.append(
                        _OrphanedTableActionGenerator(self, actual_cluster, diff_set.table_key, after_main, table_diff)
                    )
                elif TableDiffType.SCHEMA_CHANGE == table_diff.diff_type:
                    assert isinstance(table_diff, SchemaChange)
                    schema_generator = _SchemaChangeActionGenerator(
                        self,
                        actual_cluster,
                        diff_set.table_key,
                        after_main,
                        table_diff,
                        replica_switcher.get_sync_clusters(),
                    )
                    skip_attributes = schema_generator.is_data_modification_required
                    generators.append(schema_generator)
                elif TableDiffType.ATTRIBUTES_CHANGE == table_diff.diff_type:
                    if not skip_attributes:
                        assert isinstance(table_diff, TableAttributesChange)
                        generators.append(
                            _AttributeChangeActionGenerator(
                                self, actual_cluster, diff_set.table_key, after_main, table_diff
                            )
                        )

            has_actions: bool = self._has_actions(result, diff_set.table_key)
            unmount_required: bool = self._is_unmount_required(result, diff_set.table_key)
            data_modification_required: bool = self._is_data_modification_required(result, diff_set.table_key)
            full_downtime_required: bool = diff_set.table_key in full_downtime_tables
            generators.insert(
                0,
                _PrepareActionGenerator(
                    self,
                    actual_cluster,
                    diff_set.table_key,
                    after_main,
                    has_actions,
                    unmount_required,
                    data_modification_required,
                    full_downtime_required,
                ),
            )
            generators.append(
                _PostprocessActionGenerator(
                    self,
                    actual_cluster,
                    diff_set.table_key,
                    after_main,
                    has_actions,
                    unmount_required,
                    data_modification_required,
                    full_downtime_required,
                    is_last_cluster,
                    replica_switcher,
                )
            )
        return result

    def _generate_actions_for_cluster(
        self, generators: EnsureBase.Generators, unmounted_tables: set[str]
    ) -> list[ActionBatch]:
        result: list[ActionBatch] = list()
        action_collector = DbTableActionCollector()

        for table_key in sorted(generators):
            for generator in generators[table_key]:
                if generator.has_actions:
                    generator.fill(action_collector)
                    if generator.is_unmount_required:
                        unmounted_tables.add(generator.table_key)

        result.extend(
            action_collector.dump(
                self.settings.parallel_processing_for_unmounted,
                self.settings.get_batch_size_for_parallel(self.SCENARIO_NAME),
            )
        )
        return result

    def _wait_until_in_memory_preloaded(self, cluster: YtCluster, unmounted_tables: set[str]) -> list[ActionBatch]:
        return generate_wait_until_in_memory_preloaded(self.settings, cluster, unmounted_tables)

    @staticmethod
    def _iterate_generators(generators: EnsureBase.Generators) -> Generator[_ActionGeneratorBase, None, None]:
        for table_path in sorted(generators):
            for generator in generators[table_path]:
                yield generator

    @classmethod
    def _has_actions(cls, generators: EnsureBase.Generators, table_key: str | None = None) -> bool:
        def _check(generator: _ActionGeneratorBase) -> bool:
            if table_key and table_key != generator.table_key:
                return False
            return generator.has_actions

        for generator in cls._iterate_generators(generators):
            if _check(generator):
                return True
        return False

    @classmethod
    def _is_property_set(
        cls, property_name: str, generators: EnsureBase.Generators, table_key: str | None = None
    ) -> bool:
        def _check(generator: _ActionGeneratorBase) -> bool:
            if table_key and table_key != generator.table_key:
                return False
            return getattr(generator, property_name, False)

        for generator in cls._iterate_generators(generators):
            if _check(generator):
                return True
        return False

    @classmethod
    def _is_unmount_required(cls, generators: EnsureBase.Generators, table_key: str | None = None) -> bool:
        return cls._is_property_set("is_unmount_required", generators, table_key)

    @classmethod
    def _is_data_modification_required(cls, generators: EnsureBase.Generators, table_key: str | None = None) -> bool:
        return cls._is_property_set("is_data_modification_required", generators, table_key)


@scenario
class EnsureAttributes(EnsureBase):
    SCENARIO_NAME: ClassVar[str] = "ensure_attributes"
    SCENARIO_DESCRIPTION: ClassVar[str] = "Ensure table attributes"

    def __init__(
        self,
        desired: YtDatabase,
        actual: YtDatabase,
        settings: Settings,
        yt_client_factory: YtClientFactory,
    ):
        super().__init__(desired, actual, settings, yt_client_factory)

    @property
    def diff_types(self) -> set[int]:
        return {TableDiffType.ATTRIBUTES_CHANGE, NodeDiffType.ATTRIBUTES_CHANGE}

    def pre_action(self):
        super().pre_action()
        self.has_diff |= self.node_manager.ensure_nodes_from_diff(
            self.db_diff, self.actual, mode=NodeManager.EnsureMode.ATTRIBUTES_ONLY
        )
        self.has_diff |= self.node_manager.ensure_links_from_diff(self.db_diff, self.actual, attributes_only=True)

    def post_action(self):
        super().post_action()
        other_diff: set[int] = {
            TableDiffType.MISSING_TABLE,
            TableDiffType.ORPHANED_TABLE,
            TableDiffType.SCHEMA_CHANGE,
            TableDiffType.ATTRIBUTES_CHANGE,
            NodeDiffType.MISSING_NODE,
        }
        has_other_changes = False
        for cluster_name in self.desired.clusters:
            if self.db_diff.has_diff_for(cluster_name, other_diff):
                has_other_changes = True
        if has_other_changes:
            LOG.warning(
                "DB has diff other than attributes, consider call 'dump_diff' for details and 'ensure' to sync changes"
            )


class EnsureFull(EnsureBase):
    def __init__(
        self,
        desired: YtDatabase,
        actual: YtDatabase,
        settings: Settings,
        yt_client_factory: YtClientFactory,
    ):
        super().__init__(desired, actual, settings, yt_client_factory)

    def pre_action(self):
        super().pre_action()
        if self.settings.ensure_folders:
            self.has_diff |= self.node_manager.ensure_nodes(self.desired, self.actual)

    def post_action(self):
        super().post_action()
        if self.settings.ensure_folders:
            self.has_diff |= self.node_manager.ensure_links(self.desired, self.actual)
        if self.settings.ensure_collocation:
            self.has_diff |= ensure_collocation(
                self.desired.main, self.actual.main, self.yt_client_factory, self.settings
            )
        self.has_diff |= ensure_db_consumers(self.desired, self.actual, self.yt_client_factory)

    @property
    def diff_types(self) -> set[int]:
        return {
            TableDiffType.MISSING_TABLE,
            TableDiffType.ORPHANED_TABLE,
            TableDiffType.SCHEMA_CHANGE,
            TableDiffType.ATTRIBUTES_CHANGE,
        } | NodeDiffType.all()


@scenario
class Ensure(EnsureFull):
    SCENARIO_NAME: ClassVar[str] = "ensure"
    SCENARIO_DESCRIPTION: ClassVar[str] = "Ensure table attributes and schema (without heavy transform)"

    def __init__(
        self,
        desired: YtDatabase,
        actual: YtDatabase,
        settings: Settings,
        yt_client_factory: YtClientFactory,
    ):
        super().__init__(desired, actual, settings, yt_client_factory)
        self._diff_types: set[str] = {
            TableDiffType.MISSING_TABLE,
            TableDiffType.ORPHANED_TABLE,
            TableDiffType.SCHEMA_CHANGE,
            TableDiffType.ATTRIBUTES_CHANGE,
            NodeDiffType.MISSING_NODE,
            NodeDiffType.ATTRIBUTES_CHANGE,
        }

    def pre_action(self):
        super().pre_action()
        assert (
            not self.db_diff.is_data_modification_required()
        ), "Diff has heavy changes, call 'ensure_heavy' scenario instead"

    def setup(self, **kwargs):
        if kwargs.get("skip_attributes", False):
            self._diff_types.remove(TableDiffType.ATTRIBUTES_CHANGE)
            self._diff_types.remove(NodeDiffType.ATTRIBUTES_CHANGE)

    @property
    def diff_types(self) -> set[int]:
        return self._diff_types


@scenario
class EnsureHeavy(EnsureFull):
    SCENARIO_NAME: ClassVar[str] = "ensure_heavy"
    SCENARIO_DESCRIPTION: ClassVar[str] = "Ensure table attributes and schema (with heavy transform)"

    def __init__(
        self,
        desired: YtDatabase,
        actual: YtDatabase,
        settings: Settings,
        yt_client_factory: YtClientFactory,
    ):
        super().__init__(desired, actual, settings, yt_client_factory)
