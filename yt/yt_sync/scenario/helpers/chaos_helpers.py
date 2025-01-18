from copy import deepcopy
from typing import Iterable

from yt.yt_sync.action import ActionBase
from yt.yt_sync.action import ActionBatch
from yt.yt_sync.action import ChaosTransformTableSchemaAction
from yt.yt_sync.action import CopyReplicationProgressAction
from yt.yt_sync.action import CreateReplicaAction
from yt.yt_sync.action import CreateTableAction
from yt.yt_sync.action import DbTableActionCollector
from yt.yt_sync.action import MountTableAction
from yt.yt_sync.action import MoveTableAction
from yt.yt_sync.action import RemoveReplicaAction
from yt.yt_sync.action import RemoveTableAction
from yt.yt_sync.action import ReshardTableAction
from yt.yt_sync.action import SetUpstreamReplicaAction
from yt.yt_sync.action import SwitchReplicaModeAction
from yt.yt_sync.action import SwitchReplicaStateAction
from yt.yt_sync.action import SwitchRttAction
from yt.yt_sync.action import TableActionCollector
from yt.yt_sync.action import UnmountTableAction
from yt.yt_sync.action import WaitChaosReplicationLagAction
from yt.yt_sync.core import Settings
from yt.yt_sync.core.model import YtCluster
from yt.yt_sync.core.model import YtReplica
from yt.yt_sync.core.model import YtTable


class ChaosToolbox:
    def __init__(
        self,
        settings: Settings,
        action_collector: DbTableActionCollector,
        replicated_table: YtTable,
        main_cluster_name: str,
        actual_cluster_name: str,
        table_key: str,
    ):
        self.settings: Settings = settings
        self.action_collector: DbTableActionCollector = action_collector
        self.replicated_table: YtTable = replicated_table
        self.main_cluster_name: str = main_cluster_name
        self.actual_cluster_name: str = actual_cluster_name
        self.table_key: str = table_key

    def add_action_to(self, action: ActionBase):
        self.action_collector.add(self.actual_cluster_name, self.table_key, action)

    def create_and_mount_replicated_log(
        self,
        desired_replica: YtReplica,
        desired_log: YtTable,
        actual_log: YtTable,
        actual_table: YtTable,
        actual_create_source: bool = False,
        actual_reshard_source: bool = False,
    ):
        self.create_replicated_log(
            desired_replica,
            desired_log,
            actual_log,
            actual_create_source=actual_create_source,
            actual_reshard_source=actual_reshard_source,
        )
        self.mount_replication_log(actual_log)
        self.wait_replication(actual_table)

    def create_replica_table(
        self,
        desired_table: YtTable,
        actual_table: YtTable,
        actual_create_source: bool = False,
        actual_reshard_source: bool = False,
    ):
        reshard_source = deepcopy(actual_table) if actual_reshard_source else desired_table
        if actual_create_source:
            self.add_action_to(CreateTableAction(actual_table, actual_table, self.replicated_table))
        else:
            self.add_action_to(CreateTableAction(desired_table, actual_table, self.replicated_table))

        self.add_action_to(ReshardTableAction(reshard_source, actual_table))

    def create_replicated_log(
        self,
        desired_replica: YtReplica,
        desired_log: YtTable,
        actual_log: YtTable,
        forced_mode: str | None = None,
        actual_create_source: bool = False,
        actual_reshard_source: bool = False,
    ):
        mode = YtReplica.Mode.ASYNC if self.actual_cluster_name in self.settings.always_async else YtReplica.Mode.SYNC
        if forced_mode:
            mode = forced_mode
        self.action_collector.add(
            self.main_cluster_name,
            self.table_key,
            CreateReplicaAction(self.replicated_table, desired_replica, actual_log, {"catchup": False, "mode": mode}),
        )
        self.create_replica_table(
            desired_log,
            actual_log,
            actual_create_source=actual_create_source,
            actual_reshard_source=actual_reshard_source,
        )

    def mount_replication_log(self, actual_log: YtTable):
        self.add_action_to(MountTableAction(actual_log))

    def wait_replication(self, actual_table: YtTable):
        self.action_collector.add(
            self.main_cluster_name,
            self.table_key,
            WaitChaosReplicationLagAction(self.replicated_table, actual_table),
        )

    def remove_replicated_log(self, actual_log: YtTable, with_replica: bool = True):
        self.action_collector.add(
            self.main_cluster_name,
            self.table_key,
            SwitchReplicaStateAction(self.replicated_table, enabled=False, for_table=actual_log),
        )
        if with_replica:
            self.action_collector.add(
                self.main_cluster_name,
                self.table_key,
                RemoveReplicaAction(self.replicated_table, actual_log),
            )

        self.add_action_to(UnmountTableAction(actual_log))
        self.add_action_to(RemoveTableAction(actual_log))

    def remove_chaos_replica(self, actual_table: YtTable):
        self.action_collector.add(
            self.main_cluster_name,
            self.table_key,
            SwitchReplicaStateAction(self.replicated_table, enabled=False, for_table=actual_table),
        )
        self.action_collector.add(
            self.main_cluster_name,
            self.table_key,
            RemoveReplicaAction(self.replicated_table, actual_table),
        )

    def remove_replica_table(self, actual_table: YtTable):
        self.remove_chaos_replica(actual_table)
        self.action_collector.add(self.actual_cluster_name, self.table_key, RemoveTableAction(actual_table))

    def transform_table_schema(
        self,
        desired_table: YtTable,
        actual_table: YtTable,
        actual_tmp_table: YtTable,
        enable: bool,
    ):
        self.action_collector.add(self.actual_cluster_name, self.table_key, UnmountTableAction(actual_table))
        self.action_collector.add(
            self.actual_cluster_name,
            self.table_key,
            ChaosTransformTableSchemaAction(self.settings, desired_table, actual_table, actual_tmp_table),
        )
        self.action_collector.add(
            self.actual_cluster_name, self.table_key, SetUpstreamReplicaAction(actual_tmp_table, self.replicated_table)
        )
        self.action_collector.add(
            self.actual_cluster_name, self.table_key, ReshardTableAction(desired_table, actual_tmp_table)
        )
        self.action_collector.add(
            self.actual_cluster_name, self.table_key, CopyReplicationProgressAction(actual_table, actual_tmp_table)
        )

        if enable:
            self.action_collector.add(
                self.main_cluster_name,
                self.table_key,
                SwitchReplicaStateAction(self.replicated_table, enabled=True, for_table=actual_tmp_table),
            )
            self.action_collector.add(self.actual_cluster_name, self.table_key, MountTableAction(actual_tmp_table))

    def move_table(self, source: YtTable, destination: YtTable, wait_lag: bool):
        self.action_collector.add(self.actual_cluster_name, self.table_key, MoveTableAction(source, destination))
        self.action_collector.add(
            self.actual_cluster_name, self.table_key, SetUpstreamReplicaAction(destination, self.replicated_table)
        )
        self.action_collector.add(
            self.main_cluster_name,
            self.table_key,
            SwitchReplicaStateAction(self.replicated_table, enabled=True, for_table=destination),
        )
        self.action_collector.add(self.actual_cluster_name, self.table_key, MountTableAction(destination))

        if wait_lag:
            self.action_collector.add(
                self.main_cluster_name,
                self.table_key,
                WaitChaosReplicationLagAction(self.replicated_table, destination),
            )


class ChaosTableRttController:
    def __init__(self, main_desired: YtCluster, main_actual: YtCluster):
        assert (
            main_desired.name == main_actual.name
        ), f"Inconsistent desired={main_desired.name} and actual={main_actual.name} clusters"
        self.main_desired: YtCluster = main_desired
        self.main_actual: YtCluster = main_actual

    def switch_rtt_off(self, sync_replicas: set[str], table_keys: Iterable[str]) -> list[ActionBatch]:
        action_collector = TableActionCollector(self.main_actual.name)
        for table_key in table_keys:
            actual_table = self.main_actual.tables[table_key]
            desired_table = self.main_desired.tables[table_key]
            action_collector.add(table_key, SwitchRttAction(actual_table, desired_table, enable=False))
            action_collector.add(
                table_key,
                SwitchReplicaModeAction(actual_table, sync_replicas, set([YtReplica.ContentType.QUEUE])),
            )
        return action_collector.dump()

    def switch_rtt_on(self, sync_replicas: set[str], table_keys: Iterable[str]) -> list[ActionBatch]:
        action_collector = TableActionCollector(self.main_actual.name)
        for table_key in table_keys:
            actual_table = self.main_actual.tables[table_key]
            desired_table = self.main_desired.tables[table_key]
            action_collector.add(
                table_key, SwitchReplicaModeAction(actual_table, sync_replicas, set([YtReplica.ContentType.QUEUE]))
            )
            action_collector.add(table_key, SwitchRttAction(actual_table, desired_table, enable=True))
        return action_collector.dump()
