from copy import deepcopy
import logging
from typing import ClassVar

from yt.yt_sync.action import ActionBatch
from yt.yt_sync.action import AlterTableAttributesAction
from yt.yt_sync.action import CreateReplicatedTableAction
from yt.yt_sync.action import CreateTableAction
from yt.yt_sync.action import DbTableActionCollector
from yt.yt_sync.action import FreezeTableAction
from yt.yt_sync.action import GenerateInitialReplicationProgressAction
from yt.yt_sync.action import MountTableAction
from yt.yt_sync.action import RemoveTableAction
from yt.yt_sync.action import ReshardTableAction
from yt.yt_sync.action import SetReplicationProgressAction
from yt.yt_sync.action import SetUpstreamReplicaAction
from yt.yt_sync.action import SwitchReplicaStateAction
from yt.yt_sync.action import TableActionCollector
from yt.yt_sync.action import UnmountTableAction
from yt.yt_sync.action import WaitChaosReplicationLagAction
from yt.yt_sync.action import WaitReplicasFlushedAction
from yt.yt_sync.core.client import YtClientFactory
from yt.yt_sync.core.model import YtCluster
from yt.yt_sync.core.model import YtDatabase
from yt.yt_sync.core.model import YtNativeConsumer
from yt.yt_sync.core.model import YtReplica
from yt.yt_sync.core.model import YtTable
from yt.yt_sync.core.settings import Settings
from yt.yt_sync.core.state_builder import load_actual_db

from .base import ScenarioBase
from .helpers import ensure_collocation
from .helpers import ensure_db_consumers
from .helpers import NodeManager
from .helpers import unregister_queues
from .registry import scenario

LOG = logging.getLogger("yt_sync")


@scenario
class MigrateToChaos(ScenarioBase):
    SCENARIO_NAME: ClassVar[str] = "migrate_to_chaos"
    SCENARIO_DESCRIPTION: ClassVar[str] = "Migrate from replicated DB to chaos one"

    def __init__(
        self,
        desired: YtDatabase,
        actual: YtDatabase,
        settings: Settings,
        yt_client_factory: YtClientFactory,
    ):
        assert desired.is_chaos, "Desired should be CHAOS DB to perform migration"
        assert settings.is_chaos, "Settings should be for CHAOS DB"
        super().__init__(desired, actual, settings, yt_client_factory)

        self.node_manager = NodeManager(self.yt_client_factory, self.settings)
        self.override_main: str | None = None
        self.destination_desired: YtDatabase = None
        self.destination_actual: YtDatabase = None
        self.active_tables: set[str] = set()

    def _check_replicas(self):
        desired_replicas = set([r.name for r in self.desired.replicas])
        assert desired_replicas, "Can't migrate non-replicated DB."

        actual_replicas = set([r.name for r in self.actual.replicas])
        assert desired_replicas == actual_replicas, "Can't migrate DB with mismatched replicas"

    def setup(self, **kwargs):
        self.override_main = kwargs.get("override_main", None)

    def _copy_cluster(self, src_cluster: YtCluster, dst_cluster: YtCluster):
        for src_node in src_cluster.nodes.values():
            dst_node = deepcopy(src_node)
            dst_node.cluster_name = dst_cluster.name
            dst_cluster.add_node(dst_node)

        for src_table in src_cluster.tables.values():
            dst_table = deepcopy(src_table)
            dst_table.cluster_name = dst_cluster.name
            dst_cluster.add_table(dst_table)

    def _copy_consumers(self, src_database: YtDatabase, dst_database: YtDatabase):
        src_main: YtCluster = src_database.main
        dst_main: YtCluster = dst_database.main
        for src_cluster in src_database.clusters.values():
            dst_cluster = dst_main if src_cluster.is_main else dst_database.clusters[src_cluster.name]
            for src_consumer in src_cluster.consumers.values():
                dst_table: YtTable = dst_cluster.tables[src_consumer.table.key]
                dst_consumer = YtNativeConsumer(dst_table)
                for src_registration in src_consumer.registrations.values():
                    dst_cluster: str = src_registration.cluster_name
                    if src_registration.cluster_name in src_database.clusters:
                        src_reg_cluster: YtCluster = src_database.clusters[src_registration.cluster_name]
                        if src_reg_cluster.find_table_by_path(src_registration.path) is not None:
                            dst_cluster: str = (
                                dst_main.name
                                if src_registration.cluster_name == src_main.name
                                else src_registration.cluster_name
                            )
                    dst_consumer.add_registration(
                        dst_cluster, src_registration.path, src_registration.vital, src_registration.partitions
                    )
                dst_cluster.add_consumer(dst_consumer)

    def _build_destinations(self):
        if self.override_main and self.override_main != self.desired.main.name:
            LOG.info("Destination main is overridden: %s -> %s", self.desired.main.name, self.override_main)
            self.destination_desired = YtDatabase(is_chaos=True)
            destination_main = self.destination_desired.add_or_get_cluster(
                YtCluster.make(YtCluster.Type.MAIN, self.override_main, is_chaos=True)
            )
            self._copy_cluster(self.desired.main, destination_main)
            for src_replica in self.desired.replicas:
                dst_replica = self.destination_desired.add_or_get_cluster(src_replica)
                self._copy_cluster(src_replica, dst_replica)
            self._copy_consumers(self.desired, self.destination_desired)
            self.destination_actual = load_actual_db(self.settings, self.yt_client_factory, self.destination_desired)
        else:
            self.destination_desired = self.desired
            self.destination_actual = self.actual

    def _collect_and_ensure_active_tables(self):
        active_replicas: dict[str, set[str]] = dict()
        for replica in self.destination_actual.replicas:
            replica_active_tables: set[str] = set()
            for table in replica.tables_sorted:
                if table.exists and table.table_type == YtTable.Type.TABLE:
                    replica_active_tables.add(table.key)
            active_replicas[replica.name] = replica_active_tables

        # detect if all desired replicas present
        for desired_table in self.destination_desired.main.tables_sorted:
            if not desired_table.is_replicated:
                continue
            table_replicas: set[str] = set()
            for replica in desired_table.replicas.values():
                if desired_table.is_ordered:
                    table_replicas.add(replica.cluster_name)
                elif replica.content_type == YtReplica.ContentType.DATA:
                    table_replicas.add(replica.cluster_name)
            active = True
            for replica in table_replicas:
                if desired_table.key not in active_replicas.get(replica, set()):
                    active = False
            if active:
                self.active_tables.add(desired_table.key)

        # check schemas and log skipped tables
        for cluster in self.destination_actual.all_clusters:
            for actual_table in cluster.tables_sorted:
                if not actual_table.exists:
                    continue
                desired_table = self.destination_desired.clusters[cluster.name].tables[actual_table.key]
                assert (
                    desired_table.schema == actual_table.schema
                ), f"Can't migrate with schema diff {actual_table.table_type} {actual_table.rich_path}"
                if actual_table.table_type == YtTable.Type.REPLICATION_LOG:
                    if actual_table.chaos_data_table not in self.active_tables:
                        LOG.info("Skip %s %s, not full replicas", actual_table.table_type, actual_table.rich_path)
                elif actual_table.key not in self.active_tables:
                    LOG.info("Skip %s %s, not full replicas", actual_table.table_type, actual_table.rich_path)

    def pre_action(self):
        super().pre_action()
        self._check_replicas()
        self._build_destinations()
        self._collect_and_ensure_active_tables()

        if self.settings.ensure_folders:
            self.has_diff |= self.node_manager.ensure_nodes(self.destination_desired, self.destination_actual)

        self.has_diff |= unregister_queues(self.actual, self.yt_client_factory)

    def post_action(self):
        super().post_action()
        if self.settings.ensure_folders:
            self.has_diff |= self.node_manager.ensure_links(self.destination_desired, self.destination_actual)
        if self.settings.ensure_collocation:
            self.has_diff |= ensure_collocation(
                self.destination_desired.main, self.destination_actual.main, self.yt_client_factory, self.settings
            )
        self.has_diff |= ensure_db_consumers(
            self.destination_desired, self.destination_actual, self.yt_client_factory, clean=True
        )

    def generate_actions(self) -> list[ActionBatch]:
        result: list[ActionBatch] = []

        result.extend(self._disable_replicated_tables())
        result.extend(self._disable_replication_logs())
        result.extend(self._wait_chaos_replicas_in_sync())
        result.extend(self._remove_replicated())
        result.extend(self._remove_chaos_replicated())
        result.extend(self._remove_chaos_replication_logs())
        result.extend(self._unmount_replicas())
        result.extend(self._create_chaos_replicated_tables())
        result.extend(self._setup_replicas())
        result.extend(self._create_replication_logs())
        result.extend(self._mount_tables())

        return result

    def _is_active(self, table: YtTable) -> bool:
        if table.table_type == YtTable.Type.REPLICATION_LOG:
            return table.chaos_data_table in self.active_tables
        return table.key in self.active_tables

    def _disable_replicated_tables(self) -> list[ActionBatch]:
        actual_main: YtCluster = self.actual.main
        action_collector = TableActionCollector(actual_main.name)
        for actual_table in actual_main.tables_sorted:
            if actual_table.key not in self.active_tables:
                continue
            if not actual_table.exists:
                continue
            if actual_table.is_replicated and not actual_table.is_chaos_replicated:
                action_collector.add(actual_table.key, FreezeTableAction(actual_table))
                action_collector.add(actual_table.key, WaitReplicasFlushedAction(actual_table))
        return action_collector.dump()

    def _disable_replication_logs(self) -> list[ActionBatch]:
        action_collector = DbTableActionCollector()
        for replica in self.destination_actual.replicas:
            for table_key in sorted(self.active_tables):
                if table_key not in replica.tables:
                    continue
                actual_table = replica.tables[table_key]
                replication_log = replica.get_replication_log_for(actual_table)
                if replication_log is None:
                    continue
                if not replication_log.exists:
                    continue
                action_collector.add(replica.name, table_key, FreezeTableAction(replication_log))
        return action_collector.dump(True)

    def _wait_chaos_replicas_in_sync(self) -> list[ActionBatch]:
        actual_main: YtCluster = self.destination_actual.main
        action_collector = DbTableActionCollector()
        for replica in self.destination_actual.replicas:
            for table_key in sorted(self.active_tables):
                if table_key not in replica.tables:
                    continue
                actual_table = replica.tables[table_key]
                replication_log = replica.get_replication_log_for(actual_table)
                if replication_log is None or not replication_log.exists:
                    continue
                replicated_table = self.destination_actual.get_replicated_table_for(actual_table)
                if replicated_table and replicated_table.exists:
                    action_collector.add(
                        actual_main.name, table_key, WaitChaosReplicationLagAction(replicated_table, actual_table)
                    )
        return action_collector.dump(True)

    def _remove_replicated(self) -> list[ActionBatch]:
        actual_main = self.actual.main
        action_collector = TableActionCollector(actual_main.name)

        for table_key in sorted(self.active_tables):
            if table_key not in actual_main.tables:
                continue
            actual_table = actual_main.tables[table_key]
            if not actual_table.exists or not (actual_table.is_replicated and not actual_table.is_chaos_replicated):
                continue
            action_collector.add(table_key, SwitchReplicaStateAction(actual_table, enabled=False))
            action_collector.add(table_key, UnmountTableAction(actual_table))
            action_collector.add(table_key, RemoveTableAction(actual_table))
        return action_collector.dump()

    def _remove_chaos_replicated(self) -> list[ActionBatch]:
        actual_main = self.destination_actual.main
        action_collector = TableActionCollector(actual_main.name)

        for table_key in sorted(self.active_tables):
            if table_key not in actual_main.tables:
                continue
            actual_table = actual_main.tables[table_key]
            if not actual_table.exists or not actual_table.is_chaos_replicated:
                continue
            action_collector.add(table_key, RemoveTableAction(actual_table))
        return action_collector.dump()

    def _remove_chaos_replication_logs(self) -> list[ActionBatch]:
        action_collector = DbTableActionCollector()
        for replica in self.destination_actual.replicas:
            for table_key in sorted(self.active_tables):
                if table_key not in replica.tables:
                    continue
                actual_table = replica.tables[table_key]
                replication_log = replica.get_replication_log_for(actual_table)
                if replication_log and replication_log.exists:
                    action_collector.add(replica.name, replication_log.key, UnmountTableAction(replication_log))
                    action_collector.add(replica.name, replication_log.key, RemoveTableAction(replication_log))
        return action_collector.dump(True)

    def _create_chaos_replicated_tables(self) -> list[ActionBatch]:
        desired_main = self.destination_desired.main
        actual_main = self.destination_actual.main
        action_collector = TableActionCollector(desired_main.name)
        for desired_table in desired_main.tables_sorted:
            if not self._is_active(desired_table):
                continue
            actual_table = actual_main.tables[desired_table.key]
            action_collector.add(desired_table.key, CreateReplicatedTableAction(desired_table, actual_table))
        return action_collector.dump()

    def _unmount_replicas(self) -> list[ActionBatch]:
        action_collector = DbTableActionCollector()
        for replica_cluster in self.destination_desired.replicas:
            for desired_table in replica_cluster.tables_sorted:
                actual_table = self.destination_actual.clusters[replica_cluster.name].tables[desired_table.key]
                if desired_table.table_type == YtTable.Type.REPLICATION_LOG:
                    continue
                if not self._is_active(desired_table):
                    continue
                action_collector.add(replica_cluster.name, desired_table.key, UnmountTableAction(actual_table))
        return action_collector.dump(parallel=True)

    def _setup_replicas(self) -> list[ActionBatch]:
        action_collector = DbTableActionCollector()

        dummy_table = YtTable.make(
            "_dummy_",
            "_dummy_",
            YtTable.Type.TABLE,
            "//tmp/_dummy_",
            True,
            {"dynamic": True},
            ensure_empty_schema=False,
        )
        action_collector.add(
            self.destination_actual.main.name, dummy_table.key, GenerateInitialReplicationProgressAction(dummy_table)
        )

        for replica_cluster in self.destination_desired.replicas:
            for desired_table in replica_cluster.tables_sorted:
                actual_table = self.destination_actual.clusters[replica_cluster.name].tables[desired_table.key]
                replicated_table = self.destination_actual.get_replicated_table_for(actual_table)
                if desired_table.table_type == YtTable.Type.REPLICATION_LOG:
                    continue
                if not self._is_active(desired_table):
                    continue

                action_collector.add(
                    replica_cluster.name, desired_table.key, AlterTableAttributesAction(desired_table, actual_table)
                )
                action_collector.add(
                    replica_cluster.name, desired_table.key, SetUpstreamReplicaAction(actual_table, replicated_table)
                )
                action_collector.add(
                    replica_cluster.name, desired_table.key, SetReplicationProgressAction(dummy_table, actual_table)
                )

        return action_collector.dump(parallel=True)

    def _create_replication_logs(self) -> list[ActionBatch]:
        action_collector = DbTableActionCollector()
        for replica_cluster in self.destination_desired.replicas:
            for desired_table in replica_cluster.tables_sorted:
                actual_table = self.destination_actual.clusters[replica_cluster.name].tables[desired_table.key]
                replicated_table = self.destination_actual.get_replicated_table_for(actual_table)
                if desired_table.table_type != YtTable.Type.REPLICATION_LOG:
                    continue
                if not self._is_active(desired_table):
                    continue
                action_collector.add(
                    replica_cluster.name,
                    desired_table.key,
                    CreateTableAction(desired_table, actual_table, replicated_table),
                )
                action_collector.add(
                    replica_cluster.name, desired_table.key, ReshardTableAction(desired_table, actual_table)
                )
        return action_collector.dump(parallel=True)

    def _mount_tables(self) -> list[ActionBatch]:
        action_collector = DbTableActionCollector()
        for cluster in self.destination_desired.mountable_clusters:
            for table in cluster.tables_sorted:
                if not self._is_active(table):
                    continue
                actual_table = self.destination_actual.clusters[cluster.name].tables[table.key]
                action_collector.add(cluster.name, table.key, MountTableAction(actual_table))
        return action_collector.dump(parallel=True)
