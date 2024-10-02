import logging
from typing import ClassVar

from yt.yt_sync.action import ActionBatch
from yt.yt_sync.action import AlterTableAttributesAction
from yt.yt_sync.action import CreateReplicatedTableAction
from yt.yt_sync.action import CreateTableAction
from yt.yt_sync.action import DbTableActionCollector
from yt.yt_sync.action import FreezeTableAction
from yt.yt_sync.action import MountTableAction
from yt.yt_sync.action import RemoveTableAction
from yt.yt_sync.action import ReshardTableAction
from yt.yt_sync.action import SetUpstreamReplicaAction
from yt.yt_sync.action import SwitchReplicaStateAction
from yt.yt_sync.action import TableActionCollector
from yt.yt_sync.action import UnmountTableAction
from yt.yt_sync.action import WaitReplicasFlushedAction
from yt.yt_sync.core.client import YtClientFactory
from yt.yt_sync.core.model import YtDatabase
from yt.yt_sync.core.model import YtTable
from yt.yt_sync.core.settings import Settings

from .base import ScenarioBase
from .helpers import ensure_collocation
from .helpers import ensure_db_consumers
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

    def _check_replicas(self):
        desired_replicas = set([r.name for r in self.desired.replicas])
        assert desired_replicas, "Can't migrate non-replicated DB."

        actual_replicas = set([r.name for r in self.actual.replicas])
        assert desired_replicas == actual_replicas, "Can't migrate DB with mismatched replicas"

    def _check_master_tables(self):
        main = self.actual.main
        for table_key in sorted(main.tables):
            actual_table = main.tables[table_key]
            assert actual_table.exists, f"Can't migrate non-existing table {main.name}:{actual_table.path}"
            assert (
                actual_table.table_type == YtTable.Type.REPLICATED_TABLE
            ), f"Can't migrate non-replicated table {main.name}:{actual_table.path} with type {actual_table.table_type}"
            desired_table = self.desired.main.tables[table_key]
            assert (
                actual_table.schema == desired_table.schema
            ), f"Can't migrate with schema diff {actual_table.table_type} {main.name}:{actual_table.path}"

    def _check_replica_tables(self):
        for desired_replica in self.desired.replicas:
            actual_replica = self.actual.clusters[desired_replica.name]
            for table_key in sorted(desired_replica.tables):
                desired_table = desired_replica.tables[table_key]
                actual_table = actual_replica.tables[table_key]
                if desired_table.table_type == YtTable.Type.REPLICATION_LOG:
                    assert (
                        not actual_table.exists
                    ), f"Can't migrate to existing replication_log {actual_replica.name}:{actual_table.path}"
                    continue
                assert (
                    actual_table.exists
                ), f"Can't migrate to non-existing {actual_table.table_type} {actual_replica.name}:{actual_table.path}"
                assert (
                    desired_table.schema == actual_table.schema
                ), f"Can't migrate with schema diff {actual_table.table_type} {actual_replica.name}:{actual_table.path}"

    def pre_action(self):
        super().pre_action()
        self._check_replicas()
        self._check_master_tables()
        self._check_replica_tables()
        unregister_queues(self.actual, self.yt_client_factory)

    def post_action(self):
        super().post_action()
        ensure_collocation(self.desired.main, self.actual.main, self.yt_client_factory, self.settings)
        ensure_db_consumers(self.desired, self.actual, self.yt_client_factory, True)

    def generate_actions(self) -> list[ActionBatch]:
        result: list[ActionBatch] = []

        result.extend(self._remove_replicated())
        result.extend(self._create_chaos_replicated_tables())
        result.extend(self._create_replicaton_logs_and_ensure_table_attributes())
        result.extend(self._mount_tables())

        return result

    def _remove_replicated(self) -> list[ActionBatch]:
        action_collector = TableActionCollector(self.actual.main.name)
        for table_key in sorted(self.actual.main.tables):
            actual_table = self.actual.main.tables[table_key]
            action_collector.add(table_key, FreezeTableAction(actual_table))
            action_collector.add(table_key, WaitReplicasFlushedAction(actual_table))
            action_collector.add(table_key, SwitchReplicaStateAction(actual_table, False))
            action_collector.add(table_key, UnmountTableAction(actual_table))
            action_collector.add(table_key, RemoveTableAction(actual_table))
        return action_collector.dump()

    def _create_chaos_replicated_tables(self) -> list[ActionBatch]:
        action_collector = TableActionCollector(self.desired.main.name)
        for table_key in sorted(self.desired.main.tables):
            desired_table = self.desired.main.tables[table_key]
            actual_table = self.actual.main.tables[table_key]
            action_collector.add(table_key, CreateReplicatedTableAction(desired_table, actual_table))
        return action_collector.dump()

    def _create_replicaton_logs_and_ensure_table_attributes(self) -> list[ActionBatch]:
        action_collector = DbTableActionCollector()
        for replica_cluster in self.desired.replicas:
            for table_key in sorted(replica_cluster.tables):
                desired_table = replica_cluster.tables[table_key]
                actual_table = self.actual.clusters[replica_cluster.name].tables[table_key]
                replicated_table = self.actual.get_replicated_table_for(actual_table)
                if desired_table.table_type == YtTable.Type.REPLICATION_LOG:
                    action_collector.add(
                        replica_cluster.name,
                        table_key,
                        CreateTableAction(desired_table, actual_table, replicated_table),
                    )
                    action_collector.add(
                        replica_cluster.name, table_key, ReshardTableAction(desired_table, actual_table)
                    )
                else:
                    action_collector.add(replica_cluster.name, table_key, UnmountTableAction(actual_table))
                    action_collector.add(
                        replica_cluster.name, table_key, AlterTableAttributesAction(desired_table, actual_table)
                    )
                action_collector.add(
                    replica_cluster.name, table_key, SetUpstreamReplicaAction(actual_table, replicated_table)
                )
        return action_collector.dump(parallel=True)

    def _mount_tables(self) -> list[ActionBatch]:
        action_collector = DbTableActionCollector()
        for cluster in self.desired.mountable_clusters:
            for table in cluster.tables.values():
                actual_table = self.actual.clusters[cluster.name].tables[table.key]
                action_collector.add(cluster.name, table.key, MountTableAction(actual_table))
        return action_collector.dump(parallel=True)
