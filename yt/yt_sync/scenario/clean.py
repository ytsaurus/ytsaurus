import logging
from typing import ClassVar

from yt.yt_sync.action import ActionBatch
from yt.yt_sync.action import CreateReplicatedTableAction
from yt.yt_sync.action import CreateTableAction
from yt.yt_sync.action import MountTableAction
from yt.yt_sync.action import RegisterQueueExportAction
from yt.yt_sync.action import RemoveTableAction
from yt.yt_sync.action import ReshardTableAction
from yt.yt_sync.action import TableActionCollector
from yt.yt_sync.core.client import YtClientFactory
from yt.yt_sync.core.model import YtDatabase
from yt.yt_sync.core.model import YtTable
from yt.yt_sync.core.settings import Settings

from .base import ScenarioBase
from .helpers import ensure_collocation
from .helpers import ensure_db_consumers
from .helpers import NodeManager
from .registry import scenario

LOG = logging.getLogger("yt_sync")


@scenario
class CleanScenario(ScenarioBase):
    SCENARIO_NAME: ClassVar[str] = "clean"
    SCENARIO_DESCRIPTION: ClassVar[str] = "Re-create all tables from scratch"

    def __init__(
        self,
        desired: YtDatabase,
        actual: YtDatabase,
        settings: Settings,
        yt_client_factory: YtClientFactory,
    ):
        super().__init__(desired, actual, settings, yt_client_factory)
        self.node_manager = NodeManager(self.yt_client_factory, self.settings)

    def pre_action(self):
        if self.settings.ensure_folders:
            self.has_diff |= self.node_manager.remove_nodes(self.desired, self.actual, remove_folders=False)
            self.has_diff |= self.node_manager.ensure_nodes(self.desired, self.actual)

    def post_action(self):
        super().post_action()
        if self.settings.ensure_folders:
            self.has_diff |= self.node_manager.ensure_links(self.desired, self.actual)
        if self.settings.ensure_collocation:
            self.has_diff |= ensure_collocation(
                self.desired.main, self.actual.main, self.yt_client_factory, self.settings
            )
        self.has_diff |= ensure_db_consumers(self.desired, self.actual, self.yt_client_factory, clean=True)

    def generate_actions(self) -> list[ActionBatch]:
        result: list[ActionBatch] = list()

        # remove existing tables
        for cluster in self.actual.all_clusters:
            action_collector = TableActionCollector(cluster_name=cluster.name)
            for table in cluster.tables.values():
                if table.exists:
                    action_collector.add(table.key, RemoveTableAction(table))
            result.extend(action_collector.dump())

        # detect main cluster
        desired_main_cluster = self.desired.main
        actual_main_cluster = self.actual.main

        # create main cluster tables
        create_main_action_collector = TableActionCollector(cluster_name=desired_main_cluster.name)
        for desired_table in desired_main_cluster.tables.values():
            actual_table = actual_main_cluster.tables[desired_table.key]
            if desired_table.is_replicated:
                create_main_action_collector.add(
                    desired_table.key, CreateReplicatedTableAction(desired_table, actual_table)
                )
            else:
                create_main_action_collector.add(desired_table.key, CreateTableAction(desired_table, actual_table))
                if "static_export_config" in desired_table.attributes:
                    create_main_action_collector.add(
                        desired_table.key, RegisterQueueExportAction(desired_table, actual_table, clean=True)
                    )
        result.extend(create_main_action_collector.dump())

        # create replica tables
        for cluster in self.desired.replicas:
            action_collector = TableActionCollector(cluster_name=cluster.name)
            for table in cluster.tables.values():
                if YtTable.Type.REPLICATION_LOG == table.table_type:
                    assert table.chaos_data_table
                    replicated_table = actual_main_cluster.tables[table.chaos_data_table]
                else:
                    replicated_table = actual_main_cluster.tables[table.key]
                actual_table = self.actual.clusters[cluster.name].tables[table.key]
                action_collector.add(table.key, CreateTableAction(table, actual_table, replicated_table))
                if "static_export_config" in table.attributes:
                    action_collector.add(table.key, RegisterQueueExportAction(table, None, clean=True))
            result.extend(action_collector.dump())

        # reshard tables
        for cluster in self.desired.mountable_clusters:
            action_collector = TableActionCollector(cluster_name=cluster.name)
            for table in cluster.tables.values():
                actual_table = self.actual.clusters[cluster.name].tables[table.key]
                action_collector.add(table.key, ReshardTableAction(table, actual_table))
            result.extend(action_collector.dump())

        # mount tables (should be done only after all tables are resharded for chaos sake)
        for cluster in self.desired.mountable_clusters:
            action_collector = TableActionCollector(cluster_name=cluster.name)
            for table in cluster.tables.values():
                actual_table = self.actual.clusters[cluster.name].tables[table.key]
                action_collector.add(table.key, MountTableAction(actual_table))
            result.extend(action_collector.dump())

        return result
