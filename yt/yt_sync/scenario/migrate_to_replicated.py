import logging
from typing import ClassVar

from yt.yt_sync.action import ActionBatch
from yt.yt_sync.action import CreateReplicatedTableAction
from yt.yt_sync.action import CreateTableAction
from yt.yt_sync.action import DbTableActionCollector
from yt.yt_sync.action import EliminateChunkViews
from yt.yt_sync.action import FreezeTableAction
from yt.yt_sync.action import MountTableAction
from yt.yt_sync.action import MoveTableAction
from yt.yt_sync.action import RemoteCopyAction
from yt.yt_sync.action import RemoveTableAction
from yt.yt_sync.action import ReshardTableAction
from yt.yt_sync.action import SetUpstreamReplicaAction
from yt.yt_sync.action import TableActionCollector
from yt.yt_sync.action import UnmountTableAction
from yt.yt_sync.core.client import YtClientFactory
from yt.yt_sync.core.model import YtDatabase
from yt.yt_sync.core.settings import Settings

from .base import ScenarioBase
from .helpers import ensure_collocation
from .helpers import ensure_db_consumers
from .helpers import NodeManager
from .registry import scenario

LOG = logging.getLogger("yt_sync")


@scenario
class MigrateToReplicated(ScenarioBase):
    SCENARIO_NAME: ClassVar[str] = "migrate_to_replicated"
    SCENARIO_DESCRIPTION: ClassVar[str] = "Migrate from single-cluster tables to replicated tables"

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
            self.has_diff |= self.node_manager.ensure_nodes(
                self.desired, self.actual, mode=NodeManager.EnsureMode.ATTRIBUTES_AND_FOLDERS
            )

    def post_action(self):
        super().post_action()
        if self.settings.ensure_collocation:
            self.has_diff |= ensure_collocation(
                self.desired.main, self.actual.main, self.yt_client_factory, self.settings
            )
        self.has_diff |= ensure_db_consumers(self.desired, self.actual, self.yt_client_factory, clean=True)

    def generate_actions(self) -> list[ActionBatch]:
        result: list[ActionBatch] = list()

        # detect main cluster
        desired_main_cluster = self.desired.main
        actual_main_cluster = self.actual.main

        # eliminate main cluster tables chunk views
        eliminate_chunk_views_action_collector = TableActionCollector(cluster_name=desired_main_cluster.name)
        for desired_table in desired_main_cluster.tables.values():
            actual_table = actual_main_cluster.tables[desired_table.key]
            assert not (
                desired_table.is_chaos_replicated or actual_table.is_chaos_replicated
            ), "Chaos replicated tables aren't supported yet."
            if (
                actual_table.exists
                and desired_table.is_replicated
                and (not actual_table.is_replicated)
                and actual_table.is_data_table
            ):
                eliminate_chunk_views_action_collector.add(actual_table.key, EliminateChunkViews(actual_table))
        result.extend(eliminate_chunk_views_action_collector.dump())

        # freeze main cluster tables
        freeze_main_action_collector = TableActionCollector(cluster_name=desired_main_cluster.name)
        for desired_table in desired_main_cluster.tables.values():
            actual_table = actual_main_cluster.tables[desired_table.key]
            if actual_table.exists and desired_table.is_replicated and (not actual_table.is_replicated):
                freeze_main_action_collector.add(actual_table.key, FreezeTableAction(actual_table))
        result.extend(freeze_main_action_collector.dump())

        # Move tables/queues/consumers to tmp path
        move_main_action_collector = TableActionCollector(cluster_name=desired_main_cluster.name)
        for desired_table in desired_main_cluster.tables.values():
            actual_table = actual_main_cluster.tables[desired_table.key]
            if actual_table.exists and desired_table.is_replicated and (not actual_table.is_replicated):
                actual_tmp_table = actual_main_cluster.get_or_add_temporary_table_for(actual_table)
                move_main_action_collector.add(actual_table.key, UnmountTableAction(actual_table))
                move_main_action_collector.add(actual_table.key, MoveTableAction(actual_table, actual_tmp_table))
        result.extend(move_main_action_collector.dump())

        # create main cluster tables
        create_main_action_collector = TableActionCollector(cluster_name=desired_main_cluster.name)
        for desired_table in desired_main_cluster.tables.values():
            actual_table = actual_main_cluster.tables[desired_table.key]
            if actual_table.exists and (not actual_table.is_replicated) and desired_table.is_replicated:
                create_main_action_collector.add(
                    desired_table.key, CreateReplicatedTableAction(desired_table, actual_table)
                )
        result.extend(create_main_action_collector.dump())

        # create replica tables
        for cluster in self.desired.replicas:
            action_collector = TableActionCollector(cluster_name=cluster.name)
            for table in cluster.tables.values():
                if table.key not in actual_main_cluster.tables:
                    continue
                actual_table = self.actual.clusters[cluster.name].tables[table.key]
                main_table = actual_main_cluster.tables[table.key]
                if main_table.exists and (not main_table.is_replicated):
                    action_collector.add(
                        table.key,
                        CreateTableAction(table, actual_table, None if main_table.is_data_table else main_table),
                    )
            result.extend(action_collector.dump())

        # reshard tables
        for cluster in self.desired.mountable_clusters:
            action_collector = TableActionCollector(cluster_name=cluster.name)
            for table in cluster.tables.values():
                if table.key not in actual_main_cluster.tables:
                    continue
                actual_table = self.actual.clusters[cluster.name].tables[table.key]
                main_table = actual_main_cluster.tables[table.key]
                desired_main_table = desired_main_cluster.tables[table.key]
                if main_table.exists and (not main_table.is_replicated) and desired_main_table.is_replicated:
                    action_collector.add(table.key, ReshardTableAction(table, actual_table))
            result.extend(action_collector.dump())

        # remote copy tables
        remote_copy_collector = DbTableActionCollector(iteration_delay=5)
        for cluster in self.desired.replicas:
            for table in cluster.tables.values():
                actual_table = actual_main_cluster.tables[table.key]
                if actual_table.exists and (not actual_table.is_replicated) and actual_table.is_data_table:
                    from_table = actual_main_cluster.get_or_add_temporary_table_for(table)
                    to_table = self.actual.clusters[cluster.name].tables[table.key]
                    remote_copy_collector.add(
                        cluster.name, table.key, RemoteCopyAction(self.settings, from_table, to_table)
                    )
        result.extend(remote_copy_collector.dump(parallel=True))

        # set upstream replica id
        for cluster in self.desired.replicas:
            action_collector = TableActionCollector(cluster_name=cluster.name)
            for table in cluster.tables.values():
                if table.key not in actual_main_cluster.tables:
                    continue
                actual_table = self.actual.clusters[cluster.name].tables[table.key]
                main_table = actual_main_cluster.tables[table.key]
                if main_table.exists and (not main_table.is_replicated) and main_table.is_data_table:
                    action_collector.add(table.key, SetUpstreamReplicaAction(actual_table, main_table))
            result.extend(action_collector.dump())

        # mount tables
        for cluster in self.desired.mountable_clusters:
            action_collector = TableActionCollector(cluster_name=cluster.name)
            for table in cluster.tables.values():
                if table.key not in actual_main_cluster.tables:
                    continue
                actual_table = self.actual.clusters[cluster.name].tables[table.key]
                main_table = actual_main_cluster.tables[table.key]
                desired_main_table = desired_main_cluster.tables[table.key]
                if main_table.exists and (not main_table.is_replicated) and desired_main_table.is_replicated:
                    action_collector.add(table.key, MountTableAction(actual_table))
            result.extend(action_collector.dump())

        # Remove previously moved tables/queues/consumers
        remove_main_action_collector = TableActionCollector(cluster_name=desired_main_cluster.name)
        for desired_table in desired_main_cluster.tables.values():
            actual_table = actual_main_cluster.tables[desired_table.key]
            if actual_table.exists and desired_table.is_replicated and (not actual_table.is_replicated):
                actual_tmp_table = actual_main_cluster.get_or_add_temporary_table_for(actual_table)
                remove_main_action_collector.add(actual_tmp_table.key, RemoveTableAction(actual_tmp_table))
        result.extend(remove_main_action_collector.dump())

        return result
