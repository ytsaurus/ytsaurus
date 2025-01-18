from dataclasses import dataclass
from dataclasses import field as dataclass_field
import logging
from typing import ClassVar

from yt.yt_sync.action import ActionBatch
from yt.yt_sync.action import DbTableActionCollector
from yt.yt_sync.action import RemoveTableAction
from yt.yt_sync.action import UnmountTableAction
from yt.yt_sync.core import Types
from yt.yt_sync.core.client import YtClientFactory
from yt.yt_sync.core.diff import DbDiff
from yt.yt_sync.core.helpers import is_valid_uuid
from yt.yt_sync.core.model import get_folder
from yt.yt_sync.core.model import is_in_subtree
from yt.yt_sync.core.model import YtCluster
from yt.yt_sync.core.model import YtDatabase
from yt.yt_sync.core.model import YtNativeConsumer
from yt.yt_sync.core.model import YtNode
from yt.yt_sync.core.model import YtTable
from yt.yt_sync.core.settings import Settings
from yt.yt_sync.scenario.helpers import NodeManager
from yt.yt_sync.scenario.helpers import UnmanagedDatabaseBuilder
from yt.yt_sync.scenario.helpers import unregister_queues

from .base import ScenarioBase
from .registry import scenario

LOG = logging.getLogger("yt_sync")


@dataclass
class ReplicaConfig:
    cluster_name: str
    path: str
    replica_id: str
    is_unmanaged: bool

    @property
    def key(self) -> Types.ReplicaKey:
        return (self.cluster_name, self.path)

    @property
    def rich_path(self) -> str:
        return f"{self.cluster_name}:{self.path}"


@dataclass
class FederationConfig:
    main_table: YtTable
    is_unmanaged: bool
    replicas: dict[Types.ReplicaKey, ReplicaConfig] = dataclass_field(default_factory=dict)

    def check_and_log(self, settings: Settings) -> bool:
        success = True
        for replica_key, replica in self.main_table.replicas.items():
            if replica_key not in self.replicas:
                if not self.is_unmanaged:
                    LOG.error(
                        "Replica for managed federation at '%s' is neither managed nor in 'managed_roots' setting",
                        replica.rich_path,
                    )
                    success = False
                elif all(
                    [not is_in_subtree(replica_key[1], r) for r in settings.get_managed_roots_for(replica_key[0])]
                ):
                    LOG.error(
                        "Replica for unmanaged federation at '%s' is neither managed nor in 'managed_roots' setting",
                        replica.rich_path,
                    )
                    success = False
                else:
                    LOG.warning(
                        "Replica for unmanaged federation at '%s' is in 'managed_roots' but does not exist",
                        replica.rich_path,
                    )
                continue

            if replica.replica_id != self.replicas[replica_key].replica_id:
                LOG.error(
                    "Replica at '%s' has conflicting replica_id (in replicated table: '%s', in replica: '%s')",
                    replica.rich_path,
                    replica.replica_id,
                    self.replicas[replica_key].replica_id,
                )
                success = False
        conflicting_management: list[ReplicaConfig] = [
            replica_config
            for replica_config in self.replicas.values()
            if replica_config.is_unmanaged != self.is_unmanaged
        ]
        if conflicting_management:
            LOG.error("Federation is partially managed:")
            LOG.error(
                "  - %s replicated table at '%s'",
                "unmanaged" if self.is_unmanaged else "managed",
                self.main_table.rich_path,
            )
            for replica_config in sorted(self.replicas.values(), key=lambda r: r.is_unmanaged == self.is_unmanaged):
                LOG.error(
                    "  - %s replica at '%s'",
                    "unmanaged" if replica_config.is_unmanaged else "managed",
                    replica_config.rich_path,
                )
            success = False
        return success


@scenario
class DropUnmanagedScenario(ScenarioBase):
    SCENARIO_NAME: ClassVar[str] = "drop_unmanaged"
    SCENARIO_DESCRIPTION: ClassVar[str] = "Drop all unmanaged tables and empty folders"

    def __init__(
        self,
        desired: YtDatabase,
        actual: YtDatabase,
        settings: Settings,
        yt_client_factory: YtClientFactory,
    ):
        super().__init__(desired, actual, settings, yt_client_factory)
        self.unmanaged_db: YtDatabase = YtDatabase(is_chaos=settings.is_chaos)
        self.node_manager = NodeManager(self.yt_client_factory, self.settings)
        self._preserved_paths: dict[str, set[str]] = dict()
        self._federations: list[FederationConfig] = list()
        self._replica_to_federation: dict[Types.ReplicaKey, FederationConfig] = dict()
        self._orphaned_replicas: dict[Types.ReplicaKey, ReplicaConfig] = dict()
        self._drop_nodes: bool = False

    def setup(self, **kwargs):
        self._drop_nodes = kwargs.get("drop_nodes", False)

    def _preserve_node(self, cluster_name: str, node_path: str):
        preserved_cluster_paths = self._preserved_paths.setdefault(cluster_name, set())
        managed_root: str | None = None
        for root in self.settings.get_managed_roots_for(cluster_name):
            if is_in_subtree(node_path, root):
                managed_root = root
                break
        assert managed_root

        while node_path != managed_root:
            preserved_cluster_paths.add(node_path)
            node_path = get_folder(node_path)
            if node_path in preserved_cluster_paths:
                break
        else:
            preserved_cluster_paths.add(node_path)

    def _process_federations(self):
        for db, is_unmanaged in ((self.actual, False), (self.unmanaged_db, True)):
            for cluster in db.clusters.values():
                for table in cluster.tables.values():
                    if table.is_replicated:
                        federation = FederationConfig(main_table=table, is_unmanaged=is_unmanaged)
                        self._federations.append(federation)
                        for replica_key, replica in table.replicas.items():
                            self._replica_to_federation[replica_key] = federation
                            if replica_key not in self._orphaned_replicas:
                                continue

                            federation.replicas[replica_key] = self._orphaned_replicas.pop(replica_key)

                    elif is_valid_uuid(table.attributes.get("upstream_replica_id")):
                        replica = ReplicaConfig(
                            table.cluster_name, table.path, table.attributes.get("upstream_replica_id"), is_unmanaged
                        )
                        if federation := self._replica_to_federation.get(replica.key):
                            federation.replicas[replica.key] = replica
                        else:
                            self._orphaned_replicas[replica.key] = replica

    def _check_federations(self) -> bool:
        success = True
        for federation in self._federations:
            success &= federation.check_and_log(self.settings)
        orphaned_unmanaged_replicas_paths = [
            replica_config.rich_path
            for replica_config in self._orphaned_replicas.values()
            if replica_config.is_unmanaged
        ]
        if orphaned_unmanaged_replicas_paths:
            LOG.debug("Orphaned unmanaged replicas:")
            for replica_path in sorted(orphaned_unmanaged_replicas_paths):
                LOG.debug("  - %s", replica_path)
        return success

    def _preservation_lookup(self, unmanaged_cluster: YtCluster):
        for node in unmanaged_cluster.nodes_sorted:
            if node.node_type == YtNode.Type.FOLDER or self._drop_nodes:
                continue

            self._preserve_node(unmanaged_cluster.name, node.path)

    def _check_cluster(self, unmanaged_cluster: YtCluster):
        self._preservation_lookup(unmanaged_cluster)
        for node in unmanaged_cluster.nodes_sorted:
            if node.node_type != YtNode.Type.FOLDER:
                LOG.debug("Node (%s) at '%s' is not a folder, skip", node.readable_node_type, node.rich_path)
                return
            if node.path in self._preserved_paths.get(unmanaged_cluster.name, set()):
                LOG.debug("Preserved %s at '%s', skip", node.readable_node_type, node.rich_path)

    def _process_consumers(self):
        for unmanaged_cluster in self.unmanaged_db.clusters.values():
            yt_client = self.yt_client_factory(unmanaged_cluster.name)
            for table in unmanaged_cluster.tables_sorted:
                if table.is_consumer:
                    registrations = yt_client.list_queue_consumer_registrations(consumer_path=table.path)
                    consumer = unmanaged_cluster.add_consumer(YtNativeConsumer(table))
                    for registration in registrations:
                        consumer.add_registration(
                            registration["queue_path"].attributes["cluster"],
                            str(registration["queue_path"]),
                            True,
                            None,
                        )

    def _generate_unmanaged_db(self) -> bool:
        diff = DbDiff.generate(self.settings, self.desired, self.actual)
        assert diff.is_valid(), "Database diff is invalid, run 'dump_diff' scenario"

        self.unmanaged_db = UnmanagedDatabaseBuilder(
            self.desired, self.actual, self.yt_client_factory, self.settings
        ).build()
        for cluster in self.unmanaged_db.clusters.values():
            self._check_cluster(cluster)

        success = True
        self._process_federations()
        success &= self._check_federations()
        self._process_consumers()
        return success

    def pre_action(self):
        assert self._generate_unmanaged_db(), "Database state is invalid for drop_unmanaged scenario"
        unregister_queues(self.unmanaged_db, self.yt_client_factory)

    def post_action(self):
        dropped_db = YtDatabase()
        for cluster in self.unmanaged_db.clusters.values():
            folder_cluster = dropped_db.add_or_get_cluster(cluster)
            for node in cluster.nodes.values():
                if node.path not in self._preserved_paths.get(cluster.name, set()):
                    folder_cluster.add_node(node)
        self.node_manager.remove_nodes(dropped_db, dropped_db, remove_folders=True)

    def generate_actions(self) -> list[ActionBatch]:
        result: list[ActionBatch] = list()

        # Unmount affected tables
        action_collector = DbTableActionCollector()
        for cluster in self.unmanaged_db.clusters.values():
            for table in cluster.tables_sorted:
                if not table.exists:
                    continue
                action_collector.add(cluster.name, table.key, UnmountTableAction(table))
        result.extend(action_collector.dump(True))

        # Remove affected tables
        action_collector = DbTableActionCollector()
        for cluster in self.unmanaged_db.clusters.values():
            for table in cluster.tables_sorted:
                if not table.exists:
                    continue
                action_collector.add(cluster.name, table.key, RemoveTableAction(table))
        result.extend(action_collector.dump(True))

        return result
