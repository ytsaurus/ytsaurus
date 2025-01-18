from copy import deepcopy
import logging
from typing import Any
from typing import ClassVar

from yt.yt_sync.action import ActionBatch
from yt.yt_sync.action import CreateReplicatedTableAction
from yt.yt_sync.action import DbTableActionCollector
from yt.yt_sync.action import FreezeTableAction
from yt.yt_sync.action import MountTableAction
from yt.yt_sync.action import MoveTableAction
from yt.yt_sync.action import RemoveTableAction
from yt.yt_sync.action import ReshardTableAction
from yt.yt_sync.action import SetUpstreamReplicaAction
from yt.yt_sync.action import SwitchReplicaStateAction
from yt.yt_sync.action import UnmountTableAction
from yt.yt_sync.action import WaitReplicasFlushedAction
from yt.yt_sync.core.client import YtClientFactory
from yt.yt_sync.core.client import YtClientProxy
from yt.yt_sync.core.model import is_in_subtree
from yt.yt_sync.core.model import Types
from yt.yt_sync.core.model import YtDatabase
from yt.yt_sync.core.model import YtNode
from yt.yt_sync.core.model import YtReplica
from yt.yt_sync.core.model import YtTable
from yt.yt_sync.core.settings import Settings
from yt.yt_sync.core.state_builder import ActualStateBuilder

from .base import ScenarioBase
from .helpers import NodeManager
from .helpers import unregister_queues
from .registry import scenario

LOG = logging.getLogger("yt_sync")


def patch_path(cluster_name: str, path: str, destination_config: dict[str, dict[str, str]], force: bool = True) -> str:
    from_to: dict[str, str] | None = destination_config.get(cluster_name, destination_config.get("default", None))
    if from_to is None:
        return path

    from_prefix = from_to["from"]
    to_prefix = from_to["to"]

    if is_in_subtree(path, to_prefix):
        return path
    if is_in_subtree(path, from_prefix):
        return to_prefix + path[len(from_prefix) :]

    if not force:
        return path
    assert False, f"Path '{path}' doesn't match prefix '{from_prefix}' on cluster '{cluster_name}'"


def _make_patched_replicas(
    replicas: dict[Types.ReplicaKey, YtReplica], destination_config: dict[str, dict[str, str]]
) -> dict[Types.ReplicaKey, YtReplica]:
    result: dict[Types.ReplicaKey, YtReplica] = dict()
    for replica in replicas.values():
        destination_replica = deepcopy(replica)
        destination_replica.replica_path = patch_path(replica.cluster_name, replica.replica_path, destination_config)
        result[destination_replica.key] = destination_replica
    return result


@scenario
class MoveScenario(ScenarioBase):
    SCENARIO_NAME: ClassVar[str] = "move"
    SCENARIO_DESCRIPTION: ClassVar[str] = "Move all tables to another folder"

    def __init__(
        self,
        desired: YtDatabase,
        actual: YtDatabase,
        settings: Settings,
        yt_client_factory: YtClientFactory,
    ):
        assert not settings.is_chaos, "Chaos DB is not supported yet"
        super().__init__(desired, actual, settings, yt_client_factory)

        self.destination_desired: YtDatabase = YtDatabase(is_chaos=self.settings.is_chaos)
        self.destination_actual: YtDatabase = YtDatabase(is_chaos=self.settings.is_chaos)
        self._destination_config: dict[str, dict[str, str]] = dict()
        self._skipped_items: set[Types.ReplicaKey] = set()
        # cluster_name -> (source_path -> destination_path)
        self._src_to_dst: dict[str, dict[str, str]] = dict()
        # cluster_name -> (destination_path -> source_path)
        self._dst_to_src: dict[str, dict[str, str]] = dict()

        self.node_manager = NodeManager(self.yt_client_factory, self.settings)

    def setup(self, **kwargs):
        self._destination_config = kwargs.get("destination_config")
        LOG.debug("Destination config: %s", self._destination_config)

        assert isinstance(
            self._destination_config, dict
        ), f"Destination config must be dict, got {type(self._destination_config)}"
        assert self._destination_config, "Destination config cannot be empty"

        def _ensure_path(from_to: dict[str, Any], key: str, cluster_name: str):
            value = from_to.get(key, None)
            assert (
                isinstance(value, str) and value
            ), f"Bad key '{key}' in destination config for '{cluster_name}', expected non-empty string, got '{value}'"
            assert (
                value != "/"
            ), f"Path for key '{key}' of destination config for '{cluster_name}' is root, which is not allowed"
            from_to[key] = value.rstrip("/")

        for cluster_name, from_to in self._destination_config.items():
            assert (
                cluster_name == "default" or cluster_name in self.desired.clusters
            ), f"Unknown cluster '{cluster_name}' in destination config"
            if cluster_name == "default" and from_to is None:
                assert len(self._destination_config) > 1, "Cluster must be specified for partial move"
                continue
            else:
                assert isinstance(
                    from_to, dict
                ), f"Cluster '{cluster_name}' destination config must be dict, got {type(from_to)}"
                _ensure_path(from_to, "from", cluster_name)
                _ensure_path(from_to, "to", cluster_name)

            if cluster_name == "default" and len(self._destination_config) > 1:
                # Partial move is possible, not checking default setting
                continue
            assert from_to["from"] != from_to["to"], f"Equal 'from' and 'to' in destination config for '{cluster_name}'"

    @classmethod
    def build_destination_desired(
        cls, desired: YtDatabase, settings: Settings, destination_config: dict[str, dict[str, str]]
    ) -> YtDatabase:
        destination_desired = YtDatabase(is_chaos=settings.is_chaos)
        for cluster in desired.clusters.values():
            destination_cluster = destination_desired.add_or_get_cluster(cluster)
            for table in cluster.tables.values():
                destination_table = deepcopy(table)
                destination_table.path = patch_path(cluster.name, table.path, destination_config)

                if destination_table.is_replicated:
                    destination_table.replicas = _make_patched_replicas(table.replicas, destination_config)

                destination_cluster.add_table(destination_table)

            for node in cluster.nodes.values():
                destination_node = deepcopy(node)
                destination_node.path = patch_path(cluster.name, node.path, destination_config)
                if destination_node.node_type == YtNode.Type.LINK:
                    destination_node.link_target_path = patch_path(
                        cluster.name, node.link_target_path, destination_config, force=False
                    )
                destination_cluster.add_node(destination_node)

        destination_desired.ensure_db_integrity(settings.always_async)
        return destination_desired

    def _generate_mappings(self):
        for cluster in self.desired.clusters.values():
            destination_cluster = self.destination_desired.add_or_get_cluster(cluster)
            for table in cluster.tables.values():
                destination_table = destination_cluster.tables[table.key]
                self._src_to_dst.setdefault(cluster.name, dict())[table.path] = destination_table.path
                self._dst_to_src.setdefault(cluster.name, dict())[destination_table.path] = table.path

            for node in cluster.nodes.values():
                destination_node_path = patch_path(cluster.name, node.path, self._destination_config)
                self._src_to_dst.setdefault(cluster.name, dict())[node.path] = destination_node_path
                self._dst_to_src.setdefault(cluster.name, dict())[destination_node_path] = node.path

    def _item_type_name(self, item: YtTable | YtNode) -> str:
        return "Table" if isinstance(item, YtTable) else "Node"

    def _check_inplace(
        self, source_item: YtTable | YtNode, destination_item: YtTable | YtNode, log: bool = True
    ) -> bool:
        if source_item.replica_key != destination_item.replica_key:
            return False
        if (
            isinstance(source_item, YtTable)
            and source_item.is_replicated
            and not self._check_federation_inplace(source_item.key)
        ):
            LOG.warning(
                "Replicated table is inplace but federation with key '%s' is not inplace, recreate",
                source_item.rich_path,
            )
            return False
        LOG.info(
            "%s at %s is already inplace, skip",
            self._item_type_name(source_item),
            destination_item.rich_path,
        )
        self._skipped_items.add(source_item.replica_key)
        return True

    def _check_already_moved(self, source_item: YtTable | YtNode, destination_item: YtTable | YtNode) -> bool:
        if source_item.exists or not destination_item.exists:
            return False
        if (
            isinstance(source_item, YtTable)
            and source_item.is_replicated
            and not self._check_federation_inplace(source_item.key)
        ):
            LOG.warning(
                "Replicated table is already moved but table federation with key '%s' -> '%s' is not inplace, recreate",
                source_item.rich_path,
                destination_item.rich_path,
            )
            return False
        LOG.info(
            "%s does not exist in source (%s) but in destination (%s), skip",
            self._item_type_name(source_item),
            source_item.rich_path,
            destination_item.rich_path,
        )
        self._skipped_items.add(source_item.replica_key)
        return True

    def _check_missing_item(self, source_item: YtTable | YtNode, destination_item: YtTable | YtNode) -> bool:
        if source_item.exists or destination_item.exists:
            return False
        LOG.error(
            "%s not found neither in source (%s) nor in destination (%s)",
            self._item_type_name(source_item),
            source_item.rich_path,
            destination_item.rich_path,
        )
        return True

    def _check_federation_inplace(self, source_table_key: str) -> bool:
        src_actual_main = self.actual.main
        if not src_actual_main.tables[source_table_key].is_replicated:
            return False

        for src_cluster in self.actual.clusters.values():
            if source_table_key not in src_cluster.tables:
                continue
            if src_cluster.name != src_actual_main.name and src_cluster.tables[source_table_key].exists:
                return False

        for dst_cluster in self.destination_actual.clusters.values():
            if source_table_key not in dst_cluster.tables:
                continue
            if not dst_cluster.tables[source_table_key].exists:
                return False

        src_main_table = src_actual_main.tables[source_table_key]
        if not src_main_table.exists:
            return True

        dst_main_table = self.destination_actual.main.tables[source_table_key]
        return (
            dst_main_table.replicas.keys()
            == _make_patched_replicas(src_main_table.replicas, self._destination_config).keys()
            and all(not replica.enabled for replica in src_main_table.replicas.values())
            and all(replica.enabled for replica in dst_main_table.replicas.values())
        )

    def _check_actual_table_state(self, source_table: YtTable, destination_table: YtTable) -> bool:
        if source_table.table_type != destination_table.table_type:
            LOG.error(
                "Table from '%s' to '%s' has different source (%s) and destination (%s) types",
                source_table.rich_path,
                destination_table.rich_path,
                source_table.table_type,
                destination_table.table_type,
            )
            return False
        if source_table.exists and destination_table.exists and source_table.schema != destination_table.schema:
            LOG.error(
                "Table from '%s' to '%s' has different source and destination schemas:\n  Source: %s\n  Destination: %s",
                source_table.rich_path,
                destination_table.rich_path,
                source_table.schema,
                destination_table.schema,
            )
            return False

        if source_table.is_ordered and self.actual.get_replicated_table_for(source_table).is_replicated:
            LOG.info("Ordered replicated tables (at '%s') not supported yet (YT-21995), skip", source_table.rich_path)
            self._skipped_items.add(source_table.replica_key)
            return True

        if self._check_inplace(source_table, destination_table):
            return True

        if self._check_missing_item(source_table, destination_table):
            return False

        if source_table.exists and destination_table.exists:
            if source_table.is_replicated:
                if self._check_federation_inplace(source_table.key):
                    LOG.info(
                        "Replicated table with inplace federation exists both in source (%s) and in destination (%s), skip",
                        source_table.rich_path,
                        destination_table.rich_path,
                    )
                    self._skipped_items.add(source_table.replica_key)
                elif (
                    destination_table.replicas.keys()
                    == _make_patched_replicas(source_table.replicas, self._destination_config).keys()
                ):
                    LOG.warning(
                        "Replicated table with same config exists both in source (%s) and in destination (%s), recreate",
                        source_table.rich_path,
                        destination_table.rich_path,
                    )
                return True
            LOG.error(
                "Table with type '%s' exists both in source (%s) and in destination (%s)",
                source_table.table_type,
                source_table.rich_path,
                destination_table.rich_path,
            )
            return False

        self._check_already_moved(source_table, destination_table)
        return True

    def _check_actual_node_state(self, source_node: YtNode, destination_node: YtNode) -> bool:
        if source_node.node_type != destination_node.node_type:
            LOG.error(
                "Node from '%s' to '%s' has different source (%s) and destination (%s) types",
                source_node.rich_path,
                destination_node.rich_path,
                source_node.readable_node_type,
                destination_node.readable_node_type,
            )
            return False

        if self._check_inplace(source_node, destination_node):
            return True

        if self._check_missing_item(source_node, destination_node):
            return False

        if source_node.exists and destination_node.exists:
            match source_node.node_type:
                case YtNode.Type.FOLDER:
                    return True
                case YtNode.Type.LINK:
                    if source_node.yt_attributes["target_path"] != destination_node.yt_attributes["target_path"]:
                        LOG.error(
                            "Links with different target paths exists both in source (%s) and in destination (%s)",
                            source_node.rich_path,
                            destination_node.rich_path,
                        )
                        return False
                    return True
                case _:
                    LOG.error(
                        "Node with type '%s' exists both in source (%s) and in destination (%s)",
                        source_node.readable_node_type,
                        source_node.rich_path,
                        destination_node.rich_path,
                    )
                    return False

        self._check_already_moved(source_node, destination_node)
        return True

    def _build_destination_actual(self) -> bool:
        self.destination_actual = ActualStateBuilder(self.settings, self.yt_client_factory).build_from(
            self.destination_desired
        )
        result = True
        for src_cluster in self.actual.clusters.values():
            dst_cluster = self.destination_actual.clusters[src_cluster.name]
            for src_table in src_cluster.tables_sorted:
                dst_table = dst_cluster.tables[src_table.key]
                result &= self._check_actual_table_state(src_table, dst_table)

            for src_node in src_cluster.nodes_sorted:
                dst_path = self._src_to_dst[src_cluster.name][src_node.path]
                dst_node = dst_cluster.nodes[dst_path]
                result &= self._check_actual_node_state(src_node, dst_node)
        return result

    def _drop_offsets(self) -> bool:
        has_diff: bool = False
        src_actual_main = self.actual.main
        yt_client: YtClientProxy = self.yt_client_factory(src_actual_main.name)
        with yt_client.Transaction(type="tablet"):
            for consumer in sorted(src_actual_main.consumers.values(), key=lambda c: c.table.path):
                for registration in sorted(consumer.registrations.values(), key=lambda r: r.path):
                    if registration.cluster_name == src_actual_main.name and src_actual_main.find_table_by_path(
                        registration.path
                    ):
                        LOG.warning(
                            "Drop offsets in consumer %s:%s for queue %s:%s",
                            consumer.table.cluster_name,
                            consumer.table.path,
                            registration.cluster_name,
                            registration.path,
                        )
                        rows = yt_client.select_rows(
                            f"queue_cluster, queue_path, partition_index from [{consumer.table.path}] where queue_path='{registration.path}'"
                        )
                        yt_client.delete_rows(table=consumer.table.path, input_stream=rows)
                        has_diff = True
                    else:
                        LOG.info("Skip external registration %s:%s", registration.cluster_name, registration.path)
        return has_diff

    def _skip_table(self, table: YtTable, dst_to_src: bool = False) -> bool:
        return (
            table.cluster_name,
            table.path if not dst_to_src else self._dst_to_src[table.cluster_name][table.path],
        ) in self._skipped_items

    def _setup_destination_database(self) -> bool:
        self.destination_desired = self.build_destination_desired(self.desired, self.settings, self._destination_config)
        self._generate_mappings()
        assert self._build_destination_actual(), "Database state is invalid for move scenario"
        has_diff: bool = False
        if self.settings.ensure_folders:
            has_diff |= self.node_manager.ensure_nodes(
                self.destination_desired, self.destination_actual, mode=NodeManager.EnsureMode.ATTRIBUTES_AND_FOLDERS
            )

            moved_nodes = False
            # Move all nodes
            for cluster in self.actual.clusters.values():
                yt_client = self.yt_client_factory(cluster.name)
                batch_client = yt_client.create_batch_client(raise_errors=True)
                dst_cluster = self.destination_actual.clusters[cluster.name]
                for node in cluster.nodes_sorted:
                    dst_path = self._src_to_dst[cluster.name][node.path]
                    if not node.exists or node.node_type == YtNode.Type.FOLDER or dst_path == node.path:
                        continue
                    if node.node_type == YtNode.Type.LINK:
                        self.node_manager.request_node_removal(node, batch_client)
                        has_diff = True
                        continue
                    moved_nodes = True

                    self.node_manager.move_node(
                        node,
                        dst_cluster.nodes[dst_path],
                        batch_client,
                    )
                    has_diff = True
                batch_client.commit_batch()
            if moved_nodes:
                has_diff |= self.node_manager.ensure_nodes(
                    self.destination_desired, self.destination_actual, mode=NodeManager.EnsureMode.ATTRIBUTES_ONLY
                )
        return has_diff

    def pre_action(self):
        super().pre_action()

        self.has_diff |= self._drop_offsets()
        self.has_diff |= unregister_queues(self.actual, self.yt_client_factory)
        self.has_diff |= self._setup_destination_database()

    def post_action(self):
        super().post_action()
        if self.settings.ensure_folders:
            self.has_diff |= self.node_manager.ensure_links(self.destination_desired, self.destination_actual)
        LOG.warning("Run 'ensure' scenario now for destination settings")

    def generate_actions(self) -> list[ActionBatch]:
        result: list[ActionBatch] = list()

        src_actual_main = self.actual.main
        dst_actual_main = self.destination_actual.main
        dst_desired_main = self.destination_desired.main

        main_cluster_name = src_actual_main.name

        # freeze, wait, disable replicated tables from source and destination
        action_collector = DbTableActionCollector()
        for main_cluster, is_destination in (
            (src_actual_main, False),
            (dst_actual_main, True),
        ):
            for table in main_cluster.tables_sorted:
                if not table.exists or not table.is_replicated or self._skip_table(table, dst_to_src=is_destination):
                    continue
                if not is_destination and self._src_to_dst[main_cluster_name][table.path] == table.path:
                    continue
                action_collector.add(main_cluster_name, table.key, FreezeTableAction(table))
                action_collector.add(main_cluster_name, table.key, WaitReplicasFlushedAction(table))
                action_collector.add(main_cluster_name, table.key, SwitchReplicaStateAction(table, enabled=False))
        result.extend(action_collector.dump(True))

        # unmount source and destination tables
        action_collector = DbTableActionCollector()
        for db, is_destination in (
            (self.actual, False),
            (self.destination_actual, True),
        ):
            for cluster in db.mountable_clusters:
                for table in cluster.tables_sorted:
                    if not table.exists:
                        continue

                    replicated_table: YtTable = db.get_replicated_table_for(table)
                    if self._skip_table(replicated_table, dst_to_src=is_destination):
                        continue
                    if not is_destination and self._src_to_dst[cluster.name][table.path] == table.path:
                        continue

                    action_collector.add(cluster.name, table.key, UnmountTableAction(table))
        result.extend(action_collector.dump(True))

        # remove existing replicated tables from destination
        action_collector = DbTableActionCollector()
        for table in dst_actual_main.tables_sorted:
            if not table.exists or not table.is_replicated or self._skip_table(table, dst_to_src=True):
                continue
            action_collector.add(main_cluster_name, table.key, RemoveTableAction(table))
        result.extend(action_collector.dump(True))

        # create main cluster replicated tables
        action_collector = DbTableActionCollector()
        for actual_table in src_actual_main.tables_sorted:
            if not actual_table.is_replicated or self._skip_table(actual_table):
                continue
            table_key = actual_table.key
            destination_desired_table = dst_desired_main.tables[table_key]
            destination_actual_table = dst_actual_main.tables[table_key]

            action_collector.add(
                main_cluster_name,
                table_key,
                CreateReplicatedTableAction(destination_desired_table, destination_actual_table),
            )
            action_collector.add(
                main_cluster_name,
                table_key,
                ReshardTableAction(destination_desired_table, destination_actual_table),
            )
        result.extend(action_collector.dump(True))

        # move data tables
        action_collector = DbTableActionCollector()
        for cluster in self.actual.clusters.values():
            for from_table in cluster.tables_sorted:
                if from_table.is_replicated or self._skip_table(from_table):
                    continue
                to_table = self.destination_actual.clusters[cluster.name].tables[from_table.key]
                action_collector.add(cluster.name, to_table.key, MoveTableAction(from_table, to_table))
        result.extend(action_collector.dump(True))

        # set upstream replica ids
        action_collector = DbTableActionCollector()
        for replica_cluster in self.destination_actual.replicas:
            for table in replica_cluster.tables_sorted:
                replicated_table: YtTable = self.destination_actual.get_replicated_table_for(table)

                if self._skip_table(replicated_table, dst_to_src=True):
                    continue
                action_collector.add(
                    replica_cluster.name, replicated_table.key, SetUpstreamReplicaAction(table, replicated_table)
                )
        result.extend(action_collector.dump(True))

        # mount tables
        action_collector = DbTableActionCollector()
        for cluster in self.destination_actual.mountable_clusters:
            for table in cluster.tables_sorted:
                action_collector.add(cluster.name, table.key, MountTableAction(table))
        result.extend(action_collector.dump(True))

        # remove replicated tables from source
        action_collector = DbTableActionCollector()
        for table in src_actual_main.tables_sorted:
            if (
                not table.is_replicated
                or table.is_ordered
                or (self._skip_table(table) and not table.exists)
                or self._src_to_dst[main_cluster_name][table.path] == table.path
            ):
                continue
            action_collector.add(main_cluster_name, table.key, RemoveTableAction(table))
        result.extend(action_collector.dump(True))

        return result
