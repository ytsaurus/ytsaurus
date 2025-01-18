from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field as dataclass_field
from enum import auto
from enum import Enum
import logging

from yt.yt_sync.core import YtClientFactory
from yt.yt_sync.core.client import YtClientProxy
from yt.yt_sync.core.diff import DbDiff
from yt.yt_sync.core.diff import MissingNode
from yt.yt_sync.core.diff import NodeAttributesChange
from yt.yt_sync.core.diff import NodeDiffType
from yt.yt_sync.core.model import get_folder
from yt.yt_sync.core.model import get_list_lca
from yt.yt_sync.core.model import is_in_subtree
from yt.yt_sync.core.model import Types
from yt.yt_sync.core.model import YtCluster
from yt.yt_sync.core.model import YtDatabase
from yt.yt_sync.core.model import YtNode
from yt.yt_sync.core.model import YtNodeAttributes
from yt.yt_sync.core.settings import Settings

LOG = logging.getLogger("yt_sync")


@dataclass
class _Folder:
    path: str
    attributes: Types.Attributes = dataclass_field(default_factory=dict)
    children: set[_Folder] = dataclass_field(default_factory=set)
    furthest_child_provider: str | None = None
    furthest_child_intersections: int = 0

    def __hash__(self) -> int:
        return self.path.__hash__()

    def separates_batch(self) -> bool:
        return not self.children or self.attributes


class NodeManager:
    class EnsureMode(Enum):
        ATTRIBUTES_ONLY = auto()
        ATTRIBUTES_AND_FOLDERS = auto()
        FULL = auto()

    def __init__(self, yt_client_factory: YtClientFactory, settings: Settings):
        self.yt_client_factory: YtClientFactory = yt_client_factory
        self.settings: Settings = settings

    def _generate_trees(self, db_diff: DbDiff, actual_cluster: YtCluster) -> list[_Folder]:
        folder_nodes: dict[str, _Folder] = dict()
        for table in actual_cluster.tables_sorted:
            if table.exists:
                continue
            assert not actual_cluster.nodes[
                table.folder
            ].is_implicit, f"Access to parent of non-existing {table.table_type} at '{table.rich_path}' is denied"
        for node_diff in db_diff.node_diff_for(actual_cluster.name, {NodeDiffType.MISSING_NODE}):
            assert isinstance(node_diff, MissingNode)
            desired_node = node_diff.desired_node
            if desired_node.node_type == YtNode.Type.FOLDER:
                folder_nodes[desired_node.path] = _Folder(desired_node.path, desired_node.yt_attributes)
            else:
                assert not actual_cluster.nodes[
                    desired_node.folder
                ].is_implicit, f"Access to parent of non-existing {desired_node.readable_node_type} at '{desired_node.rich_path}' is denied"

        if not folder_nodes:
            return []

        lca_path = get_list_lca(list(folder_nodes.keys()))
        LOG.debug("Nodes LCA path: %s", lca_path)

        tree_roots: list[_Folder] = list()
        if lca_folder := folder_nodes.get(lca_path):
            tree_roots.append(lca_folder)

        for folder in sorted(folder_nodes):
            if folder == lca_path:
                continue
            assert not actual_cluster.nodes[folder].exists

            pre_ancestor = folder
            ancestor: str | None = get_folder(folder)
            if not ancestor or ancestor == "/":
                tree_roots.append(folder_nodes[folder])
                continue

            ancestor_node = actual_cluster.nodes[ancestor]
            if ancestor_node.exists:
                pre_ancestor_node = actual_cluster.nodes[pre_ancestor]
                assert (
                    not ancestor_node.is_implicit
                ), f"Access to parent of non-existing folder at '{pre_ancestor_node.rich_path}' is denied"
                tree_roots.append(folder_nodes[folder])
                continue

            while ancestor != "/" and ancestor not in folder_nodes and not actual_cluster.nodes[ancestor].exists:
                folder_nodes[ancestor] = _Folder(ancestor)
                folder_nodes[ancestor].children.add(folder_nodes[pre_ancestor])
                pre_ancestor = ancestor
                ancestor = get_folder(ancestor)

            if ancestor == "/":
                tree_roots.append(folder_nodes[pre_ancestor])
            elif (ancestor_node := actual_cluster.nodes[ancestor]).exists:
                pre_ancestor_node = actual_cluster.nodes[pre_ancestor]
                assert (
                    not ancestor_node.is_implicit
                ), f"Access to parent of non-existing folder at '{pre_ancestor_node.rich_path}' is denied"
                tree_roots.append(folder_nodes[pre_ancestor])
            else:
                folder_nodes[ancestor].children.add(folder_nodes[pre_ancestor])

        for tree_root in tree_roots:
            self._find_furthest_children(tree_root)

        return tree_roots

    def _find_furthest_children(self, tree: _Folder):
        for child in tree.children:
            self._find_furthest_children(child)
            if child.furthest_child_intersections + 1 > tree.furthest_child_intersections:
                tree.furthest_child_provider = child.path
                tree.furthest_child_intersections = child.furthest_child_intersections + 1

    @classmethod
    def _generate_creation_batches(cls, trees: list[_Folder]) -> list[set[_Folder]]:
        batches: list[set[_Folder]] = list()
        active_nodes: set[_Folder] = set(trees)
        while active_nodes:
            batch: set[_Folder] = set()
            next_active_nodes: set[_Folder] = set()
            while active_nodes:
                node = active_nodes.pop()
                if node.separates_batch():
                    batch.add(node)
                    next_active_nodes.update(node.children)
                    continue

                for child in node.children:
                    if child.path != node.furthest_child_provider:
                        next_active_nodes.add(child)
                    else:
                        active_nodes.add(child)
            batches.append(batch)
            active_nodes = next_active_nodes
        return batches

    def _ensure_attributes_for_node(self, node_diff: NodeAttributesChange, batch_client: YtClientProxy) -> bool:
        has_diff: bool = False
        for attribute_path, desired, actual in node_diff.get_changes():
            if desired is None:
                LOG.warning("Remove attribute at '%s/@%s'", node_diff.desired_node.rich_path, attribute_path)
                batch_client.remove(f"{node_diff.desired_node.path}&/@{attribute_path}")
                has_diff = True
            else:
                LOG.warning(
                    "Set attribute at '%s/@%s': actual=%s, desired=%s",
                    node_diff.desired_node.rich_path,
                    attribute_path,
                    actual,
                    desired,
                )
                batch_client.set(f"{node_diff.desired_node.path}&/@{attribute_path}", desired)
                has_diff = True
            node_diff.actual_node.attributes.set_value(attribute_path, desired)
        return has_diff

    def _ensure_nodes_attributes(self, db_diff: DbDiff, batch_client: YtClientProxy) -> bool:
        has_diff: bool = False
        cluster_name = batch_client.cluster
        for node_diff in db_diff.node_diff_for(cluster_name, {NodeDiffType.ATTRIBUTES_CHANGE}):
            assert isinstance(node_diff, NodeAttributesChange)
            actual_node = node_diff.actual_node
            if actual_node.node_type == YtNode.Type.LINK:
                continue

            has_diff |= self._ensure_attributes_for_node(node_diff, batch_client)
        batch_client.commit_batch()
        return has_diff

    def _create_node(self, desired_node: YtNode, actual_node: YtNode, batch_client: YtClientProxy):
        if desired_node.node_type == YtNode.Type.LINK:
            LOG.warning(
                "Create link at '%s' -> '%s:%s'",
                desired_node.rich_path,
                desired_node.cluster_name,
                desired_node.link_target_path,
            )
        else:
            LOG.warning("Create %s at '%s'", desired_node.readable_node_type, desired_node.rich_path)
        batch_client.create(
            type=desired_node.node_type,
            path=desired_node.path,
            attributes=desired_node.yt_attributes,
        )
        actual_node.exists = True
        actual_node.node_type = desired_node.node_type
        actual_node.attributes = YtNodeAttributes.make(desired_node.yt_attributes)
        actual_node.link_target_path = desired_node.link_target_path

    def _ensure_folders(self, db_diff: DbDiff, actual_cluster: YtCluster, batch_client: YtClientProxy) -> bool:
        trees = self._generate_trees(db_diff, actual_cluster)
        batches: list[set[_Folder]] = self._generate_creation_batches(trees)
        has_diff: bool = False
        for batch in batches:
            for folder in sorted(batch, key=lambda f: f.path):
                LOG.warning("Create folder at '%s:%s'", batch_client.cluster, folder.path)
                batch_client.create(
                    type=YtNode.Type.FOLDER,
                    path=folder.path,
                    attributes=folder.attributes,
                    ignore_existing=True,
                    recursive=True,
                )
                actual_cluster.nodes[folder.path].exists = True
                has_diff = True
            batch_client.commit_batch()
        return has_diff

    def _ensure_missing_nodes(self, db_diff: DbDiff, batch_client: YtClientProxy) -> bool:
        has_diff: bool = False
        cluster_name = batch_client.cluster
        for node_diff in db_diff.node_diff_for(cluster_name, {NodeDiffType.MISSING_NODE}):
            assert isinstance(node_diff, MissingNode)
            desired_node = node_diff.desired_node
            assert desired_node.node_type != YtNode.Type.ANY
            if desired_node.node_type == YtNode.Type.FOLDER or desired_node.node_type == YtNode.Type.LINK:
                continue

            self._create_node(desired_node, node_diff.actual_node, batch_client)
            has_diff = True
        batch_client.commit_batch()
        return has_diff

    def _ensure_links_attributes(self, db_diff: DbDiff, batch_client: YtClientProxy) -> bool:
        removed_links: list[NodeAttributesChange] = list()
        has_diff: bool = False
        for node_diff in db_diff.node_diff_for(batch_client.cluster, {NodeDiffType.ATTRIBUTES_CHANGE}):
            assert isinstance(node_diff, NodeAttributesChange)
            actual_node = node_diff.actual_node
            if actual_node.node_type != YtNode.Type.LINK:
                continue
            if node_diff.is_link_path_changed():
                self.request_node_removal(actual_node, batch_client)
                removed_links.append(node_diff)
            else:
                has_diff |= self._ensure_attributes_for_node(node_diff, batch_client)
        batch_client.commit_batch()

        for node_diff in removed_links:
            self._create_node(node_diff.desired_node, node_diff.actual_node, batch_client)
            has_diff = True
        return has_diff

    def _ensure_links(self, db_diff: DbDiff, cluster_name: str, attributes_only: bool) -> bool:
        yt_client = self.yt_client_factory(cluster_name)
        batch_client = yt_client.create_batch_client(raise_errors=True)
        has_diff: bool = False
        if not attributes_only:
            for node_diff in db_diff.node_diff_for(cluster_name, {NodeDiffType.MISSING_NODE}):
                assert isinstance(node_diff, MissingNode)
                desired_node = node_diff.desired_node
                assert desired_node.node_type != YtNode.Type.ANY
                if desired_node.node_type != YtNode.Type.LINK:
                    continue

                self._create_node(desired_node, node_diff.actual_node, batch_client)
                has_diff = True

        has_diff |= self._ensure_links_attributes(db_diff, batch_client)
        batch_client.commit_batch()
        return has_diff

    def _generate_db_diff(self, desired_db: YtDatabase, actual_db: YtDatabase, nodes_only=True) -> DbDiff:
        nodes_db_diff = DbDiff.generate(self.settings, desired_db, actual_db, nodes_only)
        assert nodes_db_diff.is_valid(), "Bad db diff, run 'dump_diff' scenario for details"
        return nodes_db_diff

    def _has_desired_diff(
        self, db_diff: DbDiff, actual_db: YtDatabase, attributes_only: bool, filtered_node_type: str | None
    ) -> bool:
        for cluster in actual_db.clusters.values():
            if db_diff.has_diff_for(cluster.name, {NodeDiffType.ATTRIBUTES_CHANGE}):
                return True
            if attributes_only:
                continue

            if not filtered_node_type:
                if db_diff.has_diff_for(cluster.name, {NodeDiffType.MISSING_NODE}):
                    return True
                continue

            for diff in db_diff.node_diff_for(cluster.name, {NodeDiffType.MISSING_NODE}):
                desired_node = diff.desired_node
                if desired_node.node_type == filtered_node_type:
                    return True
        return False

    def ensure_nodes_from_diff(
        self, db_diff: DbDiff, actual_db: YtDatabase, mode: EnsureMode = EnsureMode.FULL
    ) -> bool:
        if not self._has_desired_diff(
            db_diff,
            actual_db,
            attributes_only=mode == NodeManager.EnsureMode.ATTRIBUTES_ONLY,
            filtered_node_type=YtNode.Type.FOLDER if mode == NodeManager.EnsureMode.ATTRIBUTES_AND_FOLDERS else None,
        ):
            return False

        LOG.info("Ensure YT nodes")
        has_diff: bool = True
        for cluster in actual_db.clusters.values():
            yt_client = self.yt_client_factory(cluster.name)
            batch_client = yt_client.create_batch_client(raise_errors=True)

            has_diff |= self._ensure_nodes_attributes(db_diff, batch_client)
            if mode == NodeManager.EnsureMode.ATTRIBUTES_ONLY:
                continue

            has_diff |= self._ensure_folders(db_diff, cluster, batch_client)
            if mode == NodeManager.EnsureMode.ATTRIBUTES_AND_FOLDERS:
                continue

            has_diff |= self._ensure_missing_nodes(db_diff, batch_client)
        return has_diff

    def ensure_nodes(self, desired_db: YtDatabase, actual_db: YtDatabase, mode: EnsureMode = EnsureMode.FULL) -> bool:
        nodes_db_diff = self._generate_db_diff(desired_db, actual_db)
        return self.ensure_nodes_from_diff(nodes_db_diff, actual_db, mode)

    def ensure_links_from_diff(self, db_diff: DbDiff, actual_db: YtDatabase, attributes_only: bool = False) -> bool:
        if not self._has_desired_diff(db_diff, actual_db, attributes_only, filtered_node_type=YtNode.Type.LINK):
            return False

        LOG.info("Ensure YT links")
        has_diff: bool = False
        for cluster_name in actual_db.clusters:
            has_diff |= self._ensure_links(db_diff, cluster_name, attributes_only)
        return has_diff

    def ensure_links(self, desired_db: YtDatabase, actual_db: YtDatabase, attributes_only: bool = False) -> bool:
        nodes_db_diff = self._generate_db_diff(desired_db, actual_db)
        return self.ensure_links_from_diff(nodes_db_diff, actual_db, attributes_only)

    def _assert_same_cluster(self, node: YtNode, batch_client: YtClientProxy):
        assert (
            node.cluster_name == batch_client.cluster
        ), f"Batch client cluster ({batch_client.cluster}) does not match node cluster ({node.cluster_name})"

    def move_node(self, src_node: YtNode, dst_node: YtNode, batch_client: YtClientProxy):
        self._assert_same_cluster(src_node, batch_client)
        self._assert_same_cluster(dst_node, batch_client)
        assert src_node.node_type == dst_node.node_type, (
            f"Cannot move node of different type from '{src_node.rich_path}' ({src_node.readable_node_type}) "
            + f"to '{dst_node.rich_path}' ({dst_node.readable_node_type})"
        )
        assert src_node.exists, f"Cannot move non-existing node from '{src_node.rich_path}'"
        assert (
            not dst_node.exists
        ), f"Cannot move node from '{src_node.rich_path}' to existing node at '{dst_node.rich_path}'"

        LOG.warning("Move %s from '%s' to '%s'", src_node.readable_node_type, src_node.rich_path, dst_node.rich_path)
        batch_client.move(source_path=f"{src_node.path}&", destination_path=dst_node.path)
        src_node.exists = False
        dst_node.exists = True

    def request_node_removal(self, actual_node: YtNode, batch_client: YtClientProxy):
        self._assert_same_cluster(actual_node, batch_client)
        assert actual_node.exists, f"Cannot remove non-existing node at '{actual_node.rich_path}'"

        if actual_node.node_type == YtNode.Type.LINK:
            LOG.warning(
                "Remove link at '%s' -> '%s:%s'",
                actual_node.rich_path,
                actual_node.cluster_name,
                actual_node.link_target_path,
            )
        else:
            LOG.warning("Remove %s at '%s'", actual_node.readable_node_type, actual_node.rich_path)
        batch_client.remove(actual_node.path, recursive=True)
        actual_node.exists = False

    def remove_nodes(self, desired_db: YtDatabase, actual_db: YtDatabase, remove_folders: bool = False) -> bool:
        has_diff: bool = False
        for actual_cluster in actual_db.clusters.values():
            desired_cluster = desired_db.clusters[actual_cluster.name]
            yt_client = self.yt_client_factory(actual_cluster.name)
            batch_client = yt_client.create_batch_client(raise_errors=True)

            last_removed_node: str | None = None
            for node in actual_cluster.nodes_sorted:
                if not node.exists:
                    continue
                if desired_cluster.nodes[node.path].node_type == YtNode.Type.ANY:
                    LOG.warning("Node with type ANY not removed at '%s'", node.rich_path)
                    continue
                if not remove_folders and node.node_type == YtNode.Type.FOLDER:
                    continue
                if last_removed_node and is_in_subtree(node.path, last_removed_node):
                    continue
                last_removed_node = node.path
                self.request_node_removal(node, batch_client)
                has_diff = True

            batch_client.commit_batch()
        return has_diff
