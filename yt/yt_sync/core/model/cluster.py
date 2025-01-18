from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from enum import auto
from enum import StrEnum
from typing import Generator

from .helpers import get_folder
from .helpers import get_list_lca
from .helpers import iter_attributes_recursively
from .helpers import make_log_name
from .helpers import make_tmp_name
from .native_consumer import YtNativeConsumer
from .node import YtNode
from .table import QueueOptions
from .table import YtTable
from .table_attributes import YtTableAttributes
from .tablet_state import YtTabletState
from .types import Types


@dataclass
class YtCluster:
    class Type(StrEnum):
        SINGLE = auto()
        MAIN = auto()
        REPLICA = auto()

    class Mode(StrEnum):
        SYNC = auto()
        ASYNC = auto()
        MIXED = auto()

    name: str
    cluster_type: Type
    is_chaos: bool
    mode: Mode | None = None

    # table.key -> table
    tables: dict[str, YtTable] = dataclass_field(default_factory=dict)
    tables_by_path: dict[str, YtTable] = dataclass_field(default_factory=dict)
    # consumer.table.key -> consumer
    consumers: dict[str, YtNativeConsumer] = dataclass_field(default_factory=dict)
    # path -> node
    nodes: dict[str, YtNode] = dataclass_field(default_factory=dict)
    nodes_by_link_target: dict[str, YtNode] = dataclass_field(default_factory=dict)

    _parent_folders: set[str] = dataclass_field(default_factory=set)

    @property
    def tables_sorted(self) -> Generator[YtTable, None, None]:
        for table_key in sorted(self.tables):
            yield self.tables[table_key]

    @property
    def nodes_sorted(self) -> Generator[YtNode, None, None]:
        for node_path in sorted(self.nodes):
            yield self.nodes[node_path]

    @property
    def is_main(self):
        return self.cluster_type in (self.Type.MAIN, self.Type.SINGLE)

    @property
    def is_replica(self):
        return self.cluster_type == self.Type.REPLICA

    @property
    def is_sync_replica(self):
        return self.is_replica and self.mode == self.Mode.SYNC

    @property
    def is_async_replica(self):
        return self.is_replica and self.mode == self.Mode.ASYNC

    @property
    def is_mixed_replica(self):
        return self.is_replica and (self.mode == self.Mode.MIXED or self.mode is None)

    @classmethod
    def make(
        cls,
        cluster_type: YtCluster.Type,
        name: str,
        is_chaos: bool,
        mode: YtCluster.Mode | None = None,
    ) -> YtCluster:
        return cls(name=name, cluster_type=cluster_type, is_chaos=is_chaos, mode=mode)

    def add_table(self, table: YtTable) -> YtTable:
        assert table.key not in self.tables, f"Duplicated table '{table.key}'"
        assert not self.find_table_by_path(table.path), f"Table at path '{table.path}' already exists"
        assert table.path not in self.nodes, f"Node at table's path '{table.path}' already exists"

        self._check_path_clearance(table)

        self.tables[table.key] = table
        self.tables_by_path[table.path] = table
        return table

    def add_table_from_attributes(
        self,
        table_key: str,
        table_type: str,
        table_path: str,
        exists: bool,
        table_attributes: Types.Attributes,
        source_attributes: YtTableAttributes | None = None,
        explicit_in_collocation: bool | None = None,
        ensure_empty_schema: bool = True,
        queue_options: QueueOptions | None = None,
    ) -> YtTable:
        table = YtTable.make(
            key=table_key,
            cluster_name=self.name,
            table_type=table_type,
            path=table_path,
            exists=exists,
            attributes=table_attributes,
            source_attributes=source_attributes,
            explicit_in_collocation=explicit_in_collocation,
            ensure_empty_schema=ensure_empty_schema,
            queue_options=queue_options,
        )
        return self.add_table(table)

    def get_or_add_temporary_table_for(self, table: YtTable, exists: bool | None = None) -> YtTable:
        tmp_key = make_tmp_name(table.key)
        if tmp_key in self.tables:
            return self.tables[tmp_key]
        tmp_table = deepcopy(table)
        tmp_table.key = tmp_key
        tmp_table.path = make_tmp_name(table.path)
        tmp_table.name = make_tmp_name(table.name)
        if exists is not None:
            tmp_table.exists = exists
        tmp_table.is_temporary = True
        tmp_table.tablet_state.set(YtTabletState.UNMOUNTED)
        return self.add_table(tmp_table)

    def add_consumer(self, consumer: YtNativeConsumer) -> YtNativeConsumer:
        # TODO: tests!
        assert consumer.table.key not in self.consumers, f"Duplicate consumer '{consumer.table.key}'"

        self.consumers[consumer.table.key] = consumer
        return consumer

    def add_node(self, node: YtNode) -> YtNode:
        assert not self.find_table_by_path(node.path), f"Table at path '{node.path}' already exists"
        assert not self.find_node_by_path(node.path), f"Duplicated node at '{node.path}'"

        self._check_path_clearance(node)
        self.nodes[node.path] = node
        return node

    def _is_possible_folder(self, node: YtNode) -> bool:
        return node.node_type in [YtNode.Type.FOLDER, YtNode.Type.ANY]

    def _check_path_clearance(self, item: YtTable | YtNode):
        if isinstance(item, YtNode) and item.path in self._parent_folders:
            assert self._is_possible_folder(item), f"Node at '{item.rich_path}' must be folder"

        folder = get_folder(item.path)
        assert folder, f"Node at {item.path} is root"
        while folder and folder not in self._parent_folders:
            table = self.find_table_by_path(folder)
            assert not table, f"Node at '{table.rich_path}' is a table and is an ancestor of '{item.rich_path}'"
            node = self.find_node_by_path(folder)
            assert node is None or self._is_possible_folder(
                node
            ), f"Node at '{node.rich_path}' is not folder and is an ancestor of '{item.rich_path}'"

            self._parent_folders.add(folder)
            folder = get_folder(folder)

        if isinstance(item, YtNode) and item.node_type == YtNode.Type.LINK:
            existing_link = self.nodes_by_link_target.get(item.path, None)
            assert (
                existing_link is None
            ), f"Link at '{existing_link.rich_path}' is linked to another link at '{item.rich_path}'"
            existing_node = self.find_node_by_path(item.link_target_path)
            assert (
                existing_node is None or existing_node.node_type != YtNode.Type.LINK
            ), f"Link at '{item.rich_path}' is linked to another link at '{existing_node.rich_path}'"
            self.nodes_by_link_target[item.link_target_path] = item

    def add_replication_log_for(
        self, table: YtTable, explicit_replication_log: YtTable | None = None
    ) -> YtTable | None:
        if not self.is_chaos or not table.is_chaos_replication_log_required:
            return None

        if explicit_replication_log is None:
            # TODO: remove with DesiredStateBuilderDeprecated
            log_attributes = YtTable.resolve_replication_log_attributes(table.attributes)
            log_attributes["schema"] = table.schema.yt_schema
            replication_log_table = self.add_table_from_attributes(
                make_log_name(table.key),
                YtTable.Type.REPLICATION_LOG,
                make_log_name(table.path),
                table.exists,
                log_attributes,
            )
            replication_log_table.rtt_options.enabled = table.rtt_options.enabled
        else:
            replication_log_table = self.add_table(explicit_replication_log)

        table.link_replication_log(replication_log_table)
        return replication_log_table

    def get_replication_log_for(self, table: YtTable) -> YtTable | None:
        if not table.chaos_replication_log:
            return None
        return self.tables.get(table.chaos_replication_log, None)

    def find_table_by_path(self, path: str) -> YtTable | None:
        return self.tables_by_path.get(path, None)

    def find_node_by_path(self, path: str) -> YtNode | None:
        return self.nodes.get(path, None)

    @property
    def is_mountable(self):
        return not self.is_main or not self.is_chaos

    def dump_stat(self) -> str:
        return (
            "{"
            + self.name
            + f", type={str(self.cluster_type)}, mode={str(self.mode)}, table_count={len(self.tables)}"
            + "}"
        )

    def _fix_parent_folders(self):
        for item in list(self.tables.values()) + [
            node for node in self.nodes.values() if node.node_type != YtNode.Type.FOLDER
        ]:
            if item.folder == "/":
                continue

            if item.folder in self.nodes:
                # Should never happen but nevertherless.
                node = self.nodes[item.folder]
                assert self._is_possible_folder(
                    node
                ), f"Node at '{node.rich_path}' is not a folder but is a parent of '{item.rich_path}'"
                continue

            self.add_node(
                YtNode.make(
                    cluster_name=self.name,
                    path=item.folder,
                    node_type=YtNode.Type.FOLDER,
                    exists=True,
                    attributes={},
                    is_implicit=True,
                )
            )

    def _add_folders_upto_lca(self):
        paths: list[str] = [node.path for node in self.nodes.values()]
        lca_path: str = get_list_lca(paths)
        assert lca_path != "/", f"LCA path cannot be root, split your requests. Paths: {paths}, nodes: {self.nodes}"
        if lca_path not in self.nodes:
            self.add_node(
                YtNode.make(
                    cluster_name=self.name,
                    path=lca_path,
                    node_type=YtNode.Type.FOLDER,
                    exists=True,
                    attributes={},
                    is_implicit=True,
                )
            )
        for node in self.nodes_sorted:
            if node.node_type != YtNode.Type.FOLDER or node.path == lca_path:
                continue
            path = get_folder(node.path)
            while path != "/" and path != lca_path and path not in self.nodes:
                self.add_node(
                    YtNode.make(
                        cluster_name=self.name,
                        path=path,
                        node_type=YtNode.Type.FOLDER,
                        exists=True,
                        attributes={},
                        is_implicit=True,
                    )
                )
                path = get_folder(path)

    def _fix_folders(self):
        self._fix_parent_folders()
        self._add_folders_upto_lca()

    def _propagate_attributes(self):
        def _is_propagated(source: YtNode, attribute_name: str) -> bool:
            return source.propagated_attributes is None or attribute_name in source.propagated_attributes

        propagation_items: list[YtTable | YtNode] = [table for table in self.tables.values()] + [
            node for node in self.nodes.values()
        ]
        for item in sorted(propagation_items, key=lambda x: x.path):
            ancestor_path = item.folder
            while ancestor_path != "/" and ancestor_path not in self.nodes:
                ancestor_path = get_folder(ancestor_path)
            if ancestor_path == "/":
                continue

            ancestor_node = self.nodes[ancestor_path]
            if not item.attributes:
                for attribute, value in ancestor_node.attributes.attributes.items():
                    if not _is_propagated(ancestor_node, attribute):
                        continue

                    item.attributes[attribute] = deepcopy(value)
                    item.attributes.propagated_attributes[attribute] = (
                        ancestor_node.attributes.propagated_attributes.get(attribute, ancestor_path)
                    )
                continue

            for attribute, propagated_value, value in iter_attributes_recursively(
                ancestor_node.attributes.attributes, item.attributes.attributes
            ):
                if not _is_propagated(ancestor_node, attribute):
                    continue
                if item.attributes.has_value(attribute):
                    continue

                item.attributes[attribute] = deepcopy(propagated_value)
                item.attributes.propagated_attributes[attribute] = ancestor_node.attributes.propagated_attributes.get(
                    attribute, ancestor_path
                )

    def _remove_implicit_ancestors_of_any(self):
        for node in self.nodes_sorted:
            if node.node_type != YtNode.Type.ANY:
                continue

            path = node.folder
            while path in self.nodes:
                node = self.nodes[path]
                if node.is_implicit and not node.attributes:
                    del self.nodes[path]
                path = get_folder(path)

    def finalize(self):
        self._fix_folders()
        self._propagate_attributes()
        self._remove_implicit_ancestors_of_any()
        self._parent_folders.clear()
