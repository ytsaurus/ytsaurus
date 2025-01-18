from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field as dataclass_field
from typing import Generator

from yt.yt_sync.core.helpers import get_dummy_logger
from yt.yt_sync.core.model import Types
from yt.yt_sync.core.model import YtDatabase
from yt.yt_sync.core.model import YtTable
from yt.yt_sync.core.settings import Settings

from .base import NodeDiffBase
from .base import TableDiffBase
from .base import TableDiffSet
from .base import TableDiffType
from .missing_node import MissingNode
from .missing_table import MissingTable
from .node_attributes_change import NodeAttributesChange
from .orphaned_table import OrphanedTable
from .replicas_change import ReplicasChange
from .schema_change import SchemaChange
from .table_attributes_change import TableAttributesChange
from .tablet_count_change import TabletCountChange


@dataclass
class DbDiff:
    nodes_diff: dict[Types.ReplicaKey, list[NodeDiffBase]] = dataclass_field(default_factory=dict)

    # table_key -> list[diffs], consider table_path on all clusters is same
    tables_diff: dict[str, list[TableDiffBase]] = dataclass_field(default_factory=dict)

    def is_empty(self) -> bool:
        return len(self.tables_diff) == 0 and len(self.nodes_diff) == 0

    def is_valid(self, diff_types: set[int] | None = None) -> bool:
        null_logger = get_dummy_logger()

        for node_diff_list in self.nodes_diff.values():
            for node_diff in node_diff_list:
                if diff_types and node_diff.diff_type not in diff_types:
                    continue
                if not node_diff.check_and_log(null_logger):
                    return False

        for _, table_diff in self._table_diffs(diff_types):
            if not table_diff.check_and_log(null_logger):
                return False

        return True

    def node_diff_for(
        self, cluster_name: str | None, diff_types: set[int] | None = None
    ) -> Generator[NodeDiffBase, None, None]:
        for (cluster_key, path), diff_list in self.nodes_diff.items():
            if cluster_name and cluster_name != cluster_key:
                continue
            for diff in diff_list:
                if diff_types and diff.diff_type not in diff_types:
                    continue
                if not diff.is_empty():
                    yield diff

    def table_diff_for(
        self, cluster_name: str | None, diff_types: set[int] | None = None
    ) -> Generator[TableDiffSet, None, None]:
        for table_key in sorted(self.tables_diff):
            diff_set = TableDiffSet(table_key=table_key)
            for table_diff in self.tables_diff[table_key]:
                if cluster_name and not table_diff.has_diff_for(cluster_name):
                    continue
                if diff_types and table_diff.diff_type not in diff_types:
                    continue
                diff_set.diffs.append(table_diff)
            if diff_set.diffs:
                yield diff_set

    def has_diff_for(self, cluster_name: str, diff_types: set[int] | None = None) -> bool:
        return (
            next(self.node_diff_for(cluster_name), None) is not None
            or next(self.table_diff_for(cluster_name, diff_types), None) is not None
        )

    def is_unmount_required(self, cluster_name: str, diff_types: set[int] | None = None) -> bool:
        for _, table_diff in self._table_diffs(diff_types):
            if table_diff.is_unmount_required(cluster_name):
                return True
        return False

    def is_data_modification_required(self, cluster_name: str | None = None) -> bool:
        for _, table_diff in self._table_diffs():
            if TableDiffType.SCHEMA_CHANGE == table_diff.diff_type:
                assert isinstance(table_diff, SchemaChange)
                if table_diff.is_data_modification_required(cluster_name):
                    return True
        return False

    def schema_diff_for(self, cluster_name: str | None = None) -> Generator[TableDiffSet, None, None]:
        yield from self.table_diff_for(cluster_name=cluster_name, diff_types={TableDiffType.SCHEMA_CHANGE})

    def has_schema_changes(self) -> bool:
        return next(self.schema_diff_for(), None) is not None

    @classmethod
    def generate(cls, settings: Settings, desired: YtDatabase, actual: YtDatabase, nodes_only: bool = False) -> DbDiff:
        result = DbDiff()
        if not desired.clusters:
            return result

        for cluster in desired.clusters.values():
            for desired_node in cluster.nodes.values():
                assert cluster.name in actual.clusters
                assert desired_node.path in actual.clusters[cluster.name].nodes
                diffs: list[NodeDiffBase] = [
                    NodeAttributesChange.make(desired_node, actual.clusters[cluster.name].nodes[desired_node.path]),
                    MissingNode.make(desired_node, actual.clusters[cluster.name].nodes[desired_node.path]),
                ]

                for diff in diffs:
                    if not diff.is_empty():
                        result.nodes_diff.setdefault((cluster.name, desired_node.path), list()).append(diff)
        if nodes_only:
            return result

        for table_key in desired.main.tables:
            assert table_key in actual.main.tables
            actual_main_table = actual.main.tables[table_key]
            main_table_exists = actual_main_table.exists
            desired_main_table = desired.main.tables[table_key]

            differs: list[TableDiffBase] = [
                MissingTable.make(desired_main_table),
                OrphanedTable.make(desired_main_table),
                ReplicasChange.make(desired_main_table, actual_main_table),
                SchemaChange.make(settings),
                TableAttributesChange.make(desired_main_table),
                TabletCountChange.make(desired_main_table),
            ]

            for cluster in desired.clusters.values():
                if table_key not in cluster.tables:
                    continue
                desired_table = cluster.tables[table_key]
                actual_table = cls._get_actual_table(table_key, cluster.name, actual)

                cls._apply_differs(differs, desired_table, actual_table, main_table_exists)

                if desired_table.chaos_replication_log:
                    desired_replication_log = cluster.tables[desired_table.chaos_replication_log]
                    actual_replication_log = cls._get_actual_table(desired_replication_log.key, cluster.name, actual)
                    cls._apply_differs(differs, desired_replication_log, actual_replication_log, main_table_exists)

            for differ in differs:
                result._add_diff(table_key, differ)

        return cls._postprocess(result)

    @staticmethod
    def _postprocess(src: DbDiff) -> DbDiff:
        dst = DbDiff()
        dst.nodes_diff = src.nodes_diff
        for key, diffs in src.tables_diff.items():
            for diff in diffs:
                if diff.diff_type == TableDiffType.REPLICAS_CHANGE and diff.has_master_only:
                    assert isinstance(diff, ReplicasChange)
                    new_diff: MissingTable = MissingTable.make(diff.desired_main_table)
                    for table in diff.missing_replicas.values():
                        new_diff.missing[table.replica_key] = table
                    new_diff.missing[diff.actual_main_table.replica_key] = diff.actual_main_table
                    new_diff.orphaned[diff.actual_main_table.replica_key] = diff.actual_main_table
                    new_diff.skip_orphaned_empty_check = True
                    dst._add_diff(key, new_diff)
                else:
                    dst._add_diff(key, diff)
        return dst

    def _add_diff(self, table_key: str, diff: TableDiffBase):
        if diff.is_empty():
            return
        self.tables_diff.setdefault(table_key, list()).append(diff)

    def _table_diffs(self, diff_types: set[int] | None = None) -> Generator[tuple[str, TableDiffBase], None, None]:
        for table_key in sorted(self.tables_diff):
            table_diffs = self.tables_diff[table_key]
            for table_diff in table_diffs:
                if diff_types and table_diff.diff_type not in diff_types:
                    continue
                yield table_key, table_diff

    @staticmethod
    def _apply_differs(differs: list[TableDiffBase], desired_table: YtTable, actual_table: YtTable, main_exists: bool):
        for differ in differs:
            if main_exists == differ.require_existing_main():
                differ.add_change_if_any(desired_table, actual_table)

    @classmethod
    def _get_actual_table(cls, table_key: str, cluster_name: str, actual_db: YtDatabase) -> YtTable:
        assert cluster_name in actual_db.clusters, f"Bad actual DB state: missing cluster {cluster_name}"
        actual_cluster = actual_db.clusters[cluster_name]
        assert table_key in actual_cluster.tables, f"Bad actual DB state: missing table {table_key}"
        return actual_cluster.tables[table_key]
