from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field as dataclass_field
from logging import Logger
from typing import Generator

from yt.yt_sync.core.model import Types
from yt.yt_sync.core.model import YtTable

from .base import TableDiffBase
from .base import TableDiffType


@dataclass
class OrphanedTable(TableDiffBase):
    main_table: YtTable
    orphaned_tables: dict[Types.ReplicaKey, YtTable] = dataclass_field(default_factory=dict)
    has_missing_data_replicas: bool = False
    missing_replication_logs: dict[Types.ReplicaKey, YtTable] = dataclass_field(default_factory=dict)

    def require_existing_main(self) -> bool:
        return False

    def is_empty(self) -> bool:
        if not self.main_table.is_replicated:
            return True
        return len(self.orphaned_tables) == 0

    def add_change_if_any(self, desired_table: YtTable, actual_table: YtTable):
        table_key = actual_table.replica_key
        if actual_table.exists:
            self.orphaned_tables[table_key] = actual_table
        elif YtTable.Type.TABLE == actual_table.table_type:
            self.has_missing_data_replicas = True
        elif YtTable.Type.REPLICATION_LOG == desired_table.table_type:
            self.missing_replication_logs[table_key] = desired_table

    def check_and_log(self, log: Logger) -> bool:
        if not self.main_table.is_replicated or self.is_empty():
            return True
        if self.has_missing_data_replicas:
            log.error("  orphaned table federation %s has missing data replicas", self.main_table.name)
            return False
        result = True
        for replica in self.orphaned_tables.values():
            if YtTable.Type.TABLE == replica.table_type:
                log.info("  check orphaned data replica schema for %s:%s", replica.cluster_name, replica.path)
                if not replica.schema.is_compatible(self.main_table.schema):
                    log.error("    schema is not compatible!")
                    result = False
                else:
                    log.info("    ok!")
        for replication_log in self.missing_replication_logs.values():
            log.warning("  missing replication_log %s:%s", replication_log.cluster_name, replication_log.path)
        return result

    def has_diff_for(self, cluster_name: str) -> bool:
        return not self.is_empty() and (
            self._contains_cluster_in_key(set(self.orphaned_tables.keys()), cluster_name)
            or self.main_table.cluster_name == cluster_name
        )

    def is_unmount_required(self, cluster_name: str) -> bool:
        return self._contains_cluster_in_key(self.orphaned_tables.keys(), cluster_name)

    def _yield_tables_for(
        self, cluster_name: str, tables_container: dict[Types.ReplicaKey, YtTable]
    ) -> Generator[YtTable, None, None]:
        for key, table in tables_container.items():
            cluster, _ = key
            if cluster == cluster_name:
                yield table

    def orphaned_tables_for(self, cluster_name: str) -> Generator[YtTable, None, None]:
        yield from self._yield_tables_for(cluster_name, self.orphaned_tables)

    def replication_logs_for(self, cluster_name: str) -> Generator[YtTable, None, None]:
        yield from self._yield_tables_for(cluster_name, self.missing_replication_logs)

    @classmethod
    def make(cls, main_table: YtTable) -> OrphanedTable:
        return cls(diff_type=TableDiffType.ORPHANED_TABLE, main_table=main_table)
