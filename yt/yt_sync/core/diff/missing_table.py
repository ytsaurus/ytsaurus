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
class MissingTable(TableDiffBase):
    main_table: YtTable
    missing: dict[Types.ReplicaKey, YtTable] = dataclass_field(default_factory=dict)
    orphaned: dict[Types.ReplicaKey, YtTable] = dataclass_field(default_factory=dict)
    skip_orphaned_empty_check: bool = False

    def require_existing_main(self) -> bool:
        return False

    def add_change_if_any(self, desired_table: YtTable, actual_table: YtTable):
        if actual_table.exists:
            self.orphaned[actual_table.replica_key] = actual_table
        else:
            self.missing[desired_table.replica_key] = desired_table

    def is_empty(self) -> bool:
        if self.skip_orphaned_empty_check:
            return not bool(self.missing)
        return not bool(self.missing) or bool(self.orphaned)

    def check_and_log(self, log: Logger) -> bool:
        log.warning("  table federation for %s is missing:", self.main_table.name)
        for table in self.missing.values():
            log.warning(
                "    missing %s %s:%s with spec %s",
                table.table_type,
                table.cluster_name,
                table.path,
                table.yt_attributes,
            )
        result: bool = True

        for table in self.orphaned.values():
            if table.is_replicated:
                log.warning("    existing orphaned %s will be removed: %s", table.table_type, table.rich_path)
            else:
                log.error(
                    "    orphaned %s %s exists, can't create table federation!", table.table_type, table.rich_path
                )
                result = False
        return result

    def has_diff_for(self, cluster_name: str) -> bool:
        return not self.is_empty() and self._contains_cluster_in_key(set(self.missing.keys()), cluster_name)

    def missing_tables_for(self, cluster_name: str) -> Generator[YtTable, None, None]:
        for key, table in self.missing.items():
            cluster, _ = key
            if cluster == cluster_name:
                yield table

    @classmethod
    def make(cls, main_table: YtTable) -> MissingTable:
        return cls(diff_type=TableDiffType.MISSING_TABLE, main_table=main_table)
