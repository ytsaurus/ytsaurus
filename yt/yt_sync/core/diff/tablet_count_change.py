from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field as dataclass_field
from logging import Logger
from typing import Generator

from yt.yt_sync.core.model import Types
from yt.yt_sync.core.model import YtTable
from yt.yt_sync.core.model import YtTabletInfo

from .base import TableDiffBase
from .base import TableDiffType


@dataclass
class TabletCountChange(TableDiffBase):
    main_table: YtTable

    # replica_key: (desired, actual)
    tablet_count_changes: dict[Types.ReplicaKey, tuple[YtTable, YtTable]] = dataclass_field(default_factory=dict)

    @classmethod
    def make(cls, main_table: YtTable) -> TabletCountChange:
        return cls(diff_type=TableDiffType.TABLET_COUNT_CHANGE, main_table=main_table)

    def require_existing_main(self) -> bool:
        return True

    def is_empty(self) -> bool:
        return len(self.tablet_count_changes) == 0

    def add_change_if_any(self, desired_table: YtTable, actual_table: YtTable):
        if not actual_table.exists:
            return
        if desired_table.table_type == YtTable.Type.CHAOS_REPLICATED_TABLE:
            return  # no tablets for CRT tables
        if actual_table.tablet_info.is_resharding_required(desired_table.tablet_info):
            self.tablet_count_changes[actual_table.replica_key] = (
                desired_table,
                actual_table,
            )

    def check_and_log(self, log: Logger) -> bool:
        result = True
        is_queue = self.main_table.is_ordered
        for replica_key in self.tablet_count_changes:
            cluster, path = replica_key
            desired_table, actual_table = self.tablet_count_changes[replica_key]
            desired: YtTabletInfo = desired_table.tablet_info
            actual: YtTabletInfo = actual_table.tablet_info
            if is_queue:
                if desired.has_pivot_keys:
                    log.error("  can't change %s:%s/@pivot_keys because it's ordered", cluster, path)
                    result = False
                elif desired.tablet_count < actual.tablet_count:
                    log.error(
                        "  reducing %s:%s/@tablet_count not allowed: actual=%s, desired=%s",
                        cluster,
                        path,
                        actual.tablet_count,
                        desired.tablet_count,
                    )
                    result = False
            if result:
                if desired.has_pivot_keys and desired.pivot_keys != actual.pivot_keys:
                    log.warning(
                        "  %s:%s/@pivot_keys changed: actual={count=%s}%s, desired={count=%s}%s",
                        cluster,
                        path,
                        len(actual.pivot_keys),
                        actual.pivot_keys,
                        len(desired.pivot_keys),
                        desired.pivot_keys,
                    )
                elif desired.tablet_count != actual.tablet_count:
                    log.warning(
                        "  %s:%s/@tablet_count changed: actual=%s, desired=%s",
                        cluster,
                        path,
                        actual.tablet_count,
                        desired.tablet_count,
                    )

        return result

    def get_diff_for(self, cluster_name: str) -> Generator[tuple[YtTable, YtTable], None, None]:
        for replica_key, (desired, actual) in self.tablet_count_changes.items():
            cluster, _ = replica_key
            if cluster == cluster_name:
                yield desired, actual

    def has_diff_for(self, cluster_name: str) -> bool:
        return not self.is_empty() and self._contains_cluster_in_key(self.tablet_count_changes.keys(), cluster_name)

    def is_unmount_required(self, cluster_name: str) -> bool:
        return self.has_diff_for(cluster_name)
