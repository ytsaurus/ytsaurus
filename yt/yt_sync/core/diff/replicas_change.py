from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field as dataclass_field
from logging import Logger
from typing import Generator

from yt.yt_sync.core.model import Types
from yt.yt_sync.core.model import YtReplica
from yt.yt_sync.core.model import YtTable

from .base import TableDiffBase
from .base import TableDiffType


@dataclass
class ReplicasChange(TableDiffBase):
    desired_main_table: YtTable
    actual_main_table: YtTable
    missing_replicas: dict[Types.ReplicaKey, YtTable] = dataclass_field(default_factory=dict)
    alive_replicas: dict[Types.ReplicaKey, YtTable] = dataclass_field(default_factory=dict)

    def require_existing_main(self) -> bool:
        return True

    def add_change_if_any(self, desired_table: YtTable, actual_table: YtTable):
        if desired_table.rich_path == self.desired_main_table.rich_path:
            return
        key = actual_table.replica_key
        if actual_table.exists:
            self.alive_replicas[key] = actual_table
        else:
            self.missing_replicas[key] = actual_table

    def is_empty(self) -> bool:
        if not self.desired_main_table.is_replicated:
            return True
        return not self.has_missing_replicas() and not self.has_deleted_replicas()

    def check_and_log(self, log: Logger) -> bool:
        if not self.desired_main_table.is_replicated or self.is_empty():
            return True

        if not self._has_alive_data_replicas():
            log.error(
                "  no data replicas for %s %s:%s",
                self.desired_main_table.table_type,
                self.desired_main_table.cluster_name,
                self.desired_main_table.path,
            )
            return False

        result = True
        for replica in self.alive_replicas.values():
            if YtTable.Type.TABLE == replica.table_type:
                log.info("  check alive data replica schema for %s:%s", replica.cluster_name, replica.path)
                if not replica.schema.is_compatible(self.desired_main_table.schema):
                    log.error("    schema is NOT COMPATIBLE, run 'dump_diff' scenario for details")
                    result = False
                else:
                    log.info("    ok!")

        for replica in self.missing_replicas.values():
            log.warning("  has missing replica %s:%s", replica.cluster_name, replica.path)

        for replica in self.deleted_replicas():
            log.warning("  has deleted replica %s:%s", replica.cluster_name, replica.replica_path)

        return result

    def has_diff_for(self, cluster_name: str) -> bool:
        return self.has_missing_replicas(cluster_name) or self.has_deleted_replicas(cluster_name)

    def _has_alive_data_replicas(self):
        for table in self.alive_replicas.values():
            if YtTable.Type.TABLE == table.table_type:
                return True
        return False

    def has_missing_replicas(self, cluster_name: str | None = None) -> bool:
        if cluster_name:
            return self._contains_cluster_in_key(set(self.missing_replicas.keys()), cluster_name)
        else:
            return len(self.missing_replicas) > 0

    def missing_replicas_for(self, cluster_name: str) -> Generator[YtTable, None, None]:
        for table in self.missing_replicas.values():
            if table.cluster_name == cluster_name:
                yield table

    def deleted_replicas(self, cluster_name: str | None = None) -> Generator[YtReplica, None, None]:
        for replica_key, replica in self.actual_main_table.replicas.items():
            if replica_key not in self.desired_main_table.replicas:
                if cluster_name and cluster_name != replica.cluster_name:
                    continue
                yield replica

    def has_deleted_replicas(self, cluster_name: str | None = None) -> bool:
        return next(self.deleted_replicas(cluster_name), None) is not None

    @property
    def has_master_only(self):
        return self.missing_replicas and not self.alive_replicas

    @classmethod
    def make(cls, desired_main_table: YtTable, actual_main_table: YtTable) -> ReplicasChange:
        return cls(
            diff_type=TableDiffType.REPLICAS_CHANGE,
            desired_main_table=desired_main_table,
            actual_main_table=actual_main_table,
        )
