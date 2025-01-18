from __future__ import annotations

from dataclasses import asdict
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from dataclasses import fields as dataclass_fields
from logging import ERROR
from logging import Logger
from logging import WARNING
from typing import Generator

from yt.yt_sync.core.model import Types
from yt.yt_sync.core.model import YtColumn
from yt.yt_sync.core.model import YtSchema
from yt.yt_sync.core.model import YtTable
from yt.yt_sync.core.settings import Settings

from .base import TableDiffBase
from .base import TableDiffType


@dataclass
class SchemaChange(TableDiffBase):
    settings: Settings
    # (cluster, path) -> (desired, actual)
    schema_changes: dict[Types.ReplicaKey, tuple[YtSchema, YtSchema]] = dataclass_field(default_factory=dict)

    def require_existing_main(self) -> bool:
        return True

    def add_change_if_any(self, desired_table: YtTable, actual_table: YtTable):
        assert desired_table.cluster_name == actual_table.cluster_name, "Tables should be from same cluster"
        assert desired_table.path == actual_table.path, "Tables should be with same path"

        if not actual_table.exists:
            return

        if actual_table.schema != desired_table.schema:
            self.schema_changes[desired_table.replica_key] = (desired_table.schema, actual_table.schema)

    def is_empty(self) -> bool:
        return len(self.schema_changes) == 0

    def check_and_log(self, log: Logger) -> bool:
        result = True
        for replica_key in self.schema_changes:
            cluster, path = replica_key
            desired_schema, actual_schema = self.schema_changes[replica_key]
            log.warning("  schema diff for %s:%s:", cluster, path)

            if actual_schema.is_data_modification_required(desired_schema) and actual_schema.is_ordered:
                # Schema change with data transformation for ordered tables (queues) is very rare
                # and leads to data loss because table should be recreated. Consider ordered tables removal as
                # manual operation for now.
                # In the future empty tables can be recreated automatially with ensure scenario: table emptiness check
                # should be implemented.
                log.error(
                    "    data modification (MR) for ordered tables not supported, consider remove it manually to be "
                    + "recreated in the next run of ensure"
                )
                self._log_changes(desired_schema, actual_schema, log, ERROR)
                result = False

            if (
                self.settings.is_chaos
                and actual_schema.is_ordered
                and actual_schema.is_column_set_changed(desired_schema)
            ):
                log.error("    chaos ordered tables schema cannot be changed, consider recreating them")
                self._log_changes(desired_schema, actual_schema, log, ERROR)
                result = False

            if result and actual_schema.is_compatible(desired_schema, self.settings.allow_table_full_downtime):
                self._log_changes(desired_schema, actual_schema, log, WARNING)
                if actual_schema.is_change_with_downtime(desired_schema):
                    log.log(WARNING, "    TABLE FULL DOWNTIME REQUIRED!")
            else:
                log.error("    incompatible changes found!")
                self._log_changes(desired_schema, actual_schema, log, ERROR)
                result = False

        return result

    def has_diff_for(self, cluster_name: str) -> bool:
        return not self.is_empty() and self._contains_cluster_in_key(set(self.schema_changes.keys()), cluster_name)

    def is_data_modification_required(self, cluster_name: str | None) -> bool:
        for _, desired, actual in self._get_changes_for(cluster_name):
            if actual.is_data_modification_required(desired):
                return True
        return False

    def is_key_changed(self, cluster_name) -> bool:
        for _, desired, actual in self._get_changes_for(cluster_name):
            if actual.is_key_changed(desired):
                return True
        return False

    def is_unmount_required(self, cluster_name: str) -> bool:
        return self.has_diff_for(cluster_name)

    def is_change_with_downtime(self) -> bool:
        for desired, actual in self.schema_changes.values():
            if actual.is_change_with_downtime(desired):
                return True
        return False

    def _get_changes_for(
        self, cluster_name: str | None
    ) -> Generator[tuple[Types.ReplicaKey, YtSchema, YtSchema], None, None]:
        for key in self.schema_changes:
            key_cluster, _ = key
            if cluster_name and key_cluster != cluster_name:
                continue
            desired, actual = self.schema_changes[key]
            yield key, desired, actual

    @classmethod
    def _log_column_change(cls, desired: YtColumn, actual: YtColumn, log: Logger, log_level: int):
        attribute_names = [field.name for field in dataclass_fields(YtColumn)]
        desired_attributes = asdict(desired)
        actual_attributes = asdict(actual)
        for attribute in attribute_names:
            actual_attribute = actual_attributes[attribute]
            desired_attribute = desired_attributes[attribute]
            if "expression" == attribute:
                actual_attribute = actual.normalized_expression
                desired_attribute = desired.normalized_expression
            if actual_attribute != desired_attribute:
                log.log(
                    log_level,
                    "    column [%s].%s changed: actual=%s, desired=%s",
                    desired.name,
                    attribute,
                    actual_attribute,
                    desired_attribute,
                )

    @classmethod
    def _log_changes(cls, desired: YtSchema, actual: YtSchema, log: Logger, log_level: int):
        for actual_column, desired_column in actual.get_changed_columns(desired):
            cls._log_column_change(desired_column, actual_column, log, log_level)

        has_key_changes = False
        for column in actual.get_deleted_columns(desired):
            has_key_changes |= column.is_key_column
            log.log(log_level, "    delete column [%s]", column.name)
        for column in actual.get_added_columns(desired):
            has_key_changes |= column.is_key_column
            log.log(log_level, "    add column [%s]", column.name)

        if not has_key_changes:
            # check if has key columns order change
            actual_columns = [c.name for c in actual.columns if c.is_key_column]
            desired_columns = [c.name for c in desired.columns if c.is_key_column]
            if actual_columns != desired_columns:
                log.log(
                    log_level,
                    "    key columns order changed: actual = %s, desired = %s",
                    actual_columns,
                    desired_columns,
                )

    @classmethod
    def make(cls, settings: Settings) -> SchemaChange:
        return cls(diff_type=TableDiffType.SCHEMA_CHANGE, settings=settings)
