from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field as dataclass_field
from logging import Logger
from typing import Any
from typing import Callable
from typing import Generator

from yt.yt_sync.core.model import RttOptions
from yt.yt_sync.core.model import Types
from yt.yt_sync.core.model import YtReplica
from yt.yt_sync.core.model import YtTable
from yt.yt_sync.core.model import YtTableAttributes
from yt.yt_sync.core.model import YtTabletState

from .base import TableDiffBase
from .base import TableDiffType


@dataclass
class TableAttributesChange(TableDiffBase):
    @dataclass
    class ChangeItem:
        type_changes: tuple[str, str] | None = None
        replica_changes: list[tuple[YtReplica, YtReplica]] | None = None
        attribute_changes: tuple[YtTableAttributes, YtTableAttributes] | None = None
        missing_user_attributes: dict[str, Any] | None = None
        tablet_state_changes: tuple[YtTabletState, YtTabletState] | None = None
        rtt_options_changes: tuple[RttOptions, RttOptions] | None = None

    main_table: YtTable
    changes: dict[Types.ReplicaKey, ChangeItem] = dataclass_field(default_factory=dict)

    def require_existing_main(self) -> bool:
        return True

    def add_change_if_any(self, desired_table: YtTable, actual_table: YtTable):
        assert desired_table.cluster_name == actual_table.cluster_name, "Tables should be from same cluster"
        assert desired_table.path == actual_table.path, "Tables should be with same path"

        if not actual_table.exists:
            return

        if desired_table.table_type != actual_table.table_type:
            self._get_item(desired_table).type_changes = (desired_table.table_type, actual_table.table_type)

        if desired_table.is_replicated and actual_table.is_replicated:
            for key, desired_replica in desired_table.replicas.items():
                actual_replica = actual_table.replicas.get(key, None)
                if actual_replica and not desired_replica.compare(actual_replica, not actual_table.is_rtt_enabled):
                    change = self.changes.setdefault(self.main_table.replica_key, self.ChangeItem())
                    if change.replica_changes is None:
                        change.replica_changes = list()
                    change.replica_changes.append((desired_replica, actual_replica))
            if desired_table.rtt_options.has_diff_with(actual_table.rtt_options):
                self._get_item(desired_table).rtt_options_changes = (
                    desired_table.rtt_options,
                    actual_table.rtt_options,
                )

        if desired_table.attributes.has_diff_with(actual_table.attributes):
            self._get_item(desired_table).attribute_changes = (desired_table.attributes, actual_table.attributes)

        if actual_table.attributes.missing_user_attributes:
            change = self._get_item(desired_table)
            change.missing_user_attributes = dict()
            for attr in actual_table.attributes.missing_user_attributes:
                change.missing_user_attributes[attr] = actual_table.attributes.get(attr, None)

        if desired_table.is_mountable and desired_table.tablet_state != actual_table.tablet_state:
            self._get_item(desired_table).tablet_state_changes = (desired_table.tablet_state, actual_table.tablet_state)

    def is_empty(self):
        return len(self.changes) == 0

    def check_and_log(self, log: Logger) -> bool:
        result = True
        for replica_key in self.changes:
            cluster, path = replica_key
            change = self.changes[replica_key]
            if change.type_changes:
                result = False
                desired, actual = change.type_changes
                log.error("  %s:%s type mismatch: actual=%s, desired=%s", cluster, path, actual, desired)

            if change.attribute_changes:
                desired, actual = change.attribute_changes
                for attribute_path, desired_value, actual_value in desired.changed_attributes(actual):
                    self._log_attr_change(
                        log,
                        cluster,
                        path,
                        attribute_path,
                        desired_value,
                        actual_value,
                        attribute_path in desired.propagated_attributes,
                    )

            if change.rtt_options_changes:
                desired, actual = change.rtt_options_changes
                for attribute_path, desired_value, actual_value in desired.changed_attributes(actual):
                    self._log_attr_change(
                        log,
                        cluster,
                        path,
                        attribute_path,
                        desired_value,
                        actual_value,
                    )

            if change.missing_user_attributes:
                for attr, value in change.missing_user_attributes.items():
                    log.warning("  found missing attribute %s:%s/@%s: value=%s", cluster, path, attr, value)

            if change.replica_changes:
                for desired, actual in change.replica_changes:
                    log.warning(
                        "  found replica #%s (%s:%s) attributes mismatch:",
                        actual.replica_id,
                        desired.cluster_name,
                        desired.replica_path,
                    )
                    for attribute_path, desired_value, actual_value in desired.changed_attributes(actual):
                        if desired_value is None:
                            log.warning("    remove attribute '%s': actual=%s", attribute_path, actual_value)
                        else:
                            log.warning(
                                "    update_attribute '%s': actual=%s, desired=%s",
                                attribute_path,
                                actual_value,
                                desired_value,
                            )
            if change.tablet_state_changes:
                desired, actual = change.tablet_state_changes
                log.warning(
                    "  found %s:%s/@tablet_state mismatch: actual=%s, desired=%s",
                    cluster,
                    path,
                    actual.state,
                    desired.state,
                )
        return result

    def has_diff_for(self, cluster_name: str) -> bool:
        return not self.is_empty() and self._contains_cluster_in_key(set(self.changes.keys()), cluster_name)

    def is_unmount_required(self, cluster_name: str) -> bool:
        for change in self.attribute_changes_for(cluster_name):
            assert change.attribute_changes
            desired, actual = change.attribute_changes
            if desired.is_unmount_required(actual):
                return True
        return False

    def is_remount_required(self, cluster_name: str) -> bool:
        for change in self.attribute_changes_for(cluster_name):
            assert change.attribute_changes
            desired, actual = change.attribute_changes
            if desired.is_remount_required(actual):
                return True
        return False

    def attribute_changes_for(self, cluster_name: str) -> Generator[ChangeItem, None, None]:
        yield from self._get_changes_for(cluster_name, lambda change: change.attribute_changes is not None)

    def has_replica_changes(self) -> bool:
        for change in self.changes.values():
            if change.replica_changes:
                return True
        return False

    def tablet_state_changes_for(self, cluster_name: str) -> Generator[ChangeItem, None, None]:
        yield from self._get_changes_for(cluster_name, lambda change: change.tablet_state_changes is not None)

    @classmethod
    def make(cls, main_table: YtTable) -> TableAttributesChange:
        return cls(diff_type=TableDiffType.ATTRIBUTES_CHANGE, main_table=main_table)

    def _get_changes_for(
        self, cluster_name: str, change_filter: Callable[[ChangeItem], bool]
    ) -> Generator[ChangeItem, None, None]:
        for replica_key in self.changes:
            cluster, _ = replica_key
            if cluster != cluster_name:
                continue
            change = self.changes[replica_key]
            if not change_filter(change):
                continue
            yield change

    def _log_attr_change(
        self,
        log: Logger,
        cluster: str,
        path: str,
        attribute_path: str,
        desired_value: Any | None,
        actual_value: Any | None,
        propagated_attr: bool = False,
    ):
        if desired_value is None:
            log.warning("  remove attribute %s:%s/@%s: actual=%s", cluster, path, attribute_path, actual_value)
        elif actual_value is None:
            log.warning(
                "  add %s %s:%s/@%s: value=%s",
                "propagated attribute" if propagated_attr else "attribute",
                cluster,
                path,
                attribute_path,
                desired_value,
            )
        else:
            log.warning(
                "  update attribute %s:%s/@%s: actual=%s, desired=%s",
                cluster,
                path,
                attribute_path,
                actual_value,
                desired_value,
            )

    def _get_item(self, table: YtTable) -> TableAttributesChange.ChangeItem:
        return self.changes.setdefault(table.replica_key, self.ChangeItem())
