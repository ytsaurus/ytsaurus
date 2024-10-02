from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar

from .node_attributes import YtNodeAttributes
from .types import Types


def _matches_patterns(attr_name: str, patterns: frozenset[str]):
    return any(attr_name.startswith(pattern) for pattern in patterns)


@dataclass
class YtTableAttributes(YtNodeAttributes):
    _ACTUAL_ONLY: ClassVar[frozenset[str]] = frozenset(("data_weight",))
    # Ignored by yt_sync
    _UNTRACKED: ClassVar[frozenset[str]] = frozenset(["upstream_replica_id", "max_unix_time", "yabs_data_time"])
    _UNTRACKED_PATTERNS: ClassVar[frozenset[str]] = frozenset(["last_sync_time"])

    # Require freeze -> unmount -> alter_table -> mount actions sequence on alter
    _REQUIRE_UNMOUNT: ClassVar[frozenset[str]] = frozenset(
        {
            "enable_dynamic_store_read",
            "in_memory_mode",
            "primary_medium",
            "hunk_primary_medium",
            "tablet_cell_bundle",
            "account",
            "commit_ordering",
        }
    )
    # Can be altered without remount
    _REGULAR: ClassVar[frozenset[str]] = frozenset({"version"})
    _REGULAR_PATTERNS: ClassVar[frozenset[str]] = frozenset({"static_export_config"})

    @classmethod
    def _is_untracked(cls, attr_name: str) -> bool:
        if attr_name in cls._UNTRACKED or _matches_patterns(attr_name, cls._UNTRACKED_PATTERNS):
            return True
        return False

    @classmethod
    def _are_changes_ignored(cls, attr_name: str) -> bool:
        return cls._is_untracked(attr_name) or attr_name in cls._ACTUAL_ONLY

    def is_unmount_required(self, other: YtTableAttributes) -> bool:
        if not isinstance(other, type(self)):
            raise TypeError("Can compare only with other YtTableAttributes instance")
        for path, desired, actual in self.changed_attributes(other):
            if path in self._REQUIRE_UNMOUNT:
                return True
        return False

    def is_remount_required(self, other: YtTableAttributes) -> bool:
        if not isinstance(other, type(self)):
            raise TypeError("Can compare only with other YtTableAttributes instance")
        for path, desired, actual in self.changed_attributes(other):
            is_not_regular = path not in self._REGULAR and not _matches_patterns(path, self._REGULAR_PATTERNS)
            if path not in self._REQUIRE_UNMOUNT and is_not_regular:
                return True
        return False

    @classmethod
    def make(cls, attributes: Types.Attributes, filter_: YtTableAttributes | None = None) -> YtTableAttributes:
        attrs = super().make(
            attributes,
            YtTableAttributes.make(filter_.attributes | {attr: None for attr in cls._ACTUAL_ONLY}) if filter_ else None,
        )
        return YtTableAttributes(attributes=attrs.attributes, user_attribute_keys=attrs.user_attribute_keys)
