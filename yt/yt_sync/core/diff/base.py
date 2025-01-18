from abc import ABC
from abc import abstractmethod
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from enum import IntEnum
from logging import Logger

from yt.yt_sync.core.model import Types
from yt.yt_sync.core.model import YtTable


@dataclass
class DiffBase(ABC):
    @abstractmethod
    def is_empty(self) -> bool:
        raise NotImplementedError()

    @abstractmethod
    def check_and_log(self, log: Logger) -> bool:
        raise NotImplementedError()


class TableDiffType(IntEnum):
    MISSING_TABLE = 1
    ORPHANED_TABLE = 2
    REPLICAS_CHANGE = 3
    SCHEMA_CHANGE = 4
    ATTRIBUTES_CHANGE = 5
    TABLET_COUNT_CHANGE = 6

    @classmethod
    def all(cls) -> set[int]:
        return {t.value for t in TableDiffType}


@dataclass
class TableDiffBase(DiffBase):
    diff_type: TableDiffType

    @abstractmethod
    def require_existing_main(self) -> bool:
        raise NotImplementedError()

    @abstractmethod
    def has_diff_for(self, cluster_name: str) -> bool:
        raise NotImplementedError()

    @abstractmethod
    def add_change_if_any(self, desired_table: YtTable, actual_table: YtTable):
        raise NotImplementedError()

    def is_unmount_required(self, cluster_name: str) -> bool:
        return False

    def is_remount_required(self, cluster_name: str) -> bool:
        return False

    @staticmethod
    def _contains_cluster_in_key(keys: set[Types.ReplicaKey], cluster_name: str) -> bool:
        for key in keys:
            key_cluster, _ = key
            if key_cluster == cluster_name:
                return True
        return False


@dataclass
class TableDiffSet:
    table_key: str
    diffs: list[TableDiffBase] = dataclass_field(default_factory=list)


class NodeDiffType(IntEnum):
    MISSING_NODE = 101
    ATTRIBUTES_CHANGE = 102

    @classmethod
    def all(cls) -> set[int]:
        return {t.value for t in NodeDiffType}


@dataclass
class NodeDiffBase(DiffBase):
    diff_type: NodeDiffType
