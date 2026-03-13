"""Define ground cluster configuration."""

from __future__ import annotations

from abc import ABC, abstractmethod
import enum
from dataclasses import dataclass
from typing import Any, TypeAlias


class Scope(enum.Enum):
    SEQUOIA = enum.auto()
    REPLICAS = enum.auto()


def flatten(config: dict[str, Any], prefix="") -> list[tuple[str, Any]]:
    """Flatten into a list of (path, value) tuples."""
    if prefix.endswith("/"):
        raise ValueError(f'Prefix "{prefix}" should not end with "/"')

    result = []

    def _flatten(path: str, value: Any) -> None:
        if isinstance(value, dict):
            for k, v in value.items():
                new_path = f"{path}/{k}" if path else k
                _flatten(new_path, v)
        else:
            result.append((path, value))

    _flatten(prefix, config)
    return result


ScopeList: TypeAlias = list[Scope]


@dataclass
class GroundClusterConfig:
    cluster: str
    account_resource_limits: dict[str, Any]
    master_dynamic_config: dict[str, Any]
    sequoia_components: ScopeList
    account: str = "sequoia"
    sequoia_root_cypress_path: str = "//sys/sequoia"


@dataclass
class TableGroupDescriptor:
    name: str
    attributes: dict[str, Any]


@dataclass
class SequoiaComponentConfig:
    tablet_cell_bundle: str
    tablet_cell_bundle_config: dict[str, Any]
    tablet_cell_count: int
    table_groups: list[TableGroupDescriptor]

    def get_table_group_attributes(self, name: str) -> dict[str, Any]:
        for group in self.table_groups:
            if group.name == name:
                return group.attributes
        raise RuntimeError(f'Unknown group "{name}"')


class ConfigProvider(ABC):
    @abstractmethod
    def get_ground_config(self) -> GroundClusterConfig:
        """Return the Sequoia cluster config."""
        pass

    @abstractmethod
    def get_component_config(self, scope: Scope) -> SequoiaComponentConfig:
        """Return the Sequoia component config."""
        pass
