from abc import ABC
from abc import abstractmethod
from typing import Any

from yt.yt_sync.core.client import YtClientProxy
from yt.yt_sync.core.model import YtTable


def patched_rtt_options(table: YtTable, patch: dict[str, Any]) -> dict[str, Any]:
    rtt_options: dict[str, Any] = table.rtt_options.yt_attributes
    rtt_options.update(patch)
    return rtt_options


class AttributeChangeSetterBase(ABC):
    def __init__(self, table: YtTable):
        self.table = table
        self.to_remove: set[str] = set()
        self.to_set: dict[str, Any] = dict()

    def remove(self, attribute_path: str):
        self.to_remove.add(attribute_path)

    def set(self, attribute_path: str, value: Any):
        self.to_set[attribute_path] = value

    def attribute_path(self, attribute: str) -> str:
        return f"{self.table.path}&/@{attribute}"

    @abstractmethod
    def apply_changes(self, batch_client: YtClientProxy) -> list[Any]:
        raise NotImplementedError()


class YtCommands(ABC):
    """Collection of YT commands different for CHAOS and replicated tables."""

    @abstractmethod
    def create_collocation(self, yt_client: YtClientProxy, name: str, tables: list[str]) -> str | None:
        raise NotImplementedError()

    @abstractmethod
    def remove_from_collocation(self, batch_client: YtClientProxy, collocation_holder: str):
        raise NotImplementedError()

    @abstractmethod
    def add_to_collocation(self, batch_client: YtClientProxy, table: YtTable, collocation_id: str):
        raise NotImplementedError()

    @abstractmethod
    def update_rtt_options(self, batch_client: YtClientProxy, table: YtTable, rtt_options: dict[str, Any]) -> list[Any]:
        raise NotImplementedError()

    @abstractmethod
    def set_preferred_sync_replicas(
        self,
        batch_client: YtClientProxy,
        table: YtTable,
        preferred_sync_replicas: list[str],
        min_sync_replicas_count: int | None = None,
        max_sync_replicas_count: int | None = None,
    ) -> list[Any]:
        raise NotImplementedError()

    @abstractmethod
    def set_table_rtt_enabled(self, batch_client: YtClientProxy, table: YtTable, enabled: bool) -> Any:
        raise NotImplementedError()

    @abstractmethod
    def get_attribute_setter(self, table: YtTable) -> AttributeChangeSetterBase:
        raise NotImplementedError()

    def _get_collocation_id(self, yt_client: YtClientProxy, tables: list[str]) -> str | None:
        any_table_path = next(iter(tables), None)
        if any_table_path:
            collocation_id = str(yt_client.get(f"{any_table_path}&/@replication_collocation_id"))
            return collocation_id
        return None
