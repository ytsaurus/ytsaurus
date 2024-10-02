from abc import ABC
from abc import abstractmethod
from typing import Any

from .client import YtClientProxy
from .model import YtTable


def _patched_rtt_options(table: YtTable, patch: dict[str, Any]) -> dict[str, Any]:
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


class AttributeChangeSetter(AttributeChangeSetterBase):
    def __init__(self, table: YtTable):
        super().__init__(table)

    def apply_changes(self, batch_client: YtClientProxy) -> list[Any]:
        responses = list()
        for attribute in sorted(self.to_remove):
            responses.append(batch_client.remove(self.attribute_path(attribute)))
        for attribute in sorted(self.to_set):
            responses.append(batch_client.set(self.attribute_path(attribute), self.to_set[attribute]))
        return responses


class ChaosAttributeChangeSetter(AttributeChangeSetterBase):
    def __init__(self, table: YtTable):
        super().__init__(table)

    def apply_changes(self, batch_client: YtClientProxy) -> list[Any]:
        responses = list()
        replicated_table_options = dict()
        for attribute in sorted(self.to_remove):
            # TODO: forbid removing RTT options?
            responses.append(batch_client.remove(self.attribute_path(attribute)))
        for attribute in sorted(self.to_set):
            if self.table.is_chaos_replicated and attribute.startswith("replicated_table_options"):
                value = self.to_set[attribute]
                if "/" in attribute:
                    _, rtt_option = attribute.split("/", 1)
                    replicated_table_options[rtt_option] = value
                else:
                    assert isinstance(value, dict)
                    replicated_table_options = value
            else:
                responses.append(batch_client.set(self.attribute_path(attribute), self.to_set[attribute]))
        if replicated_table_options:
            assert self.table.chaos_replication_card_id
            responses.append(
                batch_client.alter_replication_card(
                    self.table.chaos_replication_card_id,
                    replicated_table_options=_patched_rtt_options(self.table, replicated_table_options),
                ),
            )
        return responses


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

    def get_attribute_setter(self, table: YtTable) -> AttributeChangeSetterBase:
        return AttributeChangeSetter(table)

    def _get_collocation_id(self, yt_client: YtClientProxy, tables: list[str]) -> str | None:
        any_table_path = next(iter(tables), None)
        if any_table_path:
            collocation_id = str(yt_client.get(f"{any_table_path}&/@replication_collocation_id"))
            return collocation_id
        return None


class YtCommandsReplicated(YtCommands):
    def __init__(self):
        super().__init__()

    def create_collocation(self, yt_client: YtClientProxy, name: str, tables: list[str]) -> str | None:
        yt_client.create(
            "table_collocation",
            path=None,
            attributes={
                "name": name,
                "collocation_type": "replication",
                "table_paths": tables,
            },
        )
        return self._get_collocation_id(yt_client, tables)

    def remove_from_collocation(self, batch_client: YtClientProxy, collocation_holder: str):
        batch_client.remove(f"{collocation_holder}&/@replication_collocation_id")

    def add_to_collocation(self, batch_client: YtClientProxy, table: YtTable, collocation_id: str):
        return batch_client.set(f"{table.path}&/@replication_collocation_id", collocation_id)

    def update_rtt_options(self, batch_client: YtClientProxy, table: YtTable, rtt_options: dict[str, Any]) -> list[Any]:
        return [batch_client.set(f"{table.path}&/@replicated_table_options", rtt_options)]

    def set_preferred_sync_replicas(
        self,
        batch_client: YtClientProxy,
        table: YtTable,
        preferred_sync_replicas: list[str],
        min_sync_replicas_count: int | None = None,
        max_sync_replicas_count: int | None = None,
    ) -> list[Any]:
        patch = {"preferred_sync_replica_clusters": preferred_sync_replicas}
        if min_sync_replicas_count is not None:
            patch.update({"min_sync_replica_count": min_sync_replicas_count})
        if max_sync_replicas_count is not None:
            patch.update({"max_sync_replica_count": max_sync_replicas_count})
        rtt_options = _patched_rtt_options(table, patch)
        return self.update_rtt_options(batch_client, table, rtt_options)

    def set_table_rtt_enabled(self, batch_client: YtClientProxy, table: YtTable, enabled: bool) -> Any:
        attr = f"{table.path}&/@replicated_table_options/enable_replicated_table_tracker"
        return batch_client.set(attr, enabled)


class YtCommandsChaos(YtCommands):
    def __init__(self):
        super().__init__()

    def create_collocation(self, yt_client: YtClientProxy, name: str, tables: list[str]) -> str | None:
        yt_client.create(
            "replication_card_collocation",
            path=None,
            attributes={
                "name": name,
                "type": "replication",
                "table_paths": tables,
            },
        )
        return self._get_collocation_id(yt_client, tables)

    def remove_from_collocation(self, batch_client: YtClientProxy, collocation_holder: str):
        return batch_client.alter_replication_card(collocation_holder, replication_card_collocation_id="0-0-0-0")

    def add_to_collocation(self, batch_client: YtClientProxy, table: YtTable, collocation_id: str):
        assert table.chaos_replication_card_id
        return batch_client.alter_replication_card(
            table.chaos_replication_card_id, replication_card_collocation_id=collocation_id
        )

    def update_rtt_options(self, batch_client: YtClientProxy, table: YtTable, rtt_options: dict[str, Any]) -> list[Any]:
        return [
            batch_client.alter_replication_card(table.chaos_replication_card_id, replicated_table_options=rtt_options)
        ]

    def set_preferred_sync_replicas(
        self,
        batch_client: YtClientProxy,
        table: YtTable,
        preferred_sync_replicas: list[str],
        min_sync_replicas_count: int | None = None,
        max_sync_replicas_count: int | None = None,
    ) -> list[Any]:
        assert table.chaos_replication_card_id
        patch = {"preferred_sync_replica_clusters": preferred_sync_replicas}
        if min_sync_replicas_count is not None:
            patch.update({"min_sync_replica_count": min_sync_replicas_count})
        if max_sync_replicas_count is not None:
            patch.update({"max_sync_replica_count": max_sync_replicas_count})
        rtt_options = _patched_rtt_options(table, patch)
        return self.update_rtt_options(batch_client, table, rtt_options)

    def set_table_rtt_enabled(self, batch_client: YtClientProxy, table: YtTable, enabled: bool) -> Any:
        assert table.chaos_replication_card_id
        return batch_client.alter_replication_card(
            table.chaos_replication_card_id, enable_replicated_table_tracker=enabled
        )

    def get_attribute_setter(self, table: YtTable) -> AttributeChangeSetterBase:
        return ChaosAttributeChangeSetter(table)


def make_yt_commands(is_chaos: bool) -> YtCommands:
    if is_chaos:
        return YtCommandsChaos()
    else:
        return YtCommandsReplicated()
