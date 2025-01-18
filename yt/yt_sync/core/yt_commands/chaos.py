from typing import Any

from yt.yt_sync.core.client import YtClientProxy
from yt.yt_sync.core.model import YtTable

from .base import AttributeChangeSetterBase
from .base import patched_rtt_options
from .base import YtCommands


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
                    replicated_table_options=patched_rtt_options(self.table, replicated_table_options),
                ),
            )
        return responses


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
        rtt_options = patched_rtt_options(table, patch)
        return self.update_rtt_options(batch_client, table, rtt_options)

    def set_table_rtt_enabled(self, batch_client: YtClientProxy, table: YtTable, enabled: bool) -> Any:
        assert table.chaos_replication_card_id
        return batch_client.alter_replication_card(
            table.chaos_replication_card_id, enable_replicated_table_tracker=enabled
        )

    def get_attribute_setter(self, table: YtTable) -> AttributeChangeSetterBase:
        return ChaosAttributeChangeSetter(table)
