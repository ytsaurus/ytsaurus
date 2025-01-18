from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field as dataclass_field
import logging
import time
from typing import Any

from yt.yt_sync.core.client import YtClientFactory
from yt.yt_sync.core.client import YtClientProxy
from yt.yt_sync.core.helpers import is_valid_collocation_id
from yt.yt_sync.core.model import YtCluster
from yt.yt_sync.core.model import YtTable
from yt.yt_sync.core.settings import Settings
from yt.yt_sync.core.yt_commands import make_yt_commands

LOG = logging.getLogger("yt_sync")


@dataclass
class CollocationState:
    settings: Settings
    main_cluster: YtCluster

    # table_key -> collocation_id
    actual_collocations: dict[str, str | None] = dataclass_field(default_factory=dict())
    # table_key -> table_path or replication_card_id
    collocation_holders_to_exclude: dict[str, str] = dataclass_field(default_factory=dict())
    # collected collocation ids
    collocation_id: str | None = None

    @property
    def collocation_exists(self) -> bool:
        return is_valid_collocation_id(self.collocation_id)

    @property
    def is_collocation_required(self) -> bool:
        return len(self.actual_collocations) > 0

    @property
    def is_batched(self) -> bool:
        if self.settings.is_chaos:
            return self.settings.chaos_collocation_allow_batched
        return True

    def _collect_holders_to_exclude(self, collocation_holders: dict[str, str]):
        for path, holder_id in collocation_holders.items():
            if self.main_cluster.find_table_by_path(path) is None:
                LOG.debug("Found unmanaged table at %s in collocation #%s", path, self.collocation_id)
                self.collocation_holders_to_exclude[path] = holder_id

    def _collect_unmanaged_replicated(self, yt_client: YtClientProxy):
        collocation_table_paths: list[str] = yt_client.get(f"#{self.collocation_id}/@table_paths")
        # path -> path
        collocation_holders: dict[str, str] = {path: path for path in collocation_table_paths}
        self._collect_holders_to_exclude(collocation_holders)

    def _collect_unmanaged_chaos(self, yt_client: YtClientProxy, batch_client: YtClientProxy):
        if not self.actual_collocations:
            return
        table_path: str = self.main_cluster.tables[next(iter(self.actual_collocations.keys()))].path
        collocation_card_ids: list[str] = list(yt_client.get(f"{table_path}/@collocated_replication_card_ids"))
        responses: dict[str, Any] = dict()
        for card_id in collocation_card_ids:
            responses[card_id] = batch_client.get(f"#{card_id}/@table_path")
        batch_client.commit_batch()
        # path -> replication_card_id
        collocation_holders: dict[str, str] = dict()
        for card_id, response in responses.items():
            collocation_holders[response.get_result()] = card_id
        self._collect_holders_to_exclude(collocation_holders)

    def collect_unmanaged(self, yt_client: YtClientProxy, batch_client: YtClientProxy):
        if self.settings.is_chaos:
            self._collect_unmanaged_chaos(yt_client, batch_client)
        else:
            self._collect_unmanaged_replicated(yt_client)

    def add_tables_to_existing_collocation(self, log_only: bool, batch_client: YtClientProxy) -> bool:
        if not self.actual_collocations:
            return False
        LOG.debug("Add tables to collocation, batched=%s", self.is_batched)
        has_request: bool = False
        yt_commands = make_yt_commands(self.settings.is_chaos)
        for table_key, existing_collocation_id in self.actual_collocations.items():
            if not existing_collocation_id:
                has_request = True
                table = self.main_cluster.tables[table_key]
                LOG.warning(
                    "Adding table to replication collocation: table=%s, collocation_id=%s",
                    table.path,
                    self.collocation_id,
                )
                if not log_only:
                    yt_commands.add_to_collocation(batch_client, table, self.collocation_id)
                    if not self.is_batched:
                        batch_client.commit_batch()
        if has_request:
            if self.is_batched:
                batch_client.commit_batch()
            for table_key, existing_collocation_id in self.actual_collocations.items():
                if not existing_collocation_id:
                    table = self.main_cluster.tables[table_key]
                    table.replication_collocation_id = self.collocation_id
        return True

    def remove_tables_from_existing_collocation(self, log_only: bool, batch_client: YtClientProxy) -> bool:
        if not self.collocation_holders_to_exclude:
            return False
        LOG.debug("Remove tables from collocation, batched=%s", self.is_batched)
        yt_commands = make_yt_commands(self.settings.is_chaos)
        for table_path in sorted(self.collocation_holders_to_exclude):
            collocation_holder = self.collocation_holders_to_exclude[table_path]
            LOG.warning("Exclude table %s from replication collocation #%s", table_path, self.collocation_id)
            if not log_only:
                assert is_valid_collocation_id(self.collocation_id)
                yt_commands.remove_from_collocation(batch_client, collocation_holder)
                if not self.is_batched:
                    batch_client.commit_batch()
        if not log_only:
            if self.is_batched:
                batch_client.commit_batch()
            for table_path in sorted(self.collocation_holders_to_exclude):
                table: YtTable | None = self.main_cluster.find_table_by_path(table_path)
                if table:
                    table.replication_collocation_id = None
        return True

    def _wait_chaos(self, yt_client: YtClientProxy, table_path: str, expected_replication_cards: set[str]):
        TIMEOUT = 5 * 60
        DELAY = 0.3
        ITER_COUNT = int(TIMEOUT / DELAY)
        LOG.debug("Wait chaos collocation for cards %s", sorted(expected_replication_cards))
        for i in range(ITER_COUNT):
            collocation_card_ids: set[str] = set(yt_client.get(f"{table_path}/@collocated_replication_card_ids"))
            LOG.debug("Got cards from chaos collocation: %s", sorted(collocation_card_ids))
            if collocation_card_ids == expected_replication_cards:
                LOG.debug("Chaos collocation is ready")
                return
            LOG.debug("Chaos collocation not ready, wait. Iteration #%s", i)
            time.sleep(DELAY)

    def create_collocation(self, log_only: bool, yt_client: YtClientProxy) -> bool:
        assert self.settings.collocation_name
        tables = list()
        expected_replication_cards: set[str] = set()
        LOG.debug("Create table collocation, batched=%s", self.is_batched)
        for table_key in sorted(self.actual_collocations):
            table = self.main_cluster.tables[table_key]
            tables.append(self.main_cluster.tables[table_key].path)
            if table.chaos_replication_card_id:
                expected_replication_cards.add(table.chaos_replication_card_id)
        if not tables:
            return False

        LOG.warning(
            "Creating a new table replication collocation: name=%s, tables=%s",
            self.settings.collocation_name,
            tables,
        )
        if not log_only:
            yt_commands = make_yt_commands(self.settings.is_chaos)
            collocation_id: str = ""
            if self.is_batched:
                collocation_id = yt_commands.create_collocation(yt_client, self.settings.collocation_name, tables)
            else:
                head = tables[:1]
                tail = tables[1:]
                collocation_id = yt_commands.create_collocation(yt_client, self.settings.collocation_name, head)
                for path in tail:
                    table: YtTable | None = self.main_cluster.find_table_by_path(path)
                    assert table is not None
                    yt_commands.add_to_collocation(yt_client, table, collocation_id)
            assert is_valid_collocation_id(collocation_id)
            if self.settings.is_chaos:
                self._wait_chaos(yt_client, tables[0], expected_replication_cards)
            for table_key in sorted(self.actual_collocations):
                self.main_cluster.tables[table_key].replication_collocation_id = collocation_id
        return True

    @classmethod
    def build(cls, settings: Settings, desired_main: YtCluster, actual_main: YtCluster) -> CollocationState:
        actual_collocations: dict[str, str | None] = dict()  # table_key -> collocation_id
        collocation_holders_to_exclude: dict[str, str] = dict()  # table_key -> table_path or replication_card_id

        for table_key, actual_table in actual_main.tables.items():
            if not actual_table.exists or not actual_table.is_replicated:
                continue
            desired_table = desired_main.tables[table_key]
            if desired_table.in_collocation:
                if is_valid_collocation_id(actual_table.replication_collocation_id):
                    actual_collocations[actual_table.key] = actual_table.replication_collocation_id
                    LOG.debug(
                        "Found collocation_id=%s for table %s",
                        actual_table.replication_collocation_id,
                        actual_table.path,
                    )
                else:
                    actual_collocations[actual_table.key] = None
            else:
                if is_valid_collocation_id(actual_table.replication_collocation_id):
                    if settings.is_chaos:
                        collocation_holders_to_exclude[actual_table.path] = actual_table.chaos_replication_card_id
                    else:
                        collocation_holders_to_exclude[actual_table.path] = actual_table.path

        collocation_ids = set((v for v in actual_collocations.values() if v is not None))
        LOG.debug("Collected collocation ids: %s", collocation_ids)
        assert len(collocation_ids) <= 1, "Only one collocation allowed"
        return cls(
            settings=settings,
            main_cluster=actual_main,
            actual_collocations=actual_collocations,
            collocation_holders_to_exclude=collocation_holders_to_exclude,
            collocation_id=next(iter(collocation_ids), None),
        )


def ensure_collocation(
    desired_main: YtCluster,
    actual_main: YtCluster,
    yt_client_factory: YtClientFactory,
    settings: Settings,
    log_only: bool = False,
) -> bool:
    LOG.info("Ensure table collocations.")
    has_diff: bool = False
    state: CollocationState = CollocationState.build(settings, desired_main, actual_main)

    yt_client = yt_client_factory(actual_main.name)
    batch_client = yt_client.create_batch_client(raise_errors=True)
    if state.collocation_exists:
        state.collect_unmanaged(yt_client, batch_client)
        has_diff |= state.add_tables_to_existing_collocation(log_only, batch_client)
        has_diff |= state.remove_tables_from_existing_collocation(log_only, batch_client)
    elif state.is_collocation_required:
        has_diff |= state.create_collocation(log_only, yt_client)

    LOG.info("Table collocations ensured.")
    return has_diff
