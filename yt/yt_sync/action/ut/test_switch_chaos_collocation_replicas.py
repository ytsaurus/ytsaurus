import uuid

import pytest

import yt.wrapper as yt
from yt.yt_sync.action import SwitchChaosCollocationReplicasAction
from yt.yt_sync.core.model import Types
from yt.yt_sync.core.model import YtTable
from yt.yt_sync.core.test_lib import MockResult
from yt.yt_sync.core.test_lib import MockYtClientFactory
from yt.yt_sync.core.test_lib import yt_generic_error


@pytest.fixture
def yt_table(table_path: str, default_schema: Types.Schema):
    return YtTable.make(
        table_path,
        "primary",
        YtTable.Type.CHAOS_REPLICATED_TABLE,
        table_path,
        True,
        {"schema": default_schema, "replication_card_id": str(uuid.uuid4())},
        explicit_in_collocation=True,
    )


@pytest.fixture
def sync_replicas() -> set[str]:
    return {"remote0"}


def _create_mock_factory(table: YtTable, error: bool = False) -> MockYtClientFactory:
    result = MockResult(error=yt_generic_error()) if error else MockResult(result=True)
    return MockYtClientFactory(
        {table.cluster_name: {"alter_replication_card": {table.chaos_replication_card_id: result}}}
    )


def test_double_schedule(yt_table: YtTable, sync_replicas: set[str]):
    action = SwitchChaosCollocationReplicasAction(yt_table, sync_replicas)
    yt_client_factory = _create_mock_factory(yt_table)
    yt_client = yt_client_factory(yt_table.cluster_name)
    assert not action.schedule_next(yt_client)
    with pytest.raises(AssertionError):
        action.schedule_next(yt_client)


def test_process_before_schedule(yt_table: YtTable, sync_replicas: set[str]):
    action = SwitchChaosCollocationReplicasAction(yt_table, sync_replicas)
    with pytest.raises(AssertionError):
        action.process()


def test_no_replication_card_id(yt_table: YtTable, sync_replicas: set[str]):
    action = SwitchChaosCollocationReplicasAction(yt_table, sync_replicas)
    yt_client_factory = _create_mock_factory(yt_table)
    yt_client = yt_client_factory(yt_table.cluster_name)

    yt_table.chaos_replication_card_id = None
    with pytest.raises(AssertionError):
        action.schedule_next(yt_client)


def test_succes(yt_table: YtTable, sync_replicas: set[str]):
    action = SwitchChaosCollocationReplicasAction(yt_table, sync_replicas)
    yt_client_factory = _create_mock_factory(yt_table)
    yt_client = yt_client_factory(yt_table.cluster_name)
    assert action.schedule_next(yt_client) is False
    action.process()

    # assert calls
    call_tracker = yt_client_factory.get_call_tracker(yt_table.cluster_name)
    assert call_tracker
    assert 1 == len(call_tracker.calls["alter_replication_card"])
    call = call_tracker.calls["alter_replication_card"][0]
    assert call.path_or_type == yt_table.chaos_replication_card_id


def test_yt_error(yt_table: YtTable, sync_replicas: set[str]):
    action = SwitchChaosCollocationReplicasAction(yt_table, sync_replicas)
    yt_client_factory = _create_mock_factory(yt_table, error=True)
    yt_client = yt_client_factory(yt_table.cluster_name)
    assert action.schedule_next(yt_client) is False
    with pytest.raises(yt.YtResponseError):
        action.process()
