import pytest

import yt.wrapper as yt
from yt.yt_sync.action import MoveTableAction
from yt.yt_sync.core.model import Types
from yt.yt_sync.core.model import YtTable
from yt.yt_sync.core.test_lib import MockResult
from yt.yt_sync.core.test_lib import MockYtClientFactory
from yt.yt_sync.core.test_lib import yt_generic_error


@pytest.fixture
def cluster_name() -> str:
    return "r0"


@pytest.fixture
def from_table(cluster_name: str, table_path: str, default_schema: Types.Schema) -> YtTable:
    return YtTable.make(table_path, cluster_name, YtTable.Type.TABLE, table_path, True, {"schema": default_schema})


@pytest.fixture
def to_table(cluster_name: str, table_path: str, default_schema: Types.Schema) -> YtTable:
    return YtTable.make(
        table_path, cluster_name, YtTable.Type.TABLE, f"{table_path}_to", True, {"schema": default_schema}
    )


def _create_mock_factory(table: YtTable, error: bool = False) -> MockYtClientFactory:
    result = MockResult(error=yt_generic_error()) if error else MockResult(result=True)
    return MockYtClientFactory({table.cluster_name: {"move": {table.path: result}}})


def test_diff_cluster(from_table: YtTable, to_table: YtTable):
    to_table.cluster_name = "r1"
    with pytest.raises(AssertionError):
        MoveTableAction(from_table, to_table)


def test_same_path(from_table: YtTable, to_table: YtTable):
    to_table.path = from_table.path
    with pytest.raises(AssertionError):
        MoveTableAction(from_table, to_table)


def test_double_schedule(from_table: YtTable, to_table: YtTable):
    action = MoveTableAction(from_table, to_table)
    yt_client_factory = _create_mock_factory(from_table)
    yt_client = yt_client_factory(from_table.cluster_name)
    assert not action.schedule_next(yt_client)
    with pytest.raises(AssertionError):
        action.schedule_next(yt_client)


def test_process_before_schedule(from_table: YtTable, to_table: YtTable):
    action = MoveTableAction(from_table, to_table)
    with pytest.raises(AssertionError):
        action.process()


def test_from_not_exists(from_table: YtTable, to_table: YtTable):
    from_table.exists = False
    action = MoveTableAction(from_table, to_table)
    yt_client_factory = _create_mock_factory(from_table)
    yt_client = yt_client_factory(from_table.cluster_name)
    with pytest.raises(AssertionError):
        action.schedule_next(yt_client)


def test_success(from_table: YtTable, to_table: YtTable):
    to_table.exists = False
    action = MoveTableAction(from_table, to_table)
    yt_client_factory = _create_mock_factory(from_table)
    yt_client = yt_client_factory(from_table.cluster_name)
    assert not action.schedule_next(yt_client)

    call_tracker = yt_client_factory.get_call_tracker(from_table.cluster_name)
    assert call_tracker
    assert "move" in call_tracker.calls

    action.process()
    assert not from_table.exists
    assert to_table.exists


def test_fail(from_table: YtTable, to_table: YtTable):
    to_table.exists = False
    action = MoveTableAction(from_table, to_table)
    yt_client_factory = _create_mock_factory(from_table, True)
    yt_client = yt_client_factory(from_table.cluster_name)
    assert not action.schedule_next(yt_client)

    call_tracker = yt_client_factory.get_call_tracker(from_table.cluster_name)
    assert call_tracker
    assert "move" in call_tracker.calls

    with pytest.raises(yt.YtResponseError):
        action.process()
