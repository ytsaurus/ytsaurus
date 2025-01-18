import pytest

from yt.yt_sync.action import CopyReplicationProgressAction
from yt.yt_sync.core.model import Types
from yt.yt_sync.core.model import YtTable
from yt.yt_sync.core.test_lib import MockResult
from yt.yt_sync.core.test_lib import MockYtClientFactory


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


def _create_mock_factory(from_table: YtTable, to_table: YtTable) -> MockYtClientFactory:
    result = MockResult(result=[1])
    return MockYtClientFactory(
        {
            from_table.cluster_name: {
                "alter_table": {to_table.path: result},
                "get": {f"{from_table.path}/@replication_progress": result},
            }
        }
    )


def test_double_schedule(from_table: YtTable, to_table: YtTable):
    action = CopyReplicationProgressAction(from_table, to_table)
    yt_client_factory = _create_mock_factory(from_table, to_table)
    yt_client = yt_client_factory(from_table.cluster_name)
    assert not action.schedule_next(yt_client)
    with pytest.raises(AssertionError):
        action.schedule_next(yt_client)


def test_process_before_schedule(from_table: YtTable, to_table: YtTable):
    action = CopyReplicationProgressAction(from_table, to_table)
    with pytest.raises(AssertionError):
        action.process()


def test_success(from_table: YtTable, to_table: YtTable):
    action = CopyReplicationProgressAction(from_table, to_table)
    yt_client_factory = _create_mock_factory(from_table, to_table)
    yt_client = yt_client_factory(from_table.cluster_name)
    assert not action.schedule_next(yt_client)

    call_tracker = yt_client_factory.get_call_tracker(from_table.cluster_name)
    assert call_tracker
    assert "get" in call_tracker.calls
    assert "alter_table" in call_tracker.calls

    action.process()
