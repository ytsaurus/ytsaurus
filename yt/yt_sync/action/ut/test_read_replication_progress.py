import pytest

import yt.wrapper as yt
from yt.yt_sync.action import ReadReplicationProgressAction
from yt.yt_sync.core.model import Types
from yt.yt_sync.core.model import YtTable
from yt.yt_sync.core.model import YtTabletState
from yt.yt_sync.core.test_lib import MockResult
from yt.yt_sync.core.test_lib import MockYtClientFactory
from yt.yt_sync.core.test_lib import yt_generic_error

_TABLET_COUNT = 3


def _make_table(cluster_name: str, table_path: str, table_type: str, schema: Types.Schema):
    result = YtTable.make(
        table_path,
        cluster_name,
        table_type,
        table_path,
        True,
        {"dynamic": True, "schema": schema, "tablet_count": _TABLET_COUNT},
    )
    result.tablet_state.set(YtTabletState.UNMOUNTED)
    return result


def _get_replication_progress() -> list[int]:
    return [i for i in range(_TABLET_COUNT)]


def _create_mock_factory(table: YtTable, error: bool = False) -> MockYtClientFactory:
    result = MockResult(error=yt_generic_error()) if error else MockResult(result=_get_replication_progress())
    return MockYtClientFactory({table.cluster_name: {"get": {f"{table.path}/@replication_progress": result}}})


@pytest.fixture
def cluster_name() -> str:
    return "r0"


@pytest.fixture
def yt_table(cluster_name: str, table_path: str, default_schema: Types.Schema) -> YtTable:
    return _make_table(cluster_name, table_path, YtTable.Type.TABLE, default_schema)


@pytest.mark.parametrize("table_type", [YtTable.Type.REPLICATED_TABLE, YtTable.Type.CHAOS_REPLICATED_TABLE])
def test_unsupported_table_type(cluster_name: str, table_path: str, default_schema: Types.Schema, table_type: str):
    yt_table = _make_table(cluster_name, table_path, table_type, default_schema)
    with pytest.raises(AssertionError):
        ReadReplicationProgressAction(yt_table)


def test_double_schedule(yt_table: YtTable):
    action = ReadReplicationProgressAction(yt_table)
    yt_client_factory = _create_mock_factory(yt_table)
    yt_client = yt_client_factory(yt_table.cluster_name)
    assert not action.schedule_next(yt_client)
    with pytest.raises(AssertionError):
        action.schedule_next(yt_client)


def test_process_before_schedule(yt_table: YtTable):
    action = ReadReplicationProgressAction(yt_table)
    with pytest.raises(AssertionError):
        action.process()


def test_not_exists(yt_table: YtTable):
    yt_table.exists = False
    action = ReadReplicationProgressAction(yt_table)
    yt_client_factory = _create_mock_factory(yt_table)
    yt_client = yt_client_factory(yt_table.cluster_name)
    with pytest.raises(AssertionError):
        action.schedule_next(yt_client)


@pytest.mark.parametrize("tablet_state", [YtTabletState.MOUNTED, YtTabletState.FROZEN])
def test_not_unmounted(yt_table: YtTable, tablet_state: str):
    yt_table.tablet_state.set(tablet_state)
    action = ReadReplicationProgressAction(yt_table)
    yt_client_factory = _create_mock_factory(yt_table)
    yt_client = yt_client_factory(yt_table.cluster_name)
    with pytest.raises(AssertionError):
        action.schedule_next(yt_client)


def test_get_replication_progress_error(yt_table: YtTable):
    action = ReadReplicationProgressAction(yt_table)
    yt_client_factory = _create_mock_factory(yt_table, True)
    yt_client = yt_client_factory(yt_table.cluster_name)
    assert action.schedule_next(yt_client) is False
    with pytest.raises(yt.YtResponseError):
        action.process()


def test_success(yt_table: YtTable):
    assert not yt_table.chaos_replication_progress
    action = ReadReplicationProgressAction(yt_table)
    yt_client_factory = _create_mock_factory(yt_table)
    yt_client = yt_client_factory(yt_table.cluster_name)
    assert action.schedule_next(yt_client) is False
    action.process()
    assert yt_table.chaos_replication_progress == _get_replication_progress()
