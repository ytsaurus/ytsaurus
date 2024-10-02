import pytest

import yt.wrapper as yt
from yt.yt_sync.action import ReadTotalRowCountAction
from yt.yt_sync.core.model import Types
from yt.yt_sync.core.model import YtTable
from yt.yt_sync.core.model import YtTabletState
from yt.yt_sync.core.test_lib import MockResult
from yt.yt_sync.core.test_lib import MockYtClientFactory
from yt.yt_sync.core.test_lib import yt_generic_error


def _make_table(cluster_name: str, table_path: str, table_type: str, schema: Types.Schema):
    result = YtTable.make(
        table_path, cluster_name, table_type, table_path, True, {"dynamic": True, "schema": schema, "tablet_count": 2}
    )
    result.tablet_state.set(YtTabletState.FROZEN)
    return result


def _create_mock_factory(table: YtTable, error: bool = False) -> MockYtClientFactory:
    tablets = {"tablets": [{"total_row_count": x + 1} for x in range(table.effective_tablet_count)]}
    result = MockResult(error=yt_generic_error()) if error else MockResult(result=tablets)
    return MockYtClientFactory({table.cluster_name: {"get_tablet_infos": {table.path: result}}})


@pytest.fixture
def cluster_name() -> str:
    return "r0"


@pytest.fixture
def yt_table(cluster_name: str, table_path: str, ordered_schema: Types.Schema) -> YtTable:
    return _make_table(cluster_name, table_path, YtTable.Type.TABLE, ordered_schema)


def test_sorted_table(cluster_name: str, table_path: str, default_schema: Types.Schema):
    yt_table = _make_table(cluster_name, table_path, YtTable.Type.TABLE, default_schema)
    with pytest.raises(AssertionError):
        ReadTotalRowCountAction(yt_table)


@pytest.mark.parametrize(
    "table_type", [YtTable.Type.REPLICATED_TABLE, YtTable.Type.CHAOS_REPLICATED_TABLE, YtTable.Type.REPLICATION_LOG]
)
def test_unsupported_table_type(cluster_name: str, table_path: str, ordered_schema: Types.Schema, table_type: str):
    yt_table = _make_table(cluster_name, table_path, table_type, ordered_schema)
    with pytest.raises(AssertionError):
        ReadTotalRowCountAction(yt_table)


def test_double_schedule(yt_table: YtTable):
    action = ReadTotalRowCountAction(yt_table)
    yt_client_factory = _create_mock_factory(yt_table)
    yt_client = yt_client_factory(yt_table.cluster_name)
    assert not action.schedule_next(yt_client)
    with pytest.raises(AssertionError):
        action.schedule_next(yt_client)


def test_process_before_schedule(yt_table: YtTable):
    action = ReadTotalRowCountAction(yt_table)
    with pytest.raises(AssertionError):
        action.process()


def test_not_exists(yt_table: YtTable):
    yt_table.exists = False
    action = ReadTotalRowCountAction(yt_table)
    yt_client_factory = _create_mock_factory(yt_table)
    yt_client = yt_client_factory(yt_table.cluster_name)
    with pytest.raises(AssertionError):
        action.schedule_next(yt_client)


def test_mounted(yt_table: YtTable):
    yt_table.tablet_state.set(YtTabletState.MOUNTED)
    action = ReadTotalRowCountAction(yt_table)
    yt_client_factory = _create_mock_factory(yt_table)
    yt_client = yt_client_factory(yt_table.cluster_name)
    with pytest.raises(AssertionError):
        action.schedule_next(yt_client)


def test_get_tablet_infos_error(yt_table: YtTable):
    action = ReadTotalRowCountAction(yt_table)
    yt_client_factory = _create_mock_factory(yt_table, True)
    yt_client = yt_client_factory(yt_table.cluster_name)
    assert action.schedule_next(yt_client) is False
    with pytest.raises(yt.YtResponseError):
        action.process()


def test_success(yt_table: YtTable):
    assert not yt_table.total_row_count
    action = ReadTotalRowCountAction(yt_table)
    yt_client_factory = _create_mock_factory(yt_table)
    yt_client = yt_client_factory(yt_table.cluster_name)
    assert action.schedule_next(yt_client) is False
    action.process()
    assert yt_table.total_row_count == [x + 1 for x in range(yt_table.effective_tablet_count)]
