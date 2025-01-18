import pytest

import yt.wrapper as yt
from yt.yt_sync.action import SetReplicationProgressAction
from yt.yt_sync.core.model import Types
from yt.yt_sync.core.model import YtTable
from yt.yt_sync.core.test_lib import MockResult
from yt.yt_sync.core.test_lib import MockYtClientFactory
from yt.yt_sync.core.test_lib import yt_generic_error

_TABLET_COUNT = 3


def _make_table(cluster_name: str, table_path: str, table_type: str, schema: Types.Schema):
    return YtTable.make(
        table_path,
        cluster_name,
        table_type,
        table_path,
        True,
        {"dynamic": True, "schema": schema, "tablet_count": _TABLET_COUNT},
    )


def _get_replication_progress() -> list[int]:
    return [i for i in range(_TABLET_COUNT)]


def _create_mock_factory(table: YtTable, error: bool = False) -> MockYtClientFactory:
    result = MockResult(error=yt_generic_error()) if error else MockResult(result=True)
    return MockYtClientFactory({table.cluster_name: {"alter_table": {table.path: result}}})


@pytest.fixture
def cluster_name() -> str:
    return "r0"


@pytest.fixture
def src_table(cluster_name: str, table_path: str, default_schema: Types.Schema) -> YtTable:
    result = _make_table(cluster_name, f"{table_path}_src", YtTable.Type.TABLE, default_schema)
    result.chaos_replication_progress = _get_replication_progress()
    return result


@pytest.fixture
def dst_table(cluster_name: str, table_path: str, default_schema: Types.Schema) -> YtTable:
    return _make_table(cluster_name, table_path, YtTable.Type.TABLE, default_schema)


def test_ctor(cluster_name: str, table_path: str, default_schema: Types.Schema):
    for table_type in (YtTable.Type.REPLICATED_TABLE, YtTable.Type.CHAOS_REPLICATED_TABLE):
        # source is replicated
        with pytest.raises(AssertionError):
            SetReplicationProgressAction(
                _make_table(cluster_name, table_path, table_type, default_schema),
                _make_table(cluster_name, table_path, YtTable.Type.TABLE, default_schema),
            )

        # destination is replicated
        with pytest.raises(AssertionError):
            SetReplicationProgressAction(
                _make_table(cluster_name, table_path, YtTable.Type.TABLE, default_schema),
                _make_table(cluster_name, table_path, table_type, default_schema),
            )

    SetReplicationProgressAction(
        _make_table(cluster_name, table_path, YtTable.Type.TABLE, default_schema),
        _make_table(cluster_name, table_path, YtTable.Type.TABLE, default_schema),
    )


def test_double_schedule(src_table: YtTable, dst_table: YtTable):
    action = SetReplicationProgressAction(src_table, dst_table)
    yt_client_factory = _create_mock_factory(dst_table)
    yt_client = yt_client_factory(dst_table.cluster_name)
    assert not action.schedule_next(yt_client)
    with pytest.raises(AssertionError):
        action.schedule_next(yt_client)


def test_process_before_schedule(src_table: YtTable, dst_table: YtTable):
    action = SetReplicationProgressAction(src_table, dst_table)
    with pytest.raises(AssertionError):
        action.process()


def test_dst_not_exists(src_table: YtTable, dst_table: YtTable):
    dst_table.exists = False
    action = SetReplicationProgressAction(src_table, dst_table)
    yt_client_factory = _create_mock_factory(dst_table)
    yt_client = yt_client_factory(dst_table.cluster_name)
    with pytest.raises(AssertionError):
        action.schedule_next(yt_client)


def test_src_has_no_progress(src_table: YtTable, dst_table: YtTable):
    src_table.chaos_replication_progress = None
    action = SetReplicationProgressAction(src_table, dst_table)
    yt_client_factory = _create_mock_factory(dst_table)
    yt_client = yt_client_factory(dst_table.cluster_name)
    with pytest.raises(AssertionError):
        action.schedule_next(yt_client)


def test_succes(src_table: YtTable, dst_table: YtTable):
    assert src_table.chaos_replication_progress
    assert not dst_table.chaos_replication_progress

    action = SetReplicationProgressAction(src_table, dst_table)
    yt_client_factory = _create_mock_factory(dst_table)
    yt_client = yt_client_factory(dst_table.cluster_name)
    assert action.schedule_next(yt_client) is False
    action.process()

    assert dst_table.chaos_replication_progress == src_table.chaos_replication_progress

    # assert calls
    call_tracker = yt_client_factory.get_call_tracker(dst_table.cluster_name)
    assert call_tracker
    assert 1 == len(call_tracker.calls["alter_table"])
    call = call_tracker.calls["alter_table"][0]
    assert call.path_or_type == dst_table.path
    assert call.kwargs["replication_progress"] == _get_replication_progress()


def test_yt_error(src_table: YtTable, dst_table: YtTable):
    action = SetReplicationProgressAction(src_table, dst_table)
    yt_client_factory = _create_mock_factory(dst_table, error=True)
    yt_client = yt_client_factory(dst_table.cluster_name)
    assert action.schedule_next(yt_client) is False
    with pytest.raises(yt.YtResponseError):
        action.process()
