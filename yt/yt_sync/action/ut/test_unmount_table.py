import pytest

from yt.yt_sync.action.unmount_table import UnmountTableAction
from yt.yt_sync.core.client import MockResult
from yt.yt_sync.core.model import Types
from yt.yt_sync.core.model import YtTable
from yt.yt_sync.core.model import YtTabletState
from yt.yt_sync.core.test_lib import MockYtClientFactory


@pytest.fixture
def cluster_name() -> str:
    return "primary"


@pytest.fixture
def yt_table(cluster_name: str, table_path: str, default_schema: Types.Schema) -> YtTable:
    table_attrs = {"dynamic": True, "schema": default_schema}
    return YtTable.make("k", cluster_name, YtTable.Type.TABLE, table_path, True, table_attrs)


@pytest.fixture
def unmount_action(yt_table: YtTable) -> UnmountTableAction:
    return UnmountTableAction(yt_table)


@pytest.mark.parametrize("state", [YtTabletState.MOUNTED, YtTabletState.FROZEN])
def test_unmount_success(unmount_action: UnmountTableAction, yt_table: YtTable, state: str):
    yt_table.tablet_state.set(state)
    check_path = f"{yt_table.path}/@tablet_state"
    yt_client_factory = MockYtClientFactory(
        {
            yt_table.cluster_name: {
                "unmount_table": {yt_table.path: MockResult(result=True)},
                "get": {check_path: MockResult(result=YtTabletState.UNMOUNTED)},
            }
        }
    )
    yt_client = yt_client_factory(yt_table.cluster_name)

    assert unmount_action.schedule_next(yt_client) is True
    unmount_action.process()

    call_tracker = yt_client_factory.get_call_tracker(yt_table.cluster_name)
    assert call_tracker
    mount_calls = call_tracker.calls["unmount_table"]
    assert 1 == len(mount_calls)
    assert yt_table.path == mount_calls[0].path_or_type

    assert unmount_action.schedule_next(yt_client) is True
    unmount_action.process()

    get_calls = call_tracker.calls["get"]
    assert 1 == len(get_calls)
    assert check_path == get_calls[0].path_or_type

    assert unmount_action.schedule_next(yt_client) is False
    assert yt_table.tablet_state.is_unmounted


def test_unmount_not_ready(unmount_action: UnmountTableAction, yt_table: YtTable):
    check_path = f"{yt_table.path}/@tablet_state"
    yt_client_factory = MockYtClientFactory(
        {
            yt_table.cluster_name: {
                "unmount_table": {yt_table.path: MockResult(result=True)},
                "get": {check_path: MockResult(result=YtTabletState.MOUNTED)},
            }
        }
    )
    yt_client = yt_client_factory(yt_table.cluster_name)

    assert unmount_action.schedule_next(yt_client) is True
    unmount_action.process()

    call_count = 10
    for _ in range(call_count):
        assert unmount_action.schedule_next(yt_client) is True
        unmount_action.process()

    call_tracker = yt_client_factory.get_call_tracker(yt_table.cluster_name)
    assert call_tracker
    get_calls = call_tracker.calls["get"]
    assert call_count == len(get_calls)
    for call in get_calls:
        assert check_path == call.path_or_type
    assert not yt_table.tablet_state.is_unmounted


def test_already_unmounted(unmount_action: UnmountTableAction, yt_table: YtTable):
    yt_table.tablet_state.set(YtTabletState.UNMOUNTED)
    yt_client_factory = MockYtClientFactory({yt_table.cluster_name: {}})
    yt_client = yt_client_factory(yt_table.cluster_name)

    assert unmount_action.schedule_next(yt_client) is False
    unmount_action.process()

    call_tracker = yt_client_factory.get_call_tracker(yt_table.cluster_name)
    assert call_tracker
    assert "unmount_table" not in call_tracker.calls
    assert "get" not in call_tracker.calls


def test_unmount_chaos_replicated(unmount_action: UnmountTableAction, yt_table: YtTable):
    yt_table.table_type = YtTable.Type.CHAOS_REPLICATED_TABLE

    yt_client_factory = MockYtClientFactory({yt_table.cluster_name: {}})
    yt_client = yt_client_factory(yt_table.cluster_name)

    assert unmount_action.schedule_next(yt_client) is False
    unmount_action.process()

    call_tracker = yt_client_factory.get_call_tracker(yt_table.cluster_name)
    assert call_tracker
    assert "unmount_table" not in call_tracker.calls
    assert "get" not in call_tracker.calls
