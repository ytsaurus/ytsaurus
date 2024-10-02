import pytest

from yt.yt_sync.action.freeze_table import FreezeTableAction
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
    table = YtTable.make("k", cluster_name, YtTable.Type.TABLE, table_path, True, table_attrs)
    table.tablet_state.set(YtTabletState.MOUNTED)
    return table


@pytest.fixture
def freeze_action(yt_table: YtTable) -> FreezeTableAction:
    return FreezeTableAction(yt_table)


def test_freeze_success(freeze_action: FreezeTableAction, yt_table: YtTable):
    check_path = f"{yt_table.path}/@tablet_state"
    yt_client_factory = MockYtClientFactory(
        {
            yt_table.cluster_name: {
                "freeze_table": {yt_table.path: MockResult(result=True)},
                "get": {check_path: MockResult(result=YtTabletState.FROZEN)},
            }
        }
    )
    yt_client = yt_client_factory(yt_table.cluster_name)

    assert freeze_action.schedule_next(yt_client) is True
    freeze_action.process()

    call_tracker = yt_client_factory.get_call_tracker(yt_table.cluster_name)
    assert call_tracker
    mount_calls = call_tracker.calls["freeze_table"]
    assert 1 == len(mount_calls)
    assert yt_table.path == mount_calls[0].path_or_type

    assert freeze_action.schedule_next(yt_client) is True
    freeze_action.process()

    get_calls = call_tracker.calls["get"]
    assert 1 == len(get_calls)
    assert check_path == get_calls[0].path_or_type

    assert freeze_action.schedule_next(yt_client) is False
    assert yt_table.tablet_state.is_frozen


def test_freeze_not_ready(freeze_action: FreezeTableAction, yt_table: YtTable):
    check_path = f"{yt_table.path}/@tablet_state"
    yt_client_factory = MockYtClientFactory(
        {
            yt_table.cluster_name: {
                "freeze_table": {yt_table.path: MockResult(result=True)},
                "get": {check_path: MockResult(result=YtTabletState.MOUNTED)},
            }
        }
    )
    yt_client = yt_client_factory(yt_table.cluster_name)

    assert freeze_action.schedule_next(yt_client) is True
    freeze_action.process()

    call_count = 10
    for _ in range(call_count):
        assert freeze_action.schedule_next(yt_client) is True
        freeze_action.process()

    call_tracker = yt_client_factory.get_call_tracker(yt_table.cluster_name)
    assert call_tracker
    get_calls = call_tracker.calls["get"]
    assert call_count == len(get_calls)
    for call in get_calls:
        assert check_path == call.path_or_type
    assert not yt_table.tablet_state.is_frozen


@pytest.mark.parametrize("state", [YtTabletState.FROZEN, YtTabletState.UNMOUNTED])
def test_not_mounted(freeze_action: FreezeTableAction, yt_table: YtTable, state: str):
    yt_table.tablet_state.set(state)
    yt_client_factory = MockYtClientFactory({yt_table.cluster_name: {}})
    yt_client = yt_client_factory(yt_table.cluster_name)

    assert freeze_action.schedule_next(yt_client) is False
    freeze_action.process()

    call_tracker = yt_client_factory.get_call_tracker(yt_table.cluster_name)
    assert call_tracker
    assert "freeze_table" not in call_tracker.calls
    assert "get" not in call_tracker.calls
