from copy import deepcopy
import uuid

import pytest

import yt.wrapper as yt
from yt.yt_sync.action.switch_rtt import SwitchRttAction
from yt.yt_sync.core.model import Types
from yt.yt_sync.core.model import YtTable
from yt.yt_sync.core.test_lib import MockResult
from yt.yt_sync.core.test_lib import MockYtClientFactory
from yt.yt_sync.core.test_lib import yt_generic_error

from .helpers import empty_mock_client
from .helpers import simple_assert_calls
from .helpers import simple_assert_no_calls


@pytest.fixture(params=[YtTable.Type.REPLICATED_TABLE, YtTable.Type.CHAOS_REPLICATED_TABLE])
def actual_table(request, table_path: str, default_schema: Types.Schema) -> YtTable:
    table_type = str(request.param)
    table = YtTable.make(table_path, "p", table_type, table_path, True, {"schema": default_schema})
    table.chaos_replication_card_id = str(uuid.uuid4())
    return table


@pytest.fixture()
def desired_table(actual_table: YtTable) -> YtTable:
    return deepcopy(actual_table)


def test_actual_not_replicated(actual_table: YtTable, desired_table: YtTable):
    actual_table.table_type = YtTable.Type.TABLE
    with pytest.raises(AssertionError):
        SwitchRttAction(actual_table, desired_table, False)


def test_desired_not_replicated(actual_table: YtTable, desired_table: YtTable):
    desired_table.table_type = YtTable.Type.TABLE
    with pytest.raises(AssertionError):
        SwitchRttAction(actual_table, desired_table, False)


def test_process_before_schedule(actual_table: YtTable, desired_table: YtTable):
    action = SwitchRttAction(actual_table, desired_table, False)
    with pytest.raises(AssertionError):
        action.process()


def test_actual_not_exist(actual_table: YtTable, desired_table: YtTable):
    actual_table.exists = False
    action = SwitchRttAction(actual_table, desired_table, False)
    with pytest.raises(AssertionError):
        action.schedule_next(empty_mock_client())


def test_try_enable_with_desired_rtt_disabled(actual_table: YtTable, desired_table: YtTable):
    for table in (actual_table, desired_table):
        table.is_rtt_enabled = False
    action = SwitchRttAction(actual_table, desired_table, True)

    yt_client_factory = _yt_client_factory(actual_table)
    yt_client = yt_client_factory(actual_table.cluster_name)

    assert action.schedule_next(yt_client) is False
    _assert_no_calls(yt_client_factory, actual_table)
    action.process()

    assert actual_table.is_rtt_enabled is False


@pytest.mark.parametrize("enable", [True, False])
def test_already_desired_rtt_state(actual_table: YtTable, desired_table: YtTable, enable: bool):
    for table in (actual_table, desired_table):
        table.is_rtt_enabled = enable
    action = SwitchRttAction(actual_table, desired_table, enable)

    yt_client_factory = _yt_client_factory(actual_table)
    yt_client = yt_client_factory(actual_table.cluster_name)

    assert action.schedule_next(yt_client) is False
    _assert_no_calls(yt_client_factory, actual_table)
    action.process()

    assert actual_table.is_rtt_enabled is enable


@pytest.mark.parametrize("enable", [True, False])
def test_success(actual_table: YtTable, desired_table: YtTable, enable: bool):
    actual_table.is_rtt_enabled = not enable
    desired_table.is_rtt_enabled = enable

    action = SwitchRttAction(actual_table, desired_table, enable)

    yt_client_factory = _yt_client_factory(actual_table)
    yt_client = yt_client_factory(actual_table.cluster_name)

    assert action.schedule_next(yt_client) is False
    _assert_calls(yt_client_factory, actual_table, enable)
    action.process()

    assert actual_table.is_rtt_enabled is enable


@pytest.mark.parametrize("enable", [True, False])
def test_fail(actual_table: YtTable, desired_table: YtTable, enable: bool):
    actual_table.is_rtt_enabled = not enable
    desired_table.is_rtt_enabled = enable

    action = SwitchRttAction(actual_table, desired_table, enable)

    yt_client_factory = _yt_client_factory(actual_table, True)
    yt_client = yt_client_factory(actual_table.cluster_name)

    assert action.schedule_next(yt_client) is False
    _assert_calls(yt_client_factory, actual_table, enable)

    with pytest.raises(yt.YtResponseError):
        action.process()


def _assert_no_calls(yt_client_factory: MockYtClientFactory, table: YtTable):
    simple_assert_no_calls(yt_client_factory, table, _method(table))


def _assert_calls(yt_client_factory: MockYtClientFactory, table: YtTable, enable: bool):
    calls = simple_assert_calls(yt_client_factory, table, _method(table), 1, _check_path(table))
    call = calls[0]
    result = None
    if table.is_chaos_replicated:
        assert call.kwargs
        result = bool(call.kwargs["enable_replicated_table_tracker"])
    else:
        assert call.args
        result = bool(call.args[0])
    assert result is not None
    assert enable == result


def _check_path(table: YtTable) -> str:
    assert table.chaos_replication_card_id
    return (
        table.chaos_replication_card_id
        if table.is_chaos_replicated
        else f"{table.path}&/@replicated_table_options/enable_replicated_table_tracker"
    )


def _method(table: YtTable) -> str:
    return "alter_replication_card" if table.is_chaos_replicated else "set"


def _yt_client_factory(table: YtTable, with_error: bool = False) -> MockYtClientFactory:
    result = MockResult(error=yt_generic_error()) if with_error else MockResult(result=True)
    check_path = _check_path(table)
    if table.is_chaos_replicated:
        return MockYtClientFactory({table.cluster_name: {"alter_replication_card": {check_path: result}}})
    else:
        return MockYtClientFactory({table.cluster_name: {"set": {check_path: result}}})
