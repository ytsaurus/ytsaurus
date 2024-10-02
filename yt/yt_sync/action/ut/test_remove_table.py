import pytest

import yt.wrapper as yt
from yt.yt_sync.action.remove_table import RemoveTableAction
from yt.yt_sync.core.model import Types
from yt.yt_sync.core.model import YtTable
from yt.yt_sync.core.test_lib import CallTracker
from yt.yt_sync.core.test_lib import MockResult
from yt.yt_sync.core.test_lib import MockYtClientFactory
from yt.yt_sync.core.test_lib import yt_generic_error


@pytest.fixture
def yt_client_factory(table_path: str) -> MockYtClientFactory:
    return MockYtClientFactory({"primary": {"remove": {table_path: MockResult(result=True)}}})


@pytest.fixture
def table_attributes(default_schema: Types.Schema) -> Types.Attributes:
    return {"schema": default_schema}


@pytest.fixture
def yt_table(table_path: str, table_attributes: Types.Attributes) -> YtTable:
    return YtTable.make("k", "primary", YtTable.Type.TABLE, table_path, True, table_attributes)


def test_remove_action_success(yt_table: YtTable, yt_client_factory: MockYtClientFactory):
    remove_action = RemoveTableAction(yt_table)
    assert remove_action.schedule_next(yt_client_factory("primary")) is False
    _assert_calls(yt_client_factory.get_call_tracker("primary"), yt_table.path)
    remove_action.process()
    assert not yt_table.exists


def test_remove_non_existing(yt_table: YtTable, yt_client_factory: MockYtClientFactory):
    yt_table.exists = False
    remove_action = RemoveTableAction(yt_table)
    assert remove_action.schedule_next(yt_client_factory("primary")) is False
    call_tracker = yt_client_factory.get_call_tracker("primary")
    assert call_tracker
    assert "remove" not in call_tracker.calls
    remove_action.process()
    assert not yt_table.exists


def test_remove_action_fail(yt_table: YtTable):
    yt_client_factory = MockYtClientFactory(
        {"primary": {"remove": {yt_table.path: MockResult(error=yt_generic_error())}}}
    )
    remove_action = RemoveTableAction(yt_table)
    assert remove_action.schedule_next(yt_client_factory("primary")) is False
    with pytest.raises(yt.YtResponseError):
        remove_action.process()
    assert yt_table.exists


def test_process_without_schedule(yt_table: YtTable):
    remove_action = RemoveTableAction(yt_table)
    with pytest.raises(AssertionError):
        remove_action.process()


def _assert_calls(call_tracker: CallTracker | None, table_path: str):
    assert call_tracker
    calls = call_tracker.calls["remove"]
    assert 1 == len(calls)
    assert table_path == calls[0].path_or_type
