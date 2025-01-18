import pytest

import yt.wrapper as yt
from yt.yt_sync.action.wait_in_memory_preload import WaitInMemoryPreloadAction
from yt.yt_sync.core.model import Types
from yt.yt_sync.core.model import YtTable
from yt.yt_sync.core.test_lib import MockResult
from yt.yt_sync.core.test_lib import MockYtClientFactory
from yt.yt_sync.core.test_lib import yt_generic_error
from yt.yt_sync.core.test_lib import yt_retryable_error

from .helpers import simple_assert_calls


@pytest.fixture
def cluster_name() -> str:
    return "primary"


@pytest.fixture
def yt_table(cluster_name: str, table_path: str, default_schema: Types.Schema) -> YtTable:
    table_attrs = {"dynamic": True, "schema": default_schema, "in_memory_mode": "uncompressed"}
    return YtTable.make("k", cluster_name, YtTable.Type.TABLE, table_path, True, table_attrs)


@pytest.fixture
def wait_action(yt_table: YtTable) -> WaitInMemoryPreloadAction:
    return WaitInMemoryPreloadAction(yt_table)


def test_process_before_schedule(wait_action: WaitInMemoryPreloadAction):
    with pytest.raises(AssertionError):
        wait_action.process()


def test_non_existing_table(wait_action: WaitInMemoryPreloadAction, yt_table: YtTable):
    yt_table.exists = False

    yt_client_factory = MockYtClientFactory({yt_table.cluster_name: {}})
    yt_client = yt_client_factory(yt_table.cluster_name)

    with pytest.raises(AssertionError):
        wait_action.schedule_next(yt_client)


def test_not_in_memory_table(wait_action: WaitInMemoryPreloadAction, yt_table: YtTable):
    yt_table.attributes["in_memory_mode"] = "none"

    yt_client_factory = MockYtClientFactory({yt_table.cluster_name: {}})
    yt_client = yt_client_factory(yt_table.cluster_name)

    assert wait_action.schedule_next(yt_client) is False
    wait_action.process()

    call_tracker = yt_client_factory.get_call_tracker(yt_table.cluster_name)
    assert call_tracker
    assert not call_tracker.calls


def test_request_error(wait_action: WaitInMemoryPreloadAction, yt_table: YtTable):
    check_path = f"{yt_table.path}/@preload_state"
    yt_client_factory = MockYtClientFactory(
        {
            yt_table.cluster_name: {
                "get": {check_path: MockResult(error=yt_generic_error())},
            }
        }
    )
    yt_client = yt_client_factory(yt_table.cluster_name)
    assert wait_action.schedule_next(yt_client) is True

    with pytest.raises(yt.YtResponseError):
        wait_action.process()

    simple_assert_calls(yt_client_factory, yt_table, "get", 1, check_path)


def test_not_preloaded(wait_action: WaitInMemoryPreloadAction, yt_table: YtTable):
    check_path = f"{yt_table.path}/@preload_state"
    yt_client_factory = MockYtClientFactory(
        {
            yt_table.cluster_name: {
                "get": {check_path: MockResult(result="loading")},
            }
        }
    )
    yt_client = yt_client_factory(yt_table.cluster_name)

    call_count = 10
    for _ in range(call_count):
        assert wait_action.schedule_next(yt_client) is True
        wait_action.process()

    simple_assert_calls(yt_client_factory, yt_table, "get", call_count, check_path)


def test_retryable_error(wait_action: WaitInMemoryPreloadAction, yt_table: YtTable):
    check_path = f"{yt_table.path}/@preload_state"
    yt_client_factory = MockYtClientFactory(
        {
            yt_table.cluster_name: {
                "get": {check_path: MockResult(error=yt_retryable_error())},
            }
        }
    )
    yt_client = yt_client_factory(yt_table.cluster_name)

    call_count = 10
    for _ in range(call_count):
        assert wait_action.schedule_next(yt_client) is True
        wait_action.process()

    simple_assert_calls(yt_client_factory, yt_table, "get", call_count, check_path)


def test_preloaded(wait_action: WaitInMemoryPreloadAction, yt_table: YtTable):
    check_path = f"{yt_table.path}/@preload_state"
    yt_client_factory = MockYtClientFactory(
        {
            yt_table.cluster_name: {
                "get": {check_path: MockResult(result="complete")},
            }
        }
    )
    yt_client = yt_client_factory(yt_table.cluster_name)

    assert wait_action.schedule_next(yt_client) is True
    wait_action.process()
    assert wait_action.schedule_next(yt_client) is False

    simple_assert_calls(yt_client_factory, yt_table, "get", 1, check_path)


# def _assert_calls(check_path: str, call_count: int, call_tracker: CallTracker | None):
#     assert call_tracker
#     get_calls = call_tracker.calls["get"]
#     assert call_count == len(get_calls)
#     for call in get_calls:
#         assert check_path == call.path_or_type
