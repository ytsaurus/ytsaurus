from yt.yt_sync.core.model import YtReplica
from yt.yt_sync.core.model import YtTable
from yt.yt_sync.core.test_lib import CallTracker
from yt.yt_sync.core.test_lib import MockYtClientFactory
from yt.yt_sync.core.test_lib import MockYtClientProxy


def empty_mock_client() -> MockYtClientProxy:
    return MockYtClientProxy("primary", {}, CallTracker())


def first_replica(table: YtTable) -> YtReplica:
    return next(iter(table.replicas.values()))


def simple_assert_calls(
    yt_client_factory: MockYtClientFactory,
    yt_table: YtTable,
    method: str,
    call_count: int = 1,
    check_path: str | None = None,
) -> list[CallTracker.Call]:
    call_tracker = yt_client_factory.get_call_tracker(yt_table.cluster_name)
    assert call_tracker

    calls = call_tracker.calls[method]
    assert call_count == len(calls)
    if check_path:
        for call in calls:
            assert check_path == call.path_or_type
    return calls


def simple_assert_no_calls(
    yt_client_factory: MockYtClientFactory,
    yt_table: YtTable,
    method: str,
):
    call_tracker = yt_client_factory.get_call_tracker(yt_table.cluster_name)
    assert call_tracker
    assert method not in call_tracker.calls
