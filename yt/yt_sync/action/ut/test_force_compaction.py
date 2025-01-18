import pytest

from yt.yt_sync.action import CleanupForceCompactionAttributesAction
from yt.yt_sync.action import SetForceCompactionRevisionAction
from yt.yt_sync.core.client import MockResult
from yt.yt_sync.core.model import Types
from yt.yt_sync.core.model import YtTable
from yt.yt_sync.core.test_lib import MockYtClientFactory


@pytest.fixture
def cluster_name() -> str:
    return "primary"


@pytest.fixture
def yt_table(cluster_name: str, table_path: str, default_schema: Types.Schema) -> YtTable:
    table_attrs = {"dynamic": True, "schema": default_schema}
    table = YtTable.make("k", cluster_name, YtTable.Type.TABLE, table_path, True, table_attrs)
    return table


@pytest.fixture
def set_force_compaction_revision_action(yt_table: YtTable) -> SetForceCompactionRevisionAction:
    return SetForceCompactionRevisionAction(yt_table, 1)


@pytest.fixture
def cleanup_after_force_compaction_action(yt_table: YtTable) -> CleanupForceCompactionAttributesAction:
    return CleanupForceCompactionAttributesAction(yt_table)


def test_set_forced_compaction_revision(
    set_force_compaction_revision_action: SetForceCompactionRevisionAction, yt_table: YtTable
):
    check_path = f"{yt_table.path}/@forced_compaction_revision"
    yt_client_factory = MockYtClientFactory(
        {
            yt_table.cluster_name: {
                "set": {check_path: MockResult(result=True)},
            }
        }
    )
    yt_client = yt_client_factory(yt_table.cluster_name)

    assert set_force_compaction_revision_action.schedule_next(yt_client)
    set_force_compaction_revision_action.process()
    assert not set_force_compaction_revision_action.schedule_next(yt_client)

    call_tracker = yt_client_factory.get_call_tracker(yt_table.cluster_name)
    assert call_tracker
    set_calls = call_tracker.calls["set"]
    assert 1 == len(set_calls)
    assert check_path == set_calls[0].path_or_type


def test_cleanup_after_force_compaction(
    cleanup_after_force_compaction_action: CleanupForceCompactionAttributesAction, yt_table: YtTable
):
    attributes_to_remove = [
        f"{yt_table.path}/@forced_compaction_revision",
        f"{yt_table.path}/@_tablets_remounted",
        f"{yt_table.path}/@_sleep_after_tablet",
    ]
    yt_client_factory = MockYtClientFactory(
        {
            yt_table.cluster_name: {
                "remove": {attribute: MockResult(result=True) for attribute in attributes_to_remove},
            }
        }
    )
    yt_client = yt_client_factory(yt_table.cluster_name)

    assert cleanup_after_force_compaction_action.schedule_next(yt_client)
    cleanup_after_force_compaction_action.process()
    assert not cleanup_after_force_compaction_action.schedule_next(yt_client)

    call_tracker = yt_client_factory.get_call_tracker(yt_table.cluster_name)
    assert call_tracker
    remove_calls = call_tracker.calls["remove"]
    assert 3 == len(remove_calls)
    assert set(attributes_to_remove) == set(call.path_or_type for call in remove_calls)
