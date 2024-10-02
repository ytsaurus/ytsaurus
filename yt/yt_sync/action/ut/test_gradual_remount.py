import pytest

from yt.yt_sync.action import GradualRemountActon
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
def gradual_remount_action(yt_table: YtTable) -> GradualRemountActon:
    return GradualRemountActon(yt_table, 0)


@pytest.mark.parametrize("tablets_remounted", [0, 2])
def test_gradual_remount(
    gradual_remount_action: GradualRemountActon,
    yt_table: YtTable,
    tablets_remounted: int,
):
    tablet_count = 4
    yt_client_factory = MockYtClientFactory(
        {
            yt_table.cluster_name: {
                "get": {
                    f"{yt_table.path}/@tablet_count": MockResult(result=tablet_count, raw=True),
                    f"{yt_table.path}/@_tablets_remounted": MockResult(result=tablets_remounted, raw=True),
                },
                "set": {f"{yt_table.path}/@_tablets_remounted": MockResult(result=True)},
                "remount_table": {yt_table.path: MockResult(result=True)},
            }
        }
    )
    yt_client = yt_client_factory(yt_table.cluster_name)

    while gradual_remount_action.schedule_next(yt_client):
        gradual_remount_action.process()

    call_tracker = yt_client_factory.get_call_tracker(yt_table.cluster_name)
    assert call_tracker
    remount_calls = call_tracker.calls["remount_table"]
    assert (tablet_count - tablets_remounted) == len(remount_calls)
    for call in remount_calls:
        assert call.path_or_type == yt_table.path
