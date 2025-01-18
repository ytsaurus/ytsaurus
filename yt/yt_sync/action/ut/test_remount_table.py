import pytest

import yt.wrapper as yt
from yt.yt_sync.action.remount_table import RemountTableAction
from yt.yt_sync.core.model import Types
from yt.yt_sync.core.model import YtTable
from yt.yt_sync.core.test_lib import MockResult
from yt.yt_sync.core.test_lib import MockYtClientFactory
from yt.yt_sync.core.test_lib import yt_generic_error


@pytest.fixture
def cluster_name() -> str:
    return "primary"


@pytest.fixture
def yt_client_factory(cluster_name: str, table_path: str) -> MockYtClientFactory:
    return MockYtClientFactory({cluster_name: {"remount_table": {table_path: MockResult(result=True)}}})


@pytest.fixture
def table_attributes(default_schema: Types.Schema) -> Types.Attributes:
    return {"schema": default_schema}


@pytest.fixture
def yt_table(cluster_name: str, table_path: str, table_attributes: Types.Attributes) -> YtTable:
    return YtTable.make("k", cluster_name, YtTable.Type.TABLE, table_path, True, table_attributes)


@pytest.fixture
def remount_action(yt_table: YtTable) -> RemountTableAction:
    return RemountTableAction(yt_table)


def test_process_before_schedule(remount_action: RemountTableAction):
    with pytest.raises(AssertionError):
        remount_action.process()


def test_schedule_twice(cluster_name: str, yt_client_factory: MockYtClientFactory, remount_action: RemountTableAction):
    yt_client = yt_client_factory(cluster_name)
    remount_action.schedule_next(yt_client)
    with pytest.raises(AssertionError):
        remount_action.schedule_next(yt_client)


def test_remount_unexisting(
    cluster_name: str, yt_client_factory: MockYtClientFactory, yt_table: YtTable, remount_action: RemountTableAction
):
    yt_table.exists = False
    yt_client = yt_client_factory(cluster_name)
    with pytest.raises(AssertionError):
        remount_action.schedule_next(yt_client)


@pytest.mark.parametrize("need_remount", [True, False, None])
def test_remount(
    cluster_name: str,
    yt_client_factory: MockYtClientFactory,
    remount_action: RemountTableAction,
    yt_table: YtTable,
    need_remount: bool | None,
):
    yt_table.need_remount = need_remount
    yt_client = yt_client_factory(cluster_name)
    assert remount_action.schedule_next(yt_client) is False

    call_tracker = yt_client_factory.get_call_tracker(cluster_name)
    assert call_tracker
    if need_remount in (True, None):
        assert "remount_table" in call_tracker.calls
    else:
        assert "remount_table" not in call_tracker.calls

    remount_action.process()
    assert yt_table.need_remount is None


def test_remount_fail(cluster_name: str, table_path: str, remount_action: RemountTableAction):
    yt_client_factory = MockYtClientFactory(
        {cluster_name: {"remount_table": {table_path: MockResult(error=yt_generic_error())}}}
    )
    yt_client = yt_client_factory(cluster_name)
    assert remount_action.schedule_next(yt_client) is False

    call_tracker = yt_client_factory.get_call_tracker(cluster_name)
    assert call_tracker
    assert "remount_table" in call_tracker.calls

    with pytest.raises(yt.YtResponseError):
        remount_action.process()
