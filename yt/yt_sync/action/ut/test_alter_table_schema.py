from copy import deepcopy

import pytest

import yt.wrapper as yt
from yt.yt_sync.action.alter_table_schema import AlterTableSchemaAction
from yt.yt_sync.core.model import Types
from yt.yt_sync.core.model import YtColumn
from yt.yt_sync.core.model import YtTable
from yt.yt_sync.core.spec import Column
from yt.yt_sync.core.spec import TypeV1
from yt.yt_sync.core.test_lib import MockResult
from yt.yt_sync.core.test_lib import MockYtClientFactory
from yt.yt_sync.core.test_lib import yt_generic_error


@pytest.fixture
def cluster_name() -> str:
    return "primary"


@pytest.fixture
def desired_table(cluster_name: str, table_path: str, default_schema: Types.Schema) -> YtTable:
    return YtTable.make("k", cluster_name, YtTable.Type.TABLE, table_path, True, {"schema": default_schema})


@pytest.fixture
def actual_table(desired_table: YtTable) -> YtTable:
    return deepcopy(desired_table)


def test_different_cluster(desired_table: YtTable, actual_table: YtTable):
    actual_table.cluster_name = "_other_"
    with pytest.raises(AssertionError):
        AlterTableSchemaAction(desired_table, actual_table)


def test_different_path(desired_table: YtTable, actual_table: YtTable):
    actual_table.path = "_other_"
    with pytest.raises(AssertionError):
        AlterTableSchemaAction(desired_table, actual_table)


def test_not_exists(desired_table: YtTable, actual_table: YtTable):
    actual_table.exists = False
    with pytest.raises(AssertionError):
        AlterTableSchemaAction(desired_table, actual_table)


def test_process_before_schedule(desired_table: YtTable, actual_table: YtTable):
    action = AlterTableSchemaAction(desired_table, actual_table)
    with pytest.raises(AssertionError):
        action.process()


def test_same_schema(desired_table: YtTable, actual_table: YtTable):
    action = AlterTableSchemaAction(desired_table, actual_table)
    yt_client_factory = MockYtClientFactory({actual_table.cluster_name: {}})
    yt_client = yt_client_factory(actual_table.cluster_name)
    assert action.schedule_next(yt_client) is False

    call_tracker = yt_client_factory.get_call_tracker(actual_table.cluster_name)
    assert call_tracker
    assert "alter_table" not in call_tracker.calls


def test_schedule_twice(desired_table: YtTable, actual_table: YtTable):
    desired_table.schema.columns.append(YtColumn.make(Column(name="_c", type=TypeV1.UINT64)))
    action = AlterTableSchemaAction(desired_table, actual_table)
    yt_client_factory = MockYtClientFactory(
        {
            actual_table.cluster_name: {
                "alter_table": {actual_table.path: MockResult(result=True)},
            }
        }
    )
    yt_client = yt_client_factory(actual_table.cluster_name)
    assert action.schedule_next(yt_client) is False
    _assert_call(yt_client_factory, desired_table)
    with pytest.raises(AssertionError):
        action.schedule_next(yt_client)


def test_fail(desired_table: YtTable, actual_table: YtTable):
    desired_table.schema.columns.append(YtColumn.make(Column(name="_c", type=TypeV1.UINT64)))
    action = AlterTableSchemaAction(desired_table, actual_table)
    yt_client_factory = MockYtClientFactory(
        {
            actual_table.cluster_name: {
                "alter_table": {actual_table.path: MockResult(error=yt_generic_error())},
            }
        }
    )
    yt_client = yt_client_factory(actual_table.cluster_name)
    assert action.schedule_next(yt_client) is False
    _assert_call(yt_client_factory, desired_table)

    with pytest.raises(yt.YtResponseError):
        action.process()


def test_success(desired_table: YtTable, actual_table: YtTable):
    desired_table.schema.columns.append(YtColumn.make(Column(name="_c", type=TypeV1.UINT64)))
    action = AlterTableSchemaAction(desired_table, actual_table)
    yt_client_factory = MockYtClientFactory(
        {
            actual_table.cluster_name: {
                "alter_table": {actual_table.path: MockResult(result=True)},
            }
        }
    )
    yt_client = yt_client_factory(actual_table.cluster_name)
    assert action.schedule_next(yt_client) is False
    _assert_call(yt_client_factory, desired_table)
    action.process()


def _assert_call(yt_client_factory: MockYtClientFactory, yt_table: YtTable):
    call_tracker = yt_client_factory.get_call_tracker(yt_table.cluster_name)
    assert call_tracker
    assert 1 == len(call_tracker.calls["alter_table"])
    call = call_tracker.calls["alter_table"][0]
    assert yt_table.path == call.path_or_type
    assert call.kwargs
    assert yt_table.schema.yt_schema == call.kwargs["schema"]
