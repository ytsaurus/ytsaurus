import uuid

import pytest

import yt.wrapper as yt
from yt.yt_sync.action.create_table import CreateTableAction
from yt.yt_sync.core.model import Types
from yt.yt_sync.core.model import YtReplica
from yt.yt_sync.core.model import YtSchema
from yt.yt_sync.core.model import YtTable
from yt.yt_sync.core.test_lib import CallTracker
from yt.yt_sync.core.test_lib import MockResult
from yt.yt_sync.core.test_lib import MockYtClientFactory
from yt.yt_sync.core.test_lib import yt_generic_error


@pytest.fixture
def table_attributes(default_schema: Types.Schema) -> Types.Attributes:
    return {"schema": default_schema}


@pytest.fixture
def yt_client_factory(table_path: str) -> MockYtClientFactory:
    return MockYtClientFactory({"remote0": {"create": {table_path: MockResult(result=True)}}})


def test_process_without_schedule(table_path: str, table_attributes: Types.Attributes):
    desired_table = YtTable.make("k", "primary", YtTable.Type.TABLE, table_path, True, table_attributes)
    actual_table = YtTable.make("k", "primary", YtTable.Type.TABLE, table_path, False, dict())
    with pytest.raises(AssertionError):
        CreateTableAction(desired_table, actual_table).process()


def test_create_table_fail(table_path: str, table_attributes: Types.Attributes):
    yt_client_factory = MockYtClientFactory({"primary": {"create": {table_path: MockResult(error=yt_generic_error())}}})
    desired_table = YtTable.make("k", "primary", YtTable.Type.TABLE, table_path, True, table_attributes)
    actual_table = YtTable.make("k", "primary", YtTable.Type.TABLE, table_path, False, dict())
    create_action = CreateTableAction(desired_table, actual_table)
    assert create_action.schedule_next(yt_client_factory("primary")) is False
    with pytest.raises(yt.YtResponseError):
        create_action.process()
    assert not actual_table.exists


@pytest.mark.parametrize("table_type", [YtTable.Type.TABLE, YtTable.Type.REPLICATION_LOG])
def test_create_table(
    table_path: str,
    table_type: str,
    table_attributes: Types.Attributes,
    yt_client_factory: MockYtClientFactory,
):
    desired_table = YtTable.make("k", "remote0", table_type, table_path, True, table_attributes)
    actual_table = YtTable.make("k", "remote0", YtTable.Type.CHAOS_REPLICATED_TABLE, table_path, False, dict())
    create_action = CreateTableAction(desired_table, actual_table)
    assert create_action.schedule_next(yt_client_factory("remote0")) is False

    _assert_create_call_and_get_attrs(
        yt_client_factory.get_call_tracker("remote0"), table_path, table_type, table_attributes
    )

    create_action.process()  # check no errors

    assert actual_table.exists
    assert actual_table.table_type == desired_table.table_type
    assert actual_table.schema == desired_table.schema
    assert actual_table.attributes == desired_table.attributes


def test_create_table_over_existing(
    table_path: str,
    table_attributes: Types.Attributes,
    yt_client_factory: MockYtClientFactory,
):
    desired_table = YtTable.make("k", "remote0", YtTable.Type.TABLE, table_path, True, table_attributes)
    actual_table = YtTable.make("k", "remote0", YtTable.Type.REPLICATION_LOG, table_path, True, table_attributes)
    create_action = CreateTableAction(desired_table, actual_table)
    with pytest.raises(AssertionError):
        create_action.schedule_next(yt_client_factory("remote0"))


@pytest.mark.parametrize("table_type", [YtTable.Type.TABLE, YtTable.Type.REPLICATION_LOG])
@pytest.mark.parametrize("replicated_table_type", [YtTable.Type.TABLE, YtTable.Type.REPLICATION_LOG])
def test_replicated_table_not_replicated(
    table_path: str,
    table_attributes: Types.Attributes,
    table_type: str,
    replicated_table_type: str,
    yt_client_factory: MockYtClientFactory,
):
    replicated = YtTable.make("k", "primary", replicated_table_type, table_path, True, table_attributes)
    desired_table = YtTable.make("k", "remote0", table_type, table_path, True, table_attributes)
    actual_table = YtTable.make("k", "remote0", table_type, table_path, False, dict())

    action = CreateTableAction(desired_table, actual_table, replicated)
    with pytest.raises(AssertionError):
        action.schedule_next(yt_client_factory("remote0"))


@pytest.mark.parametrize("table_type", [YtTable.Type.TABLE, YtTable.Type.REPLICATION_LOG])
@pytest.mark.parametrize("replicated_table_type", [YtTable.Type.REPLICATED_TABLE, YtTable.Type.CHAOS_REPLICATED_TABLE])
def test_with_upstream_replica(
    table_path: str,
    table_attributes: Types.Attributes,
    table_type: str,
    replicated_table_type: str,
    yt_client_factory: MockYtClientFactory,
):
    replicated = YtTable.make("k", "primary", replicated_table_type, table_path, True, table_attributes)
    replica_id = str(uuid.uuid4())
    replica = YtReplica(replica_id, "remote0", table_path, True, YtReplica.Mode.SYNC)
    replicated.add_replica(replica)

    desired_table = YtTable.make("k", "remote0", table_type, table_path, True, table_attributes)
    actual_table = YtTable.make("k", "remote0", table_type, table_path, False, dict())

    create_action = CreateTableAction(desired_table, actual_table, replicated)
    assert create_action.schedule_next(yt_client_factory("remote0")) is False

    attrs = _assert_create_call_and_get_attrs(
        yt_client_factory.get_call_tracker("remote0"), table_path, table_type, table_attributes
    )
    assert replica_id == attrs["upstream_replica_id"]

    create_action.process()  # check no errors
    assert actual_table.exists
    assert actual_table.table_type == desired_table.table_type
    assert actual_table.schema == desired_table.schema
    assert replica_id == actual_table.attributes["upstream_replica_id"]


def _assert_create_call_and_get_attrs(
    call_tracker: CallTracker | None, table_path: str, table_type: str, table_attributes: Types.Attributes
) -> Types.Attributes:
    assert call_tracker
    calls = call_tracker.calls["create"]
    assert 1 == len(calls)
    call = calls[0]
    assert table_path == call.path_or_type
    assert call.kwargs
    assert table_type == call.kwargs["type"]

    attrs = call.kwargs["attributes"]
    assert attrs["dynamic"] is True
    assert YtSchema.parse(table_attributes["schema"]).yt_schema == attrs["schema"]

    return attrs
