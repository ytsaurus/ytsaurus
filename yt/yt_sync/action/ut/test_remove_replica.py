import uuid

import pytest

import yt.wrapper as yt
from yt.yt_sync.action.remove_replica import RemoveReplicaAction
from yt.yt_sync.core.model import Types
from yt.yt_sync.core.model import YtReplica
from yt.yt_sync.core.model import YtTable
from yt.yt_sync.core.test_lib import MockResult
from yt.yt_sync.core.test_lib import MockYtClientFactory
from yt.yt_sync.core.test_lib import yt_generic_error

from .helpers import empty_mock_client
from .helpers import first_replica


@pytest.fixture
def replica_cluster_name() -> str:
    return "r0"


@pytest.fixture
def replica_for(replica_cluster_name: str, table_path: str, default_schema: Types.Schema) -> YtTable:
    return YtTable.make(
        table_path,
        replica_cluster_name,
        YtTable.Type.TABLE,
        table_path,
        True,
        {"dynamic": True, "schema": default_schema},
    )


@pytest.fixture
def replicated_table(table_path: str, default_schema: Types.Schema, replica_for: YtTable) -> YtTable:
    table = YtTable.make(
        table_path, "p", YtTable.Type.REPLICATED_TABLE, table_path, True, {"dynamic": True, "schema": default_schema}
    )
    replica = YtReplica(
        replica_id=str(uuid.uuid4()),
        cluster_name=replica_for.cluster_name,
        replica_path=replica_for.path,
        enabled=False,
        mode=YtReplica.Mode.SYNC,
    )
    table.add_replica(replica)
    return table


def test_replicated_is_not_replicated(replicated_table: YtTable, replica_for: YtTable):
    replicated_table.table_type = YtTable.Type.TABLE
    with pytest.raises(AssertionError):
        RemoveReplicaAction(replicated_table, replica_for)


def test_replica_for_is_replicated(replicated_table: YtTable, replica_for: YtTable):
    replica_for.table_type = YtTable.Type.REPLICATED_TABLE
    with pytest.raises(AssertionError):
        RemoveReplicaAction(replicated_table, replica_for)


def test_process_before_schedule(replicated_table: YtTable, replica_for: YtTable):
    with pytest.raises(AssertionError):
        RemoveReplicaAction(replicated_table, replica_for).process()


def test_no_replica_in_replicated(replicated_table: YtTable, replica_for: YtTable):
    replicated_table.replicas.clear()
    action = RemoveReplicaAction(replicated_table, replica_for)
    with pytest.raises(AssertionError):
        action.schedule_next(empty_mock_client())


def test_no_replica_id(replicated_table: YtTable, replica_for: YtTable):
    replica = first_replica(replicated_table)
    replica.replica_id = None
    action = RemoveReplicaAction(replicated_table, replica_for)
    with pytest.raises(AssertionError):
        action.schedule_next(empty_mock_client())


def test_replica_not_exists(replicated_table: YtTable, replica_for: YtTable):
    replica = first_replica(replicated_table)
    replica.exists = False
    action = RemoveReplicaAction(replicated_table, replica_for)
    with pytest.raises(AssertionError):
        action.schedule_next(empty_mock_client())


def test_replica_not_disabled(replicated_table: YtTable, replica_for: YtTable):
    replica = first_replica(replicated_table)
    replica.enabled = True
    action = RemoveReplicaAction(replicated_table, replica_for)
    with pytest.raises(AssertionError):
        action.schedule_next(empty_mock_client())


def test_success(replicated_table: YtTable, replica_for: YtTable):
    replica = first_replica(replicated_table)
    replica_id = f"#{replica.replica_id}"
    yt_client_factory = MockYtClientFactory(
        {replicated_table.cluster_name: {"remove": {replica_id: MockResult(result=True)}}}
    )
    yt_client = yt_client_factory(replicated_table.cluster_name)

    action = RemoveReplicaAction(replicated_table, replica_for)

    assert action.schedule_next(yt_client) is False

    _assert_calls(yt_client_factory, replicated_table)

    action.process()

    assert not replica.exists


def test_fail(replicated_table: YtTable, replica_for: YtTable):
    replica = first_replica(replicated_table)
    replica_id = f"#{replica.replica_id}"
    yt_client_factory = MockYtClientFactory(
        {replicated_table.cluster_name: {"remove": {replica_id: MockResult(error=yt_generic_error())}}}
    )
    yt_client = yt_client_factory(replicated_table.cluster_name)

    action = RemoveReplicaAction(replicated_table, replica_for)

    assert action.schedule_next(yt_client) is False

    _assert_calls(yt_client_factory, replicated_table)

    with pytest.raises(yt.YtResponseError):
        action.process()


def _assert_calls(yt_client_factory: MockYtClientFactory, replicated_table: YtTable):
    call_tracker = yt_client_factory.get_call_tracker(replicated_table.cluster_name)
    assert call_tracker
    remove_calls = call_tracker.calls["remove"]
    assert 1 == len(remove_calls)

    replica = first_replica(replicated_table)
    assert f"#{replica.replica_id}" == remove_calls[0].path_or_type
