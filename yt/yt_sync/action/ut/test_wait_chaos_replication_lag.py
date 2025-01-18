import uuid

import pytest

import yt.wrapper as yt
from yt.yt_sync.action.wait_chaos_replication_lag import WaitChaosReplicationLagAction
from yt.yt_sync.core.model import Types
from yt.yt_sync.core.model import YtReplica
from yt.yt_sync.core.model import YtTable
from yt.yt_sync.core.test_lib import MockResult
from yt.yt_sync.core.test_lib import MockYtClientFactory
from yt.yt_sync.core.test_lib import yt_generic_error

from .helpers import empty_mock_client
from .helpers import first_replica
from .helpers import simple_assert_calls


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
        table_path,
        "p",
        YtTable.Type.CHAOS_REPLICATED_TABLE,
        table_path,
        True,
        {"dynamic": True, "schema": default_schema},
    )
    replica = YtReplica(
        replica_id=str(uuid.uuid4()),
        cluster_name=replica_for.cluster_name,
        replica_path=replica_for.path,
        enabled=False,
        mode=YtReplica.Mode.SYNC,
        content_type=YtReplica.ContentType.DATA,
    )
    table.add_replica(replica)
    return table


@pytest.mark.parametrize("table_type", [YtTable.Type.REPLICATED_TABLE, YtTable.Type.TABLE])
def test_replicated_is_not_replicated(replicated_table: YtTable, replica_for: YtTable, table_type: str):
    replicated_table.table_type = table_type
    action = WaitChaosReplicationLagAction(replicated_table, replica_for)
    with pytest.raises(AssertionError):
        action.schedule_next(empty_mock_client())


def test_replica_for_is_replicated(replicated_table: YtTable, replica_for: YtTable):
    replica_for.table_type = YtTable.Type.CHAOS_REPLICATED_TABLE
    action = WaitChaosReplicationLagAction(replicated_table, replica_for)
    with pytest.raises(AssertionError):
        action.schedule_next(empty_mock_client())


def test_process_before_schedule(replicated_table: YtTable, replica_for: YtTable):
    action = WaitChaosReplicationLagAction(replicated_table, replica_for)
    with pytest.raises(AssertionError):
        action.process()


def test_no_replica(replicated_table: YtTable, replica_for: YtTable):
    replicated_table.replicas.clear()
    action = WaitChaosReplicationLagAction(replicated_table, replica_for)
    with pytest.raises(AssertionError):
        action.schedule_next(empty_mock_client())


def test_no_replica_id(replicated_table: YtTable, replica_for: YtTable):
    replica = first_replica(replicated_table)
    replica.replica_id = None
    action = WaitChaosReplicationLagAction(replicated_table, replica_for)
    with pytest.raises(AssertionError):
        action.schedule_next(empty_mock_client())


def test_replica_not_exists(replicated_table: YtTable, replica_for: YtTable):
    replica = first_replica(replicated_table)
    replica.exists = False
    action = WaitChaosReplicationLagAction(replicated_table, replica_for)
    with pytest.raises(AssertionError):
        action.schedule_next(empty_mock_client())


@pytest.mark.parametrize("content_type", [None, YtReplica.ContentType.QUEUE])
def test_replica_bad_content_type(replicated_table: YtTable, replica_for: YtTable, content_type: str | None):
    replica = first_replica(replicated_table)
    replica.content_type = content_type
    action = WaitChaosReplicationLagAction(replicated_table, replica_for)
    with pytest.raises(AssertionError):
        action.schedule_next(empty_mock_client())


def test_lag_not_replicated(replicated_table: YtTable, replica_for: YtTable):
    check_path = _check_path(replicated_table)
    yt_client_factory = MockYtClientFactory(
        {
            replicated_table.cluster_name: {
                "generate_timestamp": {"*": MockResult(result=2, raw=True)},
                "get": {check_path: MockResult(result=1)},
            }
        }
    )
    yt_client = yt_client_factory(replicated_table.cluster_name)

    action = WaitChaosReplicationLagAction(replicated_table, replica_for)

    call_count = 10
    for _ in range(call_count):
        assert action.schedule_next(yt_client) is True
        action.process()

    simple_assert_calls(yt_client_factory, replicated_table, "generate_timestamp", 1)
    simple_assert_calls(yt_client_factory, replicated_table, "get", call_count, check_path)


def test_failed(replicated_table: YtTable, replica_for: YtTable):
    check_path = _check_path(replicated_table)
    yt_client_factory = MockYtClientFactory(
        {
            replicated_table.cluster_name: {
                "generate_timestamp": {"*": MockResult(result=2, raw=True)},
                "get": {check_path: MockResult(error=yt_generic_error())},
            }
        }
    )
    yt_client = yt_client_factory(replicated_table.cluster_name)

    action = WaitChaosReplicationLagAction(replicated_table, replica_for)

    assert action.schedule_next(yt_client) is True

    simple_assert_calls(yt_client_factory, replicated_table, "generate_timestamp", 1)
    simple_assert_calls(yt_client_factory, replicated_table, "get", 1, check_path)

    with pytest.raises(yt.YtResponseError):
        action.process()


def test_success(replicated_table: YtTable, replica_for: YtTable):
    check_path = _check_path(replicated_table)
    yt_client_factory = MockYtClientFactory(
        {
            replicated_table.cluster_name: {
                "generate_timestamp": {"*": MockResult(result=2, raw=True)},
                "get": {check_path: MockResult(result=2)},
            }
        }
    )
    yt_client = yt_client_factory(replicated_table.cluster_name)

    action = WaitChaosReplicationLagAction(replicated_table, replica_for)

    assert action.schedule_next(yt_client) is True
    action.process()
    assert action.schedule_next(yt_client) is False
    action.process()

    simple_assert_calls(yt_client_factory, replicated_table, "generate_timestamp", 1)
    simple_assert_calls(yt_client_factory, replicated_table, "get", 1, check_path)


def _check_path(replicated_table: YtTable) -> str:
    replica = first_replica(replicated_table)
    return f"{replicated_table.path}/@replicas/{replica.replica_id}/replication_lag_timestamp"
