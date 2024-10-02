import uuid

import pytest

import yt.wrapper as yt
from yt.yt_sync.action.create_replica import CreateReplicaAction
from yt.yt_sync.core.model import Types
from yt.yt_sync.core.model import YtReplica
from yt.yt_sync.core.model import YtTable
from yt.yt_sync.core.model.helpers import make_tmp_name
from yt.yt_sync.core.test_lib import MockResult
from yt.yt_sync.core.test_lib import MockYtClientFactory
from yt.yt_sync.core.test_lib import yt_generic_error

from .helpers import empty_mock_client


@pytest.fixture
def replica_cluster_name() -> str:
    return "r0"


@pytest.fixture
def desired_replica(replica_cluster_name: str, table_path: str) -> YtReplica:
    return YtReplica(
        replica_id=str(uuid.uuid4),
        cluster_name=replica_cluster_name,
        replica_path=table_path,
        enabled=True,
        mode=YtReplica.Mode.ASYNC,
    )


@pytest.fixture
def replicated_table(table_path: str, default_schema: Types.Schema) -> YtTable:
    return YtTable.make(
        table_path, "p", YtTable.Type.REPLICATED_TABLE, table_path, True, {"dynamic": True, "schema": default_schema}
    )


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


def test_replicated_not_replicated(replicated_table: YtTable, desired_replica: YtReplica, replica_for: YtTable):
    replicated_table.table_type = YtTable.Type.TABLE
    with pytest.raises(AssertionError):
        CreateReplicaAction(replicated_table, desired_replica, replica_for)


def test_replica_for_is_replicated(replicated_table: YtTable, desired_replica: YtReplica, replica_for: YtTable):
    replica_for.table_type = YtTable.Type.REPLICATED_TABLE
    with pytest.raises(AssertionError):
        CreateReplicaAction(replicated_table, desired_replica, replica_for)


def test_process_before_schedule(replicated_table: YtTable, desired_replica: YtReplica, replica_for: YtTable):
    with pytest.raises(AssertionError):
        CreateReplicaAction(replicated_table, desired_replica, replica_for).process()


def test_replica_already_exists(replicated_table: YtTable, desired_replica: YtReplica, replica_for: YtTable):
    replicated_table.add_replica(desired_replica)
    action = CreateReplicaAction(replicated_table, desired_replica, replica_for)
    with pytest.raises(AssertionError):
        action.schedule_next(empty_mock_client())


@pytest.mark.parametrize("temporary", [True, False])
@pytest.mark.parametrize("rtt_enabled", [True, False])
def test_success(
    replicated_table: YtTable, desired_replica: YtReplica, replica_for: YtTable, temporary: bool, rtt_enabled: bool
):
    replica_for.is_temporary = temporary
    if temporary:
        replica_for.path = make_tmp_name(replica_for.path)
    desired_replica.enable_replicated_table_tracker = rtt_enabled
    replica_id = str(uuid.uuid4())
    yt_client_factory = MockYtClientFactory(
        {replicated_table.cluster_name: {"create": {replicated_table.replica_type: MockResult(result=replica_id)}}}
    )
    yt_client = yt_client_factory(replicated_table.cluster_name)

    action = CreateReplicaAction(
        replicated_table, desired_replica, replica_for, {"mode": YtReplica.Mode.SYNC, "my_attr": "1"}
    )
    assert action.schedule_next(yt_client) is False

    _assert_calls(yt_client_factory, replicated_table)

    action.process()

    replica = replicated_table.replicas.get(replica_for.replica_key)
    assert replica
    assert replica_id == replica.replica_id
    assert YtReplica.Mode.SYNC == replica.mode
    assert "1" == replica.attributes["my_attr"]
    assert temporary == replica.is_temporary
    assert replica_for.path == replica.replica_path
    assert rtt_enabled == replica.enable_replicated_table_tracker


def test_fail(replicated_table: YtTable, desired_replica: YtReplica, replica_for: YtTable):
    yt_client_factory = MockYtClientFactory(
        {
            replicated_table.cluster_name: {
                "create": {replicated_table.replica_type: MockResult(error=yt_generic_error())}
            }
        }
    )
    yt_client = yt_client_factory(replicated_table.cluster_name)

    action = CreateReplicaAction(
        replicated_table, desired_replica, replica_for, {"mode": YtReplica.Mode.SYNC, "my_attr": "1"}
    )
    assert action.schedule_next(yt_client) is False

    _assert_calls(yt_client_factory, replicated_table)

    with pytest.raises(yt.YtResponseError):
        action.process()


def _assert_calls(yt_client_factory: MockYtClientFactory, replicated_table: YtTable):
    call_tracker = yt_client_factory.get_call_tracker(replicated_table.cluster_name)
    assert call_tracker
    create_calls = call_tracker.calls["create"]
    assert 1 == len(create_calls)
    assert replicated_table.replica_type == create_calls[0].path_or_type
