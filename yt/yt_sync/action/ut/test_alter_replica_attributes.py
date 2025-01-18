from copy import deepcopy
import uuid

import pytest

import yt.wrapper as yt
from yt.yt_sync.action.alter_replica_attributes import AlterReplicaAttributesAction
from yt.yt_sync.core.model import Types
from yt.yt_sync.core.model import YtReplica
from yt.yt_sync.core.model import YtTable
from yt.yt_sync.core.test_lib import MockResult
from yt.yt_sync.core.test_lib import MockYtClientFactory
from yt.yt_sync.core.test_lib import yt_generic_error


def _make_replica(default_replica_attrs: Types.Attributes, patch: Types.Attributes | None = None) -> YtReplica:
    replica_attrs: Types.Attributes = deepcopy(default_replica_attrs)
    if patch:
        replica_attrs.update(patch)
    return YtReplica.make(str(uuid.uuid4()), replica_attrs)


def _create_mock_factory(desired: YtReplica, actual_replicated: YtTable, error: bool = False) -> MockYtClientFactory:
    replica = actual_replicated.replicas[desired.key]
    result = MockResult(error=yt_generic_error()) if error else MockResult(result=True)
    return MockYtClientFactory({replica.cluster_name: {"alter_table_replica": {replica.replica_id: result}}})


@pytest.fixture
def primary_cluster() -> str:
    return "primary"


@pytest.fixture
def cluster() -> str:
    return "remote0"


@pytest.fixture
def replica_attrs(cluster: str, table_path: str) -> Types.Attributes:
    return {
        "cluster_name": cluster,
        "replica_path": table_path,
        "state": YtReplica.State.ENABLED,
        "mode": YtReplica.Mode.SYNC,
    }


@pytest.fixture
def desired(replica_attrs: Types.Attributes) -> YtReplica:
    return _make_replica(replica_attrs)


@pytest.fixture
def actual_replicated(
    replica_attrs: Types.Attributes, primary_cluster: str, cluster: str, table_path: str, default_schema: Types.Schema
) -> YtTable:
    table = YtTable.make(
        table_path,
        primary_cluster,
        YtTable.Type.REPLICATED_TABLE,
        table_path,
        exists=True,
        attributes={"schema": default_schema},
    )
    table.add_replica(_make_replica(replica_attrs))

    return table


def test_ctor(desired: YtReplica, actual_replicated: YtTable):
    AlterReplicaAttributesAction(desired, actual_replicated)

    # Non-replicated
    actual_replicated.table_type = YtTable.Type.TABLE
    with pytest.raises(AssertionError):
        AlterReplicaAttributesAction(desired, actual_replicated)


def test_process_before_schedule(desired: YtReplica, actual_replicated: YtTable):
    action = AlterReplicaAttributesAction(desired, actual_replicated)
    with pytest.raises(AssertionError):
        action.process()


def test_schedule_twice(desired: YtReplica, actual_replicated: YtTable):
    yt_client_factory = _create_mock_factory(desired, actual_replicated)
    yt_client = yt_client_factory(desired.cluster_name)
    action = AlterReplicaAttributesAction(desired, actual_replicated)
    action.schedule_next(yt_client)
    with pytest.raises(AssertionError):
        action.schedule_next(yt_client)


def test_yt_error(desired: YtReplica, actual_replicated: YtTable):
    desired.enable_replicated_table_tracker = False
    yt_client_factory = _create_mock_factory(desired, actual_replicated, error=True)
    yt_client = yt_client_factory(desired.cluster_name)

    action = AlterReplicaAttributesAction(desired, actual_replicated)
    assert action.schedule_next(yt_client) is False
    with pytest.raises(yt.YtResponseError):
        action.process()


def test_success(desired: YtReplica, actual_replicated: YtTable):
    desired.enable_replicated_table_tracker = False
    desired.enabled = False
    yt_client_factory = _create_mock_factory(desired, actual_replicated)
    yt_client = yt_client_factory(desired.cluster_name)

    action = AlterReplicaAttributesAction(desired, actual_replicated)
    assert action.schedule_next(yt_client) is False
    action.process()

    actual = actual_replicated.replicas[desired.key]

    assert actual.enable_replicated_table_tracker is False
    assert actual.enabled is False

    call_tracker = yt_client_factory.get_call_tracker(desired.cluster_name)
    assert call_tracker
    call = call_tracker.calls["alter_table_replica"][0]
    assert call.path_or_type == actual.replica_id
    assert call.kwargs == {"enable_replicated_table_tracker": False, "enabled": False}


def test_only_enable_replicated_table_tracker(desired: YtReplica, actual_replicated: YtTable):
    desired.enable_replicated_table_tracker = False
    yt_client_factory = _create_mock_factory(desired, actual_replicated)
    yt_client = yt_client_factory(desired.cluster_name)

    action = AlterReplicaAttributesAction(desired, actual_replicated)
    action.schedule_next(yt_client)
    action.process()

    call_tracker = yt_client_factory.get_call_tracker(desired.cluster_name)
    call = call_tracker.calls["alter_table_replica"][0]
    assert call.kwargs == {"enable_replicated_table_tracker": False, "enabled": True}
