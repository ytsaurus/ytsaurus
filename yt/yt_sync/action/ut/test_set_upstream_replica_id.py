import uuid

import pytest

import yt.wrapper as yt
from yt.yt_sync.action.set_upstream_replica import SetUpstreamReplicaAction
from yt.yt_sync.core.model import Types
from yt.yt_sync.core.model import YtReplica
from yt.yt_sync.core.model import YtTable
from yt.yt_sync.core.test_lib import MockResult
from yt.yt_sync.core.test_lib import MockYtClientFactory
from yt.yt_sync.core.test_lib import yt_generic_error


@pytest.fixture
def table_attributes(default_schema: Types.Schema) -> Types.Attributes:
    return {"dynamic": True, "schema": default_schema}


@pytest.fixture
def yt_replicated_table(table_path: str, table_attributes: Types.Attributes) -> YtTable:
    table = YtTable.make("k", "p", YtTable.Type.REPLICATED_TABLE, table_path, True, table_attributes)
    for cluster in ("r0", "r1"):
        table.add_replica(
            YtReplica(
                replica_id=str(uuid.uuid4()),
                cluster_name=cluster,
                replica_path=table_path,
                enabled=True,
                mode=YtReplica.Mode.ASYNC,
            )
        )
    return table


@pytest.fixture
def yt_table(table_path: str, table_attributes: Types.Attributes) -> YtTable:
    return YtTable.make("k", "r0", YtTable.Type.TABLE, table_path, True, table_attributes)


def test_create_with_non_replicated(yt_table: YtTable):
    with pytest.raises(AssertionError):
        SetUpstreamReplicaAction(yt_table, yt_table).schedule_next(None)  # type: ignore


def test_create_with_replicated(yt_replicated_table: YtTable):
    with pytest.raises(AssertionError):
        SetUpstreamReplicaAction(yt_replicated_table, yt_replicated_table).schedule_next(None)  # type: ignore


def test_process_before_schedule(yt_table: YtTable, yt_replicated_table: YtTable):
    with pytest.raises(AssertionError):
        SetUpstreamReplicaAction(yt_table, yt_replicated_table).process()


def test_unexisting_replica(yt_table: YtTable, yt_replicated_table: YtTable):
    yt_table.cluster_name = "_other_"
    action = SetUpstreamReplicaAction(yt_table, yt_replicated_table)
    yt_client_factory = MockYtClientFactory({yt_table.cluster_name: {}})
    yt_client = yt_client_factory(yt_table.cluster_name)

    with pytest.raises(AssertionError):
        action.schedule_next(yt_client)


def test_schedule_twice(yt_table: YtTable, yt_replicated_table: YtTable):
    action = SetUpstreamReplicaAction(yt_table, yt_replicated_table)
    yt_client_factory = MockYtClientFactory(
        {
            yt_table.cluster_name: {
                "alter_table": {yt_table.path: MockResult(result=True)},
            }
        }
    )
    yt_client = yt_client_factory(yt_table.cluster_name)

    action.schedule_next(yt_client)
    with pytest.raises(AssertionError):
        action.schedule_next(yt_client)


def test_fail(yt_table: YtTable, yt_replicated_table: YtTable):
    action = SetUpstreamReplicaAction(yt_table, yt_replicated_table)
    yt_client_factory = MockYtClientFactory(
        {
            yt_table.cluster_name: {
                "alter_table": {yt_table.path: MockResult(error=yt_generic_error())},
            }
        }
    )
    yt_client = yt_client_factory(yt_table.cluster_name)

    assert action.schedule_next(yt_client) is False
    _assert_call(yt_client_factory, yt_table)
    with pytest.raises(yt.YtResponseError):
        action.process()


def test_success(yt_table: YtTable, yt_replicated_table: YtTable):
    action = SetUpstreamReplicaAction(yt_table, yt_replicated_table)
    yt_client_factory = MockYtClientFactory(
        {
            yt_table.cluster_name: {
                "alter_table": {yt_table.path: MockResult(result=True)},
            }
        }
    )
    yt_client = yt_client_factory(yt_table.cluster_name)

    assert action.schedule_next(yt_client) is False
    _assert_call(yt_client_factory, yt_table)
    action.process()

    assert yt_table.attributes.get("upstream_replica_id") is not None


def _assert_call(yt_client_factory: MockYtClientFactory, yt_table: YtTable):
    call_tracker = yt_client_factory.get_call_tracker(yt_table.cluster_name)
    assert call_tracker
    assert 1 == len(call_tracker.calls["alter_table"])
    call = call_tracker.calls["alter_table"][0]
    assert yt_table.path == call.path_or_type
    assert call.kwargs
    assert call.kwargs["upstream_replica_id"] is not None
