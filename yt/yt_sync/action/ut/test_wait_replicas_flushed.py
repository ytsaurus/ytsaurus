import uuid

import pytest

import yt.wrapper as yt
from yt.yt_sync.action.wait_replicas_flushed import WaitReplicasFlushedAction
from yt.yt_sync.core.client import MockResult
from yt.yt_sync.core.model import Types
from yt.yt_sync.core.model import YtReplica
from yt.yt_sync.core.model import YtTable
from yt.yt_sync.core.test_lib import MockYtClientFactory
from yt.yt_sync.core.test_lib import yt_generic_error
from yt.yt_sync.core.test_lib import yt_retryable_error

from .helpers import simple_assert_calls


@pytest.fixture
def table_attributes(default_schema: Types.Schema) -> Types.Attributes:
    return {"dynamic": True, "schema": default_schema}


@pytest.fixture
def yt_table(table_path: str, table_attributes: Types.Attributes) -> YtTable:
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


def test_non_replicated_table(yt_table: YtTable):
    yt_table.table_type = YtTable.Type.TABLE
    with pytest.raises(AssertionError):
        WaitReplicasFlushedAction(yt_table)


def test_process_before_schedule(yt_table: YtTable):
    with pytest.raises(AssertionError):
        WaitReplicasFlushedAction(yt_table).process()


def test_success(yt_table: YtTable):
    action = WaitReplicasFlushedAction(yt_table)

    response = dict()
    for replica in yt_table.replicas.values():
        response[f"#{replica.replica_id}/@tablets"] = MockResult(
            result=[{"flushed_row_count": 10, "current_replication_row_index": 10}]
        )

    yt_client_factory = MockYtClientFactory({yt_table.cluster_name: {"get": response}})
    yt_client = yt_client_factory(yt_table.cluster_name)
    assert action.schedule_next(yt_client) is True
    _assert_calls(yt_client_factory, yt_table)
    action.process()
    assert action.schedule_next(yt_client) is False


def test_not_ready(yt_table: YtTable):
    action = WaitReplicasFlushedAction(yt_table)

    response = dict()
    for replica in yt_table.replicas.values():
        response[f"#{replica.replica_id}/@tablets"] = MockResult(
            result=[{"flushed_row_count": 9, "current_replication_row_index": 10}]
        )

    yt_client_factory = MockYtClientFactory({yt_table.cluster_name: {"get": response}})
    yt_client = yt_client_factory(yt_table.cluster_name)
    assert action.schedule_next(yt_client) is True
    _assert_calls(yt_client_factory, yt_table)
    action.process()
    assert action.schedule_next(yt_client) is True


def test_retryable_error(yt_table: YtTable):
    action = WaitReplicasFlushedAction(yt_table)

    response = dict()
    for replica in yt_table.replicas.values():
        response[f"#{replica.replica_id}/@tablets"] = MockResult(error=yt_retryable_error())

    yt_client_factory = MockYtClientFactory({yt_table.cluster_name: {"get": response}})
    yt_client = yt_client_factory(yt_table.cluster_name)
    assert action.schedule_next(yt_client) is True
    _assert_calls(yt_client_factory, yt_table)
    action.process()
    assert action.schedule_next(yt_client) is True


def test_fail(yt_table: YtTable):
    action = WaitReplicasFlushedAction(yt_table)

    response = dict()
    for replica in yt_table.replicas.values():
        response[f"#{replica.replica_id}/@tablets"] = MockResult(error=yt_generic_error())

    yt_client_factory = MockYtClientFactory({yt_table.cluster_name: {"get": response}})
    yt_client = yt_client_factory(yt_table.cluster_name)
    assert action.schedule_next(yt_client) is True
    _assert_calls(yt_client_factory, yt_table)
    with pytest.raises(yt.YtResponseError):
        action.process()


def _assert_calls(yt_client_factory: MockYtClientFactory, yt_table: YtTable):
    calls = simple_assert_calls(yt_client_factory, yt_table, "get", len(yt_table.replicas))

    paths: set[str] = set()
    for replica in yt_table.replicas.values():
        paths.add(f"#{replica.replica_id}/@tablets")
    for call in calls:
        assert call.path_or_type in paths
