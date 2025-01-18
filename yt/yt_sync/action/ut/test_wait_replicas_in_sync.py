from typing import Any
from typing import Iterable
import uuid

import pytest

import yt.wrapper as yt
from yt.yt_sync.action.wait_replicas_in_sync import WaitReplicasInSyncAction
from yt.yt_sync.core.client import MockResult
from yt.yt_sync.core.model import Types
from yt.yt_sync.core.model import YtReplica
from yt.yt_sync.core.model import YtTable
from yt.yt_sync.core.model.helpers import make_log_name
from yt.yt_sync.core.test_lib import MockYtClientFactory
from yt.yt_sync.core.test_lib import yt_generic_error
from yt.yt_sync.core.test_lib import yt_retryable_error

from .helpers import simple_assert_calls
from .helpers import simple_assert_no_calls

_MAX_TIMESTAMP: int = 0x3FFFFFFFFFFFFF00


@pytest.fixture
def table_attributes(default_schema: Types.Schema) -> Types.Attributes:
    return {"dynamic": True, "schema": default_schema}


def _add_replicas(table: YtTable):
    is_chaos = YtTable.Type.CHAOS_REPLICATED_TABLE == table.table_type
    for cluster in ("r0", "r1"):
        table.add_replica(
            YtReplica(
                replica_id=str(uuid.uuid4()),
                cluster_name=cluster,
                replica_path=table.path,
                enabled=True,
                mode=YtReplica.Mode.ASYNC,
                content_type=YtReplica.ContentType.DATA if is_chaos else None,
            )
        )
        if is_chaos and not table.schema.is_ordered:
            table.add_replica(
                YtReplica(
                    replica_id=str(uuid.uuid4()),
                    cluster_name=cluster,
                    replica_path=make_log_name(table.path),
                    enabled=True,
                    mode=YtReplica.Mode.ASYNC,
                    content_type=YtReplica.ContentType.QUEUE,
                )
            )


@pytest.fixture(params=[YtTable.Type.REPLICATED_TABLE, YtTable.Type.CHAOS_REPLICATED_TABLE])
def yt_table(request: pytest.FixtureRequest, table_path: str, table_attributes: Types.Attributes) -> YtTable:
    table_type = str(request.param)
    table = YtTable.make("k", "p", table_type, table_path, True, table_attributes)
    _add_replicas(table)
    return table


@pytest.fixture()
def ordered_table(table_path: str, ordered_schema: Types.Schema) -> YtTable:
    table = YtTable.make(
        "k", "p", YtTable.Type.REPLICATED_TABLE, table_path, True, {"dynamic": True, "schema": ordered_schema}
    )
    _add_replicas(table)
    return table


@pytest.fixture()
def chaos_ordered_table(table_path: str, ordered_schema: Types.Schema) -> YtTable:
    table = YtTable.make(
        "k", "p", YtTable.Type.CHAOS_REPLICATED_TABLE, table_path, True, {"dynamic": True, "schema": ordered_schema}
    )
    _add_replicas(table)
    return table


def test_non_replicated_table(table_path: str, table_attributes: Types.Attributes):
    table = YtTable.make("k", "c", YtTable.Type.TABLE, table_path, True, table_attributes)
    with pytest.raises(AssertionError):
        WaitReplicasInSyncAction(table, set())


def test_process_before_schedule(yt_table: YtTable):
    with pytest.raises(AssertionError):
        WaitReplicasInSyncAction(yt_table, set()).process()


def test_fail(yt_table: YtTable):
    action = WaitReplicasInSyncAction(yt_table, {"r0"})
    yt_client_factory = MockYtClientFactory(
        {
            yt_table.cluster_name: {
                "generate_timestamp": {"*": MockResult(result=_MAX_TIMESTAMP, raw=True)},
                "get_in_sync_replicas": {yt_table.path: MockResult(error=yt_generic_error())},
            }
        }
    )
    yt_client = yt_client_factory(yt_table.cluster_name)
    assert action.schedule_next(yt_client) is True

    simple_assert_calls(yt_client_factory, yt_table, "generate_timestamp", 1)
    simple_assert_calls(yt_client_factory, yt_table, "get_in_sync_replicas", 1)

    with pytest.raises(yt.YtResponseError):
        action.process()


def test_not_ready(yt_table: YtTable):
    action = WaitReplicasInSyncAction(yt_table, {"r0", "r1"})
    response = [_first_replica(yt_table).replica_id]
    yt_client_factory = MockYtClientFactory(
        {
            yt_table.cluster_name: {
                "generate_timestamp": {"*": MockResult(result=_MAX_TIMESTAMP)},
                "get_in_sync_replicas": {yt_table.path: MockResult(result=response)},
            }
        }
    )
    yt_client = yt_client_factory(yt_table.cluster_name)
    assert action.schedule_next(yt_client) is True
    action.process()
    assert action.schedule_next(yt_client) is True

    simple_assert_calls(yt_client_factory, yt_table, "generate_timestamp", 1)
    simple_assert_calls(yt_client_factory, yt_table, "get_in_sync_replicas", 2)


def test_retryable_error(yt_table: YtTable):
    action = WaitReplicasInSyncAction(yt_table, {"r0", "r1"})
    yt_client_factory = MockYtClientFactory(
        {
            yt_table.cluster_name: {
                "generate_timestamp": {"*": MockResult(result=_MAX_TIMESTAMP)},
                "get_in_sync_replicas": {yt_table.path: MockResult(error=yt_retryable_error())},
            }
        }
    )
    yt_client = yt_client_factory(yt_table.cluster_name)
    assert action.schedule_next(yt_client) is True
    action.process()
    assert action.schedule_next(yt_client) is True

    simple_assert_calls(yt_client_factory, yt_table, "generate_timestamp", 1)
    simple_assert_calls(yt_client_factory, yt_table, "get_in_sync_replicas", 2)


def test_success_dry_run(yt_table: YtTable):
    action = WaitReplicasInSyncAction(yt_table, {"r0", "r1"})
    action.dry_run = True
    response = [r.replica_id for r in _replicas(yt_table)]
    yt_client_factory = MockYtClientFactory(
        {
            yt_table.cluster_name: {
                "generate_timestamp": {"*": MockResult(result=_MAX_TIMESTAMP)},
                "get_in_sync_replicas": {yt_table.path: MockResult(result=response)},
            }
        }
    )
    yt_client = yt_client_factory(yt_table.cluster_name)
    assert action.schedule_next(yt_client) is False

    action.process()

    simple_assert_no_calls(yt_client_factory, yt_table, "generate_timestamp")
    simple_assert_no_calls(yt_client_factory, yt_table, "get_in_sync_replicas")


def test_success(yt_table: YtTable):
    action = WaitReplicasInSyncAction(yt_table, {"r0", "r1"})
    response = [r.replica_id for r in _replicas(yt_table)]
    yt_client_factory = MockYtClientFactory(
        {
            yt_table.cluster_name: {
                "generate_timestamp": {"*": MockResult(result=_MAX_TIMESTAMP)},
                "get_in_sync_replicas": {yt_table.path: MockResult(result=response)},
            }
        }
    )
    yt_client = yt_client_factory(yt_table.cluster_name)
    assert action.schedule_next(yt_client) is True

    action.process()
    assert action.schedule_next(yt_client) is False

    simple_assert_calls(yt_client_factory, yt_table, "generate_timestamp", 1)
    simple_assert_calls(yt_client_factory, yt_table, "get_in_sync_replicas", 1)


def _ordered_table_get_tablet_infos(
    ordered_table: YtTable, total_row_count: int, replica_row_count: int, mode: str
) -> Types.Attributes:
    replica_infos: list[dict[str, Any]] = list()
    for replica in ordered_table.replicas.values():
        replica_info = {
            "replica_id": replica.replica_id,
            "cluster_name": replica.cluster_name,
            "mode": mode,
            "state": YtReplica.State.ENABLED,
            "committed_replication_row_index": replica_row_count,
        }
        replica_infos.append(replica_info)
    return {"tablets": [{"total_row_count": total_row_count, "replica_infos": replica_infos}]}


def test_ordered_table_in_sync(ordered_table: YtTable):
    total_row_count = 11
    tablet_infos = _ordered_table_get_tablet_infos(ordered_table, total_row_count, total_row_count, YtReplica.Mode.SYNC)

    yt_client_factory = MockYtClientFactory(
        {
            ordered_table.cluster_name: {
                "get_tablet_infos": {ordered_table.path: MockResult(result=tablet_infos)},
            }
        }
    )
    yt_client = yt_client_factory(ordered_table.cluster_name)

    action = WaitReplicasInSyncAction(ordered_table, {"r0", "r1"})
    assert action.schedule_next(yt_client) is True
    action.process()

    simple_assert_calls(yt_client_factory, ordered_table, "get_tablet_infos", 1, ordered_table.path)

    assert action.schedule_next(yt_client) is False
    action.process()


def test_ordered_table_in_sync_with_lag(ordered_table: YtTable):
    total_row_count = 11
    tablet_infos = _ordered_table_get_tablet_infos(
        ordered_table, total_row_count, total_row_count - 1, YtReplica.Mode.SYNC
    )

    yt_client_factory = MockYtClientFactory(
        {
            ordered_table.cluster_name: {
                "get_tablet_infos": {ordered_table.path: MockResult(result=tablet_infos)},
            }
        }
    )
    yt_client = yt_client_factory(ordered_table.cluster_name)

    action = WaitReplicasInSyncAction(ordered_table, {"r0", "r1"})
    assert action.schedule_next(yt_client) is True
    action.process()

    simple_assert_calls(yt_client_factory, ordered_table, "get_tablet_infos", 1, ordered_table.path)

    assert action.schedule_next(yt_client) is True
    action.process()

    simple_assert_calls(yt_client_factory, ordered_table, "get_tablet_infos", 2, ordered_table.path)


def test_ordered_table_async(ordered_table: YtTable):
    total_row_count = 11
    tablet_infos = _ordered_table_get_tablet_infos(
        ordered_table, total_row_count, total_row_count, YtReplica.Mode.ASYNC
    )

    yt_client_factory = MockYtClientFactory(
        {
            ordered_table.cluster_name: {
                "get_tablet_infos": {ordered_table.path: MockResult(result=tablet_infos)},
            }
        }
    )
    yt_client = yt_client_factory(ordered_table.cluster_name)

    action = WaitReplicasInSyncAction(ordered_table, {"r0", "r1"})
    assert action.schedule_next(yt_client) is True
    action.process()

    simple_assert_calls(yt_client_factory, ordered_table, "get_tablet_infos", 1, ordered_table.path)

    assert action.schedule_next(yt_client) is True
    action.process()

    simple_assert_calls(yt_client_factory, ordered_table, "get_tablet_infos", 2, ordered_table.path)


def _chaos_ordered_table_replicas(chaos_ordered_table: YtTable, state: str, mode: str) -> Types.Attributes:
    result: Types.Attributes = dict()
    for replica in chaos_ordered_table.replicas.values():
        replica_info = {"state": state, "mode": mode}
        result[replica.replica_id] = replica_info
    return result


def test_chaos_ordered_table_in_sync(chaos_ordered_table: YtTable):
    replicas = _chaos_ordered_table_replicas(chaos_ordered_table, YtReplica.State.ENABLED, YtReplica.Mode.SYNC)
    get_path: str = f"{chaos_ordered_table.path}/@replicas"
    yt_client_factory = MockYtClientFactory(
        {
            chaos_ordered_table.cluster_name: {
                "get": {get_path: MockResult(result=replicas)},
            }
        }
    )
    yt_client = yt_client_factory(chaos_ordered_table.cluster_name)

    action = WaitReplicasInSyncAction(chaos_ordered_table, {"r0", "r1"})
    assert action.schedule_next(yt_client) is True
    action.process()

    simple_assert_calls(yt_client_factory, chaos_ordered_table, "get", 1, get_path)

    assert action.schedule_next(yt_client) is False
    action.process()


def test_chaos_ordered_table_async(chaos_ordered_table: YtTable):
    replicas = _chaos_ordered_table_replicas(chaos_ordered_table, YtReplica.State.ENABLED, YtReplica.Mode.ASYNC)
    get_path: str = f"{chaos_ordered_table.path}/@replicas"
    yt_client_factory = MockYtClientFactory(
        {
            chaos_ordered_table.cluster_name: {
                "get": {get_path: MockResult(result=replicas)},
            }
        }
    )
    yt_client = yt_client_factory(chaos_ordered_table.cluster_name)

    action = WaitReplicasInSyncAction(chaos_ordered_table, {"r0", "r1"})
    assert action.schedule_next(yt_client) is True
    action.process()

    simple_assert_calls(yt_client_factory, chaos_ordered_table, "get", 1, get_path)

    assert action.schedule_next(yt_client) is True
    action.process()

    simple_assert_calls(yt_client_factory, chaos_ordered_table, "get", 2, get_path)


def test_chaos_ordered_table_insync_disabled(chaos_ordered_table: YtTable):
    replicas = _chaos_ordered_table_replicas(chaos_ordered_table, YtReplica.State.DISABLED, YtReplica.Mode.SYNC)
    get_path: str = f"{chaos_ordered_table.path}/@replicas"
    yt_client_factory = MockYtClientFactory(
        {
            chaos_ordered_table.cluster_name: {
                "get": {get_path: MockResult(result=replicas)},
            }
        }
    )
    yt_client = yt_client_factory(chaos_ordered_table.cluster_name)

    action = WaitReplicasInSyncAction(chaos_ordered_table, {"r0", "r1"})
    assert action.schedule_next(yt_client) is True
    action.process()

    simple_assert_calls(yt_client_factory, chaos_ordered_table, "get", 1, get_path)

    assert action.schedule_next(yt_client) is True
    action.process()

    simple_assert_calls(yt_client_factory, chaos_ordered_table, "get", 2, get_path)


def test_non_existing_table(yt_table: YtTable):
    yt_table.exists = False
    action = WaitReplicasInSyncAction(yt_table, {"r0", "r1"})
    yt_client_factory = MockYtClientFactory(
        {
            yt_table.cluster_name: {
                "generate_timestamp": {"*": MockResult(result=_MAX_TIMESTAMP)},
                "get_in_sync_replicas": {yt_table.path: MockResult(result=1)},
            }
        }
    )
    yt_client = yt_client_factory(yt_table.cluster_name)
    assert action.schedule_next(yt_client) is False
    action.process()

    simple_assert_no_calls(yt_client_factory, yt_table, "generate_timestamp")
    simple_assert_no_calls(yt_client_factory, yt_table, "get_in_sync_replicas")


def test_success_by_cluster(yt_table: YtTable):
    replica = _first_replica(yt_table)
    action = WaitReplicasInSyncAction(yt_table, {replica.cluster_name})
    yt_client_factory = MockYtClientFactory(
        {
            yt_table.cluster_name: {
                "generate_timestamp": {"*": MockResult(result=_MAX_TIMESTAMP)},
                "get_in_sync_replicas": {yt_table.path: MockResult(result=[replica.replica_id])},
            }
        }
    )
    yt_client = yt_client_factory(yt_table.cluster_name)
    assert action.schedule_next(yt_client) is True

    action.process()
    assert action.schedule_next(yt_client) is False

    simple_assert_calls(yt_client_factory, yt_table, "generate_timestamp", 1)
    simple_assert_calls(yt_client_factory, yt_table, "get_in_sync_replicas", 1)


def _replicas(yt_table: YtTable) -> Iterable[YtReplica]:
    return [r for r in yt_table.replicas.values() if YtReplica.ContentType.QUEUE != r.content_type]


def _first_replica(yt_table: YtTable) -> YtReplica:
    return next(iter(_replicas(yt_table)))
