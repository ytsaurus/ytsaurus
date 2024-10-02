from dataclasses import dataclass
import uuid

import pytest

import yt.wrapper as yt
from yt.yt_sync.action import CloneChaosReplicaAction
from yt.yt_sync.core.model import Types
from yt.yt_sync.core.model import YtReplica
from yt.yt_sync.core.model import YtTable
from yt.yt_sync.core.test_lib import MockResult
from yt.yt_sync.core.test_lib import MockYtClientFactory
from yt.yt_sync.core.test_lib import yt_generic_error

_TABLET_COUNT = 3


@dataclass
class Clusters:
    primary: str
    src: str
    dst: str


def _make_table(cluster_name: str, table_path: str, table_type: str, schema: Types.Schema):
    result = YtTable.make(
        table_path,
        cluster_name,
        table_type,
        table_path,
        True,
        {"dynamic": True, "schema": schema, "tablet_count": _TABLET_COUNT},
    )
    return result


def _add_replica(replicated_table: YtTable, table: YtTable):
    replicated_table.add_replica(
        YtReplica(
            replica_id=str(uuid.uuid4()),
            cluster_name=table.cluster_name,
            replica_path=table.path,
            enabled=True,
            mode=YtReplica.Mode.SYNC,
            enable_replicated_table_tracker=True,
            content_type=(
                YtReplica.ContentType.QUEUE
                if table.is_ordered or table.table_type == YtTable.Type.REPLICATION_LOG
                else YtReplica.ContentType.DATA
            ),
        )
    )


def _make_tables(
    clusters: Clusters, table_path: str, schema: Types.Schema
) -> tuple[YtTable, YtTable, YtTable, YtTable]:
    desired_replicated = _make_table(clusters.primary, table_path, YtTable.Type.CHAOS_REPLICATED_TABLE, schema)
    actual_replicated = _make_table(clusters.primary, table_path, YtTable.Type.CHAOS_REPLICATED_TABLE, schema)
    src_table = _make_table(clusters.src, table_path, YtTable.Type.TABLE, schema)
    dst_table = _make_table(clusters.dst, table_path, YtTable.Type.TABLE, schema)

    _add_replica(actual_replicated, src_table)
    _add_replica(desired_replicated, src_table)
    _add_replica(desired_replicated, dst_table)

    return (desired_replicated, actual_replicated, src_table, dst_table)


def _get_replication_progress() -> list[int]:
    return [i for i in range(_TABLET_COUNT)]


def _create_mock_factory(
    replicated_table: YtTable,
    source_table: YtTable,
    new_replica_id: str | None = None,
    error: bool = False,
) -> MockYtClientFactory:
    existing_replica_id: str = replicated_table.replicas[source_table.replica_key].replica_id
    assert existing_replica_id
    if error:
        return MockYtClientFactory(
            {
                replicated_table.cluster_name: {
                    "get": {f"#{existing_replica_id}/@replication_progress": MockResult(error=yt_generic_error())},
                    "create": {YtReplica.ReplicaType.CHAOS_TABLE_REPLICA: MockResult(error=yt_generic_error())},
                }
            }
        )

    assert new_replica_id
    replication_progress: list[int] = _get_replication_progress()
    return MockYtClientFactory(
        {
            replicated_table.cluster_name: {
                "get": {f"#{existing_replica_id}/@replication_progress": MockResult(result=replication_progress)},
                "create": {YtReplica.ReplicaType.CHAOS_TABLE_REPLICA: MockResult(result=new_replica_id)},
            }
        }
    )


@pytest.fixture
def clusters() -> Clusters:
    return Clusters(primary="p", src="r0", dst="r1")


def test_ctor(clusters: Clusters, table_path: str, default_schema: Types.Schema):
    for table_type in (YtTable.Type.TABLE, YtTable.Type.REPLICATED_TABLE, YtTable.Type.REPLICATION_LOG):
        # desired replicated is not replicated
        with pytest.raises(AssertionError):
            CloneChaosReplicaAction(
                _make_table(clusters.primary, table_path, table_type, default_schema),
                _make_table(clusters.primary, table_path, YtTable.Type.CHAOS_REPLICATED_TABLE, default_schema),
                _make_table(clusters.src, table_path, YtTable.Type.TABLE, default_schema),
                _make_table(clusters.dst, table_path, YtTable.Type.TABLE, default_schema),
            )

        # actual replicated is not chaos replicated
        with pytest.raises(AssertionError):
            CloneChaosReplicaAction(
                _make_table(clusters.primary, table_path, YtTable.Type.CHAOS_REPLICATED_TABLE, default_schema),
                _make_table(clusters.primary, table_path, table_type, default_schema),
                _make_table(clusters.src, table_path, YtTable.Type.TABLE, default_schema),
                _make_table(clusters.dst, table_path, YtTable.Type.TABLE, default_schema),
            )

    for table_type in (YtTable.Type.REPLICATED_TABLE, YtTable.Type.CHAOS_REPLICATED_TABLE):
        # source is replicated
        with pytest.raises(AssertionError):
            CloneChaosReplicaAction(
                _make_table(clusters.primary, table_path, YtTable.Type.CHAOS_REPLICATED_TABLE, default_schema),
                _make_table(clusters.primary, table_path, YtTable.Type.CHAOS_REPLICATED_TABLE, default_schema),
                _make_table(clusters.src, table_path, table_type, default_schema),
                _make_table(clusters.dst, table_path, YtTable.Type.TABLE, default_schema),
            )

        # destination is replicated
        with pytest.raises(AssertionError):
            CloneChaosReplicaAction(
                _make_table(clusters.primary, table_path, YtTable.Type.CHAOS_REPLICATED_TABLE, default_schema),
                _make_table(clusters.primary, table_path, YtTable.Type.CHAOS_REPLICATED_TABLE, default_schema),
                _make_table(clusters.src, table_path, YtTable.Type.TABLE, default_schema),
                _make_table(clusters.dst, table_path, table_type, default_schema),
            )

    # clone to self
    with pytest.raises(AssertionError):
        CloneChaosReplicaAction(
            _make_table(clusters.primary, table_path, YtTable.Type.CHAOS_REPLICATED_TABLE, default_schema),
            _make_table(clusters.primary, table_path, YtTable.Type.CHAOS_REPLICATED_TABLE, default_schema),
            _make_table(clusters.src, table_path, YtTable.Type.TABLE, default_schema),
            _make_table(clusters.src, table_path, YtTable.Type.TABLE, default_schema),
        )

    # everything ok
    CloneChaosReplicaAction(
        _make_table(clusters.primary, table_path, YtTable.Type.CHAOS_REPLICATED_TABLE, default_schema),
        _make_table(clusters.primary, table_path, YtTable.Type.CHAOS_REPLICATED_TABLE, default_schema),
        _make_table(clusters.src, table_path, YtTable.Type.TABLE, default_schema),
        _make_table(clusters.dst, table_path, YtTable.Type.TABLE, default_schema),
    )


def test_process_before_schedule(clusters: Clusters, table_path: str, default_schema: Types.Schema):
    action = CloneChaosReplicaAction(
        _make_table(clusters.primary, table_path, YtTable.Type.CHAOS_REPLICATED_TABLE, default_schema),
        _make_table(clusters.primary, table_path, YtTable.Type.CHAOS_REPLICATED_TABLE, default_schema),
        _make_table(clusters.src, table_path, YtTable.Type.TABLE, default_schema),
        _make_table(clusters.dst, table_path, YtTable.Type.TABLE, default_schema),
    )
    with pytest.raises(AssertionError):
        action.process()


def test_success(clusters: Clusters, table_path: str, default_schema: Types.Schema):
    desired_replicated, actual_replicated, src_table, dst_table = _make_tables(clusters, table_path, default_schema)

    new_replica_id = str(uuid.uuid4())
    yt_client_factory = _create_mock_factory(actual_replicated, src_table, new_replica_id)
    yt_client = yt_client_factory(clusters.primary)

    action = CloneChaosReplicaAction(desired_replicated, actual_replicated, src_table, dst_table)

    assert action.schedule_next(yt_client) is True
    action.process()
    assert action.schedule_next(yt_client) is False
    action.process()

    assert dst_table.replica_key in actual_replicated.replicas
    assert new_replica_id == actual_replicated.replicas[dst_table.replica_key].replica_id

    # assert calls
    call_tracker = yt_client_factory.get_call_tracker(actual_replicated.cluster_name)
    assert call_tracker
    assert 1 == len(call_tracker.calls["get"])
    assert 1 == len(call_tracker.calls["create"])
    call = call_tracker.calls["create"][0]
    assert call.path_or_type == YtReplica.ReplicaType.CHAOS_TABLE_REPLICA

    attributes = call.kwargs["attributes"]
    assert attributes["replication_progress"] == _get_replication_progress()
    assert attributes["catchup"] is True
    assert attributes["cluster_name"] == clusters.dst
    assert attributes["replica_path"] == table_path
    assert attributes["mode"] == YtReplica.Mode.ASYNC
    assert attributes["enable_replicated_table_tracker"] is True
    assert attributes["content_type"] == YtReplica.ContentType.DATA
    assert attributes["table_path"] == table_path


def test_yt_error(clusters: Clusters, table_path: str, default_schema: Types.Schema):
    desired_replicated, actual_replicated, src_table, dst_table = _make_tables(clusters, table_path, default_schema)

    yt_client_factory = _create_mock_factory(actual_replicated, src_table, error=True)
    yt_client = yt_client_factory(clusters.primary)

    action = CloneChaosReplicaAction(desired_replicated, actual_replicated, src_table, dst_table)

    assert action.schedule_next(yt_client) is True
    with pytest.raises(yt.YtResponseError):
        action.process()


def test_actual_replicated_not_exists(clusters: Clusters, table_path: str, default_schema: Types.Schema):
    desired_replicated, actual_replicated, src_table, dst_table = _make_tables(clusters, table_path, default_schema)
    actual_replicated.exists = False

    new_replica_id = str(uuid.uuid4())
    yt_client_factory = _create_mock_factory(actual_replicated, src_table, new_replica_id)
    yt_client = yt_client_factory(clusters.primary)

    action = CloneChaosReplicaAction(desired_replicated, actual_replicated, src_table, dst_table)

    with pytest.raises(AssertionError):
        action.schedule_next(yt_client)


def test_source_not_exists(clusters: Clusters, table_path: str, default_schema: Types.Schema):
    desired_replicated, actual_replicated, src_table, dst_table = _make_tables(clusters, table_path, default_schema)
    src_table.exists = False

    new_replica_id = str(uuid.uuid4())
    yt_client_factory = _create_mock_factory(actual_replicated, src_table, new_replica_id)
    yt_client = yt_client_factory(clusters.primary)

    action = CloneChaosReplicaAction(desired_replicated, actual_replicated, src_table, dst_table)

    with pytest.raises(AssertionError):
        action.schedule_next(yt_client)


def test_no_source_replica(clusters: Clusters, table_path: str, default_schema: Types.Schema):
    desired_replicated, actual_replicated, src_table, dst_table = _make_tables(clusters, table_path, default_schema)

    new_replica_id = str(uuid.uuid4())
    yt_client_factory = _create_mock_factory(actual_replicated, src_table, new_replica_id)
    yt_client = yt_client_factory(clusters.primary)

    # remove replica
    actual_replicated.replicas.pop(src_table.replica_key, None)

    action = CloneChaosReplicaAction(desired_replicated, actual_replicated, src_table, dst_table)

    with pytest.raises(AssertionError):
        action.schedule_next(yt_client)
