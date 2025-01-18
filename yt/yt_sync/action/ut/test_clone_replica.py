from dataclasses import dataclass
from typing import Any
import uuid

import pytest

import yt.wrapper as yt
from yt.yt_sync.action import CloneReplicaAction
from yt.yt_sync.core.model import Types
from yt.yt_sync.core.model import YtReplica
from yt.yt_sync.core.model import YtTable
from yt.yt_sync.core.test_lib import MockResult
from yt.yt_sync.core.test_lib import MockYtClientFactory
from yt.yt_sync.core.test_lib import yt_generic_error

_TABLET_COUNT = 2


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
            mode=YtReplica.Mode.ASYNC,
            enable_replicated_table_tracker=True,
        )
    )


def _make_tables(
    clusters: Clusters, table_path: str, schema: Types.Schema
) -> tuple[YtTable, YtTable, YtTable, YtTable]:
    desired_replicated = _make_table(clusters.primary, table_path, YtTable.Type.REPLICATED_TABLE, schema)
    actual_replicated = _make_table(clusters.primary, table_path, YtTable.Type.REPLICATED_TABLE, schema)
    src_table = _make_table(clusters.src, table_path, YtTable.Type.TABLE, schema)
    dst_table = _make_table(clusters.dst, table_path, YtTable.Type.TABLE, schema)

    _add_replica(actual_replicated, src_table)
    _add_replica(desired_replicated, src_table)
    _add_replica(desired_replicated, dst_table)

    return (desired_replicated, actual_replicated, src_table, dst_table)


def _create_mock_factory(
    replicated_table: YtTable,
    new_replica_id: str | None = None,
    error: bool = False,
    broken_replica_id: bool = False,
    broken_row_index: bool = False,
) -> MockYtClientFactory:
    if error:
        return MockYtClientFactory(
            {
                replicated_table.cluster_name: {
                    "get_tablet_infos": {replicated_table.path: MockResult(error=yt_generic_error())},
                    "create": {replicated_table.replica_type: MockResult(error=yt_generic_error())},
                }
            }
        )

    assert new_replica_id
    tablets: list[dict] = []
    for tablet in range(replicated_table.effective_tablet_count):
        tablet_info: dict[str, Any] = {"total_row_count": tablet + 1}
        replica_infos: list[dict] = []
        for i, replica in enumerate(replicated_table.replicas.values()):
            row_index = 100 * (tablet + 1) + i + 1
            replica_infos.append(
                {
                    "replica_id": f"_bad_{replica.replica_id}_bad_" if broken_replica_id else replica.replica_id,
                    "current_replication_row_index": row_index + 1 if broken_row_index else row_index,
                    "committed_replication_row_index": row_index,
                }
            )
        tablet_info["replica_infos"] = replica_infos
        tablets.append(tablet_info)

    get_tablet_infos_result = {"tablets": tablets}
    return MockYtClientFactory(
        {
            replicated_table.cluster_name: {
                "get_tablet_infos": {replicated_table.path: MockResult(result=get_tablet_infos_result)},
                "create": {replicated_table.replica_type: MockResult(result=new_replica_id)},
            }
        }
    )


@pytest.fixture
def clusters() -> Clusters:
    return Clusters(primary="p", src="r0", dst="r1")


def test_ctor(clusters: Clusters, table_path: str, default_schema: Types.Schema):
    # desired replicated is not replicated
    with pytest.raises(AssertionError):
        CloneReplicaAction(
            _make_table(clusters.primary, table_path, YtTable.Type.TABLE, default_schema),
            _make_table(clusters.primary, table_path, YtTable.Type.REPLICATED_TABLE, default_schema),
            _make_table(clusters.src, table_path, YtTable.Type.TABLE, default_schema),
            _make_table(clusters.dst, table_path, YtTable.Type.TABLE, default_schema),
        )

    # actual replicated is not replicated
    with pytest.raises(AssertionError):
        CloneReplicaAction(
            _make_table(clusters.primary, table_path, YtTable.Type.REPLICATED_TABLE, default_schema),
            _make_table(clusters.primary, table_path, YtTable.Type.TABLE, default_schema),
            _make_table(clusters.src, table_path, YtTable.Type.TABLE, default_schema),
            _make_table(clusters.dst, table_path, YtTable.Type.TABLE, default_schema),
        )

    # source table is replicated
    with pytest.raises(AssertionError):
        CloneReplicaAction(
            _make_table(clusters.primary, table_path, YtTable.Type.REPLICATED_TABLE, default_schema),
            _make_table(clusters.primary, table_path, YtTable.Type.REPLICATED_TABLE, default_schema),
            _make_table(clusters.src, table_path, YtTable.Type.REPLICATED_TABLE, default_schema),
            _make_table(clusters.dst, table_path, YtTable.Type.TABLE, default_schema),
        )

    # destination table is replicated
    with pytest.raises(AssertionError):
        CloneReplicaAction(
            _make_table(clusters.primary, table_path, YtTable.Type.REPLICATED_TABLE, default_schema),
            _make_table(clusters.primary, table_path, YtTable.Type.REPLICATED_TABLE, default_schema),
            _make_table(clusters.src, table_path, YtTable.Type.TABLE, default_schema),
            _make_table(clusters.dst, table_path, YtTable.Type.REPLICATED_TABLE, default_schema),
        )

    # source == destination
    with pytest.raises(AssertionError):
        CloneReplicaAction(
            _make_table(clusters.primary, table_path, YtTable.Type.REPLICATED_TABLE, default_schema),
            _make_table(clusters.primary, table_path, YtTable.Type.REPLICATED_TABLE, default_schema),
            _make_table(clusters.src, table_path, YtTable.Type.TABLE, default_schema),
            _make_table(clusters.src, table_path, YtTable.Type.TABLE, default_schema),
        )

    # desired replicated is chaotic one
    with pytest.raises(AssertionError):
        CloneReplicaAction(
            _make_table(clusters.primary, table_path, YtTable.Type.CHAOS_REPLICATED_TABLE, default_schema),
            _make_table(clusters.primary, table_path, YtTable.Type.REPLICATED_TABLE, default_schema),
            _make_table(clusters.src, table_path, YtTable.Type.TABLE, default_schema),
            _make_table(clusters.dst, table_path, YtTable.Type.TABLE, default_schema),
        )

    # actual replicated is chaotic one
    with pytest.raises(AssertionError):
        CloneReplicaAction(
            _make_table(clusters.primary, table_path, YtTable.Type.REPLICATED_TABLE, default_schema),
            _make_table(clusters.primary, table_path, YtTable.Type.CHAOS_REPLICATED_TABLE, default_schema),
            _make_table(clusters.src, table_path, YtTable.Type.TABLE, default_schema),
            _make_table(clusters.dst, table_path, YtTable.Type.TABLE, default_schema),
        )

    # everything ok
    CloneReplicaAction(
        _make_table(clusters.primary, table_path, YtTable.Type.REPLICATED_TABLE, default_schema),
        _make_table(clusters.primary, table_path, YtTable.Type.REPLICATED_TABLE, default_schema),
        _make_table(clusters.src, table_path, YtTable.Type.TABLE, default_schema),
        _make_table(clusters.dst, table_path, YtTable.Type.TABLE, default_schema),
    )


def test_process_before_schedule(clusters: Clusters, table_path: str, default_schema: Types.Schema):
    action = CloneReplicaAction(
        _make_table(clusters.primary, table_path, YtTable.Type.REPLICATED_TABLE, default_schema),
        _make_table(clusters.primary, table_path, YtTable.Type.REPLICATED_TABLE, default_schema),
        _make_table(clusters.src, table_path, YtTable.Type.TABLE, default_schema),
        _make_table(clusters.dst, table_path, YtTable.Type.TABLE, default_schema),
    )
    with pytest.raises(AssertionError):
        action.process()


def test_success(clusters: Clusters, table_path: str, default_schema: Types.Schema):
    desired_replicated, actual_replicated, src_table, dst_table = _make_tables(clusters, table_path, default_schema)

    new_replica_id = str(uuid.uuid4())
    yt_client_factory = _create_mock_factory(actual_replicated, new_replica_id)
    yt_client = yt_client_factory(clusters.primary)

    action = CloneReplicaAction(desired_replicated, actual_replicated, src_table, dst_table)

    assert action.schedule_next(yt_client) is True
    action.process()
    assert action.schedule_next(yt_client) is False
    action.process()

    assert dst_table.replica_key in actual_replicated.replicas
    assert new_replica_id == actual_replicated.replicas[dst_table.replica_key].replica_id

    # assert calls
    call_tracker = yt_client_factory.get_call_tracker(actual_replicated.cluster_name)
    assert call_tracker
    assert 1 == len(call_tracker.calls["get_tablet_infos"])
    assert 1 == len(call_tracker.calls["create"])
    call = call_tracker.calls["create"][0]
    assert call.path_or_type == YtReplica.ReplicaType.TABLE_REPLICA

    attributes = call.kwargs["attributes"]
    assert len(attributes["start_replication_row_indexes"]) == _TABLET_COUNT
    assert attributes["cluster_name"] == clusters.dst
    assert attributes["replica_path"] == table_path
    assert attributes["mode"] == YtReplica.Mode.ASYNC
    assert attributes["enable_replicated_table_tracker"] is True
    assert attributes["table_path"] == table_path


def test_yt_error(clusters: Clusters, table_path: str, default_schema: Types.Schema):
    desired_replicated, actual_replicated, src_table, dst_table = _make_tables(clusters, table_path, default_schema)

    yt_client_factory = _create_mock_factory(actual_replicated, error=True)
    yt_client = yt_client_factory(clusters.primary)

    action = CloneReplicaAction(desired_replicated, actual_replicated, src_table, dst_table)

    assert action.schedule_next(yt_client) is True
    with pytest.raises(yt.YtResponseError):
        action.process()


def test_actual_replicated_not_exists(clusters: Clusters, table_path: str, default_schema: Types.Schema):
    desired_replicated, actual_replicated, src_table, dst_table = _make_tables(clusters, table_path, default_schema)
    actual_replicated.exists = False

    new_replica_id = str(uuid.uuid4())
    yt_client_factory = _create_mock_factory(actual_replicated, new_replica_id)
    yt_client = yt_client_factory(clusters.primary)

    action = CloneReplicaAction(desired_replicated, actual_replicated, src_table, dst_table)

    with pytest.raises(AssertionError):
        action.schedule_next(yt_client)


def test_source_not_exists(clusters: Clusters, table_path: str, default_schema: Types.Schema):
    desired_replicated, actual_replicated, src_table, dst_table = _make_tables(clusters, table_path, default_schema)
    actual_replicated.exists = False

    new_replica_id = str(uuid.uuid4())
    yt_client_factory = _create_mock_factory(actual_replicated, new_replica_id)
    yt_client = yt_client_factory(clusters.primary)

    action = CloneReplicaAction(desired_replicated, actual_replicated, src_table, dst_table)

    with pytest.raises(AssertionError):
        action.schedule_next(yt_client)


def test_no_source_replica_in_replicated(clusters: Clusters, table_path: str, default_schema: Types.Schema):
    desired_replicated, actual_replicated, src_table, dst_table = _make_tables(clusters, table_path, default_schema)

    new_replica_id = str(uuid.uuid4())
    yt_client_factory = _create_mock_factory(actual_replicated, new_replica_id, broken_replica_id=True)
    yt_client = yt_client_factory(clusters.primary)

    # remove replica from replicated
    actual_replicated.replicas.pop(src_table.replica_key, None)

    action = CloneReplicaAction(desired_replicated, actual_replicated, src_table, dst_table)

    assert action.schedule_next(yt_client) is True
    with pytest.raises(AssertionError):
        action.process()


def test_no_source_replica_in_response(clusters: Clusters, table_path: str, default_schema: Types.Schema):
    desired_replicated, actual_replicated, src_table, dst_table = _make_tables(clusters, table_path, default_schema)

    new_replica_id = str(uuid.uuid4())
    yt_client_factory = _create_mock_factory(actual_replicated, new_replica_id, broken_replica_id=True)
    yt_client = yt_client_factory(clusters.primary)

    action = CloneReplicaAction(desired_replicated, actual_replicated, src_table, dst_table)

    assert action.schedule_next(yt_client) is True
    with pytest.raises(AssertionError):
        action.process()


def test_replica_not_flushed(clusters: Clusters, table_path: str, default_schema: Types.Schema):
    desired_replicated, actual_replicated, src_table, dst_table = _make_tables(clusters, table_path, default_schema)

    new_replica_id = str(uuid.uuid4())
    yt_client_factory = _create_mock_factory(actual_replicated, new_replica_id, broken_row_index=True)
    yt_client = yt_client_factory(clusters.primary)

    action = CloneReplicaAction(desired_replicated, actual_replicated, src_table, dst_table)

    assert action.schedule_next(yt_client) is True
    with pytest.raises(AssertionError):
        action.process()
