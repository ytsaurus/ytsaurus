from copy import deepcopy
import uuid

import pytest

from yt.yt_sync.action.create_replicated_table import CreateReplicatedTableAction
from yt.yt_sync.core.model import Types
from yt.yt_sync.core.model import YtReplica
from yt.yt_sync.core.model import YtTable
from yt.yt_sync.core.test_lib import CallTracker
from yt.yt_sync.core.test_lib import MockResult
from yt.yt_sync.core.test_lib import MockYtClientFactory

_RTT_OPTIONS = {
    "enable_replicated_table_tracker": True,
    "enable_preload_state_check": True,
    "min_sync_replica_count": 2,
    "max_sync_replica_count": 3,
}


def _check_rtt_options(table: YtTable):
    assert table.is_rtt_enabled is True
    assert table.min_sync_replica_count == 2
    assert table.max_sync_replica_count == 3
    assert table.rtt_options.attributes.get("enable_preload_state_check") is True


@pytest.fixture
def table_attributes(default_schema: Types.Schema) -> Types.Attributes:
    return {"schema": default_schema}


@pytest.mark.parametrize("table_type", [YtTable.Type.TABLE, YtTable.Type.REPLICATION_LOG])
def test_bad_table_type(table_type: str, table_path: str, table_attributes: Types.Attributes):
    desired_table = YtTable.make("k", "primary", table_type, table_path, True, table_attributes)
    actual_table = YtTable.make("k", "primary", table_type, table_path, False, dict())
    with pytest.raises(AssertionError):
        CreateReplicatedTableAction(desired_table, actual_table)


@pytest.mark.parametrize("table_type", [YtTable.Type.REPLICATED_TABLE, YtTable.Type.CHAOS_REPLICATED_TABLE])
def test_process_without_schedule(table_type: str, table_path: str, table_attributes: Types.Attributes):
    desired_table = YtTable.make("k", "primary", table_type, table_path, True, table_attributes)
    actual_table = YtTable.make("k", "primary", table_type, table_path, False, dict())
    with pytest.raises(AssertionError):
        CreateReplicatedTableAction(desired_table, actual_table).process()


@pytest.mark.parametrize(
    "table_type,replica_type",
    [
        (YtTable.Type.REPLICATED_TABLE, YtReplica.ReplicaType.TABLE_REPLICA),
        (YtTable.Type.CHAOS_REPLICATED_TABLE, YtReplica.ReplicaType.CHAOS_TABLE_REPLICA),
    ],
)
@pytest.mark.parametrize("dry_run", [True, False])
def test_create_replicated_table(
    table_path: str, table_attributes: Types.Attributes, table_type: str, replica_type: str, dry_run: bool
):
    desired_table = YtTable.make("k", "primary", table_type, table_path, True, table_attributes)
    is_chaos = YtTable.Type.CHAOS_REPLICATED_TABLE == table_type
    desired_table.add_replica(
        YtReplica(
            replica_id=None,
            cluster_name="remote0",
            replica_path=table_path,
            enabled=True,
            mode=YtReplica.Mode.SYNC,
            enable_replicated_table_tracker=True,
            content_type=YtReplica.ContentType.DATA if is_chaos else None,
        )
    )
    desired_table.add_replica(
        YtReplica(
            replica_id=None,
            cluster_name="remote1",
            replica_path=table_path,
            enabled=True,
            mode=YtReplica.Mode.ASYNC,
            content_type=YtReplica.ContentType.QUEUE if is_chaos else None,
        )
    )
    actual_table = YtTable.make("k", "primary", YtTable.Type.TABLE, table_path, False, dict())
    actual_table.replication_collocation_id = str(uuid.uuid4())

    replica_id = str(uuid.uuid4())
    replication_card_id = str(uuid.uuid4())
    yt_client_factory = MockYtClientFactory(
        {
            "primary": {
                "create": {table_path: MockResult(result=True), replica_type: MockResult(result=replica_id)},
                "get": {
                    f"{table_path}/@replication_card_id": MockResult(result=replication_card_id),
                    f"{table_path}/@replicated_table_options": MockResult(result=_RTT_OPTIONS),
                },
            },
        }
    )
    create_action = CreateReplicatedTableAction(desired_table, actual_table)
    create_action.dry_run = dry_run

    yt_client = yt_client_factory("primary")
    assert create_action.schedule_next(yt_client) is True
    create_action.process()

    call_tracker = yt_client_factory.get_call_tracker("primary")
    assert call_tracker

    create_calls = call_tracker.calls["create"]

    def _get_create_calls(path_or_type: str) -> list[CallTracker.Call]:
        return [c for c in create_calls if c.path_or_type == path_or_type]

    create_table_calls = _get_create_calls(table_path)
    assert 1 == len(create_table_calls)
    assert 0 == len(_get_create_calls(replica_type))

    assert create_action.schedule_next(yt_client) is True
    create_action.process()

    create_replica_calls = _get_create_calls(replica_type)
    assert 2 == len(create_replica_calls)
    for call in create_replica_calls:
        assert call.kwargs
        assert "attributes" in call.kwargs
        attrs = call.kwargs["attributes"]
        assert attrs["cluster_name"] in ("remote0", "remote1")
        assert table_path == attrs["replica_path"]
        assert attrs["enabled"] is True
        assert attrs["mode"] in ("sync", "async")
        if is_chaos:
            assert attrs["content_type"] in ("data", "queue")
        else:
            assert "content_type" not in attrs

    assert actual_table.exists
    assert actual_table.attributes == desired_table.attributes
    assert actual_table.table_type == desired_table.table_type
    assert actual_table.schema == desired_table.schema
    assert actual_table.replication_collocation_id is None
    assert 2 == len(actual_table.replicas)
    for replica in actual_table.replicas.values():
        assert replica_id == replica.replica_id

    assert create_action.schedule_next(yt_client) is False
    create_action.process()

    if dry_run:
        assert "get" not in call_tracker.calls
    else:
        assert len(call_tracker.calls["get"]) == 2 if is_chaos else 1
        _check_rtt_options(actual_table)
        if is_chaos:
            assert replication_card_id == actual_table.chaos_replication_card_id

    # no more calls
    with pytest.raises(AssertionError):
        create_action.schedule_next(yt_client)
    with pytest.raises(AssertionError):
        create_action.process()


@pytest.mark.parametrize(
    "table_type,replica_type",
    [
        (YtTable.Type.REPLICATED_TABLE, YtReplica.ReplicaType.TABLE_REPLICA),
        (YtTable.Type.CHAOS_REPLICATED_TABLE, YtReplica.ReplicaType.CHAOS_TABLE_REPLICA),
    ],
)
def test_create_replicated_table_over_existing_and_partial_replicas(
    table_path: str, table_attributes: Types.Attributes, table_type: str, replica_type: str
):
    desired_table = YtTable.make("k", "primary", table_type, table_path, True, table_attributes)
    actual_table = deepcopy(desired_table)
    is_chaos = YtTable.Type.CHAOS_REPLICATED_TABLE == table_type
    desired_table.add_replica(
        YtReplica(
            replica_id=None,
            cluster_name="remote0",
            replica_path=table_path,
            enabled=True,
            mode=YtReplica.Mode.SYNC,
            enable_replicated_table_tracker=True,
            content_type=YtReplica.ContentType.DATA if is_chaos else None,
        )
    )
    desired_table.add_replica(
        YtReplica(
            replica_id=None,
            cluster_name="remote1",
            replica_path=table_path,
            enabled=True,
            mode=YtReplica.Mode.ASYNC,
            enable_replicated_table_tracker=True,
            content_type=YtReplica.ContentType.QUEUE if is_chaos else None,
        )
    )

    replica_id = str(uuid.uuid4())
    actual_table.add_replica(
        YtReplica(
            replica_id=replica_id,
            cluster_name="remote0",
            replica_path=table_path,
            enabled=True,
            mode=YtReplica.Mode.SYNC,
            enable_replicated_table_tracker=True,
            content_type=YtReplica.ContentType.DATA if is_chaos else None,
        )
    )

    replication_card_id = str(uuid.uuid4())
    yt_client_factory = MockYtClientFactory(
        {
            "primary": {
                "create": {table_path: MockResult(result=True), replica_type: MockResult(result=replica_id)},
                "get": {
                    f"{table_path}/@replication_card_id": MockResult(result=replication_card_id),
                    f"{table_path}/@replicated_table_options": MockResult(result=_RTT_OPTIONS),
                },
            }
        }
    )
    create_action = CreateReplicatedTableAction(desired_table, actual_table)

    yt_client = yt_client_factory("primary")
    assert create_action.schedule_next(yt_client) is True
    create_action.process()

    call_tracker = yt_client_factory.get_call_tracker("primary")
    assert call_tracker
    assert "create" not in call_tracker.calls

    assert create_action.schedule_next(yt_client) is True
    create_action.process()

    create_calls = call_tracker.calls["create"]
    assert 1 == len(create_calls)
    for call in create_calls:
        assert replica_type == call.path_or_type
        assert call.kwargs
        assert "attributes" in call.kwargs
        attrs = call.kwargs["attributes"]
        assert attrs["cluster_name"] in ("remote0", "remote1")
        assert table_path == attrs["replica_path"]
        assert attrs["enabled"] is True
        assert attrs["mode"] in ("sync", "async")
        if is_chaos:
            assert attrs["content_type"] in ("data", "queue")
        else:
            assert "content_type" not in attrs

    assert actual_table.exists
    assert actual_table.attributes == desired_table.attributes
    assert actual_table.table_type == desired_table.table_type
    assert actual_table.schema == desired_table.schema
    assert 2 == len(actual_table.replicas)
    for replica in actual_table.replicas.values():
        assert replica_id == replica.replica_id

    assert create_action.schedule_next(yt_client) is False
    create_action.process()

    assert len(call_tracker.calls["get"]) == 2 if is_chaos else 1
    _check_rtt_options(actual_table)
    if is_chaos:
        assert replication_card_id == actual_table.chaos_replication_card_id

    # no more calls
    with pytest.raises(AssertionError):
        create_action.schedule_next(yt_client)
    with pytest.raises(AssertionError):
        create_action.process()
