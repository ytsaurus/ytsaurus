import uuid

import pytest

import yt.wrapper as yt
from yt.yt_sync.action.switch_replica_state import SwitchReplicaStateAction
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


def test_not_replicated(yt_table: YtTable):
    yt_table.table_type = YtTable.Type.TABLE
    with pytest.raises(AssertionError):
        SwitchReplicaStateAction(yt_table, True)


def test_process_before_schedule(yt_table: YtTable):
    with pytest.raises(AssertionError):
        SwitchReplicaStateAction(yt_table, True).process()


@pytest.mark.parametrize("enabled", [True, False])
def test_already_desired_state(yt_table: YtTable, enabled: bool):
    _setup_replica_state(yt_table, enabled)
    action = SwitchReplicaStateAction(yt_table, enabled)

    yt_client_factory = MockYtClientFactory({yt_table.cluster_name: {}})
    yt_client = yt_client_factory(yt_table.cluster_name)

    assert action.schedule_next(yt_client) is False

    call_tracker = yt_client_factory.get_call_tracker(yt_table.cluster_name)
    assert call_tracker
    assert "alter_table_replica" not in call_tracker.calls


@pytest.mark.parametrize("enabled", [True, False])
def test_non_existing_replica(yt_table: YtTable, enabled: bool):
    _setup_replica_state(yt_table, not enabled, False)
    action = SwitchReplicaStateAction(yt_table, enabled)

    yt_client_factory = MockYtClientFactory({yt_table.cluster_name: {}})
    yt_client = yt_client_factory(yt_table.cluster_name)

    assert action.schedule_next(yt_client) is False

    call_tracker = yt_client_factory.get_call_tracker(yt_table.cluster_name)
    assert call_tracker
    assert "alter_table_replica" not in call_tracker.calls


@pytest.mark.parametrize("enabled", [True, False])
def test_success(yt_table: YtTable, enabled: bool):
    _setup_replica_state(yt_table, not enabled)
    action = SwitchReplicaStateAction(yt_table, enabled)

    desired_state = YtReplica.State.ENABLED if enabled else YtReplica.State.DISABLED

    alter_responses = dict()
    get_responses = dict()

    for replica in yt_table.replicas.values():
        alter_responses[replica.replica_id] = MockResult(result=True)
        get_responses[f"#{replica.replica_id}/@state"] = MockResult(result=desired_state)

    yt_client_factory = MockYtClientFactory(
        {
            yt_table.cluster_name: {
                "alter_table_replica": alter_responses,
                "get": get_responses,
            }
        }
    )
    yt_client = yt_client_factory(yt_table.cluster_name)

    assert action.schedule_next(yt_client) is True  # modify
    action.process()
    assert action.schedule_next(yt_client) is True  # check
    action.process()
    assert action.schedule_next(yt_client) is False  # ready
    action.process()

    for replica in yt_table.replicas.values():
        assert enabled == replica.enabled


@pytest.mark.parametrize("enabled", [True, False])
def test_success_single_replica(yt_table: YtTable, table_attributes: Types.Attributes, enabled: bool):
    _setup_replica_state(yt_table, not enabled)
    data_table = YtTable.make("k", "r0", YtTable.Type.TABLE, yt_table.path, True, table_attributes)
    replica = next(iter(yt_table.get_replicas_for("r0")))
    assert replica.replica_id

    desired_state = YtReplica.State.ENABLED if enabled else YtReplica.State.DISABLED

    alter_responses: dict[str, MockResult] = {replica.replica_id: MockResult(result=True)}
    get_responses: dict[str, MockResult] = {f"#{replica.replica_id}/@state": MockResult(result=desired_state)}

    yt_client_factory = MockYtClientFactory(
        {
            yt_table.cluster_name: {
                "alter_table_replica": alter_responses,
                "get": get_responses,
            }
        }
    )
    yt_client = yt_client_factory(yt_table.cluster_name)

    action = SwitchReplicaStateAction(yt_table, enabled, data_table)

    assert action.schedule_next(yt_client) is True  # modify
    action.process()
    assert action.schedule_next(yt_client) is True  # check
    action.process()
    assert action.schedule_next(yt_client) is False  # ready
    action.process()

    assert enabled == replica.enabled


@pytest.mark.parametrize("enabled", [True, False])
def test_fail_alter(yt_table: YtTable, enabled: bool):
    _setup_replica_state(yt_table, not enabled)
    action = SwitchReplicaStateAction(yt_table, enabled)

    desired_state = YtReplica.State.ENABLED if enabled else YtReplica.State.DISABLED

    alter_responses = dict()
    get_responses = dict()

    for replica in yt_table.replicas.values():
        alter_responses[replica.replica_id] = MockResult(error=yt_generic_error())
        get_responses[f"#{replica.replica_id}/@state"] = MockResult(result=desired_state)

    yt_client_factory = MockYtClientFactory(
        {
            yt_table.cluster_name: {
                "alter_table_replica": alter_responses,
                "get": get_responses,
            }
        }
    )
    yt_client = yt_client_factory(yt_table.cluster_name)

    assert action.schedule_next(yt_client) is True  # modify
    with pytest.raises(yt.YtResponseError):
        action.process()


@pytest.mark.parametrize("enabled", [True, False])
def test_fail_get(yt_table: YtTable, enabled: bool):
    _setup_replica_state(yt_table, not enabled)
    action = SwitchReplicaStateAction(yt_table, enabled)

    alter_responses = dict()
    get_responses = dict()

    for replica in yt_table.replicas.values():
        alter_responses[replica.replica_id] = MockResult(result=True)
        get_responses[f"#{replica.replica_id}/@state"] = MockResult(error=yt_generic_error())

    yt_client_factory = MockYtClientFactory(
        {
            yt_table.cluster_name: {
                "alter_table_replica": alter_responses,
                "get": get_responses,
            }
        }
    )
    yt_client = yt_client_factory(yt_table.cluster_name)

    assert action.schedule_next(yt_client) is True  # modify
    action.process()
    assert action.schedule_next(yt_client) is True  # check
    with pytest.raises(yt.YtResponseError):
        action.process()


def _setup_replica_state(yt_table: YtTable, enabled: bool, exists: bool = True):
    for replica in yt_table.replicas.values():
        replica.enabled = enabled
        replica.exists = exists
