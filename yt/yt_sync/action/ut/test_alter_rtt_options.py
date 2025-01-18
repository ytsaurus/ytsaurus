from copy import deepcopy
import uuid

import pytest

import yt.wrapper as yt
from yt.yt_sync.action.alter_rtt_options import AlterRttOptionsAction
from yt.yt_sync.core.model import Types
from yt.yt_sync.core.model import YtReplica
from yt.yt_sync.core.model import YtTable
from yt.yt_sync.core.test_lib import CallTracker
from yt.yt_sync.core.test_lib import MockResult
from yt.yt_sync.core.test_lib import MockYtClientFactory
from yt.yt_sync.core.test_lib import yt_generic_error


@pytest.fixture
def cluster_name() -> str:
    return "primary"


@pytest.fixture
def desired_table(cluster_name: str, table_path: str, default_schema: Types.Schema) -> YtTable:
    table = YtTable.make("k", cluster_name, YtTable.Type.REPLICATED_TABLE, table_path, True, {"schema": default_schema})
    table.rtt_options.enabled = True
    table.rtt_options.preferred_sync_replica_clusters = {"remote0"}
    for replica_cluster in ("remote0", "remote1"):
        replica = YtReplica(
            replica_id=str(uuid.uuid4()),
            cluster_name=replica_cluster,
            replica_path=table_path,
            enabled=True,
            mode=YtReplica.Mode.SYNC if replica_cluster == "remote0" else YtReplica.Mode.ASYNC,
            enable_replicated_table_tracker=True,
            exists=True,
        )
        table.add_replica(replica)
    return table


@pytest.fixture
def actual_table(desired_table: YtTable) -> YtTable:
    return deepcopy(desired_table)


def test_process_before_schedule(desired_table: YtTable, actual_table: YtTable):
    action = AlterRttOptionsAction(desired_table, actual_table)
    with pytest.raises(AssertionError):
        action.process()


def test_schedule_twice(desired_table: YtTable, actual_table: YtTable):
    yt_client_factory = MockYtClientFactory({desired_table.cluster_name: {}})
    yt_client = yt_client_factory(desired_table.cluster_name)
    action = AlterRttOptionsAction(desired_table, actual_table)
    action.schedule_next(yt_client)
    with pytest.raises(AssertionError):
        action.schedule_next(yt_client)


def test_alter_fail(desired_table: YtTable, actual_table: YtTable):
    yt_client_factory = MockYtClientFactory(
        {
            desired_table.cluster_name: {
                "set": {f"{desired_table.path}&/@replicated_table_options": MockResult(error=yt_generic_error())}
            }
        }
    )
    yt_client = yt_client_factory(desired_table.cluster_name)
    desired_table.rtt_options.max_sync_replica_count = 3
    action = AlterRttOptionsAction(desired_table, actual_table)
    assert action.schedule_next(yt_client) is False
    with pytest.raises(yt.YtResponseError):
        action.process()


def test_alter_no_attr_change(desired_table: YtTable, actual_table: YtTable):
    yt_client_factory = MockYtClientFactory({desired_table.cluster_name: {}})
    yt_client = yt_client_factory(desired_table.cluster_name)
    desired_table.rtt_options.max_sync_replica_count = 2
    actual_table.rtt_options.max_sync_replica_count = 2

    action = AlterRttOptionsAction(desired_table, actual_table)
    assert action.schedule_next(yt_client) is False

    _assert_no_call(yt_client_factory.get_call_tracker(desired_table.cluster_name))

    action.process()
    assert actual_table.rtt_options.max_sync_replica_count == 2
    assert actual_table.need_remount is False


def test_alter_no_explicit_preferred_sync_replicas_change(desired_table: YtTable, actual_table: YtTable):
    yt_client_factory = MockYtClientFactory({desired_table.cluster_name: {}})
    yt_client = yt_client_factory(desired_table.cluster_name)
    desired_table.rtt_options.preferred_sync_replica_clusters = {"remote1"}  # default is remote0

    action = AlterRttOptionsAction(desired_table, actual_table)
    assert action.schedule_next(yt_client) is False

    _assert_no_call(yt_client_factory.get_call_tracker(desired_table.cluster_name))

    action.process()
    assert actual_table.rtt_options.preferred_sync_replica_clusters == {"remote0"}
    assert actual_table.need_remount is False


def test_alter_implicit_preferred_sync_replicas_change(desired_table: YtTable, actual_table: YtTable):
    yt_client_factory = MockYtClientFactory(
        {
            desired_table.cluster_name: {
                "set": {f"{desired_table.path}&/@replicated_table_options": MockResult(result=True)}
            }
        }
    )

    yt_client = yt_client_factory(desired_table.cluster_name)

    for replica in desired_table.replicas.values():
        replica.mode = YtReplica.Mode.SYNC if replica.cluster_name == "remote1" else YtReplica.Mode.ASYNC

    action = AlterRttOptionsAction(desired_table, actual_table)
    assert action.schedule_next(yt_client) is False

    _assert_set_call(yt_client_factory.get_call_tracker(desired_table.cluster_name))

    action.process()
    assert actual_table.rtt_options.preferred_sync_replica_clusters == {"remote1"}
    assert actual_table.need_remount is True


def test_alter_over_existing_attributes(desired_table: YtTable, actual_table: YtTable):
    yt_client_factory = MockYtClientFactory(
        {
            desired_table.cluster_name: {
                "set": {f"{desired_table.path}&/@replicated_table_options": MockResult(result=True)}
            }
        }
    )
    yt_client = yt_client_factory(desired_table.cluster_name)

    actual_table.rtt_options.attributes["attr1"] = "val1"
    actual_table.rtt_options.attributes["attr2"] = "val2"
    actual_table.rtt_options.min_sync_replica_count = 1

    desired_table.rtt_options.attributes["attr1"] = "val1_changed"
    desired_table.rtt_options.min_sync_replica_count = 2

    action = AlterRttOptionsAction(desired_table, actual_table)
    assert action.schedule_next(yt_client) is False

    _assert_set_call(yt_client_factory.get_call_tracker(desired_table.cluster_name))

    action.process()
    assert actual_table.rtt_options.attributes["attr1"] == "val1_changed"
    assert actual_table.rtt_options.attributes["attr2"] == "val2"
    assert actual_table.rtt_options.min_sync_replica_count == 2
    assert actual_table.need_remount is True


def test_alter_chaos_table(desired_table: YtTable, actual_table: YtTable):
    desired_table.table_type = YtTable.Type.CHAOS_REPLICATED_TABLE
    desired_table.rtt_options.max_sync_replica_count = 1
    actual_table.table_type = YtTable.Type.CHAOS_REPLICATED_TABLE
    actual_table.chaos_replication_card_id = str(uuid.uuid4())

    yt_client_factory = MockYtClientFactory(
        {
            desired_table.cluster_name: {
                "alter_replication_card": {actual_table.chaos_replication_card_id: MockResult(result=True)}
            }
        }
    )
    yt_client = yt_client_factory(desired_table.cluster_name)

    action = AlterRttOptionsAction(desired_table, actual_table)
    assert action.schedule_next(yt_client) is False

    call_tracker = yt_client_factory.get_call_tracker(desired_table.cluster_name)
    assert call_tracker
    assert len(call_tracker.calls["alter_replication_card"]) == 1

    action.process()
    assert actual_table.rtt_options.max_sync_replica_count == 1
    assert actual_table.need_remount is True


def _assert_no_call(call_tracker: CallTracker | None):
    assert call_tracker
    assert "set" not in call_tracker.calls


def _assert_set_call(call_tracker: CallTracker | None):
    assert call_tracker
    assert 1 == len(call_tracker.calls["set"])
    call = call_tracker.calls["set"][0]
    assert call.path_or_type.endswith("/@replicated_table_options")
