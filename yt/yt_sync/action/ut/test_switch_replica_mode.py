from uuid import uuid4

import pytest

import yt.wrapper as yt
from yt.yt_sync.action.switch_replica_mode import SwitchReplicaModeAction
from yt.yt_sync.core.model import Types
from yt.yt_sync.core.model import YtReplica
from yt.yt_sync.core.model import YtTable
from yt.yt_sync.core.model.helpers import make_log_name
from yt.yt_sync.core.model.table import RttOptions
from yt.yt_sync.core.test_lib import CallTracker
from yt.yt_sync.core.test_lib import MockResult
from yt.yt_sync.core.test_lib import MockYtClientFactory
from yt.yt_sync.core.test_lib import yt_generic_error

_RTT_OPTIONS = {
    "enable_replicated_table_tracker": True,
    "enable_preload_state_check": True,
    "min_sync_replica_count": 1,
    "max_sync_replica_count": 1,
}


@pytest.fixture
def yt_table(table_path: str, default_schema: Types.Schema) -> YtTable:
    table = YtTable.make("k", "primary", YtTable.Type.REPLICATED_TABLE, table_path, True, {"schema": default_schema})
    table.fill_replicas(
        {
            str(uuid4()): {
                "cluster_name": "remote0",
                "replica_path": table_path,
                "state": "enabled",
                "mode": YtReplica.Mode.SYNC,
            },
            str(uuid4()): {
                "cluster_name": "remote1",
                "replica_path": table_path,
                "state": "enabled",
                "mode": YtReplica.Mode.ASYNC,
            },
        }
    )

    return table


@pytest.fixture
def chaos_table(table_path: str, default_schema: Types.Schema) -> YtTable:
    table = YtTable.make(
        "k", "primary", YtTable.Type.CHAOS_REPLICATED_TABLE, table_path, True, {"schema": default_schema}
    )
    table.fill_replicas(
        {
            str(uuid4()): {
                "cluster_name": "remote0",
                "replica_path": table_path,
                "state": "enabled",
                "mode": YtReplica.Mode.SYNC,
                "content_type": YtReplica.ContentType.DATA,
            },
            str(uuid4()): {
                "cluster_name": "remote0",
                "replica_path": make_log_name(table.path),
                "state": "enabled",
                "mode": YtReplica.Mode.SYNC,
                "content_type": YtReplica.ContentType.QUEUE,
            },
            str(uuid4()): {
                "cluster_name": "remote1",
                "replica_path": table_path,
                "state": "enabled",
                "mode": YtReplica.Mode.ASYNC,
                "content_type": YtReplica.ContentType.DATA,
            },
            str(uuid4()): {
                "cluster_name": "remote1",
                "replica_path": make_log_name(table.path),
                "state": "enabled",
                "mode": YtReplica.Mode.ASYNC,
                "content_type": YtReplica.ContentType.QUEUE,
            },
        }
    )
    table.chaos_replication_card_id = str(uuid4())

    return table


def test_create_not_replicated(yt_table: YtTable):
    yt_table.table_type = YtTable.Type.TABLE
    with pytest.raises(AssertionError):
        SwitchReplicaModeAction(yt_table, {"remote0"})


def test_process_before_schedule(yt_table: YtTable):
    action = SwitchReplicaModeAction(yt_table, {"remote0"})
    with pytest.raises(AssertionError):
        action.process()


@pytest.mark.parametrize("sync_replicas", [set(), {"replica_1"}, {"replica_0", "replica_1"}])
def test_switch_error(yt_table: YtTable, sync_replicas: set[str]):
    yt_client_factory = _yt_client_factory(yt_table, True)
    yt_client = yt_client_factory("primary")
    action = SwitchReplicaModeAction(yt_table, sync_replicas)
    assert action.schedule_next(yt_client) is False
    with pytest.raises(yt.YtResponseError):
        action.process()


def test_switch_to_mode(yt_table: YtTable):
    sync_replicas: set[str] = {"remote0", "remote1"}
    yt_client_factory = _yt_client_factory(yt_table)
    yt_client = yt_client_factory("primary")
    action = SwitchReplicaModeAction(yt_table, sync_replicas)
    assert action.schedule_next(yt_client) is False

    call = _get_call(yt_client_factory, "alter_table_replica")
    assert call.kwargs
    assert YtReplica.Mode.SYNC == call.kwargs["mode"]

    action.process()

    for cluster_name in ("remote0", "remote1"):
        replica = next(iter(yt_table.get_replicas_for(cluster_name)))
        assert YtReplica.Mode.SYNC == replica.mode


def test_switch_non_existing_replicas(yt_table: YtTable):
    for r in yt_table.replicas.values():
        r.exists = False

    yt_client_factory = _yt_client_factory(yt_table)
    yt_client = yt_client_factory("primary")
    action = SwitchReplicaModeAction(yt_table, {"remote0", "remote1"})
    assert action.schedule_next(yt_client) is False

    _assert_no_calls(yt_client_factory, "alter_table_replica")

    action.process()

    for cluster_name, mode in (("remote0", YtReplica.Mode.SYNC), ("remote1", YtReplica.Mode.ASYNC)):
        replica = next(iter(yt_table.get_replicas_for(cluster_name)))
        assert mode == replica.mode


def test_flip_replicas(yt_table: YtTable):
    sync_replicas: set[str] = {"remote1"}
    yt_client_factory = _yt_client_factory(yt_table)
    yt_client = yt_client_factory("primary")
    action = SwitchReplicaModeAction(yt_table, sync_replicas)
    assert action.schedule_next(yt_client) is False

    call_tracker = yt_client_factory.get_call_tracker("primary")
    assert call_tracker
    calls = call_tracker.calls["alter_table_replica"]
    assert 2 == len(calls)
    for cluster_name, mode in (("remote0", YtReplica.Mode.ASYNC), ("remote1", YtReplica.Mode.SYNC)):
        replica = next(iter(yt_table.get_replicas_for(cluster_name)))
        call = next(iter([c for c in calls if c.path_or_type == replica.replica_id]), None)
        assert call
        assert call.kwargs
        assert mode == call.kwargs["mode"]

    action.process()

    for cluster_name, mode in (("remote0", YtReplica.Mode.ASYNC), ("remote1", YtReplica.Mode.SYNC)):
        replica = next(iter(yt_table.get_replicas_for(cluster_name)))
        assert mode == replica.mode


def test_nothing_to_switch(yt_table: YtTable):
    sync_replicas: set[str] = {"remote0"}
    yt_client_factory = _yt_client_factory(yt_table)
    yt_client = yt_client_factory("primary")

    action = SwitchReplicaModeAction(yt_table, sync_replicas)
    assert action.schedule_next(yt_client) is False

    _assert_no_calls(yt_client_factory, "alter_table_replica")

    action.process()

    for cluster_name, mode in (("remote0", YtReplica.Mode.SYNC), ("remote1", YtReplica.Mode.ASYNC)):
        replica = next(iter(yt_table.get_replicas_for(cluster_name)))
        assert mode == replica.mode


@pytest.mark.parametrize("sync_replicas", [set(), {"remote0", "remote1"}])
def test_switch_to_mode_rtt(yt_table: YtTable, sync_replicas: set[str]):
    _setup_for_rtt(yt_table)

    yt_client_factory = _yt_client_factory_rtt(yt_table)
    yt_client = yt_client_factory("primary")

    action = SwitchReplicaModeAction(yt_table, sync_replicas)
    assert action.schedule_next(yt_client) is False

    call = _get_call(yt_client_factory, "set")
    assert call.args
    requested_rtt_options = dict(call.args[0])
    expected_rtt_opts = _RTT_OPTIONS
    if sync_replicas:
        expected_rtt_opts["preferred_sync_replica_clusters"] = sorted(sync_replicas)
        assert expected_rtt_opts == requested_rtt_options
    else:
        expected_rtt_opts["preferred_sync_replica_clusters"] = []
    assert expected_rtt_opts == requested_rtt_options

    action.process()

    actual_sync_replicas = set([r.cluster_name for r in yt_table.replicas.values() if YtReplica.Mode.SYNC == r.mode])
    assert sync_replicas == actual_sync_replicas
    actual_preferred_replicas = yt_table.rtt_options.preferred_sync_replica_clusters
    assert actual_preferred_replicas is not None
    assert sync_replicas == set(actual_preferred_replicas)


@pytest.mark.parametrize("min_sync_replica_count", [None, 2])
@pytest.mark.parametrize("max_sync_replica_count", [None, 3])
def test_switch_to_mode_rtt_update_min_max_sync_count(
    yt_table: YtTable, min_sync_replica_count: int | None, max_sync_replica_count: int | None
):
    SYNC_REPLICAS: set[str] = set(["remote0", "remote1"])
    _setup_for_rtt(yt_table)

    yt_client_factory = _yt_client_factory_rtt(yt_table)
    yt_client = yt_client_factory("primary")

    action = SwitchReplicaModeAction(
        yt_table,
        SYNC_REPLICAS,
        min_sync_replica_count=min_sync_replica_count,
        max_sync_replica_count=max_sync_replica_count,
    )
    assert action.schedule_next(yt_client) is False

    call_tracker = yt_client_factory.get_call_tracker("primary")
    assert call_tracker
    assert len(call_tracker.calls["set"]) == 1
    replicas_call = call_tracker.calls["set"][0]

    assert replicas_call.args
    requested_rtt_options = dict(replicas_call.args[0])

    expected_rtt_opts = _RTT_OPTIONS
    if min_sync_replica_count is not None:
        expected_rtt_opts["min_sync_replica_count"] = min_sync_replica_count
    if max_sync_replica_count is not None:
        expected_rtt_opts["max_sync_replica_count"] = max_sync_replica_count

    assert expected_rtt_opts == requested_rtt_options

    action.process()

    actual_sync_replicas = set([r.cluster_name for r in yt_table.replicas.values() if YtReplica.Mode.SYNC == r.mode])
    assert SYNC_REPLICAS == actual_sync_replicas
    actual_preferred_replicas = yt_table.rtt_options.preferred_sync_replica_clusters
    assert actual_preferred_replicas is not None
    assert SYNC_REPLICAS == set(actual_preferred_replicas)
    assert yt_table.min_sync_replica_count == min_sync_replica_count if min_sync_replica_count else 1
    assert yt_table.max_sync_replica_count == max_sync_replica_count if max_sync_replica_count else 1


@pytest.mark.parametrize("force_fix_preferred", [False, True])
def test_switch_to_mode_rtt_fix_preferred(yt_table: YtTable, force_fix_preferred: bool):
    sync_replicas = set(["remote0"])
    _setup_for_rtt(yt_table)
    yt_table.rtt_options.preferred_sync_replica_clusters = {"remote1"}

    yt_client_factory = _yt_client_factory_rtt(yt_table)
    yt_client = yt_client_factory("primary")

    action = SwitchReplicaModeAction(yt_table, sync_replicas, force_fix_preferred=force_fix_preferred)
    assert action.schedule_next(yt_client) is False

    if force_fix_preferred:
        expected_rtt_opts = _RTT_OPTIONS
        expected_rtt_opts["preferred_sync_replica_clusters"] = sorted(sync_replicas)

        call = _get_call(yt_client_factory, "set")
        assert call.args
        requested_rtt_opts = dict(call.args[0])

        assert expected_rtt_opts == requested_rtt_opts
    else:
        _assert_no_calls(yt_client_factory, "set")

    action.process()
    for cluster_name, mode in (("remote0", YtReplica.Mode.SYNC), ("remote1", YtReplica.Mode.ASYNC)):
        replica = next(iter(yt_table.get_replicas_for(cluster_name)))
        assert mode == replica.mode


def test_nothing_to_switch_rtt(yt_table: YtTable):
    sync_replicas: set[str] = {"remote0"}
    _setup_for_rtt(yt_table)

    yt_client_factory = _yt_client_factory_rtt(yt_table)
    yt_client = yt_client_factory("primary")

    action = SwitchReplicaModeAction(yt_table, sync_replicas)
    assert action.schedule_next(yt_client) is False

    _assert_no_calls(yt_client_factory, "set")

    action.process()
    for cluster_name, mode in (("remote0", YtReplica.Mode.SYNC), ("remote1", YtReplica.Mode.ASYNC)):
        replica = next(iter(yt_table.get_replicas_for(cluster_name)))
        assert mode == replica.mode


def test_switch_error_rtt(yt_table: YtTable):
    sync_replicas: set[str] = {"remote1"}
    _setup_for_rtt(yt_table)

    yt_client_factory = _yt_client_factory_rtt(yt_table, True)
    yt_client = yt_client_factory("primary")
    action = SwitchReplicaModeAction(yt_table, sync_replicas)
    assert action.schedule_next(yt_client) is False
    with pytest.raises(yt.YtResponseError):
        action.process()


@pytest.mark.parametrize("content_type", [YtReplica.ContentType.DATA, YtReplica.ContentType.QUEUE])
def test_switch_with_content_type(chaos_table: YtTable, content_type: str):
    yt_client_factory = _yt_client_factory(chaos_table)
    yt_client = yt_client_factory("primary")
    action = SwitchReplicaModeAction(chaos_table, {"remote0", "remote1"}, {content_type})
    assert action.schedule_next(yt_client) is False

    call = _get_call(yt_client_factory, "alter_table_replica")
    assert call.kwargs
    assert YtReplica.Mode.SYNC == call.kwargs["mode"]

    action.process()

    seen_replicas = 0
    for cluster_name in ("remote0", "remote1"):
        for replica in [r for r in chaos_table.get_replicas_for(cluster_name) if r.content_type == content_type]:
            assert YtReplica.Mode.SYNC == replica.mode
            seen_replicas += 1
    assert 2 == seen_replicas


@pytest.mark.parametrize("sync_replicas", [set(), {"remote0", "remote1"}])
def test_switch_to_mode_rtt_chaos(chaos_table: YtTable, sync_replicas: set[str]):
    _setup_for_rtt(chaos_table)

    yt_client_factory = _yt_client_factory_rtt(chaos_table)
    yt_client = yt_client_factory("primary")

    action = SwitchReplicaModeAction(chaos_table, sync_replicas)
    assert action.schedule_next(yt_client) is False

    call = _get_call(yt_client_factory, "alter_replication_card")
    assert chaos_table.chaos_replication_card_id == call.path_or_type
    assert call.kwargs
    assert "replicated_table_options" in call.kwargs
    assert sync_replicas == set(call.kwargs["replicated_table_options"]["preferred_sync_replica_clusters"])

    action.process()

    assert sync_replicas == chaos_table.preferred_sync_replicas
    assert sync_replicas == chaos_table.rtt_options.preferred_sync_replica_clusters or set()


@pytest.mark.parametrize("min_sync_replica_count", [None, 2])
@pytest.mark.parametrize("max_sync_replica_count", [None, 3])
def test_switch_to_mode_rtt_chaos_update_min_max_sync_count(
    chaos_table: YtTable, min_sync_replica_count: int | None, max_sync_replica_count: int | None
):
    SYNC_REPLICAS: set[str] = set(["remote0", "remote1"])
    _setup_for_rtt(chaos_table)

    yt_client_factory = _yt_client_factory_rtt(chaos_table)
    yt_client = yt_client_factory("primary")

    action = SwitchReplicaModeAction(
        chaos_table,
        SYNC_REPLICAS,
        min_sync_replica_count=min_sync_replica_count,
        max_sync_replica_count=max_sync_replica_count,
    )
    assert action.schedule_next(yt_client) is False

    call = _get_call(yt_client_factory, "alter_replication_card")
    assert chaos_table.chaos_replication_card_id == call.path_or_type
    assert call.kwargs
    assert "replicated_table_options" in call.kwargs
    rtt_opts_data = call.kwargs["replicated_table_options"]

    expected_rtt_opts = _RTT_OPTIONS
    expected_rtt_opts["preferred_sync_replica_clusters"] = sorted(SYNC_REPLICAS)
    if min_sync_replica_count is not None:
        expected_rtt_opts["min_sync_replica_count"] = min_sync_replica_count
    if max_sync_replica_count is not None:
        expected_rtt_opts["max_sync_replica_count"] = max_sync_replica_count

    assert rtt_opts_data == expected_rtt_opts

    action.process()

    assert SYNC_REPLICAS == chaos_table.preferred_sync_replicas
    assert SYNC_REPLICAS == chaos_table.rtt_options.preferred_sync_replica_clusters
    assert chaos_table.min_sync_replica_count == min_sync_replica_count if min_sync_replica_count else 1
    assert chaos_table.max_sync_replica_count == max_sync_replica_count if max_sync_replica_count else 1


@pytest.mark.parametrize("sync_replica", ["remote0", "remote1"])
def test_switch_to_mode_rtt_non_strict(yt_table: YtTable, sync_replica: str):
    _setup_for_rtt(yt_table)
    for replica in yt_table.replicas.values():
        replica.mode = YtReplica.Mode.SYNC

    yt_client_factory = _yt_client_factory_rtt(yt_table)
    yt_client = yt_client_factory("primary")

    action = SwitchReplicaModeAction(yt_table, {sync_replica}, strict_preferred=False)
    assert action.schedule_next(yt_client) is False

    _assert_no_calls(yt_client_factory, "set")


def _setup_for_rtt(yt_table: YtTable):
    yt_table.is_rtt_enabled = True
    for replica in yt_table.replicas.values():
        replica.enable_replicated_table_tracker = True
    yt_table.rtt_options = RttOptions.make(_RTT_OPTIONS)


def _yt_client_factory(yt_table: YtTable, with_error: bool = False) -> MockYtClientFactory:
    result = MockResult(error=yt_generic_error()) if with_error else MockResult(result=True)
    call_results: dict[str, MockResult] = {
        r.replica_id: result for r in yt_table.replicas.values()  # type:ignore
    }
    return MockYtClientFactory({"primary": {"alter_table_replica": call_results}})


def _yt_client_factory_rtt(yt_table: YtTable, with_error: bool = False) -> MockYtClientFactory:
    result = MockResult(error=yt_generic_error()) if with_error else MockResult(result=True)
    if yt_table.is_chaos_replicated:
        assert yt_table.chaos_replication_card_id
        return MockYtClientFactory(
            {"primary": {"alter_replication_card": {yt_table.chaos_replication_card_id: result}}}
        )
    else:
        return MockYtClientFactory(
            {
                "primary": {
                    "set": {
                        f"{yt_table.path}&/@replicated_table_options": result,
                    }
                }
            }
        )


def _get_call(yt_client_factory: MockYtClientFactory, method: str) -> CallTracker.Call:
    call_tracker = yt_client_factory.get_call_tracker("primary")
    assert call_tracker
    assert 1 == len(call_tracker.calls[method])
    return call_tracker.calls[method][0]


def _assert_no_calls(yt_client_factory: MockYtClientFactory, method: str):
    call_tracker = yt_client_factory.get_call_tracker("primary")
    assert call_tracker
    assert method not in call_tracker.calls
