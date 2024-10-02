from copy import deepcopy
from typing import Any

import pytest

import yt.wrapper as yt
from yt.yt_sync.action.alter_table_attributes import AlterTableAttributesAction
from yt.yt_sync.core.model import Types
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
    return YtTable.make("k", cluster_name, YtTable.Type.TABLE, table_path, True, {"schema": default_schema})


@pytest.fixture
def actual_table(desired_table: YtTable) -> YtTable:
    return deepcopy(desired_table)


def test_create_inconsistent_cluster(desired_table: YtTable, actual_table: YtTable):
    actual_table.cluster_name = "_unknown_"
    with pytest.raises(AssertionError):
        AlterTableAttributesAction(desired_table, actual_table)


def test_create_inconsistent_path(desired_table: YtTable, actual_table: YtTable):
    actual_table.path = "_unknown_"
    with pytest.raises(AssertionError):
        AlterTableAttributesAction(desired_table, actual_table)


def test_create_actual_not_exists(desired_table: YtTable, actual_table: YtTable):
    actual_table.exists = False
    with pytest.raises(AssertionError):
        AlterTableAttributesAction(desired_table, actual_table)


def test_process_before_schedule(desired_table: YtTable, actual_table: YtTable):
    action = AlterTableAttributesAction(desired_table, actual_table)
    with pytest.raises(AssertionError):
        action.process()


def test_schedule_twice(desired_table: YtTable, actual_table: YtTable):
    yt_client_factory = MockYtClientFactory({desired_table.cluster_name: {}})
    yt_client = yt_client_factory(desired_table.cluster_name)
    action = AlterTableAttributesAction(desired_table, actual_table)
    action.schedule_next(yt_client)
    with pytest.raises(AssertionError):
        action.schedule_next(yt_client)


def test_alter_fail(desired_table: YtTable, actual_table: YtTable):
    yt_client_factory = MockYtClientFactory(
        {desired_table.cluster_name: {"set": {f"{desired_table.path}&/@my_attr": MockResult(error=yt_generic_error())}}}
    )
    yt_client = yt_client_factory(desired_table.cluster_name)
    desired_table.attributes["my_attr"] = "1"
    action = AlterTableAttributesAction(desired_table, actual_table)
    assert action.schedule_next(yt_client) is False
    with pytest.raises(yt.YtResponseError):
        action.process()


@pytest.mark.parametrize("desired_value", [1, 0, True, False, "value", None])
def test_alter_add_attribute(desired_table: YtTable, actual_table: YtTable, desired_value: Any):
    attr_path = f"{desired_table.path}&/@my_attr"
    yt_client_factory = MockYtClientFactory({desired_table.cluster_name: {"set": {attr_path: MockResult(result=True)}}})
    yt_client = yt_client_factory(desired_table.cluster_name)
    desired_table.attributes["my_attr"] = desired_value

    action = AlterTableAttributesAction(desired_table, actual_table)
    assert action.schedule_next(yt_client) is False

    if desired_value is None:
        _assert_no_call(yt_client_factory.get_call_tracker(desired_table.cluster_name))
    else:
        _assert_set_call(yt_client_factory.get_call_tracker(desired_table.cluster_name), attr_path, desired_value)

    action.process()
    if desired_value is None:
        assert "my_attr" not in actual_table.attributes
    else:
        assert desired_value == actual_table.attributes["my_attr"]


@pytest.mark.parametrize("desired_value", [1, 0, True, False, "value"])
def test_alter_update_attribute(desired_table: YtTable, actual_table: YtTable, desired_value: Any):
    attr_path = f"{desired_table.path}&/@my_attr"
    yt_client_factory = MockYtClientFactory({desired_table.cluster_name: {"set": {attr_path: MockResult(result=True)}}})
    yt_client = yt_client_factory(desired_table.cluster_name)
    actual_table.attributes["my_attr"] = "2"
    desired_table.attributes["my_attr"] = desired_value

    action = AlterTableAttributesAction(desired_table, actual_table)
    assert action.schedule_next(yt_client) is False

    _assert_set_call(yt_client_factory.get_call_tracker(desired_table.cluster_name), attr_path, desired_value)

    action.process()
    assert desired_value == actual_table.attributes["my_attr"]


def test_alter_no_change(desired_table: YtTable, actual_table: YtTable):
    yt_client_factory = MockYtClientFactory({desired_table.cluster_name: {}})
    yt_client = yt_client_factory(desired_table.cluster_name)
    actual_table.attributes["my_attr"] = "1"
    desired_table.attributes["my_attr"] = "1"

    action = AlterTableAttributesAction(desired_table, actual_table)
    assert action.schedule_next(yt_client) is False

    _assert_no_call(yt_client_factory.get_call_tracker(desired_table.cluster_name))

    action.process()
    assert "1" == actual_table.attributes["my_attr"]


def test_alter_remove_attr(desired_table: YtTable, actual_table: YtTable):
    attr_path = f"{desired_table.path}&/@my_attr"
    yt_client_factory = MockYtClientFactory(
        {desired_table.cluster_name: {"remove": {attr_path: MockResult(result=True)}}}
    )
    yt_client = yt_client_factory(desired_table.cluster_name)
    desired_table.attributes["my_attr"] = None
    actual_table.attributes["my_attr"] = "1"

    action = AlterTableAttributesAction(desired_table, actual_table)
    assert action.schedule_next(yt_client) is False

    action.process()
    assert "my_attr" not in actual_table.attributes.attributes


def test_alter_preferred_sync_replicas(desired_table: YtTable, actual_table: YtTable):
    desired_table.table_type = YtTable.Type.REPLICATED_TABLE
    desired_table.rtt_options.preferred_sync_replica_clusters = {"remote0"}
    actual_table.table_type = YtTable.Type.REPLICATED_TABLE
    actual_table.rtt_options.preferred_sync_replica_clusters = {"remote1"}

    yt_client_factory = MockYtClientFactory({desired_table.cluster_name: {}})
    yt_client = yt_client_factory(desired_table.cluster_name)

    action = AlterTableAttributesAction(desired_table, actual_table)
    assert action.schedule_next(yt_client) is False

    call_tracker = yt_client_factory.get_call_tracker(desired_table.cluster_name)
    assert call_tracker
    assert "set" not in call_tracker.calls

    action.process()

    assert actual_table.rtt_options.preferred_sync_replica_clusters == {"remote1"}


def _assert_set_call(call_tracker: CallTracker | None, attr_path: str, desired_value: Any):
    assert call_tracker
    assert 1 == len(call_tracker.calls["set"])
    call = call_tracker.calls["set"][0]
    assert attr_path == call.path_or_type
    assert call.args
    assert desired_value == call.args[0]


def _assert_no_call(call_tracker: CallTracker | None):
    assert call_tracker
    assert "set" not in call_tracker.calls
