from copy import deepcopy
from typing import Callable

import pytest

from yt.yt_sync.action.reshard_table import ReshardTableAction
from yt.yt_sync.core.client import MockResult
from yt.yt_sync.core.model import Types
from yt.yt_sync.core.model import YtTable
from yt.yt_sync.core.test_lib import MockYtClientFactory


@pytest.fixture
def cluster_name() -> str:
    return "primary"


@pytest.fixture
def check_path(table_path: str) -> str:
    return f"{table_path}/@tablet_state"


@pytest.fixture
def yt_client_factory(cluster_name: str, table_path: str, check_path: str) -> MockYtClientFactory:
    return MockYtClientFactory(
        {
            cluster_name: {
                "reshard_table": {table_path: MockResult(result=True)},
                "get": {check_path: MockResult(result="mounted")},
            }
        }
    )


def _assert_reshard_table(
    cluster_name: str,
    table_path: str,
    table_attributes: Types.Attributes,
    yt_client_factory: MockYtClientFactory,
    assert_func: Callable[[Types.Attributes], None],
    check_path: str,
):
    desired_table = YtTable.make("k", cluster_name, YtTable.Type.TABLE, table_path, True, table_attributes)
    actual_table = deepcopy(desired_table)
    actual_table.tablet_info.tablet_count = None
    actual_table.tablet_info.pivot_keys = None
    yt_client = yt_client_factory(cluster_name)

    action = ReshardTableAction(desired_table, actual_table)

    assert action.schedule_next(yt_client) is True
    action.process()

    call_tracker = yt_client_factory.get_call_tracker(cluster_name)
    assert call_tracker
    reshard_calls = call_tracker.calls["reshard_table"]
    assert 1 == len(reshard_calls)
    assert table_path == reshard_calls[0].path_or_type
    assert_func(reshard_calls[0].kwargs)

    assert action.schedule_next(yt_client) is True
    action.process()

    get_calls = call_tracker.calls["get"]
    assert 1 == len(get_calls)
    assert check_path == get_calls[0].path_or_type

    assert action.schedule_next(yt_client) is False

    assert actual_table.tablet_count == desired_table.tablet_count
    assert actual_table.pivot_keys == desired_table.pivot_keys


def test_not_reshardable_table(
    cluster_name: str, table_path: str, default_schema: Types.Schema, yt_client_factory: MockYtClientFactory
):
    table_attributes = {"dynamic": True, "schema": default_schema}
    desired_table = YtTable.make("k", cluster_name, YtTable.Type.TABLE, table_path, True, table_attributes)
    actual_table = deepcopy(desired_table)

    yt_client = yt_client_factory(cluster_name)

    action = ReshardTableAction(desired_table, actual_table)
    assert action.schedule_next(yt_client) is False
    action.process()

    call_tracker = yt_client_factory.get_call_tracker(cluster_name)
    assert call_tracker
    assert "reshard_table" not in call_tracker.calls


def test_pivot_keys(
    cluster_name: str,
    table_path: str,
    default_schema: Types.Schema,
    check_path: str,
    yt_client_factory: MockYtClientFactory,
):
    pivot_keys = [[], [1], [1, 123], [1, 456]]
    table_attributes = {"dynamic": True, "schema": default_schema, "pivot_keys": pivot_keys}

    def _assert(kwargs: Types.Attributes):
        assert pivot_keys == kwargs["pivot_keys"]
        assert "tablet_count" not in kwargs
        assert "uniform" not in kwargs

    _assert_reshard_table(cluster_name, table_path, table_attributes, yt_client_factory, _assert, check_path)


def test_empty_pivot_keys(
    cluster_name: str,
    table_path: str,
    default_schema: Types.Schema,
    check_path: str,
    yt_client_factory: MockYtClientFactory,
):
    pivot_keys = [[]]
    tablet_count = 11
    table_attributes = {
        "dynamic": True,
        "schema": default_schema,
        "pivot_keys": pivot_keys,
        "tablet_count": tablet_count,
    }

    def _assert(kwargs: Types.Attributes):
        assert tablet_count == kwargs["tablet_count"]
        assert kwargs["uniform"] is True
        assert "pivot_keys" not in kwargs

    _assert_reshard_table(cluster_name, table_path, table_attributes, yt_client_factory, _assert, check_path)


def test_tablet_count_uniform(
    cluster_name: str,
    table_path: str,
    default_schema: Types.Schema,
    check_path: str,
    yt_client_factory: MockYtClientFactory,
):
    tablet_count = 11
    table_attributes = {"dynamic": True, "schema": default_schema, "tablet_count": tablet_count}

    def _assert(kwargs: Types.Attributes):
        assert tablet_count == kwargs["tablet_count"]
        assert kwargs["uniform"] is True
        assert "pivot_keys" not in kwargs

    _assert_reshard_table(cluster_name, table_path, table_attributes, yt_client_factory, _assert, check_path)


def test_tablet_count_nonuniform(
    cluster_name: str,
    table_path: str,
    check_path: str,
    yt_client_factory: MockYtClientFactory,
):
    tablet_count = 11
    table_attributes = {
        "dynamic": True,
        "schema": [{"name": "key", "type": "uint64"}, {"name": "value", "type": "uint64"}],
        "tablet_count": tablet_count,
    }

    def _assert(kwargs: Types.Attributes):
        assert tablet_count == kwargs["tablet_count"]
        assert kwargs["uniform"] is False
        assert "pivot_keys" not in kwargs

    _assert_reshard_table(cluster_name, table_path, table_attributes, yt_client_factory, _assert, check_path)


def test_pivot_keys_for_ordered(
    cluster_name: str,
    table_path: str,
    check_path: str,
    yt_client_factory: MockYtClientFactory,
):
    pivot_keys = [[], [1], [1, 123], [1, 456]]
    tablet_count = 11
    table_attributes = {
        "dynamic": True,
        "schema": [{"name": "key", "type": "uint64"}, {"name": "value", "type": "uint64"}],
        "tablet_count": tablet_count,
        "pivot_keys": pivot_keys,
    }

    def _assert(kwargs: Types.Attributes):
        assert tablet_count == kwargs["tablet_count"]
        assert kwargs["uniform"] is False
        assert "pivot_keys" not in kwargs

    _assert_reshard_table(cluster_name, table_path, table_attributes, yt_client_factory, _assert, check_path)


def test_reduce_tablet_count_for_ordered(
    cluster_name: str,
    table_path: str,
    ordered_schema: Types.Schema,
    yt_client_factory: MockYtClientFactory,
):
    desired_table = YtTable.make(
        table_path, cluster_name, YtTable.Type.TABLE, table_path, True, {"schema": ordered_schema, "tablet_count": 11}
    )
    actual_table = deepcopy(desired_table)
    actual_table.tablet_info.tablet_count = 12

    yt_client = yt_client_factory(cluster_name)

    action = ReshardTableAction(desired_table, actual_table)
    with pytest.raises(AssertionError):
        action.schedule_next(yt_client)


def test_reshard_not_ready(cluster_name: str, table_path: str, default_schema: Types.Schema, check_path: str):
    tablet_count = 11
    table_attributes = {"dynamic": True, "schema": default_schema, "tablet_count": tablet_count}
    desired_table = YtTable.make("k", cluster_name, YtTable.Type.TABLE, table_path, True, table_attributes)
    actual_table = deepcopy(desired_table)
    actual_table.tablet_info.tablet_count = None

    yt_client_factory = MockYtClientFactory(
        {
            cluster_name: {
                "reshard_table": {table_path: MockResult(result=True)},
                "get": {check_path: MockResult(result="transient")},
            }
        }
    )
    yt_client = yt_client_factory(cluster_name)

    action = ReshardTableAction(desired_table, actual_table)

    assert action.schedule_next(yt_client) is True
    action.process()

    call_count = 10
    for _ in range(call_count):
        assert action.schedule_next(yt_client) is True
        action.process()

    call_tracker = yt_client_factory.get_call_tracker(cluster_name)
    assert call_tracker
    get_calls = call_tracker.calls["get"]
    assert call_count == len(get_calls)
    for call in get_calls:
        assert check_path == call.path_or_type
