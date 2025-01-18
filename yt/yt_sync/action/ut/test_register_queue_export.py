from typing import Any

import pytest

from yt.yt_sync.action import RegisterQueueExportAction
from yt.yt_sync.core.model import Types
from yt.yt_sync.core.model import YtTable
from yt.yt_sync.core.model import YtTabletState
from yt.yt_sync.core.test_lib import MockResult
from yt.yt_sync.core.test_lib import MockYtClientFactory


def _make_table(cluster_name: str, table_path: str, table_type: str, schema: Types.Schema, attrs_patch: dict[str, Any]):
    result = YtTable.make(
        table_path,
        cluster_name,
        table_type,
        table_path,
        True,
        {"dynamic": True, "schema": schema, "tablet_count": 2, **attrs_patch},
    )
    result.tablet_state.set(YtTabletState.FROZEN)
    return result


@pytest.fixture
def cluster_name() -> str:
    return "r0"


@pytest.fixture
def old_yt_table(cluster_name: str, table_path: str, ordered_schema: Types.Schema) -> YtTable:
    return _make_table(
        cluster_name,
        table_path,
        YtTable.Type.TABLE,
        ordered_schema,
        {
            "static_export_config": {
                "default": {
                    "export_directory": "//tmp/export1",
                    "export_period": 300000,
                },
            },
        },
    )


@pytest.fixture
def new_yt_table(cluster_name: str, table_path: str, ordered_schema: Types.Schema) -> YtTable:
    return _make_table(
        cluster_name,
        table_path,
        YtTable.Type.TABLE,
        ordered_schema,
        {
            "static_export_config": {
                "default": {
                    "export_directory": "//tmp/export2",
                    "export_period": 300000,
                },
            },
        },
    )


def test_success(old_yt_table: YtTable, new_yt_table: YtTable):
    action = RegisterQueueExportAction(new_yt_table, old_yt_table)
    yt_client_factory = MockYtClientFactory(
        {
            old_yt_table.cluster_name: {
                "create": {"//tmp/export2": MockResult("ok")},
                "remove": {"//tmp/export1": MockResult("ok")},
                "get": {
                    "//tmp/export1/@": MockResult(
                        {"queue_static_export_destination": {"originating_queue_id": "another-queue-guid"}}
                    ),
                    "//tmp/export2/@": MockResult({}),
                    old_yt_table.path + "/@id": MockResult("some-queue-guid"),
                },
                "set": {"//tmp/export2/@queue_static_export_destination": MockResult("ok")},
            },
        }
    )
    yt_client = yt_client_factory(old_yt_table.cluster_name)

    assert action.schedule_next(yt_client)
    action.process()
    assert action.schedule_next(yt_client)
    action.process()
    assert not action.schedule_next(yt_client)
    action.process()

    set_calls = yt_client_factory.get_call_tracker(old_yt_table.cluster_name).calls["set"]
    assert len(set_calls) == 1
    set_call = set_calls[0]

    assert set_call.path_or_type == "//tmp/export2/@queue_static_export_destination"
    assert set_call.args == [{"originating_queue_id": "some-queue-guid"}]


def test_clean(new_yt_table: YtTable):
    action = RegisterQueueExportAction(new_yt_table, new_yt_table, clean=True)

    yt_client_factory = MockYtClientFactory(
        {
            new_yt_table.cluster_name: {
                "create": {"//tmp/export2": MockResult("ok")},
                "remove": {"//tmp/export2": MockResult("ok")},
                "get": {"//tmp/export2/@": MockResult({}), new_yt_table.path + "/@id": MockResult("some-queue-guid")},
                "set": {"//tmp/export2/@queue_static_export_destination": MockResult("ok")},
            },
        }
    )
    yt_client = yt_client_factory(new_yt_table.cluster_name)

    assert action.schedule_next(yt_client)
    action.process()

    remove_calls = yt_client_factory.get_call_tracker(new_yt_table.cluster_name).calls["remove"]
    assert len(remove_calls) == 1

    assert action.schedule_next(yt_client)
    action.process()
    assert action.schedule_next(yt_client)
    action.process()
    assert not action.schedule_next(yt_client)
    action.process()
