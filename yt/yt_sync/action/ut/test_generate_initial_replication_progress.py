import pytest

from yt.yt_sync.action.generate_initial_replication_progress import GenerateInitialReplicationProgressAction
from yt.yt_sync.core.client import MockResult
from yt.yt_sync.core.model import Types
from yt.yt_sync.core.model import YtTable
from yt.yt_sync.core.test_lib import MockYtClientFactory

from .helpers import simple_assert_calls
from .helpers import simple_assert_no_calls

_MAX_TIMESTAMP: int = 0x3FFFFFFFFFFFFF00


@pytest.fixture
def yt_table(table_path: str, default_schema: Types.Schema) -> YtTable:
    return YtTable.make(
        table_path, "primary", YtTable.Type.TABLE, table_path, True, {"dynamic": True, "schema": default_schema}
    )


def test_succes(yt_table: YtTable):
    assert yt_table.chaos_replication_progress is None
    yt_client_factory = MockYtClientFactory(
        {
            yt_table.cluster_name: {
                "generate_timestamp": {"*": MockResult(result=_MAX_TIMESTAMP, raw=True)},
            }
        }
    )

    yt_client = yt_client_factory(yt_table.cluster_name)

    action = GenerateInitialReplicationProgressAction(yt_table)
    assert action.schedule_next(yt_client) is False
    action.process()

    simple_assert_calls(yt_client_factory, yt_table, "generate_timestamp", 1)

    assert yt_table.chaos_replication_progress is not None


def test_already_set(yt_table: YtTable):
    yt_table.chaos_replication_progress = {}

    yt_client_factory = MockYtClientFactory(
        {
            yt_table.cluster_name: {
                "generate_timestamp": {"*": MockResult(result=_MAX_TIMESTAMP, raw=True)},
            }
        }
    )

    yt_client = yt_client_factory(yt_table.cluster_name)

    action = GenerateInitialReplicationProgressAction(yt_table)
    assert action.schedule_next(yt_client) is False
    action.process()

    simple_assert_no_calls(yt_client_factory, yt_table, "generate_timestamp")
