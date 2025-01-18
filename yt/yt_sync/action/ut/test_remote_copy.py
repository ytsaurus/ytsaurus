from copy import deepcopy

import pytest

from yt.yt_sync.action.remote_copy import RemoteCopyAction
from yt.yt_sync.core import Settings
from yt.yt_sync.core.constants import CONSUMER_ATTRS
from yt.yt_sync.core.constants import CONSUMER_SCHEMA
from yt.yt_sync.core.model import Types
from yt.yt_sync.core.model import YtTable
from yt.yt_sync.core.test_lib import MockOperation
from yt.yt_sync.core.test_lib import MockResult
from yt.yt_sync.core.test_lib import MockYtClientFactory


@pytest.fixture
def settings() -> Settings:
    return Settings(db_type=Settings.REPLICATED_DB)


@pytest.fixture
def table_attributes(default_schema: Types.Schema) -> Types.Attributes:
    return {"dynamic": True, "schema": default_schema}


@pytest.fixture
def src_table(table_path: str, table_attributes: Types.Attributes) -> YtTable:
    return YtTable.make("k", "r0", YtTable.Type.TABLE, table_path, True, table_attributes)


@pytest.fixture
def dst_table(table_path: str, table_attributes: Types.Attributes) -> YtTable:
    return YtTable.make("k", "r1", YtTable.Type.TABLE, table_path, True, table_attributes)


def test_create_copy_to_self(settings: Settings, src_table: YtTable):
    with pytest.raises(AssertionError):
        RemoteCopyAction(settings, src_table, src_table)


def test_create_with_replicated(settings: Settings, table_path: str, table_attributes: Types.Attributes):
    src_table = YtTable.make("k", "p0", YtTable.Type.REPLICATED_TABLE, table_path, True, table_attributes)
    dst_table = YtTable.make("k", "p1", YtTable.Type.REPLICATED_TABLE, table_path, True, table_attributes)
    with pytest.raises(AssertionError):
        RemoteCopyAction(settings, src_table, dst_table)


@pytest.mark.parametrize("src_exists", [True, False])
@pytest.mark.parametrize("dst_exists", [True, False])
def test_create_with_nonexistent(
    settings: Settings, src_table: YtTable, dst_table: YtTable, src_exists: bool, dst_exists: bool
):
    src_table.exists = src_exists
    dst_table.exists = dst_exists

    yt_client_factory = MockYtClientFactory(
        {dst_table.cluster_name: {"run_remote_copy": {dst_table.path: MockResult(result=MockOperation("running"))}}}
    )
    yt_client = yt_client_factory(dst_table.cluster_name)
    if src_exists and dst_exists:
        RemoteCopyAction(settings, src_table, dst_table).schedule_next(yt_client) is True
    else:
        with pytest.raises(AssertionError):
            RemoteCopyAction(settings, src_table, dst_table).schedule_next(yt_client)


def test_create_with_ordered(settings: Settings, table_path: str, ordered_schema: Types.Schema):
    src_table = YtTable.make(
        "k", "r0", YtTable.Type.TABLE, table_path, True, {"dynamic": True, "schema": ordered_schema}
    )
    dst_table = YtTable.make(
        "k", "r1", YtTable.Type.TABLE, table_path, True, {"dynamic": True, "schema": ordered_schema}
    )
    with pytest.raises(AssertionError):
        RemoteCopyAction(settings, src_table, dst_table)


def test_create_with_consumer(settings: Settings, table_path: str):
    table_attributes = deepcopy(CONSUMER_ATTRS)
    table_attributes["schema"] = CONSUMER_SCHEMA
    src_table = YtTable.make("k", "r0", YtTable.Type.TABLE, table_path, True, table_attributes)
    dst_table = YtTable.make("k", "r1", YtTable.Type.TABLE, table_path, True, table_attributes)
    RemoteCopyAction(settings, src_table, dst_table)


def test_with_hunks(settings: Settings, table_path: str, default_schema: Types.Schema):
    default_schema.append({"name": "column_with_hunks", "type": "string", "max_inline_hunk_size": 10})
    table_attributes = {"dynamic": True, "schema": default_schema}
    src_table = YtTable.make("k", "r0", YtTable.Type.TABLE, table_path, True, table_attributes)
    dst_table = YtTable.make("k", "r1", YtTable.Type.TABLE, table_path, True, table_attributes)
    with pytest.raises(AssertionError):
        RemoteCopyAction(settings, src_table, dst_table)


def test_process_before_schedule(settings: Settings, src_table: YtTable, dst_table: YtTable):
    with pytest.raises(AssertionError):
        RemoteCopyAction(settings, src_table, dst_table).process()


def test_operation_running(settings: Settings, src_table: YtTable, dst_table: YtTable):
    yt_client_factory = MockYtClientFactory(
        {
            dst_table.cluster_name: {
                "run_remote_copy": {dst_table.path: MockResult(result=MockOperation("running"), raw=True)}
            }
        }
    )
    yt_client = yt_client_factory(dst_table.cluster_name)

    action = RemoteCopyAction(settings, src_table, dst_table)
    action.schedule_next(yt_client) is True
    action.process()

    action.schedule_next(yt_client) is True


def test_operation_failed(settings: Settings, src_table: YtTable, dst_table: YtTable):
    yt_client_factory = MockYtClientFactory(
        {
            dst_table.cluster_name: {
                "run_remote_copy": {dst_table.path: MockResult(result=MockOperation("failed"), raw=True)}
            }
        }
    )
    yt_client = yt_client_factory(dst_table.cluster_name)

    action = RemoteCopyAction(settings, src_table, dst_table)
    action.schedule_next(yt_client) is True
    with pytest.raises(Exception):
        action.process()


def test_operation_completed(settings: Settings, src_table: YtTable, dst_table: YtTable):
    yt_client_factory = MockYtClientFactory(
        {
            dst_table.cluster_name: {
                "run_remote_copy": {dst_table.path: MockResult(result=MockOperation("completed"), raw=True)}
            }
        }
    )
    yt_client = yt_client_factory(dst_table.cluster_name)

    action = RemoteCopyAction(settings, src_table, dst_table)
    action.schedule_next(yt_client) is True
    action.process()

    action.schedule_next(yt_client) is False
