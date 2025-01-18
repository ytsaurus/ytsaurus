import uuid

import pytest

from yt.yt_sync.action.eliminate_chunk_views import EliminateChunkViews
from yt.yt_sync.core.model import Types
from yt.yt_sync.core.model import YtReplica
from yt.yt_sync.core.model import YtTable


@pytest.fixture
def table_attributes(default_schema: Types.Schema) -> Types.Attributes:
    return {"dynamic": True, "schema": default_schema}


@pytest.fixture
def yt_replicated_table(table_path: str, table_attributes: Types.Attributes) -> YtTable:
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


@pytest.fixture
def yt_table(table_path: str, table_attributes: Types.Attributes) -> YtTable:
    return YtTable.make("k", "r0", YtTable.Type.TABLE, table_path, True, table_attributes)


def test_create_with_replicated(yt_replicated_table: YtTable):
    with pytest.raises(AssertionError):
        EliminateChunkViews(yt_replicated_table)


def test_create_with_nonexistent(yt_table: YtTable):
    yt_table.exists = False
    with pytest.raises(AssertionError):
        EliminateChunkViews(yt_table).schedule_next(None)  # type: ignore


def test_process_before_schedule(yt_table: YtTable):
    with pytest.raises(AssertionError):
        EliminateChunkViews(yt_table).process()
