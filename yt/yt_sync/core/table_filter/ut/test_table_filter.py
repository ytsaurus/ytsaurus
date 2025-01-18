import pytest

from yt.yt_sync.core.model import get_node_name
from yt.yt_sync.core.model import Types
from yt.yt_sync.core.model import YtCluster
from yt.yt_sync.core.model import YtTable
from yt.yt_sync.core.table_filter import TableNameFilter


@pytest.fixture
def yt_cluster(table_path: str, default_schema: Types.Schema) -> YtCluster:
    cluster = YtCluster.make(YtCluster.Type.MAIN, "primary", False)
    cluster.add_table_from_attributes(table_path, YtTable.Type.TABLE, table_path, True, {"schema": default_schema})
    return cluster


def test_empty_table_filter(yt_cluster: YtCluster, table_path: str):
    table_filter = TableNameFilter([])
    seen_tables = set()
    for table in table_filter(yt_cluster):
        seen_tables.add(table.path)
    assert table_path in seen_tables
    assert table_filter.is_ok(table_path)


def test_table_filter_pass(yt_cluster: YtCluster, table_path: str):
    table_name = get_node_name(table_path)
    table_filter = TableNameFilter([table_name])
    seen_tables = set()
    for table in table_filter(yt_cluster):
        seen_tables.add(table.path)
    assert table_path in seen_tables
    assert table_filter.is_ok(table_path)


def test_table_filter_not_pass(yt_cluster: YtCluster, table_path: str):
    table_filter = TableNameFilter(["_some_table"])
    seen_tables = set()
    for table in table_filter(yt_cluster):
        seen_tables.add(table.path)
    assert table_path not in seen_tables
    assert not table_filter.is_ok(table_path)


def test_table_filter_pattern_match(table_path: str):
    assert TableNameFilter(["*"]).is_ok(table_path)
    assert not TableNameFilter(["*some_table"]).is_ok(table_path)
