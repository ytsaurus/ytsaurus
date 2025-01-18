import pytest

import yt.wrapper as yt
from yt.yt_sync.action import CreateOrderedTableFromAction
from yt.yt_sync.core.model import Types
from yt.yt_sync.core.model import YtTable
from yt.yt_sync.core.test_lib import MockResult
from yt.yt_sync.core.test_lib import MockYtClientFactory
from yt.yt_sync.core.test_lib import yt_generic_error

_TABLET_COUNT = 2


def _make_table(
    cluster_name: str,
    table_path: str,
    table_type: str,
    schema: Types.Schema,
    exists: bool = True,
    with_total_row_count: bool = False,
):
    result = YtTable.make(
        table_path,
        cluster_name,
        table_type,
        table_path,
        exists,
        {"dynamic": True, "schema": schema, "tablet_count": _TABLET_COUNT},
    )
    if with_total_row_count:
        result.total_row_count = [(x + 1) * 100 for x in range(_TABLET_COUNT)]
    return result


def _create_mock_factory(table: YtTable, error: bool = False) -> MockYtClientFactory:
    result = MockResult(error=yt_generic_error()) if error else MockResult(result=True)
    return MockYtClientFactory({table.cluster_name: {"create": {table.path: result}}})


@pytest.fixture
def cluster_name() -> str:
    return "r0"


def test_ctor(cluster_name: str, table_path: str, default_schema: Types.Schema, ordered_schema: Types.Schema):
    src_table = f"{table_path}_src"

    # bad schema for desired
    with pytest.raises(AssertionError):
        CreateOrderedTableFromAction(
            _make_table(cluster_name, table_path, YtTable.Type.TABLE, default_schema),
            _make_table(cluster_name, table_path, YtTable.Type.TABLE, ordered_schema, False),
            _make_table(cluster_name, src_table, YtTable.Type.TABLE, ordered_schema),
        )

    # bad schema for source
    with pytest.raises(AssertionError):
        CreateOrderedTableFromAction(
            _make_table(cluster_name, table_path, YtTable.Type.TABLE, ordered_schema),
            _make_table(cluster_name, table_path, YtTable.Type.TABLE, ordered_schema, False),
            _make_table(cluster_name, src_table, YtTable.Type.TABLE, default_schema),
        )

    # bad table type for desired
    with pytest.raises(AssertionError):
        CreateOrderedTableFromAction(
            _make_table(cluster_name, table_path, YtTable.Type.REPLICATED_TABLE, ordered_schema),
            _make_table(cluster_name, table_path, YtTable.Type.TABLE, ordered_schema, False),
            _make_table(cluster_name, src_table, YtTable.Type.TABLE, ordered_schema),
        )

    # bad table type for source
    with pytest.raises(AssertionError):
        CreateOrderedTableFromAction(
            _make_table(cluster_name, table_path, YtTable.Type.TABLE, ordered_schema),
            _make_table(cluster_name, table_path, YtTable.Type.TABLE, ordered_schema, False),
            _make_table(cluster_name, src_table, YtTable.Type.REPLICATION_LOG, ordered_schema),
        )

    CreateOrderedTableFromAction(
        _make_table(cluster_name, table_path, YtTable.Type.TABLE, ordered_schema),
        _make_table(cluster_name, table_path, YtTable.Type.TABLE, ordered_schema, False),
        _make_table(cluster_name, src_table, YtTable.Type.TABLE, ordered_schema),
    )


def test_process_before_schedule(cluster_name: str, table_path: str, ordered_schema: Types.Schema):
    with pytest.raises(AssertionError):
        CreateOrderedTableFromAction(
            _make_table(cluster_name, table_path, YtTable.Type.TABLE, ordered_schema),
            _make_table(cluster_name, table_path, YtTable.Type.TABLE, ordered_schema, False),
            _make_table(cluster_name, f"{table_path}_src", YtTable.Type.TABLE, ordered_schema),
        ).process()


def test_success(cluster_name: str, table_path: str, ordered_schema: Types.Schema):
    desired = _make_table(cluster_name, table_path, YtTable.Type.TABLE, ordered_schema)
    actual = _make_table(cluster_name, table_path, YtTable.Type.TABLE, ordered_schema, False)
    source = _make_table(
        cluster_name, f"{table_path}_src", YtTable.Type.TABLE, ordered_schema, with_total_row_count=True
    )

    yt_client_factory = _create_mock_factory(desired)
    yt_client = yt_client_factory(cluster_name)

    action = CreateOrderedTableFromAction(desired, actual, source)

    assert action.schedule_next(yt_client) is False
    action.process()

    assert actual.exists is True
    assert actual.total_row_count
    assert actual.total_row_count == source.total_row_count


def test_yt_error(cluster_name: str, table_path: str, ordered_schema: Types.Schema):
    desired = _make_table(cluster_name, table_path, YtTable.Type.TABLE, ordered_schema)
    actual = _make_table(cluster_name, table_path, YtTable.Type.TABLE, ordered_schema, False)
    source = _make_table(
        cluster_name, f"{table_path}_src", YtTable.Type.TABLE, ordered_schema, with_total_row_count=True
    )

    yt_client_factory = _create_mock_factory(desired, error=True)
    yt_client = yt_client_factory(cluster_name)

    action = CreateOrderedTableFromAction(desired, actual, source)

    assert action.schedule_next(yt_client) is False
    with pytest.raises(yt.YtResponseError):
        action.process()


def test_actual_exists(cluster_name: str, table_path: str, ordered_schema: Types.Schema):
    desired = _make_table(cluster_name, table_path, YtTable.Type.TABLE, ordered_schema)
    actual = _make_table(cluster_name, table_path, YtTable.Type.TABLE, ordered_schema, True)
    source = _make_table(
        cluster_name, f"{table_path}_src", YtTable.Type.TABLE, ordered_schema, with_total_row_count=True
    )

    yt_client_factory = _create_mock_factory(desired)
    yt_client = yt_client_factory(cluster_name)

    action = CreateOrderedTableFromAction(desired, actual, source)

    with pytest.raises(AssertionError):
        action.schedule_next(yt_client)


def test_source_not_exists(cluster_name: str, table_path: str, ordered_schema: Types.Schema):
    desired = _make_table(cluster_name, table_path, YtTable.Type.TABLE, ordered_schema)
    actual = _make_table(cluster_name, table_path, YtTable.Type.TABLE, ordered_schema, False)
    source = _make_table(
        cluster_name, f"{table_path}_src", YtTable.Type.TABLE, ordered_schema, exists=False, with_total_row_count=True
    )

    yt_client_factory = _create_mock_factory(desired)
    yt_client = yt_client_factory(cluster_name)

    action = CreateOrderedTableFromAction(desired, actual, source)

    with pytest.raises(AssertionError):
        action.schedule_next(yt_client)


def test_source_not_has_total_row_count(cluster_name: str, table_path: str, ordered_schema: Types.Schema):
    desired = _make_table(cluster_name, table_path, YtTable.Type.TABLE, ordered_schema)
    actual = _make_table(cluster_name, table_path, YtTable.Type.TABLE, ordered_schema, False)
    source = _make_table(
        cluster_name, f"{table_path}_src", YtTable.Type.TABLE, ordered_schema, with_total_row_count=False
    )

    yt_client_factory = _create_mock_factory(desired)
    yt_client = yt_client_factory(cluster_name)

    action = CreateOrderedTableFromAction(desired, actual, source)

    with pytest.raises(AssertionError):
        action.schedule_next(yt_client)
