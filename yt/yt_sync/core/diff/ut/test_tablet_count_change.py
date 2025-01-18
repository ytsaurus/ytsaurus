from copy import deepcopy
from logging import Logger

import pytest

from yt.yt_sync.core.diff import DbDiff
from yt.yt_sync.core.diff import TableDiffType
from yt.yt_sync.core.diff import TabletCountChange
from yt.yt_sync.core.model import Types
from yt.yt_sync.core.model import YtDatabase
from yt.yt_sync.core.model import YtSchema
from yt.yt_sync.core.model import YtTable
from yt.yt_sync.core.settings import Settings

from .helpers import DiffDbTestBase


class TestTabletCountChange(DiffDbTestBase):
    @pytest.fixture()
    def cluster_name(self) -> str:
        return "primary"

    @pytest.fixture()
    def main_table(self, cluster_name: str, table_path: str, default_schema: Types.Schema) -> YtTable:
        return YtTable.make(
            table_path, cluster_name, YtTable.Type.TABLE, table_path, True, {"dynamic": True, "schema": default_schema}
        )

    def test_db_diff(self, settings: Settings, desired_db: YtDatabase, cluster_name: str, table_path: str):
        actual_db = deepcopy(desired_db)
        for cluster in actual_db.all_clusters:
            cluster.tables[table_path].tablet_info.tablet_count = 1
        for cluster in desired_db.all_clusters:
            cluster.tables[table_path].tablet_info.tablet_count = 2

        diff = DbDiff.generate(settings, desired_db, actual_db)
        assert not diff.is_empty()

        assert table_path in diff.tables_diff
        diff_list = diff.tables_diff[table_path]
        assert 1 == len(diff_list)
        table_diff = diff_list[0]
        assert TableDiffType.TABLET_COUNT_CHANGE == table_diff.diff_type
        assert isinstance(table_diff, TabletCountChange)
        assert next(table_diff.get_diff_for(cluster_name), None) is not None
        assert next(table_diff.get_diff_for("_bad_"), None) is None

    def test_not_existing_table(self, cluster_name: str, main_table: YtTable):
        actual_table = deepcopy(main_table)
        main_table.tablet_info.tablet_count = 10
        actual_table.exists = False
        change = TabletCountChange.make(main_table)
        change.add_change_if_any(main_table, actual_table)
        assert change.is_empty()
        assert next(change.get_diff_for(cluster_name), None) is None

    def test_crt_table(self, cluster_name: str, main_table: YtTable):
        main_table.table_type = YtTable.Type.CHAOS_REPLICATED_TABLE
        actual_table = deepcopy(main_table)
        main_table.tablet_info.tablet_count = 10

        change = TabletCountChange.make(main_table)
        change.add_change_if_any(main_table, actual_table)
        assert change.is_empty()
        assert next(change.get_diff_for(cluster_name), None) is None

    @pytest.mark.parametrize("actual,desired", [(1, 10), (10, 1)])
    def test_tablet_count_diff(
        self, main_table: YtTable, cluster_name: str, dummy_logger: Logger, actual: int, desired: int
    ):
        actual_table = deepcopy(main_table)
        actual_table.tablet_info.tablet_count = actual
        main_table.tablet_info.tablet_count = desired

        change = TabletCountChange.make(main_table)
        change.add_change_if_any(main_table, actual_table)
        assert not change.is_empty()
        assert change.has_diff_for(cluster_name)
        assert (main_table, actual_table) == next(change.get_diff_for(cluster_name), None)
        assert change.is_unmount_required(cluster_name)
        assert change.check_and_log(dummy_logger)

    @pytest.mark.parametrize("actual", [[[]], [[], [1], [10]]])
    @pytest.mark.parametrize("desired", [[[], [5], [10]], [[], [2]]])
    def test_pivot_keys_diff(
        self,
        main_table: YtTable,
        cluster_name: str,
        dummy_logger: Logger,
        actual: list[list[int]],
        desired: list[list[int]],
    ):
        actual_table = deepcopy(main_table)
        actual_table.tablet_info.pivot_keys = actual
        main_table.tablet_info.pivot_keys = desired

        change = TabletCountChange.make(main_table)
        change.add_change_if_any(main_table, actual_table)
        assert not change.is_empty()
        assert change.has_diff_for(cluster_name)
        assert (main_table, actual_table) == next(change.get_diff_for(cluster_name), None)
        assert change.is_unmount_required(cluster_name)
        assert change.check_and_log(dummy_logger)

    @pytest.mark.parametrize("actual,desired", [(1, 10), (10, 1)])
    def test_ordered_tablet_count(
        self,
        main_table: YtTable,
        cluster_name: str,
        ordered_schema: Types.Schema,
        dummy_logger: Logger,
        actual: int,
        desired: int,
    ):
        main_table.schema = YtSchema.parse(ordered_schema)
        actual_table = deepcopy(main_table)
        actual_table.tablet_info.tablet_count = actual
        main_table.tablet_info.tablet_count = desired

        change = TabletCountChange.make(main_table)
        change.add_change_if_any(main_table, actual_table)
        assert not change.is_empty()
        assert change.has_diff_for(cluster_name)
        assert (main_table, actual_table) == next(change.get_diff_for(cluster_name), None)
        assert change.is_unmount_required(cluster_name)
        assert change.check_and_log(dummy_logger) == bool(desired > actual)

    def test_ordered_pivot_keys(
        self,
        main_table: YtTable,
        cluster_name: str,
        ordered_schema: Types.Schema,
        dummy_logger: Logger,
    ):
        main_table.schema = YtSchema.parse(ordered_schema)
        actual_table = deepcopy(main_table)
        actual_table.tablet_info.pivot_keys = [[], [1]]
        main_table.tablet_info.pivot_keys = [[], [1], [2]]

        change = TabletCountChange.make(main_table)
        change.add_change_if_any(main_table, actual_table)
        assert not change.is_empty()
        assert change.has_diff_for(cluster_name)
        assert (main_table, actual_table) == next(change.get_diff_for(cluster_name), None)
        assert change.is_unmount_required(cluster_name)
        assert not change.check_and_log(dummy_logger)
