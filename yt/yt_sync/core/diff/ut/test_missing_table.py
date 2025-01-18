from logging import Logger

import pytest

from yt.yt_sync.core.diff import DbDiff
from yt.yt_sync.core.diff import MissingTable
from yt.yt_sync.core.diff import TableDiffType
from yt.yt_sync.core.model import Types
from yt.yt_sync.core.model import YtDatabase
from yt.yt_sync.core.model import YtTable
from yt.yt_sync.core.settings import Settings

from .helpers import DiffDbTestBase


class TestMissingTable(DiffDbTestBase):
    @pytest.fixture
    def main_table(self, table_path: str, default_schema: Types.Schema):
        return YtTable.make(
            table_path,
            "primary",
            YtTable.Type.REPLICATED_TABLE,
            table_path,
            False,
            {"dynamic": True, "schema": default_schema},
        )

    def _check_and_get(self, diff: DbDiff, table_path: str) -> MissingTable:
        assert table_path in diff.tables_diff
        diff_list = diff.tables_diff[table_path]
        assert 1 == len(diff_list)
        table_diff = diff_list[0]
        assert TableDiffType.MISSING_TABLE == table_diff.diff_type
        assert isinstance(table_diff, MissingTable)
        return table_diff

    def test_diff_missing_tables(
        self, settings: Settings, desired_db: YtDatabase, actual_db: YtDatabase, table_path: str
    ):
        diff = DbDiff.generate(settings, desired_db, actual_db)
        table_diff = self._check_and_get(diff, table_path)

        assert 3 == len(table_diff.missing)
        clusters = ("primary", "remote0", "remote1")
        for table in table_diff.missing.values():
            assert table.cluster_name in clusters
        for cluster in clusters:
            assert table_diff.has_diff_for(cluster)
        assert not table_diff.orphaned
        for cluster in clusters:
            assert next(table_diff.missing_tables_for(cluster), None) is not None

    def test_diff_missing_tables_with_orphaned(
        self, settings: Settings, desired_db: YtDatabase, actual_db: YtDatabase, table_path: str
    ):
        actual_db.clusters["remote1"].tables[table_path].exists = True

        diff = DbDiff.generate(settings, desired_db, actual_db)
        assert table_path in diff.tables_diff
        assert 1 == len(diff.tables_diff[table_path])
        table_diff = diff.tables_diff[table_path][0]
        assert TableDiffType.MISSING_TABLE != table_diff.diff_type

    def test_add_existing(self, main_table: YtTable):
        diff = MissingTable.make(main_table)
        desired_table = YtTable.make(
            main_table.key, "remote0", YtTable.Type.TABLE, main_table.path, True, main_table.yt_attributes
        )
        actual_table = YtTable.make(
            main_table.key, "remote0", YtTable.Type.TABLE, main_table.path, True, main_table.yt_attributes
        )
        diff.add_change_if_any(desired_table, actual_table)
        assert diff.orphaned
        assert not diff.missing
        assert diff.is_empty()
        for cluster in ("primary", "remote0", "remote1"):
            assert not diff.has_diff_for(cluster)

    def test_add_non_existing(self, main_table: YtTable):
        diff = MissingTable.make(main_table)
        desired_table = YtTable.make(
            main_table.key, "remote0", YtTable.Type.TABLE, main_table.path, True, main_table.yt_attributes
        )
        actual_table = YtTable.make(
            main_table.key, "remote0", YtTable.Type.TABLE, main_table.path, False, main_table.yt_attributes
        )
        diff.add_change_if_any(desired_table, actual_table)
        assert desired_table in diff.missing.values()
        assert not diff.orphaned
        assert not diff.is_empty()

    def test_check_and_log_on_empty(self, main_table: YtTable, dummy_logger: Logger):
        diff = MissingTable.make(main_table)
        assert diff.check_and_log(dummy_logger)

    def test_check_and_log_on_missing(self, main_table: YtTable, dummy_logger: Logger):
        diff = MissingTable.make(main_table)
        desired_table = YtTable.make(
            main_table.key, "remote0", YtTable.Type.TABLE, main_table.path, True, main_table.yt_attributes
        )
        actual_table = YtTable.make(
            main_table.key, "remote0", YtTable.Type.TABLE, main_table.path, False, main_table.yt_attributes
        )
        diff.add_change_if_any(desired_table, actual_table)
        assert diff.check_and_log(dummy_logger)

    def test_check_and_log_with_orphaned(self, main_table: YtTable, dummy_logger: Logger):
        diff = MissingTable.make(main_table)
        desired_table = YtTable.make(
            main_table.key, "remote0", YtTable.Type.TABLE, main_table.path, True, main_table.yt_attributes
        )
        actual_table = YtTable.make(
            main_table.key, "remote0", YtTable.Type.TABLE, main_table.path, True, main_table.yt_attributes
        )
        diff.add_change_if_any(desired_table, actual_table)
        assert not diff.check_and_log(dummy_logger)

    def test_skip_orphaned_empty_check(self, main_table: YtTable):
        diff = MissingTable.make(main_table)
        diff.missing[main_table.key] = main_table
        diff.orphaned[main_table.key] = main_table
        assert diff.is_empty()
        diff.skip_orphaned_empty_check = True
        assert not diff.is_empty()


class TestMissingTableChaos(TestMissingTable):
    @pytest.fixture
    def is_chaos(self) -> bool:
        return True

    def test_diff_missing_tables(
        self, settings: Settings, desired_db: YtDatabase, actual_db: YtDatabase, table_path: str
    ):
        diff = DbDiff.generate(settings, desired_db, actual_db)
        table_diff = self._check_and_get(diff, table_path)
        assert 5 == len(table_diff.missing)

        replicated_tables = 0
        replication_logs = 0
        data_tables = 0
        for missing_table in table_diff.missing.values():
            if missing_table.is_replicated:
                replicated_tables += 1
                assert "primary" == missing_table.cluster_name
            if YtTable.Type.REPLICATION_LOG == missing_table.table_type:
                replication_logs += 1
                assert missing_table.cluster_name in ("remote0", "remote1")
                assert table_path == missing_table.chaos_data_table
            if YtTable.Type.TABLE == missing_table.table_type:
                data_tables += 1
                assert missing_table.cluster_name in ("remote0", "remote1")
                assert table_path == missing_table.path

        assert 1 == replicated_tables
        assert 2 == replication_logs
        assert 2 == data_tables

        assert not table_diff.orphaned

        for cluster in ("primary", "remote0", "remote1"):
            assert table_diff.has_diff_for(cluster)
