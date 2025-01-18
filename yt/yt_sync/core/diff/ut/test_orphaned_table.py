from copy import deepcopy
from logging import Logger

import pytest

from yt.yt_sync.core.diff import DbDiff
from yt.yt_sync.core.diff import OrphanedTable
from yt.yt_sync.core.diff import TableDiffType
from yt.yt_sync.core.model import Types
from yt.yt_sync.core.model import YtDatabase
from yt.yt_sync.core.model import YtReplica
from yt.yt_sync.core.model import YtSchema
from yt.yt_sync.core.model import YtTable
from yt.yt_sync.core.settings import Settings

from .helpers import DiffDbTestBase


class TestOrphanedTables(DiffDbTestBase):
    def _check_and_get(self, diff: DbDiff, table_path: str) -> OrphanedTable:
        assert table_path in diff.tables_diff
        diff_list = diff.tables_diff[table_path]
        assert 1 == len(diff_list)
        table_diff = diff_list[0]
        assert TableDiffType.ORPHANED_TABLE == table_diff.diff_type
        assert isinstance(table_diff, OrphanedTable)
        return table_diff

    def _make_diff_from_dbs(self, desired_db: YtDatabase, actual_db: YtDatabase, table_path: str) -> OrphanedTable:
        main_cluster = desired_db.clusters["primary"]
        assert main_cluster
        main_table = main_cluster.tables[table_path]
        diff = OrphanedTable.make(main_table)
        for desired_cluster in desired_db.clusters.values():
            if desired_cluster == main_cluster:
                continue
            actual_cluster = actual_db.clusters[desired_cluster.name]
            for table_path in desired_cluster.tables:
                diff.add_change_if_any(desired_cluster.tables[table_path], actual_cluster.tables[table_path])
        return diff

    def test_orphaned_tables(self, settings: Settings, desired_db: YtDatabase, actual_db: YtDatabase, table_path: str):
        actual_db.clusters["remote0"].tables[table_path].exists = True
        diff = DbDiff.generate(settings, desired_db, actual_db)
        table_diff = self._check_and_get(diff, table_path)
        assert not table_diff.is_empty()
        assert table_diff.has_missing_data_replicas
        assert YtReplica.make_key("remote0", table_path) in table_diff.orphaned_tables

        assert table_diff.has_diff_for("primary")
        assert next(table_diff.orphaned_tables_for("primary"), None) is None
        assert table_diff.has_diff_for("remote0")
        assert next(table_diff.orphaned_tables_for("remote0"), None) is not None
        assert not table_diff.has_diff_for("remote1")
        assert next(table_diff.orphaned_tables_for("remote1"), None) is None

    def test_main_not_replicated(
        self,
        desired_db: YtDatabase,
        actual_db: YtDatabase,
        table_path: str,
        dummy_logger: Logger,
    ):
        desired_db.clusters["primary"].tables[table_path].table_type = YtTable.Type.TABLE
        actual_db.clusters["remote0"].tables[table_path].exists = True

        diff = self._make_diff_from_dbs(desired_db, actual_db, table_path)
        assert diff.is_empty()
        assert diff.check_and_log(dummy_logger)

        for cluster in ("primary", "remote0", "remote1"):
            assert not diff.has_diff_for(cluster)

    def test_has_missing_data_replicas(
        self,
        desired_db: YtDatabase,
        actual_db: YtDatabase,
        table_path: str,
        is_chaos: bool,
        dummy_logger: Logger,
    ):
        actual_db.clusters["remote0"].tables[table_path].exists = True
        if is_chaos:
            log_path = desired_db.clusters["remote0"].tables[table_path].chaos_replication_log
            assert log_path
            actual_db.clusters["remote0"].tables[log_path].exists = True

        diff = self._make_diff_from_dbs(desired_db, actual_db, table_path)
        assert not diff.is_empty()
        assert not diff.check_and_log(dummy_logger)
        assert YtReplica.make_key("remote0", table_path) in diff.orphaned_tables
        if is_chaos:
            assert log_path
            assert YtReplica.make_key("remote0", log_path) in diff.orphaned_tables

    def test_has_all_data_replicas(
        self,
        desired_db: YtDatabase,
        actual_db: YtDatabase,
        table_path: str,
        is_chaos: bool,
        dummy_logger: Logger,
    ):
        actual_db.clusters["remote0"].tables[table_path].exists = True
        actual_db.clusters["remote1"].tables[table_path].exists = True
        if is_chaos:
            log_path = desired_db.clusters["remote0"].tables[table_path].chaos_replication_log
            assert log_path
            actual_db.clusters["remote0"].tables[log_path].exists = True

        diff = self._make_diff_from_dbs(desired_db, actual_db, table_path)
        assert not diff.is_empty()
        assert diff.check_and_log(dummy_logger)
        assert YtReplica.make_key("remote0", table_path) in diff.orphaned_tables
        assert YtReplica.make_key("remote1", table_path) in diff.orphaned_tables
        if is_chaos:
            assert log_path
            assert YtReplica.make_key("remote0", log_path) in diff.orphaned_tables

        for cluster in ("remote0", "remote1"):
            assert diff.has_diff_for(cluster)

    def test_has_data_replica_incompatible_schema(
        self,
        desired_db: YtDatabase,
        actual_db: YtDatabase,
        table_path: str,
        default_schema: Types.Schema,
        dummy_logger: Logger,
    ):
        actual_db.clusters["remote0"].tables[table_path].exists = True
        actual_db.clusters["remote1"].tables[table_path].exists = True

        new_schema = deepcopy(default_schema)
        new_schema[-1]["type"] = "string"
        actual_db.clusters["remote1"].tables[table_path].schema = YtSchema.parse(new_schema)

        diff = self._make_diff_from_dbs(desired_db, actual_db, table_path)
        assert not diff.is_empty()
        assert not diff.check_and_log(dummy_logger)
        assert YtReplica.make_key("remote0", table_path) in diff.orphaned_tables
        assert YtReplica.make_key("remote1", table_path) in diff.orphaned_tables


class TestOrphanedTablesChaos(TestOrphanedTables):
    @pytest.fixture
    def is_chaos(self) -> bool:
        return True

    @pytest.mark.parametrize("log_exists", [True, False])
    def test_orphaned_tables(
        self, settings: Settings, desired_db: YtDatabase, actual_db: YtDatabase, table_path: str, log_exists: bool
    ):
        log_path = desired_db.clusters["remote0"].tables[table_path].chaos_replication_log
        assert log_path

        actual_db.clusters["remote0"].tables[table_path].exists = True
        actual_db.clusters["remote0"].tables[log_path].exists = log_exists

        diff = DbDiff.generate(settings, desired_db, actual_db)
        table_diff = self._check_and_get(diff, table_path)
        assert not table_diff.is_empty()
        assert table_diff.has_missing_data_replicas
        assert YtReplica.make_key("remote0", table_path) in table_diff.orphaned_tables

        assert table_diff.has_diff_for("primary")
        assert next(table_diff.orphaned_tables_for("primary"), None) is None
        assert table_diff.has_diff_for("remote0")
        assert next(table_diff.orphaned_tables_for("remote0"), None) is not None
        assert not table_diff.has_diff_for("remote1")
        assert next(table_diff.orphaned_tables_for("remote1"), None) is None

        log_key = YtReplica.make_key("remote0", log_path)
        if log_exists:
            assert log_key in table_diff.orphaned_tables
            assert log_key not in table_diff.missing_replication_logs
            assert next(table_diff.replication_logs_for("remote0"), None) is None
        else:
            assert log_key not in table_diff.orphaned_tables
            assert log_key in table_diff.missing_replication_logs
            assert next(table_diff.replication_logs_for("remote0"), None) is not None

    def test_has_queue_replica_incompatible_schema(
        self,
        desired_db: YtDatabase,
        actual_db: YtDatabase,
        table_path: str,
        default_schema: Types.Schema,
        dummy_logger: Logger,
    ):
        log_path = desired_db.clusters["remote0"].tables[table_path].chaos_replication_log
        assert log_path

        actual_db.clusters["remote0"].tables[table_path].exists = True
        actual_db.clusters["remote1"].tables[table_path].exists = True

        new_schema = deepcopy(default_schema)
        new_schema[-1]["type"] = "string"
        actual_db.clusters["remote1"].tables[log_path].schema = YtSchema.parse(new_schema)
        actual_db.clusters["remote1"].tables[log_path].exists = True

        diff = self._make_diff_from_dbs(desired_db, actual_db, table_path)
        assert not diff.is_empty()
        assert diff.check_and_log(dummy_logger)
        assert YtReplica.make_key("remote0", table_path) in diff.orphaned_tables
        assert YtReplica.make_key("remote1", table_path) in diff.orphaned_tables
        assert YtReplica.make_key("remote1", log_path) in diff.orphaned_tables
