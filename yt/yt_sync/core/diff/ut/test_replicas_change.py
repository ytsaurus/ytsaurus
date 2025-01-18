from copy import deepcopy
from logging import Logger

import pytest

from yt.yt_sync.core.diff import DbDiff
from yt.yt_sync.core.diff import ReplicasChange
from yt.yt_sync.core.diff import TableDiffType
from yt.yt_sync.core.model import Types
from yt.yt_sync.core.model import YtDatabase
from yt.yt_sync.core.model import YtReplica
from yt.yt_sync.core.model import YtSchema
from yt.yt_sync.core.model import YtTable
from yt.yt_sync.core.settings import Settings

from .helpers import DiffDbTestBase


class TestReplicasChange(DiffDbTestBase):
    def _check_and_get(self, diff: DbDiff, table_path: str) -> ReplicasChange:
        assert table_path in diff.tables_diff
        diff_list = diff.tables_diff[table_path]
        assert 1 == len(diff_list)
        table_diff = diff_list[0]
        assert TableDiffType.REPLICAS_CHANGE == table_diff.diff_type
        assert isinstance(table_diff, ReplicasChange)
        return table_diff

    def test_diff_missing_replica(
        self,
        settings: Settings,
        desired_db: YtDatabase,
        actual_db: YtDatabase,
        main_cluster_name: str,
        sync_cluster_name: str,
        async_cluster_name: str,
        table_path: str,
        folder_path: str,
    ):
        table_folder_path = actual_db.clusters[main_cluster_name].tables[table_path].folder

        actual_db.clusters[main_cluster_name].nodes[table_folder_path].exists = True
        actual_db.clusters[main_cluster_name].tables[table_path].exists = True

        actual_db.clusters[sync_cluster_name].nodes[table_folder_path].exists = True
        actual_db.clusters[sync_cluster_name].nodes[folder_path].exists = True
        desired_db.clusters[sync_cluster_name].nodes[folder_path].attributes.attributes = {}
        actual_db.clusters[sync_cluster_name].tables[table_path].exists = True

        diff = DbDiff.generate(settings, desired_db, actual_db)
        assert not diff.has_diff_for(sync_cluster_name)
        assert diff.has_diff_for(async_cluster_name)

        table_diff = self._check_and_get(diff, table_path)
        assert not table_diff.is_empty()

        assert YtReplica.make_key(sync_cluster_name, table_path) not in table_diff.missing_replicas
        assert YtReplica.make_key(async_cluster_name, table_path) in table_diff.missing_replicas

        assert table_diff.has_missing_replicas()  # has changes for any cluster
        assert not table_diff.has_missing_replicas(sync_cluster_name)
        assert next(table_diff.missing_replicas_for(sync_cluster_name), None) is None
        assert next(table_diff.missing_replicas_for(async_cluster_name), None) is not None

        assert not table_diff.has_deleted_replicas()
        assert next(table_diff.deleted_replicas(), None) is None

    def test_empty_diff(
        self,
        main_cluster_name: str,
        sync_cluster_name: str,
        async_cluster_name: str,
        table_path: str,
        default_schema: Types.Schema,
        dummy_logger: Logger,
    ):
        attrs = {"dynamic": True, "schema": default_schema}
        main_table = YtTable.make("k", main_cluster_name, YtTable.Type.REPLICATED_TABLE, table_path, True, attrs)
        diff = ReplicasChange.make(main_table, main_table)
        assert diff.is_empty()
        assert diff.check_and_log(dummy_logger)

        for cluster in (main_cluster_name, sync_cluster_name, async_cluster_name):
            assert not diff.has_diff_for(cluster)
        assert not diff.has_deleted_replicas()
        assert next(diff.deleted_replicas(), None) is None

    def _make_diff_from_dbs(self, desired_db: YtDatabase, actual_db: YtDatabase, table_path: str) -> ReplicasChange:
        desired_main_cluster = desired_db.main
        actual_main_cluster = actual_db.main
        desired_main_table = desired_main_cluster.tables[table_path]
        actual_main_table = actual_main_cluster.tables[table_path]
        diff = ReplicasChange.make(desired_main_table, actual_main_table)
        for desired_cluster in desired_db.replicas:
            actual_cluster = actual_db.clusters[desired_cluster.name]
            for table_key in desired_cluster.tables:
                diff.add_change_if_any(desired_cluster.tables[table_key], actual_cluster.tables[table_key])
        return diff

    def test_main_not_replicated(
        self,
        desired_db: YtDatabase,
        actual_db: YtDatabase,
        sync_cluster_name: str,
        table_path: str,
        dummy_logger: Logger,
    ):
        main = desired_db.main.tables[table_path]
        main.table_type = YtTable.Type.TABLE
        actual_db.clusters[sync_cluster_name].tables[table_path].exists = True

        diff = self._make_diff_from_dbs(desired_db, actual_db, table_path)
        assert diff.is_empty()
        assert diff.check_and_log(dummy_logger)

    def test_no_alive_data_replicas(
        self,
        desired_db: YtDatabase,
        actual_db: YtDatabase,
        sync_cluster_name: str,
        async_cluster_name: str,
        table_path: str,
        is_chaos: bool,
        dummy_logger: Logger,
    ):
        if is_chaos:
            for cluster_name in (sync_cluster_name, async_cluster_name):
                log_path = desired_db.clusters[cluster_name].tables[table_path].chaos_replication_log
                assert log_path
                actual_db.clusters[cluster_name].tables[log_path].exists = True

        diff = self._make_diff_from_dbs(desired_db, actual_db, table_path)
        assert not diff.is_empty()
        assert not diff.check_and_log(dummy_logger)

    def test_has_alive_data_replicas(
        self,
        desired_db: YtDatabase,
        actual_db: YtDatabase,
        sync_cluster_name: str,
        table_path: str,
        is_chaos: bool,
        dummy_logger: Logger,
    ):
        if is_chaos:
            log_path = desired_db.clusters[sync_cluster_name].tables[table_path].chaos_replication_log
            assert log_path
            actual_db.clusters[sync_cluster_name].tables[log_path].exists = True
        actual_db.clusters[sync_cluster_name].tables[table_path].exists = True

        diff = self._make_diff_from_dbs(desired_db, actual_db, table_path)
        assert not diff.is_empty()
        assert diff.check_and_log(dummy_logger)

    def test_incompatible_data_replica_schema(
        self,
        desired_db: YtDatabase,
        actual_db: YtDatabase,
        sync_cluster_name: str,
        table_path: str,
        dummy_logger: Logger,
        default_schema: Types.Schema,
    ):
        new_schema = deepcopy(default_schema)
        new_schema[-1]["type"] = "string"

        actual_data_replica = actual_db.clusters[sync_cluster_name].tables[table_path]
        actual_data_replica.exists = True
        actual_data_replica.schema = YtSchema.parse(new_schema)

        diff = self._make_diff_from_dbs(desired_db, actual_db, table_path)

        assert not diff.is_empty()
        assert not diff.check_and_log(dummy_logger)

    def test_deleted_replicas(
        self,
        is_chaos: bool,
        sync_cluster_name: str,
        async_cluster_name: str,
        table_path: str,
        default_schema: Types.Schema,
    ):
        desired_db = YtDatabase()
        self._add_clusters(desired_db, is_chaos)
        self._add_table(desired_db, table_path, default_schema, True, is_chaos, skip_clusters=set([async_cluster_name]))

        actual_db = YtDatabase()
        self._add_clusters(actual_db, is_chaos)
        self._add_table(actual_db, table_path, default_schema, True, is_chaos)

        diff = self._make_diff_from_dbs(desired_db, actual_db, table_path)
        assert diff.has_deleted_replicas()  # for any cluster
        assert not diff.has_deleted_replicas(sync_cluster_name)
        assert diff.has_deleted_replicas(async_cluster_name)

        for replica in diff.deleted_replicas():
            assert replica.cluster_name == async_cluster_name
        assert next(diff.deleted_replicas(sync_cluster_name), None) is None
        assert next(diff.deleted_replicas(async_cluster_name), None) is not None

    @pytest.mark.parametrize("exists", [True, False])
    def test_add_replicated(
        self, is_chaos: bool, main_cluster_name: str, table_path: str, default_schema: Types.Schema, exists: bool
    ):
        replicated_table_type: str = YtTable.Type.CHAOS_REPLICATED_TABLE if is_chaos else YtTable.Type.REPLICATED_TABLE
        replicated: YtTable = YtTable.make(
            table_path,
            main_cluster_name,
            replicated_table_type,
            table_path,
            True,
            {"dynamic": True, "schema": default_schema},
        )
        diff: ReplicasChange = ReplicasChange.make(replicated, replicated)
        table_copy = deepcopy(replicated)
        table_copy.exists = exists
        diff.add_change_if_any(table_copy, table_copy)
        assert not diff.alive_replicas
        assert not diff.missing_replicas

    def test_has_master_only(
        self,
        is_chaos: bool,
        main_cluster_name: str,
        sync_cluster_name: str,
        table_path: str,
        default_schema: Types.Schema,
    ):
        replicated_table_type: str = YtTable.Type.CHAOS_REPLICATED_TABLE if is_chaos else YtTable.Type.REPLICATED_TABLE
        replicated: YtTable = YtTable.make(
            table_path,
            main_cluster_name,
            replicated_table_type,
            table_path,
            True,
            {"dynamic": True, "schema": default_schema},
        )
        diff: ReplicasChange = ReplicasChange.make(replicated, replicated)
        assert not diff.has_master_only

        table: YtTable = YtTable.make(
            table_path,
            sync_cluster_name,
            YtTable.Type.TABLE,
            table_path,
            True,
            {"dynamic": True, "schema": default_schema},
        )
        diff.missing_replicas[table.key] = table
        assert diff.has_master_only
        diff.alive_replicas[table.key] = table
        assert not diff.has_master_only


class TestReplicasChangeChaos(TestReplicasChange):
    @pytest.fixture
    def is_chaos(self) -> bool:
        return True

    def test_diff_missing_replica(
        self,
        settings: Settings,
        desired_db: YtDatabase,
        actual_db: YtDatabase,
        sync_cluster_name: str,
        async_cluster_name: str,
        table_path: str,
    ):
        log_path = desired_db.clusters[sync_cluster_name].tables[table_path].chaos_replication_log
        assert log_path

        actual_db.main.tables[table_path].exists = True
        actual_db.clusters[sync_cluster_name].tables[table_path].exists = True

        diff = DbDiff.generate(settings, desired_db, actual_db)
        table_diff = self._check_and_get(diff, table_path)
        assert YtReplica.make_key(sync_cluster_name, table_path) not in table_diff.missing_replicas

        expected_missing = [
            YtReplica.make_key(sync_cluster_name, log_path),
            YtReplica.make_key(async_cluster_name, table_path),
            YtReplica.make_key(async_cluster_name, log_path),
        ]
        for key in expected_missing:
            assert key in table_diff.missing_replicas

        assert table_diff.has_missing_replicas()  # has changes for any cluster
        assert table_diff.has_missing_replicas(sync_cluster_name)  # replication log here
        assert next(table_diff.missing_replicas_for(sync_cluster_name), None) is not None
        assert next(table_diff.missing_replicas_for(async_cluster_name), None) is not None

    def test_incompatible_queue_replica_schema(
        self,
        desired_db: YtDatabase,
        actual_db: YtDatabase,
        sync_cluster_name: str,
        table_path: str,
        dummy_logger: Logger,
        default_schema: Types.Schema,
    ):
        new_schema = deepcopy(default_schema)
        new_schema[-1]["type"] = "string"

        actual_db.clusters[sync_cluster_name].tables[table_path].exists = True  # ensure alive data replica

        log_path = desired_db.clusters[sync_cluster_name].tables[table_path].chaos_replication_log
        assert log_path
        actual_queue_replica = actual_db.clusters[sync_cluster_name].tables[log_path]
        actual_queue_replica.exists = True
        actual_queue_replica.schema = YtSchema.parse(new_schema)

        diff = self._make_diff_from_dbs(desired_db, actual_db, table_path)

        assert not diff.is_empty()
        assert diff.check_and_log(dummy_logger)
