from copy import deepcopy

import pytest

from yt.yt_sync.core.diff import DbDiff
from yt.yt_sync.core.diff import NodeDiffType
from yt.yt_sync.core.diff import TableDiffType
from yt.yt_sync.core.model import get_folder
from yt.yt_sync.core.model import Types
from yt.yt_sync.core.model import YtCluster
from yt.yt_sync.core.model import YtColumn
from yt.yt_sync.core.model import YtDatabase
from yt.yt_sync.core.model import YtNode
from yt.yt_sync.core.model import YtNodeAttributes
from yt.yt_sync.core.model import YtTable
from yt.yt_sync.core.settings import Settings
from yt.yt_sync.core.spec import Column
from yt.yt_sync.core.spec import TypeV1

from .helpers import DiffDbTestBase


class TestBaseDiff(DiffDbTestBase):
    def test_empty_desired(self, settings: Settings):
        assert not DbDiff.generate(settings, YtDatabase(), YtDatabase()).tables_diff

    def test_no_main_in_desired(self, settings: Settings):
        desired = YtDatabase()
        desired.add_or_get_cluster(YtCluster.make(YtCluster.Type.REPLICA, "remote0", False, YtCluster.Mode.SYNC))
        with pytest.raises(AssertionError):
            DbDiff.generate(settings, desired, YtDatabase())

    def test_no_main_in_actual(self, settings: Settings, desired_db: YtDatabase):
        with pytest.raises(AssertionError):
            DbDiff.generate(settings, desired_db, YtDatabase())

    def test_no_table_in_actual_main(self, settings: Settings, desired_db: YtDatabase):
        actual_db = YtDatabase()
        self._add_clusters(actual_db, False)
        with pytest.raises(AssertionError):
            DbDiff.generate(settings, desired_db, actual_db)

    def test_no_table_in_actual_replica(
        self, settings: Settings, desired_db: YtDatabase, table_path: str, default_schema: Types.Schema, is_chaos: bool
    ):
        actual_db = YtDatabase()
        self._add_clusters(actual_db, False)
        self._add_table(actual_db, table_path, default_schema, False, is_chaos)
        actual_db.clusters["remote1"].tables.clear()

        with pytest.raises(AssertionError):
            DbDiff.generate(settings, desired_db, actual_db)

    def test_no_diff(self, settings: Settings, desired_db: YtDatabase):
        diff = DbDiff.generate(settings, desired_db, desired_db)
        assert not diff.tables_diff

        for cluster in ("primary", "remote0", "remote1"):
            assert not diff.has_diff_for(cluster)

    @pytest.mark.parametrize("attrs_only", [True, False])
    def test_incompatible_schema(self, settings: Settings, desired_db: YtDatabase, table_path: str, attrs_only: bool):
        actual_db = deepcopy(desired_db)
        for cluster in desired_db.all_clusters:
            cluster.tables[table_path].schema.columns[-1].column_type = "string"
        diff = DbDiff.generate(settings, desired_db, actual_db)
        assert attrs_only == diff.is_valid({TableDiffType.ATTRIBUTES_CHANGE} if attrs_only else set())
        for cluster in ("primary", "remote0", "remote1"):
            assert diff.has_diff_for(cluster)

    @pytest.mark.parametrize("attrs_only", [True, False])
    def test_incompatible_attr(self, settings: Settings, desired_db: YtDatabase, table_path: str, attrs_only: bool):
        actual_db = deepcopy(desired_db)
        for cluster in desired_db.all_clusters:
            cluster.tables[table_path].table_type = YtTable.Type.REPLICATION_LOG
        diff = DbDiff.generate(settings, desired_db, actual_db)
        assert not diff.is_valid({TableDiffType.ATTRIBUTES_CHANGE} if attrs_only else set())
        for cluster in ("primary", "remote0", "remote1"):
            assert diff.has_diff_for(cluster)

    def test_is_unmount_required(self, settings: Settings, desired_db: YtDatabase, table_path: str):
        actual_db = deepcopy(desired_db)
        desired_db.clusters["primary"].tables[table_path].attributes["account"] = "1"
        actual_db.clusters["remote0"].tables[table_path].schema.columns.append(
            YtColumn.make(Column(name="added_column", type=TypeV1.UINT64))
        )
        diff = DbDiff.generate(settings, desired_db, actual_db)
        assert diff.is_unmount_required("primary")
        assert diff.is_unmount_required("remote0")
        assert not diff.is_unmount_required("remote1")
        assert diff.is_data_modification_required()
        assert diff.is_data_modification_required("remote0")

    def test_missing_replicas_to_missing_table(
        self,
        settings: Settings,
        desired_db: YtDatabase,
        table_path: str,
        main_cluster_name: str,
        remote_clusters: list[str],
    ):
        actual_db = deepcopy(desired_db)
        for cluster_name in remote_clusters:
            actual_db.clusters[cluster_name].tables[table_path].exists = False
        diff = DbDiff.generate(settings, desired_db, actual_db)
        assert next(diff.table_diff_for(main_cluster_name, {TableDiffType.MISSING_TABLE}), None) is not None

    def test_missing_folders_for_missing_table(
        self,
        settings: Settings,
        desired_db: YtDatabase,
        table_path: str,
        folder_path: str,
    ):
        actual_db = deepcopy(desired_db)
        actual_db.clusters["remote0"].nodes[folder_path].exists = False

        diff = DbDiff.generate(settings, desired_db, actual_db)

        assert not diff.has_diff_for("primary")

        current_diff = iter(diff.node_diff_for("remote0", {NodeDiffType.MISSING_NODE}))
        node_diff = next(current_diff, None)
        assert node_diff is not None
        assert node_diff.desired_node.path == folder_path
        assert not node_diff.is_empty()
        assert next(current_diff, None) is None
        assert next(diff.node_diff_for("remote0", {NodeDiffType.ATTRIBUTES_CHANGE}), None) is None

        assert not diff.has_diff_for("remote1")

    def test_conflicting_node_type(self, settings: Settings, desired_db: YtDatabase, folder_path: str):
        actual_db = deepcopy(desired_db)
        actual_db.clusters["remote0"].nodes[folder_path].node_type = YtNode.Type.FILE
        db_diff = DbDiff.generate(settings, desired_db, actual_db)
        assert not db_diff.is_valid({NodeDiffType.ATTRIBUTES_CHANGE})

    @pytest.mark.parametrize("node_exists", [True, False])
    def test_any_node_type(self, settings: Settings, desired_db: YtDatabase, folder_path: str, node_exists: bool):
        actual_db = deepcopy(desired_db)
        desired_db.clusters["remote0"].nodes[folder_path].node_type = YtNode.Type.ANY
        actual_db.clusters["remote0"].nodes[folder_path].exists = node_exists
        actual_db.clusters["remote0"].nodes[folder_path].attributes["attribute"] = "new_value"
        db_diff = DbDiff.generate(settings, desired_db, actual_db)
        assert db_diff.is_valid() == node_exists

    def test_node_diff(self, settings: Settings, desired_db: YtDatabase, table_path: str, folder_path: str):
        table_folder_path = get_folder(table_path)
        actual_db = deepcopy(desired_db)
        actual_db.clusters["remote0"].nodes[folder_path].exists = False
        actual_db.clusters["remote0"].nodes[folder_path].attributes = YtNodeAttributes.make({"special": True})
        desired_db.clusters["primary"].nodes[table_folder_path].attributes = YtNodeAttributes.make({"special": True})
        diff = DbDiff.generate(settings, desired_db, actual_db)
        assert diff.is_valid()

        assert next(diff.node_diff_for("primary", {NodeDiffType.MISSING_NODE}), None) is None
        current_diff = iter(diff.node_diff_for("primary", {NodeDiffType.ATTRIBUTES_CHANGE}))
        diff_remote = next(current_diff, None)
        assert diff_remote is not None
        assert diff_remote.diff_type == NodeDiffType.ATTRIBUTES_CHANGE
        assert diff_remote.desired_node.path == table_folder_path
        assert not diff_remote.is_empty()
        assert diff_remote.get_changes_as_dict() == {"special": (True, None)}
        assert next(current_diff, None) is None

        current_diff = iter(diff.node_diff_for("remote0", {NodeDiffType.MISSING_NODE}))
        diff_remote = next(current_diff, None)
        assert diff_remote is not None
        assert diff_remote.diff_type == NodeDiffType.MISSING_NODE
        assert diff_remote.desired_node.path == folder_path
        assert not diff_remote.is_empty()
        assert next(current_diff, None) is None
        assert next(diff.node_diff_for("remote0", {NodeDiffType.ATTRIBUTES_CHANGE}), None) is None

        assert next(diff.node_diff_for("remote1"), None) is None

    @pytest.mark.parametrize("nodes_only", [True, False])
    def test_diff_for(
        self, settings: Settings, desired_db: YtDatabase, table_path: str, folder_path: str, nodes_only: bool
    ):
        table_folder_path = get_folder(table_path)
        actual_db = deepcopy(desired_db)
        desired_db.clusters["primary"].tables[table_path].attributes["my_attr"] = "1"
        desired_db.clusters["primary"].nodes[table_folder_path].attributes["attribute"] = "prev"
        actual_db.clusters["remote0"].tables[table_path].schema.columns.append(
            YtColumn.make(Column(name="added_column", type=TypeV1.UINT64))
        )
        actual_db.clusters["remote0"].nodes[folder_path].exists = False
        diff = DbDiff.generate(settings, desired_db, actual_db, nodes_only=nodes_only)

        assert next(diff.node_diff_for("primary"), None) is not None
        assert next(diff.node_diff_for("remote0"), None) is not None
        assert next(diff.node_diff_for("remote1"), None) is None
        assert next(diff.node_diff_for(None), None) is not None

        if nodes_only:
            assert next(diff.table_diff_for("primary"), None) is None
            assert next(diff.table_diff_for("remote0"), None) is None
            assert next(diff.table_diff_for("remote1"), None) is None
            assert next(diff.table_diff_for(None), None) is None
        else:
            assert next(diff.table_diff_for("primary"), None) is not None
            assert next(diff.table_diff_for("remote0"), None) is not None
            assert next(diff.table_diff_for("remote1"), None) is None
            assert next(diff.table_diff_for(None), None) is not None

        assert next(diff.node_diff_for("primary", set([NodeDiffType.MISSING_NODE])), None) is None
        assert next(diff.node_diff_for("primary", set([NodeDiffType.ATTRIBUTES_CHANGE])), None) is not None
        if nodes_only:
            assert next(diff.table_diff_for("primary", set([TableDiffType.ATTRIBUTES_CHANGE])), None) is None
        else:
            assert next(diff.table_diff_for("primary", set([TableDiffType.ATTRIBUTES_CHANGE])), None) is not None
        assert next(diff.table_diff_for("primary", set([TableDiffType.SCHEMA_CHANGE])), None) is None

        assert next(diff.node_diff_for("remote0", set([NodeDiffType.MISSING_NODE])), None) is not None
        assert next(diff.node_diff_for("remote0", set([NodeDiffType.ATTRIBUTES_CHANGE])), None) is None
        assert next(diff.table_diff_for("remote0", set([TableDiffType.ATTRIBUTES_CHANGE])), None) is None
        if nodes_only:
            assert next(diff.table_diff_for("remote0", set([TableDiffType.SCHEMA_CHANGE])), None) is None
        else:
            assert next(diff.table_diff_for("remote0", set([TableDiffType.SCHEMA_CHANGE])), None) is not None

        assert next(diff.node_diff_for(None, set([NodeDiffType.MISSING_NODE])), None) is not None
        assert next(diff.node_diff_for(None, set([NodeDiffType.ATTRIBUTES_CHANGE])), None) is not None
        if nodes_only:
            assert next(diff.table_diff_for(None, set([TableDiffType.ATTRIBUTES_CHANGE])), None) is None
            assert next(diff.table_diff_for(None, set([TableDiffType.SCHEMA_CHANGE])), None) is None
        else:
            assert next(diff.table_diff_for(None, set([TableDiffType.ATTRIBUTES_CHANGE])), None) is not None
            assert next(diff.table_diff_for(None, set([TableDiffType.SCHEMA_CHANGE])), None) is not None

        assert next(diff.schema_diff_for("primary"), None) is None
        if nodes_only:
            assert next(diff.schema_diff_for("remote0"), None) is None
            assert next(diff.schema_diff_for(None), None) is None
        else:
            assert next(diff.schema_diff_for("remote0"), None) is not None
            assert next(diff.schema_diff_for(None), None) is not None

        assert diff.has_schema_changes() != nodes_only


class TestBaseDiffChaos(DiffDbTestBase):
    @pytest.fixture
    def is_chaos(self) -> bool:
        return True

    def test_no_diff(self, settings: Settings, desired_db: YtDatabase):
        diff = DbDiff.generate(settings, desired_db, desired_db)
        assert not diff.tables_diff
