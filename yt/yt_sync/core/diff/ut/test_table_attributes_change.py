from copy import deepcopy
from logging import Logger

import pytest

from yt.yt_sync.core.diff import DbDiff
from yt.yt_sync.core.diff import TableAttributesChange
from yt.yt_sync.core.diff import TableDiffType
from yt.yt_sync.core.model import Types
from yt.yt_sync.core.model import YtCluster
from yt.yt_sync.core.model import YtDatabase
from yt.yt_sync.core.model import YtReplica
from yt.yt_sync.core.model import YtTable
from yt.yt_sync.core.model import YtTableAttributes
from yt.yt_sync.core.model import YtTabletState
from yt.yt_sync.core.settings import Settings

from .helpers import DiffDbTestBase


class TestTableAttributeChange(DiffDbTestBase):
    @pytest.fixture
    def affected_cluster(self) -> str:
        return "primary"

    def _check_and_get(self, diff: DbDiff, table_path: str) -> TableAttributesChange:
        assert table_path in diff.tables_diff
        diff_list = diff.tables_diff[table_path]
        assert 1 == len(diff_list)
        table_diff = diff_list[0]
        assert TableDiffType.ATTRIBUTES_CHANGE == table_diff.diff_type
        assert isinstance(table_diff, TableAttributesChange)
        return table_diff

    def _check_and_get_attr_diff(
        self, table_diff: TableAttributesChange, affected_cluster: str, table_path: str
    ) -> TableAttributesChange.ChangeItem:
        assert 1 == len(table_diff.changes)
        key = YtReplica.make_key(affected_cluster, table_path)
        assert key in table_diff.changes
        return table_diff.changes[key]

    def _test_table_type_diff(
        self,
        settings: Settings,
        desired_db: YtDatabase,
        affected_cluster: str,
        modify_table_path: str,
        get_table_path: str,
        key_table_path: str,
        desired_table_type: str,
    ):
        actual_db = deepcopy(desired_db)
        actual_db.clusters[affected_cluster].tables[modify_table_path].table_type = YtTable.Type.TABLE

        diff = DbDiff.generate(settings, desired_db, actual_db)
        table_diff = self._check_and_get(diff, get_table_path)
        change = self._check_and_get_attr_diff(table_diff, affected_cluster, key_table_path)

        assert not change.attribute_changes
        assert not change.missing_user_attributes
        assert change.type_changes == (desired_table_type, YtTable.Type.TABLE)

        assert diff.has_diff_for(affected_cluster)
        for cluster in {"primary", "remote0", "remote1"} - {affected_cluster}:
            assert not diff.has_diff_for(cluster)

    def _test_attribute_diff(
        self,
        settings: Settings,
        desired_db: YtDatabase,
        affected_cluster: str,
        modify_table_path: str,
        get_table_path: str,
        key_table_path: str,
    ):
        desired_db.clusters[affected_cluster].tables[modify_table_path].attributes["my_attr"] = "v1"
        actual_db = deepcopy(desired_db)
        actual_db.clusters[affected_cluster].tables[modify_table_path].attributes["my_attr"] = "v2"

        diff = DbDiff.generate(settings, desired_db, actual_db)
        table_diff = self._check_and_get(diff, get_table_path)
        change = self._check_and_get_attr_diff(table_diff, affected_cluster, key_table_path)

        assert not change.type_changes
        assert not change.missing_user_attributes
        assert change.attribute_changes
        desired, actual = change.attribute_changes
        assert "v1" == desired["my_attr"]
        assert "v2" == actual["my_attr"]

        assert diff.has_diff_for(affected_cluster)
        for cluster in {"primary", "remote0", "remote1"} - {affected_cluster}:
            assert not diff.has_diff_for(cluster)

    def _test_missing_user_attributes_diff(
        self,
        settings: Settings,
        desired_db: YtDatabase,
        affected_cluster: str,
        modify_table_path: str,
        get_table_path: str,
        key_table_path: str,
    ):
        desired_db.clusters[affected_cluster].tables[modify_table_path].attributes["my_attr"] = "v"
        actual_db = deepcopy(desired_db)
        actual_db.clusters[affected_cluster].tables[modify_table_path].attributes.user_attribute_keys = frozenset(
            ["my_attr", "my_attr2"]
        )

        diff = DbDiff.generate(settings, desired_db, actual_db)
        table_diff = self._check_and_get(diff, get_table_path)
        assert not table_diff.has_replica_changes()
        change = self._check_and_get_attr_diff(table_diff, affected_cluster, key_table_path)

        assert not change.type_changes
        assert not change.attribute_changes
        assert change.missing_user_attributes
        assert 1 == len(change.missing_user_attributes)
        assert "my_attr2" in change.missing_user_attributes

        assert diff.has_diff_for(affected_cluster)
        for cluster in {"primary", "remote0", "remote1"} - {affected_cluster}:
            assert not diff.has_diff_for(cluster)

    def test_table_type_diff(self, settings: Settings, desired_db: YtDatabase, affected_cluster: str, table_path: str):
        self._test_table_type_diff(
            settings=settings,
            desired_db=desired_db,
            affected_cluster=affected_cluster,
            modify_table_path=table_path,
            get_table_path=table_path,
            key_table_path=table_path,
            desired_table_type=YtTable.Type.REPLICATED_TABLE,
        )

    def test_attribute_diff(self, settings: Settings, desired_db: YtDatabase, affected_cluster: str, table_path: str):
        self._test_attribute_diff(
            settings=settings,
            desired_db=desired_db,
            affected_cluster=affected_cluster,
            modify_table_path=table_path,
            get_table_path=table_path,
            key_table_path=table_path,
        )

    def test_missing_user_attributes_diff(
        self, settings: Settings, desired_db: YtDatabase, affected_cluster: str, table_path: str
    ):
        self._test_missing_user_attributes_diff(
            settings=settings,
            desired_db=desired_db,
            affected_cluster=affected_cluster,
            modify_table_path=table_path,
            get_table_path=table_path,
            key_table_path=table_path,
        )

    def test_replica_attrs_diff(self, settings: Settings, desired_db: YtDatabase, table_path: str, is_chaos: bool):
        actual_db = deepcopy(desired_db)
        actual_main = actual_db.clusters["primary"].tables[table_path]

        log_path = None
        if is_chaos:
            log_path = actual_db.clusters["remote0"].tables[table_path].chaos_replication_log
            assert log_path
            replica_key = YtReplica.make_key("remote0", log_path)
        else:
            replica_key = YtReplica.make_key("remote0", table_path)
        actual_main.replicas[replica_key].enabled = False

        diff = DbDiff.generate(settings, desired_db, actual_db)
        table_diff = self._check_and_get(diff, table_path)
        assert table_diff.has_replica_changes()

        attr_diff = self._check_and_get_attr_diff(table_diff, "primary", table_path)
        assert len(attr_diff.replica_changes) == 1
        desired, actual = attr_diff.replica_changes[0]
        if is_chaos:
            assert desired.replica_path == log_path
            assert actual.replica_path == log_path
        else:
            assert desired.replica_path == table_path
            assert actual.replica_path == table_path

    @pytest.mark.parametrize("rtt_enabled", [True, False], ids=["rtt_on", "rtt_off"])
    def test_replica_mode_diff(
        self, settings: Settings, desired_db: YtDatabase, table_path: str, is_chaos: bool, rtt_enabled: bool
    ):
        desired_main = desired_db.clusters["primary"].tables[table_path]
        desired_main.rtt_options.enabled = rtt_enabled

        actual_db = deepcopy(desired_db)
        actual_main = actual_db.clusters["primary"].tables[table_path]

        log_path = None
        if is_chaos:
            log_path = actual_db.clusters["remote0"].tables[table_path].chaos_replication_log
            assert log_path
            replica_key = YtReplica.make_key("remote0", log_path)
        else:
            replica_key = YtReplica.make_key("remote0", table_path)

        desired_replica: YtReplica = desired_main.replicas[replica_key]
        desired_replica.enable_replicated_table_tracker = rtt_enabled

        actual_replica: YtReplica = actual_main.replicas[replica_key]
        actual_replica.enable_replicated_table_tracker = rtt_enabled
        actual_replica.mode = YtReplica.Mode.SYNC  # replica mode diff

        diff = DbDiff.generate(settings, desired_db, actual_db)
        if rtt_enabled:
            assert diff.is_empty()
            return

        table_diff = self._check_and_get(diff, table_path)
        assert table_diff.has_replica_changes()

        attr_diff = self._check_and_get_attr_diff(table_diff, "primary", table_path)
        assert len(attr_diff.replica_changes) == 1
        desired, actual = attr_diff.replica_changes[0]
        if is_chaos:
            assert desired.replica_path == log_path
            assert actual.replica_path == log_path
        else:
            assert desired.replica_path == table_path
            assert actual.replica_path == table_path

    def test_rtt_options_diff(self, settings: Settings, desired_db: YtDatabase, table_path: str):
        desired_main = desired_db.clusters["primary"].tables[table_path]
        desired_main.rtt_options.enabled = True
        desired_main.rtt_options.preferred_sync_replica_clusters = {"remote0"}

        actual_db = deepcopy(desired_db)
        actual_main = actual_db.clusters["primary"].tables[table_path]
        actual_main.rtt_options.preferred_sync_replica_clusters = {"remote1"}

        diff = DbDiff.generate(settings, desired_db, actual_db)
        table_diff = self._check_and_get(diff, table_path)
        attr_diff = self._check_and_get_attr_diff(table_diff, "primary", table_path)
        assert attr_diff.rtt_options_changes is not None

    def test_add_change_on_same(self, table_path: str, default_schema: Types.Schema):
        table = YtTable.make("k", "primary", YtTable.Type.TABLE, table_path, True, {"schema": default_schema})
        diff = TableAttributesChange.make(table)
        diff.add_change_if_any(table, table)
        assert diff.is_empty()

    def test_check_and_log_type_diff(self, table_path: str, default_schema: Types.Schema, dummy_logger: Logger):
        table1 = YtTable.make(
            "k", "primary", YtTable.Type.REPLICATED_TABLE, table_path, True, {"schema": default_schema}
        )
        diff = TableAttributesChange.make(table1)
        table2 = YtTable.make("k", "primary", YtTable.Type.TABLE, table_path, True, {"schema": default_schema})
        diff.add_change_if_any(table1, table2)
        assert not diff.is_empty()
        assert not diff.check_and_log(dummy_logger)

    def test_check_and_log_attr_diff(self, table_path: str, default_schema: Types.Schema, dummy_logger: Logger):
        table1 = YtTable.make(
            "k", "primary", YtTable.Type.TABLE, table_path, True, {"schema": default_schema, "my_attr": 2}
        )
        diff = TableAttributesChange.make(table1)
        table1 = YtTable.make(
            "k", "primary", YtTable.Type.TABLE, table_path, True, {"schema": default_schema, "my_attr": 2}
        )
        table2 = YtTable.make(
            "k", "primary", YtTable.Type.TABLE, table_path, True, {"schema": default_schema, "my_attr": 1}
        )
        diff.add_change_if_any(table1, table2)
        assert not diff.is_empty()
        assert diff.check_and_log(dummy_logger)

    def test_replica_attrs_on_not_replicated(self, table_path: str, default_schema: Types.Schema, is_chaos: bool):
        table_attrs = {"schema": default_schema}
        table1 = YtTable.make("k", "primary", YtTable.Type.REPLICATED_TABLE, table_path, True, table_attrs)
        diff = TableAttributesChange.make(table1)

        yt_cluster = YtCluster.make(YtCluster.Type.REPLICA, "remote0", is_chaos, YtCluster.Mode.SYNC)
        replica_table = yt_cluster.add_table_from_attributes("k", YtTable.Type.TABLE, table_path, True, table_attrs)
        table1.add_replica_for(replica_table, yt_cluster, YtReplica.Mode.SYNC)

        table2 = YtTable.make("k", "primary", YtTable.Type.TABLE, table_path, True, table_attrs)

        diff.add_change_if_any(table1, table2)
        assert not diff.is_empty()
        change_item = diff.changes[YtReplica.make_key("primary", table_path)]
        assert not change_item.replica_changes

    def test_unmount_required_not(self, table_path: str, default_schema: Types.Schema):
        table1 = YtTable.make(
            "k", "primary", YtTable.Type.TABLE, table_path, True, {"schema": default_schema, "my_attr": 2}
        )
        diff = TableAttributesChange.make(table1)
        table2 = YtTable.make(
            "k", "primary", YtTable.Type.TABLE, table_path, True, {"schema": default_schema, "my_attr": 1}
        )
        diff.add_change_if_any(table1, table2)
        assert not diff.is_unmount_required("primary")

    @pytest.mark.parametrize("unmount_attr", YtTableAttributes._REQUIRE_UNMOUNT)
    def test_unmount_required_update(self, table_path: str, default_schema: Types.Schema, unmount_attr: str):
        table1 = YtTable.make(
            "k", "primary", YtTable.Type.TABLE, table_path, True, {"schema": default_schema, unmount_attr: "2"}
        )

        diff = TableAttributesChange.make(table1)

        table2 = YtTable.make(
            "k", "primary", YtTable.Type.TABLE, table_path, True, {"schema": default_schema, unmount_attr: "1"}
        )
        diff.add_change_if_any(table1, table2)
        assert diff.is_unmount_required("primary")
        assert not diff.is_unmount_required("remote0")

    @pytest.mark.parametrize("unmount_attr", YtTableAttributes._REQUIRE_UNMOUNT)
    def test_unmount_required_add(self, table_path: str, default_schema: Types.Schema, unmount_attr: str):
        table1 = YtTable.make(
            "k", "primary", YtTable.Type.TABLE, table_path, True, {"schema": default_schema, unmount_attr: "2"}
        )

        diff = TableAttributesChange.make(table1)

        table2 = YtTable.make("k", "primary", YtTable.Type.TABLE, table_path, True, {"schema": default_schema})
        diff.add_change_if_any(table1, table2)
        assert diff.is_unmount_required("primary")
        assert not diff.is_unmount_required("remote0")

    @pytest.mark.parametrize("unmount_attr", YtTableAttributes._REQUIRE_UNMOUNT)
    def test_unmount_required_remove(self, table_path: str, default_schema: Types.Schema, unmount_attr: str):
        table1 = YtTable.make(
            "k", "primary", YtTable.Type.TABLE, table_path, True, {"schema": default_schema, unmount_attr: None}
        )

        diff = TableAttributesChange.make(table1)

        table2 = YtTable.make(
            "k", "primary", YtTable.Type.TABLE, table_path, True, {"schema": default_schema, unmount_attr: "1"}
        )
        diff.add_change_if_any(table1, table2)
        assert diff.is_unmount_required("primary")
        assert not diff.is_unmount_required("remote0")

    @pytest.mark.parametrize("unmount_attr", YtTableAttributes._REQUIRE_UNMOUNT)
    def test_mount_required_absent(self, table_path: str, default_schema: Types.Schema, unmount_attr: str):
        table1 = YtTable.make("k", "primary", YtTable.Type.TABLE, table_path, True, {"schema": default_schema})
        diff = TableAttributesChange.make(table1)
        table2 = YtTable.make(
            "k", "primary", YtTable.Type.TABLE, table_path, True, {"schema": default_schema, unmount_attr: "1"}
        )
        diff.add_change_if_any(table1, table2)
        assert not diff.is_unmount_required("primary")
        assert not diff.is_unmount_required("remote0")

    def test_remount_required(self, table_path: str, default_schema: Types.Schema):
        table1 = YtTable.make(
            "k", "primary", YtTable.Type.TABLE, table_path, True, {"schema": default_schema, "my_attr": 1}
        )
        diff = TableAttributesChange.make(table1)
        table2 = YtTable.make(
            "k", "primary", YtTable.Type.TABLE, table_path, True, {"schema": default_schema, "my_attr": 2}
        )
        diff.add_change_if_any(table1, table2)
        assert diff.is_remount_required("primary")

    def test_remount_required_not(self, table_path: str, default_schema: Types.Schema):
        table1 = YtTable.make(
            "k", "primary", YtTable.Type.TABLE, table_path, True, {"schema": default_schema, "version": 1}
        )
        diff = TableAttributesChange.make(table1)
        table2 = YtTable.make(
            "k", "primary", YtTable.Type.TABLE, table_path, True, {"schema": default_schema, "version": 2}
        )
        diff.add_change_if_any(table1, table2)
        assert not diff.is_remount_required("primary")

    @pytest.mark.parametrize("table_type", [YtTable.Type.TABLE, YtTable.Type.REPLICATED_TABLE])
    def test_tablet_state_change(self, table_path: str, default_schema: Types.Schema, table_type: str):
        table1 = YtTable.make("k", "primary", table_type, table_path, True, {"schema": default_schema})
        diff = TableAttributesChange.make(table1)
        table1.tablet_state.set(YtTabletState.MOUNTED)
        table2 = YtTable.make("k", "primary", table_type, table_path, True, {"schema": default_schema})
        table2.tablet_state.set(YtTabletState.UNMOUNTED)
        diff.add_change_if_any(table1, table2)
        assert diff.has_diff_for("primary")
        assert diff.changes[table1.replica_key].tablet_state_changes


class TestAttributeChangeChaos(TestTableAttributeChange):
    @pytest.fixture
    def is_chaos(self) -> bool:
        return True

    @pytest.fixture
    def affected_cluster(self) -> str:
        return "remote1"

    def _get_log_path(self, db: YtDatabase, affected_cluster: str, table_path: str):
        log_path = db.clusters[affected_cluster].tables[table_path].chaos_replication_log
        assert log_path
        return log_path

    def test_table_type_diff(self, settings: Settings, desired_db: YtDatabase, affected_cluster: str, table_path: str):
        log_path = self._get_log_path(desired_db, affected_cluster, table_path)
        self._test_table_type_diff(
            settings=settings,
            desired_db=desired_db,
            affected_cluster=affected_cluster,
            modify_table_path=log_path,
            get_table_path=table_path,
            key_table_path=log_path,
            desired_table_type=YtTable.Type.REPLICATION_LOG,
        )

    def test_attribute_diff(self, settings: Settings, desired_db: YtDatabase, affected_cluster: str, table_path: str):
        log_path = self._get_log_path(desired_db, affected_cluster, table_path)
        self._test_attribute_diff(
            settings=settings,
            desired_db=desired_db,
            affected_cluster=affected_cluster,
            modify_table_path=log_path,
            get_table_path=table_path,
            key_table_path=log_path,
        )

    def test_missing_user_attributes_diff(
        self, settings: Settings, desired_db: YtDatabase, affected_cluster: str, table_path: str
    ):
        log_path = self._get_log_path(desired_db, affected_cluster, table_path)
        self._test_missing_user_attributes_diff(
            settings=settings,
            desired_db=desired_db,
            affected_cluster=affected_cluster,
            modify_table_path=log_path,
            get_table_path=table_path,
            key_table_path=log_path,
        )

    @pytest.mark.parametrize(
        "table_type", [YtTable.Type.TABLE, YtTable.Type.REPLICATION_LOG, YtTable.Type.CHAOS_REPLICATED_TABLE]
    )
    def test_tablet_state_change(self, table_path: str, default_schema: Types.Schema, table_type: str):
        table1 = YtTable.make("k", "primary", table_type, table_path, True, {"schema": default_schema})
        diff = TableAttributesChange.make(table1)
        table1.tablet_state.set(YtTabletState.MOUNTED)
        table2 = YtTable.make("k", "primary", table_type, table_path, True, {"schema": default_schema})
        table2.tablet_state.set(YtTabletState.UNMOUNTED)
        diff.add_change_if_any(table1, table2)
        if table1.is_mountable:
            assert diff.has_diff_for("primary")
            assert diff.changes[table1.replica_key].tablet_state_changes
        else:
            assert not diff.has_diff_for("primary")
