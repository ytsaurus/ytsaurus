from copy import deepcopy
from logging import Logger

import pytest

from yt.yt_sync.core.diff import DbDiff
from yt.yt_sync.core.diff import SchemaChange
from yt.yt_sync.core.diff import TableDiffType
from yt.yt_sync.core.model import Types
from yt.yt_sync.core.model import YtDatabase
from yt.yt_sync.core.model import YtReplica
from yt.yt_sync.core.model import YtSchema
from yt.yt_sync.core.model import YtTable
from yt.yt_sync.core.settings import Settings

from .helpers import DiffDbTestBase


class TestSchemaChange(DiffDbTestBase):
    @pytest.fixture
    def new_schema(self, default_schema: Types.Schema) -> Types.Schema:
        new_schema = deepcopy(default_schema)
        new_schema.append({"name": "new_field", "type": "string"})
        return new_schema

    @pytest.fixture
    def adjusted_actual_db(self, actual_db: YtDatabase, new_schema: Types.Schema) -> YtDatabase:
        for cluster in actual_db.clusters.values():
            for table in cluster.tables.values():
                table.exists = True
                if "remote1" == table.cluster_name:
                    table.schema = YtSchema.parse(new_schema)
        return actual_db

    def _check_and_get(self, diff: DbDiff, table_path: str) -> SchemaChange:
        assert table_path in diff.tables_diff
        diff_list = diff.tables_diff[table_path]
        assert 1 == len(diff_list)
        table_diff = diff_list[0]
        assert TableDiffType.SCHEMA_CHANGE == table_diff.diff_type
        assert isinstance(table_diff, SchemaChange)
        return table_diff

    def _check_table_diff(
        self,
        table_diff: SchemaChange,
        expected_keys: list[Types.ReplicaKey],
        default_schema: Types.Schema,
        new_schema: Types.Schema,
    ):
        desired_yt_schema = YtSchema.parse(default_schema)
        actual_yt_schema = YtSchema.parse(new_schema)

        for key in expected_keys:
            assert key in table_diff.schema_changes
            diff_desired_schema, diff_actual_schema = table_diff.schema_changes[key]
            assert desired_yt_schema == diff_desired_schema
            assert actual_yt_schema == diff_actual_schema

    def test_diff_schema_change(
        self,
        settings: Settings,
        desired_db: YtDatabase,
        adjusted_actual_db: YtDatabase,
        default_schema: Types.Schema,
        new_schema: Types.Schema,
        table_path: str,
    ):
        diff = DbDiff.generate(settings, desired_db, adjusted_actual_db)
        table_diff = self._check_and_get(diff, table_path)
        expected_keys = [YtReplica.make_key("remote1", table_path)]
        self._check_table_diff(table_diff, expected_keys, default_schema, new_schema)

    def test_add_schema_change_on_same_table(self, table_path: str, default_schema: Types.Schema, settings: Settings):
        diff = SchemaChange.make(settings)
        table = YtTable.make("k", "primary", YtTable.Type.TABLE, table_path, True, {"schema": default_schema})
        diff.add_change_if_any(table, table)
        assert diff.is_empty()
        assert not diff.is_unmount_required("primary")
        assert not diff.is_data_modification_required("primary")
        assert not diff.is_key_changed("primary")
        assert not diff.is_change_with_downtime()

    def test_add_schema_change_diff_cluster(self, table_path: str, default_schema: Types.Schema, settings: Settings):
        diff = SchemaChange.make(settings)
        table1 = YtTable.make("k", "primary", YtTable.Type.TABLE, table_path, True, {"schema": default_schema})
        table2 = YtTable.make("k", "remote0", YtTable.Type.TABLE, table_path, True, {"schema": default_schema})
        with pytest.raises(AssertionError):
            diff.add_change_if_any(table1, table2)

    def test_add_schema_change_diff_path(self, table_path: str, default_schema: Types.Schema, settings: Settings):
        diff = SchemaChange.make(settings)
        table1 = YtTable.make("k", "primary", YtTable.Type.TABLE, table_path, True, {"schema": default_schema})
        table2 = YtTable.make("k", "primary", YtTable.Type.TABLE, table_path + "_", True, {"schema": default_schema})
        with pytest.raises(AssertionError):
            diff.add_change_if_any(table1, table2)

    def test_add_schema_change_compatible_no_unmout(
        self, table_path: str, default_schema: Types.Schema, settings: Settings, dummy_logger: Logger
    ):
        diff = SchemaChange.make(settings)
        new_schema = deepcopy(default_schema)
        new_schema.append({"name": "v2", "type": "string"})
        table1 = YtTable.make("k", "primary", YtTable.Type.TABLE, table_path, True, {"schema": new_schema})
        table2 = YtTable.make(
            "k", "primary", YtTable.Type.TABLE, table_path, True, {"schema": default_schema}
        )  # add column
        diff.add_change_if_any(table1, table2)
        assert not diff.is_empty()
        assert diff.check_and_log(dummy_logger)
        assert diff.has_diff_for("primary")
        assert diff.is_unmount_required("primary")
        assert not diff.is_data_modification_required("primary")
        assert not diff.is_data_modification_required("primary")
        assert not diff.is_key_changed("primary")
        assert not diff.is_change_with_downtime()

    def test_add_schema_change_compatible_with_unmout(
        self, table_path: str, default_schema: Types.Schema, settings: Settings, dummy_logger: Logger
    ):
        diff = SchemaChange.make(settings)
        new_schema = deepcopy(default_schema)
        new_schema.append({"name": "v2", "type": "string"})
        table1 = YtTable.make("k", "primary", YtTable.Type.TABLE, table_path, True, {"schema": default_schema})
        table2 = YtTable.make(
            "k", "primary", YtTable.Type.TABLE, table_path, True, {"schema": new_schema}
        )  # delete column
        diff.add_change_if_any(table1, table2)
        assert not diff.is_empty()
        assert diff.check_and_log(dummy_logger)
        assert diff.has_diff_for("primary")
        assert diff.is_unmount_required("primary")
        assert diff.is_data_modification_required("primary")
        assert not diff.is_key_changed("primary")

        # Until https://st.yandex-team.ru/YTSYNC-25
        # assert not diff.is_change_with_downtime()
        assert diff.is_change_with_downtime()

    def test_add_schema_change_key(
        self, table_path: str, default_schema: Types.Schema, settings: Settings, dummy_logger: Logger
    ):
        diff = SchemaChange.make(settings)
        new_schema = deepcopy(default_schema)
        new_schema[0]["expression"] = "farm_hash(Key) % 20"
        table1 = YtTable.make("k", "primary", YtTable.Type.TABLE, table_path, True, {"schema": default_schema})
        table2 = YtTable.make(
            "k", "primary", YtTable.Type.TABLE, table_path, True, {"schema": new_schema}
        )  # change key
        diff.add_change_if_any(table1, table2)
        assert not diff.is_empty()
        assert diff.check_and_log(dummy_logger)
        assert diff.has_diff_for("primary")
        assert diff.is_unmount_required("primary")
        assert diff.is_data_modification_required("primary")
        assert diff.is_key_changed("primary")

        # Until https://st.yandex-team.ru/YTSYNC-25
        # assert not diff.is_change_with_downtime()
        assert diff.is_change_with_downtime()

    def test_add_schema_change_sort_order(
        self, table_path: str, default_schema: Types.Schema, settings: Settings, dummy_logger: Logger
    ):
        diff = SchemaChange.make(settings)
        new_schema = deepcopy(default_schema)
        new_schema[0]["sort_order"] = "descending"
        table1 = YtTable.make("k", "primary", YtTable.Type.TABLE, table_path, True, {"schema": default_schema})
        table2 = YtTable.make(
            "k", "primary", YtTable.Type.TABLE, table_path, True, {"schema": new_schema}
        )  # change key
        diff.add_change_if_any(table1, table2)
        assert not diff.is_empty()
        assert diff.check_and_log(dummy_logger)
        assert diff.has_diff_for("primary")
        assert diff.is_unmount_required("primary")
        assert diff.is_data_modification_required("primary")
        assert diff.is_key_changed("primary")
        assert diff.is_change_with_downtime()

    def test_add_subkey(
        self,
        table_path: str,
        default_schema: Types.Schema,
        default_schema_broad_key: Types.Schema,
        settings: Settings,
        dummy_logger: Logger,
    ):
        diff = SchemaChange.make(settings)
        table1 = YtTable.make(
            "k", "primary", YtTable.Type.TABLE, table_path, True, {"schema": default_schema_broad_key}
        )
        table2 = YtTable.make("k", "primary", YtTable.Type.TABLE, table_path, True, {"schema": default_schema})
        diff.add_change_if_any(table1, table2)
        assert not diff.is_empty()
        assert diff.check_and_log(dummy_logger)
        assert diff.has_diff_for("primary")
        assert diff.is_unmount_required("primary")
        assert not diff.is_data_modification_required("primary")
        assert diff.is_key_changed("primary")
        assert not diff.is_change_with_downtime()

    def test_add_schema_change_incompatible(
        self, table_path: str, default_schema: Types.Schema, settings: Settings, dummy_logger: Logger
    ):
        diff = SchemaChange.make(settings)
        new_schema = deepcopy(default_schema)
        new_schema[-1]["type"] = "string"
        table1 = YtTable.make("k", "primary", YtTable.Type.TABLE, table_path, True, {"schema": default_schema})
        table2 = YtTable.make("k", "primary", YtTable.Type.TABLE, table_path, True, {"schema": new_schema})
        diff.add_change_if_any(table1, table2)
        assert not diff.is_empty()
        assert not diff.check_and_log(dummy_logger)
        assert diff.has_diff_for("primary")

    @pytest.mark.parametrize("compatible", [True, False])
    def test_ordered_table_compatible_schema_change(
        self,
        table_path: str,
        ordered_schema: Types.Schema,
        settings: Settings,
        dummy_logger: Logger,
        compatible: bool,
        is_chaos: bool,
    ):
        diff = SchemaChange.make(settings)
        new_schema = deepcopy(ordered_schema)
        new_schema.append({"name": "Value2", "type": "string"})

        table1 = YtTable.make("k", "primary", YtTable.Type.TABLE, table_path, True, {"schema": ordered_schema})
        table2 = YtTable.make("k", "primary", YtTable.Type.TABLE, table_path, True, {"schema": new_schema})

        if compatible:
            # add column
            diff.add_change_if_any(table2, table1)
        else:
            # remove column
            diff.add_change_if_any(table1, table2)

        assert not diff.is_empty()
        if is_chaos:
            assert not diff.check_and_log(dummy_logger)
        else:
            assert compatible == diff.check_and_log(dummy_logger)
        assert diff.has_diff_for("primary")

    @pytest.mark.parametrize("allow_full_downtime", [True, False])
    def test_full_downtime_schema_change(
        self,
        table_path: str,
        default_schema_broad_key: Types.Attributes,
        settings: Settings,
        dummy_logger: Logger,
        allow_full_downtime: bool,
    ):
        settings.allow_table_full_downtime = allow_full_downtime
        diff = SchemaChange.make(settings)

        # remove column
        changed_schema = [c for c in default_schema_broad_key if c["name"] != "SubKey"]
        # add column
        changed_schema.append({"name": "v2", "type": "string"})

        table1 = YtTable.make(
            "k", "primary", YtTable.Type.TABLE, table_path, True, {"schema": default_schema_broad_key}
        )
        table2 = YtTable.make("k", "primary", YtTable.Type.TABLE, table_path, True, {"schema": changed_schema})

        diff.add_change_if_any(table2, table1)
        assert diff.check_and_log(dummy_logger) == allow_full_downtime

    def test_key_order_change(
        self,
        table_path: str,
        settings: Settings,
        dummy_logger: Logger,
    ):
        diff = SchemaChange.make(settings)
        schema1 = [
            {"name": "key1", "type": "int64", "sort_order": "ascending"},
            {"name": "key2", "type": "int64", "sort_order": "ascending"},
            {"name": "key3", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "int64"},
        ]
        schema2 = [
            {"name": "key1", "type": "int64", "sort_order": "ascending"},
            {"name": "key3", "type": "int64", "sort_order": "ascending"},
            {"name": "key2", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "int64"},
        ]

        table1 = YtTable.make("k", "primary", YtTable.Type.TABLE, table_path, True, {"schema": schema1})
        table2 = YtTable.make("k", "primary", YtTable.Type.TABLE, table_path, True, {"schema": schema2})
        diff.add_change_if_any(table2, table1)
        assert diff.check_and_log(dummy_logger)


class TestSchemaChangeChaos(TestSchemaChange):
    @pytest.fixture
    def is_chaos(self) -> bool:
        return True

    def test_diff_schema_change(
        self,
        settings: Settings,
        desired_db: YtDatabase,
        adjusted_actual_db: YtDatabase,
        default_schema: Types.Schema,
        new_schema: Types.Schema,
        table_path: str,
    ):
        log_path = desired_db.clusters["remote1"].tables[table_path].chaos_replication_log
        assert log_path

        diff = DbDiff.generate(settings, desired_db, adjusted_actual_db)
        table_diff = self._check_and_get(diff, table_path)
        expected_keys = [YtReplica.make_key("remote1", table_path), YtReplica.make_key("remote1", log_path)]
        self._check_table_diff(table_diff, expected_keys, default_schema, new_schema)
