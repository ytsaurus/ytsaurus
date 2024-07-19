from yt.test_helpers import assert_items_equal

from yt.environment.migrationlib import TableInfo, Conversion, Migration

from yt.wrapper.ypath import ypath_split
from yt_commands import (authors, get, insert_rows, ls, print_debug, set,
                         read_table, create_tablet_cell_bundle, sync_create_cells)

from yt_env_setup import YTEnvSetup

import pytest


def check_table_schema(table, table_info):
    real_schema = get(table + "/@schema")
    for i, column in enumerate(real_schema):
        for key in ['type_v3', 'required']:
            del column[key]
    assert_items_equal(real_schema, table_info.schema)


def check_table_rows(table, DATA):
    assert_items_equal(read_table(table), DATA)


class TestMigration(YTEnvSetup):
    USE_DYNAMIC_TABLES = True
    NUM_SCHEDULERS = 1

    INITIAL_TABLE_INFOS = {
        "test_table": TableInfo(
            [("id", "int64")],
            [("field", "string")],
            attributes={"tablet_cell_bundle": "sys"},
        ),
    }
    INITIAL_VERSION = 0
    TRANSFORMS = {
        1: [
            Conversion(
                "test_table",
                table_info=TableInfo(
                    [("id", "int64"), ("key_field", "int64", "1")],
                    [("field", "string")],
                ),
                use_default_mapper=True,
            )
        ],
        2: [
            Conversion(
                "test_table2",
                table_info=TableInfo(
                    [("id", "int64")],
                    [("other_field", "int64")],
                ),
                source="test_table",
                use_default_mapper=True,
            )
        ],
    }
    DATA = {
        0 : [{"id": 1, "field": "a"}, {"id": 2, "field": "b"}],
        1 : [{"id": 1, "key_field": 1, "field": "a"}, {"id": 2, "key_field": 1, "field": "b"}],
        2 : [{"id": 1, "other_field" : None}, {"id": 2, "other_field": None}],
    }
    MIGRATION = Migration(
        initial_table_infos=INITIAL_TABLE_INFOS,
        initial_version=INITIAL_VERSION,
        transforms=TRANSFORMS,
    )

    @authors("mpereskokova")
    def test_migration(self):
        create_tablet_cell_bundle("sys")
        sync_create_cells(1, tablet_cell_bundle="sys")

        client = self.Env.create_client()

        self.MIGRATION.create_tables(
            client=client,
            target_version=self.INITIAL_VERSION,
            tables_path="//tmp/test_migration_path",
            shard_count=1,
            override_tablet_cell_bundle=None,
        )
        table = "//tmp/test_migration_path/test_table"
        check_table_schema(table, self.INITIAL_TABLE_INFOS["test_table"])
        insert_rows(table, self.DATA[0])

        self.MIGRATION.run(
            client=client,
            tables_path="//tmp/test_migration_path",
            target_version=1,
            shard_count=1,
            force=True,
        )
        check_table_schema(table, self.TRANSFORMS[1][0].table_info)
        check_table_rows(table, self.DATA[1])

        self.MIGRATION.run(
            client=client,
            tables_path="//tmp/test_migration_path",
            target_version=self.MIGRATION.get_latest_version(),
            shard_count=1,
            force=True,
        )
        table = "//tmp/test_migration_path/test_table2"
        check_table_schema(table, self.TRANSFORMS[2][0].table_info)
        check_table_rows(table, self.DATA[2])

    @authors("apachee")
    def test_get_schemas(self):
        def _check_schema(schema_mapping, table, table_info):
            assert schema_mapping == {table: table_info.schema}

        _check_schema(self.MIGRATION.get_schemas(self.INITIAL_VERSION), "test_table", self.INITIAL_TABLE_INFOS["test_table"])
        _check_schema(self.MIGRATION.get_schemas(1), "test_table", self.TRANSFORMS[1][0].table_info)
        _check_schema(self.MIGRATION.get_schemas(2), "test_table2", self.TRANSFORMS[2][0].table_info)
        _check_schema(self.MIGRATION.get_schemas(), "test_table2", self.TRANSFORMS[2][0].table_info)


class TestConversionFilterCallback(YTEnvSetup):
    USE_DYNAMIC_TABLES = True
    NUM_SCHEDULERS = 1

    @authors("apachee")
    @pytest.mark.parametrize("ignored_tables", [
        [],
        ["test_table"],
        ["test_table2"],
        ["test_table", "test_table2"],
    ])
    def test_filter_callback(self, ignored_tables):
        create_tablet_cell_bundle("sys")
        sync_create_cells(1, tablet_cell_bundle="sys")

        def _filter_callback(client, table_path):
            table_path, table_name = ypath_split(table_path)
            ignored_tables = client.get("{}/@_ignored_tables".format(table_path))
            return table_name not in ignored_tables

        tables_path = "//tmp/test_filter_callback"

        INITIAL_TABLE_INFOS = {
            "test_table": TableInfo(
                [("id", "int64")],
                [("field", "string")],
                attributes={"tablet_cell_bundle": "sys"},
            ),
            "test_table2": TableInfo(
                [("id", "int64")],
                [("field2", "string")],
                attributes={"tablet_cell_bundle": "sys"},
            )
        }
        INITIAL_VERSION = 0
        TRANSFORMS = {
            1: [
                Conversion(
                    "test_table",
                    table_info=TableInfo(
                        [("id", "int64"), ("key_field", "int64", "1")],
                        [("field", "string")],
                    ),
                    use_default_mapper=True,
                    filter_callback=_filter_callback,
                )
            ],
            2: [
                Conversion(
                    "test_table2",
                    table_info=TableInfo(
                        [("id", "int64"), ("key_field2", "int64", "1")],
                        [("field2", "string")],
                    ),
                    use_default_mapper=True,
                    filter_callback=_filter_callback
                )
            ],
        }

        MIGRATION = Migration(
            initial_table_infos=INITIAL_TABLE_INFOS,
            initial_version=INITIAL_VERSION,
            transforms=TRANSFORMS,
        )

        client = self.Env.create_client()

        MIGRATION.create_tables(
            client=client,
            target_version=INITIAL_VERSION,
            tables_path=tables_path,
            shard_count=1,
            override_tablet_cell_bundle=None,
        )
        set("{}/@_ignored_tables".format(tables_path), ignored_tables)

        print_debug("tables: ", ls(tables_path))

        for table, table_info in INITIAL_TABLE_INFOS.items():
            check_table_schema("{}/{}".format(tables_path, table), table_info)

        MIGRATION.run(
            client=client,
            tables_path=tables_path,
            target_version=2,
            shard_count=1,
            force=False,
        )

        if "test_table" in ignored_tables:
            check_table_schema("{}/test_table".format(tables_path), INITIAL_TABLE_INFOS["test_table"])
        else:
            check_table_schema("{}/test_table".format(tables_path), TRANSFORMS[1][0].table_info)

        if "test_table2" in ignored_tables:
            check_table_schema("{}/test_table2".format(tables_path), INITIAL_TABLE_INFOS["test_table2"])
        else:
            check_table_schema("{}/test_table2".format(tables_path), TRANSFORMS[2][0].table_info)
