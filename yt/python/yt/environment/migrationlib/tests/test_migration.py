from yt.test_helpers import assert_items_equal

from yt.environment.migrationlib import TableInfo, Conversion, Migration

from yt_commands import (authors, get, insert_rows,
                         read_table, create_tablet_cell_bundle, sync_create_cells)

from yt_env_setup import YTEnvSetup


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
