from yt_dynamic_tables_base import DynamicTablesBase

from yt.common import wait

from yt_commands import (
    authors, get, exists, remove, create_secondary_index, create_dynamic_table,
    sync_create_cells, sync_mount_table, mount_table, get_driver,
    select_rows, insert_rows,
    sorted_dicts, raises_yt_error,
)

##################################################################

EMPTY_COLUMN_NAME = "$empty"

EMPTY_COLUMN = {"name": EMPTY_COLUMN_NAME, "type": "int64"}

PRIMARY_SCHEMA = [
    {"name": "keyA", "type": "int64", "sort_order": "ascending"},
    {"name": "keyB", "type": "string", "sort_order": "ascending"},
    {"name": "keyC", "type": "boolean", "sort_order": "ascending"},
    {"name": "valueA", "type": "string"},
    {"name": "valueB", "type": "int64"},
    {"name": "valueC", "type": "uint64"},
]

INDEX_ON_VALUE_SCHEMA = [
    {"name": "valueA", "type": "string", "sort_order": "ascending"},
    {"name": "keyA", "type": "int64", "sort_order": "ascending"},
    {"name": "keyB", "type": "string", "sort_order": "ascending"},
    {"name": "keyC", "type": "boolean", "sort_order": "ascending"},
    EMPTY_COLUMN
]

##################################################################


class TestSecondaryIndexBase(DynamicTablesBase):
    def _create_basic_tables(self, table_path="//tmp/table", index_table_path="//tmp/index_table", kind="full_sync"):
        table_id = create_dynamic_table(table_path, PRIMARY_SCHEMA, external_cell_tag=11)
        index_table_id = create_dynamic_table(index_table_path, INDEX_ON_VALUE_SCHEMA, external_cell_tag=11)
        index_id = create_secondary_index(
            table_path,
            index_table_path,
            kind)

        return table_id, index_table_id, index_id


class TestSecondaryIndex(TestSecondaryIndexBase):
    @authors("sabdenovch")
    def test_secondary_index_create_index(self):
        table_id, index_table_id, index_id = self._create_basic_tables()

        assert get("#{}/@kind".format(index_id)) == "full_sync"
        assert get("#{}/@table_id".format(index_id)) == table_id
        assert get("#{}/@table_path".format(index_id)) == "//tmp/table"
        assert get("#{}/@index_table_id".format(index_id)) == index_table_id
        assert get("#{}/@index_table_path".format(index_id)) == "//tmp/index_table"

        assert get("//tmp/table/@secondary_indices") == {
            index_id: {
                "index_path": "//tmp/index_table",
                "kind": "full_sync",
            }
        }

        assert get("//tmp/index_table/@index_to") == {
            "index_id": index_id,
            "table_path": "//tmp/table",
            "kind": "full_sync",
        }

    @authors("sabdenovch")
    def test_secondary_index_delete_index(self):
        _, _, index_id = self._create_basic_tables()

        remove(f"#{index_id}")
        assert not exists("//tmp/table/@secondary_indices")
        assert not exists("//tmp/index_table/@index_to")
        if self.NUM_SECONDARY_MASTER_CELLS:
            wait(lambda: not exists(f"#{index_id}", driver=get_driver(1)))

    @authors("sabdenovch")
    def test_secondary_index_delete_primary_table(self):
        _, _, index_id = self._create_basic_tables()

        remove("//tmp/table")
        assert not exists("//tmp/index_table/@index_to")
        wait(lambda: not exists(f"#{index_id}"))
        if self.NUM_SECONDARY_MASTER_CELLS:
            wait(lambda: not exists(f"#{index_id}", driver=get_driver(1)))

    @authors("sabdenovch")
    def test_secondary_index_delete_index_table(self):
        _, _, index_id = self._create_basic_tables()

        remove("//tmp/index_table")
        assert not exists("//tmp/table/@secondary_indices")
        wait(lambda: not exists(f"#{index_id}"))
        if self.NUM_SECONDARY_MASTER_CELLS:
            wait(lambda: not exists(f"#{index_id}", driver=get_driver(1)))

    @authors("sabdenovch")
    def test_secondary_index_select_simple(self):
        sync_create_cells(1)
        self._create_basic_tables()
        sync_mount_table("//tmp/table")
        sync_mount_table("//tmp/index_table")

        table_rows = [
            {"keyA": 0, "keyB": "alpha", "keyC": False, "valueA": "one", "valueB": 100, "valueC": 150},
            {"keyA": 1, "keyB": "beta", "keyC": True, "valueA": "two", "valueB": 100, "valueC": 240},
            {"keyA": 1, "keyB": "gamma", "keyC": False, "valueA": "one", "valueB": 100, "valueC": 110},
        ]
        index_table_rows = [
            {"valueA": "one", "keyA": 0, "keyB": "alpha", "keyC": False, EMPTY_COLUMN_NAME: 0},
            {"valueA": "two", "keyA": 1, "keyB": "beta", "keyC": True, EMPTY_COLUMN_NAME: 0},
            {"valueA": "one", "keyA": 1, "keyB": "gamma", "keyC": False, EMPTY_COLUMN_NAME: 0},
        ]
        insert_rows("//tmp/table", table_rows)
        insert_rows("//tmp/index_table", index_table_rows)

        rows = select_rows("keyA, keyB, keyC, valueA, valueB, valueC from [//tmp/table] with index [//tmp/index_table]")
        assert sorted_dicts(rows) == sorted_dicts(table_rows)

        filtered = [{"valueB": 100, "valueC": 240}]
        rows = select_rows("valueB, valueC from [//tmp/table] with index [//tmp/index_table] where valueA = \"two\"")
        assert rows == filtered

    @authors("sabdenovch")
    def test_secondary_index_select_with_alias(self):
        sync_create_cells(1)
        self._create_basic_tables()
        sync_mount_table("//tmp/table")
        sync_mount_table("//tmp/index_table")

        table_rows = [{"keyA": 0, "keyB": "alpha", "keyC": False, "valueA": "one", "valueB": 100, "valueC": 150}]
        index_table_rows = [{"valueA": "one", "keyA": 0, "keyB": "alpha", "keyC": False, EMPTY_COLUMN_NAME: 0}]
        insert_rows("//tmp/table", table_rows)
        insert_rows("//tmp/index_table", index_table_rows)

        aliased_table_rows = [{
            "Alias.keyA": 0,
            "Alias.keyB": "alpha",
            "Alias.keyC": False,
            "Alias.valueA": "one",
            "Alias.valueB": 100,
            "Alias.valueC": 150,
        }]
        rows = select_rows("Alias.keyA, Alias.keyB, Alias.keyC, Alias.valueA, Alias.valueB, Alias.valueC "
                           "from [//tmp/table] Alias with index [//tmp/index_table]")
        assert sorted_dicts(rows) == sorted_dicts(aliased_table_rows)

    @authors("sabdenovch")
    def test_secondary_index_illegal_create_on_mounted(self):
        sync_create_cells(1)
        create_dynamic_table("//tmp/table", PRIMARY_SCHEMA, external_cell_tag=11)
        create_dynamic_table("//tmp/index_table", INDEX_ON_VALUE_SCHEMA, external_cell_tag=11)

        mount_table("//tmp/table")
        with raises_yt_error("Cannot create index on a mounted table"):
            create_secondary_index("//tmp/table", "//tmp/index_table", "full_sync")

##################################################################


class TestSecondaryIndexMulticell(TestSecondaryIndex):
    NUM_SECONDARY_MASTER_CELLS = 2

    @authors("sabdenovch")
    def test_secondary_index_different_cell_tags(self):
        create_dynamic_table("//tmp/table", PRIMARY_SCHEMA, external_cell_tag=11)
        create_dynamic_table("//tmp/index_table", INDEX_ON_VALUE_SCHEMA, external_cell_tag=12)
        with raises_yt_error("Table and index table external cell tags differ"):
            create_secondary_index("//tmp/table", "//tmp/index_table")
