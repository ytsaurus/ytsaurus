from yt_dynamic_tables_base import DynamicTablesBase

from yt.common import wait

from yt_commands import (
    authors, get, exists, remove, create_secondary_index, create_dynamic_table,
    sync_create_cells,
)

##################################################################

EMPTY_COLUMN = {"name": "$empty", "type": "int64"}

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
        table_id = create_dynamic_table(table_path, PRIMARY_SCHEMA)
        index_table_id = create_dynamic_table(index_table_path, INDEX_ON_VALUE_SCHEMA)
        index_id = create_secondary_index(
            table_path,
            index_table_path,
            kind)

        return table_id, index_table_id, index_id


class TestSecondaryIndex(TestSecondaryIndexBase):
    @authors("sabdenovch")
    def test_secondary_index_create_index(self):
        sync_create_cells(1)

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
        sync_create_cells(1)
        table_id, index_table_id, index_id = self._create_basic_tables()

        remove(f"#{index_id}")
        assert not exists("//tmp/table/@secondary_indices")
        assert not exists("//tmp/index_table/@index_to")

    @authors("sabdenovch")
    def test_secondary_index_delete_primary_table(self):
        sync_create_cells(1)
        table_id, index_table_id, index_id = self._create_basic_tables()

        remove("//tmp/table")
        assert not exists("//tmp/index_table/@index_to")
        wait(lambda: not exists(f"#{index_id}"))

    @authors("sabdenovch")
    def test_secondary_index_delete_index_table(self):
        sync_create_cells(1)
        table_id, index_table_id, index_id = self._create_basic_tables()

        remove("//tmp/index_table")
        assert not exists("//tmp/table/@secondary_indices")
        wait(lambda: not exists(f"#{index_id}"))
