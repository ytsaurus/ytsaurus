from yt_dynamic_tables_base import DynamicTablesBase

from yt.common import wait

from yt_commands import (
    authors, get, exists, remove, create_secondary_index, create_dynamic_table,
    sync_create_cells, sync_mount_table, mount_table, get_driver,
    select_rows, insert_rows, delete_rows,
    sorted_dicts, raises_yt_error,
)

from yt.test_helpers import assert_items_equal

##################################################################

EMPTY_COLUMN_NAME = "$empty"

EMPTY_COLUMN = {"name": EMPTY_COLUMN_NAME, "type": "int64"}

PRIMARY_SCHEMA = [
    {"name": "keyA", "type": "int64", "sort_order": "ascending"},
    {"name": "keyB", "type": "string", "sort_order": "ascending"},
    {"name": "valueA", "type": "int64"},
    {"name": "valueB", "type": "boolean"},
]

PRIMARY_SCHEMA_WITH_EXTRA_KEY = [
    {"name": "keyA", "type": "int64", "sort_order": "ascending"},
    {"name": "keyB", "type": "string", "sort_order": "ascending"},
    {"name": "keyC", "type": "int64", "sort_order": "ascending"},
    {"name": "valueA", "type": "int64"},
    {"name": "valueB", "type": "boolean"},
]

INDEX_ON_VALUE_SCHEMA = [
    {"name": "valueA", "type": "int64", "sort_order": "ascending"},
    {"name": "keyA", "type": "int64", "sort_order": "ascending"},
    {"name": "keyB", "type": "string", "sort_order": "ascending"},
    {"name": "valueB", "type": "boolean"},
]

INDEX_ON_KEY_SCHEMA = [
    {"name": "keyB", "type": "string", "sort_order": "ascending"},
    {"name": "keyA", "type": "int64", "sort_order": "ascending"},
    EMPTY_COLUMN,
]

##################################################################


class TestSecondaryIndexBase(DynamicTablesBase):
    def _create_basic_tables(
        self,
        table_path="//tmp/table",
        index_table_path="//tmp/index_table",
        index_schema=INDEX_ON_VALUE_SCHEMA,
        kind="full_sync",
        mount=False
    ):
        table_id = create_dynamic_table(table_path, PRIMARY_SCHEMA, external_cell_tag=11)
        index_table_id = create_dynamic_table(index_table_path, index_schema, external_cell_tag=11)
        index_id = create_secondary_index(table_path, index_table_path, kind)

        if mount:
            sync_create_cells(1)
            sync_mount_table(table_path)
            sync_mount_table(index_table_path)

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
        self._create_basic_tables(mount=True)

        table_rows = [
            {"keyA": 0, "keyB": "alpha", "valueA": 100, "valueB": True},
            {"keyA": 1, "keyB": "alpha", "valueA": 200, "valueB": True},
            {"keyA": 1, "keyB": "beta", "valueA": 100, "valueB": True},
        ]
        insert_rows("//tmp/table", table_rows)
        insert_rows("//tmp/index_table", table_rows)

        rows = select_rows("keyA, keyB, valueA, valueB from [//tmp/table] with index [//tmp/index_table]")
        assert_items_equal(sorted_dicts(rows), sorted_dicts(table_rows))

        filtered = [{"keyA": 1, "keyB": "alpha", "valueA": 200, "valueB": True}]
        rows = select_rows("keyA, keyB, valueA, valueB from [//tmp/table] "
                           "with index [//tmp/index_table] where valueA = 200")
        assert_items_equal(rows, filtered)

    @authors("sabdenovch")
    def test_secondary_index_select_with_alias(self):
        self._create_basic_tables(mount=True)

        table_rows = [{"keyA": 0, "keyB": "alpha", "valueA": 100, "valueB": False}]
        insert_rows("//tmp/table", table_rows)
        insert_rows("//tmp/index_table", table_rows)

        aliased_table_rows = [{
            "Alias.keyA": 0,
            "Alias.keyB": "alpha",
            "Alias.valueA": 100,
            "Alias.valueB": False,
        }]
        rows = select_rows("Alias.keyA, Alias.keyB, Alias.valueA, Alias.valueB "
                           "from [//tmp/table] Alias with index [//tmp/index_table]")
        assert_items_equal(sorted_dicts(rows), sorted_dicts(aliased_table_rows))

    @authors("sabdenovch")
    def test_secondary_index_illegal_create_on_mounted(self):
        sync_create_cells(1)
        create_dynamic_table("//tmp/table", PRIMARY_SCHEMA, external_cell_tag=11)
        create_dynamic_table("//tmp/index_table", INDEX_ON_VALUE_SCHEMA, external_cell_tag=11)

        mount_table("//tmp/table")
        with raises_yt_error("Cannot create index on a mounted table"):
            create_secondary_index("//tmp/table", "//tmp/index_table", "full_sync")

    @authors("sabdenovch")
    def test_secondary_index_insert_simple(self):
        self._create_basic_tables(mount=True)

        rows = [{"keyA": 0, "keyB": "key", "valueA": 0, "valueB": False}]
        insert_rows("//tmp/table", rows)
        assert_items_equal(select_rows("* from [//tmp/table]"), rows)
        assert_items_equal(select_rows("* from [//tmp/index_table]"), rows)

    @authors("sabdenovch")
    def test_secondary_index_utility_column(self):
        self._create_basic_tables(index_schema=INDEX_ON_KEY_SCHEMA, mount=True)

        rows = [{"keyA": 0, "keyB": "key", "valueA": 0, "valueB": False}]
        expected_index_rows = [{"keyB": "key", "keyA": 0, EMPTY_COLUMN_NAME: None}]
        insert_rows("//tmp/table", rows)
        assert_items_equal(select_rows("* from [//tmp/table]"), rows)
        assert_items_equal(select_rows("* from [//tmp/index_table]"), expected_index_rows)

    @authors("sabdenovch")
    def test_secondary_index_insert_same_key_twice(self):
        self._create_basic_tables(mount=True)

        insert_rows("//tmp/table", [{"keyA": 0, "keyB": "key", "valueA": 0, "valueB": False}])
        update = [{"keyA": 0, "keyB": "key", "valueA": 1, "valueB": True}]
        insert_rows("//tmp/table", update)
        assert_items_equal(select_rows("* from [//tmp/index_table]"), update)

    @authors("sabdenovch")
    def test_secondary_index_insert_multiple(self):
        self._create_basic_tables(mount=True)

        rows = []
        for i in range(10):
            row = {"keyA": i, "keyB": "key", "valueA": 123, "valueB": i % 2 == 0}
            rows.append(row)
            insert_rows("//tmp/table", [row])

        assert_items_equal(select_rows("* from [//tmp/index_table]"), rows)

    @authors("sabdenovch")
    def test_secondary_index_update_partial(self):
        self._create_basic_tables(mount=True)

        insert_rows("//tmp/table", [{"keyA": 0, "keyB": "keyB", "valueA": 123, "valueB": False}])
        insert_rows("//tmp/table", [{"keyA": 0, "keyB": "keyB", "valueB": True}], update=True)

        index_rows = select_rows("* from [//tmp/index_table]")
        assert_items_equal(index_rows,  [{"valueA": 123, "keyA": 0, "keyB": "keyB", "valueB": True}])

    @authors("orlovorlov", "sabdenovch")
    def test_secondary_index_insert_missing_index_key(self):
        self._create_basic_tables(mount=True)

        insert_rows("//tmp/table", [
            {"keyA": 0, "keyB": "B1", "valueB": True},
            {"keyA": 1, "keyB": "B2", "valueA": None, "valueB": True},
        ])

        expected = [
            {"keyA": 0, "keyB": "B1", "valueA": None, "valueB": True},
            {"keyA": 1, "keyB": "B2", "valueA": None, "valueB": True},
        ]
        assert_items_equal(select_rows("* from [//tmp/index_table]"), expected)

    @authors("orlovorlov", "sabdenovch")
    def test_secondary_index_delete_rows(self):
        self._create_basic_tables(mount=True)

        N = 8

        def key(i):
            return {"keyA": i, "keyB": "b%02x" % (N - i - 1)}

        def row(i):
            return {"keyA": i, "keyB": "b%02x" % (N - i - 1), "valueA": (11 * i + 5) // 7, "valueB": False}

        insert_rows("//tmp/table", [row(i) for i in range(N)])
        delete_rows("//tmp/table", [key(i) for i in range(N // 4, N - N // 4)])
        remaining = list(range(N // 4)) + list(range(N - N // 4, N))

        table_rows = select_rows("* from [//tmp/table]")
        assert_items_equal(table_rows, [row(i) for i in remaining])

        index_table_rows = select_rows("* from [//tmp/index_table]")
        assert_items_equal(sorted_dicts(index_table_rows), sorted_dicts([row(i) for i in remaining]))

    @authors("orlovorlov", "sabdenovch")
    def test_secondary_index_update_nonkey(self):
        self._create_basic_tables(mount=True)

        N = 8

        def row(i, b):
            return {"keyA": i, "keyB": "b%02x" % (N - i - 1), "valueA": i, "valueB": b}
        insert_rows("//tmp/table", [row(i, False) for i in range(N)])
        insert_rows("//tmp/table", [row(i, True) for i in range(N // 4, N - N // 4)])

        index_table_rows = select_rows("* from [//tmp/index_table]")
        assert_items_equal(index_table_rows, [row(i, N // 4 <= i and i < N - N // 4) for i in range(N)])

    @authors("orlovorlov", "sabdenovch")
    def test_secondary_index_insert_update_index_key(self):
        self._create_basic_tables(mount=True)

        N = 8

        def row(i, a):
            return {"keyA": i, "keyB": "b%02x" % (N - i - 1), "valueA": a, "valueB": False}
        insert_rows("//tmp/table", [row(i, i % 2) for i in range(N)])
        insert_rows("//tmp/table", [row(i, i % 3) for i in range(N // 4, N - N // 4)])

        index_table_rows = select_rows("* from [//tmp/index_table]")
        expected = [row(i, i % 3 if N // 4 <= i and i < N - N // 4 else i % 2) for i in range(N)]
        assert_items_equal(sorted_dicts(index_table_rows), sorted_dicts(expected))

    @authors("orlovorlov", "sabdenovch")
    def test_secondary_index_insert_drop_index_key(self):
        self._create_basic_tables(mount=True)

        N = 8

        def row(i, a):
            return {"keyA": i, "keyB": "b%02x" % (N - i - 1), "valueA": a, "valueB": False}
        insert_rows("//tmp/table", [row(i, i % 3) for i in range(N)])
        insert_rows("//tmp/table", [row(i, None) for i in range(N // 4, N - N // 4)])

        index_table_rows = select_rows("* from [//tmp/index_table]")
        expected = [row(i, None if N // 4 <= i and i < N - N // 4 else i % 3) for i in range(N)]
        assert_items_equal(sorted_dicts(index_table_rows), sorted_dicts(expected))

    @authors("orlovorlov", "sabdenovch")
    def test_secondary_index_multiple_indices(self):
        sync_create_cells(1)
        self._create_basic_tables()
        create_dynamic_table("//tmp/index_table_auxiliary", INDEX_ON_KEY_SCHEMA, external_cell_tag=11)
        create_secondary_index("//tmp/table", "//tmp/index_table_auxiliary", "full_sync")
        sync_mount_table("//tmp/table")
        sync_mount_table("//tmp/index_table")
        sync_mount_table("//tmp/index_table_auxiliary")

        N = 8

        def row(i):
            return {"keyA": i, "keyB": "b%02x" % i, "valueA": N - i - 1, "valueB": i % 2 == 0}

        def aux_row(i):
            return {"keyB": "b%02x" % i, "keyA": i, EMPTY_COLUMN_NAME: None}

        insert_rows("//tmp/table", [row(i) for i in range(N)])

        assert_items_equal(select_rows("* from [//tmp/index_table]"), [row(i) for i in range(N - 1, -1, -1)])
        assert_items_equal(select_rows("* from [//tmp/index_table_auxiliary]"), [aux_row(i) for i in range(N)])

##################################################################


class TestSecondaryIndexMulticell(TestSecondaryIndex):
    NUM_SECONDARY_MASTER_CELLS = 2

    @authors("sabdenovch")
    def test_secondary_index_different_cell_tags(self):
        create_dynamic_table("//tmp/table", PRIMARY_SCHEMA, external_cell_tag=11)
        create_dynamic_table("//tmp/index_table", INDEX_ON_VALUE_SCHEMA, external_cell_tag=12)
        with raises_yt_error("Table and index table external cell tags differ"):
            create_secondary_index("//tmp/table", "//tmp/index_table")
