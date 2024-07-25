from yt_dynamic_tables_base import DynamicTablesBase

from yt.common import wait

from yt_commands import (
    create, create_secondary_index, create_dynamic_table, create_table_replica, create_table_collocation,
    authors, set, get, exists, remove, copy, get_driver, alter_table,
    sync_create_cells, sync_mount_table, sync_unmount_table, sync_enable_table_replica,
    select_rows, insert_rows, delete_rows, commit_transaction, start_transaction,
    sorted_dicts, raises_yt_error,
)

from yt.test_helpers import assert_items_equal

from copy import deepcopy
import pytest

##################################################################

EMPTY_COLUMN_NAME = "$empty"

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

PRIMARY_SCHEMA_WITH_AGGREGATE = [
    {"name": "key", "type": "int64", "sort_order": "ascending"},
    {"name": "value", "type": "int64"},
    {"name": "aggregate_value", "type": "int64", "aggregate": "sum"},
]

PRIMARY_SCHEMA_WITH_EXPRESSION = [
    {"name": "__hash__", "type": "uint64", "sort_order": "ascending", "expression": "farm_hash(keyA)"},
    {"name": "keyA", "type": "int64", "sort_order": "ascending"},
    {"name": "keyB", "type": "string", "sort_order": "ascending"},
    {"name": "valueA", "type": "int64"},
    {"name": "valueB", "type": "boolean"},
]

INDEX_ON_VALUE_SCHEMA = [
    {"name": "valueA", "type": "int64", "sort_order": "ascending"},
    {"name": "keyA", "type": "int64", "sort_order": "ascending"},
    {"name": "keyB", "type": "string", "sort_order": "ascending"},
    {"name": "valueB", "type": "boolean"},
]

INDEX_ON_VALUE_SCHEMA_WITH_EXPRESSION = [
    {"name": "__hash__", "type": "uint64", "sort_order": "ascending", "expression": "farm_hash(valueA)"},
    {"name": "valueA", "type": "int64", "sort_order": "ascending"},
    {"name": "keyA", "type": "int64", "sort_order": "ascending"},
    {"name": "keyB", "type": "string", "sort_order": "ascending"},
    {"name": "valueB", "type": "boolean"},
]

INDEX_ON_KEY_SCHEMA = [
    {"name": "keyB", "type": "string", "sort_order": "ascending"},
    {"name": "keyA", "type": "int64", "sort_order": "ascending"},
    {"name": EMPTY_COLUMN_NAME, "type": "int64"},
]

PRIMARY_SCHEMA_WITH_LIST = [
    {"name": "key", "type": "int64", "sort_order": "ascending"},
    {"name": "value", "type_v3": {"type_name": "list", "item": {"type_name": "optional", "item": "int64"}}},
]

UNFOLDING_INDEX_SCHEMA = [
    {"name": "value", "type_v3": {"type_name": "optional", "item": "int64"}, "sort_order": "ascending"},
    {"name": "key", "type": "int64", "sort_order": "ascending"},
    {"name": EMPTY_COLUMN_NAME, "type": "int64"}
]

##################################################################


class TestSecondaryIndexBase(DynamicTablesBase):
    def _mount(self, *tables, cell_count=1):
        sync_create_cells(cell_count)
        for table in tables:
            sync_mount_table(table)

    def _unmount(self, *tables):
        for table in tables:
            sync_unmount_table(table)

    def _create_table(self, table_path, table_schema, external_cell_tag=11):
        return create_dynamic_table(table_path, table_schema, external_cell_tag=external_cell_tag)

    def _create_secondary_index(
        self,
        table_path="//tmp/table",
        index_table_path="//tmp/index_table",
        kind="full_sync",
        predicate=None,
    ):
        index_id = create_secondary_index(table_path, index_table_path, kind, predicate)
        return index_id, None

    def _create_basic_tables(
        self,
        table_path="//tmp/table",
        table_schema=PRIMARY_SCHEMA,
        index_table_path="//tmp/index_table",
        index_schema=INDEX_ON_VALUE_SCHEMA,
        kind="full_sync",
        mount=False,
        predicate=None,
    ):
        table_id = self._create_table(table_path, table_schema)
        index_table_id = self._create_table(index_table_path, index_schema)
        index_id, _ = self._create_secondary_index(table_path, index_table_path, kind, predicate)

        if mount:
            self._mount(table_path, index_table_path)

        return table_id, index_table_id, index_id, None


##################################################################


class TestSecondaryIndexReplicatedBase(TestSecondaryIndexBase):
    NUM_REMOTE_CLUSTERS = 1
    REPLICA_CLUSTER_NAME = "remote_0"

    def setup_method(self, method):
        super(TestSecondaryIndexReplicatedBase, self).setup_method(method)
        self.REPLICA_DRIVER = get_driver(cluster=self.REPLICA_CLUSTER_NAME)

    def _mount(self, *tables, cell_count=1):
        sync_create_cells(cell_count)
        sync_create_cells(cell_count, driver=self.REPLICA_DRIVER)

        for table in tables:
            sync_mount_table(table)
            sync_mount_table(table + "_replica", driver=self.REPLICA_DRIVER)

    def _unmount(self, *tables):
        for table in tables:
            sync_unmount_table(table)
            sync_unmount_table(table + "_replica", driver=self.REPLICA_DRIVER)

    def _create_table(self, table_path, table_schema, external_cell_tag=11):
        table_id = create(
            "replicated_table",
            table_path,
            attributes={
                "schema": table_schema,
                "dynamic": True,
                "external_cell_tag": external_cell_tag,
            })

        replica_id = create_table_replica(
            table_path,
            self.REPLICA_CLUSTER_NAME,
            table_path + "_replica",
            attributes={"mode": "sync"})

        create(
            "table",
            table_path + "_replica",
            driver=self.REPLICA_DRIVER,
            attributes={
                "schema": table_schema,
                "dynamic": True,
                "upstream_replica_id": replica_id,
            })

        sync_enable_table_replica(replica_id)

        return table_id

    def _create_secondary_index(
        self,
        table_path="//tmp/table",
        index_table_path="//tmp/index_table",
        kind="full_sync",
        predicate=None
    ):
        collocation_id = create_table_collocation(table_paths=[table_path, index_table_path])
        index_id = create_secondary_index(table_path, index_table_path, kind, predicate)
        return index_id, collocation_id

    def _create_basic_tables(
        self,
        table_path="//tmp/table",
        table_schema=PRIMARY_SCHEMA,
        index_table_path="//tmp/index_table",
        index_schema=INDEX_ON_VALUE_SCHEMA,
        kind="full_sync",
        mount=False,
        predicate=None,
    ):
        table_id = self._create_table(table_path, table_schema)
        index_table_id = self._create_table(index_table_path, index_schema)
        index_id, collocation_id = self._create_secondary_index(table_path, index_table_path, kind, predicate)

        if mount:
            self._mount(table_path, index_table_path)

        return table_id, index_table_id, index_id, collocation_id


##################################################################


class TestSecondaryIndexMaster(TestSecondaryIndexBase):
    @authors("sabdenovch")
    def test_forbid_create_secondary_index(self):
        set("//sys/@config/allow_everyone_create_secondary_indices", False)
        with raises_yt_error("Could not verify permission"):
            self._create_basic_tables()
        set("//sys/users/root/@allow_create_secondary_indices", True)
        create_secondary_index("//tmp/table", "//tmp/index_table", "full_sync")

    @authors("sabdenovch")
    def test_create_index(self):
        table_id, index_table_id, index_id, _ = self._create_basic_tables()

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
    def test_delete_index(self):
        _, _, index_id, _ = self._create_basic_tables()

        remove(f"#{index_id}")
        assert not exists("//tmp/table/@secondary_indices")
        assert not exists("//tmp/index_table/@index_to")
        if self.NUM_SECONDARY_MASTER_CELLS:
            wait(lambda: not exists(f"#{index_id}", driver=get_driver(1)))

    @authors("sabdenovch")
    def test_delete_primary_table(self):
        _, _, index_id, _ = self._create_basic_tables()

        remove("//tmp/table")
        assert not exists("//tmp/index_table/@index_to")
        wait(lambda: not exists(f"#{index_id}"))
        if self.NUM_SECONDARY_MASTER_CELLS:
            wait(lambda: not exists(f"#{index_id}", driver=get_driver(1)))

    @authors("sabdenovch")
    def test_delete_index_table(self):
        _, _, index_id, _ = self._create_basic_tables()

        remove("//tmp/index_table")
        assert not exists("//tmp/table/@secondary_indices")
        wait(lambda: not exists(f"#{index_id}"))
        if self.NUM_SECONDARY_MASTER_CELLS:
            wait(lambda: not exists(f"#{index_id}", driver=get_driver(1)))

    @authors("sabdenovch")
    def test_illegal_create_on_mounted(self):
        self._create_table("//tmp/table", PRIMARY_SCHEMA)
        self._create_table("//tmp/index_table", INDEX_ON_VALUE_SCHEMA)

        self._mount("//tmp/table")
        with raises_yt_error("Cannot create index on a mounted table"):
            self._create_secondary_index()

    @authors("sabdenovch")
    @pytest.mark.parametrize("schema_pair", (
        ([{"name": "not_sorted", "type": "int64"}], INDEX_ON_VALUE_SCHEMA),
        (PRIMARY_SCHEMA, [{"name": "not_sorted", "type": "int64"}]),
        (PRIMARY_SCHEMA_WITH_EXTRA_KEY, INDEX_ON_VALUE_SCHEMA),
        (PRIMARY_SCHEMA, INDEX_ON_VALUE_SCHEMA + [{"name": "valueMissingFromPrimary", "type": "string"}]),
        (PRIMARY_SCHEMA_WITH_AGGREGATE, [
            {"name": "aggregate_value", "type": "int64", "sort_order": "ascending"},
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": EMPTY_COLUMN_NAME, "type": "int64"},
        ]),
        (PRIMARY_SCHEMA_WITH_AGGREGATE, [
            {"name": "value", "type": "int64", "sort_order": "ascending"},
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "aggregate_value", "type": "int64", "aggregate": "sum"},
        ]),
        ([
            {"name": "keyA", "type": "int64", "sort_order": "ascending"},
            {"name": "keyB", "type": "string", "sort_order": "ascending"},
            {"name": "valueA", "type": "int64", "lock": "A"},
            {"name": "valueB", "type": "boolean", "lock": "B"},
        ], INDEX_ON_VALUE_SCHEMA)
    ))
    def test_illegal_schema_combinations(self, schema_pair):
        self._create_table("//tmp/table", schema_pair[0])
        self._create_table("//tmp/index", schema_pair[1])

        with raises_yt_error():
            create_secondary_index("//tmp/table", "//tmp/index", "full_sync")

    @authors("sabdenovch")
    def test_predicate_and_locks(self):
        self._create_table(
            "//tmp/table",
            PRIMARY_SCHEMA + [{"name": "predicatedValue", "type": "int64", "lock": "someLock"}])
        self._create_table("//tmp/index", INDEX_ON_VALUE_SCHEMA)

        with raises_yt_error():
            create_secondary_index("//tmp/table", "//tmp/index", "full_sync", predicate="predicatedValue >= 0")

    @authors("sabdenovch")
    def test_alter_extra_key_order(self):
        self._create_basic_tables()

        with raises_yt_error():
            alter_table("//tmp/table", schema=PRIMARY_SCHEMA_WITH_EXTRA_KEY)

        alter_table("//tmp/index_table", schema=[
            {"name": "valueA", "type": "int64", "sort_order": "ascending"},
            {"name": "keyA", "type": "int64", "sort_order": "ascending"},
            {"name": "keyB", "type": "string", "sort_order": "ascending"},
            {"name": "keyC", "type": "int64", "sort_order": "ascending"},
            {"name": "valueB", "type": "boolean"},
        ])
        alter_table("//tmp/table", schema=PRIMARY_SCHEMA_WITH_EXTRA_KEY)

    @authors("sabdenovch")
    def test_alter_extra_value_order(self):
        self._create_basic_tables()

        with raises_yt_error():
            alter_table("//tmp/table", schema=INDEX_ON_VALUE_SCHEMA + [{"name": "extraValue", "type": "int64"}])

        alter_table("//tmp/table", schema=PRIMARY_SCHEMA + [{"name": "extraValue", "type": "int64"}])
        alter_table("//tmp/index_table", schema=INDEX_ON_VALUE_SCHEMA + [{"name": "extraValue", "type": "int64"}])


##################################################################


class TestSecondaryIndexSelect(TestSecondaryIndexBase):
    @authors("sabdenovch")
    def test_simple(self):
        self._create_table("//tmp/table", PRIMARY_SCHEMA)
        self._create_table("//tmp/index_table", INDEX_ON_VALUE_SCHEMA)
        self._mount("//tmp/table", "//tmp/index_table")

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
    def test_with_alias(self):
        self._create_table("//tmp/table", PRIMARY_SCHEMA)
        self._create_table("//tmp/index_table", INDEX_ON_VALUE_SCHEMA)
        self._mount("//tmp/table", "//tmp/index_table")

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
    def test_join_on_all_shared_columns(self):
        self._create_table("//tmp/table", PRIMARY_SCHEMA)
        self._create_table("//tmp/index_table", INDEX_ON_VALUE_SCHEMA)
        self._mount("//tmp/table", "//tmp/index_table")

        table_rows = [{"keyA": 0, "keyB": "alpha", "valueA": 100}]
        insert_rows("//tmp/table", table_rows)
        insert_rows("//tmp/index_table", table_rows + [{"keyA": 0, "keyB": "alpha", "valueA": 200}])

        expected = list(select_rows("keyA, keyB, valueA from [//tmp/table]"))
        actual = list(select_rows("keyA, keyB, valueA from [//tmp/table] with index [//tmp/index_table]"))
        assert actual == expected

    @authors("sabdenovch")
    def test_unfolding(self):
        self._create_table("//tmp/table", PRIMARY_SCHEMA_WITH_LIST)
        self._create_table("//tmp/index_table", UNFOLDING_INDEX_SCHEMA)
        self._mount("//tmp/table", "//tmp/index_table")

        insert_rows("//tmp/table", [
            {"key": 0, "value": [14, 13, 12]},
            {"key": 1, "value": [11, 12]},
            {"key": 2, "value": [13, 11]},
        ])
        insert_rows("//tmp/index_table", [
            {"value": 1, "key": 1},
            {"value": 1, "key": 2},
            {"value": 2, "key": 0},
            {"value": 2, "key": 1},
            {"value": 3, "key": 0},
            {"value": 3, "key": 2},
            {"value": 4, "key": 0},
        ])

        self._unmount("//tmp/table", "//tmp/index_table")
        self._create_secondary_index("//tmp/table", "//tmp/index_table", kind="unfolding")
        self._mount("//tmp/table", "//tmp/index_table", cell_count=0)

        expected_table_rows = [
            {"key": 0, "value": [14, 13, 12]},
            {"key": 1, "value": [11, 12]},
        ]
        rows = select_rows("key, value "
                           "from [//tmp/table] with index [//tmp/index_table] "
                           "where list_contains(value, 2)")
        assert_items_equal(sorted_dicts(rows), sorted_dicts(expected_table_rows))

        expected_table_rows = [
            {"key": 0, "value": [14, 13, 12]},
            {"key": 1, "value": [11, 12]},
            {"key": 2, "value": [13, 11]},
        ]
        rows = select_rows("key, first(to_any(value)) as value "
                           "from [//tmp/table] with index [//tmp/index_table] "
                           "where list_contains(value, 2) or list_contains(value, 3) "
                           "group by key")
        assert_items_equal(sorted_dicts(rows), sorted_dicts(expected_table_rows))

        rows = select_rows("key, first(to_any(value)) as value "
                           "from [//tmp/table] with index [//tmp/index_table] "
                           "where value in (1, 4) "
                           "group by key")
        assert_items_equal(sorted_dicts(rows), sorted_dicts(expected_table_rows))

    @authors("sabdenovch")
    @pytest.mark.parametrize("table_schema", (PRIMARY_SCHEMA, PRIMARY_SCHEMA_WITH_EXPRESSION))
    @pytest.mark.parametrize("index_table_schema", (INDEX_ON_VALUE_SCHEMA, INDEX_ON_VALUE_SCHEMA_WITH_EXPRESSION))
    def test_secondary_index_different_evaluated_columns(self, table_schema, index_table_schema):
        self._create_table("//tmp/table", table_schema)
        self._create_table("//tmp/index_table", index_table_schema)
        self._mount("//tmp/table", "//tmp/index_table")

        table_rows = [{"keyA": 1, "keyB": "alpha"}]
        insert_rows("//tmp/table", table_rows)
        insert_rows("//tmp/index_table", table_rows)

        assert_items_equal(
            sorted_dicts(select_rows("keyA, keyB from [//tmp/table]")),
            sorted_dicts(select_rows("keyA, keyB from [//tmp/table] with index [//tmp/index_table]")),
        )


##################################################################


class TestSecondaryIndexModifications(TestSecondaryIndexBase):
    def _insert_rows(self, rows, table="//tmp/table", **kwargs):
        insert_rows(table, rows, **kwargs)

    def _delete_rows(self, rows, table="//tmp/table"):
        delete_rows(table, rows)

    def _expect_from_index(self, expected, index_table="//tmp/index_table"):
        actual = select_rows(f"* from [{index_table}]")
        assert_items_equal(sorted_dicts(actual), sorted_dicts(expected))

    @authors("sabdenovch")
    def test_simple(self):
        self._create_basic_tables(mount=True)

        rows = [{"keyA": 0, "keyB": "key", "valueA": 0, "valueB": False}]
        self._insert_rows(rows)

        self._expect_from_index(rows)

    @authors("sabdenovch")
    def test_utility_column(self):
        self._create_basic_tables(index_schema=INDEX_ON_KEY_SCHEMA, mount=True)

        rows = [{"keyA": 0, "keyB": "key", "valueA": 0, "valueB": False}]
        self._insert_rows(rows)

        self._expect_from_index([{"keyB": "key", "keyA": 0, EMPTY_COLUMN_NAME: None}])

    @authors("sabdenovch")
    def test_same_key_twice(self):
        self._create_basic_tables(mount=True)

        self._insert_rows([{"keyA": 0, "keyB": "key", "valueA": 0, "valueB": False}])
        update = [{"keyA": 0, "keyB": "key", "valueA": 1, "valueB": True}]
        self._insert_rows(update)

        self._expect_from_index(update)

    @authors("sabdenovch")
    def test_same_key_twice_in_one_transaction(self):
        self._create_basic_tables(mount=True)

        self._insert_rows([
            {"keyA": 0, "keyB": "key", "valueA": 0, "valueB": False},
            {"keyA": 0, "keyB": "key", "valueA": 1},
        ], update=True)

        self._expect_from_index([{"keyA": 0, "keyB": "key", "valueA": 1, "valueB": False}])

    @authors("sabdenovch")
    def test_multiple(self):
        self._create_basic_tables(mount=True)

        rows = []
        for i in range(10):
            row = {"keyA": i, "keyB": "key", "valueA": 123, "valueB": i % 2 == 0}
            rows.append(row)
            self._insert_rows([row])

        self._expect_from_index(rows)

    @authors("sabdenovch")
    def test_update_partial(self):
        self._create_basic_tables(mount=True)

        self._insert_rows([{"keyA": 0, "keyB": "keyB", "valueA": 123, "valueB": False}])
        self._insert_rows([{"keyA": 0, "keyB": "keyB", "valueB": True}], update=True)

        self._expect_from_index([{"valueA": 123, "keyA": 0, "keyB": "keyB", "valueB": True}])

    @authors("sabdenovch")
    def test_missing_index_key(self):
        self._create_basic_tables(mount=True)

        self._insert_rows([
            {"keyA": 0, "keyB": "B1", "valueB": True},
            {"keyA": 1, "keyB": "B2", "valueA": None, "valueB": True},
        ])

        self._expect_from_index([
            {"keyA": 0, "keyB": "B1", "valueA": None, "valueB": True},
            {"keyA": 1, "keyB": "B2", "valueA": None, "valueB": True},
        ])

    @authors("sabdenovch")
    def test_delete_rows(self):
        self._create_basic_tables(mount=True)

        N = 8

        def key(i):
            return {"keyA": i, "keyB": "b%02x" % (N - i - 1)}

        def row(i):
            return {"keyA": i, "keyB": "b%02x" % (N - i - 1), "valueA": (11 * i + 5) // 7, "valueB": False}

        self._insert_rows([row(i) for i in range(N)])
        self._delete_rows([key(i) for i in range(N // 4, N - N // 4)])
        remaining = list(range(N // 4)) + list(range(N - N // 4, N))

        self._expect_from_index([row(i) for i in remaining])

    @authors("sabdenovch")
    def test_update_nonkey(self):
        self._create_basic_tables(mount=True)

        N = 8

        def row(i, b):
            return {"keyA": i, "keyB": "b%02x" % (N - i - 1), "valueA": i, "valueB": b}

        self._insert_rows([row(i, False) for i in range(N)])
        self._insert_rows([row(i, True) for i in range(N // 4, N - N // 4)])

        self._expect_from_index([row(i, N // 4 <= i and i < N - N // 4) for i in range(N)])

    @authors("sabdenovch")
    def test_update_index_key(self):
        self._create_basic_tables(mount=True)

        N = 8

        def row(i, a):
            return {"keyA": i, "keyB": "b%02x" % (N - i - 1), "valueA": a, "valueB": False}

        self._insert_rows([row(i, i % 2) for i in range(N)])
        self._insert_rows([row(i, i % 3) for i in range(N // 4, N - N // 4)])

        self._expect_from_index([row(i, i % 3 if N // 4 <= i and i < N - N // 4 else i % 2) for i in range(N)])

    @authors("sabdenovch")
    def test_drop_index_key(self):
        self._create_basic_tables(mount=True)

        N = 8

        def row(i, a):
            return {"keyA": i, "keyB": "b%02x" % (N - i - 1), "valueA": a, "valueB": False}

        self._insert_rows([row(i, i % 3) for i in range(N)])
        self._insert_rows([row(i, None) for i in range(N // 4, N - N // 4)])

        self._expect_from_index([row(i, None if N // 4 <= i and i < N - N // 4 else i % 3) for i in range(N)])

    @authors("sabdenovch")
    def test_multiple_indices(self):
        self._create_basic_tables()
        self._create_table("//tmp/index_table_auxiliary", INDEX_ON_KEY_SCHEMA)
        if self.NUM_REMOTE_CLUSTERS:
            collocation_id = get("//tmp/table/@replication_collocation_id")
            set("//tmp/index_table_auxiliary/@replication_collocation_id", collocation_id)

        create_secondary_index("//tmp/table", "//tmp/index_table_auxiliary", kind="full_sync")

        self._mount("//tmp/table", "//tmp/index_table", "//tmp/index_table_auxiliary")

        N = 8

        def row(i):
            return {"keyA": i, "keyB": "b%02x" % i, "valueA": N - i - 1, "valueB": i % 2 == 0}

        def aux_row(i):
            return {"keyB": "b%02x" % i, "keyA": i, EMPTY_COLUMN_NAME: None}

        self._insert_rows([row(i) for i in range(N)])

        self._expect_from_index([row(i) for i in range(N - 1, -1, -1)])
        self._expect_from_index([aux_row(i) for i in range(N)], index_table="//tmp/index_table_auxiliary")

    @authors("sabdenovch")
    def test_unfolding_modifications(self):
        self._create_basic_tables(
            table_schema=PRIMARY_SCHEMA_WITH_LIST,
            index_schema=UNFOLDING_INDEX_SCHEMA,
            kind="unfolding",
            mount=True)

        self._insert_rows([
            {"key": 0, "value": [1, 1, 1]},
            {"key": 1, "value": [None]},
        ])
        self._expect_from_index([
            {"value": None, "key": 1, EMPTY_COLUMN_NAME: None},
            {"value": 1, "key": 0, EMPTY_COLUMN_NAME: None},
        ])

        self._insert_rows([
            {"key": 0, "value": [1, 2, 3]},
            {"key": 1, "value": [4, 5, 6]},
            {"key": 2, "value": [7, 8, 9]},
        ])
        self._expect_from_index([
            {"value": 1, "key": 0, EMPTY_COLUMN_NAME: None},
            {"value": 2, "key": 0, EMPTY_COLUMN_NAME: None},
            {"value": 3, "key": 0, EMPTY_COLUMN_NAME: None},
            {"value": 4, "key": 1, EMPTY_COLUMN_NAME: None},
            {"value": 5, "key": 1, EMPTY_COLUMN_NAME: None},
            {"value": 6, "key": 1, EMPTY_COLUMN_NAME: None},
            {"value": 7, "key": 2, EMPTY_COLUMN_NAME: None},
            {"value": 8, "key": 2, EMPTY_COLUMN_NAME: None},
            {"value": 9, "key": 2, EMPTY_COLUMN_NAME: None},
        ])

        self._delete_rows([{"key": 1}])
        self._expect_from_index([
            {"value": 1, "key": 0, EMPTY_COLUMN_NAME: None},
            {"value": 2, "key": 0, EMPTY_COLUMN_NAME: None},
            {"value": 3, "key": 0, EMPTY_COLUMN_NAME: None},
            {"value": 7, "key": 2, EMPTY_COLUMN_NAME: None},
            {"value": 8, "key": 2, EMPTY_COLUMN_NAME: None},
            {"value": 9, "key": 2, EMPTY_COLUMN_NAME: None},
        ])

        self._insert_rows([
            {"key": 2, "value": [4, 5, 6]},
            {"key": 4, "value": [7, 8, 9]},
        ])
        self._expect_from_index([
            {"value": 1, "key": 0, EMPTY_COLUMN_NAME: None},
            {"value": 2, "key": 0, EMPTY_COLUMN_NAME: None},
            {"value": 3, "key": 0, EMPTY_COLUMN_NAME: None},
            {"value": 4, "key": 2, EMPTY_COLUMN_NAME: None},
            {"value": 5, "key": 2, EMPTY_COLUMN_NAME: None},
            {"value": 6, "key": 2, EMPTY_COLUMN_NAME: None},
            {"value": 7, "key": 4, EMPTY_COLUMN_NAME: None},
            {"value": 8, "key": 4, EMPTY_COLUMN_NAME: None},
            {"value": 9, "key": 4, EMPTY_COLUMN_NAME: None},
        ])

    @authors("sabdenovch")
    @pytest.mark.parametrize("table_schema", (PRIMARY_SCHEMA, PRIMARY_SCHEMA_WITH_EXPRESSION))
    @pytest.mark.parametrize("index_table_schema", (INDEX_ON_VALUE_SCHEMA, INDEX_ON_VALUE_SCHEMA_WITH_EXPRESSION))
    def test_different_evaluated_columns(self, table_schema, index_table_schema):
        self._create_basic_tables(table_schema=table_schema, index_schema=index_table_schema, mount=True)
        index_row = {"keyA": 1, "keyB": "alpha", "valueA": 3, "valueB": True}
        if index_table_schema == INDEX_ON_VALUE_SCHEMA_WITH_EXPRESSION:
            index_row["__hash__"] = 3509085207357874260
        self._insert_rows([{"keyA": 1, "keyB": "alpha", "valueA": 3, "valueB": True}])
        self._expect_from_index([index_row])

    @authors("sabdenovch")
    def test_predicate(self):
        self._create_basic_tables(
            mount=True,
            table_schema=[
                {"name": "keyA", "type": "int64", "sort_order": "ascending"},
                {"name": "keyB", "type": "string", "sort_order": "ascending"},
                {"name": "valueA", "type": "int64", "lock": "alpha"},
                {"name": "valueB", "type": "boolean", "lock": "alpha"},
            ],
            index_schema=[
                {"name": "valueA", "type": "int64", "sort_order": "ascending"},
                {"name": "keyA", "type": "int64", "sort_order": "ascending"},
                {"name": "keyB", "type": "string", "sort_order": "ascending"},
                {"name": EMPTY_COLUMN_NAME, "type": "int64"}
            ],
            predicate="not valueB")

        self._insert_rows([
            {"keyA": 0, "keyB": "bca", "valueA": 3, "valueB": False},
            {"keyA": 1, "keyB": "aba", "valueA": 1, "valueB": False},
            {"keyA": 1, "keyB": "bac", "valueA": 3, "valueB": True},
            {"keyA": 2, "keyB": "cab", "valueA": 2, "valueB": True},
            {"keyA": 3, "keyB": "cbc", "valueA": 4, "valueB": False},
        ])
        self._expect_from_index([
            {"keyA": 0, "keyB": "bca", "valueA": 3, EMPTY_COLUMN_NAME: None},
            {"keyA": 1, "keyB": "aba", "valueA": 1, EMPTY_COLUMN_NAME: None},
            {"keyA": 3, "keyB": "cbc", "valueA": 4, EMPTY_COLUMN_NAME: None},
        ])

        self._insert_rows([
            {"keyA": 0, "keyB": "bca", "valueA": 2},
            {"keyA": 1, "keyB": "aba", "valueB": True},
            {"keyA": 3, "keyB": "cbc", "valueB": False},
        ], update=True)
        self._expect_from_index([
            {"keyA": 0, "keyB": "bca", "valueA": 2, EMPTY_COLUMN_NAME: None},
            {"keyA": 3, "keyB": "cbc", "valueA": 4, EMPTY_COLUMN_NAME: None},
        ])

    @authors("sabdenovch")
    def test_consequtive_inserts_under_transaction(self):
        self._create_basic_tables(mount=True)
        tx = start_transaction(type="tablet")
        self._insert_rows([{"keyA": 0, "keyB": "key", "valueA": 123}], tx=tx)
        self._insert_rows([{"keyA": 0, "keyB": "key", "valueA": 456}], tx=tx)
        commit_transaction(tx)
        self._expect_from_index([{"keyA": 0, "keyB": "key", "valueA": 456, "valueB": None}])

    @authors("sabdenovch")
    def test_forbid_shared_write_locks(self):
        index_schema = deepcopy(INDEX_ON_VALUE_SCHEMA[:3]) + [{"name": EMPTY_COLUMN_NAME, "type": "int64"}]
        table_schema = deepcopy(PRIMARY_SCHEMA)
        table_schema[2]["lock"] = "alpha"
        table_schema[3]["lock"] = "beta"

        self._create_basic_tables(mount=True, table_schema=table_schema, index_schema=index_schema)

        with raises_yt_error():
            tx = start_transaction(type="tablet")
            self._insert_rows([{"keyA": 0, "keyB": "key", "valueA": 123}], update=True, tx=tx, lock_type="shared_write")
            commit_transaction(tx)

        with raises_yt_error():
            tx = start_transaction(type="tablet")
            self._insert_rows([{"keyA": 0, "keyB": "key", "valueB": True}], update=True, tx=tx, lock_type="shared_write")
            commit_transaction(tx)


##################################################################


class TestSecondaryIndexMulticell(TestSecondaryIndexMaster):
    NUM_SECONDARY_MASTER_CELLS = 2

    @authors("sabdenovch")
    def test_different_cell_tags(self):
        self._create_table("//tmp/table", PRIMARY_SCHEMA, external_cell_tag=11)
        self._create_table("//tmp/index_table", INDEX_ON_VALUE_SCHEMA, external_cell_tag=12)
        with raises_yt_error("Table and index table external cell tags differ"):
            self._create_secondary_index()

    @authors("sabdenovch")
    def test_forbid_create_beyond_portal(self):
        create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 12})
        with raises_yt_error("Table and index table native cell tags differ"):
            self._create_basic_tables(
                index_table_path="//tmp/p/index_table")

    @authors("sabdenovch")
    def test_forbid_move_beyond_portal(self):
        self._create_basic_tables()
        create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 12})
        with raises_yt_error("Cannot cross-cell copy neither a table with a secondary index nor an index table itself"):
            copy("//tmp/table", "//tmp/p/table")
        with raises_yt_error("Cannot cross-cell copy neither a table with a secondary index nor an index table itself"):
            copy("//tmp/index_table", "//tmp/p/index_table")


##################################################################


class TestSecondaryIndexReplicatedMaster(TestSecondaryIndexReplicatedBase, TestSecondaryIndexMaster):
    @authors("sabdenovch")
    def test_holds_collocation(self):
        _ = self._create_table("//tmp/stranger", PRIMARY_SCHEMA)
        _ = self._create_table("//tmp/table", PRIMARY_SCHEMA)
        _ = self._create_table("//tmp/index_table", INDEX_ON_VALUE_SCHEMA)
        index_id, collocation_id = self._create_secondary_index()

        with raises_yt_error("Cannot remove table //tmp/table from collocation"):
            remove("//tmp/table/@replication_collocation_id")
        with raises_yt_error("Cannot remove table //tmp/index_table from collocation"):
            remove("//tmp/index_table/@replication_collocation_id")
        with raises_yt_error("Cannot remove collocation"):
            remove(f"#{collocation_id}")

        remove("//tmp/stranger")
        remove("//tmp/index_table")

        assert exists(f"#{collocation_id}")
        assert not exists(f"#{index_id}")

        remove("//tmp/table")


##################################################################


class TestSecondaryIndexReplicatedSelect(TestSecondaryIndexReplicatedBase, TestSecondaryIndexSelect):
    @authors("sabdenovch")
    def test_picks_sync_replicas(self):
        table_path = "//tmp/table"
        index_table_path = "//tmp/index_table"

        self._create_table(table_path, PRIMARY_SCHEMA)
        self._create_table(index_table_path, INDEX_ON_VALUE_SCHEMA)
        set(
            "//tmp/table/@replicated_table_options",
            {
                "min_sync_replica_count": 2,
                "max_sync_replica_count": 2,
            },
        )
        self._mount(table_path, index_table_path)

        replica_id = create_table_replica(
            table_path,
            self.REPLICA_CLUSTER_NAME,
            table_path + "_replica_2",
            attributes={"mode": "sync"}
        )

        create(
            "table",
            table_path + "_replica_2",
            driver=self.REPLICA_DRIVER,
            attributes={
                "schema": PRIMARY_SCHEMA,
                "dynamic": True,
                "upstream_replica_id": replica_id,
            },
        )

        sync_enable_table_replica(replica_id)
        sync_mount_table(table_path + "_replica_2", driver=self.REPLICA_DRIVER)

        insert_rows(table_path, [
            {"keyA": i, "keyB": f"key{i}", "valueA": i, "valueB": i % 2 == 0} for i in range(10)
        ])

        replica_id = create_table_replica(
            index_table_path,
            self.REPLICA_CLUSTER_NAME,
            index_table_path + "_replica_2",
            attributes={"mode": "async"}
        )

        create(
            "table",
            index_table_path + "_replica_2",
            driver=self.REPLICA_DRIVER,
            attributes={
                "schema": INDEX_ON_VALUE_SCHEMA,
                "dynamic": True,
                "upstream_replica_id": replica_id,
            },
        )

        sync_enable_table_replica(replica_id)
        sync_mount_table(index_table_path + "_replica_2", driver=self.REPLICA_DRIVER)

        insert_rows(index_table_path, [
            {"keyA": i, "keyB": f"key{i}", "valueA": i, "valueB": i % 2 == 0} for i in range(10)
        ])

        assert_items_equal(
            sorted_dicts(select_rows(f"keyA, keyB, valueA, valueB FROM [{table_path}] WITH INDEX [{index_table_path}] where valueA < 5")),
            sorted_dicts([{"keyA": i, "keyB": f"key{i}", "valueA": i, "valueB": i % 2 == 0} for i in range(5)])
        )


##################################################################


class TestSecondaryIndexReplicatedModifications(TestSecondaryIndexReplicatedBase, TestSecondaryIndexModifications):
    pass


##################################################################


class TestSecondaryIndexModificationsOverRpc(TestSecondaryIndexModifications):
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True
