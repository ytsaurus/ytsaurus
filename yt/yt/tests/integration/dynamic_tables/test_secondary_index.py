from yt_dynamic_tables_base import DynamicTablesBase

from yt.common import wait

from yt_commands import (
    create, create_secondary_index, create_table_replica, create_table_collocation, create_user,
    authors, set, get, exists, remove, copy, get_driver, alter_table,
    sync_create_cells, sync_mount_table, sync_unmount_table, sync_enable_table_replica,
    select_rows, explain_query, insert_rows, delete_rows,
    commit_transaction, start_transaction,
    sorted_dicts, raises_yt_error,
)

from yt.test_helpers import assert_items_equal

from copy import deepcopy
import builtins
import pytest
import yt_error_codes

##################################################################

EMPTY_COLUMN_NAME = "$empty"

PRIMARY_SCHEMA = [
    {"name": "keyA", "type": "int64", "sort_order": "ascending"},
    {"name": "keyB", "type": "string", "sort_order": "ascending"},
    {"name": "valueA", "type": "int64"},
    {"name": "valueB", "type": "boolean"},
]

EXTRA_KEY_COLUMN = {"name": "keyC", "type": "int64", "sort_order": "ascending"}

PRIMARY_SCHEMA_WITH_EXTRA_KEY = [
    {"name": "keyA", "type": "int64", "sort_order": "ascending"},
    {"name": "keyB", "type": "string", "sort_order": "ascending"},
    EXTRA_KEY_COLUMN,
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
    {"name": "value", "type_v3": {
        "type_name": "optional",
        "item": {
            "type_name": "list",
            "item": {
                "type_name": "optional",
                "item": "int64"
            }
        }
    }},
]

UNFOLDING_INDEX_SCHEMA = [
    {"name": "value", "type_v3": {"type_name": "optional", "item": "int64"}, "sort_order": "ascending"},
    {"name": "key", "type": "int64", "sort_order": "ascending"},
    {"name": EMPTY_COLUMN_NAME, "type": "int64"}
]

UNIQUE_VALUE_INDEX_SCHEMA = [
    {"name": "valueB", "type": "boolean", "sort_order": "ascending"},
    {"name": "keyA", "type": "int64"},
    {"name": "keyB", "type": "string"},
]

UNIQUE_KEY_VALUE_PAIR_INDEX_SCHEMA = [
    {"name": "__hash__", "type": "uint64", "sort_order": "ascending", "expression": "farm_hash(valueA)"},
    {"name": "valueA", "type": "int64", "sort_order": "ascending"},
    {"name": "keyB", "type": "string", "sort_order": "ascending"},
    {"name": "keyA", "type": "int64"},
]

##################################################################


class TestSecondaryIndexBase(DynamicTablesBase):
    NUM_MASTERS = 3
    NUM_SECONDARY_MASTER_CELLS = 2

    def _sync_create_cells(self, cell_count=1):
        sync_create_cells(cell_count)

    def _mount(self, *tables):
        for table in tables:
            sync_mount_table(table)

    def _unmount(self, *tables):
        for table in tables:
            sync_unmount_table(table)

    def _create_table(self, table_path, table_schema):
        attributes = {
            "dynamic": True,
            "schema": table_schema,
        }
        return create("table", table_path, attributes=attributes)

    def _create_map_node(self, path):
        create("map_node", path)

    def _create_secondary_index(
        self,
        table_path="//tmp/table",
        index_table_path="//tmp/index_table",
        kind="full_sync",
        table_to_index_correspondence="bijective",
        **kwargs
    ):
        index_id = create_secondary_index(
            table_path,
            index_table_path,
            kind,
            table_to_index_correspondence,
            **kwargs)
        return index_id, None

    def _create_basic_tables(
        self,
        table_path="//tmp/table",
        table_schema=PRIMARY_SCHEMA,
        index_table_path="//tmp/index_table",
        index_schema=INDEX_ON_VALUE_SCHEMA,
        kind="full_sync",
        mount=False,
        **kwargs
    ):
        table_id = self._create_table(table_path, table_schema)
        index_table_id = self._create_table(index_table_path, index_schema)
        index_id, _ = self._create_secondary_index(table_path, index_table_path, kind, "bijective", **kwargs)

        if mount:
            self._sync_create_cells()
            self._mount(table_path, index_table_path)

        return table_id, index_table_id, index_id, None

    def _validate_index_state(self, table_path, index_table_path, **kwargs):
        attrs = get(index_table_path, attributes=["ref_counter", "id", "index_to"], **kwargs).attributes
        if "tx" not in kwargs:
            # Transactions create extra references.
            assert attrs["ref_counter"] == 1
        assert attrs["index_to"]["table_path"] == table_path
        index_table_id = attrs["id"]
        secondary_index_id = attrs["index_to"]["index_id"]

        attrs = get(table_path, attributes=["ref_counter", "id", "secondary_indices"], **kwargs).attributes
        if "tx" not in kwargs:
            # Transactions create extra references.
            assert attrs["ref_counter"] == 1
        table_id = attrs["id"]
        assert attrs["secondary_indices"][secondary_index_id] == {
            "index_path": index_table_path,
            "kind": "full_sync",
        }

        attrs = get(f"#{secondary_index_id}", attributes=[
            "ref_counter",
            "table_path",
            "table_id",
            "index_table_path",
            "index_table_id",
        ]).attributes
        assert attrs["ref_counter"] == 1
        if "tx" not in kwargs:
            # Secondary index is an unversioned master object - its proxy has no transactional awareness and
            # cannot resolve the paths correctly in this case.
            assert attrs["table_path"] == table_path
            assert attrs["index_table_path"] == index_table_path
        assert attrs["table_id"] == table_id
        assert attrs["index_table_id"] == index_table_id

        return table_id, index_table_id, secondary_index_id


##################################################################


class TestSecondaryIndexReplicatedBase(TestSecondaryIndexBase):
    NUM_REMOTE_CLUSTERS = 1
    REPLICA_CLUSTER_NAME = "remote_0"

    def setup_method(self, method):
        super(TestSecondaryIndexReplicatedBase, self).setup_method(method)
        self.REPLICA_DRIVER = get_driver(cluster=self.REPLICA_CLUSTER_NAME)

    def _sync_create_cells(self, cell_count=1):
        sync_create_cells(cell_count)
        sync_create_cells(cell_count, driver=self.REPLICA_DRIVER)

    def _mount(self, *tables):
        for table in tables:
            sync_mount_table(table)
            sync_mount_table(table + "_replica", driver=self.REPLICA_DRIVER)

    def _unmount(self, *tables):
        for table in tables:
            sync_unmount_table(table)
            sync_unmount_table(table + "_replica", driver=self.REPLICA_DRIVER)

    def _create_table(self, table_path, table_schema):
        # Force the same external cell tag due to collocations.
        attributes = {"schema": table_schema, "dynamic": True, "external_cell_tag": 11}
        table_id = create(
            "replicated_table",
            table_path,
            attributes=attributes,
        )

        replica_id = create_table_replica(
            table_path,
            self.REPLICA_CLUSTER_NAME,
            table_path + "_replica",
            attributes={"mode": "sync"},
        )

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

    def _create_map_node(self, path):
        create("map_node", path)
        create("map_node", path, driver=self.REPLICA_DRIVER)

    def _create_secondary_index(
        self,
        table_path="//tmp/table",
        index_table_path="//tmp/index_table",
        kind="full_sync",
        table_to_index_correspondence="bijective",
        **kwargs
    ):
        collocation_id = create_table_collocation(table_paths=list(builtins.set([table_path, index_table_path])))
        index_id = create_secondary_index(table_path, index_table_path, kind, table_to_index_correspondence, **kwargs)
        return index_id, collocation_id

    def _create_basic_tables(
        self,
        table_path="//tmp/table",
        table_schema=PRIMARY_SCHEMA,
        index_table_path="//tmp/index_table",
        index_schema=INDEX_ON_VALUE_SCHEMA,
        kind="full_sync",
        mount=False,
        **kwargs
    ):
        table_id = self._create_table(table_path, table_schema)
        index_table_id = self._create_table(index_table_path, index_schema)
        index_id, collocation_id = self._create_secondary_index(table_path, index_table_path, kind, **kwargs)

        if mount:
            self._sync_create_cells()
            self._mount(table_path, index_table_path)

        return table_id, index_table_id, index_id, collocation_id


##################################################################


class TestSecondaryIndexMaster(TestSecondaryIndexBase):
    @authors("sabdenovch")
    def test_forbid_create_secondary_index(self):
        set("//sys/@config/allow_everyone_create_secondary_indices", False)
        create_user("index_user")
        with raises_yt_error("Could not verify permission"):
            self._create_basic_tables()
        with raises_yt_error("Could not verify permission"):
            create_secondary_index("//tmp/table", "//tmp/index_table", "full_sync", authenticated_user="index_user")
        set("//sys/users/index_user/@allow_create_secondary_indices", True)
        create_secondary_index("//tmp/table", "//tmp/index_table", "full_sync", authenticated_user="index_user")

    @authors("sabdenovch")
    def test_ouroboros(self):
        self._create_table("//tmp/table", PRIMARY_SCHEMA)
        with raises_yt_error("Table cannot be an index to itself"):
            self._create_secondary_index("//tmp/table", "//tmp/table")

    @authors("sabdenovch")
    def test_create_and_delete_index(self):
        table_id, index_table_id, index_id, _ = self._create_basic_tables()

        assert get(f"#{index_id}/@kind") == "full_sync"
        assert get(f"#{index_id}/@table_id") == table_id
        assert get(f"#{index_id}/@table_path") == "//tmp/table"
        assert get(f"#{index_id}/@index_table_id") == index_table_id
        assert get(f"#{index_id}/@index_table_path") == "//tmp/index_table"

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

        self._create_table("//tmp/index_index_table", INDEX_ON_VALUE_SCHEMA_WITH_EXPRESSION)
        with raises_yt_error("Cannot create secondary index for a secondary index"):
            create_secondary_index("//tmp/index_table", "//tmp/index_index_table", kind="full_sync")
        with raises_yt_error("Index cannot have multiple primary tables"):
            create_secondary_index("//tmp/index_index_table", "//tmp/index_table", kind="full_sync")
        self._create_table("//tmp/super_table", PRIMARY_SCHEMA_WITH_EXPRESSION)
        with raises_yt_error("Cannot use a table with indices as an index"):
            create_secondary_index("//tmp/super_table", "//tmp/table", kind="full_sync")

        remove(f"#{index_id}")

        assert not exists("//tmp/table/@secondary_indices")
        assert not exists("//tmp/index_table/@index_to")
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

        self._sync_create_cells()
        self._mount("//tmp/table")
        with raises_yt_error("Cannot create index on a mounted table"):
            self._create_secondary_index()

    @authors("sabdenovch")
    def test_predicate_and_locks(self):
        schema = deepcopy(PRIMARY_SCHEMA + [{"name": "predicatedValue", "type": "int64", "lock": "someLock"}])
        self._create_table("//tmp/table", schema)
        self._create_table("//tmp/index", INDEX_ON_VALUE_SCHEMA)

        with raises_yt_error():
            self._create_secondary_index("//tmp/table", "//tmp/index", "full_sync", attributes={
                "predicate": "predicatedValue >= 0",
            })

        schema[2]["lock"] = "someLock"
        schema[3]["lock"] = "someLock"
        alter_table("//tmp/table", schema=schema)
        index_id = create_secondary_index("//tmp/table", "//tmp/index", "full_sync", attributes={
            "predicate": "predicatedValue >= 0",
        })

        assert get(f"#{index_id}/@predicate") == "predicatedValue >= 0"

    @authors("sabdenovch")
    @pytest.mark.parametrize("kind_and_schemas", (
        (
            "full_sync",
            PRIMARY_SCHEMA,
            PRIMARY_SCHEMA_WITH_EXTRA_KEY,
            INDEX_ON_VALUE_SCHEMA,
            INDEX_ON_VALUE_SCHEMA[:3] + [EXTRA_KEY_COLUMN, INDEX_ON_VALUE_SCHEMA[-1]],
        ),
        (
            "unfolding",
            PRIMARY_SCHEMA_WITH_LIST,
            [PRIMARY_SCHEMA_WITH_LIST[0], EXTRA_KEY_COLUMN, PRIMARY_SCHEMA_WITH_LIST[-1]],
            UNFOLDING_INDEX_SCHEMA,
            UNFOLDING_INDEX_SCHEMA[:2] + [EXTRA_KEY_COLUMN, UNFOLDING_INDEX_SCHEMA[-1]],
        ),
        (
            "unique",
            PRIMARY_SCHEMA,
            PRIMARY_SCHEMA_WITH_EXTRA_KEY,
            UNIQUE_VALUE_INDEX_SCHEMA,
            UNIQUE_VALUE_INDEX_SCHEMA + [{"name": "keyC", "type": "int64"}],
        ),
    ))
    def test_alter_extra_key_order(self, kind_and_schemas):
        self._create_basic_tables(
            table_schema=kind_and_schemas[1],
            index_schema=kind_and_schemas[3],
            kind=kind_and_schemas[0],
            attributes={"unfolded_column": "value"} if kind_and_schemas[0] == "unfolding" else {})

        with raises_yt_error():
            alter_table("//tmp/table", schema=kind_and_schemas[2])

        alter_table("//tmp/index_table", schema=kind_and_schemas[4])
        alter_table("//tmp/table", schema=kind_and_schemas[2])

    @authors("sabdenovch")
    def test_alter_extra_value_order(self):
        self._create_basic_tables()

        with raises_yt_error():
            alter_table("//tmp/table", schema=INDEX_ON_VALUE_SCHEMA + [{"name": "extraValue", "type": "int64"}])

        alter_table("//tmp/table", schema=PRIMARY_SCHEMA + [{"name": "extraValue", "type": "int64"}])
        alter_table("//tmp/index_table", schema=INDEX_ON_VALUE_SCHEMA + [{"name": "extraValue", "type": "int64"}])

    @authors("sabdenovch")
    def test_copy_with_abandonment(self):
        self._create_basic_tables()

        with raises_yt_error("Cannot copy table"):
            copy("//tmp/table", "//tmp/table_copy")
        with raises_yt_error("Cannot copy table"):
            copy("//tmp/index_table", "//tmp/index_table_copy")

        copy("//tmp/table", "//tmp/table_copy", allow_secondary_index_abandonment=True)
        assert not exists("//tmp/table_copy/@secondary_indices")
        copy("//tmp/index_table", "//tmp/index_table_copy", allow_secondary_index_abandonment=True)
        assert not exists("//tmp/index_table_copy/@index_to")

##################################################################


# This test suite is not iterated over with replicated tables, because:
# 1) Collocations beyond portals are not supported yet;
# 2) Replicated tables cannot be moved.
class TestSecondaryIndexPortal(TestSecondaryIndexBase):
    @authors("sabdenovch")
    def test_forbid_create_beyond_portal(self):
        create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 12})
        with raises_yt_error("Table and index table native cell tags differ"):
            self._create_basic_tables(index_table_path="//tmp/p/index_table")

    @authors("sabdenovch")
    def test_forbid_move_beyond_portal(self):
        self._create_basic_tables()
        create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 12})
        with raises_yt_error("Cannot cross-cell copy neither a table with a secondary index nor an index table itself"):
            copy("//tmp/table", "//tmp/p/table")
        with raises_yt_error("Cannot cross-cell copy neither a table with a secondary index nor an index table itself"):
            copy("//tmp/index_table", "//tmp/p/index_table")

    @authors("sabdenovch")
    def test_mount_info_reaches_beyond_portal(self):
        create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 12})
        self._create_basic_tables(table_path="//tmp/p/table", index_table_path="//tmp/p/index_table", mount=True)

        rows = []
        for i in range(10):
            row = {"keyA": i, "keyB": "key", "valueA": 123, "valueB": i % 2 == 0}
            rows.append(row)
            insert_rows("//tmp/p/table", [row])

        assert_items_equal(sorted_dicts(select_rows("* from [//tmp/p/index_table]")), sorted_dicts(rows))


##################################################################


class TestSecondaryIndexSelect(TestSecondaryIndexBase):
    @authors("sabdenovch")
    def test_simple(self):
        self._create_table("//tmp/table", PRIMARY_SCHEMA)
        self._create_table("//tmp/index_table", INDEX_ON_VALUE_SCHEMA)
        self._sync_create_cells()
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
        self._sync_create_cells()
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
        self._sync_create_cells()
        self._mount("//tmp/table", "//tmp/index_table")

        table_rows = [{"keyA": 0, "keyB": "alpha", "valueA": 100}]
        insert_rows("//tmp/table", table_rows)
        insert_rows("//tmp/index_table", table_rows + [{"keyA": 0, "keyB": "alpha", "valueA": 200}])

        expected = select_rows("keyA, keyB, valueA from [//tmp/table]")
        actual = select_rows("keyA, keyB, valueA from [//tmp/table] with index [//tmp/index_table]")
        assert actual == expected

        plan = explain_query("keyA, keyB, valueA from [//tmp/table] with index [//tmp/index_table] where valueA = 100")
        assert plan["query"]["constraints"] == "Constraints:\n100: <universe>"

    @authors("sabdenovch")
    def test_unfolding(self):
        self._create_basic_tables(
            table_schema=PRIMARY_SCHEMA_WITH_LIST,
            index_schema=UNFOLDING_INDEX_SCHEMA,
            kind="unfolding",
            mount=True,
            attributes={
                "unfolded_column": "value"
            },
        )

        insert_rows("//tmp/table", [
            {"key": 0, "value": [14, 13, 12]},
            {"key": 1, "value": [11, 12]},
            {"key": 2, "value": [13, 11]},
            {"key": 3, "value": [None, 12]},
            {"key": 4, "value": None},
        ])

        query = """
            key, value from [//tmp/table] with index [//tmp/index_table]
            where list_contains(value, 12)
        """
        rows = select_rows(query)
        expected = select_rows("key, value from [//tmp/table] where list_contains(value, 12)")
        assert_items_equal(sorted_dicts(rows), sorted_dicts(expected))
        assert explain_query(query)["query"]["constraints"] != "Constraints: <universe>"

        query = """
            key, first(to_any(value)) as value from [//tmp/table] with index [//tmp/index_table]
            where list_contains(value, 12) or list_contains(value, 13)
            group by key
        """
        rows = select_rows(query)
        expected = select_rows("""
            key, value from [//tmp/table]
            where list_contains(value, 12) or list_contains(value, 13)
        """)
        assert_items_equal(sorted_dicts(rows), sorted_dicts(expected))
        assert explain_query(query)["query"]["constraints"] != "Constraints: <universe>"

        query = """
            key, first(to_any(value)) as value from [//tmp/table] with index [//tmp/index_table]
            where value in (#, 11, 14)
            group by key
        """
        rows = select_rows(query)
        expected = select_rows("""
            key, value from [//tmp/table]
            where list_contains(value, 11) or list_contains(value, 14) or list_contains(value, #)
        """)
        assert_items_equal(sorted_dicts(rows), sorted_dicts(expected))
        assert explain_query(query)["query"]["constraints"] != "Constraints: <universe>"

    @authors("sabdenovch")
    @pytest.mark.parametrize("table_schema", (PRIMARY_SCHEMA, PRIMARY_SCHEMA_WITH_EXPRESSION))
    @pytest.mark.parametrize("index_table_schema", (INDEX_ON_VALUE_SCHEMA, INDEX_ON_VALUE_SCHEMA_WITH_EXPRESSION))
    def test_different_evaluated_columns(self, table_schema, index_table_schema):
        self._create_table("//tmp/table", table_schema)
        self._create_table("//tmp/index_table", index_table_schema)
        self._sync_create_cells()
        self._mount("//tmp/table", "//tmp/index_table")

        table_rows = [{"keyA": 1, "keyB": "alpha"}]
        insert_rows("//tmp/table", table_rows)
        insert_rows("//tmp/index_table", table_rows)

        assert_items_equal(
            sorted_dicts(select_rows("keyA, keyB from [//tmp/table]")),
            sorted_dicts(select_rows("keyA, keyB from [//tmp/table] with index [//tmp/index_table]")),
        )

    @authors("sabdenovch")
    def test_correspondence(self):
        self._create_table("//tmp/table", PRIMARY_SCHEMA)
        self._create_table("//tmp/index_table", INDEX_ON_VALUE_SCHEMA)
        self._sync_create_cells()
        index_id, _ = self._create_secondary_index(table_to_index_correspondence=None)

        assert get(f"#{index_id}/@table_to_index_correspondence") == "invalid"

        self._mount("//tmp/table", "//tmp/index_table")

        table_rows = [{"keyA": 0, "keyB": "alpha", "valueA": 100}]
        insert_rows("//tmp/index_table", [{"keyA": 0, "keyB": "alpha", "valueA": 200}])

        with raises_yt_error("Cannot use index"):
            select_rows("* from [//tmp/table] WITH INDEX [//tmp/index_table]")

        sync_unmount_table("//tmp/table")
        set(f"#{index_id}/@table_to_index_correspondence", "bijective")
        sync_mount_table("//tmp/table")
        insert_rows("//tmp/table", table_rows)
        assert len(select_rows("* from [//tmp/table] WITH INDEX [//tmp/index_table]")) == 2
        plan = explain_query("* from [//tmp/table] WITH INDEX [//tmp/index_table] where valueA = 100")
        assert plan["query"]["constraints"] == "Constraints:\n100: <universe>"

        sync_unmount_table("//tmp/table")
        set(f"#{index_id}/@table_to_index_correspondence", "injective")
        sync_mount_table("//tmp/table")
        assert len(select_rows("* from [//tmp/table] WITH INDEX [//tmp/index_table]")) == 1
        plan = explain_query("* from [//tmp/table] WITH INDEX [//tmp/index_table] where valueA = 100")
        assert plan["query"]["constraints"] == "Constraints:\n100: <universe>"


##################################################################


class TestSecondaryIndexModifications(TestSecondaryIndexBase):
    def _insert_rows(self, rows, table="//tmp/table", **kwargs):
        insert_rows(table, rows, **kwargs)

    def _delete_rows(self, rows, table="//tmp/table"):
        delete_rows(table, rows)

    def _expect_from_index(self, expected, index_table="//tmp/index_table"):
        actual = select_rows(f"* from [{index_table}]")
        for row in actual:
            if "__hash__" in row:
                del row["__hash__"]
        assert_items_equal(sorted_dicts(actual), sorted_dicts(expected))

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

        create_secondary_index(
            "//tmp/table",
            "//tmp/index_table_auxiliary",
            kind="full_sync",
            table_to_index_correspondence="bijective")

        self._sync_create_cells(1)
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
            mount=True,
            attributes={
                "unfolded_column": "value",
            },
        )

        self._insert_rows([
            {"key": 0, "value": [1, 1, 1]},
            {"key": 1, "value": [None]},
            {"key": 2, "value": None},
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
            attributes={
                "predicate": "not valueB",
            },
        )

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
    def test_secondary_index_forbid_shared_write_locks(self):
        self._create_basic_tables(mount=True)
        tx = start_transaction(type="tablet")
        self._insert_rows([{"keyA": 0, "keyB": "key", "valueA": 123}], update=True, tx=tx, lock_type="shared_write")
        with raises_yt_error():
            commit_transaction(tx)

    @authors("sabdenovch")
    def test_secondary_index_unique_value(self):
        self._create_basic_tables(
            table_schema=PRIMARY_SCHEMA_WITH_EXPRESSION,
            index_schema=UNIQUE_VALUE_INDEX_SCHEMA,
            kind="unique",
            mount=True,
        )

        # Conflict within write itself.
        with raises_yt_error(yt_error_codes.UniqueIndexConflict):
            self._insert_rows([
                {"keyA": 1, "keyB": "yyy", "valueB": True},
                {"keyA": 2, "keyB": "yyy", "valueB": True},
            ])
        # True, False and implicit Null - no conflict.
        self._insert_rows([
            {"keyA": 0, "keyB": "xxx", "valueA": 123, "valueB": True},
            {"keyA": 1, "keyB": "xxx", "valueA": 456, "valueB": False},
            {"keyA": 1, "keyB": "yyy", "valueA": 456},
        ], update=True)
        self._expect_from_index([
            {"valueB": None, "keyA": 1, "keyB": "yyy"},
            {"valueB": False, "keyA": 1, "keyB": "xxx"},
            {"valueB": True, "keyA": 0, "keyB": "xxx"},
        ])
        # Existing True has different key - conflict.
        with raises_yt_error(yt_error_codes.UniqueIndexConflict):
            self._insert_rows([
                {"keyA": 2, "keyB": "yyy", "valueB": True},
            ])
        # Update leaves unique column untouched.
        self._insert_rows([
            {"keyA": 0, "keyB": "xxx", "valueA": 789},
            {"keyA": 1, "keyB": "xxx", "valueA": 789},
            {"keyA": 1, "keyB": "yyy", "valueA": 789},
        ], update=True)
        # Flip keys between False and True.
        self._insert_rows([
            {"keyA": 0, "keyB": "xxx", "valueB": False},
            {"keyA": 1, "keyB": "xxx", "valueB": True},
        ])
        # Rotate keys between False, True and Null
        self._insert_rows([
            {"keyA": 0, "keyB": "xxx", "valueB": True},
            {"keyA": 1, "keyB": "xxx", "valueB": None},
            {"keyA": 1, "keyB": "yyy", "valueB": False},
        ])

    @authors("sabdenovch")
    def test_secondary_index_unique_key_value_pair(self):
        self._create_basic_tables(
            table_schema=PRIMARY_SCHEMA_WITH_EXPRESSION,
            index_schema=UNIQUE_KEY_VALUE_PAIR_INDEX_SCHEMA,
            kind="unique",
            mount=True,
        )

        # Setup.
        self._insert_rows([
            {"keyA": 0, "keyB": "xxx", "valueA": 111, "valueB": True},
            {"keyA": 0, "keyB": "yyy", "valueA": 222, "valueB": True},
            {"keyA": 1, "keyB": "xxx", "valueA": 222, "valueB": True},
            {"keyA": 1, "keyB": "yyy", "valueA": 111, "valueB": False},
        ])
        self._expect_from_index([
            {"valueA": 111, "keyB": "xxx", "keyA": 0},
            {"valueA": 111, "keyB": "yyy", "keyA": 1},
            {"valueA": 222, "keyB": "xxx", "keyA": 1},
            {"valueA": 222, "keyB": "yyy", "keyA": 0},
        ])
        # Conflict with existing row.
        with raises_yt_error(yt_error_codes.UniqueIndexConflict):
            self._insert_rows([
                {"keyA": 3, "keyB": "yyy", "valueA": 111},
            ])
        # Write that leads to no change.
        self._insert_rows([
            {"keyA": 0, "keyB": "xxx", "valueA": 111, "valueB": True},
            {"keyA": 0, "keyB": "yyy", "valueA": 222, "valueB": True},
            {"keyA": 1, "keyB": "xxx", "valueA": 222, "valueB": True},
            {"keyA": 1, "keyB": "yyy", "valueA": 111, "valueB": False},
        ])
        # Flip valueA.
        self._insert_rows([
            {"keyA": 0, "keyB": "xxx", "valueA": 222},
            {"keyA": 0, "keyB": "yyy", "valueA": 111},
            {"keyA": 1, "keyB": "xxx", "valueA": 111},
            {"keyA": 1, "keyB": "yyy", "valueA": 222},
        ])
        # Rotate valueA.
        self._insert_rows([
            {"keyA": 0, "keyB": "xxx", "valueA": 111},
            {"keyA": 0, "keyB": "yyy", "valueA": 111},
            {"keyA": 1, "keyB": "xxx", "valueA": 222},
            {"keyA": 1, "keyB": "yyy", "valueA": 222},
        ])

    @authors("sabdenovch")
    def test_secondary_index_unique_concurrent_conflict(self):
        self._create_basic_tables(
            table_schema=PRIMARY_SCHEMA,
            index_schema=UNIQUE_VALUE_INDEX_SCHEMA,
            kind="unique",
            mount=True,
        )

        tx1 = start_transaction(type="tablet")
        tx2 = start_transaction(type="tablet")
        self._insert_rows([
            {"keyA": 1, "valueB": True},
        ], tx=tx1)
        self._insert_rows([
            {"keyA": 2, "valueB": True},
        ], tx=tx2)
        commit_transaction(tx1)
        with raises_yt_error("Row lock conflict due to concurrent write"):
            commit_transaction(tx2)

    @authors("sabdenovch")
    def test_secondary_index_unique_with_predicate(self):
        self._create_basic_tables(
            table_schema=PRIMARY_SCHEMA,
            index_schema=UNIQUE_VALUE_INDEX_SCHEMA,
            kind="unique",
            mount=True,
            attributes={
                "predicate": "valueA > 100",
            },
        )

        # Both rows satisfy predicate, but there is a conflict.
        with raises_yt_error(yt_error_codes.UniqueIndexConflict):
            self._insert_rows([
                {"keyA": 1, "valueA": 200, "valueB": True},
                {"keyA": 2, "valueA": 200, "valueB": True},
            ])
        # Conflict is neutralized by predicate - second row is not indexed.
        self._insert_rows([
            {"keyA": 0, "valueA": 200, "valueB": True},
            {"keyA": 1, "valueA": 0, "valueB": True},
        ])
        # Now first row is not indexed.
        self._insert_rows([
            {"keyA": 0, "valueA": 0, "valueB": True},
            {"keyA": 1, "valueA": 200, "valueB": True},
        ])
        # Both indexed, and no conflict.
        self._insert_rows([
            {"keyA": 0, "valueA": 200, "valueB": True},
            {"keyA": 1, "valueA": 200, "valueB": False},
        ])
        # Conflict, but neither is indexed.
        self._insert_rows([
            {"keyA": 0, "valueA": 0, "valueB": False},
            {"keyA": 1, "valueA": 0, "valueB": False},
        ])


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
        self._sync_create_cells(1)
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
