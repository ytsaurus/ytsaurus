from yt_dynamic_tables_base import DynamicTablesBase

from yt.common import wait

from yt_commands import (
    create, create_secondary_index, create_table_replica, create_table_collocation, create_user,
    authors, make_ace, set, get, exists, remove, copy, get_driver, alter_table,
    sync_create_cells, sync_mount_table, sync_unmount_table, sync_enable_table_replica,
    select_rows, explain_query, insert_rows, delete_rows,
    commit_transaction, start_transaction,
    sorted_dicts, raises_yt_error,
)

from yt.test_helpers import assert_items_equal

import yt.yson as yson

from copy import deepcopy
import builtins
import itertools
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

INDEX_ON_VALUE_SCHEMA = [
    {"name": "valueA", "type": "int64", "sort_order": "ascending"},
    {"name": "keyA", "type": "int64", "sort_order": "ascending"},
    {"name": "keyB", "type": "string", "sort_order": "ascending"},
    {"name": "valueB", "type": "boolean"},
]

INDEX_ON_KEY_SCHEMA = [
    {"name": "keyB", "type": "string", "sort_order": "ascending"},
    {"name": "keyA", "type": "int64", "sort_order": "ascending"},
    {"name": "valueA", "type": "int64"},
    {"name": "valueB", "type": "boolean"},
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


def prepend_hash(schema):
    hash_column = {"name": "__hash__", "sort_order": "ascending", "type": "uint64"}
    hash_column["expression"] = f"farm_hash(`{schema[0]["name"]}`)"

    return [hash_column] + schema

##################################################################


@pytest.mark.enabled_multidaemon
class TestSecondaryIndexBase(DynamicTablesBase):
    ENABLE_MULTIDAEMON = True
    NUM_MASTERS = 3
    NUM_SECONDARY_MASTER_CELLS = 2

    MASTER_CELL_DESCRIPTORS = {
        "11": {"roles": ["chunk_host"]},
        "12": {"roles": ["chunk_host"]},
    }

    def setup_method(self, method):
        super(TestSecondaryIndexBase, self).setup_method(method)
        self.collocation_id = None

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

    def _get_index_path(self, table_path="//tmp/table", index_name="secondary"):
        return f"//tmp/{index_name}"

    def _create_secondary_index(
        self,
        table_path="//tmp/table",
        index_table_path="//tmp/secondary",
        kind="full_sync",
        **kwargs
    ):
        if self.NUM_REMOTE_CLUSTERS:
            if self.collocation_id is None:
                self.collocation_id = create_table_collocation(table_paths=list(builtins.set([table_path, index_table_path])))

            set(table_path + "/@replication_collocaton_id", self.collocation_id)
            set(index_table_path + "/@replication_collocaton_id", self.collocation_id)

        if "table_to_index_correspondence" not in kwargs:
            kwargs["table_to_index_correspondence"] = "bijective"
        if kwargs["table_to_index_correspondence"] is None:
            del kwargs["table_to_index_correspondence"]

        index_id = create_secondary_index(
            table_path,
            index_table_path,
            kind,
            attributes=kwargs)

        return index_id, self.collocation_id

    def _create_basic_tables(
        self,
        table_path="//tmp/table",
        table_schema=PRIMARY_SCHEMA,
        index_name="secondary",
        index_schema=INDEX_ON_VALUE_SCHEMA,
        kind="full_sync",
        mount=False,
        **kwargs
    ):
        index_table_path = self._get_index_path(table_path, index_name)
        table_id = self._create_table(table_path, table_schema)
        index_table_id = self._create_table(index_table_path, index_schema)
        index_id, _ = self._create_secondary_index(table_path, index_table_path, kind, **kwargs)

        if mount:
            self._sync_create_cells()
            self._mount(table_path, index_table_path)

        return table_id, index_table_id, index_id, None

    def _add_index(
        self,
        index_name="secondary",
        table_path="//tmp/table",
        schema=INDEX_ON_VALUE_SCHEMA,
        kind="full_sync",
        table_to_index_correspondence="bijective",
        mount=True,
        **kwargs
    ):
        index_path = self._get_index_path(table_path, index_name)
        self._create_table(index_path, schema)
        if self.NUM_REMOTE_CLUSTERS:
            collocation_id = get(table_path + "/@replication_collocation_id")
            set(index_path + "/@replication_collocation_id", collocation_id)

        create_secondary_index(
            table_path,
            index_path,
            kind=kind,
            table_to_index_correspondence=table_to_index_correspondence,
            attributes=kwargs)

        if mount:
            self._mount(index_path)


##################################################################


@pytest.mark.enabled_multidaemon
class TestSecondaryIndexReplicatedBase(TestSecondaryIndexBase):
    ENABLE_MULTIDAEMON = True
    NUM_REMOTE_CLUSTERS = 1
    REPLICA_CLUSTER_NAME = "remote_0"
    NUM_SECONDARY_MASTER_CELLS_REMOTE_0 = 2
    MASTER_CELL_DESCRIPTORS_REMOTE_0 = {
        "20": {"roles": ["transaction_coordinator"]},
        "21": {"roles": ["chunk_host", "cypress_node_host"]},
        "22": {"roles": ["chunk_host", "cypress_node_host"]},
    }

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

    def _create_basic_tables(
        self,
        table_path="//tmp/table",
        table_schema=PRIMARY_SCHEMA,
        index_name="secondary",
        index_schema=INDEX_ON_VALUE_SCHEMA,
        kind="full_sync",
        mount=False,
        **kwargs
    ):
        index_table_path = self._get_index_path(table_path, index_name)
        table_id = self._create_table(table_path, table_schema)
        index_table_id = self._create_table(index_table_path, index_schema)
        index_id, collocation_id = self._create_secondary_index(table_path, index_table_path, kind, **kwargs)

        if mount:
            self._sync_create_cells()
            self._mount(table_path, index_table_path)

        return table_id, index_table_id, index_id, collocation_id


##################################################################


@pytest.mark.enabled_multidaemon
class TestSecondaryIndexMaster(TestSecondaryIndexBase):
    ENABLE_MULTIDAEMON = True

    @authors("sabdenovch")
    def test_forbid_create_secondary_index(self):
        create_user("index_user")
        self._create_table("//tmp/table", PRIMARY_SCHEMA)
        self._create_table("//tmp/secondary", INDEX_ON_VALUE_SCHEMA)
        if self.NUM_REMOTE_CLUSTERS:
            create_table_collocation(table_paths=["//tmp/table", "//tmp/secondary"])
        with raises_yt_error("Access denied"):
            create_secondary_index("//tmp/table", "//tmp/secondary", "full_sync", authenticated_user="index_user")
        set("//sys/schemas/secondary_index/@acl", [make_ace("allow", "index_user", ["create", "remove", "read"])])
        id = create_secondary_index("//tmp/table", "//tmp/secondary", "full_sync", authenticated_user="index_user")
        get(f"#{id}/@", authenticated_user="index_user")
        remove(f"#{id}", authenticated_user="index_user")

    @authors("sabdenovch")
    def test_user_can_write_attributes(self):
        create_user("index_user")
        self._create_table("//tmp/table", PRIMARY_SCHEMA)
        self._create_table("//tmp/secondary", INDEX_ON_VALUE_SCHEMA)
        set("//sys/schemas/secondary_index/@acl", [make_ace("allow", "index_user", ["create", "remove", "read"])])
        if self.NUM_REMOTE_CLUSTERS:
            create_table_collocation(table_paths=["//tmp/table", "//tmp/secondary"])
        id = create_secondary_index("//tmp/table", "//tmp/secondary", "full_sync", authenticated_user="index_user")
        with raises_yt_error(yt_error_codes.AuthorizationErrorCode):
            set(f"#{id}/@table_to_index_correspondence", "bijective", authenticated_user="index_user")
        set("//tmp/table/@acl", [make_ace("allow", "index_user", ["write"])])
        set(f"#{id}/@table_to_index_correspondence", "bijective", authenticated_user="index_user")

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
        assert get(f"#{index_id}/@index_table_path") == "//tmp/secondary"

        assert get("//tmp/table/@secondary_indices") == {
            index_id: {
                "index_path": "//tmp/secondary",
                "kind": "full_sync",
                "table_to_index_correspondence": "bijective"
            }
        }

        assert get("//tmp/secondary/@index_to") == {
            "index_id": index_id,
            "table_path": "//tmp/table",
            "kind": "full_sync",
            "table_to_index_correspondence": "bijective",
        }

        self._create_table("//tmp/secondary_2", prepend_hash(INDEX_ON_VALUE_SCHEMA))
        with raises_yt_error("Cannot create secondary index for a secondary index"):
            create_secondary_index("//tmp/secondary", "//tmp/secondary_2", kind="full_sync")
        with raises_yt_error("Index cannot have multiple primary tables"):
            create_secondary_index("//tmp/secondary_2", "//tmp/secondary", kind="full_sync")
        self._create_table("//tmp/super_table", prepend_hash(PRIMARY_SCHEMA))
        with raises_yt_error("Cannot use a table with indices as an index"):
            create_secondary_index("//tmp/super_table", "//tmp/table", kind="full_sync")

        remove(f"#{index_id}")

        assert not exists("//tmp/table/@secondary_indices")
        assert not exists("//tmp/secondary/@index_to")
        wait(lambda: not exists(f"#{index_id}", driver=get_driver(1)))

    @authors("sabdenovch")
    def test_delete_primary_table(self):
        _, _, index_id, _ = self._create_basic_tables()

        remove("//tmp/table")
        assert not exists("//tmp/secondary/@index_to")
        wait(lambda: not exists(f"#{index_id}"))
        if self.NUM_SECONDARY_MASTER_CELLS:
            wait(lambda: not exists(f"#{index_id}", driver=get_driver(1)))

    @authors("sabdenovch")
    def test_delete_index_table(self):
        _, _, index_id, _ = self._create_basic_tables()

        remove("//tmp/secondary")
        assert not exists("//tmp/table/@secondary_indices")
        wait(lambda: not exists(f"#{index_id}"))
        if self.NUM_SECONDARY_MASTER_CELLS:
            wait(lambda: not exists(f"#{index_id}", driver=get_driver(1)))

    @authors("sabdenovch")
    def test_illegal_create_on_mounted(self):
        self._create_table("//tmp/table", PRIMARY_SCHEMA)
        self._create_table("//tmp/secondary", INDEX_ON_VALUE_SCHEMA)

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
            self._create_secondary_index("//tmp/table", "//tmp/index", "full_sync", predicate="predicatedValue >= 0")

        schema[2]["lock"] = "someLock"
        schema[3]["lock"] = "someLock"
        alter_table("//tmp/table", schema=schema)
        index_id, _ = self._create_secondary_index("//tmp/table", "//tmp/index", "full_sync", predicate="predicatedValue >= 0")

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
            **({"unfolded_index_column": "value", "unfolded_table_column": "value"} if kind_and_schemas[0] == "unfolding" else {}))

        with raises_yt_error():
            alter_table("//tmp/table", schema=kind_and_schemas[2])

        alter_table("//tmp/secondary", schema=kind_and_schemas[4])
        alter_table("//tmp/table", schema=kind_and_schemas[2])

    @authors("sabdenovch")
    def test_alter_extra_value_order(self):
        self._create_basic_tables()

        with raises_yt_error():
            alter_table("//tmp/table", schema=INDEX_ON_VALUE_SCHEMA + [{"name": "extraValue", "type": "int64"}])

        alter_table("//tmp/table", schema=PRIMARY_SCHEMA + [{"name": "extraValue", "type": "int64"}])
        alter_table("//tmp/secondary", schema=INDEX_ON_VALUE_SCHEMA + [{"name": "extraValue", "type": "int64"}])

    @authors("sabdenovch")
    def test_copy_with_abandonment(self):
        self._create_basic_tables()

        with raises_yt_error("Cannot copy table"):
            copy("//tmp/table", "//tmp/table_copy")
        with raises_yt_error("Cannot copy table"):
            copy("//tmp/secondary", "//tmp/secondary_copy")

        copy("//tmp/table", "//tmp/table_copy", allow_secondary_index_abandonment=True)
        assert not exists("//tmp/table_copy/@secondary_indices")
        copy("//tmp/secondary", "//tmp/secondary_copy", allow_secondary_index_abandonment=True)
        assert not exists("//tmp/secondary_copy/@index_to")

    @authors("sabdenovch")
    def test_evaluated(self):
        index_schema = [
            {"name": "eva01", "type": "int64", "sort_order": "ascending"},
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": EMPTY_COLUMN_NAME, "type": "int64"},
        ]
        table_schema = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "any"},
        ]

        self._create_table("//tmp/secondary", index_schema)
        self._create_table("//tmp/table", table_schema)

        evaluated_columns_schema = [
            {"name": "eva01", "type": "int64", "expression": "try_get_int64(value, \"/inner_field\")"}
        ]
        secondary_index_id, _ = self._create_secondary_index(evaluated_columns_schema=evaluated_columns_schema)

        assert get(f"#{secondary_index_id}/@evaluated_columns_schema")[0]["expression"] \
            == evaluated_columns_schema[0]["expression"]
        assert get("//tmp/table/@secondary_indices")[secondary_index_id]["evaluated_columns_schema"][0]["expression"] \
            == evaluated_columns_schema[0]["expression"]

        with raises_yt_error("Columns collision"):
            self._add_index("secondary_2", schema=index_schema, evaluated_columns_schema=[{
                "name": "eva01",
                "type": "int64", "expression":
                "try_get_int64(value, \"/inner_field_other\")",
            }])

        self._add_index(
            "secondary_3",
            schema=index_schema,
            evaluated_columns_schema=[
                {"name": "eva01", "type": "int64", "expression": "try_get_int64(value, \"/inner_field\")"}
            ],
            mount=False,
        )


##################################################################


# This test suite is not iterated over with replicated tables, because:
# 1) Collocations beyond portals are not supported yet;
# 2) Replicated tables cannot be moved.
@pytest.mark.enabled_multidaemon
class TestSecondaryIndexPortal(TestSecondaryIndexBase):
    ENABLE_MULTIDAEMON = True

    MASTER_CELL_DESCRIPTORS = {
        "11": {"roles": ["chunk_host", "cypress_node_host"]},
        "12": {"roles": ["chunk_host", "cypress_node_host"]},
    }

    def _create_basic_tables(
        self,
        table_path="//tmp/table",
        table_schema=PRIMARY_SCHEMA,
        index_name="secondary",
        index_schema=INDEX_ON_VALUE_SCHEMA,
        kind="full_sync",
        mount=False,
        **kwargs
    ):
        index_table_path = self._get_index_path(table_path, index_name)
        table_id = self._create_table(table_path, table_schema)
        index_table_id = self._create_table(index_table_path, index_schema)
        wait(lambda: exists(f"#{table_id}"))
        wait(lambda: exists(f"#{index_table_id}"))
        index_id, _ = self._create_secondary_index(table_path, index_table_path, kind, **kwargs)

        if mount:
            self._sync_create_cells()
            self._mount(table_path, index_table_path)

        return table_id, index_table_id, index_id, None

    @authors("sabdenovch")
    def test_forbid_create_beyond_portal(self):
        create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 12})
        with raises_yt_error("Table and index table native cell tags differ"):
            self._create_basic_tables(index_name="p/secondary")

    @authors("sabdenovch")
    def test_forbid_move_beyond_portal(self):
        self._create_basic_tables()
        create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 12})
        with raises_yt_error("Cannot cross-cell copy neither a table with a secondary index nor an index table itself"):
            copy("//tmp/table", "//tmp/p/table")
        with raises_yt_error("Cannot cross-cell copy neither a table with a secondary index nor an index table itself"):
            copy("//tmp/secondary", "//tmp/p/secondary")

    @authors("sabdenovch")
    def test_mount_info_reaches_beyond_portal(self):
        create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 12})
        self._create_basic_tables(table_path="//tmp/p/table", index_name="p/index_table", mount=True)

        rows = []
        for i in range(10):
            row = {"keyA": i, "keyB": "key", "valueA": 123, "valueB": i % 2 == 0}
            rows.append(row)
            insert_rows("//tmp/p/table", [row])

        assert_items_equal(sorted_dicts(select_rows("* from [//tmp/p/index_table]")), sorted_dicts(rows))


##################################################################


@pytest.mark.enabled_multidaemon
class TestSecondaryIndexSelect(TestSecondaryIndexBase):
    ENABLE_MULTIDAEMON = True

    @authors("sabdenovch")
    def test_with_alias(self):
        self._create_basic_tables(mount=True)

        table_rows = [{"keyA": 0, "keyB": "alpha", "valueA": 100, "valueB": False}]
        insert_rows("//tmp/table", table_rows)

        aliased_table_rows = [{
            "Alias.keyA": 0,
            "Alias.keyB": "alpha",
            "Alias.valueA": 100,
            "Alias.valueB": False,
        }]
        rows = select_rows(f"""Alias.keyA, Alias.keyB, Alias.valueA, Alias.valueB
            from [//tmp/table] Alias with index [{self._get_index_path()}] I""")
        assert_items_equal(sorted_dicts(rows), sorted_dicts(aliased_table_rows))

    @authors("sabdenovch")
    def test_join_on_all_shared_columns(self):
        _, _, index_id, _ = self._create_basic_tables()
        index_table_path = self._get_index_path()
        remove(f"#{index_id}")
        self._sync_create_cells()
        self._mount("//tmp/table", self._get_index_path())

        table_rows = [{"keyA": 0, "keyB": "alpha", "valueA": 100}]
        insert_rows("//tmp/table", table_rows)
        insert_rows(index_table_path, table_rows + [{"keyA": 0, "keyB": "alpha", "valueA": 200}])

        expected = select_rows("keyA, keyB, valueA from [//tmp/table]")
        actual = select_rows(f"keyA, keyB, valueA from [//tmp/table] with index [{index_table_path}] I")
        assert actual == expected

        plan = explain_query(f"keyA, keyB, valueA from [//tmp/table] with index [{index_table_path}] I where valueA = 100")
        assert plan["query"]["constraints"] == "Constraints:\n100: <universe>"

    @pytest.mark.parametrize("strong_typing", [False, True])
    @authors("sabdenovch")
    def test_unfolding(self, strong_typing):
        table_schema = PRIMARY_SCHEMA_WITH_LIST if strong_typing else [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "any"},
        ]
        _, _, secondary_index_id, _ = self._create_basic_tables(
            table_schema=table_schema,
            index_schema=UNFOLDING_INDEX_SCHEMA,
            kind="unfolding",
            mount=True,
            unfolded_table_column="value",
            unfolded_index_column="value",
        )

        index_table_path = self._get_index_path()

        assert get("//tmp/table/@secondary_indices")[secondary_index_id]["unfolded_columns"] == {
            "table_column": "value",
            "index_column": "value",
        }

        # Avoid "twin nulls" problem.
        format = yson.YsonString(b"yson")
        format.attributes["enable_null_to_yson_entity_conversion"] = False

        insert_rows("//tmp/table", [
            {"key": 0, "value": [14, 13, 12]},
            {"key": 1, "value": [11, 12]},
            {"key": 2, "value": [13, 11]},
            {"key": 3, "value": [None, 12]},
            {"key": 4, "value": None},
        ], input_format=format)

        query = f"""
            key, value from [//tmp/table] with index [{index_table_path}] I
            where list_contains(value, 12)
        """
        rows = select_rows(query)
        expected = select_rows("key, value from [//tmp/table] where list_contains(value, 12)")
        assert_items_equal(sorted_dicts(rows), sorted_dicts(expected))
        assert explain_query(query)["query"]["constraints"] != "Constraints: <universe>"

        query = f"""
            key, first(to_any(value)) as value from [//tmp/table] with index [{index_table_path}] I
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

        query = f"""
            key, first(to_any(value)) as value from [//tmp/table] with index [{index_table_path}] I
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
    @pytest.mark.parametrize("table_schema", (PRIMARY_SCHEMA, prepend_hash(PRIMARY_SCHEMA)))
    @pytest.mark.parametrize("index_schema", (INDEX_ON_VALUE_SCHEMA, prepend_hash(INDEX_ON_VALUE_SCHEMA)))
    def test_different_evaluated_columns(self, table_schema, index_schema):
        self._create_basic_tables(table_schema=table_schema, index_schema=index_schema, mount=True)

        table_rows = [{"keyA": 1, "keyB": "alpha"}]
        insert_rows("//tmp/table", table_rows)

        assert_items_equal(
            sorted_dicts(select_rows("keyA, keyB from [//tmp/table]")),
            sorted_dicts(select_rows(f"keyA, keyB from [//tmp/table] with index [{self._get_index_path()}] I")),
        )

    @authors("sabdenovch")
    def test_correspondence(self):
        _, _, index_id, _ = self._create_basic_tables(table_to_index_correspondence=None, mount=True)
        index_table_path = self._get_index_path()

        assert get(f"#{index_id}/@table_to_index_correspondence") == "invalid"

        table_rows = [{"keyA": 0, "keyB": "alpha", "valueA": 100}]
        insert_rows(index_table_path, [{"keyA": 0, "keyB": "alpha", "valueA": 200}])

        with raises_yt_error("Cannot use index"):
            select_rows(f"* from [//tmp/table] WITH INDEX [{index_table_path}] I")

        sync_unmount_table("//tmp/table")
        set(f"#{index_id}/@table_to_index_correspondence", "bijective")
        sync_mount_table("//tmp/table")
        insert_rows("//tmp/table", table_rows)
        assert len(select_rows(f"* from [//tmp/table] WITH INDEX [{index_table_path}] I")) == 2
        plan = explain_query(f"* from [//tmp/table] WITH INDEX [{index_table_path}] I where valueA = 100")
        assert plan["query"]["constraints"] == "Constraints:\n100: <universe>"

        sync_unmount_table("//tmp/table")
        set(f"#{index_id}/@table_to_index_correspondence", "injective")
        sync_mount_table("//tmp/table")
        assert len(select_rows(f"* from [//tmp/table] WITH INDEX [{index_table_path}] I")) == 1
        plan = explain_query(f"* from [//tmp/table] WITH INDEX [{index_table_path}] I where valueA = 100")
        assert plan["query"]["constraints"] == "Constraints:\n100: <universe>"

    @authors("sabdenovch")
    def test_unfolding_text(self):
        table_schema = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "text", "type": "string"},
            {"name": "tokens", "type_v3": {
                "type_name": "list",
                "item": {
                    "type_name": "optional",
                    "item": "string",
                },
            }},
        ]
        index_schema = [
            {"name": "token", "type": "string", "sort_order": "ascending"},
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "count", "type": "int64", "aggregate": "sum"}
        ]
        self._create_basic_tables(
            table_schema=table_schema,
            index_schema=index_schema,
            kind="unfolding",
            mount=True,
            unfolded_index_column="token",
            unfolded_table_column="tokens",
            evaluated_columns_schema=[{"name": "count", "type": "int64", "expression": "1"}]
        )

        def make_row(key, text: str):
            return {"key": key, "text": text, "tokens": text.split(" ")}

        insert_rows("//tmp/table", [
            make_row(1, "Lorem ipsum dolor sit amet, consectetur adipiscing elit"),
            make_row(2, "Phasellus in felis cursus, pellentesque nisi quis, luctus velit"),
            make_row(3, "Nullam sed orci sit amet orci malesuada consequat eu ac lacus"),
            make_row(4, "Nunc ante magna, porttitor eu pharetra a, accumsan id tellus"),
            make_row(5, "Nam nec accumsan augue, eu hendrerit ex"),
            make_row(6, "Phasellus ultrices varius mi, vitae feugiat enim placerat in"),
            make_row(7, "Quisque egestas egestas dui, non mattis urna dapibus eget"),
            make_row(8, "Vivamus eros dolor, maximus vel blandit non, ultricies sit amet est"),
            make_row(9, " ".join(itertools.repeat("АБЫР", 15))),
        ])

        index_table_path = self._get_index_path()

        print("FDDAFDS", select_rows("* from [//tmp/table]"))
        print("DFAFDSF", select_rows(f"* from [{index_table_path}]"))

        # lines with token eu
        query = f"""
            key from [//tmp/table] with index [{index_table_path}] I
            where I.token in ("eu") group by key
        """
        rows = select_rows(query)
        assert builtins.set([3, 4, 5]) == builtins.set([row["key"] for row in rows])
        assert explain_query(query)["query"]["constraints"] != "Constraints: <universe>"

        # lines with tokens starting with ma
        query = f"""
            key from [//tmp/table] with index [{index_table_path}] AS Unfolded
            where is_prefix("ma", Unfolded.token) group by key
        """
        rows = select_rows(query)
        assert builtins.set([3, 4, 7, 8]) == builtins.set([row["key"] for row in rows])
        assert explain_query(query)["query"]["constraints"] != "Constraints: <universe>"

        query = f"[count] as c from [{index_table_path}] where key = 9 and token = 'АБЫР'"
        assert select_rows(query)[0]["c"] == 15

        insert_rows("//tmp/table", [
            make_row(9, " ".join(itertools.repeat("РЫБА", 3))),
        ])

        query = f"[count] as c from [{index_table_path}] where key = 9"
        rows = select_rows(query)
        assert len(rows) == 1
        assert rows[0]["c"] == 3


##################################################################


@pytest.mark.enabled_multidaemon
class TestSecondaryIndexModifications(TestSecondaryIndexBase):
    ENABLE_MULTIDAEMON = True
    NUM_TEST_PARTITIONS = 2

    def _insert_rows(self, rows, table="//tmp/table", **kwargs):
        insert_rows(table, rows, **kwargs)

    def _delete_rows(self, rows, table="//tmp/table"):
        delete_rows(table, rows)

    def _expect_from_index(self, expected, index_name="secondary", table_path="//tmp/table"):
        actual = select_rows(f"* from [{self._get_index_path(table_path, index_name)}]")
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
    def test_leaks(self):
        table_schema = [
            {"name": "key", "type": "string", "sort_order": "ascending"},
            {"name": "value", "type": "string"},
        ]
        index_table_schema = [
            {"name": "value", "type": "string", "sort_order": "ascending"},
            {"name": "key", "type": "string", "sort_order": "ascending"},
            {"name": "$empty", "type": "int64"},
        ]
        self._create_basic_tables(
            table_schema=table_schema,
            index_schema=index_table_schema,
            mount=True)

        for shift in [0, 1, 2, 2, 2, -1, 5]:
            rows = [{"key": f"{i}", "value": f"{i + shift}"} for i in range(100)]
            self._insert_rows(rows)

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
    def test_lookup_skip(self):
        self._create_basic_tables(
            mount=True,
            table_path="//tmp/lookupless",
            index_schema=INDEX_ON_KEY_SCHEMA + [{"name": "eva01", "type": "int64"}],
            evaluated_columns_schema=[{
                "name": "eva01",
                "type": "int64",
                "expression": "-keyA",
            }],
        )

        N = 8

        assert self._get_table_profiling("//tmp/lookupless").get_counter("lookup/row_count") == 0

        def row(i, a):
            return {"keyA": i, "keyB": f"b{(N - i) % 3}", "valueA": a, "valueB": None}

        def index_row(i, a):
            return {"keyA": i, "keyB": f"b{(N - i) % 3}", "valueA": a, "valueB": None, "eva01": -i}

        self._insert_rows([row(i, 11) for i in range(N)], table="//tmp/lookupless")
        self._expect_from_index([index_row(i, 11) for i in range(N)])
        self._insert_rows([row(i, 22) for i in range(N // 2, N + N // 2)], table="//tmp/lookupless")
        self._expect_from_index([index_row(i, 11) for i in range(N // 2)] + [index_row(i, 22) for i in range(N // 2, N + N // 2)])

        assert self._get_table_profiling("//tmp/lookupless").get_counter("lookup/row_count") == 0

    @authors("sabdenovch")
    def test_multiple_indices(self):
        self._sync_create_cells(1)
        self._create_basic_tables()
        self._add_index(index_name="auxiliary", schema=INDEX_ON_KEY_SCHEMA, mount=False)
        self._mount("//tmp/table", self._get_index_path(), self._get_index_path(index_name="auxiliary"))

        N = 8

        def row(i):
            return {"keyA": i, "keyB": "b%02x" % i, "valueA": N - i - 1, "valueB": i % 2 == 0}

        self._insert_rows([row(i) for i in range(N)])

        self._expect_from_index([row(i) for i in range(N - 1, -1, -1)])
        self._expect_from_index([row(i) for i in range(N)], index_name="auxiliary")

    @pytest.mark.parametrize("strong_typing", [False, True])
    @authors("sabdenovch")
    def test_unfolding_modifications(self, strong_typing):
        table_schema = PRIMARY_SCHEMA_WITH_LIST if strong_typing else [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "any"},
        ]
        self._create_basic_tables(
            table_schema=table_schema,
            index_schema=UNFOLDING_INDEX_SCHEMA,
            kind="unfolding",
            mount=True,
            unfolded_index_column="value",
            unfolded_table_column="value",
        )

        # Avoid "twin nulls" problem.
        format = yson.YsonString(b"yson")
        format.attributes["enable_null_to_yson_entity_conversion"] = False

        self._insert_rows([
            {"key": 0, "value": [1, 1, 1]},
            {"key": 1, "value": [None]},
            {"key": 2, "value": None},
        ], input_format=format)
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
    @pytest.mark.parametrize("schemas", (
        (PRIMARY_SCHEMA, prepend_hash(INDEX_ON_VALUE_SCHEMA)),
        (prepend_hash(PRIMARY_SCHEMA), INDEX_ON_VALUE_SCHEMA),
        (prepend_hash(PRIMARY_SCHEMA), prepend_hash(INDEX_ON_VALUE_SCHEMA)),
    ))
    def test_different_evaluated_columns(self, schemas):
        table_schema, index_table_schema = schemas
        self._create_basic_tables(table_schema=table_schema, index_schema=index_table_schema, mount=True)
        index_row = {"keyA": 1, "keyB": "alpha", "valueA": 3, "valueB": True}
        self._insert_rows([{"keyA": 1, "keyB": "alpha", "valueA": 3, "valueB": True}])
        self._expect_from_index([index_row])

    @authors("sabdenovch")
    @pytest.mark.parametrize("schemas", (
        (PRIMARY_SCHEMA_WITH_LIST, prepend_hash(UNFOLDING_INDEX_SCHEMA)),
        (prepend_hash(PRIMARY_SCHEMA_WITH_LIST), UNFOLDING_INDEX_SCHEMA),
        (prepend_hash(PRIMARY_SCHEMA_WITH_LIST), prepend_hash(UNFOLDING_INDEX_SCHEMA)),
    ))
    def test_different_evaluated_columns_unfolding(self, schemas):
        table_schema, index_table_schema = schemas
        self._create_basic_tables(
            table_schema=table_schema,
            index_schema=index_table_schema,
            kind="unfolding",
            unfolded_index_column="value",
            unfolded_table_column="value",
            mount=True)
        self._insert_rows([{"key": 1, "value": [3]}])
        self._expect_from_index([{"key": 1, "value": 3, EMPTY_COLUMN_NAME: None}])

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
            predicate="not valueB",
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
            table_schema=prepend_hash(PRIMARY_SCHEMA),
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
            table_schema=prepend_hash(PRIMARY_SCHEMA),
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
            predicate="valueA > 100",
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

    @authors("sabdenovch")
    def test_evaluated(self):
        table_schema = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "any"},
        ]
        duplicated_evaluated_column = {"name": "eva01", "type": "int64", "expression": 'try_get_int64(value, "/field")'}

        index_name = "secondary"
        index_schema = [
            {"name": "eva01", "type": "int64", "sort_order": "ascending"},
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": EMPTY_COLUMN_NAME, "type": "int64"},
        ]
        self._create_basic_tables(
            index_name=index_name,
            table_schema=table_schema,
            index_schema=index_schema,
            evaluated_columns_schema=[duplicated_evaluated_column],
            mount=False,
        )
        index_table_path = self._get_index_path(index_name=index_name)

        another_index_name = "seconary_2"
        another_index_schema = [{"name": "eva00", "type": "string", "sort_order": "ascending"}] + index_schema
        self._add_index(
            index_name=another_index_name,
            schema=another_index_schema,
            evaluated_columns_schema=[
                {"name": "eva00", "type": "string", "expression": 'try_get_string(value, "/name")'},
                duplicated_evaluated_column,
            ],
            mount=False,
        )
        another_index_table_path = self._get_index_path(index_name=another_index_name)

        self._sync_create_cells()
        self._mount("//tmp/table", index_table_path, another_index_table_path)

        self._insert_rows(
            [
                {"key": 0, "value": {"field": 31, "name": "Biel"}},
                {"key": 1, "value": {"field": 13, "name": "Hathaway"}},
                {"key": 2, "value": {"field": 33, "name": "Coleman"}},
            ]
        )

        assert select_rows(f"key, eva01 from [{index_table_path}] limit 20") == [
            {"key": 1, "eva01": 13},
            {"key": 0, "eva01": 31},
            {"key": 2, "eva01": 33},
        ]

        # TODO(sabdenovch): Implement expression recognition in predicate.
        index_query = f"key, value from [//tmp/table] with index [{index_table_path}] AS Index where Index.eva01 = 33"
        plan = explain_query(index_query)
        assert plan["query"]["constraints"] == "Constraints:\n33: <universe>"

        query = "key, value from [//tmp/table] where try_get_int64(value, \"/field\") = 33"
        assert select_rows(query) == select_rows(index_query)

        assert select_rows(f"key, eva00, eva01 from [{another_index_table_path}] limit 20") == [
            {"eva00": "Biel", "eva01": 31, "key": 0},
            {"eva00": "Coleman", "eva01": 33, "key": 2},
            {"eva00": "Hathaway", "eva01": 13, "key": 1},
        ]

        prefix = "key, try_get_int64(value, \"/field\") from [//tmp/table]"
        suffix = "where try_get_string(value, \"/name\") = \"Biel\""
        query = f"{prefix} {suffix}"
        index_query = f"{prefix} with index [{another_index_table_path}] as i {suffix}"
        assert select_rows(query) == select_rows(index_query)


##################################################################


@pytest.mark.enabled_multidaemon
class TestSecondaryIndexReplicatedMaster(TestSecondaryIndexReplicatedBase, TestSecondaryIndexMaster):
    ENABLE_MULTIDAEMON = True

    @authors("sabdenovch")
    def test_holds_collocation(self):
        _ = self._create_table("//tmp/table", PRIMARY_SCHEMA)
        _ = self._create_table("//tmp/secondary", INDEX_ON_VALUE_SCHEMA)
        index_id, collocation_id = self._create_secondary_index()

        with raises_yt_error("Cannot remove table //tmp/table from collocation"):
            remove("//tmp/table/@replication_collocation_id")
        with raises_yt_error("Cannot remove table //tmp/secondary from collocation"):
            remove("//tmp/secondary/@replication_collocation_id")
        with raises_yt_error("Cannot remove collocation"):
            remove(f"#{collocation_id}")

        remove("//tmp/secondary")

        assert exists(f"#{collocation_id}")
        assert not exists(f"#{index_id}")

        remove("//tmp/table")


##################################################################


@pytest.mark.enabled_multidaemon
class TestSecondaryIndexReplicatedSelect(TestSecondaryIndexReplicatedBase, TestSecondaryIndexSelect):
    ENABLE_MULTIDAEMON = True

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
            sorted_dicts(select_rows(f"keyA, keyB, valueA, valueB FROM [{table_path}] WITH INDEX [{index_table_path}] I where valueA < 5")),
            sorted_dicts([{"keyA": i, "keyB": f"key{i}", "valueA": i, "valueB": i % 2 == 0} for i in range(5)])
        )

    @authors("sabdenovch")
    def test_postpone_index_resolve(self):
        self._create_table("//tmp/table", PRIMARY_SCHEMA)

        create("table", "//tmp/index_table", driver=self.REPLICA_DRIVER, attributes={
            "dynamic": True,
            "schema": INDEX_ON_VALUE_SCHEMA,
        })

        self._sync_create_cells()
        self._mount("//tmp/table")
        sync_mount_table("//tmp/index_table", driver=self.REPLICA_DRIVER)

        select_rows("* from [//tmp/table] with index [//tmp/index_table] I")

        remove("//tmp/index_table", driver=self.REPLICA_DRIVER)

        with raises_yt_error(yt_error_codes.ResolveErrorCode):
            select_rows("* from [//tmp/table] with index [//tmp/index_table] I")


##################################################################


@pytest.mark.enabled_multidaemon
class TestSecondaryIndexReplicatedModifications(TestSecondaryIndexReplicatedBase, TestSecondaryIndexModifications):
    ENABLE_MULTIDAEMON = True


##################################################################


@pytest.mark.enabled_multidaemon
class TestSecondaryIndexModificationsOverRpc(TestSecondaryIndexModifications):
    ENABLE_MULTIDAEMON = True
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True
