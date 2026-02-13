from yt_chaos_test_base import ChaosTestBase
from yt_env_setup import YTEnvSetup

from yt_commands import (
    authors, commit_transaction, create, get, alter_table, raises_yt_error, start_transaction, write_table, copy, set, exists,
)

import pytest
from copy import deepcopy


##################################################################


def _ensure_schema_object_created(schema, optimize_for=None):
    attributes = {"schema": schema}
    if optimize_for is not None:
        attributes["optimize_for"] = optimize_for
    create("table", "//tmp/tmp-schema-table", attributes=attributes, force=True)
    return get("//tmp/tmp-schema-table/@schema_id")


def _maybe_write_data_into_table(table_path, table_is_empty):
    if table_is_empty:
        return
    for i in range(10):
        write_table(table_path, [{"b": i, "num": 11}])


def _drop_constraints(schema):
    for column in schema:
        if "constraint" in column.keys():
            del column["constraint"]


def _apply_constraints(schema, constraints):
    def try_apply_constraint(column, key):
        if key in column.keys() and column[key] in constraints.keys():
            column["constraint"] = constraints[column[key]]
            return True

        return False

    for column in schema:
        if not try_apply_constraint(column, "name"):
            try_apply_constraint(column, "stable_name")


@pytest.mark.enabled_multidaemon
class TestConstraintsRestrictions(ChaosTestBase):
    ENABLE_MULTIDAEMON = True

    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1
    NUM_REMOTE_CLUSTERS = 1
    NUM_SECONDARY_MASTER_CELLS_REMOTE_0 = 1

    DELTA_DRIVER_CONFIG = {
        "enable_read_from_async_replicas": True,
        "chaos_residency_cache": {
            "enable_client_mode" : True,
        },
    }

    MASTER_CELL_DESCRIPTORS_REMOTE_0 = {
        "21": {"roles": ["chunk_host", "cypress_node_host"]},
    }

    @authors("cherepashka")
    @pytest.mark.parametrize("deduplicate_schema_on_create", [False, True])
    @pytest.mark.parametrize("deduplicate_schema_on_alter", [False, True])
    def test_forbid_constraints(self, deduplicate_schema_on_create, deduplicate_schema_on_alter):
        schema = [
            {"name": "num", "type": "int64", "required": False, "type_v3": {"type_name": "optional", "item": "int64"}},
        ]
        constraints = {
            "num": "BETWEEN 10 AND 15",
        }

        set("//sys/@config/table_manager/enable_column_constraints_for_tables", False)

        if deduplicate_schema_on_create:
            schema_id = _ensure_schema_object_created(schema)
            with raises_yt_error("Creation of tables with column constraints is prohibited by system administrator"):
                create(
                    "table",
                    "//tmp/table",
                    attributes={
                        "schema_id": schema_id,
                        "constraints": constraints,
                    },
                )

        else:
            _apply_constraints(schema, constraints)
            with raises_yt_error("Creation of tables with column constraints is prohibited by system administrator"):
                create(
                    "table",
                    "//tmp/table",
                    attributes={
                        "constrained_schema": schema,
                    },
                )

        assert not exists("//tmp/table")
        _drop_constraints(schema)
        create(
            "table",
            "//tmp/table",
            attributes={
                "schema": schema,
            },
        )

        if deduplicate_schema_on_alter:
            schema_id = _ensure_schema_object_created(schema)

            with raises_yt_error("Table schema alter of tables with column constraints is prohibited by system administrator"):
                alter_table("//tmp/table", schema_id=schema_id, constraints=constraints)
        else:
            _apply_constraints(schema, constraints)
            with raises_yt_error("Table schema alter of tables with column constraints is prohibited by system administrator"):
                alter_table("//tmp/table", constrained_schema=schema)

        _drop_constraints(schema)
        assert get("//tmp/table/@constraints") == {}
        assert list(get("//tmp/table/@constrained_schema")) == schema

        set("//sys/@config/table_manager/enable_column_constraints_for_tables", True)

    @authors("cherepashka")
    @pytest.mark.parametrize("deduplicate_schema_on_create", [False, True])
    @pytest.mark.parametrize("deduplicate_schema_on_alter", [False, True])
    def test_constraints_are_prohobited_for_chaos_replicated_tables(self, deduplicate_schema_on_create, deduplicate_schema_on_alter):
        schema = [
            {"name": "b", "type": "int64", "sort_order": "ascending", "required": False, "type_v3": {"type_name": "optional", "item": "int64"}},
            {"name": "num", "type": "int64", "required": False, "type_v3": {"type_name": "optional", "item": "int64"}},
        ]
        constraints = {"num": "BETWEEN 10 AND 15"}
        cell_id = self._sync_create_chaos_bundle_and_cell()

        replicas = [
            {"cluster_name": "primary", "content_type": "data", "mode": "async", "enabled": True, "replica_path": "//tmp/t"},
            {"cluster_name": "remote_0", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/r0"},
        ]
        card_id, _ = self._create_chaos_tables(cell_id, replicas)

        if deduplicate_schema_on_create:
            schema_id = _ensure_schema_object_created(schema)
            with raises_yt_error("is not supported for chaos replicated tables"):
                create(
                    "chaos_replicated_table",
                    "//tmp/crt",
                    attributes={
                        "replication_card_id": card_id,
                        "chaos_cell_bundle": "c",
                        "schema_id": schema_id,
                        "constraints": constraints,
                    })

        else:
            _apply_constraints(schema, constraints)
            with raises_yt_error("is not supported for chaos replicated tables"):
                create(
                    "chaos_replicated_table",
                    "//tmp/crt",
                    attributes={
                        "replication_card_id": card_id,
                        "chaos_cell_bundle": "c",
                        "constrained_schema": schema,
                    })
            with raises_yt_error("Cannot specify constraints"):
                create(
                    "chaos_replicated_table",
                    "//tmp/crt",
                    attributes={
                        "replication_card_id": card_id,
                        "chaos_cell_bundle": "c",
                        "schema": schema,
                    })

        assert not exists("//tmp/crt")
        _drop_constraints(schema)
        create(
            "chaos_replicated_table",
            "//tmp/crt",
            attributes={
                "replication_card_id": card_id,
                "chaos_cell_bundle": "c",
                "schema": schema,
            })

        if deduplicate_schema_on_alter:
            schema_id = _ensure_schema_object_created(schema)
            with raises_yt_error("Table schema alter with constraints is not supported for chaos replicated tables"):
                alter_table("//tmp/crt", schema_id=schema_id, constraints=constraints)
        else:
            _apply_constraints(schema, constraints)
            with raises_yt_error("Table schema alter with constraints is not supported for chaos replicated tables"):
                alter_table("//tmp/crt", constrained_schema=schema)


@pytest.mark.enabled_multidaemon
class TestConstraints(YTEnvSetup):
    ENABLE_MULTIDAEMON = True
    NUM_TEST_PARTITIONS = 10

    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    @authors("cherepashka")
    @pytest.mark.parametrize("optimize_for", ["lookup", "scan"])
    @pytest.mark.parametrize("materialized", [False, True])
    @pytest.mark.parametrize("deduplicate_schema_on_create", [False, True])
    def test_simple(self, materialized, optimize_for, deduplicate_schema_on_create):
        schema = [
            {"name": "b", "type": "int64", "sort_order": "ascending", "required": False, "type_v3": {"type_name": "optional", "item": "int64"}},
            {"name": "num", "type": "int64", "required": False, "type_v3": {"type_name": "optional", "item": "int64"}},
            {
                "name": "doubled_num",
                "type": "int64",
                "expression": "num * 2",
                "materialized": materialized,
                "required": False,
                "type_v3": {"type_name": "optional", "item": "int64"}
            },
        ]

        constraints = {
            "num": "BETWEEN 10 AND 15",
            "doubled_num": "BETWEEN 20 AND 30",
        }

        if deduplicate_schema_on_create:
            schema_id = _ensure_schema_object_created(schema)
            create(
                "table",
                "//tmp/table",
                attributes={
                    "optimize_for": optimize_for,
                    "schema_id": schema_id,
                    "constraints": constraints,
                },
            )
            _apply_constraints(schema, constraints)

        else:
            _apply_constraints(schema, constraints)
            create(
                "table",
                "//tmp/table",
                attributes={
                    "optimize_for": optimize_for,
                    "constrained_schema": schema,
                },
            )
            schema_id = get("//tmp/table/@schema_id")

        assert get("//tmp/table/@constraints") == {"num": "BETWEEN 10 AND 15", "doubled_num": "BETWEEN 20 AND 30"}
        assert list(get("//tmp/table/@constrained_schema")) == schema
        _drop_constraints(schema)
        assert list(get("//tmp/table/@schema")) == schema
        assert get("//tmp/table/@schema_id") == schema_id

        # Constraining values per se is not implemented yet.
        write_table("//tmp/table", [{"num": 45}])

    @authors("cherepashka")
    def test_create_constraint_via_schema_and_constraint_params(self):
        schema = [
            {"name": "b", "type": "int64", "sort_order": "ascending", "required": False, "type_v3": {"type_name": "optional", "item": "int64"}},
            {"constraint": "BETWEEN 10 AND 15", "name": "num", "type": "int64", "required": False, "type_v3": {"type_name": "optional", "item": "int64"}},
        ]
        constraints = {
            "num": "BETWEEN 10 AND 15",
        }

        # It is ok to specify non conflicting `constrained_schema` and `constraints` attributes.
        create(
            "table",
            "//tmp/t1",
            attributes={
                "constrained_schema": schema,
                "constraints": constraints,
            },
        )
        constraints["num"] = "BETWEEN 20 AND 30"
        # Conflicting `constrained_schema` and `constraints` attributes are prohibited.
        with raises_yt_error("Received conflicting constraints"):
            create(
                "table",
                "//tmp/t2",
                attributes={
                    "constrained_schema": schema,
                    "constraints": constraints,
                },
            )
        _drop_constraints(schema)
        # But its ok to specify schema with no constraints and constraints separately.
        create(
            "table",
            "//tmp/t3",
            attributes={
                "schema": schema,
                "constraints": constraints,
            },
        )

    @authors("cherepashka")
    def test_alter_constraint_via_schema_and_constraint_params(self):
        base_schema = [
            {"name": "b", "type": "int64", "sort_order": "ascending", "required": False, "type_v3": {"type_name": "optional", "item": "int64"}},
        ]

        create("table", "//tmp/t", attributes={"schema": base_schema})

        constraints = {
            "num": "BETWEEN 20 AND 30",
        }
        constrained_schema = [
            {"name": "b", "type": "int64", "sort_order": "ascending", "required": False, "type_v3": {"type_name": "optional", "item": "int64"}},
            {"constraint": "BETWEEN 10 AND 15", "name": "num", "type": "int64", "required": False, "type_v3": {"type_name": "optional", "item": "int64"}},
        ]
        # Conflicting `schema` and `constraints` attributes are prohibited.
        with raises_yt_error("Received conflicting constraints"):
            alter_table("//tmp/t", constrained_schema=constrained_schema, constraints=constraints)

        constraints["num"] = "BETWEEN 10 AND 15"
        alter_table("//tmp/t", constrained_schema=constrained_schema, constraints=constraints)

    @authors("cherepashka")
    @pytest.mark.parametrize("optimize_for", ["lookup", "scan"])
    @pytest.mark.parametrize("materialized", [False, True])
    @pytest.mark.parametrize("deduplicate_schema_on_alter", [False, True])
    def test_add_constraints_via_alter(self, materialized, optimize_for, deduplicate_schema_on_alter):
        initial_schema = [
            {"name": "b", "type": "int64", "sort_order": "ascending", "required": False, "type_v3": {"type_name": "optional", "item": "int64"}},
        ]
        schema = [
            {"name": "b", "type": "int64", "sort_order": "ascending", "required": False, "type_v3": {"type_name": "optional", "item": "int64"}},
            {"name": "num", "type": "int64", "required": False, "type_v3": {"type_name": "optional", "item": "int64"}},
            {
                "name": "doubled_num",
                "type": "int64",
                "expression": "num * 2",
                "materialized": materialized,
                "required": False,
                "type_v3": {"type_name": "optional", "item": "int64"}
            },
        ]

        create(
            "table",
            "//tmp/table",
            attributes={
                "optimize_for": optimize_for,
                "schema": initial_schema,
            },
        )
        assert get("//tmp/table/@constraints") == {}
        assert list(get("//tmp/table/@constrained_schema")) == initial_schema

        constraints = {
            "num": "BETWEEN 10 AND 15",
            "doubled_num": "BETWEEN 20 AND 30",
        }

        if deduplicate_schema_on_alter:
            schema_id = _ensure_schema_object_created(schema, optimize_for)
            alter_table("//tmp/table", schema_id=schema_id, constraints=constraints)
            _apply_constraints(schema, constraints)
        else:
            _apply_constraints(schema, constraints)
            alter_table("//tmp/table", constrained_schema=schema)
            schema_id = get("//tmp/table/@schema_id")

        assert get("//tmp/table/@constraints") == {"num": "BETWEEN 10 AND 15", "doubled_num": "BETWEEN 20 AND 30"}
        assert list(get("//tmp/table/@constrained_schema")) == schema
        _drop_constraints(schema)
        assert list(get("//tmp/table/@schema")) == schema
        assert get("//tmp/table/@schema_id") == schema_id

        # Constraining values per se is not implemented yet.
        write_table("//tmp/table", [{"num": 45}])

    @authors("cherepashka")
    @pytest.mark.parametrize("deduplicate_schema_on_alter", [False, True])
    def test_alter_under_transaction(self, deduplicate_schema_on_alter):
        old_schema = [
            {"name": "b", "type": "int64", "sort_order": "ascending", "required": False, "type_v3": {"type_name": "optional", "item": "int64"}},
            {"constraint": "BETWEEN 10 AND 15", "name": "num", "type": "int64", "required": False, "type_v3": {"type_name": "optional", "item": "int64"}},
        ]

        create("table", "//tmp/table", attributes={"constrained_schema": old_schema})

        assert get("//tmp/table/@constraints") == {"num": "BETWEEN 10 AND 15"}
        assert list(get("//tmp/table/@constrained_schema")) == old_schema

        # Drop constraint for `num` column and add column `bum` with constraint under transaction.
        tx = start_transaction()

        new_schema = [
            {"name": "b", "type": "int64", "sort_order": "ascending", "required": False, "type_v3": {"type_name": "optional", "item": "int64"}},
            {"name": "num", "type": "int64", "required": False, "type_v3": {"type_name": "optional", "item": "int64"}},
            {"name": "bum", "type": "int64", "required": False, "type_v3": {"type_name": "optional", "item": "int64"}},
        ]

        new_constraints = {
            "bum": "BETWEEN 10 AND 15",
        }

        if deduplicate_schema_on_alter:
            schema_id = _ensure_schema_object_created(new_schema)
            alter_table("//tmp/table", schema_id=schema_id, constraints=new_constraints, tx=tx)
            _apply_constraints(new_schema, new_constraints)
        else:
            _apply_constraints(new_schema, new_constraints)
            alter_table("//tmp/table", constrained_schema=new_schema, tx=tx)

        assert get("//tmp/table/@constraints") == {"num": "BETWEEN 10 AND 15"}
        assert get("//tmp/table/@constraints", tx=tx) == new_constraints
        assert list(get("//tmp/table/@constrained_schema")) == old_schema
        assert list(get("//tmp/table/@constrained_schema", tx=tx)) == new_schema

        commit_transaction(tx)

        assert get("//tmp/table/@constraints") == new_constraints
        assert list(get("//tmp/table/@constrained_schema")) == new_schema

    @authors("cherepashka")
    @pytest.mark.parametrize("deduplicate_schema_on_alter", [False, True])
    @pytest.mark.parametrize("table_is_empty", [False, True])
    def test_cannot_modify_existing_constraint_via_alter(self, deduplicate_schema_on_alter, table_is_empty):
        schema = [
            {"name": "b", "type": "int64", "sort_order": "ascending", "required": False, "type_v3": {"type_name": "optional", "item": "int64"}},
            {"constraint": "BETWEEN 10 AND 15", "name": "num", "type": "int64", "required": False, "type_v3": {"type_name": "optional", "item": "int64"}},
        ]

        create("table", "//tmp/table", attributes={"constrained_schema": schema})
        assert get("//tmp/table/@constraints") == {"num": "BETWEEN 10 AND 15"}
        assert list(get("//tmp/table/@constrained_schema")) == schema

        _maybe_write_data_into_table("//tmp/table", table_is_empty)

        constraints = dict()
        constraints[schema[1]["name"]] = "BETWEEN 15 AND 30"
        # It is forbidden to change existing constraints.
        if deduplicate_schema_on_alter:
            _drop_constraints(schema)
            schema_id = _ensure_schema_object_created(schema)
            # It is allowed to change constraints for empty tables.
            if table_is_empty:
                alter_table("//tmp/table", schema_id=schema_id, constraints=constraints)
            else:
                with raises_yt_error("cannot be changed"):
                    alter_table("//tmp/table", schema_id=schema_id, constraints=constraints)
            _apply_constraints(schema, constraints)
        else:
            _apply_constraints(schema, constraints)
            # It is allowed to change constraints for empty tables.
            if table_is_empty:
                alter_table("//tmp/table", constrained_schema=schema)
            else:
                with raises_yt_error("cannot be changed"):
                    alter_table("//tmp/table", constrained_schema=schema)

        if table_is_empty:
            assert get("//tmp/table/@constraints") == {"num": "BETWEEN 15 AND 30"}
            assert list(get("//tmp/table/@constrained_schema")) == schema
        else:
            assert get("//tmp/table/@constraints") == {"num": "BETWEEN 10 AND 15"}
            schema[1]["constraint"] = "BETWEEN 10 AND 15"
            assert list(get("//tmp/table/@constrained_schema")) == schema

        del schema[1]["constraint"]
        # But it is ok to drop the constraint.
        alter_table("//tmp/table", constrained_schema=schema)
        assert get("//tmp/table/@constraints") == {}
        assert list(get("//tmp/table/@constrained_schema")) == schema

    @authors("cherepashka")
    @pytest.mark.parametrize("deduplicate_schema_on_alter", [False, True])
    @pytest.mark.parametrize("drop_by_empty_column_to_constraint", [False, True])
    def test_drop_constraint_via_alter(self, deduplicate_schema_on_alter, drop_by_empty_column_to_constraint):
        casual_schema = [
            {"name": "b", "type": "int64", "sort_order": "ascending", "required": False, "type_v3": {"type_name": "optional", "item": "int64"}},
            {"name": "num", "type": "int64", "required": False, "type_v3": {"type_name": "optional", "item": "int64"}},
            {"name": "c", "type": "int64", "required": False, "type_v3": {"type_name": "optional", "item": "int64"}},
        ]
        create("table", "//tmp/casual_table", attributes={"schema": casual_schema})
        casual_schema_id = get("//tmp/casual_table/@schema_id")
        assert get("//tmp/casual_table/@constraints") == {}
        assert list(get("//tmp/casual_table/@constrained_schema")) == casual_schema

        constrained_schema = [
            {"name": "b", "type": "int64", "sort_order": "ascending", "required": False, "type_v3": {"type_name": "optional", "item": "int64"}},
            {"constraint": "BETWEEN 10 AND 15", "name": "num", "type": "int64", "required": False, "type_v3": {"type_name": "optional", "item": "int64"}},
        ]

        create("table", "//tmp/table", attributes={"constrained_schema": constrained_schema})

        assert get("//tmp/table/@constraints") == {"num": "BETWEEN 10 AND 15"}
        assert list(get("//tmp/table/@constrained_schema")) == constrained_schema

        # To drop constraints it is needed to specify `constraints` as empty field or `constrained_schema` without cosntraints.
        if deduplicate_schema_on_alter:
            alter_table("//tmp/table", schema_id=casual_schema_id)
        else:
            alter_table("//tmp/table", schema=casual_schema)
        constrained_casual_schema = deepcopy(casual_schema)
        _apply_constraints(constrained_casual_schema, {"num": "BETWEEN 10 AND 15"})
        assert get("//tmp/table/@constraints") == {"num": "BETWEEN 10 AND 15"}
        assert list(get("//tmp/table/@constrained_schema")) == constrained_casual_schema

        if deduplicate_schema_on_alter:
            alter_table("//tmp/table", schema_id=casual_schema_id, constraints={})
        else:
            if drop_by_empty_column_to_constraint:
                alter_table("//tmp/table", constraints={})
            else:
                alter_table("//tmp/table", constrained_schema=casual_schema)
        assert get("//tmp/table/@constraints") == {}
        assert list(get("//tmp/table/@constrained_schema")) == list(get("//tmp/casual_table/@constrained_schema")) == casual_schema

    @authors("cherepashka")
    def test_copy(self):
        schema = [
            {"constraint": "BETWEEN 10 AND 15", "name": "num", "type": "int64", "required": False, "type_v3": {"type_name": "optional", "item": "int64"}},
        ]
        create("table", "//tmp/table", attributes={"constrained_schema": schema})
        copy("//tmp/table", "//tmp/copy")

        assert get("//tmp/table/@constraints") == get("//tmp/copy/@constraints") == {"num": "BETWEEN 10 AND 15"}
        assert list(get("//tmp/table/@constrained_schema")) == list(get("//tmp/copy/@constrained_schema")) == schema

    @authors("cherepashka")
    @pytest.mark.parametrize("deduplicate_schema_on_alter", [False, True])
    def test_remove_constrained_column(self, deduplicate_schema_on_alter):
        schema = [
            {"constraint": "BETWEEN 10 AND 15", "name": "num1", "type": "int64", "required": False, "type_v3": {"type_name": "optional", "item": "int64"}},
            {"constraint": "BETWEEN 1 AND 2", "name": "num2", "type": "int64", "required": False, "type_v3": {"type_name": "optional", "item": "int64"}},
        ]
        create("table", "//tmp/table", attributes={"constrained_schema": schema})
        assert get("//tmp/table/@constraints") == {"num1": "BETWEEN 10 AND 15", "num2": "BETWEEN 1 AND 2"}

        # Drop "num1" column with constraint.
        schema = [
            {"name": "num1", "type": "int64", "required": False, "type_v3": {"type_name": "optional", "item": "int64"}},
        ]
        constraints = {"num1": "BETWEEN 10 AND 15"}
        if deduplicate_schema_on_alter:
            _drop_constraints(schema)
            schema_id = _ensure_schema_object_created(schema)
            alter_table("//tmp/table", schema_id=schema_id, constraints=constraints)
            _apply_constraints(schema, constraints)
        else:
            _apply_constraints(schema, constraints)
            alter_table("//tmp/table", constrained_schema=schema)

        assert get("//tmp/table/@constraints") == constraints
        assert list(get("//tmp/table/@constrained_schema")) == schema

    @authors("cherepashka")
    def test_rename_constrained_column_on_create(self):
        schema = [
            {"constraint": "BETWEEN 10 AND 15", "name": "num1", "stable_name": "_num1", "type": "int64", "required": False, "type_v3": {"type_name": "optional", "item": "int64"}},
            {"constraint": "BETWEEN 1 AND 2", "name": "num2", "stable_name": "_num2", "type": "int64", "required": False, "type_v3": {"type_name": "optional", "item": "int64"}},
        ]
        create("table", "//tmp/table", attributes={"constrained_schema": schema})

        assert get("//tmp/table/@constraints") == {"num1": "BETWEEN 10 AND 15", "num2": "BETWEEN 1 AND 2"}
        assert list(get("//tmp/table/@constrained_schema")) == schema

    @authors("cherepashka")
    def test_rename_constrained_column_on_alter(self):
        schema = [
            {"constraint": "BETWEEN 10 AND 15", "name": "num1", "type": "int64", "required": False, "type_v3": {"type_name": "optional", "item": "int64"}},
            {"constraint": "BETWEEN 1 AND 2", "name": "num2", "type": "int64", "required": False, "type_v3": {"type_name": "optional", "item": "int64"}},
        ]
        create("table", "//tmp/table", attributes={"constrained_schema": schema})
        assert get("//tmp/table/@constraints") == {"num1": "BETWEEN 10 AND 15", "num2": "BETWEEN 1 AND 2"}
        new_schema = [
            {"name": "num1_new", "stable_name": "num1", "type": "int64", "required": False, "type_v3": {"type_name": "optional", "item": "int64"}},
            {"name": "num2_new", "stable_name": "num2", "type": "int64", "required": False, "type_v3": {"type_name": "optional", "item": "int64"}},
        ]
        alter_table("//tmp/table", schema=new_schema)

        new_constraints = {"num1_new": "BETWEEN 10 AND 15", "num2_new": "BETWEEN 1 AND 2"}
        _apply_constraints(new_schema, new_constraints)
        assert get("//tmp/table/@constraints") == new_constraints
        assert list(get("//tmp/table/@constrained_schema")) == new_schema

        newer_schema = [
            {"name": "num1_newer", "stable_name": "num1", "type": "int64", "required": False, "type_v3": {"type_name": "optional", "item": "int64"}},
            {"name": "num2_newer", "stable_name": "num2", "type": "int64", "required": False, "type_v3": {"type_name": "optional", "item": "int64"}},
        ]
        alter_table("//tmp/table", schema=newer_schema)

        newer_constraints = {"num1_newer": "BETWEEN 10 AND 15", "num2_newer": "BETWEEN 1 AND 2"}
        _apply_constraints(newer_schema, newer_constraints)
        assert get("//tmp/table/@constraints") == newer_constraints
        assert list(get("//tmp/table/@constrained_schema")) == newer_schema

    @authors("cherepashka")
    @pytest.mark.parametrize("deduplicate_schema_on_alter", [False, True])
    def test_alter_parallel_is_forbidden(self, deduplicate_schema_on_alter):
        schema = [
            {"name": "num", "type": "int64", "required": False, "type_v3": {"type_name": "optional", "item": "int64"}},
        ]
        create("table", "//tmp/table", attributes={"schema": schema})

        new_schema = [
            {"name": "num", "type": "int64", "required": False, "type_v3": {"type_name": "optional", "item": "int64"}},
            {"name": "num1", "type": "int64", "required": False, "type_v3": {"type_name": "optional", "item": "int64"}},
        ]
        constraints1 = {"num": "BETWEEN 1 AND 2"}
        constraints2 = {"num": "BETWEEN 3 AND 4"}
        tx1 = start_transaction()
        tx2 = start_transaction()
        if deduplicate_schema_on_alter:
            schema_id = _ensure_schema_object_created(new_schema)
            alter_table("//tmp/table", schema_id=schema_id, constraints=constraints1, tx=tx1)
            with raises_yt_error("Cannot take \"exclusive\" lock for node"):
                alter_table("//tmp/table", schema_id=schema_id, constraints=constraints2, tx=tx2)
        else:
            _apply_constraints(new_schema, constraints1)
            alter_table("//tmp/table", constrained_schema=new_schema, tx=tx1)

            _apply_constraints(new_schema, constraints2)
            with raises_yt_error("Cannot take \"exclusive\" lock for node"):
                alter_table("//tmp/table", constrained_schema=new_schema, tx=tx2)
        commit_transaction(tx1)
        commit_transaction(tx2)

        _apply_constraints(new_schema, constraints1)
        assert list(get("//tmp/table/@constrained_schema")) == new_schema

    @authors("cherepashka")
    def test_create_constraint_validation(self):
        constrained_schema = [
            {"constraint": "BETWEEN 10 AND 15", "name": "num1", "type": "int64", "required": False, "type_v3": {"type_name": "optional", "item": "int64"}},
            {"constraint": "BETWEEN 1 AND 2", "name": "num2", "type": "int64", "required": False, "type_v3": {"type_name": "optional", "item": "int64"}},
        ]
        with raises_yt_error("Cannot specify constraints in \"schema\" option, use \"constrained_schema\" instead"):
            create("table", "//tmp/table", attributes={"schema": constrained_schema})
        with raises_yt_error("Cannot create table with constraints and without schema"):
            create("table", "//tmp/table", attributes={"constraints": {"num": "BETWEEN 1 AND 2"}})

    @authors("cherepashka")
    @pytest.mark.parametrize("table_is_empty", [False, True])
    def test_alter_constrained_column_type_validation(self, table_is_empty):
        initial_schema = [
            {"name": "num", "type": "int8", "required": False, "type_v3": {"type_name": "optional", "item": "int8"}},
            {"name": "b", "type": "int64", "required": False, "type_v3": {"type_name": "optional", "item": "int64"}},
        ]
        create("table", "//tmp/table", attributes={"schema": initial_schema, "constraints": {"num": "BETWEEN 1 AND 2"}})
        _maybe_write_data_into_table("//tmp/table", table_is_empty)

        changed_type_schema = [
            {"name": "num", "type": "int64", "required": False, "type_v3": {"type_name": "optional", "item": "int64"}},
            {"name": "b", "type": "int64", "required": False, "type_v3": {"type_name": "optional", "item": "int64"}},
        ]
        if table_is_empty:
            alter_table("//tmp/table", schema=changed_type_schema)
        else:
            with raises_yt_error("Altering type for constrained column is forbidden"):
                alter_table("//tmp/table", schema=changed_type_schema)

    @authors("cherepashka")
    def test_mixing_schema_attributes_to_create_constraint(self):
        simple_schema = [
            {"name": "num1", "type": "int64", "required": False, "type_v3": {"type_name": "optional", "item": "int64"}},
        ]
        constrained_schema = [
            {"constraint": "BETWEEN 10 AND 15", "name": "num1", "type": "int64", "required": False, "type_v3": {"type_name": "optional", "item": "int64"}},
            {"constraint": "BETWEEN 1 AND 2", "name": "num2", "type": "int64", "required": False, "type_v3": {"type_name": "optional", "item": "int64"}},
        ]
        simple_schema_id = _ensure_schema_object_created(simple_schema)

        with raises_yt_error("Mix of \"schema\" and \"constrained_schema\" attributes are specified and the schemas do not match"):
            create("table", "//tmp/table", attributes={"schema": simple_schema, "constrained_schema": constrained_schema})
        with raises_yt_error("Mix of \"schema\", \"constrained_schema\" and \"schema_id\" attributes are specified and the schemas do not match"):
            create("table", "//tmp/table", attributes={"schema_id": simple_schema_id, "constrained_schema": constrained_schema})
        constrained_schema.pop()
        with raises_yt_error("Received conflicting constraints"):
            create("table", "//tmp/table", attributes={"schema_id": simple_schema_id, "constrained_schema": constrained_schema, "constraints": {"num2": "BETWEEN 1 AND 2"}})

        create("table", "//tmp/table", attributes={"schema_id": simple_schema_id, "constrained_schema": constrained_schema, "constraints": {"num1": "BETWEEN 10 AND 15"}})

    @authors("cherepashka")
    def test_mixing_schema_attributes_to_alter_constraint(self):
        simple_schema = [
            {"name": "num1", "type": "int64", "required": False, "type_v3": {"type_name": "optional", "item": "int64"}},
        ]
        constrained_schema = [
            {"constraint": "BETWEEN 10 AND 15", "name": "num1", "type": "int64", "required": False, "type_v3": {"type_name": "optional", "item": "int64"}},
            {"constraint": "BETWEEN 1 AND 2", "name": "num2", "type": "int64", "required": False, "type_v3": {"type_name": "optional", "item": "int64"}},
        ]
        create("table", "//tmp/table", attributes={"constrained_schema": simple_schema})
        simple_schema_id = get("//tmp/table/@schema_id")
        with raises_yt_error("Both \"schema\" and \"constrained_schema\" specified and the schemas do not match"):
            alter_table("//tmp/table", schema=simple_schema, constrained_schema=constrained_schema)
        with raises_yt_error("the schemas do not match"):
            alter_table("//tmp/table", schema_id=simple_schema_id, constrained_schema=constrained_schema)

        simple_schema = [
            {"name": "num1", "type": "int64", "required": False, "type_v3": {"type_name": "optional", "item": "int64"}},
            {"name": "num2", "type": "int64", "required": False, "type_v3": {"type_name": "optional", "item": "int64"}},
        ]
        constrained_schema = [
            {"name": "num1", "type": "int64", "required": False, "type_v3": {"type_name": "optional", "item": "int64"}},
            {"constraint": "BETWEEN 1 AND 2", "name": "num2", "type": "int64", "required": False, "type_v3": {"type_name": "optional", "item": "int64"}},
        ]

        with raises_yt_error("Received conflicting constraints"):
            alter_table("//tmp/table", schema=simple_schema, constrained_schema=constrained_schema, constraints={"num1": "BETWEEN 1 AND 2"})
        alter_table("//tmp/table", schema=simple_schema, constrained_schema=constrained_schema, constraints={"num2": "BETWEEN 1 AND 2"})
        assert get("//tmp/table/@constraints") == {"num2": "BETWEEN 1 AND 2"}


##################################################################


@pytest.mark.enabled_multidaemon
class TestConstraintsMulticell(TestConstraints):
    ENABLE_MULTIDAEMON = True
    NUM_SECONDARY_MASTER_CELLS = 2

    MASTER_CELL_DESCRIPTORS = {
        "11": {"roles": ["chunk_host"]},
        "12": {"roles": ["chunk_host"]},
    }


##################################################################


@pytest.mark.enabled_multidaemon
class TestConstraintsPortal(TestConstraintsMulticell):
    ENABLE_MULTIDAEMON = True
    ENABLE_TMP_PORTAL = True

    MASTER_CELL_DESCRIPTORS = {
        "11": {"roles": ["chunk_host", "cypress_node_host"]},
        "12": {"roles": ["chunk_host"]},
    }


##################################################################


@pytest.mark.enabled_multidaemon
class TestConstraintsRpcProxy(TestConstraints):
    ENABLE_MULTIDAEMON = True
    DRIVER_BACKEND = "rpc"
    ENABLE_HTTP_PROXY = True
    ENABLE_RPC_PROXY = True


##################################################################
