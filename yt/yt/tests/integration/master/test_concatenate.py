from yt_env_setup import YTEnvSetup

from yt_commands import (
    authors, create, get, set, remove, concatenate, start_transaction, abort_transaction,
    commit_transaction, lock,
    read_table, write_table, alter_table,
    merge, sort, get_singular_chunk_id, raises_yt_error)

from yt_type_helpers import make_schema, normalize_schema, list_type

import yt_error_codes

from yt.common import YtError

import pytest

##################################################################


@pytest.mark.enabled_multidaemon
class TestConcatenate(YTEnvSetup):
    ENABLE_MULTIDAEMON = True
    NUM_TEST_PARTITIONS = 2
    NUM_MASTERS = 1
    NUM_NODES = 9
    NUM_SCHEDULERS = 1

    @authors("ermolovd")
    def test_simple_concatenate(self):
        create("table", "//tmp/t1")
        write_table("//tmp/t1", {"key": "x"})
        assert read_table("//tmp/t1") == [{"key": "x"}]

        create("table", "//tmp/t2")
        write_table("//tmp/t2", {"key": "y"})
        assert read_table("//tmp/t2") == [{"key": "y"}]

        create("table", "//tmp/union")

        concatenate(["//tmp/t1", "//tmp/t2"], "//tmp/union")
        assert read_table("//tmp/union") == [{"key": "x"}, {"key": "y"}]

        concatenate(["//tmp/t1", "//tmp/t2"], "<append=true>//tmp/union")
        assert read_table("//tmp/union") == [{"key": "x"}, {"key": "y"}] * 2

    @authors("ermolovd")
    def test_sorted(self):
        create("table", "//tmp/t1")
        write_table("//tmp/t1", {"key": "x"})
        sort(in_="//tmp/t1", out="//tmp/t1", sort_by="key")
        assert read_table("//tmp/t1") == [{"key": "x"}]
        assert get("//tmp/t1/@sorted", "true")

        create("table", "//tmp/t2")
        write_table("//tmp/t2", {"key": "y"})
        sort(in_="//tmp/t2", out="//tmp/t2", sort_by="key")
        assert read_table("//tmp/t2") == [{"key": "y"}]
        assert get("//tmp/t2/@sorted", "true")

        create("table", "//tmp/union")
        sort(in_="//tmp/union", out="//tmp/union", sort_by="key")
        assert get("//tmp/union/@sorted", "true")

        concatenate(["//tmp/t2", "//tmp/t1"], "<append=true>//tmp/union")
        assert read_table("//tmp/union") == [{"key": "y"}, {"key": "x"}]
        assert get("//tmp/union/@sorted", "false")

    @authors("ermolovd")
    def test_infer_schema(self):
        create(
            "table",
            "//tmp/t1",
            attributes={"schema": [{"name": "key", "type": "string"}]},
        )
        write_table("//tmp/t1", {"key": "x"})

        create(
            "table",
            "//tmp/t2",
            attributes={"schema": [{"name": "key", "type": "string"}]},
        )
        write_table("//tmp/t2", {"key": "y"})

        create("table", "//tmp/union")

        concatenate(["//tmp/t1", "//tmp/t2"], "//tmp/union")
        assert read_table("//tmp/union") == [{"key": "x"}, {"key": "y"}]
        assert get("//tmp/union/@schema") == get("//tmp/t1/@schema")

    @authors("ermolovd")
    def test_infer_schema_many_columns(self):
        row = {"a": "1", "b": "2", "c": "3", "d": "4"}
        for table_path in ["//tmp/t1", "//tmp/t2"]:
            create(
                "table",
                table_path,
                attributes={
                    "schema": [
                        {"name": "b", "type": "string"},
                        {"name": "a", "type": "string"},
                        {"name": "d", "type": "string"},
                        {"name": "c", "type": "string"},
                    ]
                },
            )
            write_table(table_path, [row])

        create("table", "//tmp/union")
        concatenate(["//tmp/t1", "//tmp/t2"], "//tmp/union")

        assert read_table("//tmp/union") == [row] * 2
        assert normalize_schema(get("//tmp/union/@schema")) == make_schema(
            [
                {"name": "a", "type": "string", "required": False},
                {"name": "b", "type": "string", "required": False},
                {"name": "c", "type": "string", "required": False},
                {"name": "d", "type": "string", "required": False},
            ],
            strict=True,
            unique_keys=False,
        )

    @authors("ermolovd")
    def test_conflict_missing_output_schema_append(self):
        create(
            "table",
            "//tmp/t1",
            attributes={"schema": [{"name": "key", "type": "string"}]},
        )
        write_table("//tmp/t1", {"key": "x"})

        create(
            "table",
            "//tmp/t2",
            attributes={"schema": [{"name": "key", "type": "string"}]},
        )
        write_table("//tmp/t2", {"key": "y"})

        create("table", "//tmp/union")
        empty_schema = get("//tmp/union/@schema")

        concatenate(["//tmp/t1", "//tmp/t2"], "<append=%true>//tmp/union")
        assert get("//tmp/union/@schema_mode") == "weak"
        assert get("//tmp/union/@schema") == empty_schema

    @authors("ermolovd")
    @pytest.mark.parametrize("append", [False, True])
    def test_output_schema_same_as_input(self, append):
        schema = [{"name": "key", "type": "string"}]
        create("table", "//tmp/t1", attributes={"schema": schema})
        write_table("//tmp/t1", {"key": "x"})

        create("table", "//tmp/t2", attributes={"schema": schema})
        write_table("//tmp/t2", {"key": "y"})

        create("table", "//tmp/union", attributes={"schema": schema})
        old_schema = get("//tmp/union/@schema")

        if append:
            concatenate(["//tmp/t1", "//tmp/t2"], "<append=%true>//tmp/union")
        else:
            concatenate(["//tmp/t1", "//tmp/t2"], "//tmp/union")

        assert read_table("//tmp/union") == [{"key": "x"}, {"key": "y"}]
        assert get("//tmp/union/@schema") == old_schema

    @authors("ermolovd")
    def test_impossibility_to_concatenate_into_sorted_table(self):
        schema = [{"name": "key", "type": "string"}]
        create("table", "//tmp/t1", attributes={"schema": schema})
        write_table("//tmp/t1", {"key": "x"})

        create("table", "//tmp/t2", attributes={"schema": schema})
        write_table("//tmp/t2", {"key": "y"})

        create("table", "//tmp/union", attributes={"schema": schema})
        sort(in_="//tmp/union", out="//tmp/union", sort_by=["key"])

        with pytest.raises(YtError):
            concatenate(["//tmp/t1", "//tmp/t2"], "//tmp/union")

    @authors("ermolovd")
    @pytest.mark.parametrize("append", [False, True])
    def test_compatible_schemas(self, append):
        create(
            "table",
            "//tmp/t1",
            attributes={"schema": [{"name": "key", "type": "string"}]},
        )
        write_table("//tmp/t1", {"key": "x"})

        create(
            "table",
            "//tmp/t2",
            attributes={"schema": [{"name": "value", "type": "string"}]},
        )
        write_table("//tmp/t2", {"value": "y"})

        create(
            "table",
            "//tmp/union",
            attributes={
                "schema": [
                    {"name": "key", "type": "string"},
                    {"name": "value", "type": "string"},
                ]
            },
        )
        old_schema = get("//tmp/union/@schema")

        if append:
            concatenate(["//tmp/t1", "//tmp/t2"], "<append=%true>//tmp/union")
        else:
            concatenate(["//tmp/t1", "//tmp/t2"], "//tmp/union")

        assert read_table("//tmp/union") == [{"key": "x"}, {"value": "y"}]
        assert get("//tmp/union/@schema") == old_schema

    @authors("ermolovd")
    def test_incompatible_schemas(self):
        create(
            "table",
            "//tmp/t1",
            attributes={"schema": [{"name": "key", "type": "string"}]},
        )

        create(
            "table",
            "//tmp/t2",
            attributes={"schema": [{"name": "value", "type": "int64"}]},
        )

        create(
            "table",
            "//tmp/t3",
            attributes={"schema": [{"name": "other_column", "type": "string"}]},
        )

        create(
            "table",
            "//tmp/union",
            attributes={
                "schema": [
                    {"name": "key", "type": "string"},
                    {"name": "value", "type": "string"},
                ]
            },
        )

        with pytest.raises(YtError):
            concatenate(["//tmp/t1", "//tmp/t2"], "//tmp/union")

        with pytest.raises(YtError):
            concatenate(["//tmp/t1", "//tmp/t3"], "//tmp/union")

    @authors("ermolovd")
    def test_different_input_schemas_no_output_schema(self):
        create(
            "table",
            "//tmp/t1",
            attributes={"schema": [{"name": "key", "type": "string"}]},
        )

        create(
            "table",
            "//tmp/t2",
            attributes={"schema": [{"name": "value", "type": "int64"}]},
        )

        create("table", "//tmp/union")
        empty_schema = get("//tmp/union/@schema")

        concatenate(["//tmp/t1", "//tmp/t2"], "//tmp/union")
        assert get("//tmp/union/@schema_mode") == "weak"
        assert get("//tmp/union/@schema") == empty_schema

    @authors("ermolovd")
    def test_strong_output_schema_weak_input_schemas(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")

        create(
            "table",
            "//tmp/union",
            attributes={"schema": [{"name": "value", "type": "int64"}]},
        )

        with pytest.raises(YtError):
            concatenate(["//tmp/t1", "//tmp/t2"], "//tmp/union")

    @authors("ermolovd")
    def test_append_to_sorted_weak_schema(self):
        create("table", "//tmp/t1")
        write_table("//tmp/t1", {"key": "x"})

        create("table", "//tmp/union")

        write_table("//tmp/union", {"key": "y"})
        sort(in_="//tmp/union", out="//tmp/union", sort_by="key")
        assert get("//tmp/union/@schema_mode", "weak")
        assert get("//tmp/union/@sorted", "true")

        concatenate(["//tmp/t1"], "<append=true>//tmp/union")

        assert get("//tmp/union/@sorted", "false")

        assert read_table("//tmp/t1") == [{"key": "x"}]
        assert read_table("//tmp/union") == [{"key": "y"}, {"key": "x"}]

    @authors("ermolovd")
    def test_concatenate_unique_keys(self):
        create(
            "table",
            "//tmp/t1",
            attributes={
                "schema": make_schema(
                    [{"name": "key", "type": "string", "sort_order": "ascending"}],
                    unique_keys=True,
                )
            },
        )
        write_table("//tmp/t1", {"key": "x"})

        create(
            "table",
            "//tmp/t2",
            attributes={
                "schema": make_schema(
                    [{"name": "key", "type": "string", "sort_order": "ascending"}],
                    unique_keys=True,
                )
            },
        )
        write_table("//tmp/t2", {"key": "x"})

        create("table", "//tmp/union")
        concatenate(["//tmp/t1", "//tmp/t2"], "//tmp/union")

        assert get("//tmp/union/@sorted", "false")

        assert read_table("//tmp/union") == [{"key": "x"}, {"key": "x"}]
        assert get("//tmp/union/@schema/@unique_keys", "false")

    @authors("ermolovd", "kiselyovp")
    def test_empty_concatenate(self):
        create("table", "//tmp/union")
        orig_schema = get("//tmp/union/@schema")
        concatenate([], "//tmp/union")
        assert get("//tmp/union/@schema_mode") == "weak"
        assert get("//tmp/union/@schema") == orig_schema

    @authors("ermolovd")
    def test_lost_complex_column(self):
        create(
            "table",
            "//tmp/t1",
            attributes={
                "schema": make_schema(
                    [
                        {"name": "list_column", "type_v3": list_type("int64")},
                        {"name": "int_column", "type_v3": "int64"},
                    ]
                )
            },
        )

        create(
            "table",
            "//tmp/union",
            attributes={
                "schema": make_schema(
                    [
                        {"name": "int_column", "type_v3": "int64"},
                    ],
                    strict=False,
                )
            },
        )
        with raises_yt_error("is missing in strict part of output schema"):
            concatenate(["//tmp/t1"], "//tmp/union")

    @authors("gritukan")
    @pytest.mark.parametrize("sort_order", ["ascending", "descending"])
    def test_sorted_concatenate_simple(self, sort_order):
        create(
            "table",
            "//tmp/in1",
            attributes={"schema": make_schema([{"name": "a", "type": "int64", "sort_order": sort_order}])},
        )
        write_table("//tmp/in1", [{"a": 1}])

        create(
            "table",
            "//tmp/in2",
            attributes={"schema": make_schema([{"name": "a", "type": "int64", "sort_order": sort_order}])},
        )
        write_table("//tmp/in2", [{"a": 2}])

        create(
            "table",
            "//tmp/out",
            attributes={"schema": make_schema([{"name": "a", "type": "int64", "sort_order": sort_order}])},
        )

        concatenate(["//tmp/in1", "//tmp/in2"], "//tmp/out")
        assert get("//tmp/out/@sorted")
        assert get("//tmp/out/@sorted_by") == ["a"]
        if sort_order == "ascending":
            assert read_table("//tmp/out") == [{"a": 1}, {"a": 2}]
        else:
            assert read_table("//tmp/out") == [{"a": 2}, {"a": 1}]

    @authors("gritukan")
    @pytest.mark.parametrize("erasure", [False, True])
    def test_sorted_concatenate_stricter_chunks(self, erasure):
        def make_rows(values):
            return [{"a": value} for value in values]

        create(
            "table",
            "//tmp/in",
            attributes={"schema": make_schema([{"name": "a", "type": "int64"}])},
        )
        if erasure:
            set("//tmp/in/@erasure_codec", "reed_solomon_6_3")

        for x in [1, 3, 2]:
            write_table("<chunk_sort_columns=[a];append=true>//tmp/in", make_rows([x]))
        assert get("//tmp/in/@chunk_count") == 3

        assert read_table("//tmp/in") == make_rows([1, 3, 2])

        create(
            "table",
            "//tmp/out",
            attributes={"schema": make_schema([{"name": "a", "type": "int64", "sort_order": "ascending"}])},
        )

        concatenate(["//tmp/in"], "//tmp/out")

        assert read_table("//tmp/out") == make_rows([1, 2, 3])
        assert get("//tmp/out/@sorted")

    @authors("gritukan")
    @pytest.mark.parametrize("sort_order", ["ascending", "descending"])
    def test_sorted_concatenate_comparator(self, sort_order):
        create(
            "table",
            "//tmp/in1",
            attributes={
                "schema": make_schema([
                    {"name": "a", "type": "int64", "sort_order": sort_order},
                    {"name": "b", "type": "string"}
                ])
            },
        )
        first_chunk = [{"a": 1, "b": "x"}, {"a": 2, "b": "z"}]
        if sort_order == "descending":
            first_chunk = first_chunk[::-1]
        write_table("//tmp/in1", first_chunk)

        create(
            "table",
            "//tmp/in2",
            attributes={
                "schema": make_schema([
                    {"name": "a", "type": "int64", "sort_order": sort_order},
                    {"name": "b", "type": "string"}
                ])
            },
        )
        second_chunk = [{"a": 1, "b": "y"}, {"a": 1, "b": "z"}]
        write_table("//tmp/in2", second_chunk)

        create(
            "table",
            "//tmp/out",
            attributes={
                "schema": make_schema(
                    [
                        {"name": "a", "type": "int64", "sort_order": sort_order},
                        {"name": "b", "type": "string"},
                    ]
                )
            },
        )

        concatenate(["//tmp/in1", "//tmp/in2"], "//tmp/out")
        if sort_order == "ascending":
            assert read_table("//tmp/out") == second_chunk + first_chunk
        else:
            assert read_table("//tmp/out") == first_chunk + second_chunk
        assert get("//tmp/out/@sorted")

    @authors("gritukan")
    @pytest.mark.parametrize("sort_order", ["ascending", "descending"])
    def test_sorted_concatenate_overlapping_ranges(self, sort_order):
        def make_rows(values):
            mul = 1 if sort_order == "ascending" else -1
            return [{"a": value * mul} for value in values]

        create(
            "table",
            "//tmp/in1",
            attributes={"schema": make_schema([{"name": "a", "type": "int64", "sort_order": sort_order}])},
        )
        write_table("//tmp/in1", make_rows([1, 3]))

        create(
            "table",
            "//tmp/in2",
            attributes={"schema": make_schema([{"name": "a", "type": "int64", "sort_order": sort_order}])},
        )
        write_table("//tmp/in2", make_rows([2, 4]))

        create(
            "table",
            "//tmp/out",
            attributes={"schema": make_schema([{"name": "a", "type": "int64", "sort_order": sort_order}])},
        )

        with raises_yt_error(yt_error_codes.SortOrderViolation):
            concatenate(["//tmp/in1", "//tmp/in2"], "//tmp/out")

    @authors("gritukan")
    def test_sorted_concatenate_schema(self):
        def make_row(a, b, c):
            return {"a": a, "b": b, "c": c}

        create(
            "table",
            "//tmp/in1",
            attributes={
                "schema": make_schema(
                    [
                        {"name": "a", "type": "int64"},
                        {"name": "b", "type": "int64"},
                        {"name": "c", "type": "int64"},
                    ]
                )
            },
        )

        write_table(
            "<chunk_sort_columns=[a;b;c];append=true>//tmp/in1",
            [make_row(3, 3, 3), make_row(4, 4, 4)],
        )
        write_table(
            "<chunk_sort_columns=[a;b];append=true>//tmp/in1",
            [make_row(1, 1, 1), make_row(2, 2, 2)],
        )
        assert read_table("//tmp/in1") == [
            make_row(3, 3, 3),
            make_row(4, 4, 4),
            make_row(1, 1, 1),
            make_row(2, 2, 2),
        ]

        create(
            "table",
            "//tmp/out1",
            attributes={
                "schema": make_schema(
                    [
                        {"name": "a", "type": "int64", "sort_order": "ascending"},
                        {"name": "b", "type": "int64", "sort_order": "ascending"},
                        {"name": "c", "type": "int64"},
                    ]
                )
            },
        )

        concatenate(["//tmp/in1"], "//tmp/out1")
        assert read_table("//tmp/out1") == [
            make_row(1, 1, 1),
            make_row(2, 2, 2),
            make_row(3, 3, 3),
            make_row(4, 4, 4),
        ]

        create(
            "table",
            "//tmp/out2",
            attributes={
                "schema": make_schema(
                    [
                        {"name": "a", "type": "int64", "sort_order": "ascending"},
                        {"name": "b", "type": "int64", "sort_order": "ascending"},
                        {"name": "c", "type": "int64", "sort_order": "ascending"},
                    ]
                )
            },
        )

        with raises_yt_error(yt_error_codes.SchemaViolation):
            concatenate(["//tmp/in1"], "//tmp/out2")

        create(
            "table",
            "//tmp/in2",
            attributes={"schema": make_schema([{"name": "a", "type": "int64", "sort_order": "ascending"}])},
        )
        write_table("//tmp/in2", [{"a": 1}, {"a": 3}])

        create(
            "table",
            "//tmp/in3",
            attributes={"schema": make_schema([{"name": "a", "type": "int64", "sort_order": "ascending"}])},
        )
        write_table("//tmp/in3", [{"a": 2}, {"a": 4}])

        create("table", "//tmp/out3")
        concatenate(["//tmp/in2", "//tmp/in3"], "//tmp/out3")
        assert not get("//tmp/out3/@sorted")
        assert read_table("//tmp/out3") == [{"a": 1}, {"a": 3}, {"a": 2}, {"a": 4}]

    @authors("gritukan")
    @pytest.mark.parametrize("sort_order", ["ascending", "descending"])
    def test_sorted_concatenate_append(self, sort_order):
        def make_rows(values):
            mul = 1 if sort_order == "ascending" else -1
            return [{"a": value * mul} for value in values]

        create(
            "table",
            "//tmp/in1",
            attributes={"schema": make_schema([{"name": "a", "type": "int64", "sort_order": sort_order}])},
        )
        write_table("//tmp/in1", make_rows([1, 2]))

        create(
            "table",
            "//tmp/in2",
            attributes={"schema": make_schema([{"name": "a", "type": "int64", "sort_order": sort_order}])},
        )
        write_table("//tmp/in2", make_rows([5, 6]))

        create(
            "table",
            "//tmp/out1",
            attributes={"schema": make_schema([{"name": "a", "type": "int64", "sort_order": sort_order}])},
        )
        write_table("//tmp/out1", make_rows([3, 4]))

        with raises_yt_error(yt_error_codes.SortOrderViolation):
            concatenate(["//tmp/in1"], "<append=true>//tmp/out1")

        concatenate(["//tmp/in2"], "<append=true>//tmp/out1")
        assert read_table("//tmp/out1") == make_rows([3, 4, 5, 6])

        create(
            "table",
            "//tmp/in3",
            attributes={
                "schema": make_schema(
                    [
                        {"name": "a", "type": "int64", "sort_order": sort_order},
                    ],
                    unique_keys=False,
                )
            },
        )

        write_table("//tmp/in3", make_rows([2, 2]))

        concatenate(["//tmp/in3", "//tmp/in3"], "<append=true>//tmp/in3")
        assert read_table("//tmp/in3") == make_rows([2, 2, 2, 2, 2, 2])

        create(
            "table",
            "//tmp/in4",
            attributes={
                "schema": make_schema(
                    [
                        {"name": "a", "type": "int64", "sort_order": sort_order},
                    ],
                    unique_keys=True,
                )
            },
        )
        write_table("//tmp/in4", make_rows([2]))
        with raises_yt_error(yt_error_codes.UniqueKeyViolation):
            concatenate(["//tmp/in4", "//tmp/in4"], "<append=true>//tmp/in4")

    @authors("gritukan")
    @pytest.mark.parametrize("sort_order", ["ascending", "descending"])
    def test_sorted_concatenate_unique_keys_validation(self, sort_order):
        def make_rows(values):
            mul = 1 if sort_order == "ascending" else -1
            return [{"a": value * mul} for value in values]

        unique_keys_schema = make_schema([{
            "name": "a",
            "type": "int64",
            "sort_order": sort_order}], unique_keys=True)
        not_unique_keys_schema = make_schema([{
            "name": "a",
            "type": "int64",
            "sort_order": sort_order}], unique_keys=False)

        create("table", "//tmp/in1", attributes={"schema": unique_keys_schema})
        write_table("//tmp/in1", {"a": 1})

        create("table", "//tmp/in2", attributes={"schema": unique_keys_schema})
        write_table("//tmp/in2", {"a": 1})

        create("table", "//tmp/in3", attributes={"schema": not_unique_keys_schema})
        write_table("//tmp/in3", {"a": 2})

        create("table", "//tmp/out", attributes={"schema": unique_keys_schema})

        # No. Keys are not unique.
        with raises_yt_error(yt_error_codes.UniqueKeyViolation):
            concatenate(["//tmp/in1", "//tmp/in2"], "//tmp/out")

        # No. Schema is too weak.
        with raises_yt_error(yt_error_codes.SchemaViolation):
            concatenate(["//tmp/in1", "//tmp/in3"], "//tmp/out")

    @authors("gritukan")
    def test_input_with_custom_transaction(self):
        custom_tx = start_transaction()

        create("table", "//tmp/in", tx=custom_tx)
        write_table("//tmp/in", {"foo": "bar"}, tx=custom_tx)

        create("table", "//tmp/out")

        with pytest.raises(YtError):
            concatenate(["//tmp/in"], "//tmp/out")
        concatenate(['<transaction_id="{}">//tmp/in'.format(custom_tx)], "//tmp/out")

        assert read_table("//tmp/out") == [{"foo": "bar"}]

    @authors("levysotsky")
    def test_concatenate_renamed_columns(self):
        schema1 = make_schema([{"name": "a", "type": "int64"}])
        schema2 = make_schema([{"name": "a_new", "stable_name": "a", "type": "int64"}])

        create("table", "//tmp/t1", attributes={"schema": schema1})
        write_table("//tmp/t1", {"a": 1})

        alter_table("//tmp/t1", schema=schema2)

        write_table("<append=%true>//tmp/t1", {"a_new": 2})

        create("table", "//tmp/t2", attributes={"schema": schema2})

        write_table("//tmp/t2", {"a_new": 3})

        create("table", "//tmp/union")

        concatenate(["//tmp/t1", "//tmp/t2"], "//tmp/union")
        assert read_table("//tmp/union") == [{"a_new": 1}, {"a_new": 2}, {"a_new": 3}]

        concatenate(["//tmp/t1", "//tmp/t2"], "<append=true>//tmp/union")
        assert read_table("//tmp/union") == [{"a_new": 1}, {"a_new": 2}, {"a_new": 3}] * 2

        schema3 = make_schema([{"name": "a_new", "type": "int64"}])
        create("table", "//tmp/t3", attributes={"schema": schema3})
        write_table("//tmp/t3", {"a_new": 3})

        # Mismatched stable names.
        with raises_yt_error(yt_error_codes.IncompatibleSchemas):
            concatenate(["//tmp/t1", "//tmp/t3"], "//tmp/union")

        schema4 = make_schema([{"name": "a", "type": "int64"}])
        create("table", "//tmp/t4", attributes={"schema": schema4})
        write_table("//tmp/t4", {"a": 3})

        # Mismatched names.
        with raises_yt_error(yt_error_codes.IncompatibleSchemas):
            concatenate(["//tmp/t1", "//tmp/t4"], "//tmp/union")

    @authors("gritukan")
    def test_uniqualize_chunks(self):
        if self.DRIVER_BACKEND == "rpc":
            return

        create("table", "//tmp/t")
        write_table("//tmp/t", {"a": 1})
        concatenate(["//tmp/t", "//tmp/t"], "//tmp/t")

        chunk_ids = get("//tmp/t/@chunk_ids")
        assert len(chunk_ids) == 2
        assert chunk_ids[0] == chunk_ids[1]

        concatenate(["//tmp/t"], "//tmp/t", uniqualize_chunks=True)
        assert get("//tmp/t/@chunk_ids") == [chunk_ids[0]]
        assert read_table("//tmp/t") == [{"a": 1}]

##################################################################


@pytest.mark.enabled_multidaemon
class TestConcatenateMulticell(TestConcatenate):
    ENABLE_MULTIDAEMON = True
    NUM_SECONDARY_MASTER_CELLS = 2

    @authors("gritukan")
    def test_concatenate_imported_chunks(self):
        create("table", "//tmp/t1", attributes={"external_cell_tag": 11})
        write_table("//tmp/t1", [{"a": "b"}])

        create("table", "//tmp/t2", attributes={"external_cell_tag": 12})
        op = merge(mode="unordered", in_=["//tmp/t1"], out="//tmp/t2")
        op.track()
        chunk_id = get_singular_chunk_id("//tmp/t2")
        assert len(get("#" + chunk_id + "/@exports")) > 0

        tx1 = start_transaction()
        lock("//tmp/t2", mode="exclusive", tx=tx1)
        remove("//tmp/t2", tx=tx1)

        create("table", "//tmp/t3", attributes={"external_cell_tag": 11})

        tx2 = start_transaction()
        concatenate(['<transaction_id="{}">//tmp/t2'.format(tx2)], "//tmp/t3")

        assert read_table("//tmp/t3") == [{"a": "b"}]

        abort_transaction(tx1)
        assert read_table("//tmp/t3") == [{"a": "b"}]

    @authors("shakurov")
    @pytest.mark.parametrize("attributes", [
        {"src": {"external_cell_tag": 11}, "dst": {"external": False}},
        {"src": {"external": False}, "dst": {"external_cell_tag": 11}},
        {"src": {"external": False}, "dst": {"external": False}},
        {"src": {"external_cell_tag": 11}, "dst": {"external": False}},
    ])
    def test_concatenate_externality(self, attributes):
        create("table", "//tmp/t1", attributes=attributes["src"])
        write_table("//tmp/t1", [{"a": "b"}])
        create("table", "//tmp/t", attributes=attributes["dst"])

        tx = start_transaction()
        concatenate(["//tmp/t1"], "//tmp/t", tx=tx)
        commit_transaction(tx)

        assert read_table("//tmp/t") == [{"a": "b"}]


@pytest.mark.enabled_multidaemon
class TestConcatenatePortal(TestConcatenateMulticell):
    ENABLE_MULTIDAEMON = True
    ENABLE_TMP_PORTAL = True
    NUM_SECONDARY_MASTER_CELLS = 3

    @authors("shakurov")
    def test_concatenate_between_primary_and_secondary_shards(self):
        create("table", "//tmp/src1", attributes={"external": False})
        write_table("//tmp/src1", [{"a": "b"}])
        create("table", "//tmp/src2", attributes={"external_cell_tag": 11})
        write_table("//tmp/src2", [{"c": "d"}])

        create("portal_entrance", "//portals/p", attributes={"exit_cell_tag": 12})
        create("table", "//portals/p/dst", attributes={"exit_cell_tag": 11})

        tx = start_transaction()
        concatenate(["//tmp/src1", "//tmp/src2"], "//portals/p/dst", tx=tx)
        commit_transaction(tx)

        assert read_table("//portals/p/dst") == [{"a": "b"}, {"c": "d"}]

    @authors("shakurov")
    def test_concatenate_between_secondary_shards(self):
        create("table", "//tmp/src1", attributes={"external_cell_tag": 11})
        write_table("//tmp/src1", [{"a": "b"}])
        create("table", "//tmp/src2", attributes={"external_cell_tag": 13})
        write_table("//tmp/src2", [{"c": "d"}])

        create("portal_entrance", "//portals/p", attributes={"exit_cell_tag": 12})
        create("table", "//portals/p/dst", attributes={"external_cell_tag": 11})

        tx = start_transaction()
        concatenate(["//tmp/src1", "//tmp/src2"], "//portals/p/dst", tx=tx)
        commit_transaction(tx)

        assert read_table("//portals/p/dst") == [{"a": "b"}, {"c": "d"}]


@pytest.mark.enabled_multidaemon
class TestConcatenateShardedTx(TestConcatenatePortal):
    ENABLE_MULTIDAEMON = True
    NUM_SECONDARY_MASTER_CELLS = 5
    MASTER_CELL_DESCRIPTORS = {
        "10": {"roles": ["cypress_node_host"]},
        "11": {"roles": ["cypress_node_host", "chunk_host"]},
        "12": {"roles": ["cypress_node_host", "chunk_host"]},
        "13": {"roles": ["chunk_host"]},
        "14": {"roles": ["transaction_coordinator"]},
        "15": {"roles": ["transaction_coordinator"]},
    }


@pytest.mark.enabled_multidaemon
class TestConcatenateShardedTxCTxS(TestConcatenateShardedTx):
    ENABLE_MULTIDAEMON = True
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True

    DELTA_RPC_PROXY_CONFIG = {
        "cluster_connection": {
            "transaction_manager": {
                "use_cypress_transaction_service": True,
            }
        }
    }


@pytest.mark.enabled_multidaemon
class TestConcatenateMirroredTx(TestConcatenateShardedTxCTxS):
    ENABLE_MULTIDAEMON = True
    USE_SEQUOIA = True
    ENABLE_CYPRESS_TRANSACTIONS_IN_SEQUOIA = True
    ENABLE_TMP_ROOTSTOCK = False
    NUM_CYPRESS_PROXIES = 1

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "commit_operation_cypress_node_changes_via_system_transaction": True,
    }

    DELTA_DYNAMIC_MASTER_CONFIG = {
        "transaction_manager": {
            "forbid_transaction_actions_for_cypress_transactions": True,
        }
    }


@pytest.mark.enabled_multidaemon
class TestConcatenateRpcProxy(TestConcatenate):
    ENABLE_MULTIDAEMON = True
    DRIVER_BACKEND = "rpc"
    ENABLE_HTTP_PROXY = True
    ENABLE_RPC_PROXY = True


@pytest.mark.enabled_multidaemon
class TestConcatenateCypressProxy(TestConcatenate):
    ENABLE_MULTIDAEMON = True
    NUM_CYPRESS_PROXIES = 1
