from yt_env_setup import YTEnvSetup, find_ut_file

from yt_commands import (
    authors, create, create_table, get, read_table, write_table, alter_table, write_local_file, map,
    assert_statistics, raises_yt_error, wait)

from yt_type_helpers import (
    make_column, make_schema, list_type
)

import yt_error_codes

from yt_helpers import skip_if_renaming_disabled

from yt.common import YtError
from yt.test_helpers import assert_items_equal

import pytest
import os

##################################################################


class TestJobQuery(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 5
    NUM_SCHEDULERS = 1

    DELTA_CONTROLLER_AGENT_CONFIG = {"controller_agent": {"udf_registry_path": "//tmp/udfs"}}

    def _init_udf_registry(self):
        registry_path = "//tmp/udfs"
        create("map_node", registry_path)

        abs_path = os.path.join(registry_path, "abs_udf")
        create(
            "file",
            abs_path,
            attributes={
                "function_descriptor": {
                    "name": "abs_udf",
                    "argument_types": [{"tag": "concrete_type", "value": "int64"}],
                    "result_type": {"tag": "concrete_type", "value": "int64"},
                    "calling_convention": "simple",
                }
            },
        )

        abs_impl_path = find_ut_file("test_udfs.bc")
        write_local_file(abs_path, abs_impl_path)

    @authors("lukyan")
    def test_query_simple(self):
        create(
            "table",
            "//tmp/t1",
            attributes={"schema": [{"name": "a", "type": "string"}]},
        )
        create("table", "//tmp/t2")
        write_table("//tmp/t1", {"a": "b"})

        map(in_="//tmp/t1", out="//tmp/t2", command="cat", spec={"input_query": "a"})

        assert read_table("//tmp/t2") == [{"a": "b"}]

    @authors("gritukan")
    def test_query_invalid(self):
        create(
            "table",
            "//tmp/t1",
            attributes={"schema": [{"name": "a", "type": "string"}]},
        )
        create("table", "//tmp/t2")
        write_table("//tmp/t1", {"a": "b"})

        with raises_yt_error(yt_error_codes.OperationFailedToPrepare):
            map(
                in_="//tmp/t1",
                out="//tmp/t2",
                command="cat",
                spec={"input_query": "a,a"},
            )

        with raises_yt_error(yt_error_codes.OperationFailedToPrepare):
            map(in_="//tmp/t1", out="//tmp/t2", command="cat", spec={"input_query": "b"})

    @authors("lukyan")
    def test_query_two_input_tables(self):
        create(
            "table",
            "//tmp/t1",
            attributes={
                "schema": [
                    {"name": "a", "type": "string"},
                    {"name": "b", "type": "string"},
                ]
            },
        )
        create(
            "table",
            "//tmp/t2",
            attributes={
                "schema": [
                    {"name": "a", "type": "string"},
                    {"name": "c", "type": "string"},
                ]
            },
        )
        create("table", "//tmp/t_out")
        write_table("//tmp/t1", {"a": "1", "b": "1"})
        write_table("//tmp/t2", {"a": "2", "c": "2"})

        map(
            in_=["//tmp/t1", "//tmp/t2"],
            out="//tmp/t_out",
            command="cat",
            spec={"input_query": "*"},
        )

        expected = [{"a": "1", "b": "1", "c": None}, {"a": "2", "b": None, "c": "2"}]
        assert_items_equal(read_table("//tmp/t_out"), expected)

    @authors("savrus", "lukyan")
    def test_query_reader_projection(self):
        create(
            "table",
            "//tmp/t1",
            attributes={
                "schema": [
                    {"name": "a", "type": "string"},
                    {"name": "c", "type": "string"},
                ],
                "optimize_for": "scan",
            },
        )
        create("table", "//tmp/t2")
        write_table("//tmp/t1", {"a": "b", "c": "d"})

        op = map(in_="//tmp/t1", out="//tmp/t2", command="cat", spec={"input_query": "a"})

        assert read_table("//tmp/t2") == [{"a": "b"}]
        attrs = get("//tmp/t1/@")
        wait(lambda: assert_statistics(
            op,
            key="data.input.uncompressed_data_size",
            assertion=lambda uncompressed_data_size: uncompressed_data_size < attrs["uncompressed_data_size"]))
        wait(lambda: assert_statistics(
            op,
            key="data.input.compressed_data_size",
            assertion=lambda compressed_data_size: compressed_data_size < attrs["compressed_data_size"]))
        wait(lambda: assert_statistics(
            op,
            key="data.input.data_weight",
            assertion=lambda data_weight: data_weight < attrs["data_weight"]))

    @authors("lukyan")
    @pytest.mark.parametrize("mode", ["ordered", "unordered"])
    def test_query_filtering(self, mode):
        create("table", "//tmp/t1", attributes={"schema": [{"name": "a", "type": "int64"}]})
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"a": i} for i in range(2)])

        map(
            in_="//tmp/t1",
            out="//tmp/t2",
            command="cat",
            mode=mode,
            spec={"input_query": "a where a > 0"},
        )

        assert read_table("//tmp/t2") == [{"a": 1}]

    @authors("lukyan")
    def test_query_asterisk(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        rows = [{"a": 1, "b": 2, "c": 3}, {"b": 5, "c": 6}, {"a": 7, "c": 8}]
        write_table("//tmp/t1", rows)

        schema = [
            {"name": "z", "type": "int64"},
            {"name": "a", "type": "int64"},
            {"name": "y", "type": "int64"},
            {"name": "b", "type": "int64"},
            {"name": "x", "type": "int64"},
            {"name": "c", "type": "int64"},
            {"name": "u", "type": "int64"},
        ]

        for row in rows:
            for column in schema:
                if column["name"] not in row.keys():
                    row[column["name"]] = None

        map(
            in_="//tmp/t1",
            out="//tmp/t2",
            command="cat",
            spec={"input_query": "* where a > 0 or b > 0", "input_schema": schema},
        )

        assert_items_equal(read_table("//tmp/t2"), rows)

    @authors("lukyan")
    def test_query_schema_in_spec(self):
        create(
            "table",
            "//tmp/t1",
            attributes={"schema": [{"name": "a", "type": "string"}]},
        )
        create(
            "table",
            "//tmp/t2",
            attributes={
                "schema": [
                    {"name": "a", "type": "string"},
                    {"name": "b", "type": "string"},
                ]
            },
        )
        create("table", "//tmp/t_out")
        write_table("//tmp/t1", {"a": "b"})
        write_table("//tmp/t2", {"a": "b"})

        map(
            in_="//tmp/t1",
            out="//tmp/t_out",
            command="cat",
            spec={
                "input_query": "*",
                "input_schema": [
                    {"name": "a", "type": "string"},
                    {"name": "b", "type": "string"},
                ],
            },
        )

        assert read_table("//tmp/t_out") == [{"a": "b", "b": None}]

        map(
            in_="//tmp/t2",
            out="//tmp/t_out",
            command="cat",
            spec={
                "input_query": "*",
                "input_schema": [{"name": "a", "type": "string"}],
            },
        )

        assert read_table("//tmp/t_out") == [{"a": "b"}]

    @authors("lukyan")
    def test_query_udf(self):
        self._init_udf_registry()

        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"a": i} for i in range(-1, 1)])

        map(
            in_="//tmp/t1",
            out="//tmp/t2",
            command="cat",
            spec={
                "input_query": "a where abs_udf(a) > 0",
                "input_schema": [{"name": "a", "type": "int64"}],
            },
        )

        assert read_table("//tmp/t2") == [{"a": -1}]

    @authors("lukyan")
    def test_query_wrong_schema(self):
        create(
            "table",
            "//tmp/t1",
            attributes={"schema": [{"name": "a", "type": "string"}]},
        )
        create("table", "//tmp/t2")
        write_table("//tmp/t1", {"a": "b"})

        with pytest.raises(YtError):
            map(
                in_="//tmp/t1",
                out="//tmp/t2",
                command="cat",
                spec={
                    "input_query": "a",
                    "input_schema": [{"name": "a", "type": "int64"}],
                },
            )

    @authors("lukyan")
    def test_query_range_inference(self):
        create(
            "table",
            "//tmp/t",
            attributes={"schema": [{"name": "a", "type": "int64", "sort_order": "ascending"}]},
        )
        create("table", "//tmp/t_out")
        for i in range(3):
            write_table("<append=%true>//tmp/t", [{"a": i * 10 + j} for j in range(3)])
        assert get("//tmp/t/@chunk_count") == 3

        def _test(selector, query, rows, chunk_count):
            op = map(
                in_="//tmp/t" + selector,
                out="//tmp/t_out",
                command="cat",
                spec={"input_query": query},
            )

            assert_items_equal(read_table("//tmp/t_out"), rows)
            wait(lambda: assert_statistics(
                op,
                key="data.input.chunk_count",
                assertion=lambda actual_chunk_count: actual_chunk_count == chunk_count))

        _test("", "a where a between 5 and 15", [{"a": i} for i in range(10, 13)], 1)
        _test("[#0:]", "a where a between 5 and 15", [{"a": i} for i in range(10, 13)], 1)
        _test(
            "[11:12]",
            "a where a between 5 and 15",
            [{"a": i} for i in range(11, 12)],
            1,
        )
        _test(
            "[9:20]",
            "a where a between 5 and 15",
            [{"a": i} for i in range(10, 13)],
            1,
        )
        _test("[#2:#4]", "a where a <= 10", [{"a": 2}, {"a": 10}], 2)
        _test("[10]", "a where a > 0", [{"a": 10}], 1)

    @authors("savrus")
    def test_query_range_inference_with_computed_columns(self):
        create(
            "table",
            "//tmp/t",
            attributes={
                "schema": [
                    {
                        "name": "h",
                        "type": "int64",
                        "sort_order": "ascending",
                        "expression": "k + 100",
                    },
                    {"name": "k", "type": "int64", "sort_order": "ascending"},
                    {"name": "a", "type": "int64", "sort_order": "ascending"},
                ]
            },
        )
        create("table", "//tmp/t_out")
        for i in range(3):
            write_table("<append=%true>//tmp/t", [{"k": i, "a": i * 10 + j} for j in range(3)])
        assert get("//tmp/t/@chunk_count") == 3

        def _test(query, rows, chunk_count):
            op = map(
                in_="//tmp/t",
                out="//tmp/t_out",
                command="cat",
                spec={"input_query": query},
            )

            assert_items_equal(read_table("//tmp/t_out"), rows)
            wait(lambda: assert_statistics(
                op,
                key="data.input.chunk_count",
                assertion=lambda actual_chunk_count: actual_chunk_count == chunk_count))

        _test("a where k = 1", [{"a": i} for i in range(10, 13)], 1)
        _test(
            "a where k = 1 and a between 5 and 15",
            [{"a": i} for i in range(10, 13)],
            1,
        )
        _test(
            "a where k in (1, 2) and a between 5 and 15",
            [{"a": i} for i in range(10, 13)],
            1,
        )

    @authors("ermolovd")
    def test_job_query_composite_type(self):
        create_table("//tmp/in", schema=make_schema([
            make_column("a", list_type("int64"))
        ]))
        create_table("//tmp/out")

        write_table("//tmp/in", [{"a": [1, 2, 3]}])

        map(in_="//tmp/in", out="//tmp/out", command="cat", spec={"input_query": "a"})

        assert read_table("//tmp/out") == [{"a": [1, 2, 3]}]

    @authors("levysotsky")
    def test_query_renamed_schema(self):
        skip_if_renaming_disabled(self.Env)

        input_table = "//tmp/t_in"
        input_table_with_append = "<append=%true>" + input_table
        output_table = "//tmp/t_out"

        schema1 = make_schema([
            make_column("a", "int64"),
            make_column("b", "string"),
            make_column("c", "bool"),
        ])

        schema2 = make_schema([
            make_column("a", "int64"),
            make_column("c_renamed", "bool", stable_name="c"),
            make_column("b_renamed", "string", stable_name="b"),
        ])

        create(
            "table",
            input_table,
            attributes={
                "schema": schema1,
                "optimize_for": "scan",
            },
        )
        create("table", output_table)

        def _make_rows(inds, renamed=False):
            b_name, c_name = ("b_renamed", "c_renamed") if renamed else ("b", "c")
            return [
                {"a": i, b_name: "foo_{}".format(i), c_name: bool(i % 2)}
                for i in inds
            ]

        write_table(input_table, _make_rows(range(10)))

        alter_table(input_table, schema=schema2)

        write_table(input_table_with_append, _make_rows(range(10, 20), renamed=True))

        alter_table(input_table, schema=schema1)

        write_table(input_table_with_append, _make_rows(range(20, 30)))

        def _test(query, expected_rows):
            map(
                in_=input_table,
                out=output_table,
                command="cat",
                spec={
                    "input_query": query,
                    "max_failed_job_count": 1,
                },
            )
            assert_items_equal(read_table(output_table), expected_rows)

        _test(
            'a where b in ("foo_9", "foo_12", "foo_23")',
            [{"a": 9}, {"a": 12}, {"a": 23}],
        )

        _test(
            "b where c",
            [{"b": "foo_{}".format(i)} for i in range(30) if i % 2 == 1],
        )

        with pytest.raises(YtError):
            _test(
                'a where b_renamed in ("foo_9", "foo_12", "foo_23")',
                [{"a": 9}, {"a": 12}, {"a": 23}],
            )

        alter_table(input_table, schema=schema2)

        _test(
            'a where b_renamed in ("foo_9", "foo_12", "foo_23")',
            [{"a": 9}, {"a": 12}, {"a": 23}],
        )

        _test(
            "b_renamed where c_renamed",
            [{"b_renamed": "foo_{}".format(i)} for i in range(30) if i % 2 == 1],
        )

        with pytest.raises(YtError):
            _test(
                'a where b in ("foo_9", "foo_12", "foo_23")',
                [{"a": 9}, {"a": 12}, {"a": 23}],
            )
