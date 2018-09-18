from yt_env_setup import YTEnvSetup, unix_only, find_ut_file
from yt_commands import *

from yt.test_helpers import assert_items_equal, are_almost_equal

import pytest
import os

##################################################################

@unix_only
class TestJobQuery(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 5
    NUM_SCHEDULERS = 1

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "udf_registry_path": "//tmp/udfs"
        }
    }

    def _init_udf_registry(self):
        registry_path = "//tmp/udfs"
        create("map_node", registry_path)

        abs_path = os.path.join(registry_path, "abs_udf")
        create(
            "file", abs_path,
            attributes={"function_descriptor": {
                "name": "abs_udf",
                "argument_types": [{
                    "tag": "concrete_type",
                    "value": "int64"}],
                "result_type": {
                    "tag": "concrete_type",
                    "value": "int64"},
                "calling_convention": "simple"}})

        abs_impl_path = find_ut_file("test_udfs.bc")
        write_local_file(abs_path, abs_impl_path)

    def test_query_simple(self):
        create("table", "//tmp/t1", attributes={
            "schema": [{"name": "a", "type": "string"}]
        })
        create("table", "//tmp/t2")
        write_table("//tmp/t1", {"a": "b"})

        map(in_="//tmp/t1", out="//tmp/t2", command="cat",
            spec={"input_query": "a"})

        assert read_table("//tmp/t2") == [{"a": "b"}]

    def test_query_two_input_tables(self):
        create("table", "//tmp/t1", attributes={
            "schema": [{"name": "a", "type": "string"},
                       {"name": "b", "type": "string"}]
        })
        create("table", "//tmp/t2", attributes={
            "schema": [{"name": "a", "type": "string"},
                       {"name": "c", "type": "string"}]
        })
        create("table", "//tmp/t_out")
        write_table("//tmp/t1", {"a": "1", "b": "1"})
        write_table("//tmp/t2", {"a": "2", "c": "2"})

        map(in_=["//tmp/t1", "//tmp/t2"], out="//tmp/t_out", command="cat",
            spec={"input_query": "*"})

        expected = [{"a": "1", "b": "1", "c": None}, {"a": "2", "b": None, "c": "2"}]
        assert_items_equal(read_table("//tmp/t_out"), expected)

    def test_query_reader_projection(self):
        create("table", "//tmp/t1", attributes={
            "schema": [{"name": "a", "type": "string"}, {"name": "c", "type": "string"}],
            "optimize_for": "scan"
        })
        create("table", "//tmp/t2")
        write_table("//tmp/t1", {"a": "b", "c": "d"})

        op = map(in_="//tmp/t1", out="//tmp/t2", command="cat",
            spec={"input_query": "a"})

        assert read_table("//tmp/t2") == [{"a": "b"}]
        statistics = get("//sys/operations/{0}/@progress/job_statistics".format(op.id))
        attrs = get("//tmp/t1/@")
        assert get_statistics(statistics, "data.input.uncompressed_data_size.$.completed.map.sum") < attrs["uncompressed_data_size"]
        assert get_statistics(statistics, "data.input.compressed_data_size.$.completed.map.sum") < attrs["compressed_data_size"]
        assert get_statistics(statistics, "data.input.data_weight.$.completed.map.sum") < attrs["data_weight"]

    @pytest.mark.parametrize("mode", ["ordered", "unordered"])
    def test_query_filtering(self, mode):
        create("table", "//tmp/t1", attributes={
            "schema": [{"name": "a", "type": "int64"}]
        })
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"a": i} for i in xrange(2)])

        map(in_="//tmp/t1", out="//tmp/t2", command="cat", mode=mode,
            spec={"input_query": "a where a > 0"})

        assert read_table("//tmp/t2") == [{"a": 1}]

    def test_query_asterisk(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        rows = [
            {"a": 1, "b": 2, "c": 3},
            {"b": 5, "c": 6},
            {"a": 7, "c": 8}]
        write_table("//tmp/t1", rows)

        schema = [{"name": "z", "type": "int64"},
            {"name": "a", "type": "int64"},
            {"name": "y", "type": "int64"},
            {"name": "b", "type": "int64"},
            {"name": "x", "type": "int64"},
            {"name": "c", "type": "int64"},
            {"name": "u", "type": "int64"}]

        for row in rows:
            for column in schema:
                if column["name"] not in row.keys():
                    row[column["name"]] = None

        map(in_="//tmp/t1", out="//tmp/t2", command="cat",
            spec={
                "input_query": "* where a > 0 or b > 0",
                "input_schema": schema})

        assert_items_equal(read_table("//tmp/t2"), rows)

    def test_query_schema_in_spec(self):
        create("table", "//tmp/t1", attributes={
            "schema": [{"name": "a", "type": "string"}]
        })
        create("table", "//tmp/t2", attributes={
            "schema": [{"name": "a", "type": "string"}, {"name": "b", "type": "string"}]
        })
        create("table", "//tmp/t_out")
        write_table("//tmp/t1", {"a": "b"})
        write_table("//tmp/t2", {"a": "b"})

        map(in_="//tmp/t1", out="//tmp/t_out", command="cat",
            spec={"input_query": "*", "input_schema": [{"name": "a", "type": "string"}, {"name": "b", "type": "string"}]})

        assert read_table("//tmp/t_out") == [{"a": "b", "b": None}]

        map(in_="//tmp/t2", out="//tmp/t_out", command="cat",
            spec={"input_query": "*", "input_schema": [{"name": "a", "type": "string"}]})

        assert read_table("//tmp/t_out") == [{"a": "b"}]

    def test_query_udf(self):
        self._init_udf_registry()

        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"a": i} for i in xrange(-1, 1)])

        map(in_="//tmp/t1", out="//tmp/t2", command="cat",
            spec={"input_query": "a where abs_udf(a) > 0", "input_schema": [{"name": "a", "type": "int64"}]})

        assert read_table("//tmp/t2") == [{"a": -1}]

    def test_query_wrong_schema(self):
        create("table", "//tmp/t1", attributes={
            "schema": [{"name": "a", "type": "string"}]
        })
        create("table", "//tmp/t2")
        write_table("//tmp/t1", {"a": "b"})

        with pytest.raises(YtError):
            map(in_="//tmp/t1", out="//tmp/t2", command="cat",
                spec={"input_query": "a", "input_schema": [{"name": "a", "type": "int64"}]})

    def test_query_range_inference(self):
        create("table", "//tmp/t", attributes={
            "schema": [{"name": "a", "type": "int64", "sort_order": "ascending"}]
        })
        create("table", "//tmp/t_out")
        for i in range(3):
            write_table("<append=%true>//tmp/t", [{"a": i * 10 + j} for j in xrange(3)])
        assert get("//tmp/t/@chunk_count") == 3

        def _test(selector, query, rows, chunk_count):
            op = map(
                in_="//tmp/t" + selector,
                out="//tmp/t_out",
                command="cat",
                spec={"input_query": query})

            assert_items_equal(read_table("//tmp/t_out"), rows)
            statistics = get("//sys/operations/{0}/@progress/job_statistics".format(op.id))
            assert get_statistics(statistics, "data.input.chunk_count.$.completed.map.sum") == chunk_count

        _test("", "a where a between 5 and 15", [{"a": i} for i in xrange(10, 13)], 1)
        _test("[#0:]", "a where a between 5 and 15", [{"a": i} for i in xrange(10, 13)], 1)
        _test("[11:12]", "a where a between 5 and 15", [{"a": i} for i in xrange(11, 12)], 1)
        _test("[9:20]", "a where a between 5 and 15", [{"a": i} for i in xrange(10, 13)], 1)
        _test("[#2:#4]", "a where a <= 10", [{"a": 2}, {"a": 10}], 2)
        _test("[10]", "a where a > 0", [{"a": 10}], 1)

    def test_query_range_inference_with_computed_columns(self):
        create("table", "//tmp/t", attributes={
            "schema": [
                {"name": "h", "type": "int64", "sort_order": "ascending", "expression": "k + 100"},
                {"name": "k", "type": "int64", "sort_order": "ascending"},
                {"name": "a", "type": "int64", "sort_order": "ascending"}]
        })
        create("table", "//tmp/t_out")
        for i in range(3):
            write_table("<append=%true>//tmp/t", [{"k": i, "a": i*10 + j} for j in xrange(3)])
        assert get("//tmp/t/@chunk_count") == 3

        def _test(query, rows, chunk_count):
            op = map(
                in_="//tmp/t",
                out="//tmp/t_out",
                command="cat",
                spec={"input_query": query})

            assert_items_equal(read_table("//tmp/t_out"), rows)
            statistics = get("//sys/operations/{0}/@progress/job_statistics".format(op.id))
            assert get_statistics(statistics, "data.input.chunk_count.$.completed.map.sum") == chunk_count

        _test("a where k = 1", [{"a": i} for i in xrange(10, 13)], 1)
        _test("a where k = 1 and a between 5 and 15", [{"a": i} for i in xrange(10, 13)], 1)
        _test("a where k in (1, 2) and a between 5 and 15", [{"a": i} for i in xrange(10, 13)], 1)
