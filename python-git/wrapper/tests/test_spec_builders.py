from .helpers import TEST_DIR, check, get_test_file_path, set_config_option

from yt.wrapper.common import parse_bool
from yt.wrapper.operation_commands import add_failed_operation_stderrs_to_error_message
from yt.wrapper.spec_builders import (ReduceSpecBuilder, MergeSpecBuilder, SortSpecBuilder,
                                      MapReduceSpecBuilder, MapSpecBuilder)

import yt.wrapper as yt

from yt.packages.six.moves import xrange

import pytest
@pytest.mark.usefixtures("yt_env")
class TestSpecBuilders(object):
    def setup(self):
        yt.config["tabular_data_format"] = yt.format.JsonFormat()

    def teardown(self):
        yt.config["tabular_data_format"] = None
        yt.remove("//tmp/yt_wrapper/file_storage", recursive=True, force=True)

    def test_merge(self):
        tableX = TEST_DIR + "/tableX"
        tableY = TEST_DIR + "/tableY"
        dir = TEST_DIR + "/dir"
        res_table = dir + "/other_table"

        yt.write_table(tableX, [{"x": 1}])
        yt.write_table(tableY, [{"y": 2}])

        with pytest.raises(yt.YtError):
            spec_builder = MergeSpecBuilder() \
                .input_table_paths([tableX, tableY]) \
                .output_table_path(res_table)
            yt.run_operation(spec_builder)
        with pytest.raises(yt.YtError):
            spec_builder = MergeSpecBuilder() \
                .input_table_paths([tableX, tableY]) \
                .output_table_path(res_table)
            yt.run_operation(spec_builder)

        yt.mkdir(dir)
        spec_builder = MergeSpecBuilder() \
            .input_table_paths([tableX, tableY]) \
            .output_table_path(res_table)
        yt.run_operation(spec_builder)
        check([{"x": 1}, {"y": 2}], yt.read_table(res_table), ordered=False)

        spec_builder = MergeSpecBuilder() \
            .input_table_paths(tableX) \
            .output_table_path(res_table)
        yt.run_operation(spec_builder)
        assert not parse_bool(yt.get_attribute(res_table, "sorted"))
        check([{"x": 1}], yt.read_table(res_table))

        spec_builder = SortSpecBuilder() \
            .input_table_paths(tableX) \
            .sort_by(["x"]) \
            .output_table_path(tableX)
        yt.run_operation(spec_builder)
        spec_builder = MergeSpecBuilder() \
            .input_table_paths(tableX) \
            .output_table_path(res_table) \
            .mode("sorted")
        yt.run_operation(spec_builder)
        assert parse_bool(yt.get_attribute(res_table, "sorted"))
        check([{"x": 1}], yt.read_table(res_table))

    def test_run_operation(self):
        table = TEST_DIR + "/table"
        other_table = TEST_DIR + "/other_table"
        yt.write_table(table, [{"x": 1}, {"x": 2}])

        spec_builder = MapSpecBuilder() \
            .begin_mapper() \
                .command("cat") \
                .format("json") \
            .end_mapper() \
            .input_table_paths(table) \
            .output_table_paths(table) \
            .ordered(False)
        yt.run_operation(spec_builder)
        check([{"x": 1}, {"x": 2}], list(yt.read_table(table)), ordered=False)
        spec_builder = SortSpecBuilder() \
            .input_table_paths(table) \
            .sort_by(["x"]) \
            .output_table_path(table)
        yt.run_operation(spec_builder)

        with pytest.raises(yt.YtError):
            spec_builder = ReduceSpecBuilder() \
                .begin_reducer() \
                    .command("cat") \
                    .format("json") \
                .end_reducer() \
                .input_table_paths(table) \
                .output_table_paths([]) \
                .reduce_by(["x"])
            yt.run_operation(spec_builder)

        spec_builder = ReduceSpecBuilder() \
            .begin_reducer() \
                .command("cat") \
                .format("json") \
            .end_reducer() \
            .input_table_paths(table) \
            .output_table_paths(table) \
            .reduce_by(["x"])
        yt.run_operation(spec_builder)
        check([{"x": 1}, {"x": 2}], yt.read_table(table))

        spec_builder = MapSpecBuilder() \
            .begin_mapper() \
                .command("grep 2") \
                .format("json") \
            .end_mapper() \
            .input_table_paths(table) \
            .output_table_paths(other_table)
        yt.run_operation(spec_builder)
        check([{"x": 2}], yt.read_table(other_table))

        with pytest.raises(yt.YtError):
            spec_builder = MapSpecBuilder() \
                .begin_mapper() \
                    .command("cat") \
                    .format("json") \
                .end_mapper() \
                .input_table_paths([table, table + "xxx"]) \
                .output_table_paths(other_table)
            yt.run_operation(spec_builder)

        with pytest.raises(yt.YtError):
            spec_builder = ReduceSpecBuilder() \
                .begin_reducer() \
                    .command("cat") \
                    .format("json") \
                .end_reducer() \
                .input_table_paths(table) \
                .output_table_paths(other_table)
            yt.run_operation(spec_builder)

        # Run reduce on unsorted table
        with pytest.raises(yt.YtError):
            spec_builder = ReduceSpecBuilder() \
                .begin_reducer() \
                    .command("cat") \
                    .format("json") \
                .end_reducer() \
                .input_table_paths(other_table) \
                .output_table_paths(table) \
                .reduce_by(["x"])
            yt.run_operation(spec_builder)

        yt.write_table(table,
                       [
                           {"a": 12, "b": "ignat"},
                           {"b": "max"},
                           {"a": "x", "b": "name", "c": 0.5}
                       ])
        spec_builder = MapSpecBuilder() \
            .begin_mapper() \
                .command("PYTHONPATH=. ./capitalize_b.py") \
                .file_paths(yt.LocalFile(get_test_file_path("capitalize_b.py"))) \
                .format(yt.DsvFormat()) \
            .end_mapper() \
            .input_table_paths(yt.TablePath(table, columns=["b"])) \
            .output_table_paths(other_table)
        yt.run_operation(spec_builder)
        records = yt.read_table(other_table, raw=False)
        assert sorted([rec["b"] for rec in records]) == ["IGNAT", "MAX", "NAME"]
        assert sorted([rec["c"] for rec in records]) == []

    def test_reduce_combiner(self):
        table = TEST_DIR + "/table"
        output_table = TEST_DIR + "/output_table"
        yt.write_table(table, [{"x": 1}, {"y": 2}])

        spec_builder = MapReduceSpecBuilder() \
            .begin_reduce_combiner() \
                .command("cat") \
            .end_reduce_combiner() \
            .begin_reducer() \
                .command("cat") \
            .end_reducer() \
            .reduce_by(["x"]) \
            .sort_by(["x"]) \
            .input_table_paths(table) \
            .output_table_paths(output_table)

        yt.run_operation(spec_builder)
        check([{"x": 1}, {"y": 2}], list(yt.read_table(table)))

    @add_failed_operation_stderrs_to_error_message
    def test_python_operations_and_file_cache(self):
        def func(row):
            yield row

        input = TEST_DIR + "/input"
        output = TEST_DIR + "/output"
        yt.write_table(input, [{"x": 1}, {"y": 2}])

        # Some strange things are happen.
        # Sometimes in the first iteration some modules occurred to be unimported (like yt_env.pyc).
        # So we only tests that regularly operation files are the same in sequential runs.
        failures = 0
        for _ in xrange(3):
            spec_builder = MapSpecBuilder() \
                .begin_mapper() \
                    .command(func) \
                    .format("json") \
                .end_mapper() \
                .input_table_paths(input) \
                .output_table_paths(output)
            yt.run_operation(spec_builder)
            files_in_cache = list(yt.search("//tmp/yt_wrapper/file_storage", node_type="link"))
            assert len(files_in_cache) > 0

            spec_builder = MapSpecBuilder() \
                .begin_mapper() \
                    .command(func) \
                    .format("json") \
                .end_mapper() \
                .input_table_paths(input) \
                .output_table_paths(output)
            yt.run_operation(spec_builder)
            files_in_cache_again = list(yt.search("//tmp/yt_wrapper/file_storage", node_type="link"))
            if sorted(files_in_cache) != sorted(files_in_cache_again):
                failures += 1

        assert failures <= 1

    def test_preserve_user_spec_between_invocations(self):
        input_ = TEST_DIR + "/input"
        output = TEST_DIR + "/output"
        yt.write_table(input_, [{"x": 1}, {"y": 2}])
        spec = {"weight": 100.0, "title": "Test operation"}

        yt.run_sort(input_, input_, sort_by=["x"], spec=spec)
        yt.run_map("cat", input_, output, format="json", spec=spec)
        yt.run_map_reduce(None, "cat", input_, output, reduce_by=["x"], format="json", spec=spec)

    def test_spec_overrides_and_defaults(self):
        input_table = TEST_DIR + "/input"
        output_table = TEST_DIR + "/output"
        yt.write_table(input_table, [{"x": 1}, {"y": 2}])

        with set_config_option("spec_defaults", {"mapper": {"memory_limit": 128 * 1024 * 1024, "cpu_limit": 0.5772156649}}):
            with set_config_option("spec_overrides", {"mapper": {"memory_reserve_factor": 0.31}}):
                spec_builder = MapSpecBuilder() \
                    .begin_mapper() \
                        .command("cat") \
                        .format("json") \
                        .memory_limit(256 * 1024 * 1024) \
                        .memory_reserve_factor(0.2) \
                    .end_mapper() \
                    .input_table_paths(input_table) \
                    .output_table_paths(output_table)

                op = yt.run_operation(spec_builder)
                attributes = op.get_attributes()
                assert attributes["spec"]["mapper"]["memory_limit"] == 256 * 1024 * 1024
                assert abs(attributes["spec"]["mapper"]["cpu_limit"] - 0.5772156649) < 1e-5
                assert abs(attributes["spec"]["mapper"]["memory_reserve_factor"] - 0.31) < 1e-5
