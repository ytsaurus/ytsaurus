from .helpers import TEST_DIR, check, get_test_file_path, set_config_option, get_python

from yt.wrapper.common import update
from yt.wrapper.operation_commands import add_failed_operation_stderrs_to_error_message
from yt.wrapper.spec_builders import (ReduceSpecBuilder, MergeSpecBuilder, SortSpecBuilder,
                                      MapReduceSpecBuilder, MapSpecBuilder, VanillaSpecBuilder)

import yt.wrapper as yt

from yt.packages.six.moves import xrange

import pytest
from flaky import flaky

from copy import deepcopy

class NonCopyable:
    def __init__(self, fun):
        self._fun = fun

    def __call__(self, *args, **kwargs):
        return self._fun(*args, **kwargs)

    def __deepcopy__(self, _memo):
        raise TypeError("not copyable")

    def __copy__(self):
        raise TypeError("not copyable")

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
        assert not yt.get_attribute(res_table, "sorted")
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
        assert yt.get_attribute(res_table, "sorted")
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
                .command("PYTHONPATH=. {} capitalize_b.py".format(get_python())) \
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

    # Remove flaky after YT-10347.
    @flaky(max_runs=5)
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
            files_in_cache = list(yt.search("//tmp/yt_wrapper/file_storage", node_type="file"))
            assert len(files_in_cache) > 0

            spec_builder = MapSpecBuilder() \
                .begin_mapper() \
                    .command(func) \
                    .format("json") \
                .end_mapper() \
                .input_table_paths(input) \
                .output_table_paths(output)
            yt.run_operation(spec_builder)
            files_in_cache_again = list(yt.search("//tmp/yt_wrapper/file_storage", node_type="file"))
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

        spec_defaults = {
            "mapper": {
                "memory_limit": 128 * 1024 * 1024,
                "cpu_limit": 0.5772156649,
                "environment": {
                    "var1": "1",
                    "var2": "2"}}}

        spec_overrides = {
            "mapper": {
                "memory_reserve_factor": 0.31,
                "environment": {
                    "var2": "5",
                    "var3": "6"}}}

        with set_config_option("spec_defaults", spec_defaults):
            with set_config_option("spec_overrides", spec_overrides):
                spec_builder = MapSpecBuilder() \
                    .begin_mapper() \
                    .command("cat") \
                    .format("json") \
                    .memory_limit(256 * 1024 * 1024) \
                    .end_mapper() \
                    .input_table_paths(input_table) \
                    .output_table_paths(output_table)

                op = yt.run_operation(spec_builder)
                attributes = op.get_attributes()
                assert attributes["spec"]["mapper"]["memory_limit"] == 256 * 1024 * 1024
                assert abs(attributes["spec"]["mapper"]["cpu_limit"] - 0.5772156649) < 1e-5
                assert abs(attributes["spec"]["mapper"]["memory_reserve_factor"] - 0.31) < 1e-5
                assert attributes["spec"]["mapper"]["environment"]["var1"] == "1"
                assert attributes["spec"]["mapper"]["environment"]["var2"] == "5"
                assert attributes["spec"]["mapper"]["environment"]["var3"] == "6"

    def test_user_job_spec_defaults(self):
        input_table = TEST_DIR + "/input"
        output_table = TEST_DIR + "/output"
        yt.write_table(yt.TablePath(input_table, sorted_by=["x"]), [{"x": 1}, {"x": 2, "y": 2}])

        user_job_spec_defaults = {
            "environment": {
                "MY_ENV": "value",
            }
        }

        with set_config_option("user_job_spec_defaults", user_job_spec_defaults):
            spec_builder = MapSpecBuilder() \
                .begin_mapper() \
                .command("cat") \
                .format("json") \
                .memory_limit(256 * 1024 * 1024) \
                .end_mapper() \
                .input_table_paths(input_table) \
                .output_table_paths(output_table)
            op = yt.run_operation(spec_builder)
            attributes = op.get_attributes()
            assert attributes["spec"]["mapper"]["memory_limit"] == 256 * 1024 * 1024
            assert attributes["spec"]["mapper"]["environment"]["MY_ENV"] == "value"

            spec_builder = ReduceSpecBuilder() \
                .begin_reducer() \
                .command("cat") \
                .format("json") \
                .memory_limit(256 * 1024 * 1024) \
                .environment({"OTHER_ENV": "10"}) \
                .end_reducer() \
                .reduce_by(["x"]) \
                .input_table_paths(input_table) \
                .output_table_paths(output_table)
            op = yt.run_operation(spec_builder)
            attributes = op.get_attributes()
            assert attributes["spec"]["reducer"]["memory_limit"] == 256 * 1024 * 1024
            assert attributes["spec"]["reducer"]["environment"]["MY_ENV"] == "value"
            assert attributes["spec"]["reducer"]["environment"]["OTHER_ENV"] == "10"

    def test_spec_deepcopy(self):
        def mapper(row):
            yield {"x": row["x"] + 5, "tag": row["tag"]}
        mapper = NonCopyable(mapper)

        def reducer(key, rows):
            total = sum(row["x"] for row in rows)
            yield {"total": total, "tag": key["tag"]}
        reducer = NonCopyable(reducer)

        with pytest.raises(TypeError):
            deepcopy(mapper)
        with pytest.raises(TypeError):
            deepcopy(reducer)

        input_table = TEST_DIR + "/input"
        output_table = TEST_DIR + "/output"
        yt.write_table(input_table, [
            {"x": 3, "tag": "Paris"},
            {"x": 2, "tag": "Paris"},
            {"x": 4, "tag": "London"},
            {"x": 8, "tag": "London"},
        ])

        spec_builder = MapReduceSpecBuilder() \
            .begin_mapper() \
                .command(mapper) \
            .end_mapper() \
            .begin_reducer() \
                .command(reducer) \
            .end_reducer() \
            .reduce_by("tag") \
            .input_table_paths(input_table) \
            .output_table_paths(output_table)

        yt.run_operation(spec_builder)
        check([
                {"tag": "Paris", "total": 15},
                {"tag": "London", "total": 22},
            ],
            list(yt.read_table(output_table)),
            ordered=False
        )

    def test_vanilla_spec_builder(self):
        vanilla_spec = VanillaSpecBuilder()\
            .begin_task("sample")\
                .command("cat")\
                .job_count(1)\
            .end_task()\
            .spec({"tasks": {"sample": {"memory_limit": 666 * 1024}}, "weight": 2})

        result_spec = vanilla_spec.build()
        correct_spec = {
            "tasks": {
                "sample": {
                    "command": "cat",
                    "job_count": 1,
                    "memory_limit": 666 * 1024,
                }
            },
            "weight": 2,
        }

        assert update(result_spec, correct_spec) == result_spec

    def test_vanilla_raw_spec_builder(self):
        with set_config_option("allow_http_requests_to_yt_from_job", True):
            spec_builder = VanillaSpecBuilder().spec({
                "tasks": {
                    "script": {
                        "command": "cat",
                        "job_count": 1,
                    }
                }
            })
            result_spec = spec_builder.build()

        correct_spec = {
            "tasks": {
                "script": {
                    "command": "cat",
                    "job_count": 1,
                    "environment": {
                        "YT_ALLOW_HTTP_REQUESTS_TO_YT_FROM_JOB": "1",
                    }
                }
            }
        }

        assert update(result_spec, correct_spec) == result_spec

    def test_local_file_attributes(self):
        vanilla_spec = VanillaSpecBuilder()\
            .begin_task("sample")\
                .command("cat")\
                .job_count(1)\
                .file_paths(yt.LocalFile(get_test_file_path("capitalize_b.py"), attributes={"bypass_artifacts_cache": True}))\
            .end_task()\
            .spec({"tasks": {"sample": {"memory_limit": 666 * 1024}}, "weight": 2})

        result_spec = vanilla_spec.build()
        assert result_spec["tasks"]["sample"]["file_paths"][0].attributes == {"bypass_artifacts_cache": True, "file_name": "capitalize_b.py", "executable": True}
