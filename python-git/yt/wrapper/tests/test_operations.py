from __future__ import print_function

from .helpers import (TEST_DIR, get_test_file_path, check, set_config_option, get_tests_sandbox,
                      ENABLE_JOB_CONTROL, dumps_yt_config, get_python, wait, get_operation_path)

# Necessary for tests.
try:
    import yt.wrapper.tests.test_module
    has_test_module = True
except ImportError:
    has_test_module = False

from yt.wrapper.py_wrapper import create_modules_archive_default, TempfilesManager, TMPFS_SIZE_MULTIPLIER
from yt.wrapper.common import get_disk_size, MB
from yt.wrapper.operation_commands import add_failed_operation_stderrs_to_error_message, get_jobs_with_error_or_stderr, get_operation_error
from yt.wrapper.table import TablePath
from yt.wrapper.spec_builders import MapSpecBuilder, MapReduceSpecBuilder, VanillaSpecBuilder
from yt.wrapper.skiff import convert_to_skiff_schema

from yt.test_helpers import are_almost_equal

from yt.yson import YsonMap, YsonList
from yt.skiff import SkiffTableSwitch
import yt.logger as logger
import yt.subprocess_wrapper as subprocess

from yt.local import start, stop

from yt.packages.six import b, PY3
from yt.packages.six.moves import xrange, zip as izip

import yt.wrapper as yt

from flaky import flaky

import io
import logging
import os
import pytest
import random
import signal
import string
import sys
import tempfile
import time
import uuid

class AggregateMapper(object):
    def __init__(self):
        self.sum = 0

    def __call__(self, row):
        self.sum += int(row["x"])

    def finish(self):
        yield {"sum": self.sum}

class AggregateReducer(object):
    def __init__(self):
        self.sum = 0

    def start(self):
        for i in [1, 2]:
            yield {"sum": i}

    def __call__(self, key, rows):
        for row in rows:
            self.sum += int(row["y"])

    def finish(self):
        yield {"sum": self.sum}

class CreateModulesArchive(object):
    def __call__(self, tempfiles_manager=None, custom_python_used=False):
        return create_modules_archive_default(tempfiles_manager, custom_python_used, None)

@pytest.mark.usefixtures("yt_env_with_rpc")
class TestOperations(object):
    def setup(self):
        yt.config["tabular_data_format"] = yt.format.JsonFormat()
        self.env = {
            "YT_CONFIG_PATCHES": dumps_yt_config(),
            "PYTHONPATH": os.environ.get("PYTHONPATH", "")
        }

    def teardown(self):
        yt.config["tabular_data_format"] = None
        yt.remove("//tmp/yt_wrapper/file_storage", recursive=True, force=True)

    def random_string(self, length):
        char_set = string.ascii_lowercase + string.digits + string.ascii_uppercase
        return "".join(random.sample(char_set, length))

    def test_merge(self):
        tableX = TEST_DIR + "/tableX"
        tableY = TEST_DIR + "/tableY"
        tableZ = TEST_DIR + "/tableZ"
        dir = TEST_DIR + "/dir"
        res_table = dir + "/other_table"

        yt.write_table(tableX, [{"x": 1}])
        yt.write_table(tableY, [{"y": 2}])
        yt.write_table(tableZ, [{"x": 0}, {"x": 2}])

        with pytest.raises(yt.YtError):
            yt.run_merge([tableX, tableY], res_table)
        with pytest.raises(yt.YtError):
            yt.run_merge([tableX, tableY], res_table)

        yt.mkdir(dir)
        yt.run_merge([tableX, tableY], res_table)
        check([{"x": 1}, {"y": 2}], yt.read_table(res_table), ordered=False)

        yt.run_merge(tableX, res_table)
        assert not yt.get_attribute(res_table, "sorted")
        check([{"x": 1}], yt.read_table(res_table))

        yt.run_sort(tableX, sort_by="x")
        yt.run_merge(tableX, res_table)
        assert yt.get_attribute(res_table, "sorted")
        check([{"x": 1}], yt.read_table(res_table))

        # Test mode="auto"
        yt.run_sort(tableZ, sort_by="x")
        yt.run_merge([tableX, tableZ], res_table)
        check([{"x": 0}, {"x": 1}, {"x": 2}], yt.read_table(res_table))

        # XXX(asaitgalin): Uncomment when st/YT-5770 is done.
        # yt.run_merge(yt.TablePath(tableX, columns=["y"]), res_table)
        # assert not parse_bool(yt.get_attribute(res_table, "sorted"))
        # check([{}], yt.read_table(res_table))

    def test_auto_merge(self):
        table = TEST_DIR + "/table"
        other_table = TEST_DIR + "/other_table"
        yt.write_table(table, [{"x": i} for i in xrange(6)])

        old_auto_merge_output = yt.config["auto_merge_output"]

        yt.config["auto_merge_output"]["min_chunk_count"] = 2
        yt.config["auto_merge_output"]["max_chunk_size"] = 5 * 1024
        try:
            yt.config["auto_merge_output"]["action"] = "none"
            yt.run_map("cat", table, other_table, job_count=6)
            assert yt.get_attribute(other_table, "chunk_count") == 6
            yt.config["auto_merge_output"]["action"] = "merge"
            yt.run_map("cat", table, other_table, job_count=6)
            assert yt.get_attribute(other_table, "chunk_count") == 1
        finally:
            yt.config["auto_merge_output"].update(old_auto_merge_output)

    def test_sort(self):
        table = TEST_DIR + "/table"
        other_table = TEST_DIR + "/other_table"

        columns = [(self.random_string(7), self.random_string(7)) for _ in xrange(10)]
        yt.write_table(table, [b("x={0}\ty={1}\n".format(*c)) for c in columns], format=yt.DsvFormat(), raw=True)

        with pytest.raises(yt.YtError):
            yt.run_sort([table, other_table], other_table, sort_by=["y"])

        yt.run_sort(table, other_table, sort_by=["x"])
        assert [{"x": x, "y": y} for x, y in sorted(columns, key=lambda c: c[0])] == list(yt.read_table(other_table))

        yt.run_sort(table, sort_by=["x"])
        assert list(yt.read_table(table)) == list(yt.read_table(other_table))

        # Sort again and check that everything is ok
        yt.run_sort(table, sort_by=["x"])
        assert list(yt.read_table(table)) == list(yt.read_table(other_table))

        yt.run_sort(table, sort_by=["y"])
        assert [{"x": x, "y": y} for x, y in sorted(columns, key=lambda c: c[1])] == list(yt.read_table(table))

        assert yt.is_sorted(table)

        with pytest.raises(yt.YtError):
            yt.run_sort(table, sort_by=None)

    def test_run_operation(self):
        table = TEST_DIR + "/table"
        other_table = TEST_DIR + "/other_table"
        yt.write_table(table, [{"x": 1}, {"x": 2}])

        yt.run_map("cat", table, table)
        check([{"x": 1}, {"x": 2}], list(yt.read_table(table)), ordered=False)
        yt.run_sort(table, sort_by=["x"])
        with pytest.raises(yt.YtError):
            yt.run_reduce("cat", table, [], reduce_by=["x"])

        yt.run_reduce("cat", table, table, reduce_by=["x"])
        check([{"x": 1}, {"x": 2}], yt.read_table(table))

        with pytest.raises(yt.YtError):
            yt.run_map("cat", table, table, table_writer={"max_row_weight": 1})

        yt.run_map("grep 2", table, other_table)
        check([{"x": 2}], yt.read_table(other_table))

        with pytest.raises(yt.YtError):
            yt.run_map("cat", [table, table + "xxx"], other_table)

        with pytest.raises(yt.YtError):
            yt.run_reduce("cat", table, other_table, reduce_by=None)

        # Run reduce on unsorted table
        with pytest.raises(yt.YtError):
            yt.run_reduce("cat", other_table, table, reduce_by=["x"])

        yt.write_table(table,
                       [
                           {"a": 12, "b": "ignat"},
                                     {"b": "max"},
                           {"a": "x", "b": "name", "c": 0.5}
                       ])
        operation = yt.run_map("PYTHONPATH=. {} capitalize_b.py".format(get_python()),
                               TablePath(table, columns=["b"]), other_table,
                               local_files=get_test_file_path("capitalize_b.py"),
                               format=yt.DsvFormat())
        records = yt.read_table(other_table, raw=False)
        assert sorted([rec["b"] for rec in records]) == ["IGNAT", "MAX", "NAME"]
        assert sorted([rec["c"] for rec in records]) == []
        assert get_operation_error(operation.id) is None

    def test_stderr_table(self):
        table = TEST_DIR + "/table"
        other_table = TEST_DIR + "/other_table"
        yt.write_table(TablePath(table, sorted_by=["x"]), [{"x": 1}, {"x": 2}])

        stderr_table = TEST_DIR + "/stderr_table"

        yt.run_map("echo map >&2 ; cat", table, other_table, stderr_table=stderr_table)
        row_list = list(yt.read_table(stderr_table, raw=False))
        assert len(row_list) > 0
        assert yt.has_attribute(stderr_table, "part_size")
        for r in row_list:
            assert r["data"] == "map\n"


        yt.run_reduce("echo reduce >&2 ; cat",
                      table, other_table, stderr_table=stderr_table,
                      reduce_by=["x"])
        row_list = list(yt.read_table(stderr_table, raw=False))
        assert len(row_list) > 0
        assert yt.has_attribute(stderr_table, "part_size")
        for r in row_list:
            assert r["data"] == "reduce\n"

        yt.run_map_reduce(
            "echo mr-map >&2 ; cat",
            "echo mr-reduce >&2 ; cat",
            table, other_table,
            stderr_table=stderr_table, reduce_by=["x"])

        row_list = list(yt.read_table(stderr_table, raw=False))
        assert len(row_list) > 0
        assert yt.has_attribute(stderr_table, "part_size")
        for r in row_list:
            assert r["data"] in ["mr-map\n", "mr-reduce\n"]

    def test_stderr_table_inside_transaction(self):
        table = TEST_DIR + "/table"
        other_table = TEST_DIR + "/other_table"
        yt.write_table(TablePath(table, sorted_by=["x"]), [{"x": 1}, {"x": 2}])

        stderr_table = TEST_DIR + "/stderr_table"
        try:
            with yt.Transaction():
                yt.run_map("echo map >&2 ; cat", table, other_table, stderr_table=stderr_table)
                raise RuntimeError
        except RuntimeError:
            pass

        assert not yt.exists(other_table)

        # We expect stderr to be saved nevertheless.
        row_list = list(yt.read_table(stderr_table, raw=False))
        assert len(row_list) > 0
        assert yt.has_attribute(stderr_table, "part_size")
        for r in row_list:
            assert r["data"] == "map\n"

    @add_failed_operation_stderrs_to_error_message
    def test_run_join_operation(self):
        table1 = TEST_DIR + "/first"
        yt.write_table("<sorted_by=[x]>" + table1, [{"x": 1}])
        table2 = TEST_DIR + "/second"
        yt.write_table("<sorted_by=[x]>" + table2, [{"x": 2}])
        unsorted_table = TEST_DIR + "/unsorted_table"
        yt.write_table(unsorted_table, [{"x": 3}])
        table = TEST_DIR + "/table"

        yt.run_join_reduce("cat", ["<primary=true>" + table1, table2], table, join_by=["x"])
        check([{"x": 1}], yt.read_table(table))

        # Run join-reduce without join_by
        with pytest.raises(yt.YtError):
            yt.run_join_reduce("cat", ["<primary=true>" + table1, table2], table)

        # Run join-reduce on unsorted table
        with pytest.raises(yt.YtError):
            yt.run_join_reduce("cat", ["<primary=true>" + unsorted_table, table2], table, join_by=["x"])

        yt.run_join_reduce("cat", [table1, "<foreign=true>" + table2], table, join_by=["x"])
        check([{"x": 1}], yt.read_table(table))

        # Run join-reduce without join_by
        with pytest.raises(yt.YtError):
            yt.run_join_reduce("cat", [table1, "<foreign=true>" + table2], table)

        # Run join-reduce on unsorted table
        with pytest.raises(yt.YtError):
            yt.run_join_reduce("cat", [unsorted_table, "<foreign=true>" + table2], table, join_by=["x"])

        yt.write_table("<sorted_by=[x;y]>" + table1, [{"x": 1, "y": 1}])
        yt.write_table("<sorted_by=[x]>" + table2, [{"x": 1}])

        def func(key, rows):
            assert list(key) == ["x"]
            for row in rows:
                yield row

        yt.run_reduce(func, [table1, "<foreign=true>" + table2], table,
                      reduce_by=["x", "y"], join_by=["x"],
                      format=yt.YsonFormat())
        check([{"x": 1, "y": 1}, {"x": 1}], yt.read_table(table))

        # Reduce with join_by, but without foreign tables
        with pytest.raises(yt.YtError):
            yt.run_reduce("cat", [table1, table2], table, join_by=["x"])

    @add_failed_operation_stderrs_to_error_message
    def test_vanilla_operation(self):
        def check(op):
            job_infos = op.get_jobs_with_error_or_stderr()
            assert len(job_infos) == 1
            assert job_infos[0]["stderr"] == "aaa\n"

        op = yt.run_operation(VanillaSpecBuilder().task("sample", {"command": "echo 'aaa' >&2", "job_count": 1}))
        check(op)

        vanilla_spec = VanillaSpecBuilder().tasks({"sample": {"command": "echo 'aaa' >&2", "job_count": 1}})
        op = yt.run_operation(vanilla_spec)
        check(op)

        vanilla_spec = VanillaSpecBuilder().begin_task("sample")\
                .command("echo 'aaa' >&2")\
                .job_count(1)\
            .end_task()
        op = yt.run_operation(vanilla_spec)
        check(op)

    @add_failed_operation_stderrs_to_error_message
    def test_many_output_tables(self):
        table = TEST_DIR + "/table"
        output_tables = []
        for i in xrange(10):
            output_tables.append(TEST_DIR + "/temp%d" % i)
        append_table = TEST_DIR + "/temp_special"
        yt.write_table(table, [{"x": "1", "y": "1"}])
        yt.write_table(append_table, [{"x": "1", "y": "1"}])

        yt.run_map("PYTHONPATH=. {} many_output.py yt".format(get_python()),
                   table,
                   output_tables + [TablePath(append_table, append=True)],
                   local_files=get_test_file_path("many_output.py"),
                   format=yt.DsvFormat())

        for table in output_tables:
            assert yt.row_count(table) == 1
        check([{"x": "1", "y": "1"}, {"x": "10", "y": "10"}], yt.read_table(append_table), ordered=False)

    def test_attached_mode_simple(self):
        table = TEST_DIR + "/table"

        yt.config["detached"] = 0
        try:
            yt.write_table(table, [{"x": 1}])
            yt.run_map("cat", table, table)
            check(yt.read_table(table), [{"x": 1}])
            yt.run_merge(table, table)
            check(yt.read_table(table), [{"x": 1}])
        finally:
            yt.config["detached"] = 1

    def test_attached_mode_op_aborted(self, yt_env_with_rpc):
        script = """
from __future__ import print_function

import yt.wrapper as yt
import sys

input, output = sys.argv[1:3]
yt.config["proxy"]["request_timeout"] = 5000
yt.config["proxy"]["retries"]["count"] = 1
yt.config["detached"] = False
op = yt.run_map("sleep 1000", input, output, format="json", sync=False)
print(op.id)
"""
        dir_ = yt_env_with_rpc.env.path
        with tempfile.NamedTemporaryFile(mode="w", dir=dir_, prefix="mapper", delete=False) as file:
            file.write(script)

        table = TEST_DIR + "/table"
        yt.write_table(table, [{"x": 1}])

        op_id = subprocess.check_output([get_python(), file.name, table, table],
                                        env=self.env, stderr=sys.stderr).strip()
        wait(lambda: yt.get(get_operation_path(op_id) + "/@state") == "aborted")

    def test_abort_operation(self):
        table = TEST_DIR + "/table"
        yt.write_table(table, [{"x": 1}])
        op = yt.run_map("sleep 15; cat", table, table, sync=False)
        op.abort()
        assert op.get_state() == "aborted"

    def test_complete_operation(self):
        table = TEST_DIR + "/table"
        yt.write_table(table, [{"x": 1}])
        op = yt.run_map("sleep 15; cat", table, table, sync=False)
        while not op.get_state().is_running():
            time.sleep(0.2)
        op.complete()
        assert op.get_state() == "completed"
        op.complete()

    def test_suspend_resume(self):
        table = TEST_DIR + "/table"
        yt.write_table(table, [{"key": 1}])
        try:
            op = yt.run_map_reduce(
                "sleep 0.5; cat",
                "sleep 0.5; cat",
                table,
                table,
                sync=False,
                reduce_by=["key"],
                spec={"map_locality_timeout": 0, "reduce_locality_timeout": 0})

            wait(lambda: op.get_state() == "running")

            op.suspend()
            assert op.get_state() == "running"
            time.sleep(2.5)
            assert op.get_state() == "running"
            op.resume()
            op.wait(timeout=60 * 1000)
            assert op.get_state() == "completed"
        finally:
            if op.get_state() not in ["completed", "failed", "aborted"]:
                op.abort()

    def test_reduce_combiner(self):
        table = TEST_DIR + "/table"
        output_table = TEST_DIR + "/output_table"
        yt.write_table(table, [{"x": 1}, {"y": 2}])

        yt.run_map_reduce(mapper=None, reduce_combiner="cat", reducer="cat", reduce_by=["x"],
                          source_table=table, destination_table=output_table)
        check([{"x": 1}, {"y": 2}], list(yt.read_table(table)))

    def test_reduce_differently_sorted_table(self):
        table = TEST_DIR + "/table"
        other_table = TEST_DIR + "/other_table"
        yt.create("table", table)
        yt.run_sort(table, sort_by=["a", "b"])

        with pytest.raises(yt.YtError):
            # No reduce_by
            yt.run_reduce("cat", source_table=table, destination_table=other_table, sort_by=["a"])

        with pytest.raises(yt.YtError):
            yt.run_reduce("cat", source_table=table, destination_table=other_table, reduce_by=["c"])

    def test_reduce_sort_by(self):
        table = TEST_DIR + "/table"
        yt.write_table(table, [{"x": 1, "y": 1}])
        yt.run_sort(table, sort_by=["x", "y"])
        op = yt.run_reduce("cat", table, table, format=yt.JsonFormat(), reduce_by=["x"], sort_by=["x", "y"])
        assert "sort_by" in op.get_attributes()["spec"]

    def test_remote_copy(self):
        mode = yt.config["backend"]
        if mode == "http":
            mode = yt.config["api_version"]

        test_name = "TestYtWrapper" + mode.capitalize()
        dir = os.path.join(get_tests_sandbox(), test_name)
        id = "run_" + uuid.uuid4().hex[:8]
        instance = None
        try:
            instance = start(path=dir, id=id, node_count=3, enable_debug_logging=True, cell_tag=1)
            second_cluster_client = instance.create_client()
            second_cluster_connection = second_cluster_client.get("//sys/@cluster_connection")
            second_cluster_client.create("map_node", TEST_DIR)
            table = TEST_DIR + "/test_table"

            second_cluster_client.write_table(table, [{"a": 1, "b": 2, "c": 3}])
            yt.run_remote_copy(table, table, cluster_connection=second_cluster_connection)
            assert list(yt.read_table(table)) == [{"a": 1, "b": 2, "c": 3}]

            second_cluster_client.write_table(table, [{"a": 1, "b": True, "c": "abacaba"}])
            yt.run_remote_copy(table, table, cluster_connection=second_cluster_connection)
            assert list(yt.read_table(table)) == [{"a": 1, "b": True, "c": "abacaba"}]

            second_cluster_client.write_table(table, [])
            yt.run_remote_copy(table, table, cluster_connection=second_cluster_connection)
            assert list(yt.read_table(table)) == []

            second_cluster_client.remove(table)
            second_cluster_client.create("table", table, attributes={"compression_codec": "zlib_6"})

            second_cluster_client.write_table(table, [{"a": [i, 2, 3]} for i in xrange(100)])
            yt.run_remote_copy(table, table, cluster_connection=second_cluster_connection)
            assert list(yt.read_table(table)) == [{"a": [i, 2, 3]} for i in xrange(100)]
            assert second_cluster_client.get(table + "/@compressed_data_size") == \
                   yt.get(table + "/@compressed_data_size")

        finally:
            if instance is not None:
                stop(instance.id, path=dir)

    @add_failed_operation_stderrs_to_error_message
    def test_shuffle(self):
        table = TEST_DIR + "/table"
        yt.write_table(table, [{"x": 0}, {"x": 1}, {"x": 2}])
        yt.shuffle_table(table)
        assert len(list(yt.read_table(table))) == 3

        table = TEST_DIR + "/table"
        yt.remove(table)
        yt.create("table", table, attributes={"schema": [{"name": "x", "type": "int64", "sort_order": "ascending", "required": True}]})
        yt.write_table(table, [{"x": 0}, {"x": 1}, {"x": 2}])
        yt.shuffle_table(table)
        assert len(list(yt.read_table(table))) == 3

    def test_stderr_decoding(self):
        input_table = TEST_DIR + "/input"
        output_table = TEST_DIR + "/output"
        yt.write_table(input_table, [{"x": 0}, {"x": 1}, {"x": 2}])
        with set_config_option("operation_tracker/stderr_encoding", "latin1"):
            op = yt.run_map("echo -e -n '\\xF1\\xF2\\xF3\\xF4' >&2; cat", input_table, output_table)
            job_infos = op.get_jobs_with_error_or_stderr()
            assert len(job_infos) == 1
            assert job_infos[0]["stderr"] == "\xF1\xF2\xF3\xF4"

    def test_operation_alert(self):
        try:
            config_patch = {
                "operation_alerts": {
                    "operation_too_long_alert_min_wall_time": 0,
                    "operation_too_long_alert_estimate_duration_threshold": 5000
                }
            }

            path = "//sys/controller_agents/instances"
            children = yt.list(path)
            assert len(children) == 1
            path = "//sys/controller_agents/instances/{}/orchid/controller_agent/config".format(children[0])

            old_config = yt.get(path)

            yt.set("//sys/controller_agents/config", config_patch)

            wait(lambda: yt.get(path) != old_config)

            input_table = TEST_DIR + "/input"
            output_table = TEST_DIR + "/output"
            yt.write_table(input_table, [{"x": i} for i in xrange(100)])

            op = yt.run_map("cat; sleep 120", input_table, output_table, spec={"data_size_per_job": 1}, sync=False)
            wait(lambda: op.get_attributes(fields=["alerts"]).get("alerts", {}))
        finally:
            yt.remove("//sys/controller_agents/config", recursive=True, force=True)


    def test_operations_spec(self):
        input_table = TEST_DIR + "/input"
        output_table = TEST_DIR + "/output"
        yt.write_table(input_table, [{"x": 1}])

        op = yt.run_map("cat; sleep 120", input_table, output_table, spec={"asdfghjkl" : 1234567890}, sync=False)
        wait(lambda: op.get_attributes(fields=["unrecognized_spec"]).get("unrecognized_spec", {}))

    def test_map_without_output(self, tmpdir):
        input_table = TEST_DIR + "/input"
        yt.write_table(input_table, [{"x": 1}, {"x": 2}, {"x": 3}])
        first_file = str(tmpdir.join("first.txt"))
        second_file = str(tmpdir.join("second.txt"))

        def mapper_without_output(row):
            with open(first_file, "a+") as f:
                f.write("{}\n".format(row["x"]))

        def mapper_with_output(row):
            with open(second_file, "a+") as f:
                f.write("{}\n".format(row["x"]))
            yield 10
            yield 20
            yield 30

        for path, mapper in zip([first_file, second_file], [mapper_without_output, mapper_with_output]):
            open(path, "w").close()
            os.chmod(path, 0o666)
            yt.run_map(mapper, input_table)
            with open(path, "r") as f:
                data = f.read()
            assert set(data.split()) == {"1", "2", "3"}

    def test_reduce_without_output(self, tmpdir):
        input_table = TEST_DIR + "/input"
        yt.write_table(input_table, [
            {"x": 1, "y": 150},
            {"x": 1, "y": 250},
            {"x": 1, "y": 300},
        ])
        yt.run_sort(input_table, input_table, sort_by=["x"])
        first_file = str(tmpdir.join("first.txt"))
        second_file = str(tmpdir.join("second.txt"))

        def reducer_without_output(key, rows):
            y = sum(row["y"] for row in rows)
            with open(first_file, "a") as f:
                f.write("{}\n".format(y))

        def reducer_with_output(key, rows):
            y = sum(row["y"] for row in rows)
            with open(second_file, "a") as f:
                f.write("{}\n".format(y))
            yield 10
            yield 20
            yield 30

        for path, reducer in zip([first_file, second_file], [reducer_without_output, reducer_with_output]):
            open(path, "w").close()
            os.chmod(path, 0o666)
            yt.run_reduce(reducer, input_table, reduce_by="x")
            with open(path, "r") as f:
                data = f.read()
            assert int(data.strip()) == 700

    def test_retrying_operation_count_limit_exceeded(self):
        # TODO(ignat): Rewrite test without sleeps.
        old_value = yt.config["start_operation_request_timeout"]
        yt.config["start_operation_request_timeout"] = 2000

        yt.create("map_node", "//sys/pools/with_operation_count_limit", attributes={"max_operation_count": 1})
        time.sleep(1)

        try:
            table = TEST_DIR + "/table"
            yt.write_table(table, [{"x": 1}, {"x": 2}])

            def run_operation(index):
                return yt.run_map(
                    "cat; sleep 5",
                    table,
                    TEST_DIR + "/output_" + str(index),
                    # File is used to test toucher.
                    local_files=get_test_file_path("capitalize_b.py"),
                    sync=False,
                    spec={"pool": "with_operation_count_limit"})

            ops = []
            start_time = time.time()
            ops.append(run_operation(1))
            assert time.time() - start_time < 5.0
            ops.append(run_operation(2))
            assert time.time() - start_time > 5.0

            for op in ops:
                op.wait()

            assert time.time() - start_time > 10.0

        finally:
            yt.config["start_operation_request_timeout"] = old_value

    def test_enable_logging_failed_operation(self):
        tableX = TEST_DIR + "/tableX"
        tableY = TEST_DIR + "/tableY"
        yt.write_table(tableX, [{"x": 1}])
        with set_config_option("operation_tracker/enable_logging_failed_operation", True):
            with pytest.raises(yt.YtError):
                yt.run_map("cat; echo 'Hello %username%!' >&2; exit 1", tableX, tableY)

    def test_get_operation_command(self):
        table = TEST_DIR + "/table"
        yt.write_table(table, [{"x": 1}, {"x": 2}])

        with set_config_option("enable_operations_api", True):
            op = yt.run_map("cat; echo 'AAA' >&2", table, table)
            check([{"x": 1}, {"x": 2}], list(yt.read_table(table)), ordered=False)

            assert op.get_state() == "completed"

            assert op.get_progress()["total"] == 1
            assert op.get_progress()["completed"] == 1

            op.get_job_statistics()

            job_infos = op.get_jobs_with_error_or_stderr()
            assert len(job_infos) == 1
            assert job_infos[0]["stderr"] == "AAA\n"

    def test_list_operations(self):
        if yt.config["backend"] == "rpc":
            pytest.skip()

        assert yt.list_operations()["operations"] == []

        table = TEST_DIR + "/table"
        yt.write_table(table, [{"x": "0"}])
        yt.run_map("cat; echo 'AAA' >&2", table, table)

        operations = yt.list_operations()["operations"]
        assert len(operations) == 1

        operation = operations[0]
        assert operation["state"] == "completed"
        assert operation["type"] == "map"

    def test_iterate_operations(self):
        if yt.config["backend"] == "rpc":
            pytest.skip()

        assert list(yt.iterate_operations()) == []

        table = "//tmp/table"
        operation_count = 12
        table_paths = [0] * operation_count
        for i in xrange(operation_count):
            table_paths[i] = table + '_' + str(i)

        for i in xrange(operation_count):
            yt.write_table(table_paths[i], [{"x": "0"}])

        ops = [0] * operation_count
        for i in xrange(operation_count):
            ops[i] = yt.run_map("cat", table_paths[i], table_paths[i], sync=False, format="yson")
        start_times = [0] * operation_count
        for i in xrange(operation_count):
            ops[i].wait()
            start_times[i] = ops[i].get_attributes(["start_time"])["start_time"]
        start_times.sort()

        operations = list(yt.iterate_operations(limit_per_request=5))
        assert len(operations) == operation_count

        past_to_future = list(yt.iterate_operations(from_time=start_times[0], to_time=start_times[-1], cursor_direction="future"))
        future_to_past = list(yt.iterate_operations(from_time=start_times[0], to_time=start_times[-1], cursor_direction="past"))

        assert past_to_future == future_to_past[::-1]
        assert future_to_past == operations[1:]

    def test_empty_job_command(self):
        table = TEST_DIR + "/table"
        output_table = TEST_DIR + "/output_table"
        yt.write_table(table, [{"x": 1}, {"y": 2}])

        spec = {"mapper": {"copy_files": True}, "reduce_combiner": {"copy_files": True}}
        yt.run_map_reduce(mapper=None, reduce_combiner="cat", reducer="cat", reduce_by=["x"],
                          source_table=table, destination_table=output_table, spec=spec)
        check([{"x": 1}, {"y": 2}], list(yt.read_table(table)))
        yt.run_map_reduce(mapper=None, reducer="cat", reduce_by=["x"],
                          source_table=table, destination_table=output_table, spec=spec)
        check([{"x": 1}, {"y": 2}], list(yt.read_table(table)))

    def test_update_operation_parameters(self):
        if "update_op_parameters" not in yt.driver.get_command_list():
            pytest.skip()

        table = TEST_DIR + "/table"
        output_table = TEST_DIR + "/output_table"
        yt.write_table(table, [{"x": 1}, {"y": 2}])

        op = yt.run_map("cat; sleep 100", table, output_table, spec={"weight": 5.0}, format="json", sync=False)
        wait(lambda: op.get_state() == "running")
        yt.update_operation_parameters(op.id, {"scheduling_options_per_pool_tree": {"default": {"weight": 10.0}}})
        wait(lambda: are_almost_equal(
            yt.get_operation(op.id, include_scheduler=True)["progress"]["scheduling_info_per_pool_tree"]["default"]["weight"],
            10.0))


@pytest.mark.usefixtures("yt_env_with_rpc")
class TestOperationsFormat(object):
    def setup(self):
        yt.config["tabular_data_format"] = yt.format.JsonFormat()
        self.env = {
            "YT_CONFIG_PATCHES": dumps_yt_config(),
            "PYTHONPATH": os.environ.get("PYTHONPATH", "")
        }

    def teardown(self):
        yt.config["tabular_data_format"] = None
        yt.remove("//tmp/yt_wrapper/file_storage", recursive=True, force=True)

    @add_failed_operation_stderrs_to_error_message
    def test_yamred_dsv(self):
        def foo(rec):
            yield rec

        table = TEST_DIR + "/table"
        yt.write_table(table, [{"x": "1", "y": "2"}])

        yt.run_map(foo, table, table,
                   input_format=yt.create_format("<key_column_names=[\"y\"]>yamred_dsv"),
                   output_format=yt.YamrFormat(has_subkey=False, lenval=False))
        check([{"key": "2", "value": "x=1"}], list(yt.read_table(table)))

    def test_schemaful_dsv(self):
        def foo(rec):
            yield rec

        table = TEST_DIR + "/table"
        yt.write_table(table, [b"x=1\ty=2\n", b"x=\\n\tz=3\n"], raw=True, format=yt.DsvFormat())
        check([b"1\n", b"\\n\n"],
              sorted(list(yt.read_table(table, format=yt.SchemafulDsvFormat(columns=["x"]), raw=True))))

        yt.run_map(foo, table, table, format=yt.SchemafulDsvFormat(columns=["x"]))
        check([b"x=1\n", b"x=\\n\n"], sorted(list(yt.read_table(table, format=yt.DsvFormat(), raw=True))))

    @add_failed_operation_stderrs_to_error_message
    def test_schemaful_dsv_several_output_tables(self):
        def mapper(row):
            row["@table_index"] = int(row["x"])
            yield row

        table = TEST_DIR + "/table"
        yt.write_table(table, [{"x": "0"}, {"x": "1"}, {"x": "2"}])

        output_tables = [TEST_DIR + "/output_" + str(i) for i in xrange(4)]

        yt.run_map(mapper, table, output_tables,
                   format=yt.SchemafulDsvFormat(columns=["x"], enable_table_index=True),
                   spec={"mapper": {"enable_input_table_index": True}})

        assert list(yt.read_table(output_tables[0])) == [{"x": "0"}]
        assert list(yt.read_table(output_tables[1])) == [{"x": "1"}]
        assert list(yt.read_table(output_tables[2])) == [{"x": "2"}]
        assert list(yt.read_table(output_tables[3])) == []

        yt.write_table(table, [{"x": "0"}, {"x": "5"}, {"x": "6"}])
        with pytest.raises(yt.YtError):
            yt.run_map(mapper, table, output_tables,
                       format=yt.SchemafulDsvFormat(columns=["x"], enable_table_index=True),
                       spec={"mapper": {"enable_input_table_index": True}})

    def test_lazy_yson(self):
        def mapper(row):
            assert not isinstance(row, (YsonMap, dict))
            row["z"] = row["y"] + 1
            yield row

        def reducer(key, rows):
            result = {"x": key["x"], "res": 0}
            for row in rows:
                assert not isinstance(row, (YsonMap, dict))
                result["res"] += row["z"]
            yield result

        table = TEST_DIR + "/table"
        output_table = TEST_DIR + "/table"
        yt.write_table(table, [{"x": 1, "y": 2}, {"x": 1, "y": 3}, {"x": 3, "y": 4}])
        yt.run_map_reduce(mapper, reducer, table, output_table, format="<lazy=%true>yson", reduce_by="x")

        assert list(yt.read_table(output_table)) == [{"x": 1, "res": 7}, {"x": 3, "res": 5}]


@pytest.mark.usefixtures("yt_env_with_rpc")
class TestPythonOperations(object):
    def setup(self):
        yt.config["tabular_data_format"] = yt.format.JsonFormat()
        self.env = {
            "YT_CONFIG_PATCHES": dumps_yt_config(),
            "PYTHONPATH": os.environ.get("PYTHONPATH", "")
        }

    def teardown(self):
        yt.config["tabular_data_format"] = None
        yt.remove("//tmp/yt_wrapper/file_storage", recursive=True, force=True)

    @add_failed_operation_stderrs_to_error_message
    def test_python_operations_common(self):
        def change_x(rec):
            if "x" in rec:
                rec["x"] = int(rec["x"]) + 1
            yield rec

        def sum_y(key, recs):
            sum = 0
            for rec in recs:
                sum += int(rec.get("y", 1))
            yield {"x": key["x"], "y": sum}

        def sum_y_bytes(key, recs):
            sum = 0
            for rec in recs:
                sum += int(rec.get(b"y", 1))
            yield {b"x": key[b"x"], b"y": sum}

        @yt.raw
        def change_field(line):
            yield b"z=8\n"

        @yt.aggregator
        def sum_x(recs):
            sum = 0
            for rec in recs:
                sum += int(rec.get("x", 0))
            yield {"sum": sum}

        @yt.raw_io
        def sum_x_raw():
            sum = 0
            for line in sys.stdin:
                x = line.strip().split("=")[1]
                sum += int(x)
            sys.stdout.write("sum={0}\n".format(sum))

        table = TEST_DIR + "/table"

        yt.write_table(table, [{"x": 1}, {"y": 2}])
        yt.run_map(change_x, table, table, format=None, spec={"enable_core_dump": True})
        check(yt.read_table(table), [{"x": 2}, {"y": 2}], ordered=False)

        yt.write_table(table, [{"x": 1}, {"y": 2}])
        yt.run_map(change_x, table, table)
        check(yt.read_table(table), [{"x": 2}, {"y": 2}])

        yt.write_table(table, [{"x": 2}, {"x": 2, "y": 2}])
        yt.run_sort(table, sort_by=["x"])
        yt.run_reduce(sum_y, table, table, reduce_by=["x"])
        check(yt.read_table(table), [{"y": 3, "x": 2}], ordered=False)

        if PY3:
            yt.write_table(table, [{"x": 2}, {"x": 2, "y": 2}])
            yt.run_sort(table, sort_by=[b"x"])
            yt.run_reduce(sum_y_bytes, table, table, reduce_by=[b"x"], format=yt.YsonFormat(encoding=None))
            check(yt.read_table(table), [{"y": 3, "x": 2}], ordered=False)

        yt.write_table(table, [{"x": "1"}, {"y": "2"}])
        yt.run_map(change_field, table, table, format=yt.DsvFormat())
        check(yt.read_table(table), [{"z": "8"}, {"z": "8"}])

        yt.write_table(table, [{"x": 1}, {"x": 2}, {"x": 3}])
        yt.run_map(sum_x, table, table)
        check(yt.read_table(table), [{"sum": 6}])

        yt.write_table(table, [{"x": "3"}] * 3)
        yt.run_map(sum_x_raw, table, table, format=yt.DsvFormat())
        check(yt.read_table(table), [{"sum": "9"}])

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
        for i in xrange(5):
            yt.run_map(func, input, output)
            files_in_cache = list(yt.search("//tmp/yt_wrapper/file_storage", node_type="file"))
            assert len(files_in_cache) > 0

            yt.run_map(func, input, output)
            files_in_cache_again = list(yt.search("//tmp/yt_wrapper/file_storage", node_type="file"))
            if sorted(files_in_cache) != sorted(files_in_cache_again):
                failures += 1

        assert failures <= 2

    @add_failed_operation_stderrs_to_error_message
    def test_python_operations_with_local_python(self):
        def func(row):
            yield row

        input = TEST_DIR + "/input"
        output = TEST_DIR + "/output"
        yt.write_table(input, [{"x": 1}, {"y": 2}])

        with set_config_option("pickling/use_local_python_in_jobs", True):
            yt.run_map(func, input, output)

        check(yt.read_table(output), [{"x": 1}, {"y": 2}], ordered=False)

    @add_failed_operation_stderrs_to_error_message
    def test_python_operations_in_local_mode(self):
        with set_config_option("is_local_mode", True):
            with set_config_option("pickling/enable_local_files_usage_in_job", True):
                old_tmp_dir = yt.config["local_temp_directory"]
                yt.config["local_temp_directory"] = tempfile.mkdtemp(dir=old_tmp_dir)

                os.chmod(yt.config["local_temp_directory"], 0o755)

                try:
                    def foo(rec):
                        yield rec

                    table = TEST_DIR + "/table"

                    yt.write_table(table, [{"x": 1}, {"y": 2}])
                    yt.run_map(foo, table, table, format=None)
                    check(yt.read_table(table), [{"x": 1}, {"y": 2}], ordered=False)
                finally:
                    yt.config["local_temp_directory"] = old_tmp_dir

    @add_failed_operation_stderrs_to_error_message
    def test_cross_format_operations(self):
        @yt.raw
        def reformat(rec):
            values = rec.strip().split(b"\t", 2)
            yield b"\t".join(b"=".join([k, v]) for k, v in izip([b"k", b"s", b"v"], values)) + b"\n"

        table = TEST_DIR + "/table"
        other_table = TEST_DIR + "/other_table"

        yt.config["tabular_data_format"] = yt.format.YamrFormat(has_subkey=True)

        # Enable progress printing in this test
        old_level = logger.LOGGER.level
        logger.LOGGER.setLevel(logging.INFO)
        try:
            yt.write_table(table, [b"0\ta\tA\n", b"1\tb\tB\n"], raw=True)
            yt.run_map(reformat, table, other_table, output_format=yt.format.DsvFormat())
            assert sorted(yt.read_table(other_table, format="dsv", raw=True)) == \
                   [b"k=0\ts=a\tv=A\n", b"k=1\ts=b\tv=B\n"]
        finally:
            yt.config["tabular_data_format"] = None
            logger.LOGGER.setLevel(old_level)

        yt.write_table(table, [b"1\t2\t3\n"], format="<has_subkey=true>yamr", raw=True)
        yt.run_map(reformat, table, table, input_format="<has_subkey=true>yamr", output_format="dsv")
        yt.run_map("cat", table, table, input_format="dsv", output_format="dsv")
        assert list(yt.read_table(table, format=yt.format.DsvFormat(), raw=True)) == [b"k=1\ts=2\tv=3\n"]

    def test_python_operations_io(self):
        """All access (except read-only) to stdin/out during the operation should be disabled."""
        table = TEST_DIR + "/table_io_test"

        yt.write_table(table, [{"x": 1}, {"y": 2}])

        def print_(rec):
            print("message")

        @yt.raw
        def write(rec):
            sys.stdout.write("message")

        @yt.raw
        def input_(rec):
            input()

        @yt.raw
        def read(rec):
            sys.stdin.read()

        @yt.raw
        def close(rec):
            sys.stdin.close()

        test_mappers = [print_, write, input_, read, close]
        for mapper in test_mappers:
            with pytest.raises(yt.YtError):
                yt.run_map(mapper, table, table)


    @add_failed_operation_stderrs_to_error_message
    def test_reduce_aggregator(self):
        table = TEST_DIR + "/table"
        other_table = TEST_DIR + "/other_table"
        yt.write_table(table, [{"x": 1, "y": 2}, {"x": 0, "y": 3}, {"x": 1, "y": 4}])

        @yt.reduce_aggregator
        def reducer(row_groups):
            sum_y = 0
            for k, rows in row_groups:
                for row in rows:
                    sum_y += int(row["y"])
            yield {"sum_y": sum_y}

        yt.run_sort(table, sort_by=["x"])
        yt.run_reduce(reducer, table, other_table, reduce_by=["x"])
        assert [{"sum_y": 9}] == list(yt.read_table(other_table))

    def test_operation_receives_spec_from_config(self):
        memory_limit = yt.config["memory_limit"]
        yt.config["memory_limit"] = 123
        check_input_fully_consumed = yt.config["yamr_mode"]["check_input_fully_consumed"]
        yt.config["yamr_mode"]["check_input_fully_consumed"] = not check_input_fully_consumed
        use_yamr_descriptors = yt.config["yamr_mode"]["use_yamr_style_destination_fds"]
        yt.config["yamr_mode"]["use_yamr_style_destination_fds"] = not use_yamr_descriptors
        yt.config["table_writer"] = {"max_row_weight": 8 * 1024 * 1024}

        table = TEST_DIR + "/table"
        yt.write_table(table, [{"x": 1}])
        try:
            op = yt.run_map("sleep 1; cat", table, table, sync=False)
            spec = yt.get_attribute(get_operation_path(op.id), "spec")
            assert spec["mapper"]["memory_limit"] == 123
            assert spec["mapper"]["check_input_fully_consumed"] != check_input_fully_consumed
            assert spec["mapper"]["use_yamr_descriptors"] != use_yamr_descriptors
            assert spec["job_io"]["table_writer"]["max_row_weight"] == 8 * 1024 * 1024
        finally:
            yt.config["memory_limit"] = memory_limit
            yt.config["yamr_mode"]["check_input_fully_consumed"] = check_input_fully_consumed
            yt.config["yamr_mode"]["use_yamr_style_destination_fds"] = use_yamr_descriptors
            yt.config["table_writer"] = {}
            try:
                op.abort()
            except yt.YtError:
                pass

    @add_failed_operation_stderrs_to_error_message
    def test_operation_start_finish_methods(self):
        table = TEST_DIR + "/table"
        other_table = TEST_DIR + "/other_table"

        yt.write_table(table, [{"x": 1}, {"x": 2}])
        yt.run_map(AggregateMapper(), table, other_table)
        assert [{"sum": 3}] == list(yt.read_table(other_table))
        yt.write_table(table, [{"x": 1, "y": 2}, {"x": 0, "y": 3}, {"x": 1, "y": 4}])
        yt.run_sort(table, sort_by=["x"])
        yt.run_reduce(AggregateReducer(), table, other_table, reduce_by=["x"])
        check([{"sum": 1}, {"sum": 2}, {"sum": 9}], list(yt.read_table(other_table)))

    @add_failed_operation_stderrs_to_error_message
    def test_stdout_fd_protection(self):
        def mapper(record):
            # Pretend that we are C code writing to stdout.
            os.write(1, b"Mapping record: " + repr(record).encode("utf-8"))
            yield record

        table = TEST_DIR + "/table"
        output_table = TEST_DIR + "/my_table"
        yt.write_table(table, [{"x": 1}, {"x": 2}])

        for protection_type in ("none", "close"):
            with set_config_option("pickling/stdout_fd_protection", protection_type):
                with pytest.raises(yt.YtOperationFailedError):
                    yt.run_map(mapper, table, output_table)

        for protection_type in ("redirect_to_stderr", "drop"):
            yt.write_table(output_table, [])
            with set_config_option("pickling/stdout_fd_protection", protection_type):
                yt.run_map(mapper, table, output_table)
            check([{"x": 1}, {"x": 2}], list(yt.read_table(output_table)))

    @add_failed_operation_stderrs_to_error_message
    def test_create_modules_archive(self):
        def foo(rec):
            yield rec

        table = TEST_DIR + "/table"

        try:
            yt.config["pickling"]["create_modules_archive_function"] = \
                lambda tempfiles_manager: create_modules_archive_default(tempfiles_manager, False, None)
            yt.run_map(foo, table, table)

            with TempfilesManager(remove_temp_files=True, directory=yt.config["local_temp_directory"]) as tempfiles_manager:
                yt.config["pickling"]["create_modules_archive_function"] = \
                        lambda: create_modules_archive_default(tempfiles_manager, False, None)
                yt.run_map(foo, table, table)

            with TempfilesManager(remove_temp_files=True, directory=yt.config["local_temp_directory"]) as tempfiles_manager:
                with set_config_option("pickling/modules_chunk_size", MB):
                    yt.config["pickling"]["create_modules_archive_function"] = \
                            lambda: create_modules_archive_default(tempfiles_manager, False, None)[0]["filename"]
                    yt.run_map(foo, table, table)

            yt.config["pickling"]["create_modules_archive_function"] = CreateModulesArchive()
            yt.run_map(foo, table, table)

        finally:
            yt.config["pickling"]["create_modules_archive_function"] = None

    @add_failed_operation_stderrs_to_error_message
    def test_is_inside_job(self):
        table = TEST_DIR + "/table"
        yt.write_table(table, [{"x": 1}])

        def mapper(rec):
            yield {"flag": str(yt.is_inside_job()).lower()}

        yt.run_map(mapper, table, table)
        assert not yt.is_inside_job()
        assert list(yt.read_table(table)) == [{"flag": "true"}]

    @add_failed_operation_stderrs_to_error_message
    def test_reduce_key_modification(self):
        def reducer(key, recs):
            rec = next(recs)
            key["x"] = int(rec["y"]) + 10
            yield key

        def reducer_that_yields_key(key, recs):
            for rec in recs:
                pass
            yield key

        table = TEST_DIR + "/table"
        yt.write_table(table, [{"x": 1, "y": 1}, {"x": 1, "y": 2}, {"x": 2, "y": 3}])
        yt.run_sort(table, table, sort_by=["x"])

        with pytest.raises(yt.YtOperationFailedError):
            yt.run_reduce(reducer, table, TEST_DIR + "/other", reduce_by=["x"], format="json")

        yt.run_reduce(reducer_that_yields_key, table, TEST_DIR + "/other", reduce_by=["x"], format="json")
        check([{"x": 1}, {"x": 2}], yt.read_table(TEST_DIR + "/other"), ordered=False)

    @add_failed_operation_stderrs_to_error_message
    def test_reduce_without_reading_all_rows(self):
        def reducer1(key, recs):
            yield {"x": 10}

        def reducer2(key, recs):
            next(recs)
            yield {"x": 10}

        table = TEST_DIR + "/table"
        yt.write_table(table, [{"x": 1, "y": 1}, {"x": 1, "y": 2}, {"x": 2, "y": 3}])
        yt.run_sort(table, table, sort_by=["x"])

        yt.run_reduce(reducer1, table, TEST_DIR + "/other", reduce_by=["x"], format="json")
        check([{"x": 10}, {"x": 10}], yt.read_table(TEST_DIR + "/other"), ordered=False)

        yt.run_reduce(reducer2, table, TEST_DIR + "/other", reduce_by=["x"], format="json")
        check([{"x": 10}, {"x": 10}], yt.read_table(TEST_DIR + "/other"), ordered=False)

    @add_failed_operation_stderrs_to_error_message
    def test_table_and_row_index_from_job(self):
        @yt.aggregator
        def mapper(rows):
            for row in rows:
                assert "@table_index" in row
                assert "@row_index" in row
                row["table_index"] = int(row["@table_index"])
                del row["@table_index"]
                row["row_index"] = int(row["@row_index"])
                del row["@row_index"]
                yield row

        tableA = TEST_DIR + "/tableA"
        yt.write_table(tableA, [{"x": 1}, {"y": 1}])

        tableB = TEST_DIR + "/tableB"
        yt.write_table(tableB, [{"x": 2}])

        outputTable = TEST_DIR + "/output"

        yt.run_map(mapper, [tableA, tableB], outputTable, format=yt.YsonFormat(control_attributes_mode="row_fields"),
                   spec={"job_io": {"control_attributes": {"enable_row_index": True}}, "ordered": True})

        result = sorted(list(yt.read_table(outputTable, raw=False, format=yt.YsonFormat())),
                        key=lambda item: (item["table_index"], item["row_index"]))

        assert [
            {"table_index": 0, "row_index": 0, "x": 1},
            {"table_index": 0, "row_index": 1, "y": 1},
            {"table_index": 1, "row_index": 0, "x": 2}
        ] == result

    @add_failed_operation_stderrs_to_error_message
    def test_functions_with_context(self):
        @yt.with_context
        def mapper(row, context):
            yield {"row_index": context.row_index}

        @yt.with_context
        def reducer(key, rows, context):
            for row in rows:
                yield {"row_index": context.row_index}

        @yt.with_context
        def mapper_table_index(row, context):
            yield {"table_index": context.table_index}

        input = TEST_DIR + "/input"
        input2 = TEST_DIR + "/input2"
        output = TEST_DIR + "/output"
        yt.write_table(input, [{"x": 1, "y": "a"}, {"x": 1, "y": "b"}, {"x": 2, "y": "b"}])
        yt.run_map(mapper, input, output,
                   format=yt.YsonFormat(),
                   spec={"job_io": {"control_attributes": {"enable_row_index": True}}})

        check(yt.read_table(output), [{"row_index": index} for index in xrange(3)])

        yt.run_sort(input, input, sort_by=["x"])
        yt.run_reduce(reducer, input, output,
                      reduce_by=["x"],
                      format=yt.YsonFormat(),
                      spec={"job_io": {"control_attributes": {"enable_row_index": True}}})
        check(yt.read_table(output), [{"row_index": index} for index in xrange(3)])

        yt.write_table(input, [{"x": 1, "y": "a"}])
        yt.run_map(mapper_table_index, input, output, format=yt.YsonFormat(control_attributes_mode="iterator"))
        check(yt.read_table(output), [{"table_index": 0}])

        yt.write_table(input2, [{"x": 1, "y": "a"}])
        yt.run_map(mapper_table_index, [input, input2], output, format=yt.YsonFormat(control_attributes_mode="iterator"))
        check(yt.read_table(output), [{"table_index": 0}, {"table_index": 1}], ordered=False)


@pytest.mark.usefixtures("yt_env_with_rpc")
class TestOperationsTracker(object):
    def setup(self):
        yt.config["tabular_data_format"] = yt.format.JsonFormat()
        self.env = {
            "YT_CONFIG_PATCHES": dumps_yt_config(),
            "PYTHONPATH": os.environ.get("PYTHONPATH", "")
        }

    def teardown(self):
        yt.config["tabular_data_format"] = None
        yt.remove("//tmp/yt_wrapper/file_storage", recursive=True, force=True)

    def test_reduce_with_foreign_tables_and_disabled_key_guarantee(self):
        table1 = TEST_DIR + "/table1"
        table2 = TEST_DIR + "/table2"
        table = TEST_DIR + "/table"
        yt.write_table("<sorted_by=[x;y]>" + table1, [{"x": 1, "y": 1}])
        yt.write_table("<sorted_by=[x]>" + table2, [{"x": 1}])

        def func(_, rows):
            for row in rows:
                yield row

        op = yt.run_reduce(func, [table1, "<foreign=true>" + table2], table,
                           join_by=["x"], enable_key_guarantee=False)
        assert "enable_key_guarantee" in op.get_attributes()["spec"]
        check([{"x": 1, "y": 1}, {"x": 1}], yt.read_table(table))

    def test_operations_tracker_multiple_instances(self):
        tracker = yt.OperationsTracker()

        test_name = "TrackOperationsFromMultipleClusters"
        dir = os.path.join(get_tests_sandbox(), test_name)
        id = "run_" + uuid.uuid4().hex[:8]
        instance = None
        try:
            instance = start(path=dir, id=id, node_count=3, enable_debug_logging=True, cell_tag=1)
            clientA = yt
            clientB = instance.create_client()
            clientB.config["tabular_data_format"] = yt.format.JsonFormat()

            table = TEST_DIR + "/table"
            clientA.write_table(table, [{"x": 1, "y": 1}])

            clientB.create("table", table, recursive=True, ignore_existing=True)
            clientB.write_table(table, [{"x": 1, "y": 1}])

            op1 = clientA.run_map("sleep 30; cat", table, TEST_DIR + "/out1", sync=False)
            op2 = clientB.run_map("sleep 30; cat", table, TEST_DIR + "/out2", sync=False)

            tracker.add(op1)
            tracker.add(op2)
            tracker.abort_all()

            assert op1.get_state() == "aborted"
            assert op2.get_state() == "aborted"

            op1 = clientA.run_map("sleep 2; cat", table, TEST_DIR + "/out1", sync=False)
            op2 = clientB.run_map("sleep 2; cat", table, TEST_DIR + "/out2", sync=False)
            tracker.add_by_id(op1.id)
            tracker.add_by_id(op2.id, client=clientB)
            tracker.wait_all()

            assert op1.get_state().is_finished()
            assert op2.get_state().is_finished()

            with pytest.raises(ZeroDivisionError):
                with tracker:
                    op1 = clientA.run_map("sleep 100; cat", table, TEST_DIR + "/out", sync=False)
                    tracker.add(op1)
                    op2 = clientB.run_map("sleep 100; cat", table, TEST_DIR + "/out", sync=False)
                    tracker.add(op2)
                    raise ZeroDivisionError

            assert op2.get_state() == "aborted"
            assert op1.get_state() == "aborted"

        finally:
            if instance is not None:
                stop(instance.id, path=dir)

    def test_operations_tracker(self):
        tracker = yt.OperationsTracker()

        # To enable progress printing
        old_level = logger.LOGGER.level
        logger.LOGGER.setLevel(logging.INFO)
        try:
            table = TEST_DIR + "/table"
            yt.write_table(table, [{"x": 1, "y": 1}])

            op1 = yt.run_map("sleep 30; cat", table, TEST_DIR + "/out1", sync=False)
            op2 = yt.run_map("sleep 30; cat", table, TEST_DIR + "/out2", sync=False)

            tracker.add(op1)
            tracker.add(op2)
            tracker.abort_all()

            assert op1.get_state() == "aborted"
            assert op2.get_state() == "aborted"

            op1 = yt.run_map("sleep 2; cat", table, TEST_DIR + "/out1", sync=False)
            op2 = yt.run_map("sleep 2; cat", table, TEST_DIR + "/out2", sync=False)
            tracker.add_by_id(op1.id)
            tracker.add_by_id(op2.id)
            tracker.wait_all()

            assert op1.get_state().is_finished()
            assert op2.get_state().is_finished()

            tracker.add(yt.run_map("false", table, TEST_DIR + "/out", sync=False))
            with pytest.raises(yt.YtError):
                tracker.wait_all(check_result=True)

            assert not tracker.operations

            op = yt.run_map("cat", table, TEST_DIR + "/out", sync=False)
            tracker.add(op)
            tracker.wait_all(keep_finished=True)
            assert op.id in tracker.operations

            tracker.wait_all(keep_finished=True)
            tracker.abort_all()

            with tracker:
                op = yt.run_map("sleep 2; true", table, TEST_DIR + "/out", sync=False)
                tracker.add(op)
            assert op.get_state() == "completed"

            with pytest.raises(ZeroDivisionError):
                with tracker:
                    op = yt.run_map("sleep 100; cat", table, TEST_DIR + "/out", sync=False)
                    tracker.add(op)
                    raise ZeroDivisionError

            assert op.get_state() == "aborted"
        finally:
            logger.LOGGER.setLevel(old_level)

    def test_pool_tracker_multiple_instances(self):
        def create_spec_builder(binary, source_table, destination_table):
            return MapSpecBuilder() \
                .input_table_paths(source_table) \
                .output_table_paths(destination_table) \
                .begin_mapper() \
                .command(binary) \
                .end_mapper()

        tracker = yt.OperationsTrackerPool(pool_size=1)

        test_name = "TrackPoolOperationsFromMultipleClusters"
        dir = os.path.join(get_tests_sandbox(), test_name)
        id = "run_" + uuid.uuid4().hex[:8]
        instance = None

        # To enable progress printing
        old_level = logger.LOGGER.level
        logger.LOGGER.setLevel(logging.INFO)
        try:
            instance = start(path=dir, id=id, node_count=3, enable_debug_logging=True, cell_tag=2)
            clientA = None
            clientB = instance.create_client()
            clientB.config["tabular_data_format"] = yt.format.JsonFormat()

            table = TEST_DIR + "/table"
            yt.write_table(table, [{"x": 1, "y": 1}])
            clientB.create("table", table, recursive=True, ignore_existing=True)
            clientB.write_table(table, [{"x": 1, "y": 1}])

            assert tracker.get_operation_count() == 0

            spec_builder1 = create_spec_builder("sleep 30; cat", table, TEST_DIR + "/out1")
            spec_builder2 = create_spec_builder("sleep 30; cat", table, TEST_DIR + "/out2")

            tracker.add(spec_builder1, client=clientA)
            tracker.add(spec_builder2, client=clientB)

            assert tracker.get_operation_count() == 2

            tracker.abort_all()

            assert tracker.get_operation_count() == 0

            spec_builder1 = create_spec_builder("sleep 30; cat", table, TEST_DIR + "/out1")
            spec_builder2 = create_spec_builder("sleep 30; cat", table, TEST_DIR + "/out2")

            tracker.map([spec_builder1, spec_builder2], client=clientA)

            assert tracker.get_operation_count() == 2

            tracker.abort_all()

            assert tracker.get_operation_count() == 0

            tracker.map([spec_builder1, spec_builder2], client=clientB)

            assert tracker.get_operation_count() == 2

            tracker.abort_all()

            assert tracker.get_operation_count() == 0

            spec_builder1 = create_spec_builder("sleep 2; cat", table, TEST_DIR + "/out1")
            spec_builder2 = create_spec_builder("sleep 2; cat", table, TEST_DIR + "/out2")
            tracker.map([spec_builder1, spec_builder2], client=clientA)
            tracker.map([spec_builder1, spec_builder2], client=clientB)

            assert tracker.get_operation_count() == 4

            tracker.wait_all()

            assert tracker.get_operation_count() == 0

            tracker.map([create_spec_builder("false", table, TEST_DIR + "/out")], client=clientB)
            with pytest.raises(yt.YtError):
                tracker.wait_all(check_result=True)

            tracker.map([create_spec_builder("false", table, TEST_DIR + "/out")], client=clientA)
            with pytest.raises(yt.YtError):
                tracker.wait_all(check_result=True)

        finally:
            logger.LOGGER.setLevel(old_level)
            if instance is not None:
                stop(instance.id, path=dir)

    def test_pool_tracker(self):
        def create_spec_builder(binary, source_table, destination_table):
            return MapSpecBuilder() \
                .input_table_paths(source_table) \
                .output_table_paths(destination_table) \
                .begin_mapper() \
                    .command(binary) \
                .end_mapper()

        tracker = yt.OperationsTrackerPool(pool_size=1)

        # To enable progress printing
        old_level = logger.LOGGER.level
        logger.LOGGER.setLevel(logging.INFO)
        try:
            table = TEST_DIR + "/table"
            yt.write_table(table, [{"x": 1, "y": 1}])

            assert tracker.get_operation_count() == 0

            spec_builder1 = create_spec_builder("sleep 30; cat", table, TEST_DIR + "/out1")
            spec_builder2 = create_spec_builder("sleep 30; cat", table, TEST_DIR + "/out2")

            tracker.add(spec_builder1)
            tracker.add(spec_builder2)

            assert tracker.get_operation_count() == 2

            tracker.abort_all()

            assert tracker.get_operation_count() == 0

            spec_builder1 = create_spec_builder("sleep 30; cat", table, TEST_DIR + "/out1")
            spec_builder2 = create_spec_builder("sleep 30; cat", table, TEST_DIR + "/out2")

            tracker.map([spec_builder1, spec_builder2])

            assert tracker.get_operation_count() == 2

            tracker.abort_all()

            assert tracker.get_operation_count() == 0

            spec_builder1 = create_spec_builder("sleep 2; cat", table, TEST_DIR + "/out1")
            spec_builder2 = create_spec_builder("sleep 2; cat", table, TEST_DIR + "/out2")
            tracker.map([spec_builder1, spec_builder2])

            assert tracker.get_operation_count() == 2

            tracker.wait_all()

            assert tracker.get_operation_count() == 0

            tracker.map([create_spec_builder("false", table, TEST_DIR + "/out")])
            with pytest.raises(yt.YtError):
                tracker.wait_all(check_result=True)

            spec_builder = create_spec_builder("cat", table, TEST_DIR + "/out")
            tracker.map([spec_builder])
            tracker.wait_all(keep_finished=True)
            tracker.abort_all()

            assert tracker.get_operation_count() == 0

            with tracker:
                spec_builder = create_spec_builder("sleep 2; true", table, TEST_DIR + "/out")
                tracker.map([spec_builder])

                assert tracker.get_operation_count() == 1

            assert tracker.get_operation_count() == 0

        finally:
            logger.LOGGER.setLevel(old_level)

@pytest.mark.usefixtures("yt_env_with_rpc")
class TestOperationsTmpfs(object):
    def setup(self):
        yt.config["tabular_data_format"] = yt.format.JsonFormat()
        self.env = {
            "YT_CONFIG_PATCHES": dumps_yt_config(),
            "PYTHONPATH": os.environ.get("PYTHONPATH", "")
        }

    def teardown(self):
        yt.config["tabular_data_format"] = None
        yt.remove("//tmp/yt_wrapper/file_storage", recursive=True, force=True)

    @add_failed_operation_stderrs_to_error_message
    def test_mount_tmpfs_in_sandbox(self, yt_env_with_rpc):
        if not ENABLE_JOB_CONTROL:
            pytest.skip()

        def foo(rec):
            size = 0
            for path, dirnames, filenames in os.walk("."):
                for file_name in filenames:
                    file_path = os.path.join(path, file_name)
                    size += get_disk_size(file_path)
            yield {"size": size}

        def get_spec_option(id, name):
            return yt.get("{}/@spec/{}".format(get_operation_path(id), name))

        with set_config_option("mount_sandbox_in_tmpfs/enable", True):
            table = TEST_DIR + "/table"
            file = TEST_DIR + "/test_file"
            table_file = TEST_DIR + "/test_table_file"

            dynamic_libraries_size_correction = 0
            for key, module in sys.modules.items():
                path = getattr(module, "__file__", "")
                if path.endswith(".so"):
                    dynamic_libraries_size_correction += os.path.getsize(path) * (TMPFS_SIZE_MULTIPLIER - 1.0)

            dir_ = yt_env_with_rpc.env.path
            with tempfile.NamedTemporaryFile(dir=dir_, prefix="local_file", delete=False) as local_file:
                local_file.write(b"a" * 42000000)
            yt.write_table(table, [{"x": 1}, {"y": 2}])
            yt.write_table(table_file, [{"x": 1}, {"y": 2}])
            yt.write_file(file, b"a" * 10000000)
            table_file_object = yt.FilePath(table_file, attributes={"format": "json", "disk_size": 1000})
            op = yt.run_map(foo, table, table, local_files=[local_file.name], yt_files=[file, table_file_object])
            disk_size = next(yt.read_table(table))["size"]

            tmpfs_size = get_spec_option(op.id, "mapper/tmpfs_size")
            memory_limit = get_spec_option(op.id, "mapper/memory_limit")
            diff = disk_size - tmpfs_size
            if diff < 0:
                diff = -diff
            assert tmpfs_size > 8 * 1024
            assert diff < dynamic_libraries_size_correction + 2 * 1024 * 1024  # TMPFS_ADDEND + TMPFS_MULTIPLIER * archive_size + {rounded up file sizes}
            assert memory_limit - tmpfs_size == 512 * 1024 * 1024
            assert get_spec_option(op.id, "mapper/tmpfs_path") == "."

    def test_download_job_stderr_messages(self):
        def mapper(row):
            sys.stderr.write("Job with stderr")
            yield row

        temp_table_input = yt.create_temp_table()
        temp_table_output = yt.create_temp_table()
        yt.write_table(temp_table_input, [{"x": i} for i in range(10)], format=yt.JsonFormat(), raw=False)
        operation = yt.run_map(mapper, temp_table_input, temp_table_output,
                               spec={"data_size_per_job": 1}, input_format=yt.JsonFormat())

        job_infos = get_jobs_with_error_or_stderr(operation.id, False)
        for stderr in job_infos:
            assert stderr["stderr"] == "Job with stderr"
        assert len(job_infos) == 10

        assert yt.format_operation_stderrs(job_infos)

        old_timeout = yt.config["operation_tracker"]["stderr_download_timeout"]
        old_thread_count = yt.config["operation_tracker"]["stderr_download_thread_count"]
        yt.config["operation_tracker"]["stderr_download_timeout"] = 30
        yt.config["operation_tracker"]["stderr_download_thread_count"] = 1

        try:
            job_infos = get_jobs_with_error_or_stderr(operation.id, False)
            assert len(job_infos) < 10
        finally:
            yt.config["operation_tracker"]["stderr_download_timeout"] = old_timeout
            yt.config["operation_tracker"]["stderr_download_thread_count"] = old_thread_count

        binary = get_test_file_path("stderr_download.py")
        process = subprocess.Popen([get_python(), binary, operation.id], env=self.env, stderr=subprocess.PIPE)

        time.sleep(0.5)
        os.kill(process.pid, signal.SIGINT)

        timeout = 3.0
        start_time = time.time()
        while time.time() - start_time < timeout:
            if process.poll() is not None:
                break
            time.sleep(0.05)
        else:
            assert False, "Process did not terminate after {0:.2f} seconds".format(timeout)

    @add_failed_operation_stderrs_to_error_message
    def test_sandbox_file_name_specification(self, yt_env_with_rpc):
        def mapper(row):
            with open("cool_name.dat") as f:
                yield {"k": f.read().strip()}

        table = TEST_DIR + "/table"
        yt.write_table(table, [{"x": 1}])

        dir_ = yt_env_with_rpc.env.path
        with tempfile.NamedTemporaryFile("w", dir=dir_, prefix="mapper", delete=False) as f:
            f.write("etwas")

        yt.run_map(mapper, table, table, local_files=['<file_name="cool_name.dat">' + f.name])
        check(yt.read_table(table), [{"k": "etwas"}])

@pytest.mark.usefixtures("yt_env_with_rpc")
class TestOperationsSeveralOutputTables(object):
    def setup(self):
        yt.config["tabular_data_format"] = yt.format.JsonFormat()
        self.env = {
            "YT_CONFIG_PATCHES": dumps_yt_config(),
            "PYTHONPATH": os.environ.get("PYTHONPATH", "")
        }

    def teardown(self):
        yt.config["tabular_data_format"] = None
        yt.remove("//tmp/yt_wrapper/file_storage", recursive=True, force=True)

    @add_failed_operation_stderrs_to_error_message
    def test_yson_several_output_tables(self):
        def first_mapper(row):
            row["@table_index"] = row["x"]
            yield row

        def second_mapper(row):
            yield yt.create_table_switch(row["x"])
            yield row

        def third_mapper(row):
            yield yt.create_table_switch(row["x"] + 5)
            yield row

        table = TEST_DIR + "/table"
        yt.write_table(table, [{"x": 0}, {"x": 1}, {"x": 2}])

        output_tables = [TEST_DIR + "/output_" + str(i) for i in xrange(4)]

        yt.run_map(first_mapper, table, output_tables, format=yt.YsonFormat(control_attributes_mode="row_fields"))

        assert list(yt.read_table(output_tables[0])) == [{"x": 0}]
        assert list(yt.read_table(output_tables[1])) == [{"x": 1}]
        assert list(yt.read_table(output_tables[2])) == [{"x": 2}]
        assert list(yt.read_table(output_tables[3])) == []

        yt.run_map(second_mapper, table, output_tables, format=yt.YsonFormat(control_attributes_mode="iterator"))

        assert list(yt.read_table(output_tables[0])) == [{"x": 0}]
        assert list(yt.read_table(output_tables[1])) == [{"x": 1}]
        assert list(yt.read_table(output_tables[2])) == [{"x": 2}]
        assert list(yt.read_table(output_tables[3])) == []

        with pytest.raises(yt.YtError):
            yt.run_map(third_mapper, table, output_tables, format=yt.YsonFormat(control_attributes_mode="iterator"))

    @add_failed_operation_stderrs_to_error_message
    def test_json_several_output_tables(self):
        def first_mapper(row):
            row["@table_index"] = row["x"]
            yield row

        def second_mapper(row):
            yield {"$value": None, "$attributes": {"table_index": row["x"]}}
            yield row

        def third_mapper(row):
            yield {"$value": None, "$attributes": {"table_index": row["x"] + 5}}
            yield row

        table = TEST_DIR + "/table"
        yt.write_table(table, [{"x": 0}, {"x": 1}, {"x": 2}])

        output_tables = [TEST_DIR + "/output_" + str(i) for i in xrange(4)]

        yt.run_map(first_mapper, table, output_tables, format=yt.JsonFormat(control_attributes_mode="row_fields"))

        assert list(yt.read_table(output_tables[0])) == [{"x": 0}]
        assert list(yt.read_table(output_tables[1])) == [{"x": 1}]
        assert list(yt.read_table(output_tables[2])) == [{"x": 2}]
        assert list(yt.read_table(output_tables[3])) == []

        yt.run_map(second_mapper, table, output_tables, format=yt.JsonFormat(control_attributes_mode="iterator"))

        assert list(yt.read_table(output_tables[0])) == [{"x": 0}]
        assert list(yt.read_table(output_tables[1])) == [{"x": 1}]
        assert list(yt.read_table(output_tables[2])) == [{"x": 2}]
        assert list(yt.read_table(output_tables[3])) == []

        with pytest.raises(yt.YtError):
            yt.run_map(third_mapper, table, output_tables, format=yt.JsonFormat(control_attributes_mode="iterator"))

    @add_failed_operation_stderrs_to_error_message
    def test_yamr_several_output_tables(self):
        def first_mapper(rec):
            rec.tableIndex = int(rec.value)
            yield rec

        def second_mapper(rec):
            rec.tableIndex = int(rec.value + 5)
            yield rec

        table = TEST_DIR + "/table"
        yt.write_table(table, [{"key": "x", "value": str(i)} for i in xrange(3)])

        output_tables = [TEST_DIR + "/output_" + str(i) for i in xrange(4)]

        yt.run_map(first_mapper, table, output_tables, format=yt.YamrFormat())

        assert list(yt.read_table(output_tables[0])) == [{"key": "x", "value": "0"}]
        assert list(yt.read_table(output_tables[1])) == [{"key": "x", "value": "1"}]
        assert list(yt.read_table(output_tables[2])) == [{"key": "x", "value": "2"}]
        assert list(yt.read_table(output_tables[3])) == []

        with pytest.raises(yt.YtError):
            yt.run_map(second_mapper, table, output_tables, format=yt.YamrFormat())

    def test_multiple_mapper_output_tables_in_mapreduce(self):
        input_table = TEST_DIR + "/table"
        mapper_output_table = TEST_DIR + "/mapper_output_table"
        output_table = TEST_DIR + "/output_table"
        yt.write_table(input_table, [{"x": 1}])

        def mapper(rec):
            recs = [{"a": "b"}, {"c": "d"}]
            for i, rec in enumerate(recs):
                rec["@table_index"] = i
                yield rec

        spec_builder = MapReduceSpecBuilder() \
            .begin_mapper() \
                .format(yt.YsonFormat(control_attributes_mode="row_fields")) \
                .command(mapper) \
            .end_mapper() \
            .begin_reducer() \
                .command("cat") \
                .format("json") \
            .end_reducer() \
            .reduce_by(["a"]) \
            .mapper_output_table_count(1) \
            .input_table_paths(input_table) \
            .output_table_paths([mapper_output_table, output_table])

        yt.run_operation(spec_builder)
        check([{"c": "d"}], list(yt.read_table(mapper_output_table)))
        check([{"a": "b"}], list(yt.read_table(output_table)))

@pytest.mark.usefixtures("yt_env_with_rpc")
class TestOperationsSkiffFormat(object):
    def setup(self):
        yt.config["tabular_data_format"] = yt.format.JsonFormat()
        self.env = {
            "YT_CONFIG_PATCHES": dumps_yt_config(),
            "PYTHONPATH": os.environ.get("PYTHONPATH", "")
        }

    def teardown(self):
        yt.config["tabular_data_format"] = None
        yt.remove("//tmp/yt_wrapper/file_storage", recursive=True, force=True)

    @add_failed_operation_stderrs_to_error_message
    def test_skiff(self):
        table = TEST_DIR + "/table"
        output_table = TEST_DIR + "/output_table"

        def mapper(row):
            row["y"] = row["x"] ** 2
            yield row

        def mapper2(row):
            row["y"] = row["z"] ** 2
            yield row

        table_skiff_schemas = [
            {
                "wire_type": "tuple",
                "children": [
                    {
                        "wire_type": "int64",
                        "name": "x"
                    },
                    {
                        "wire_type": "variant8",
                        "children": [
                            {
                                "wire_type": "nothing"
                            },
                            {
                                "wire_type": "int64"
                            }
                        ],
                        "name": "y"
                    }
                ]
            }
        ]
        format = yt.format.SkiffFormat(table_skiff_schemas)

        schema = [{"name": "x", "type": "int64"}, {"name": "y", "type": "int64"}]
        schema = YsonList(schema)
        schema.attributes["strict"] = False

        yt.create("table", table, attributes={"schema": schema})
        yt.write_table(table, [{"x": 3, "y": 1}, {"x": 5}, {"x": 0, "y": 2}])

        stream = io.BytesIO()
        format.dump_rows(yt.read_table(table, format=format), stream)
        stream.seek(0)
        result = list(format.load_rows(stream))
        assert result[0]["x"] == 3
        assert result[0]["y"] == 1

        assert result[1][0] == 5
        assert result[1][1] is None

        assert result[2][1] == 2

        with pytest.raises(LookupError):
            result[2]["t"]

        yt.run_map(mapper, table, output_table, format=format)
        assert list(yt.read_table(output_table)) == [{"x": 3, "y": 9}, {"x": 5, "y": 25}, {"x": 0, "y": 0}]

    @add_failed_operation_stderrs_to_error_message
    def test_skiff_multi_table(self):
        first_table = TEST_DIR + "/table1"
        second_table = TEST_DIR + "/table2"

        first_output_table = TEST_DIR + "/table_output1"
        second_output_table = TEST_DIR + "/table_output2"

        def mapper(row):
            if row[0] < 0:
                yield SkiffTableSwitch(1)
            else:
                yield SkiffTableSwitch(0)
            row["y"] = row[0] ** 2
            yield row

        table_skiff_schemas = [
            {
                "wire_type": "tuple",
                "children": [
                    {
                        "wire_type": "int64",
                        "name": "x"
                    },
                    {
                        "wire_type": "variant8",
                        "children": [
                            {
                                "wire_type": "nothing"
                            },
                            {
                                "wire_type": "int64"
                            }
                        ],
                        "name": "y"
                    }
                ]
            },
            {
                "wire_type": "tuple",
                "children": [
                    {
                        "wire_type": "int64",
                        "name": "z"
                    },
                    {
                        "wire_type": "int64",
                        "name": "y"
                    }
                ]
            }
        ]
        format = yt.format.SkiffFormat(table_skiff_schemas)

        schema = [{"name": "x", "type": "int64"}, {"name": "y", "type": "int64"}]
        schema = YsonList(schema)
        schema.attributes["strict"] = False

        yt.create("table", first_table, attributes={"schema": schema})
        yt.write_table(first_table, [{"x": 3}, {"x": 5}, {"x": 0}])

        schema = [{"name": "z", "type": "int64"}, {"name": "y", "type": "int64"}]
        schema = YsonList(schema)
        yt.create("table", second_table, attributes={"schema": schema})
        yt.write_table(second_table, [{"z": -1, "y": 0}, {"z": -2, "y": 0}, {"z": -5, "y": 0}])

        yt.run_map(mapper, [first_table, second_table], [first_output_table, second_output_table], format=format)
        assert list(yt.read_table(first_output_table)) == [{"x": 3, "y": 9}, {"x": 5, "y": 25}, {"x": 0, "y": 0}]
        assert list(yt.read_table(second_output_table)) == [{"z": -1, "y": 1}, {"z": -2, "y": 4}, {"z": -5, "y": 25}]

    @add_failed_operation_stderrs_to_error_message
    def test_with_skiff_schemas(self):
        input_table = TEST_DIR + "/table"

        first_output_table = TEST_DIR + "/table_output1"
        second_output_table = TEST_DIR + "/table_output2"

        @yt.with_skiff_schemas
        def mapper(row, skiff_input_schemas, skiff_output_schemas):
            assert row.get_schema() is skiff_input_schemas[0]
            if row[0] < 0:
                yield SkiffTableSwitch(1)
                record = skiff_output_schemas[1].create_record()
                record["x"] = row["x"] * 2
            else:
                yield SkiffTableSwitch(0)
                record = skiff_output_schemas[0].create_record()
                record["z"] = row["x"] * 3

            yield record

        input_table_skiff_schemas = [
            {
                "wire_type": "tuple",
                "children": [
                    {
                        "wire_type": "int64",
                        "name": "f"
                    },
                    {
                        "wire_type": "int64",
                        "name": "x"
                    },
                ]
            }
        ]
        input_format = yt.format.SkiffFormat(input_table_skiff_schemas)

        output_table_skiff_schemas = [
            {
                "wire_type": "tuple",
                "children": [
                    {
                        "wire_type": "int64",
                        "name": "z"
                    }
                ]
            },
            {
                "wire_type": "tuple",
                "children": [
                    {
                        "wire_type": "int64",
                        "name": "x"
                    }
                ]
            }
        ]
        output_format = yt.format.SkiffFormat(output_table_skiff_schemas)

        schema = [{"name": "f", "type": "int64"}, {"name": "x", "type": "int64"}]
        schema = YsonList(schema)
        schema.attributes["strict"] = False

        yt.create("table", input_table, attributes={"schema": schema})
        yt.write_table(input_table, [{"f": 1, "x": 3}, {"f": -1, "x": 5}, {"f": -1, "x": 7}])

        yt.run_map(mapper, input_table, [first_output_table, second_output_table], input_format=input_format, output_format=output_format)
        assert list(yt.read_table(first_output_table)) == [{"z": 9}]
        assert list(yt.read_table(second_output_table)) == [{"x": 10}, {"x": 14}]

    @add_failed_operation_stderrs_to_error_message
    def test_skiff_schema_from_table(self):
        @yt.with_skiff_schemas
        def mapper(row, skiff_input_schemas, skiff_output_schemas):
            result = skiff_output_schemas[0].create_record()
            result["y"] = row["x"] ** 2
            yield result

        @yt.with_skiff_schemas
        def mapper2(row, skiff_input_schemas, skiff_output_schemas):
            result = skiff_output_schemas[0].create_record()
            result["y"] = row["z"] ** 2
            yield result

        input_table = TEST_DIR + "/input_table"
        input_table_schema = [{"name": "x", "type": "int64", "required": True}, {"name": "y", "type": "string"}]
        yt.create("table", input_table, attributes={"schema": input_table_schema})

        output_table = TEST_DIR + "/output_table"
        output_table_schema = [{"name": "y", "type": "int64", "required": True}]
        output_table_schema = YsonList(output_table_schema)
        output_table_schema.attributes["strict"] = True

        output_table2 = TEST_DIR + "/output_table2"
        output_table3 = TEST_DIR + "/output_table3"

        yt.create("table", output_table, attributes={"schema": output_table_schema})

        input_format = yt.format.SkiffFormat(convert_to_skiff_schema(input_table_schema))
        output_format = yt.format.SkiffFormat(convert_to_skiff_schema(output_table_schema))

        yt.write_table(input_table, [{"x": 3, "y": "aba"}, {"x": 5}, {"x": 0, "y": "abacaba"}])

        yt.run_map(mapper, input_table, output_table, input_format=input_format, output_format=output_format)
        assert list(yt.read_table(output_table)) == [{"y": 9}, {"y": 25}, {"y": 0}]

        yt.run_map(mapper, input_table, output_table)
        assert list(yt.read_table(output_table)) == [{"y": 9}, {"y": 25}, {"y": 0}]

        yt.run_map(mapper, input_table, output_table2)
        assert list(yt.read_table(output_table2)) == [{"y": 9}, {"y": 25}, {"y": 0}]

        yt.run_map(mapper2, TablePath(input_table, attributes={"rename_columns": {"x": "z"}}), output_table3)
        assert list(yt.read_table(output_table3)) == [{"y": 9}, {"y": 25}, {"y": 0}]

