from yt.wrapper.py_wrapper import create_modules_archive_default, TempfilesManager

from yt.wrapper.common import parse_bool
from yt.wrapper.operation_commands import add_failed_operation_stderrs_to_error_message
from yt.wrapper.table import TablePath
import yt.wrapper as yt
import yt.logger as logger

from helpers import TEST_DIR, PYTHONPATH, get_test_file_path, check, set_config_option

import os
import sys
import time
import string
import tempfile
import random
import logging
import pytest
try:
    import subprocess32 as subprocess
except ImportError:
    if sys.version_info[:2] <= (2, 6):
        print >>sys.stderr, "Script may not work properly on python of version <= 2.6 " \
                            "because subprocess32 library is not installed."
    import subprocess

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

# Map method for test operations with python entities
def _change_x(rec):
    if "x" in rec:
        rec["x"] = int(rec["x"]) + 1

class ChangeX__(object):
    def __init__(self, mode):
        self.change_x = {
            "method": self._change_x,
            "staticmethod": self._change_x_staticmethod,
            "classmethod": self._change_x_classmethod
            }[mode]

    def __call__(self, rec):
        self.change_x(rec)
        yield rec

    def _change_x(self, rec):
        _change_x(rec)

    @staticmethod
    def _change_x_staticmethod(rec):
        _change_x(rec)

    @classmethod
    def _change_x_classmethod(cls, rec):
        _change_x(rec)

# Map method to test metaclass pickling
from abc import ABCMeta, abstractmethod

class TAbstractClass(object):
  __metaclass__ = ABCMeta

  @abstractmethod
  def __init__(self):
    pass


class TDoSomething(TAbstractClass):
    def __init__(self):
        pass

    def do_something(self, rec):
        _change_x(rec)
        return rec


class TMapperWithMetaclass(object):
  def __init__(self):
    self.some_external_code = TDoSomething()

  def map(self, rec):
    yield self.some_external_code.do_something(rec)

class CreateModulesArchive(object):
    def __call__(self, tempfiles_manager=None):
        return create_modules_archive_default(tempfiles_manager, None)


@pytest.mark.usefixtures("yt_env")
class TestOperations(object):
    def setup(self):
        yt.config["tabular_data_format"] = yt.format.JsonFormat()

    def random_string(self, length):
        char_set = string.ascii_lowercase + string.digits + string.ascii_uppercase
        return "".join(random.sample(char_set, length))

    def test_merge(self):
        tableX = TEST_DIR + "/tableX"
        tableY = TEST_DIR + "/tableY"
        dir = TEST_DIR + "/dir"
        res_table = dir + "/other_table"

        yt.write_table(tableX, [{"x": 1}])
        yt.write_table(tableY, [{"y": 2}])

        with pytest.raises(yt.YtError):
            yt.run_merge([tableX, tableY], res_table)
        with pytest.raises(yt.YtError):
            yt.run_merge([tableX, tableY], res_table)

        yt.mkdir(dir)
        yt.run_merge([tableX, tableY], res_table)
        check([{"x": 1}, {"y": 2}], yt.read_table(res_table), ordered=False)

        yt.run_merge(tableX, res_table)
        assert not parse_bool(yt.get_attribute(res_table, "sorted"))
        check([{"x": 1}], yt.read_table(res_table))

        yt.run_sort(tableX, sort_by="x")
        yt.run_merge(tableX, res_table)
        assert parse_bool(yt.get_attribute(res_table, "sorted"))
        check([{"x": 1}], yt.read_table(res_table))

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
        yt.write_table(table, ["x={0}\ty={1}\n".format(*c) for c in columns], format=yt.DsvFormat(), raw=True)

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
                           {"a": 12,  "b": "ignat"},
                                     {"b": "max"},
                           {"a": "x", "b": "name", "c": 0.5}
                       ])
        yt.run_map("PYTHONPATH=. ./capitalize_b.py",
                   TablePath(table, columns=["b"]), other_table,
                   files=get_test_file_path("capitalize_b.py"),
                   format=yt.DsvFormat())
        records = yt.read_table(other_table, raw=False)
        assert sorted([rec["b"] for rec in records]) == ["IGNAT", "MAX", "NAME"]
        assert sorted([rec["c"] for rec in records]) == []

        with pytest.raises(yt.YtError):
            yt.run_map("cat", table, table, local_files=get_test_file_path("capitalize_b.py"),
                                            files=get_test_file_path("capitalize_b.py"))
        with pytest.raises(yt.YtError):
            yt.run_map("cat", table, table, yt_files=get_test_file_path("capitalize_b.py"),
                                            file_paths=get_test_file_path("capitalize_b.py"))

    def test_run_standalone_binary(self):
        table = TEST_DIR + "/table"
        other_table = TEST_DIR + "/other_table"
        yt.write_table(table, [{"x": 1}, {"x": 2}])

        binary = get_test_file_path("standalone_binary.py")
        subprocess.check_call(["python", binary, table, other_table], env={"YT_PROXY": yt.config["proxy"]["url"], "PYTHONPATH": PYTHONPATH}, stderr=sys.stderr)
        check([{"x": 1}, {"x": 2}], yt.read_table(other_table))

    @add_failed_operation_stderrs_to_error_message
    def test_run_join_operation(self, yt_env):
        if yt.config["api_version"] == "v2":
            pytest.skip()

        table1 = TEST_DIR + "/first"
        yt.write_table("<sorted_by=[x]>" + table1, [{"x": 1}])
        table2 = TEST_DIR + "/second"
        yt.write_table("<sorted_by=[x]>" + table2, [{"x": 2}])
        unsorted_table = TEST_DIR + "/unsorted_table"
        yt.write_table(unsorted_table, [{"x": 3}])
        table = TEST_DIR + "/table"

        if yt_env.version >= "0.17.3":
            yt.run_join_reduce("cat", ["<primary=true>" + table1, table2], table, join_by=["x"])
            check([{"x": 1}], yt.read_table(table))

            # Run join-reduce without join_by
            with pytest.raises(yt.YtError):
                yt.run_join_reduce("cat", ["<primary=true>" + table1, table2], table)

            # Run join-reduce on unsorted table
            with pytest.raises(yt.YtError):
                yt.run_join_reduce("cat", ["<primary=true>" + unsorted_table, table2], table, join_by=["x"])

        if yt_env.version >= "0.17.5" and yt_env.version < "0.18.0" or yt_env.version >= "18.2.0":
            yt.run_join_reduce("cat", [table1, "<foreign=true>" + table2], table, join_by=["x"])
            check([{"x": 1}], yt.read_table(table))

            # Run join-reduce without join_by
            with pytest.raises(yt.YtError):
                yt.run_join_reduce("cat", [table1, "<foreign=true>" + table2], table)

            # Run join-reduce on unsorted table
            with pytest.raises(yt.YtError):
                yt.run_join_reduce("cat", [unsorted_table, "<foreign=true>" + table2], table, join_by=["x"])

        if yt_env.version >= "18.2.0":
            yt.write_table("<sorted_by=[x;y]>" + table1, [{"x": 1, "y": 1}])
            yt.write_table("<sorted_by=[x]>" + table2, [{"x": 1}])

            def func(key, rows):
                assert key.keys() == ["x"]
                for row in rows:
                    del row["@table_index"]
                    yield row

            yt.run_reduce(func, [table1, "<foreign=true>" + table2], table,
                          reduce_by=["x","y"], join_by=["x"])
            check([{"x": 1, "y": 1}, {"x": 1}], yt.read_table(table))

            # Reduce with join_by, but without foreign tables
            with pytest.raises(yt.YtError):
                yt.run_reduce("cat", [table1, table2], table, join_by=["x"])

    @add_failed_operation_stderrs_to_error_message
    def test_python_operations(self):
        def change_x(rec):
            if "x" in rec:
                rec["x"] = int(rec["x"]) + 1
            yield rec

        def sum_y(key, recs):
            sum = 0
            for rec in recs:
                sum += int(rec.get("y", 1))
            yield {"x": key["x"], "y": sum}

        @yt.raw
        def change_field(line):
            yield "z=8\n"

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

        def write_statistics(row):
            yt.write_statistics({"row_count": 1})
            yt.get_blkio_cgroup_statistics()
            yt.get_memory_cgroup_statistics()
            yield row

        table = TEST_DIR + "/table"

        yt.write_table(table, [{"x": 1}, {"y": 2}])

        yt.run_map(change_x, table, table, format=None)
        check(yt.read_table(table), [{"x": 2}, {"y": 2}], ordered=False)

        yt.write_table(table, [{"x": 1}, {"y": 2}])
        yt.run_map(change_x, table, table)
        check(yt.read_table(table),  [{"x": 2}, {"y": 2}])

        for mode in ["method", "staticmethod", "classmethod"]:
            yt.write_table(table, [{"x": 1}, {"y": 2}])
            yt.run_map(ChangeX__(mode), table, table)
            check(yt.read_table(table), [{"x": 2}, {"y": 2}], ordered=False)

        yt.write_table(table, [{"x": 1}, {"y": 2}])
        yt.run_map(TMapperWithMetaclass().map, table, table)
        check(yt.read_table(table), [{"x": 2}, {"y": 2}], ordered=False)

        yt.write_table(table, [{"x": 2}, {"x": 2, "y": 2}])
        yt.run_sort(table, sort_by=["x"])
        yt.run_reduce(sum_y, table, table, reduce_by=["x"])
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

        yt.write_table(table, [{"x": 1}, {"y": 2}])
        op = yt.run_map(write_statistics, table, table, format=None, sync=False)
        op.wait()
        assert sorted(op.get_job_statistics()["custom"].keys()) == sorted(["row_count", "python_job_preparation_time"])
        assert op.get_job_statistics()["custom"]["row_count"] == {"$": {"completed": {"map": {"count": 2, "max": 1, "sum": 2, "min": 1}}}}
        check(yt.read_table(table), [{"x": 1}, {"y": 2}], ordered=False)

    @add_failed_operation_stderrs_to_error_message
    def test_python_operations_in_local_mode(self):
        old_value = yt.config["pickling"]["local_mode"]
        yt.config["pickling"]["local_mode"] = True

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
            yt.config["pickling"]["local_mode"] = old_value
            yt.config["local_temp_directory"] = old_tmp_dir

    @add_failed_operation_stderrs_to_error_message
    def test_cross_format_operations(self):
        @yt.raw
        def reformat(rec):
            values = rec.strip().split("\t", 2)
            yield "\t".join("=".join([k, v]) for k, v in zip(["k", "s", "v"], values)) + "\n"

        table = TEST_DIR + "/table"
        other_table = TEST_DIR + "/other_table"

        yt.config["tabular_data_format"] = yt.format.YamrFormat(has_subkey=True)

        # Enable progress printing in this test
        old_level = logger.LOGGER.level
        logger.LOGGER.setLevel(logging.INFO)
        try:
            yt.write_table(table, ["0\ta\tA\n", "1\tb\tB\n"], raw=True)
            yt.run_map(reformat, table, other_table, output_format=yt.format.DsvFormat())
            assert sorted(yt.read_table(other_table, format="dsv", raw=True)) == \
                   ["k=0\ts=a\tv=A\n", "k=1\ts=b\tv=B\n"]
        finally:
            yt.config["tabular_data_format"] = None
            logger.LOGGER.setLevel(old_level)

        yt.write_table(table, ["1\t2\t3\n"], format="<has_subkey=true>yamr", raw=True)
        yt.run_map(reformat, table, table, input_format="<has_subkey=true>yamr", output_format="dsv")
        yt.run_map("cat", table, table, input_format="dsv", output_format="dsv")
        assert list(yt.read_table(table, format=yt.format.DsvFormat(), raw=True)) == ["k=1\ts=2\tv=3\n"]

    def test_python_operations_io(self):
        """ All access (except read-only) to stdin/out during the operation should be disabled """
        table = TEST_DIR + "/table_io_test"

        yt.write_table(table, [{"x": 1}, {"y": 2}])

        def print_(rec):
            print 'message'

        @yt.raw
        def write(rec):
            sys.stdout.write('message')

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
    def test_many_output_tables(self):
        table = TEST_DIR + "/table"
        output_tables = []
        for i in xrange(10):
            output_tables.append(TEST_DIR + "/temp%d" % i)
        append_table = TEST_DIR + "/temp_special"
        yt.write_table(table, [{"x": "1", "y": "1"}])
        yt.write_table(append_table, [{"x": "1", "y": "1"}])

        yt.run_map("PYTHONPATH=. ./many_output.py yt",
                   table,
                   output_tables + [TablePath(append_table, append=True)],
                   files=get_test_file_path("many_output.py"),
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

    def test_attached_mode_op_aborted(self, yt_env):
        script = """
import yt.wrapper as yt
import sys

input, output, pythonpath = sys.argv[1:4]
yt.config["proxy"]["request_retry_timeout"] = 2000
yt.config["proxy"]["request_retry_count"] = 1
yt.config["detached"] = False
op = yt.run_map("sleep 100", input, output, format="json", spec={"mapper": {"environment": {"PYTHONPATH": pythonpath}}}, sync=False)
print op.id

"""
        dir_ = yt_env.env.path_to_run
        with tempfile.NamedTemporaryFile(dir=dir_, prefix="mapper", delete=False) as file:
            file.write(script)

        table = TEST_DIR + "/table"
        yt.write_table(table, [{"x": 1}])

        op_id = subprocess.check_output(["python", file.name, table, table, PYTHONPATH],
                                        env={"YT_PROXY": yt.config["proxy"]["url"], "PYTHONPATH": PYTHONPATH}, stderr=sys.stderr).strip()
        time.sleep(3)

        assert yt.get("//sys/operations/{0}/@state".format(op_id)) == "aborted"

    def test_abort_operation(self):
        table = TEST_DIR + "/table"
        yt.write_table(table, [{"x": 1}])
        op = yt.run_map("sleep 15; cat", table, table, sync=False)
        op.abort()
        assert op.get_state() == "aborted"

    def test_complete_operation(self):
        if yt.config["api_version"] == "v2":
            pytest.skip()
        table = TEST_DIR + "/table"
        yt.write_table(table, [{"x": 1}])
        op = yt.run_map("sleep 15; cat", table, table, sync=False)
        while not op.get_state().is_running():
            time.sleep(0.2)
        op.complete()
        assert op.get_state() == "completed"

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

            time.sleep(0.5)
            op.suspend()
            assert op.get_state() == "running"
            time.sleep(2.5)
            assert op.get_state() == "running"
            op.resume()
            op.wait(timeout=10)
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
        check([{"x": 1}, {"y": 2}], sorted(list(yt.read_table(table))))

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

    @add_failed_operation_stderrs_to_error_message
    def test_yamred_dsv(self):
        def foo(rec):
            yield rec

        table = TEST_DIR + "/table"
        yt.write_table(table, [{"x": "1", "y": "2"}])

        yt.run_map(foo, table, table,
                   input_format=yt.create_format("<key_column_names=[\"y\"]>yamred_dsv"),
                   output_format=yt.YamrFormat(has_subkey=False, lenval=False))
        check([{"key": "2", "value": "x=1"}], sorted(list(yt.read_table(table))))

    def test_schemaful_dsv(self):
        def foo(rec):
            yield rec

        table = TEST_DIR + "/table"
        yt.write_table(table, ["x=1\ty=2\n", "x=\\n\tz=3\n"], raw=True, format=yt.DsvFormat())
        check(["1\n", "\\n\n"],
              sorted(list(yt.read_table(table, format=yt.SchemafulDsvFormat(columns=["x"]), raw=True))))

        yt.run_map(foo, table, table, format=yt.SchemafulDsvFormat(columns=["x"]))
        check(["x=1\n", "x=\\n\n"], sorted(list(yt.read_table(table, format=yt.DsvFormat(), raw=True))))

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
            spec = yt.get_attribute("//sys/operations/{0}".format(op.id), "spec")
            assert spec["mapper"]["memory_limit"] == 123
            assert parse_bool(spec["mapper"]["check_input_fully_consumed"]) != check_input_fully_consumed
            assert parse_bool(spec["mapper"]["use_yamr_descriptors"]) != use_yamr_descriptors
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
        assert [{"sum": 1}, {"sum": 2}, {"sum": 9}] == sorted(yt.read_table(other_table))

    @add_failed_operation_stderrs_to_error_message
    def test_create_modules_archive(self):
        def foo(rec):
            yield rec

        table = TEST_DIR + "/table"

        try:
            yt.config["pickling"]["create_modules_archive_function"] = \
                lambda tempfiles_manager: create_modules_archive_default(tempfiles_manager, None)
            yt.run_map(foo, table, table)

            with TempfilesManager(remove_temp_files=True) as tempfiles_manager:
                yt.config["pickling"]["create_modules_archive_function"] = lambda: create_modules_archive_default(tempfiles_manager, None)
                yt.run_map(foo, table, table)

            with TempfilesManager(remove_temp_files=True) as tempfiles_manager:
                yt.config["pickling"]["create_modules_archive_function"] = lambda: create_modules_archive_default(tempfiles_manager, None)[0]["filename"]
                yt.run_map(foo, table, table)

            yt.config["pickling"]["create_modules_archive_function"] = CreateModulesArchive()
            yt.run_map(foo, table, table)

        finally:
            yt.config["pickling"]["create_modules_archive_function"] = None

    def test_pickling(self):
        def foo(rec):
            import test_module
            assert test_module.TEST == 1
            yield rec

        with open("/tmp/test_module.py", "w") as fout:
            fout.write("TEST = 1")

        with set_config_option("pickling/additional_files_to_archive", [("/tmp/test_module.py", "test_module.py")]):
            table = TEST_DIR + "/table"
            yt.write_table(table, [{"x": 1}])
            yt.run_map(foo, table, table)

    @add_failed_operation_stderrs_to_error_message
    def test_is_inside_job(self):
        table = TEST_DIR + "/table"
        yt.write_table(table, [{"x": 1}])

        def mapper(rec):
            yield {"flag": str(yt.is_inside_job()).lower()}

        yt.run_map(mapper, table, table)
        assert not yt.is_inside_job()
        assert list(yt.read_table(table)) == [{"flag": "true"}]

    def test_retrying_operation_count_limit_exceeded(self):
        # TODO(ignat): Rewrite test without sleeps.
        old_value = yt.config["start_operation_retries"]["retry_timeout"]
        yt.config["start_operation_retries"]["retry_timeout"] = 2000

        try:
            table = TEST_DIR + "/table"
            yt.write_table(table, [{"x": 1}, {"x": 2}])

            start_time = time.time()
            ops = []
            for i in xrange(5):
                ops.append(yt.run_map("cat; sleep 5", table, TEST_DIR + "/output_" + str(i), sync=False))

            assert time.time() - start_time < 5.0
            ops.append(yt.run_map("cat; sleep 5", table, TEST_DIR + "/output", sync=False))
            assert time.time() - start_time > 5.0

            for op in ops:
                op.wait()

            assert time.time() - start_time > 10.0

        finally:
            yt.config["start_operation_retries"]["retry_timeout"] = old_value

    @add_failed_operation_stderrs_to_error_message
    def test_reduce_key_modification(self):
        def reducer(key, recs):
            rec = next(recs)
            key["x"] = int(rec["y"]) + 10
            yield key

        def reducer_that_yileds_key(key, recs):
            for rec in recs:
                pass
            yield key

        table = TEST_DIR + "/table"
        yt.write_table(table, [{"x": 1, "y": 1}, {"x": 1, "y": 2}, {"x": 2, "y": 3}])
        yt.run_sort(table, table, sort_by=["x"])

        with pytest.raises(yt.YtOperationFailedError):
            yt.run_reduce(reducer, table, TEST_DIR + "/other", reduce_by=["x"], format="json")

        yt.run_reduce(reducer_that_yileds_key, table, TEST_DIR + "/other", reduce_by=["x"], format="json")
        check([{"x": 1}, {"x": 2}], yt.read_table(TEST_DIR + "/other"), ordered=False)

    def test_disable_yt_accesses_from_job(self, yt_env):
        first_script = """\
import yt.wrapper as yt

def mapper(rec):
    yield rec

yt.config["proxy"]["url"] = "{0}"
yt.config["pickling"]["enable_tmpfs_archive"] = False
print yt.run_map(mapper, "{1}", "{2}", spec={3}, sync=False).id
"""
        second_script = """\
import yt.wrapper as yt

def mapper(rec):
    yt.get("//@")
    yield rec

if __name__ == "__main__":
    yt.config["proxy"]["url"] = "{0}"
    yt.config["pickling"]["enable_tmpfs_archive"] = False
    print yt.run_map(mapper, "{1}", "{2}", spec={3}, sync=False).id
"""
        table = TEST_DIR + "/table"
        yt.write_table(table, [{"x": 1}, {"x": 2}])

        dir_ = yt_env.env.path_to_run
        for script in [first_script, second_script]:
            with tempfile.NamedTemporaryFile(dir=dir_, prefix="mapper", delete=False) as f:
                mapper = script.format(yt.config["proxy"]["url"],
                                       table,
                                       TEST_DIR + "/other_table",
                                       str({"max_failed_job_count": 1}))
                f.write(mapper)

            op_id = subprocess.check_output(["python", f.name]).strip()
            op_path = "//sys/operations/{0}".format(op_id)
            while not yt.exists(op_path) \
                    or yt.get(op_path + "/@state") not in ["aborted", "failed", "completed"]:
                time.sleep(0.2)
            assert yt.get(op_path + "/@state") == "failed"

            job_id = yt.list(op_path + "/jobs", attributes=["error"])[0]
            stderr_path = os.path.join(op_path, "jobs", job_id, "stderr")

            while not yt.exists(stderr_path):
                time.sleep(0.2)

            assert "Did you forget to surround" in yt.read_file(stderr_path).read()

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

        yt.run_map(mapper, [tableA, tableB], outputTable, format=yt.YsonFormat(), spec={"job_io": {"control_attributes": {"enable_row_index": True}}, "ordered": True})

        result = sorted(list(yt.read_table(outputTable, raw=False, format=yt.YsonFormat(process_table_index=False))),
                        key=lambda item: (item["table_index"], item["row_index"]))

        assert [
            {"table_index": 0, "row_index": 0, "x": 1},
            {"table_index": 0, "row_index": 1, "y": 1},
            {"table_index": 1, "row_index": 0, "x": 2},
        ] == result

    def test_reduce_sort_by(self):
        table = TEST_DIR + "/table"
        yt.write_table(table, [{"x": 1, "y": 1}])
        yt.run_sort(table, sort_by=["x", "y"])
        op = yt.run_reduce("cat", table, table, format=yt.JsonFormat(), reduce_by=["x"], sort_by=["x", "y"])
        assert "sort_by" in op.get_attributes()["spec"]
