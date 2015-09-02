from yt.wrapper.common import parse_bool
from yt.wrapper.operation_commands import add_failed_operation_stderrs_to_error_message
from yt.wrapper.table import TablePath
import yt.wrapper as yt
import yt.logger as logger

from helpers import TEST_DIR, get_test_file_path, check

import sys
import time
import string
import random
import logging
import pytest

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


@pytest.mark.usefixtures("yt_env")
class TestOperations(object):
    def setup(self):
        yt.config["tabular_data_format"] = yt.format.DsvFormat()

    def random_string(self, length):
        char_set = string.ascii_lowercase + string.digits + string.ascii_uppercase
        return "".join(random.sample(char_set, length))

    def test_merge(self):
        tableX = TEST_DIR + "/tableX"
        tableY = TEST_DIR + "/tableY"
        dir = TEST_DIR + "/dir"
        res_table = dir + "/other_table"

        yt.write_table(tableX, ["x=1\n"])
        yt.write_table(tableY, ["y=2\n"])

        with pytest.raises(yt.YtError):
            yt.run_merge([tableX, tableY], res_table)
        with pytest.raises(yt.YtError):
            yt.run_merge([tableX, tableY], res_table)

        yt.mkdir(dir)
        yt.run_merge([tableX, tableY], res_table)
        check(["x=1\n", "y=2\n"], yt.read_table(res_table))

        yt.run_merge(tableX, res_table)
        assert not parse_bool(yt.get_attribute(res_table, "sorted"))
        check(["x=1\n"], yt.read_table(res_table))

        yt.run_sort(tableX, sort_by="x")
        yt.run_merge(tableX, res_table)
        assert parse_bool(yt.get_attribute(res_table, "sorted"))
        check(["x=1\n"], yt.read_table(res_table))

    def test_auto_merge(self):
        table = TEST_DIR + "/table"
        other_table = TEST_DIR + "/other_table"
        yt.write_table(table, ["x={0}\n".format(i) for i in xrange(6)])

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
        yt.write_table(table, ["x={0}\ty={1}\n".format(*c) for c in columns])

        with pytest.raises(yt.YtError):
            yt.run_sort([table, other_table], other_table, sort_by=["y"])

        yt.run_sort(table, other_table, sort_by=["x"])
        assert [{"x": x, "y": y} for x, y in sorted(columns, key=lambda c: c[0])] == \
               map(yt.loads_row, yt.read_table(other_table))

        yt.run_sort(table, sort_by=["x"])
        assert list(yt.read_table(table)) == list(yt.read_table(other_table))

        # Sort again and check that everything is ok
        yt.run_sort(table, sort_by=["x"])
        assert list(yt.read_table(table)) == list(yt.read_table(other_table))

        yt.run_sort(table, sort_by=["y"])
        assert [{"x": x, "y": y} for x, y in sorted(columns, key=lambda c: c[1])] == \
               map(yt.loads_row, yt.read_table(table))

        assert yt.is_sorted(table)

        with pytest.raises(yt.YtError):
            yt.run_sort(table, sort_by=None)

    def test_run_operation(self):
        table = TEST_DIR + "/table"
        other_table = TEST_DIR + "/other_table"
        yt.write_table(table, ["x=1\n", "x=2\n"])

        yt.run_map("cat", table, table)
        check(["x=1\n", "x=2\n"], yt.read_table(table))
        yt.run_sort(table, sort_by=["x"])
        with pytest.raises(yt.YtError):
            yt.run_reduce("cat", table, [], reduce_by=["x"])

        yt.run_reduce("cat", table, table, reduce_by=["x"])
        check(["x=1\n", "x=2\n"], yt.read_table(table))

        with pytest.raises(yt.YtError):
            yt.run_map("cat", table, table, table_writer={"max_row_weight": 1})

        yt.run_map("grep 2", table, other_table)
        check(["x=2\n"], yt.read_table(other_table))

        with pytest.raises(yt.YtError):
            yt.run_map("cat", [table, table + "xxx"], other_table)

        with pytest.raises(yt.YtError):
            yt.run_reduce("cat", table, other_table, reduce_by=None)

        # Run reduce on unsorted table
        with pytest.raises(yt.YtError):
            yt.run_reduce("cat", other_table, table, reduce_by=["x"])

        yt.write_table(table, map(yt.dumps_row,
                                  [{"a": 12,  "b": "ignat"},
                                             {"b": "max"},
                                   {"a": "x", "b": "name", "c": 0.5}]))
        yt.run_map("PYTHONPATH=. ./capitalize_b.py",
                   TablePath(table, columns=["b"]), other_table,
                   files=get_test_file_path("capitalize_b.py"))
        records = yt.read_table(other_table, raw=False)
        assert sorted([rec["b"] for rec in records]) == ["IGNAT", "MAX", "NAME"]
        assert sorted([rec["c"] for rec in records]) == []

        with pytest.raises(yt.YtError):
            yt.run_map("cat", table, table, local_files=get_test_file_path("capitalize_b.py"),
                                            files=get_test_file_path("capitalize_b.py"))
        with pytest.raises(yt.YtError):
            yt.run_map("cat", table, table, yt_files=get_test_file_path("capitalize_b.py"),
                                            file_paths=get_test_file_path("capitalize_b.py"))

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

        #@yt.simple
        #def identity(rec):
        #    yield rec

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

        yt.write_table(table, ["x=1\n", "y=2\n"])
        yt.run_map(change_x, table, table, format=None)
        check(yt.read_table(table), ["x=2\n", "y=2\n"])

        yt.write_table(table, ["x=1\n", "y=2\n"])
        yt.run_map(change_x, table, table)
        check(yt.read_table(table), ["x=2\n", "y=2\n"])

        for mode in ["method", "staticmethod", "classmethod"]:
            yt.write_table(table, ["x=1\n", "y=2\n"])
            yt.run_map(ChangeX__(mode), table, table)
            check(yt.read_table(table), ["x=2\n", "y=2\n"])

        yt.write_table(table, ["x=1\n", "y=2\n"])
        yt.run_map(TMapperWithMetaclass().map, table, table)
        check(yt.read_table(table), ["x=2\n", "y=2\n"])

        yt.write_table(table, ["x=2\n", "x=2\ty=2\n"])
        yt.run_sort(table, sort_by=["x"])
        yt.run_reduce(sum_y, table, table, reduce_by=["x"])
        check(yt.read_table(table), ["y=3\tx=2\n"])

        yt.write_table(table, ["x=1\n", "y=2\n"])
        yt.run_map(change_field, table, table)
        check(yt.read_table(table), ["z=8\n", "z=8\n"])

        yt.write_table(table, ["x=1\n", "x=2\n", "x=3\n"])
        yt.run_map(sum_x, table, table)
        check(yt.read_table(table), ["sum=6\n"])

        #yt.run_map(identity, table, table)
        #check(yt.read_table(table), ["sum=6\n"])

        yt.write_table(table, ["x=3\n", "x=3\n", "x=3\n"])
        yt.run_map(sum_x_raw, table, table)
        check(yt.read_table(table), ["sum=9\n"])

        yt.write_table(table, ["x=1\n", "y=2\n"])
        op = yt.run_map(write_statistics, table, table, format=None, sync=False)
        op.wait()
        assert op.get_job_statistics()["custom"] == {"row_count": {"$": {"completed": {"map": {"count": 1, "max": 1, "sum": 1, "min": 1}}}}}
        check(yt.read_table(table), ["x=1\n", "y=2\n"])

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
        yt.config.DEFAULT_STRATEGY = yt.WaitStrategy(print_progress=True)
        old_level = logger.LOGGER.level
        logger.LOGGER.setLevel(logging.INFO)
        try:
            yt.write_table(table, ["0\ta\tA\n", "1\tb\tB\n"])
            yt.run_map(reformat, table, other_table, output_format=yt.format.DsvFormat())
            assert sorted(yt.read_table(other_table, format="dsv")) == \
                   ["k=0\ts=a\tv=A\n", "k=1\ts=b\tv=B\n"]
        finally:
            yt.config["tabular_data_format"] = yt.format.DsvFormat()
            yt.config.DEFAULT_STRATEGY = yt.WaitStrategy(print_progress=False)
            logger.LOGGER.setLevel(old_level)

        yt.config["tabular_data_format"] = None
        try:
            yt.write_table(table, ["1\t2\t3\n"], format="<has_subkey=true>yamr")
            yt.run_map(reformat, table, table, input_format="<has_subkey=true>yamr", output_format="dsv")
            yt.run_map("cat", table, table, input_format="dsv", output_format="dsv")
            assert list(yt.read_table(table, format=yt.format.DsvFormat())) == ["k=1\ts=2\tv=3\n"]
        finally:
            yt.config["tabular_data_format"] = yt.format.DsvFormat()

    def test_python_operations_io(self):
        """ All access (except read-only) to stdin/out during the operation should be disabled """
        table = TEST_DIR + "/table_io_test"

        yt.write_table(table, ["x=1\n", "y=2\n"])

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

    def test_many_output_tables(self):
        table = TEST_DIR + "/table"
        output_tables = []
        for i in xrange(10):
            output_tables.append(TEST_DIR + "/temp%d" % i)
        append_table = TEST_DIR + "/temp_special"
        yt.write_table(table, ["x=1\ty=1\n"])
        yt.write_table(append_table, ["x=1\ty=1\n"])

        yt.run_map("PYTHONPATH=. ./many_output.py yt",
                   table,
                   output_tables + [TablePath(append_table, append=True)],
                   files=get_test_file_path("many_output.py"))

        for table in output_tables:
            assert yt.records_count(table) == 1
        check(["x=1\ty=1\n", "x=10\ty=10\n"], yt.read_table(append_table))

    def test_attached_mode(self):
        table = TEST_DIR + "/table"

        yt.config["detached"] = 0
        try:
            yt.write_table(table, ["x=1\n"])
            yt.run_map("cat", table, table)
            check(yt.read_table(table), ["x=1\n"])
            yt.run_merge(table, table)
            check(yt.read_table(table), ["x=1\n"])
        finally:
            yt.config["detached"] = 1

    def test_abort_operation(self):
        table = TEST_DIR + "/table"
        op = yt.run_map("sleep 10; cat", table, table, sync=False)
        op.abort()
        assert op.get_state() == "aborted"

    def test_suspend_resume(self):
        table = TEST_DIR + "/table"
        yt.write_table(table, ["key=1\n"])
        try:
            op = yt.run_map_reduce("sleep 0.5; cat", "sleep 0.5; cat", table, table, sync=False, reduce_by=["key"])
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
        yt.write_table(table, ["x=1\n", "y=2\n"])

        yt.run_map_reduce(mapper=None, reduce_combiner="cat", reducer="cat", reduce_by=["x"],
                          source_table=table, destination_table=output_table)
        check(["x=1\n", "y=2\n"], sorted(list(yt.read_table(table))))

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
        yt.write_table(table, ["x=1\ty=2\n"])

        yt.run_map(foo, table, table,
                   input_format=yt.create_format("<key_column_names=[\"y\"]>yamred_dsv"),
                   output_format=yt.YamrFormat(has_subkey=False, lenval=False))
        check(["key=2\tvalue=x=1\n"], sorted(list(yt.read_table(table))))

    def test_schemaful_dsv(self):
        def foo(rec):
            yield rec

        table = TEST_DIR + "/table"
        yt.write_table(table, ["x=1\ty=2\n", "x=\\n\tz=3\n"])
        check(["1\n", "\\n\n"],
                   sorted(list(yt.read_table(table, format=yt.SchemafulDsvFormat(columns=["x"])))))

        yt.run_map(foo, table, table, format=yt.SchemafulDsvFormat(columns=["x"]))
        check(["x=1\n", "x=\\n\n"], sorted(list(yt.read_table(table))))

    @add_failed_operation_stderrs_to_error_message
    def test_reduce_aggregator(self):
        table = TEST_DIR + "/table"
        other_table = TEST_DIR + "/other_table"
        yt.write_table(table, ["x=1\ty=2\n", "x=0\ty=3\n", "x=1\ty=4\n"])

        @yt.reduce_aggregator
        def reducer(row_groups):
            sum_y = 0
            for k, rows in row_groups:
                for row in rows:
                    sum_y += int(row["y"])
            yield {"sum_y": sum_y}

        yt.run_sort(table, sort_by=["x"])
        yt.run_reduce(reducer, table, other_table, reduce_by=["x"])
        assert ["sum_y=9\n"] == list(yt.read_table(other_table))

    def test_operation_receives_spec_from_config(self):
        memory_limit = yt.config["memory_limit"]
        yt.config["memory_limit"] = 123
        check_input_fully_consumed = yt.config["yamr_mode"]["check_input_fully_consumed"]
        yt.config["yamr_mode"]["check_input_fully_consumed"] = not check_input_fully_consumed
        use_yamr_descriptors = yt.config["yamr_mode"]["use_yamr_style_destination_fds"]
        yt.config["yamr_mode"]["use_yamr_style_destination_fds"] = not use_yamr_descriptors

        table = TEST_DIR + "/table"
        yt.write_table(table, ["x=1\n"])
        try:
            op = yt.run_map("cat", table, table, sync=False)
            spec = yt.get_attribute("//sys/operations/{0}".format(op.id), "spec")
            assert spec["mapper"]["memory_limit"] == 123
            assert parse_bool(spec["mapper"]["check_input_fully_consumed"]) != check_input_fully_consumed
            assert parse_bool(spec["mapper"]["use_yamr_descriptors"]) != use_yamr_descriptors
        finally:
            if op.get_state() not in ["completed", "failed", "aborted"]:
                op.abort()
            yt.config["memory_limit"] = memory_limit
            yt.config["yamr_mode"]["check_input_fully_consumed"] = check_input_fully_consumed
            yt.config["yamr_mode"]["use_yamr_style_destination_fds"] = use_yamr_descriptors

    # TODO(ignat): replace timeout with scheduler-side option
    #def test_wait_strategy_timeout(self):
    #    records = ["x=1\n", "y=2\n", "z=3\n"]
    #    pause = 3.0
    #    sleeep = "sleep {0}; cat > /dev/null".format(pause)
    #    desired_timeout = 1.0

    #    table = TEST_DIR + "/table"
    #    yt.write_table(table, records)

    #    # skip long loading time
    #    yt.run_map(sleeep, table, "//tmp/1", strategy=yt.WaitStrategy(), job_count=1)

    #    start = time.time()
    #    yt.run_map(sleeep, table, "//tmp/1", strategy=yt.WaitStrategy(), job_count=1)
    #    usual_time = time.time() - start
    #    loading_time = usual_time - pause

    #    start = time.time()
    #    with pytest.raises(yt.YtTimeoutError):
    #        yt.run_map(sleeep, table, "//tmp/1",
    #                   strategy=yt.WaitStrategy(timeout=desired_timeout), job_count=1)
    #    timeout_time = time.time() - start
    #    self.assertAlmostEqual(timeout_time, desired_timeout, delta=loading_time)

