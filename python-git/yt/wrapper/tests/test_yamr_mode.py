from .helpers import (TEST_DIR, get_tests_location, get_tests_sandbox, get_test_file_path,
                      get_environment_for_binary_test, set_config_option, get_python)

from yt.wrapper.table_commands import copy_table, move_table
from yt.wrapper.operation_commands import add_failed_operation_stderrs_to_error_message
from yt.wrapper.common import parse_bool
from yt.wrapper import Record, dumps_row, TablePath
from yt.common import flatten, makedirp
import yt.yson as yson
import yt.subprocess_wrapper as subprocess

from yt.packages.six import b
from yt.packages.six.moves import xrange, map as imap, zip as izip

import yt.wrapper as yt

import pytest

import os
import uuid
import string
from itertools import starmap
from functools import reduce

@pytest.mark.usefixtures("yt_env_for_yamr")
class TestYamrMode(object):
    def get_temp_records(self):
        columns = [string.digits, reversed(string.ascii_lowercase[:10]), string.ascii_uppercase[:10]]
        return list(imap(dumps_row, starmap(Record, imap(flatten, reduce(izip, columns)))))

    def test_get_smart_format(self):
        from yt.wrapper.table_commands import _get_format_from_tables as get_format

        existing_table = TEST_DIR + "/table"
        yt.create("table", existing_table)
        not_existing_table = TEST_DIR + "/not_existing"

        yamred_dsv_table = TEST_DIR + "/yamred_dsv_table"
        yamred_dsv_format = yson.to_yson_type("yamred_dsv", attributes={"has_subkey": True})
        yt.create("table", yamred_dsv_table, attributes={"_format": yamred_dsv_format})

        yson_table = TEST_DIR + "/yson_table"
        yt.create("table", yson_table, attributes={"_format": "yson"})

        assert get_format([], ignore_unexisting_tables=False) is None
        assert get_format([], ignore_unexisting_tables=True) is None

        assert get_format([existing_table], ignore_unexisting_tables=False) is None
        assert get_format([existing_table], ignore_unexisting_tables=True) is None

        result = get_format([yamred_dsv_table], ignore_unexisting_tables=False)
        assert str(result.to_yson_type()) == "yamred_dsv"
        assert parse_bool(result.attributes["has_subkey"])

        assert get_format([yson_table], ignore_unexisting_tables=False).name() == "yson"

        assert get_format([existing_table, not_existing_table], ignore_unexisting_tables=False) is None

    def test_copy_move(self):
        move_table(TEST_DIR + "/a", TEST_DIR + "/b")
        assert not yt.exists(TEST_DIR + "/a")
        assert not yt.exists(TEST_DIR + "/b")

        table = TEST_DIR + "/table"
        yt.write_table(table, self.get_temp_records())
        other_table = TEST_DIR + "/other_table"
        yt.create("table", other_table)

        copy_table(table, other_table)
        assert sorted(self.get_temp_records()) == sorted(yt.read_table(other_table))

        move_table(other_table, other_table)
        assert sorted(self.get_temp_records()) == sorted(yt.read_table(other_table))

        copy_table(table, other_table)
        assert sorted(self.get_temp_records()) == sorted(yt.read_table(other_table))

        copy_table(table, TablePath(other_table, append=True))
        assert sorted(list(self.get_temp_records()) * 2) == sorted(yt.read_table(other_table))

        yt.run_sort(table, table)
        copy_table(table, TablePath(other_table, append=True))
        assert sorted(list(self.get_temp_records()) * 3) == sorted(yt.read_table(other_table))

        move_table(table, other_table)
        assert yt.exists(other_table)
        assert not yt.exists(table)
        assert list(yt.read_table(other_table)) == sorted(list(self.get_temp_records()))

        move_table([table, table], other_table)
        assert list(yt.read_table(other_table)) == []

        copy_table(table, table)
        assert not yt.exists(table)

        yt.write_table(table, self.get_temp_records())
        assert yt.exists(table)
        move_table([table, table], other_table)
        assert sorted(list(self.get_temp_records()) * 2) == sorted(yt.read_table(other_table))

        copy_table(table, table)
        assert not yt.exists(table)

        embedded_path = TEST_DIR + "dir/other_dir/table"
        copy_table(table, embedded_path)
        assert not yt.exists(embedded_path)

    @pytest.mark.timeout(1800)
    def test_mapreduce_binary(self, yt_env_for_yamr):
        env = get_environment_for_binary_test(yt_env_for_yamr)
        env["FALSE"] = "false"
        env["TRUE"] = "true"

        sandbox_dir = os.path.join(get_tests_sandbox(), "TestMapreduceBinary_" + uuid.uuid4().hex[:8])
        makedirp(sandbox_dir)
        test_binary = os.path.join(get_tests_location(), "test_mapreduce.sh")

        yt.remove("//home/wrapper_tests", recursive=True)
        proc = subprocess.Popen([test_binary], env=env, cwd=sandbox_dir)
        proc.communicate()
        assert proc.returncode == 0

        env["ENABLE_SCHEMA"] = "1"
        yt.remove("//home/wrapper_tests", recursive=True, force=True)
        proc = subprocess.Popen([test_binary], env=env, cwd=sandbox_dir)
        proc.communicate()
        assert proc.returncode == 0

    def test_empty_write(self):
        table = TEST_DIR + "/table"
        yt.write_table(table, [b"x\ty\tz\n"])
        yt.write_table(table, [])
        assert not yt.exists(table)

    def test_empty_output_table_deletion(self):
        table = TEST_DIR + "/table"
        other_table = TEST_DIR + "/subdir/other_table"

        yt.write_table(table, [b"x\ty\tz\n"])
        yt.run_map("cat 1>&2 2>/dev/null", table, other_table)
        assert not yt.exists(other_table)
        assert not yt.exists(TEST_DIR + "/subdir")

        yt.run_map("cat", TEST_DIR + "/unexisting", other_table)
        assert not yt.exists(other_table)

        yt.run_merge(TEST_DIR + "/unexisting", other_table)
        assert not yt.exists(other_table)

        yt.run_sort(TEST_DIR + "/unexisting", other_table)
        assert not yt.exists(other_table)

        yt.run_sort(TEST_DIR + "/unexisting")
        assert not yt.exists(TEST_DIR + "/unexisting")

        yt.run_map_reduce("cat", "cat", TEST_DIR + "/unexisting", other_table)
        assert not yt.exists(other_table)

    def test_treat_unexisting_as_empty(self):
        table = TEST_DIR + "/table"
        assert yt.row_count(table) == 0
        yt.run_erase(TablePath(table, start_index=0, end_index=5))

    def test_map_reduce_operation(self):
        input_table = TEST_DIR + "/input_table"
        output_table = TEST_DIR + "/output_table"
        yt.write_table(input_table,
                       [
                           b"\1a\t\t\n",
                           b"\1b\t\t\n",
                           b"\1c\t\t\n",
                           b"a b\tc\t\n",
                           b"c c\tc\tc c a\n"
                       ])
        yt.run_map_reduce("{} split.py".format(get_python()),
                          "{} collect.py".format(get_python()),
                          input_table,
                          output_table,
                          map_local_files=get_test_file_path("split.py"),
                          reduce_local_files=get_test_file_path("collect.py"))
        assert sorted(list(yt.read_table(output_table))) == sorted([b"a\t\t2\n", b"b\t\t1\n", b"c\t\t6\n"])

    def test_many_output_tables(self):
        table = TEST_DIR + "/table"
        output_tables = []
        for i in xrange(10):
            output_tables.append(TEST_DIR + "/temp%d" % i)
        append_table = TEST_DIR + "/temp_special"
        yt.write_table(table, [b"1\t1\t1\n"])
        yt.write_table(append_table, [b"1\t1\t1\n"])

        yt.run_map("PYTHONPATH=. {} many_output.py yamr".format(get_python()),
                   table,
                   output_tables + [TablePath(append_table, append=True)],
                   local_files=get_test_file_path("many_output.py"))

        for table in output_tables:
            assert yt.row_count(table) == 1
        assert sorted(yt.read_table(append_table)) == [b"1\t1\t1\n", b"10\t10\t10\n"]

    def test_reduce_unsorted(self):
        input_table = TEST_DIR + "/input_table"
        output_table = TEST_DIR + "/output_table"
        yt.create("table", input_table)
        data = [b"0\ti\tA\n", b"0\th\tB\n", b"0\tg\tC\n", b"0\tf\tD\n", b"0\te\tE\n",
                b"0\td\tF\n", b"0\tc\tG\n", b"0\tb\tH\n", b"0\ta\tI\n"]
        yt.write_table(input_table, data)
        yt.run_reduce("cat", source_table=input_table, destination_table=output_table)
        assert list(yt.read_table(output_table)) == data[::-1]

    def test_run_operations(self):
        table = TEST_DIR + "/table"
        other_table = TEST_DIR + "/other_table"
        yt.write_table(table, [b"0\ta\tA\n", b"1\tb\tB\n", b"2\tc\tC\n"])
        yt.run_map("PYTHONPATH=. {} my_op.py".format(get_python()),
                   table, other_table,
                   local_files=list(imap(get_test_file_path, ["my_op.py", "helpers.py"])))
        assert yt.row_count(other_table) == 2 * yt.row_count(table)

        test_run_operations_dir = os.path.join(get_tests_sandbox(), "test_run_operations")
        makedirp(test_run_operations_dir)

        cpp_file = get_test_file_path("bin.cpp")
        cpp_bin = os.path.join(test_run_operations_dir, "cpp_bin")
        subprocess.check_call(["g++", cpp_file, "-O2", "-static-libgcc", "-L.", "-o", cpp_bin])

        yt.run_sort(table)
        yt.run_reduce("./cpp_bin", table, other_table, local_files=cpp_bin)
        assert sorted(yt.read_table(other_table)) == \
               [b("key{0}\tsubkey\tvalue=value\n".format(i)) for i in xrange(5)]

    @add_failed_operation_stderrs_to_error_message
    def test_python_operations(self):
        def yamr_func(key, records):
            for rec in records:
                pass
            yield yt.Record("10", "20")

        @yt.raw
        def inc_key_yamr(rec):
            rec = yt.loads_row(rec)
            rec.key = str(int(rec.key) + 1)
            yield yt.dumps_row(rec)

        table = TEST_DIR + "/table"
        output_table = TEST_DIR + "/output_table"

        with set_config_option("tabular_data_format", yt.format.YamrFormat(has_subkey=False)):
            yt.write_table(table, [b"a\tb\n"])
            yt.run_map_reduce(mapper=None, reducer=yamr_func,
                              source_table=table, destination_table=output_table, reduce_by="key")
            assert list(yt.read_table(output_table)) == [b"10\t20\n"]

        with pytest.raises(yt.YtError):
            yt.run_map_reduce(mapper=None, reducer=yamr_func,
                              source_table=table, destination_table=output_table, reduce_by="subkey")

        yt.write_table(table, [b"1\t2\t3\n", b"4\t5\t6\n"])
        yt.run_map(inc_key_yamr, table, table)
        assert sorted(yt.read_table(table)) == [b"2\t2\t3\n", b"5\t5\t6\n"]

    def test_lenval_python_operations(self):
        def foo(rec):
            yield rec

        table = TEST_DIR + "/table"
        yt.write_table(table, [b"1\t2\t3\n"])
        yt.run_map(foo, table, table, format=yt.YamrFormat(has_subkey=True, lenval=True))
        assert list(yt.read_table(table)) == [b"1\t2\t3\n"]

    def test_empty_input_tables(self):
        table = TEST_DIR + "/table"
        other_table = TEST_DIR + "/temp_other"
        another_table = TEST_DIR + "/temp_another"

        yt.write_table(table, [b"1\t2\t3\n"])
        yt.run_map("PYTHONPATH=. {} my_op.py".format(get_python()),
                   [table, other_table], another_table,
                   local_files=list(imap(get_test_file_path, ["my_op.py", "helpers.py"])))
        assert not yt.exists(other_table)

    def test_reduce_unexisting_tables(self):
        with set_config_option("yamr_mode/run_map_reduce_if_source_is_not_sorted", False):
            table = TEST_DIR + "/table"
            yt.write_table(table, [b"0\ta\tA\n"])
            yt.run_sort(table)

            output_table = TEST_DIR + "/output_table"
            yt.create("table", output_table)

            yt.run_reduce("cat", [table, TEST_DIR + "/unexisting_table"], output_table)

    def test_reduce_with_output_sorted(self):
        table = TEST_DIR + "/table"
        output_table = TEST_DIR + "/output_table"
        yt.write_table(table, [b"3\t1\t3\n", b"1\t2\t4\n"])
        yt.run_reduce("cat", table, "<sorted_by=[key]>" + output_table)
        assert list(yt.read_table(output_table)) == [b"1\t2\t4\n", b"3\t1\t3\n"]
        assert yt.get_attribute(output_table, "sorted", False)

    def test_reduce_differently_sorted_table(self):
        table = TEST_DIR + "/table"
        yt.write_table(table, [b"1\t2\t3\n"])
        yt.run_sort(table, sort_by=["key"])

        # NB: map-reduce should be run
        yt.run_reduce("cat", table, TEST_DIR + "/other_table", sort_by=["subkey"], reduce_by=["subkey"])
        assert list(yt.read_table(table)) == [b"1\t2\t3\n"]

    def test_throw_on_missing_destination(self):
        table = TEST_DIR + "/table"
        yt.write_table(table, [b"1\t2\t3\n"])
        with pytest.raises(yt.YtError):
            yt.run_map("cat", source_table=table, destination_table=None)
