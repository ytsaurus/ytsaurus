from yt.environment import YTEnv
from yt.wrapper.table_commands import copy_table, move_table
from yt.wrapper.tests.base import YtTestBase, TEST_DIR
from yt.wrapper.operation_commands import add_failed_operation_stderrs_to_error_message
from yt.wrapper.common import parse_bool
import yt.wrapper as yt
from yt.wrapper import Record, dumps_row, TablePath
import yt.wrapper.config as config
from yt.common import flatten
import yt.yson as yson

import os
import string
import subprocess
from itertools import imap, izip, starmap

import pytest

TESTS_LOCATION = os.path.dirname(os.path.abspath(__file__))

def _get_test_file_path(path):
    return os.path.join(TESTS_LOCATION, "files", path)

class YamrModeTester(YtTestBase, YTEnv):
    @classmethod
    def setup_class(cls, conf=None):
        if conf is None:
            conf = {}
        super(YamrModeTester, cls).setup_class(conf)
        yt.set_yamr_mode()
        config["yamr_mode"]["treat_unexisting_as_empty"] = False
        if not yt.exists("//sys/empty_yamr_table"):
            yt.create("table", "//sys/empty_yamr_table", recursive=True)
        if not yt.is_sorted("//sys/empty_yamr_table"):
            yt.run_sort("//sys/empty_yamr_table", "//sys/empty_yamr_table", sort_by=["key", "subkey"])
        config["yamr_mode"]["treat_unexisting_as_empty"] = True

    @classmethod
    def teardown_class(cls):
        super(YamrModeTester, cls).teardown_class()

    def get_temp_records(self):
        columns = [string.digits, reversed(string.ascii_lowercase[:10]), string.ascii_uppercase[:10]]
        return map(dumps_row, starmap(Record, imap(flatten, reduce(izip, columns))))

    def test_get_smart_format(self):
        from yt.wrapper.table_commands import _get_format_from_tables as get_format

        existing_table = TEST_DIR + '/table'
        yt.create_table(existing_table)
        not_existing_table = TEST_DIR + '/not_existing'

        yamred_dsv_table = TEST_DIR + '/yamred_dsv_table'
        yamred_dsv_format = yson.to_yson_type("yamred_dsv", attributes={"has_subkey": True})
        yt.create_table(yamred_dsv_table, attributes={"_format": yamred_dsv_format})

        yson_table = TEST_DIR + '/yson_table'
        yt.create_table(yson_table, attributes={"_format": "yson"})

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
        yt.create_table(other_table)

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

    def test_mapreduce_binary(self):
        env = self.get_environment()
        if yt.config["api_version"] == "v2":
            env["FALSE"] = '"false"'
            env["TRUE"] = '"true"'
        else:
            env["FALSE"] = "false"
            env["TRUE"] = "true"
        proc = subprocess.Popen(
            os.path.join(TESTS_LOCATION, "../test_mapreduce.sh"),
            shell=True,
            env=env)
        proc.communicate()
        assert proc.returncode == 0

    def test_empty_write(self):
        table = TEST_DIR + "/table"
        yt.write_table(table, ["x\ty\tz\n"])
        yt.write_table(table, [])
        assert not yt.exists(table)

    def test_empty_output_table_deletion(self):
        table = TEST_DIR + "/table"
        other_table = TEST_DIR + "/subdir/other_table"

        yt.write_table(table, ["x\ty\tz\n"])
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
        assert yt.records_count(table) == 0
        yt.run_erase(TablePath(table, start_index=0, end_index=5))

    def test_map_reduce_operation(self):
        input_table = TEST_DIR + "/input_table"
        output_table = TEST_DIR + "/output_table"
        yt.write_table(input_table,
                       [
                           "\1a\t\t\n",
                           "\1b\t\t\n",
                           "\1c\t\t\n",
                           "a b\tc\t\n",
                           "c c\tc\tc c a\n"
                       ])
        yt.run_map_reduce("./split.py", "./collect.py", input_table, output_table,
                          map_files=_get_test_file_path("split.py"), reduce_files=_get_test_file_path("collect.py"))
        assert sorted(list(yt.read_table(output_table))) == sorted(["a\t\t2\n", "b\t\t1\n", "c\t\t6\n"])

    def test_many_output_tables(self):
        table = TEST_DIR + "/table"
        output_tables = []
        for i in xrange(10):
            output_tables.append(TEST_DIR + "/temp%d" % i)
        append_table = TEST_DIR + "/temp_special"
        yt.write_table(table, ["1\t1\t1\n"])
        yt.write_table(append_table, ["1\t1\t1\n"])

        yt.run_map("PYTHONPATH=. ./many_output.py yamr",
                   table,
                   output_tables + [TablePath(append_table, append=True)],
                   files=_get_test_file_path("many_output.py"))

        for table in output_tables:
            assert yt.records_count(table) == 1
        assert sorted(yt.read_table(append_table)) == ["1\t1\t1\n", "10\t10\t10\n"]

    def test_reduce_unsorted(self):
        input_table = TEST_DIR + "/input_table"
        output_table = TEST_DIR + "/output_table"
        yt.create("table", input_table)
        data = ['0\ti\tA\n', '0\th\tB\n', '0\tg\tC\n', '0\tf\tD\n', '0\te\tE\n',
                '0\td\tF\n', '0\tc\tG\n', '0\tb\tH\n', '0\ta\tI\n']
        yt.write_table(input_table, data)
        yt.run_reduce("cat", source_table=input_table, destination_table=output_table)
        assert list(yt.read_table(output_table)) == data[::-1]

    def test_run_operations(self):
        table = TEST_DIR + "/table"
        other_table = TEST_DIR + "/other_table"
        yt.write_table(table, ["0\ta\tA\n", "1\tb\tB\n", "2\tc\tC\n"])
        yt.run_map("PYTHONPATH=. ./my_op.py",
                   table, other_table,
                   files=map(_get_test_file_path, ["my_op.py", "helpers.py"]))
        assert yt.records_count(other_table) == 2 * yt.records_count(table)

        yt.run_sort(table)
        yt.run_reduce("./cpp_bin",
                      table, other_table,
                      files=_get_test_file_path("cpp_bin"))
        assert sorted(yt.read_table(other_table)) == \
               ["key{0}\tsubkey\tvalue=value\n".format(i) for i in xrange(5)]

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

        yt.config["tabular_data_format"] = yt.format.YamrFormat(has_subkey=False)
        try:
            yt.write_table(table, ["a\tb\n"])
            yt.run_map_reduce(mapper=None, reducer=yamr_func,
                              source_table=table, destination_table=output_table, reduce_by="key")
            assert list(yt.read_table(output_table)) == ["10\t20\n"]
        finally:
            yt.config["tabular_data_format"] = yt.format.YamrFormat(has_subkey=True)

        with pytest.raises(yt.YtError):
            yt.run_map_reduce(mapper=None, reducer=yamr_func,
                              source_table=table, destination_table=output_table, reduce_by="subkey")

        yt.write_table(table, ["1\t2\t3\n", "4\t5\t6\n"])
        yt.run_map(inc_key_yamr, table, table)
        assert sorted(yt.read_table(table)) == ["2\t2\t3\n", "5\t5\t6\n"]

    def test_lenval_python_operations(self):
        def foo(rec):
            yield rec

        table = TEST_DIR + "/table"
        yt.write_table(table, ["1\t2\t3\n"])
        yt.run_map(foo, table, table, format=yt.YamrFormat(has_subkey=True, lenval=True))
        assert list(yt.read_table(table)) == ["1\t2\t3\n"]

    def test_empty_input_tables(self):
        table = TEST_DIR + "/table"
        other_table = TEST_DIR + "/temp_other"
        another_table = TEST_DIR + "/temp_another"

        yt.write_table(table, ["1\t2\t3\n"])
        yt.run_map("PYTHONPATH=. ./my_op.py",
                   [table, other_table], another_table,
                   files=map(_get_test_file_path, ["my_op.py", "helpers.py"]))
        assert not yt.exists(other_table)

    def test_reduce_unexisting_tables(self):
        old_option = config["yamr_mode"]["run_map_reduce_if_source_is_not_sorted"]
        config["yamr_mode"]["run_map_reduce_if_source_is_not_sorted"] = False
        try:
            table = TEST_DIR + "/table"
            yt.write_table(table, ["0\ta\tA\n"])
            yt.run_sort(table)

            output_table = TEST_DIR + "/output_table"
            yt.create("table", output_table)

            yt.run_reduce("cat", [table, TEST_DIR + "/unexisting_table"], output_table)
        finally:
            config["yamr_mode"]["run_map_reduce_if_source_is_not_sorted"] = old_option

    def test_reduce_with_output_sorted(self):
        table = TEST_DIR + "/table"
        output_table = TEST_DIR + "/output_table"
        yt.write_table(table, ["3\t1\t3\n", "1\t2\t4\n"])
        yt.run_reduce("cat", table, "<sorted_by=[key]>" + output_table)
        assert list(yt.read_table(output_table)) == ["1\t2\t4\n", "3\t1\t3\n"]
        assert parse_bool(yt.get_attribute(output_table, "sorted", False))

    def test_reduce_differently_sorted_table(self):
        table = TEST_DIR + "/table"
        yt.write_table(table, ["1\t2\t3\n"])
        yt.run_sort(table, sort_by=["key"])

        with pytest.raises(yt.YtError):
            yt.run_reduce("cat", table, TEST_DIR + "/other_table", sort_by=["subkey"], reduce_by=["subkey"])

    def test_throw_on_missing_destination(self):
        table = TEST_DIR + "/table"
        yt.write_table(table, ["1\t2\t3\n"])
        with pytest.raises(yt.YtError):
            yt.run_map("cat", source_table=table, destination_table=None)

class TestYamrModeV2(YamrModeTester):
    @classmethod
    def setup_class(cls):
        super(TestYamrModeV2, cls).setup_class({"api_version": "v2"})

    @classmethod
    def teardown_class(cls):
        super(TestYamrModeV2, cls).teardown_class()
