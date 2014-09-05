#!/usr/bin/python

import pytest

from yt.environment import YTEnv
from yt.wrapper.table_commands import copy_table, move_table
import yt.wrapper as yt
from yt.wrapper import Record, YtError, YtResponseError, dumps_row, loads_row, TablePath
import yt.wrapper.config as config
from yt.common import flatten

from yt.wrapper.tests.base import YtTestBase, TEST_DIR

import os
import random
import string
import subprocess
from itertools import imap, izip, starmap, chain
from functools import partial

LOCATION = os.path.dirname(os.path.abspath(__file__))
def _test_file_path(path):
    return os.path.join(LOCATION, "files", path)

def _module_file_path(path):
    return os.path.join(LOCATION, "..", path)

class TestMapreduceMode(YtTestBase, YTEnv):
    def setup_class(cls):
        YtTestBase._setup_class(YTEnv)
        config.set_mapreduce_mode()

    @classmethod
    def teardown_class(cls):
        YtTestBase._teardown_class()

    def read_records(self, table, format=None):
        return map(partial(loads_row, format=format),
                   yt.read_table(table, format))

    def temp_records(self):
        columns = [string.digits, reversed(string.ascii_lowercase[:10]), string.ascii_uppercase[:10]]
        return map(dumps_row, starmap(Record, imap(flatten, reduce(izip, columns))))


    def create_temp_table(self):
        table = TEST_DIR + "/temp"
        yt.write_table(table, self.temp_records())
        return table

    def dsv_records(self):
        return map(
            partial(dumps_row, format=yt.DsvFormat()),
                [{"a": 12,  "b": "ignat"},
                           {"b": "max",  "c": 17.5},
                 {"a": "x", "b": "name", "c": 0.5}])

    def create_dsv_table(self):
        table = TEST_DIR + "/dsv"
        yt.write_table(table, self.dsv_records(), format=yt.DsvFormat())
        return table

    def run_capitilize_b(self, src, dst):
        yt.run_map("ls -la 1>&2; PYTHONPATH=. ./capitilize_b.py", src, dst,
                   files=map(_module_file_path, ["config.py", "common.py", "record.py", "format.py"]) + [_test_file_path("capitilize_b.py")],
                   format=yt.DsvFormat())

    def run_accumulate_c(self, src, dst):
        yt.run_reduce("PYTHONPATH=. ./accumulate_c.py", src, dst,
                      reduce_by="c",
                      files=map(_module_file_path, ["config.py", "common.py", "record.py", "format.py"]) + [_test_file_path("accumulate_c.py")],
                      format=yt.DsvFormat())

    def random_string(self, length):
        char_set = string.ascii_uppercase + string.digits
        return "".join(random.sample(char_set, length))

    def test_common_operations(self):
        self.assertTrue(yt.exists("/"))
        self.assertTrue(yt.exists("//sys"))
        self.assertFalse(yt.exists("//\"sys\""))

        self.assertFalse(yt.exists('//%s/%s' %
                                (self.random_string(10), self.random_string(10))))

        random_strA = self.random_string(10)
        random_strB = self.random_string(10)


        half_path = '%s/"%s"' % (TEST_DIR, random_strA)
        full_path = '%s/"%s"/"%s"' % (TEST_DIR, random_strA, random_strB)
        self.assertRaises(YtResponseError, lambda: yt.set(full_path, {}))
        yt.set(half_path, {})
        yt.set(full_path, {})
        self.assertTrue(yt.exists(full_path))
        self.assertEqual(yt.get(full_path), {})
        yt.remove(half_path, recursive=True)
        self.assertRaises(YtError, lambda: yt.remove(full_path))
        yt.remove(full_path, force=True)

    def test_read_write(self):
        table = TEST_DIR + "/temp"
        if yt.exists(table):
            yt.remove(table)
        yt.create_table(table)
        yt.set_attribute(table, "my_attr", 10)

        records = map(dumps_row, [Record("x", "y", "z"), Record("key", "subkey", "value")])
        yt.write_table(table, records)
        self.assertEqual(sorted(yt.read_table(table)), sorted(records))
        self.assertEqual(yt.get_attribute(table, "my_attr"), 10)

        # check rewrite case
        yt.write_table(TablePath(table, append=True), records)
        self.assertEqual(sorted(yt.read_table(table)), sorted(records + records))

    def test_huge_table(self):
        POWER = 3
        records = \
            yt.StringIterIO(
                imap(dumps_row,
                     (Record(str(i), str(i * i), "long long string with strange symbols #*@*&^$#%@(#!@:L|L|KL..,,.~`")
                 for i in xrange(10 ** POWER))))
        table = TEST_DIR + "/temp"
        yt.write_table(table, records)
        self.assertEqual(yt.records_count(table), 10 ** POWER)

        records_count = 0
        for rec in yt.read_table(table):
            records_count += 1
        self.assertEqual(records_count, 10 ** POWER)

    def test_copy_move(self):
        table = self.create_temp_table()
        other_table = TEST_DIR + "/temp_other"
        yt.create_table(other_table)

        copy_table(table, other_table)
        assert sorted(self.temp_records()) == sorted(yt.read_table(other_table))

        copy_table(table, other_table)
        assert sorted(self.temp_records()) == sorted(yt.read_table(other_table))

        copy_table(table, TablePath(other_table, append=True))
        assert sorted(list(self.temp_records()) + list(self.temp_records())) == \
               sorted(yt.read_table(other_table))

        yt.run_sort(table, table)
        copy_table(table, TablePath(other_table, append=True))
        assert sorted(list(self.temp_records()) * 3) == sorted(yt.read_table(other_table))

        move_table(table, other_table)
        assert yt.exists(other_table)
        assert not yt.exists(table)
        assert list(yt.read_table(other_table)) == sorted(list(self.temp_records()))

        copy_table(table, table)
        assert not yt.exists(table)

        embedded_path = TEST_DIR + "dir/other_dir/table"
        copy_table(table, embedded_path)
        assert embedded_path

    def test_sort(self):
        table = self.create_temp_table()
        files_count = len(list(yt.list(TEST_DIR)))
        yt.run_sort(table)
        self.assertEqual(len(list(yt.list(TEST_DIR))), files_count)
        self.assertEqual(self.read_records(table)[0].key, "0")
        self.assertEqual(sorted(list(self.temp_records())),
                         list(yt.read_table(table)))
        self.assertTrue(yt.is_sorted(table))

        yt.run_sort(table, sort_by=["subkey"])
        self.assertEqual(self.read_records(table)[0].subkey, "a")

        unexisting_table = TEST_DIR + "/unexisting"
        yt.run_sort(unexisting_table)
        self.assertFalse(yt.exists(unexisting_table))

    def test_attributes(self):
        table = self.create_temp_table()
        self.assertEqual(yt.records_count(table), 10)
        self.assertFalse(yt.is_sorted(table))

        yt.set_attribute(table, "my_attribute", {})
        yt.set_attribute(table, "my_attribute/000", 10)
        self.assertEqual(yt.get_attribute(table, "my_attribute/000"), 10)
        #self.assertEqual(yt.list_attributes(table, "my_attribute"), ["000"])

        result = yt.search(table, node_type='table', attributes=('my_attribute', ))
        self.assertEqual(len(result), 1)
        self.assertEqual(str(result[0]), table)
        self.assertEqual(result[0].attributes['my_attribute'], {'000': 10})

    def test_operations(self):
        table = self.create_temp_table()
        other_table = TEST_DIR + "/temp_other"
        yt.run_map("PYTHONPATH=. ./my_op.py",
                   table, other_table,
                   files=map(_test_file_path, ["my_op.py", "helpers.py"]))
        self.assertEqual(2 * yt.records_count(table), yt.records_count(other_table))

        yt.run_sort(table)
        yt.run_reduce("./cpp_bin",
                      table, other_table,
                      files=_test_file_path("cpp_bin"))

    def test_abort_operation(self):
        strategy = yt.AsyncStrategy()
        table = self.create_temp_table()
        other_table = TEST_DIR + "/temp_other"
        yt.run_map("PYTHONPATH=. ./my_op.py 10.0",
                   table, other_table,
                   files=map(_test_file_path, ["my_op.py", "helpers.py"]),
                   strategy=strategy)

        type, operation, finalization = strategy.operations[-1]
        yt.abort_operation(operation)
        self.assertEqual(yt.get_operation_state(operation), "aborted")

        finalization("none")


    def test_dsv(self):
        table = self.create_dsv_table()
        other_table = TEST_DIR + "/dsv_capital"
        self.run_capitilize_b(table, other_table)

        recs = self.read_records(other_table, format=yt.DsvFormat())
        self.assertEqual(
            sorted([rec["b"] for rec in recs]),
            ["IGNAT", "MAX", "NAME"])
        self.assertEqual(
            sorted([rec["c"] for rec in recs if "c" in rec]),
            ["0.5", "17.5"])

        self.run_accumulate_c(table, other_table)
        recs = self.read_records(other_table, format=yt.DsvFormat())
        self.assertEqual(
            sorted(recs),
            [{"a": "12", "c": "0.0"}, {"a": "x", "c": "0.5"}])

    def test_many_output_tables(self):
        table = self.create_temp_table()
        output_tables = []
        for i in xrange(15):
            output_tables.append(TEST_DIR + "/temp%d" % i)

        append_table = TEST_DIR + "/temp_special"
        copy_table(table, append_table)

        yt.run_map("PYTHONPATH=. ./many_output.py",
                   table,
                   output_tables + [TablePath(append_table, append=True)],
                   files=_test_file_path("many_output.py"))

        for t in output_tables:
            self.assertEqual(yt.records_count(t), 1)
        self.assertEqual(yt.records_count(append_table), 11)

    def test_range_operations(self):
        table = self.create_dsv_table()
        other_table = TEST_DIR + "/dsv_capital"

        self.run_capitilize_b(TablePath(table, columns=["b"]), other_table)
        recs = self.read_records(other_table, format=yt.DsvFormat())

        self.assertEqual(
            sorted([rec["b"] for rec in recs]),
            ["IGNAT", "MAX", "NAME"])
        self.assertEqual(
            sorted([rec["c"] for rec in recs if "c" in rec]),
            [])

        yt.run_sort(table, sort_by=["b", "c"])
        self.assertEqual(
            self.read_records(TablePath(table, lower_key="a", upper_key="n", columns=["b"]),
                              format=yt.DsvFormat()),
            [{"b": "ignat"}, {"b": "max"}])

        self.assertEqual(
            self.read_records(TablePath(table, columns=["c"]),
                              format=yt.DsvFormat()),
            [{}, {"c": "17.5"}, {"c": "0.5"}])

        self.assertEqual(
            self.read_records(TablePath(table, columns=["b"], end_index=2),
                              format=yt.DsvFormat()),
            [{"b": "ignat"}, {"b": "max"}])

        self.assertEqual(
            self.read_records(table + '{b}[:#2]', format=yt.DsvFormat()),
            [{"b": "ignat"}, {"b": "max"}])

    def test_merge(self):
        table = self.create_temp_table()
        other_table = TEST_DIR + "/temp_other"
        another_table = TEST_DIR + "/temp_another"
        yt.run_sort(table)
        assert yt.is_sorted(table)
        yt.run_merge(table, other_table, mode="sorted")
        assert yt.is_sorted(other_table)
        yt.run_merge([table, other_table], another_table, mode="sorted")
        assert yt.is_sorted(another_table)
        assert yt.records_count(another_table) == 20

        yt.run_merge(TEST_DIR + "/unexisting", other_table)
        assert yt.records_count(other_table) == 0
        yt.run_merge([table, TEST_DIR + "/unexisting"], other_table, mode="sorted")
        assert yt.records_count(other_table) == yt.records_count(table)

    def test_digit_names(self):
        table = TEST_DIR + '/123'
        yt.write_table(table, self.temp_records())
        yt.run_sort(table)
        self.assertEqual(self.read_records(table)[0].key, "0")

    def test_empty_input_tables(self):
        table = self.create_temp_table()
        other_table = TEST_DIR + "/temp_other"
        another_table = TEST_DIR + "/temp_another"
        yt.run_map("PYTHONPATH=. ./my_op.py",
                   [table, other_table], another_table,
                   files=map(_test_file_path, ["my_op.py", "helpers.py"]))
        self.assertFalse(yt.exists(other_table))

    def test_file_operations(self):
        dest = []
        for i in xrange(2):
            self.assertTrue(yt.smart_upload_file(_test_file_path("my_op.py"), placement_strategy="random").find("/my_op.py") != -1)

        for d in dest:
            self.assertEqual(list(yt.download_file(dest)),
                             open(_test_file_path("my_op.py")).readlines())

        dest = TEST_DIR+"/file_dir/some_file"
        yt.smart_upload_file(_test_file_path("my_op.py"), destination=dest, placement_strategy="ignore")
        self.assertEqual(yt.get_attribute(dest, "file_name"), "some_file")

    def test_map_reduce_operation(self):
        input = TEST_DIR + "/input"
        output = TEST_DIR + "/output"
        yt.write_table(input,
            [
                "\1a\t\t\n",
                "\1b\t\t\n",
                "\1c\t\t\n",
                "a b\tc\t\n",
                "c c\tc\tc c a\n"
            ])
        yt.run_map_reduce("./split.py", "./collect.py", input, output,
                          map_files=_test_file_path("split.py"), reduce_files=_test_file_path("collect.py"))
        self.assertEqual(
            sorted(list(yt.read_table(output))),
            sorted(["a\t\t2\n", "b\t\t1\n", "c\t\t6\n"]))

    def test_python_operations(self):
        @yt.raw
        def func(rec):
            yield rec.strip() + "aaaaaaaaaa\n"

        @yt.raw
        def func_smart(rec):
            rec = yt.loads_row(rec)
            rec.key = "xxx"
            yield yt.dumps_row(rec)

        table = self.create_temp_table()
        other_table = TEST_DIR + "/temp_other"

        for f in [func, func_smart]:
            yt.run_map(f, table, other_table)
            self.assertEqual(
                sorted(list(yt.read_table(other_table))),
                sorted(list(chain(*imap(f, self.temp_records())))))

    def test_empty_output_table_deletion(self):
        table = self.create_temp_table()
        other_table = TEST_DIR + "/ttt/temp_other"
        yt.run_map("cat 1>&2 2>/dev/null", table, other_table)
        self.assertFalse(yt.exists(other_table))
        self.assertFalse(yt.exists(TEST_DIR + "/ttt"))

    def test_reformatting(self):
        @yt.raw
        def reformat(rec):
            values = rec.strip().split("\t", 2)
            yield "\t".join("=".join([k, v]) for k, v in zip(["k", "s", "v"], values)) + "\n"
        table = self.create_temp_table()
        other_table = TEST_DIR + "/temp_other"
        yt.run_map(reformat, table, other_table, output_format=yt.DsvFormat())
        self.assertTrue(yt.exists(other_table))
        self.assertEqual(
            sorted(
                map(lambda str: str.split("=", 1)[0],
                    yt.read_table(other_table, format=yt.DsvFormat())\
                        .next().strip().split("\t"))),
            ["k", "s", "v"])

    def test_table_ranges_with_exists(self):
        table = self.create_temp_table()
        self.assertTrue(yt.exists(table))
        self.assertTrue(yt.exists(table + "/@"))
        self.assertTrue(yt.exists(table + "/@compression_ratio"))
        self.assertTrue(len(yt.list_attributes(table)) > 1)
        self.assertTrue(len(yt.get_attribute(table, "channels")) == 0)

    def test_mapreduce_binary(self):
        proc = subprocess.Popen(
            "YT_USE_TOKEN=0 YT_PROXY=%s %s" %
                (config.http.PROXY,
                 os.path.join(LOCATION, "../test_mapreduce.sh")),
            shell=True)
        proc.communicate()
        self.assertEqual(proc.returncode, 0)

    def test_inplace_operations(self):
        table = self.create_temp_table()

        yt.run_map("cat", table, table)
        self.assertEqual(yt.records_count(table), 10)

        yt.run_reduce("cat", table, table)
        self.assertEqual(yt.records_count(table), 10)

    def test_sort_of_sorted_tables(self):
        table = self.create_temp_table()
        other_table = TEST_DIR + "/temp_other"
        result_table = TEST_DIR + "/result"

        yt.run_sort(table)
        copy_table(table, other_table)
        self.assertTrue(yt.is_sorted(other_table))

        yt.run_sort([table, other_table], result_table)
        self.assertTrue(yt.is_sorted(result_table))
        self.assertEqual(yt.records_count(result_table), 20)

    def test_sort_of_one_sorted_table(self):
        table = self.create_temp_table()
        other_table = TEST_DIR + "/temp_other"

        yt.run_sort(table)
        self.assertTrue(yt.is_sorted(table))

        yt.run_sort([table], other_table)
        self.assertTrue(yt.is_sorted(other_table))
        self.assertEqual(yt.records_count(other_table), 10)

    def test_erase(self):
        table = self.create_temp_table()
        self.assertEqual(yt.records_count(table), 10)

        yt.run_erase(TablePath(table, start_index=0, end_index=5))
        self.assertEqual(yt.records_count(table), 5)

        yt.run_erase(TablePath(table, start_index=0, end_index=5))
        self.assertEqual(yt.records_count(table), 0)

    def test_empty_write(self):
        table = self.create_temp_table()
        yt.write_table(table, [])
        self.assertFalse(yt.exists(table))

    def test_get_smart_format(self):
        from yt.wrapper.table_commands import _get_format_from_tables as get_format

        existing_table = TEST_DIR + '/existing'
        yt.create_table(existing_table)
        not_existing_table = TEST_DIR + '/not_existing'
        dsv_table = TablePath(TEST_DIR + '/dsv_table', columns="1")
        yt.create_table(dsv_table)
        yamr_table = TEST_DIR + '/yamr_table'
        yt.create_table(yamr_table, attributes={"_format": "yamr"})
        yson_table = TEST_DIR + '/yson_table'
        yt.create_table(yson_table, attributes={"_format": "yson"})

        assert get_format([], ignore_unexisting_tables=False) == None
        assert get_format([], ignore_unexisting_tables=True) == None

        assert get_format([existing_table], ignore_unexisting_tables=False) == None
        assert get_format([existing_table], ignore_unexisting_tables=True) == None

        assert get_format([dsv_table], ignore_unexisting_tables=False).name() == "dsv"
        assert get_format([yamr_table], ignore_unexisting_tables=False).name() == "yamr"
        assert get_format([yson_table], ignore_unexisting_tables=False).name() == "yson"

        with pytest.raises(YtError):
            get_format([[dsv_table, not_existing_table], None], ignore_unexisting_tables=False)

        assert get_format([existing_table, not_existing_table], ignore_unexisting_tables=False) == None
        assert get_format([[dsv_table, not_existing_table], None], ignore_unexisting_tables=True).name() == "dsv"

        with pytest.raises(YtError):
            get_format([dsv_table, yson_table], ignore_unexisting_tables=False)