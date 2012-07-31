#!/usr/bin/python

import yt
import config
from yt import Record, YtError, record_to_line, line_to_record, Table

from common import flatten

from yt_environment import YTEnv

import os
import sys
import logging
import random
import string
import shutil
from itertools import imap, izip, starmap
from functools import partial

import unittest
    
TEST_DIR = "//home/tests"

class YtTest(YTEnv):
    NUM_MASTERS = 1
    NUM_HOLDERS = 3
    START_SCHEDULER = True
    START_PROXY = True

    @classmethod  
    def setUpClass(cls):
        if os.path.exists("test.log"):
            os.remove("test.log")
        logging.basicConfig(level=logging.WARNING)
        logging.disable(logging.INFO)

        ports = {
            "master": 18001,
            "node": 17001,
            "scheduler": 18101,
            "proxy": 18080}
        # (TODO): remake this strange stuff.
        cls.env = cls()
        cls.env.set_environment("tests/sandbox", "tests/sandbox/pids.txt", ports)

        config.PROXY = "localhost:%d" % ports["proxy"]
    
    @classmethod
    def tearDownClass(cls):
        cls.env.clear_environment()
    
    def setUp(self):
        os.environ["PATH"] = ".:" + os.environ["PATH"]
        if not yt.exists(TEST_DIR):
            yt.set(TEST_DIR, "{}")

        config.WAIT_TIMEOUT = 0.2
        config.DEFAULT_STRATEGY = yt.WaitStrategy(print_progress=False)

    def tearDown(self):
        yt.remove(TEST_DIR)

    def read_records(self, table, format=None):
        return map(
            partial(line_to_record, format=format),
            yt.read_table(table, format))

    def temp_records(self):
        columns = [string.digits, reversed(string.ascii_lowercase[:10]), string.ascii_uppercase[:10]]
        return map(record_to_line, starmap(Record, imap(flatten, reduce(izip, columns))))


    def create_temp_table(self):
        table = TEST_DIR + "/temp"
        yt.write_table(table, self.temp_records())
        return table

    def dsv_records(self):
        return map(
            partial(record_to_line, format=yt.DsvFormat()), 
                [{"a": 12,  "b": "ignat"},
                           {"b": "max",  "c": 17.5},
                 {"a": "x", "b": "name", "c": 0.5}])

    def create_dsv_table(self):
        table = TEST_DIR + "/dsv"
        yt.write_table(table, self.dsv_records(), format=yt.DsvFormat())
        return table

    def run_capitilize_b(self, src, dst):
        yt.run_map("PYTHONPATH=. ./capitilize_b.py", src, dst,
                   files=["config.py", "common.py", "record.py", "format.py", "tests/capitilize_b.py"],
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
        self.assertRaises(YtError, lambda: yt.set(full_path, "{}"))
        self.assertEqual(yt.set(half_path, "{}"), None)
        self.assertEqual(yt.set(full_path, "{}"), None)
        self.assertTrue(yt.exists(full_path))
        self.assertEqual(yt.get(full_path), {})
        self.assertEqual(yt.remove(half_path), None)
        self.assertEqual(yt.remove(full_path), None) # Do nothing

    def test_read_write(self):
        table = TEST_DIR + "/temp"
        if yt.exists(table):
            yt.remove(table)
        yt.create_table(table)
        
        records = map(record_to_line, [Record("x", "y", "z"), Record("key", "subkey", "value")])
        yt.write_table(table, records)
        self.assertEqual(sorted(yt.read_table(table)), sorted(records))

        # check rewrite case
        yt.write_table(Table(table, append=True), records)
        self.assertEqual(sorted(yt.read_table(table)), sorted(records + records))
    
    def test_huge_table(self):
        POWER = 3
        records = \
            imap(record_to_line,
                 (Record(str(i), str(i * i), "long long string with strange symbols #*@*&^$#%@(#!@:L|L|KL..,,.~`")
                 for i in xrange(10 ** POWER)))
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
        
        yt.copy_table(table, other_table)
        self.assertEqual(sorted(self.temp_records()),
                         sorted(yt.read_table(other_table)))
        
        yt.copy_table(table, other_table)
        self.assertEqual(sorted(self.temp_records()),
                         sorted(yt.read_table(other_table)))

        yt.copy_table(table, Table(other_table, append=True))
        self.assertEqual(sorted(list(self.temp_records()) + list(self.temp_records())),
                         sorted(yt.read_table(other_table)))

        yt.move_table(table, other_table)
        self.assertFalse(yt.exists(table))
        self.assertEqual(list(yt.read_table(other_table)),
                         sorted(list(self.temp_records())))

        self.assertRaises(YtError, lambda: yt.copy_table(table, table))

    def test_sort(self):
        table = self.create_temp_table()
        files_count = len(list(yt.list(TEST_DIR)))
        yt.sort_table(table)
        self.assertEqual(len(list(yt.list(TEST_DIR))), files_count)
        self.assertEqual(self.read_records(table)[0].key, "0")
        self.assertEqual(sorted(list(self.temp_records())), 
                         list(yt.read_table(table)))
        self.assertTrue(yt.is_sorted(table))

        yt.sort_table(table, columns=["subkey"])
        self.assertEqual(self.read_records(table)[0].subkey, "a")

    def test_attributes(self):
        table = self.create_temp_table()
        self.assertEqual(yt.records_count(table), 10)
        self.assertFalse(yt.is_sorted(table))

    def test_operations(self):
        table = self.create_temp_table()
        other_table = TEST_DIR + "/temp_other"
        yt.run_map("PYTHONPATH=. ./my_op.py",
                   table, other_table,
                   files=["tests/my_op.py", "tests/helpers.py"])
        self.assertEqual(2 * yt.records_count(table), yt.records_count(other_table))

        yt.sort_table(table)
        yt.run_reduce("./cpp_bin",
                      table, other_table,
                      files="tests/cpp_bin")

    def test_abort_operation(self):
        strategy = yt.AsyncStrategy()
        table = self.create_temp_table()
        other_table = TEST_DIR + "/temp_other"
        yt.run_map("PYTHONPATH=. ./my_op.py 10.0",
                   table, other_table,
                   files=["tests/my_op.py", "tests/helpers.py"],
                   strategy=strategy)

        operation = strategy.operations[-1]
        yt.abort_operation(operation)
        self.assertEqual(yt.get_operation_state(operation), "aborted")


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

    def test_many_output_tables(self):
        table = self.create_temp_table()
        other_table = TEST_DIR + "/temp_other"
        another_table = TEST_DIR + "/temp_another" 
        yt.copy_table(table, another_table)
        
        yt.run_map("PYTHONPATH=. ./many_output.py",
                   table,
                   [other_table, Table(another_table, append=True)],
                   files="tests/many_output.py")
        self.assertEqual(yt.records_count(other_table), 1)
        self.assertEqual(yt.records_count(another_table), 11)

    def test_range_operations(self):
        table = self.create_dsv_table()
        other_table = TEST_DIR + "/dsv_capital"
        
        self.run_capitilize_b(Table(table, columns=["b"]), other_table)
        recs = self.read_records(other_table, format=yt.DsvFormat())
        
        self.assertEqual(
            sorted([rec["b"] for rec in recs]),
            ["IGNAT", "MAX", "NAME"]) 
        self.assertEqual(
            sorted([rec["c"] for rec in recs if "c" in rec]),
            []) 

        yt.sort_table(table, columns=["b", "c"])
        self.assertEqual( 
            self.read_records(Table(table, lower_key="a", upper_key="n", columns=["b"]),
                              format=yt.DsvFormat()),
            [{"b": "ignat"}, {"b": "max"}])
        self.assertEqual( 
            self.read_records(Table(table, columns=["c"]),
                              format=yt.DsvFormat()),
            [{"c": "17.5"}, {"c": "0.5"}])

    def test_merge(self):
        table = self.create_temp_table()
        other_table = TEST_DIR + "/temp_other"
        another_table = TEST_DIR + "/temp_another"
        yt.sort_table(table)
        self.assertTrue(yt.is_sorted(table))
        yt.merge_tables(table, other_table, mode="sorted")
        self.assertTrue(yt.is_sorted(other_table))
        yt.merge_tables([table, other_table], another_table, mode="sorted")
        self.assertTrue(yt.is_sorted(another_table))
        self.assertEqual(yt.records_count(another_table), 20)

    def test_digit_names(self):
        table = TEST_DIR + '/"123"'
        yt.write_table(table, self.temp_records())
        yt.sort_table(table)
        self.assertEqual(self.read_records(table)[0].key, "0")


if __name__ == "__main__":
    #suite = unittest.TestSuite()
    #suite.addTest(YtTest("test_operations"))
    #unittest.TextTestRunner().run(suite)
    unittest.main()


