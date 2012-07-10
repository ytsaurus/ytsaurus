#!/usr/bin/python

import yt
from yt import Record, YtError, record_to_line, line_to_record

from common import flatten

import os
import random
import string
from itertools import imap, izip, starmap

import unittest
    
TEST_DIR = "//home/tests"

class YtTest(unittest.TestCase):
    def setUp(self):
        os.environ["PATH"] = ".:" + os.environ["PATH"]
        if not yt.exists(TEST_DIR):
            yt.set(TEST_DIR, "{}")

        self.old_timeout = yt.WAIT_TIMEOUT
        yt.WAIT_TIMEOUT = 0.2

    def tearDown(self):
        yt.remove(TEST_DIR)
        yt.WAIT_ITMEOUT = self.old_timeout


    def temp_records(self):
        columns = [string.digits, reversed(string.ascii_lowercase[:10]), string.ascii_uppercase[:10]]
        return map(record_to_line, starmap(Record, imap(flatten, reduce(izip, columns))))

    def create_temp_table(self):
        return yt.write_table(
            TEST_DIR + "/temp",
            self.temp_records())

    def random_string(self, length):
        char_set = string.ascii_uppercase + string.digits
        return "".join(random.sample(char_set, length))

    def test_common_operations(self):
        self.assertTrue(yt.exists("/"))
        self.assertTrue(yt.exists("//sys"))
        self.assertTrue(yt.exists("//\"sys\""))
        
        self.assertFalse(yt.exists('//"%s"/"%s"' %
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
        yt.write_table(table, records, append=True)
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

        yt.copy_table(table, other_table, append=True)
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
        self.assertEqual(line_to_record(list(yt.read_table(table))[0]).key, "0")
        self.assertEqual(sorted(list(self.temp_records())), 
                         list(yt.read_table(table)))
        self.assertTrue(yt.is_sorted(table))

        yt.sort_table(table, key_columns=["subkey"])
        self.assertEqual(line_to_record(list(yt.read_table(table))[0]).subkey, "a")

    def test_attributes(self):
        table = self.create_temp_table()
        self.assertEqual(yt.records_count(table), 10)
        self.assertFalse(yt.is_sorted(table))

    def test_operations(self):
        table = self.create_temp_table()
        other_table = TEST_DIR + "/temp_other"
        yt.run_map("PYTHONPATH=. ./my_op.py",
                   table, other_table,
                   files=["test/my_op.py", "test/helpers.py"])
        self.assertEqual(2 * yt.records_count(table), yt.records_count(other_table))

        yt.sort_table(table)
        yt.run_reduce("./cpp_bin",
                      table, other_table,
                      files="test/cpp_bin")

    def test_abort_operation(self):
        strategy = yt.AsyncStrategy()
        table = self.create_temp_table()
        other_table = TEST_DIR + "/temp_other"
        yt.run_map("PYTHONPATH=. ./my_op.py 10.0",
                   table, other_table,
                   files=["test/my_op.py", "test/helpers.py"],
                   strategy=strategy)

        operation = strategy.operations[-1]
        yt.abort_operation(operation)
        self.assertEqual(yt.get_operation_state(operation), "aborted")


    def test_dsv(self):
        old_format = yt.DEFAULT_FORMAT
        yt.DEFAULT_FORMAT = yt.DsvFormat()

        table = TEST_DIR + "/dsv"
        other_table = TEST_DIR + "/dsv_capital"

        #data = [{"a": 12, "b": "ignat"},
        #                   {"b": "max",  "c": 17.5}]
        #print data
        #print list(imap(record_to_line, data))
        #print list(imap(line_to_record, imap(record_to_line, data)))

        yt.write_table(table, 
            imap(record_to_line,
                 [{"a": 12, "b": "ignat"},
                           {"b": "max",  "c": 17.5}]))
        yt.run_map("PYTHONPATH=. ./capitilize_b.py", table, other_table,
               files=["yt.py", "common.py", "record.py", "format.py", "test/capitilize_b.py"])
        yt.DEFAULT_FORMAT = old_format
        print list(yt.read_table(other_table, format=yt.DsvFormat()))
        yt.DEFAULT_FORMAT = yt.DsvFormat()
        self.assertEqual(
            sorted([rec["b"] for rec in map(line_to_record, yt.read_table(other_table))]),
            ["IGNAT", "MAX"]) 
        
        yt.DEFAULT_FORMAT= old_format

if __name__ == "__main__":
    #suite = unittest.TestSuite()
    #suite.addTest(YtTest("test_dsv"))
    #unittest.TextTestRunner().run(suite)
    unittest.main()


