#!/usr/bin/python

from yt.wrapper.tests.base import YtTestBase, TEST_DIR
from yt.environment import YTEnv
import yt.wrapper.config as config
import yt.wrapper as yt

import sys
from StringIO import StringIO

class DefaultYtTest(YtTestBase, YTEnv):
    @classmethod
    def setUpClass(cls):
        YtTestBase.setUpClass(YTEnv)

    @classmethod
    def tearDownClass(cls):
        YtTestBase.tearDownClass()

    def setUp(self):
        super(DefaultYtTest, self).setUp()
        yt.config.DEFAULT_FORMAT = yt.format.DsvFormat()

    # Check equality of records in dsv format
    def check(self, recordsA, recordsB):
        def prepare(records):
            return map(yt.line_to_record, sorted(list(records)))
        self.assertEqual(prepare(recordsA), prepare(recordsB))


    def test_get_set_exists(self):
        self.assertTrue(yt.get("/"))
        self.assertTrue(len(yt.list("/")) > 1)
        self.assertRaises(yt.YtError, lambda: yt.get("//none"))

        self.assertTrue(yt.exists("/"))
        self.assertTrue(yt.exists(TEST_DIR))
        self.assertFalse(yt.exists(TEST_DIR + "/some_node"))

        self.assertRaises(yt.YtError, lambda: yt.set(TEST_DIR + "/some_node/embedded_node", {}))
        yt.set(TEST_DIR + "/some_node", {})

        self.assertTrue(yt.exists(TEST_DIR + "/some_node"))


    def test_remove(self):
        for recursive in [False, True]:
            self.assertRaises(yt.YtError, lambda: yt.remove(TEST_DIR + "/some_node", recursive=recursive))
            yt.remove(TEST_DIR + "/some_node", recursive=recursive, check_existance=True)


        for check_existance in [False, True]:
            yt.set(TEST_DIR + "/some_node", {})
            yt.remove(TEST_DIR + "/some_node",
                      recursive=True,
                      check_existance=check_existance)


    def test_mkdir(self):
        yt.mkdir(TEST_DIR, recursive=True)
        self.assertRaises(yt.YtError, lambda: yt.mkdir(TEST_DIR))


        self.assertRaises(yt.YtError, lambda: yt.mkdir(TEST_DIR + "/x/y"))
        yt.mkdir(TEST_DIR + "/x")
        yt.mkdir(TEST_DIR + "/x/y/z", recursive=True)


    def test_search(self):
        yt.mkdir(TEST_DIR + "/dir/other_dir", recursive=True)
        yt.create_table(TEST_DIR + "/dir/table")
        yt.upload_file("", TEST_DIR + "/file")

        self.assertEqual(set(yt.search(TEST_DIR)),
                        {TEST_DIR, TEST_DIR + "/dir", TEST_DIR + "/dir/other_dir", TEST_DIR + "/dir/table", TEST_DIR + "/file"})

        self.assertEqual(set(yt.search(TEST_DIR, node_type="file")),
                         {TEST_DIR + "/file"})

        self.assertEqual(set(yt.search(TEST_DIR, node_type="table", path_filter=lambda x: x.find("dir") != -1)),
                         {TEST_DIR + "/dir/table"})

        # Search empty tables
        res = yt.search(
            TEST_DIR,
            attributes=["row_count"],
            object_filter=\
                lambda x: x.get("$attributes", {}).get("row_count", -1) == 0)
        self.assertEqual(set(res),
                         {TEST_DIR + "/dir/table"})

    def test_file_commands(self):
        self.assertRaises(yt.YtError, lambda: yt.upload_file("", TEST_DIR + "/dir/file"))

        file_path = TEST_DIR + "/file"
        yt.upload_file("", file_path)
        self.assertEqual("", yt.download_file(file_path, "string"))

        # TODO: add tests for smart upload files

    def test_read_write(self):
        table = TEST_DIR + "/table"
        yt.create_table(table)
        self.check([], yt.read_table(table))

        yt.write_table(table, ["x=1\n"])
        self.check(["x=1\n"], yt.read_table(table))

        yt.write_table(yt.TablePath(table, append=True), ["y=1\n"])
        self.check(["x=1\n", "y=1\n"], yt.read_table(table))

        yt.write_table(table, ["y=1\n"])
        self.check(["y=1\n"], yt.read_table(table))

    def test_empty_table(self):
        dir = TEST_DIR + "/dir"
        table = dir + "/table"

        self.assertRaises(yt.YtError, lambda: yt.create_table(table))
        self.assertRaises(yt.YtError, lambda: yt.records_count(table))

        yt.create_table(table, recursive=True)
        self.assertEqual(0, yt.records_count(table))
        self.check([], yt.read_table(table, format=yt.DsvFormat()))

        yt.erase_table(table)
        self.assertEqual(0, yt.records_count(table))

        yt.remove(dir, recursive=True)
        self.assertRaises(yt.YtError, lambda: yt.create_table(table))

    def test_simple_copy_move(self):
        table = TEST_DIR + "/table"
        dir = TEST_DIR + "/dir"
        other_table = dir + "/other_table"
        yt.create_table(table)
        self.assertEqual([], list(yt.read_table(table)))

        self.assertRaises(yt.YtError, lambda: yt.copy_table(table, table))

        yt.move_table(table, table)
        self.assertEqual([], list(yt.read_table(table)))

        self.assertRaises(yt.YtError, lambda: yt.copy_table(table, other_table))
        self.assertRaises(yt.YtError, lambda: yt.move_table(table, other_table))

        yt.mkdir(dir)

        yt.copy_table(table, other_table)
        self.assertTrue(yt.exists(table))
        self.assertTrue(yt.exists(other_table))

        yt.move_table(table, other_table)
        self.assertFalse(yt.exists(table))
        self.assertTrue(yt.exists(other_table))

    def test_many_copy_move(self):
        tableX = TEST_DIR + "/tableX"
        tableY = TEST_DIR + "/tableY"
        dir = TEST_DIR + "/dir"
        res_table = dir + "/other_table"

        yt.write_table(tableX, ["x=1\n"])
        yt.write_table(tableY, ["y=2\n"])

        self.assertRaises(yt.YtError, lambda: yt.copy_table([tableX, tableY], res_table))
        self.assertRaises(yt.YtError, lambda: yt.move_table([tableX, tableY], res_table))

        yt.mkdir(dir)
        yt.copy_table([tableX, tableY], res_table)
        self.check(["x=1\n"], yt.read_table(tableX))
        self.check(["y=2\n"], yt.read_table(tableY))
        self.check(["x=1\n", "y=2\n"], yt.read_table(res_table))

        yt.remove(res_table)
        yt.move_table([tableX, tableY], res_table)
        self.assertFalse(yt.exists(tableX))
        self.assertFalse(yt.exists(tableY))
        self.check(["x=1\n", "y=2\n"], yt.read_table(res_table))

    def test_run_operation(self):
        table = TEST_DIR + "/table"
        other_table = TEST_DIR + "/other_table"
        yt.write_table(table, ["x=1\n", "x=2\n"])

        yt.run_map("cat", table, table)
        self.check(["x=1\n", "x=2\n"], yt.read_table(table))

        yt.run_map("grep 2", table, other_table)
        self.check(["x=2\n"], yt.read_table(other_table))

        self.assertRaises(yt.YtError, lambda: yt.run_map("cat", [table, table + "xxx"], other_table))

    def test_sort(self):
        table = TEST_DIR + "/table"
        other_table = TEST_DIR + "/other_table"
        yt.write_table(table, ["y=2\n", "x=1\n"])

        self.assertRaises(yt.YtError, lambda: yt.sort_table([table, other_table], other_table, sort_by=["y"]))

        yt.sort_table(table, other_table, sort_by=["y"])
        self.assertEqual(["x=1\n", "y=2\n"], list(yt.read_table(other_table)))

        yt.sort_table(table, sort_by=["x"])
        self.assertEqual(["y=2\n", "x=1\n"], list(yt.read_table(table)))

    def test_printing_stderr(self):
        table = TEST_DIR + "/table"
        yt.write_table(table, ["x=1\n"])

        # Prepare
        yt.config.PRINT_STDERRS = True
        old = yt.logger.info
        output = []
        def print_info(msg, *args, **kwargs):
            output.append(msg)
        yt.logger.info = print_info

        yt.run_map("cat 1>&2", table, table)

        # Return settings back
        yt.logger.info = old
        yt.config.PRINT_STDERRS = False

        self.assertTrue(any(map(lambda line: line.find("x=1") != -1, output)))

