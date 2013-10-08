#!/usr/bin/python

import yt.yson as yson
from yt.wrapper.tests.base import YtTestBase, TEST_DIR
from yt.environment import YTEnv
import yt.wrapper as yt

import os
import tempfile
import subprocess

import pytest

class TestDefaultBehaviour(YtTestBase, YTEnv):
    @classmethod
    def setup_class(cls):
        YtTestBase._setup_class(YTEnv)
        yt.config.format.TABULAR_DATA_FORMAT = yt.format.DsvFormat()

    @classmethod
    def teardown_class(cls):
        YtTestBase._teardown_class()

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
            yt.remove(TEST_DIR + "/some_node", recursive=recursive, force=True)

        for force in [False, True]:
            yt.set(TEST_DIR + "/some_node", {})
            yt.remove(TEST_DIR + "/some_node",
                      recursive=True,
                      force=force)


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
                         set([TEST_DIR, TEST_DIR + "/dir", TEST_DIR + "/dir/other_dir", TEST_DIR + "/dir/table", TEST_DIR + "/file"]))

        self.assertEqual(set(yt.search(TEST_DIR, node_type="file")),
                         set([TEST_DIR + "/file"]))

        self.assertEqual(set(yt.search(TEST_DIR, node_type="table", path_filter=lambda x: x.find("dir") != -1)),
                         set([TEST_DIR + "/dir/table"]))

        # Search empty tables
        res = yt.search(
            TEST_DIR,
            attributes=["row_count"],
            object_filter=\
                lambda x: x.attributes.get("row_count", -1) == 0)
        self.assertEqual(set(res),
                set([yson.to_yson_type(TEST_DIR + "/dir/table", {"row_count": 0})]))

    def test_create(self):
        with pytest.raises(yt.YtError): 
            yt.create("map_node", TEST_DIR + "/map", attributes={"type": "table"})

    def test_file_commands(self):
        self.assertRaises(yt.YtError, lambda: yt.upload_file("", TEST_DIR + "/dir/file"))

        file_path = TEST_DIR + "/file"
        yt.upload_file("", file_path)
        self.assertEqual("", yt.download_file(file_path, "string"))

        _, filename = tempfile.mkstemp()
        with open(filename, "w") as fout:
            fout.write("some content")

        destinationA = yt.smart_upload_file(filename, placement_strategy="hash")
        self.assertTrue(destinationA.startswith(yt.config.FILE_STORAGE))

        destinationB = yt.smart_upload_file(filename, placement_strategy="hash")
        self.assertEqual(destinationA, destinationB)

        destination = yt.smart_upload_file(filename, placement_strategy="random")
        self.assertTrue(destination.startswith(os.path.join(os.path.basename(filename), yt.config.FILE_STORAGE)))

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

        yt.run_erase(table)
        self.assertEqual(0, yt.records_count(table))

        yt.remove(dir, recursive=True)
        self.assertRaises(yt.YtError, lambda: yt.create_table(table))

    def test_simple_copy_move(self):
        table = TEST_DIR + "/table"
        dir = TEST_DIR + "/dir"
        other_table = dir + "/other_table"
        yt.create_table(table)
        self.assertEqual([], list(yt.read_table(table)))

        self.assertRaises(yt.YtError, lambda: yt.copy(table, table))
        self.assertRaises(yt.YtError, lambda: yt.move(table, table))

        self.assertRaises(yt.YtError, lambda: yt.copy(table, other_table))
        self.assertRaises(yt.YtError, lambda: yt.move(table, other_table))

        yt.mkdir(dir)
        yt.copy(table, other_table)

        self.assertTrue(yt.exists(table))
        self.assertTrue(yt.exists(other_table))

        # Remove it after fixes in move
        yt.remove(other_table)

        yt.move(table, other_table)
        self.assertFalse(yt.exists(table))
        self.assertTrue(yt.exists(other_table))

    def test_merge(self):
        tableX = TEST_DIR + "/tableX"
        tableY = TEST_DIR + "/tableY"
        dir = TEST_DIR + "/dir"
        res_table = dir + "/other_table"

        yt.write_table(tableX, ["x=1\n"])
        yt.write_table(tableY, ["y=2\n"])

        self.assertRaises(yt.YtError, lambda: yt.run_merge([tableX, tableY], res_table))
        self.assertRaises(yt.YtError, lambda: yt.run_merge([tableX, tableY], res_table))

        yt.mkdir(dir)
        yt.run_merge([tableX, tableY], res_table)
        self.check(["x=1\n"], yt.read_table(tableX))
        self.check(["y=2\n"], yt.read_table(tableY))
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

        self.assertRaises(yt.YtError, lambda: yt.run_sort([table, other_table], other_table, sort_by=["y"]))

        yt.run_sort(table, other_table, sort_by=["y"])
        self.assertItemsEqual(["x=1\n", "y=2\n"], yt.read_table(other_table))

        yt.run_sort(table, sort_by=["x"])
        self.assertItemsEqual(["y=2\n", "x=1\n"], yt.read_table(table))

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

    def test_write_many_chunks(self):
        yt.config.WRITE_BUFFER_SIZE = 1
        table = TEST_DIR + "/table"
        yt.write_table(table, ["x=1\n", "y=2\n", "z=3\n"])
        yt.write_table(table, ["x=1\n", "y=2\n", "z=3\n"])
        yt.write_table(table, ["x=1\n", "y=2\n", "z=3\n"])

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


        table = TEST_DIR + "/table"

        yt.write_table(table, ["x=1\n", "y=2\n"])
        yt.run_map(change_x, table, table)
        self.assertItemsEqual(["x=2\n", "y=2\n"], yt.read_table(table))

        yt.write_table(table, ["x=1\n", "y=2\n"])
        yt.run_map(ChangeX__(), table, table)
        self.assertItemsEqual(["x=2\n", "y=2\n"], yt.read_table(table))

        yt.write_table(table, ["x=2\n", "x=2\ty=2\n"])
        yt.run_sort(table, sort_by=["x"])
        yt.run_reduce(sum_y, table, table, reduce_by=["x"])
        self.assertItemsEqual(["x=2\ty=3\n"], yt.read_table(table))

    def test_binary_data_with_dsv(self):
        record = {"\tke\n\\\\y=": "\\x\\y\tz\n"}

        table = TEST_DIR + "/table"
        yt.write_table(table, map(yt.record_to_line, [record]))
        self.assertItemsEqual([record], map(yt.line_to_record, yt.read_table(table)))

    def test_yt_binary(self):
        current_dir = os.path.dirname(os.path.abspath(__file__))
        proc = subprocess.Popen(
            "YT_USE_TOKEN=0 YT_PROXY=%s %s" % (yt.config.http.PROXY, os.path.join(current_dir, "../test_yt.sh")),
            shell=True)
        proc.communicate()
        self.assertEqual(proc.returncode, 0)


    def check_command(self, command, post_action=None, check_action=None):
        mutation_id = yt.common.generate_uuid()
        def run_command():
            yt.config.MUTATION_ID = mutation_id
            result = command()
            yt.config.MUTATION_ID = None
            return result

        result = run_command()
        if post_action is not None:
            post_action()
        for _ in xrange(5):
            assert result == run_command()
            if check_action is not None:
                assert check_action()

    def test_master_mutation_id(self):
        test_dir = os.path.join(TEST_DIR, "test")
        test_dir2 = os.path.join(TEST_DIR, "test2")
        test_dir3 = os.path.join(TEST_DIR, "test3")

        self.check_command(
            lambda: yt.set(test_dir, {"a": "b"}),
            lambda: yt.set(test_dir, {}),
            lambda: yt.get(test_dir) == {})

        self.check_command(
            lambda: yt.remove(test_dir3, force=True),
            lambda: yt.mkdir(test_dir3),
            lambda: yt.get(test_dir3) == {})

        parent_tx = yt.start_transaction()
        transaction_count = yt.get("//sys/transactions/@count")
        self.check_command(
            lambda: yt.start_transaction(parent_tx),
            None,
            lambda: yt.get("//sys/transactions/@count") == transaction_count + 1)

        id = yt.start_transaction()
        self.check_command(lambda: yt.abort_transaction(id))

        id = yt.start_transaction()
        self.check_command(lambda: yt.commit_transaction(id))

        self.check_command(lambda: yt.move(test_dir, test_dir2))

    def test_scheduler_mutation_id(self):
        table = TEST_DIR + "/table"
        other_table = TEST_DIR + "/other_table"
        yt.write_table(table, ["x=1\n", "x=2\n"])
        yt.create_table(other_table)

        for command, params in \
            [(
                "map",
                {"spec":
                    {"mapper":
                        {"command": "sleep 1; cat"},
                     "input_table_paths": [table],
                     "output_table_paths": [other_table]}})]:

            op_count = yt.get("//sys/operations/@count")
            self.check_command(
                lambda: yt.driver.make_request(command, params),
                None,
                lambda: yt.get("//sys/operations/@count") == op_count + 1)

# Map method for test operations with python entities
class ChangeX__(object):
    def __call__(self, rec):
        if "x" in rec:
            rec["x"] = int(rec["x"]) + 1
        yield rec
