#!/usr/bin/python
# -*- coding: utf-8 -*-

from yt.wrapper.http import get_user_name
from yt.wrapper.client import Yt
from yt.wrapper.common import parse_bool
from yt.wrapper.table import TablePath
from yt.wrapper.tests.base import YtTestBase, TEST_DIR
from yt.wrapper.operation_commands import add_failed_operation_stderrs_to_error_message
from yt.environment import YTEnv
from yt.common import update
import yt.yson as yson
import yt.packages.simplejson as json
import yt.wrapper as yt

import os
import time
import random
import inspect
import tempfile
import string
import subprocess
import shutil
from StringIO import StringIO
from itertools import imap

import pytest

TESTS_LOCATION = os.path.dirname(os.path.abspath(__file__))

def _get_test_file_path(name):
    return os.path.join(TESTS_LOCATION, "files", name)

def test_docs_exist():
    functions = inspect.getmembers(yt, lambda o: inspect.isfunction(o) and \
                                                 not o.__name__.startswith('_'))
    functions_without_doc = filter(lambda (name, func): not inspect.getdoc(func), functions)
    assert not functions_without_doc

    classes = inspect.getmembers(yt, lambda o: inspect.isclass(o))
    for name, cl in classes:
        assert inspect.getdoc(cl)
        if name == "PingTransaction":
            continue # Python Thread is not documented O_o
        public_methods = inspect.getmembers(cl, lambda o: inspect.ismethod(o) and \
                                                          not o.__name__.startswith('_'))
        methods_without_doc = [method for name, method in public_methods
                                                            if (not inspect.getdoc(method))]
        assert not methods_without_doc

def test_reliable_remove_tempfiles():
    def dummy_buggy_upload(*args, **kwargs):
        raise TypeError

    def foo(rec):
        yield rec

    real_upload = yt.table_commands._prepare_binary.func_globals['_reliably_upload_files']
    yt.table_commands._prepare_binary.func_globals['_reliably_upload_files'] = dummy_buggy_upload
    old_tmp_dir = yt.config["local_temp_directory"]
    yt.config["local_temp_directory"] = tempfile.mkdtemp(dir=old_tmp_dir)
    try:
        files_before_fail = os.listdir(yt.config["local_temp_directory"])
        with pytest.raises(TypeError):
            yt.table_commands._prepare_binary(foo, "mapper")
        files_after_fail = os.listdir(yt.config["local_temp_directory"])
        assert files_after_fail == files_before_fail
    finally:
        yt.table_commands._prepare_binary.func_globals['_reliably_upload_files'] = real_upload
        shutil.rmtree(yt.config["local_temp_directory"])
        yt.config["local_temp_directory"] = old_tmp_dir

class NativeModeTester(YtTestBase, YTEnv):
    @classmethod
    def setup_class(cls, config=None):
        if config is None:
            config = {}
        config["tabular_data_format"] = yt.format.DsvFormat()
        super(NativeModeTester, cls).setup_class(config)
        yt.create("user", attributes={"name": "tester"})
        yt.create("group", attributes={"name": "testers"})
        yt.create("group", attributes={"name": "super_testers"})

    @classmethod
    def teardown_class(cls):
        yt.remove("//sys/users/tester", force=True)
        yt.remove("//sys/groups/testers", force=True)
        yt.remove("//sys/groups/super_testers", force=True)
        super(NativeModeTester, cls).teardown_class()

    # Check equality of records in dsv format
    def check(self, recordsA, recordsB):
        def prepare(records):
            return map(yt.loads_row, sorted(list(records)))
        assert prepare(recordsA) == prepare(recordsB)

    def get_temp_dsv_records(self):
        columns = (string.digits, reversed(string.ascii_lowercase[:10]), string.ascii_uppercase)
        def dumps_row(row):
            return "x={0}\ty={1}\tz={2}\n".format(*row)
        return map(dumps_row, zip(*columns))

    def random_string(self, length):
        char_set = string.ascii_lowercase + string.digits + string.ascii_uppercase
        return "".join(random.sample(char_set, length))

    ###
    ### test_cypress_commands
    ###

    def test_ypath(self):
        path = yt.TablePath("<append=false;sort-by=[key]>//my/table")
        assert str(path) == "//my/table"
        assert repr(path) == "//my/table"
        assert not path.append
        assert path.attributes == {"append": "false", "sort_by": ["key"]}

    def test_get_set_exists(self):
        assert yt.get("/")
        assert len(yt.list("/")) > 1
        with pytest.raises(yt.YtError):
            yt.get("//none")

        assert yt.exists("/")
        assert yt.exists(TEST_DIR)
        assert not yt.exists(TEST_DIR + "/some_node")

        with pytest.raises(yt.YtError):
            yt.set(TEST_DIR + "/some_node/embedded_node", {})
        yt.set(TEST_DIR + "/some_node", {})

        assert yt.exists(TEST_DIR + "/some_node")

        yt.set_attribute(TEST_DIR + "/some_node", "attr", 1)
        assert yt.get(TEST_DIR + "/some_node", attributes=["attr", "other_attr"])\
                .attributes == {"attr": 1L}

        assert json.loads(yt.get(TEST_DIR, format=yt.format.JsonFormat())) == {"some_node": {}}
        assert json.loads(yt.get(TEST_DIR, format="json")) == {"some_node": {}}

    def test_remove(self):
        for recursive in [False, True]:
            with pytest.raises(yt.YtError):
                yt.remove(TEST_DIR + "/some_node", recursive=recursive)
            yt.remove(TEST_DIR + "/some_node", recursive=recursive, force=True)

        for force in [False, True]:
            yt.set(TEST_DIR + "/some_node", {})
            yt.remove(TEST_DIR + "/some_node",
                      recursive=True,
                      force=force)

    def test_mkdir(self):
        yt.mkdir(TEST_DIR, recursive=True)
        with pytest.raises(yt.YtError):
            yt.mkdir(TEST_DIR)

        with pytest.raises(yt.YtError):
            yt.mkdir(TEST_DIR + "/x/y")
        yt.mkdir(TEST_DIR + "/x")
        yt.mkdir(TEST_DIR + "/x/y/z", recursive=True)

    def test_search(self):
        yt.mkdir(TEST_DIR + "/dir/other_dir", recursive=True)
        yt.create_table(TEST_DIR + "/dir/table")
        yt.upload_file("", TEST_DIR + "/file")

        assert set(yt.search(TEST_DIR)) == set([TEST_DIR, TEST_DIR + "/dir",
                                                TEST_DIR + "/dir/other_dir",
                                                TEST_DIR + "/dir/table",
                                                TEST_DIR + "/file"])

        res = yt.search(TEST_DIR, map_node_order=lambda path, object: sorted(object))
        assert list(res) == [TEST_DIR, TEST_DIR + "/dir", TEST_DIR + "/dir/other_dir",
                             TEST_DIR + "/dir/table", TEST_DIR + "/file"]

        assert set(yt.search(TEST_DIR, node_type="file")) == set([TEST_DIR + "/file"])

        assert set(yt.search(TEST_DIR, node_type="table",
                             path_filter=lambda x: x.find("dir") != -1)) == set([TEST_DIR + "/dir/table"])

        # Search empty tables
        res = yt.search(TEST_DIR, attributes=["row_count"],
                        object_filter=lambda x: x.attributes.get("row_count", -1) == 0)
        assert sorted(list(res)) == sorted([yson.to_yson_type(TEST_DIR + "/dir/table",
                                                              {"row_count": 0})])

    def test_create(self):
        with pytest.raises(yt.YtError):
            yt.create("map_node", TEST_DIR + "/map", attributes={"type": "table"})
        yt.create("map_node", TEST_DIR + "/dir")
        with pytest.raises(yt.YtError):
            yt.create("map_node", TEST_DIR + "/dir")
        yt.create("map_node", TEST_DIR + "/dir", ignore_existing=True)

        try:
            yt.create("user", attributes={"name": "test_user"})
            assert "test_user" in yt.get("//sys/users")
            yt.create("group", attributes={"name": "test_group"})
            assert "test_group" in yt.get("//sys/groups")
            yt.create("account", attributes={"name": "test_account"})
            assert "test_account" in yt.get("//sys/accounts")
        finally:
            yt.remove("//sys/users/test_user", force=True)
            yt.remove("//sys/groups/test_group", force=True)
            yt.remove("//sys/accounts/test_account", force=True)

    def test_attributes_commands(self):
        table = TEST_DIR + "/table_with_attributes"
        yt.write_table(table, ["x=1\ty=1\n", "x=2\ty=2\n"])
        assert yt.records_count(table) == 2
        assert not yt.is_sorted(table)

        yt.set_attribute(table, "my_attribute", {})
        yt.set_attribute(table, "my_attribute/000", 10)
        assert yt.get_attribute(table, "my_attribute/000") == 10
        assert yt.list_attributes(table, "my_attribute") == ["000"]
        assert yt.get_attribute(table, "user_attribute_keys") == ["my_attribute"]
        assert yt.get(table + "/@my_attribute") == {"000": 10}

        dir_name = TEST_DIR + "/dir"
        yt.create("map_node", dir_name, attributes={"attr": 1})
        yt.set_attribute(dir_name, "second_attr", "str")
        assert yt.has_attribute(dir_name, "second_attr")
        assert yt.get(dir_name, attributes=["attr", "second_attr"]).attributes == \
                {"attr": 1, "second_attr": "str"}

        result = list(yt.search(table, node_type='table', attributes=('my_attribute', )))
        assert len(result) == 1
        assert str(result[0]) == table
        assert result[0].attributes['my_attribute'] == {'000': 10}

    def test_link(self):
        table = TEST_DIR + "/table_with_attributes"
        link = TEST_DIR + "/table_link"
        yt.write_table(table, ["x=1\ty=1\n", "x=2\ty=2\n"])
        yt.link(table, link)
        assert not parse_bool(yt.get_attribute(link + "&", "broken"))
        assert yt.get_attribute(link + "&", "target_id") == yt.get_attribute(table, "id")

        with pytest.raises(yt.YtError):
            yt.link(table, link)
        yt.link(table, link, ignore_existing=True)

        other_link = TEST_DIR + "/other_link"
        yt.link(link, other_link, recursive=False)
        assert yt.get_attribute(other_link + "&", "target_id") == yt.get_attribute(link, "id")
        yt.remove(other_link, force=True)
        yt.link(link, other_link, recursive=True)
        assert yt.get_attribute(other_link + "&", "target_id") == yt.get_attribute(table, "id")

    def test_list(self):
        tables = ["{0}/{1}".format(TEST_DIR, name) for name in ("a", "b", "c")]
        for table in tables:
            yt.create_table(table)
        assert set(yt.list(TEST_DIR)) == set(["a", "b", "c"])
        assert set(yt.list(TEST_DIR, absolute=True)) == \
                set(["{0}/{1}".format(TEST_DIR, x) for x in ("a", "b", "c")])
        yt.mkdir(TEST_DIR + "/subdir")
        yt.create_table(TEST_DIR + "/subdir/table")

        result = yt.list(TEST_DIR + "/subdir", attributes=["type"])[0]
        assert str(result) == "table"
        assert result.attributes == {"type": "table"}

    def test_get_type(self):
        table = TEST_DIR + "/table"
        map_node = TEST_DIR + "/map_node"
        yt.create("table", table)
        yt.create("map_node", map_node)
        assert yt.get_type(table) == "table"
        assert yt.get_type(map_node) == "map_node"

    def test_simple_copy_move(self):
        table = TEST_DIR + "/table"
        dir = TEST_DIR + "/dir"
        other_table = dir + "/other_table"
        yt.create_table(table)
        assert list(yt.read_table(table, format=yt.format.DsvFormat())) == []

        with pytest.raises(yt.YtError):
            yt.copy(table, table)
        with pytest.raises(yt.YtError):
            yt.move(table, table)

        with pytest.raises(yt.YtError):
            yt.copy(table, other_table)
        with pytest.raises(yt.YtError):
            yt.move(table, other_table)

        yt.mkdir(dir)
        yt.copy(table, other_table)

        assert yt.exists(table)
        assert yt.exists(other_table)

        # Remove it after fixes in move
        yt.remove(other_table)

        yt.move(table, other_table)
        assert not yt.exists(table)
        assert yt.exists(other_table)

    def test_transactions(self):
        table = TEST_DIR + "/transaction_test_table"

        yt.create_table(table)
        yt.write_table(table, ["x=1\n"], format=yt.format.DsvFormat())

        def read_table(client=None):
            return yt.read_table(table, format=yt.format.DsvFormat(), client=client).read()

        new_client = yt.client.Yt(token=yt.config["token"], config=yt.config)

        with yt.Transaction():
            yt.write_table(table, ["x=2\n"], format=yt.format.DsvFormat())
            assert read_table(new_client) == "x=1\n"

        assert read_table(new_client) == "x=2\n"

        with yt.Transaction(timeout=2000, ping=False):
            yt.write_table(table, ["x=3\n"], format=yt.format.DsvFormat())
            time.sleep(3)

        assert read_table() == "x=2\n"
        assert read_table(new_client) == "x=2\n"

        with yt.Transaction(timeout=1000):
            yt.write_table(table, ["x=3\n"], format=yt.format.DsvFormat())
            time.sleep(3)

        assert read_table() == "x=3\n"
        assert read_table(new_client) == "x=3\n"

        with yt.Transaction(timeout=1000):
            yt.write_table(table, ["x=4\n"], format=yt.format.DsvFormat())
            time.sleep(3)

        assert read_table() == "x=4\n"

        with yt.Transaction():
            yt.write_table(table, ["x=5\n"], format=yt.format.DsvFormat())
            time.sleep(3)
            read_table(new_client) == "x=4\n"

        assert read_table() == "x=5\n"
        assert read_table(new_client) == "x=5\n"

        try:
            with yt.Transaction(timeout=3000) as tx:
                transaction_id = tx.transaction_id
                raise yt.YtError("test error")
        except:
            pass

        assert not yt.exists("//sys/transactions/" + transaction_id)

    def test_lock(self):
        dir = TEST_DIR + "/dir"

        yt.mkdir(dir)
        assert len(yt.get(dir + "/@locks")) == 0

        with yt.Transaction():
            yt.lock(dir)
            assert len(yt.get(dir + "/@locks")) == 1

        assert len(yt.get(dir + "/@locks")) == 0
        with yt.Transaction():
            assert yt.lock(dir, waitable=True) != "0-0-0-0"
            assert yt.lock(dir, waitable=True) == "0-0-0-0"
            assert yt.lock(dir, waitable=True, wait_for=1000) == "0-0-0-0"

        tx = yt.start_transaction()

        yt.config.TRANSACTION = tx
        yt.lock(dir, waitable=True)
        #with pytest.raises(yt.YtError):
        #    yt.lock(dir, waitable=True)
        assert yt.lock(dir, waitable=True) == "0-0-0-0"
        yt.config.TRANSACTION = "0-0-0-0"

        with pytest.raises(yt.YtError):
            with yt.Transaction():
                yt.lock(dir, waitable=True, wait_for=1000)

        yt.abort_transaction(tx)

    def test_copy_move_sorted_table(self):
        def is_sorted_by_y(table_path):
            sorted_by = yt.get_attribute(table_path, "sorted_by", None)
            if sorted_by is None:
                sorted_by = yt.get_attribute(table_path, "key_columns", None)
            return sorted_by == ["y"]

        table = TEST_DIR + "/table"
        other_table = TEST_DIR + "/other_table"
        another_table = TEST_DIR + "/another_table"

        yt.write_table(table, ["x=1\ty=2\n", "x=3\ty=1\n", "x=2\ty=3\n"])
        yt.run_sort(table, sort_by=["y"])

        yt.copy(table, other_table)
        assert yt.is_sorted(other_table)
        assert is_sorted_by_y(other_table)

        yt.move(table, another_table)
        assert yt.is_sorted(another_table)
        assert is_sorted_by_y(another_table)

    def test_utf8(self):
        yt.create("table", TEST_DIR + "/table", attributes={"attr": u"капуста"})

    ###
    ### test_acl_commands
    ###

    def test_check_permission(self):
        assert yt.check_permission("tester", "read", "//sys")["action"] == "allow"
        assert yt.check_permission("tester", "write", "//sys")["action"] == "deny"
        assert yt.check_permission("root", "write", "//sys")["action"] == "allow"
        assert yt.check_permission("root", "administer", "//home")["action"] == "allow"
        assert yt.check_permission("root", "use", "//home")["action"] == "allow"
        permissions = ["read", "write", "administer", "remove"]
        yt.create("map_node", "//home/tester", attributes={"inherit_acl": "false",
            "acl": [{"action": "allow",
                     "subjects": ["tester"],
                     "permissions": permissions}]})
        try:
            for permission in permissions:
                assert yt.check_permission("tester", permission, "//home/tester")["action"] == "allow"
        finally:
            yt.remove("//home/tester", force=True)

    def test_add_remove_member(self):
        yt.add_member("tester", "testers")
        assert yt.get_attribute("//sys/groups/testers", "members") == ["tester"]
        assert set(yt.get_attribute("//sys/users/tester", "member_of")) == set(["users", "testers"])
        assert set(yt.get_attribute("//sys/users/tester", "member_of_closure")) == set(["users", "testers", "everyone"])

        yt.remove_member("tester", "testers")
        assert yt.get_attribute("//sys/groups/testers", "members") == []
        assert "testers" not in yt.get_attribute("//sys/users/tester", "member_of")

        yt.add_member("testers", "super_testers")
        assert yt.get_attribute("//sys/groups/testers", "member_of") == ["super_testers"]
        assert yt.get_attribute("//sys/groups/super_testers", "members") == ["testers"]
        yt.add_member("tester", "testers")
        assert "super_testers" in yt.get_attribute("//sys/users/tester", "member_of_closure")

        yt.remove_member("testers", "super_testers")
        assert yt.get_attribute("//sys/groups/super_testers", "members") == []
        assert "super_testers" not in yt.get_attribute("//sys/groups/testers", "member_of")

    ###
    ### test_file_commands
    ###

    def test_file_commands(self):
        with pytest.raises(yt.YtError):
            yt.upload_file("", TEST_DIR + "/dir/file")

        file_path = TEST_DIR + "/file"
        yt.upload_file("", file_path)
        assert yt.download_file(file_path).read() == ""

        yt.upload_file("0" * 1000, file_path)
        assert yt.download_file(file_path).read() == "0" * 1000

        _, filename = tempfile.mkstemp()
        with open(filename, "w") as fout:
            fout.write("some content")

        destinationA = yt.smart_upload_file(filename, placement_strategy="hash")
        assert destinationA.startswith(yt.config["remote_temp_files_directory"])

        destinationB = yt.smart_upload_file(filename, placement_strategy="hash")
        assert destinationA == destinationB

        destination = yt.smart_upload_file(filename, placement_strategy="random")
        path = os.path.join(os.path.basename(filename), yt.config["remote_temp_files_directory"])
        assert destination.startswith(path)

        destination = TEST_DIR + "/file_dir/some_file"
        yt.smart_upload_file(filename, destination=destination, placement_strategy="ignore")
        assert yt.get_attribute(destination, "file_name") == "some_file"

    ###
    ### test_table_commands
    ###

    def test_read_write(self):
        table = TEST_DIR + "/table"
        yt.create_table(table)
        self.check([], yt.read_table(table))

        yt.write_table(table, "x=1\n")
        self.check(["x=1\n"], yt.read_table(table))

        yt.write_table(table, ["x=1\n"])
        self.check(["x=1\n"], yt.read_table(table))

        yt.write_table(table, [{"x": 1}], raw=False)
        self.check(["x=1\n"], yt.read_table(table))

        yt.write_table(table, iter(["x=1\n"]))
        self.check(["x=1\n"], yt.read_table(table))

        yt.write_table(yt.TablePath(table, append=True), ["y=1\n"])
        self.check(["x=1\n", "y=1\n"], yt.read_table(table))

        yt.write_table(yt.TablePath(table), ["x=1\n", "y=1\n"])
        self.check(["x=1\n", "y=1\n"], yt.read_table(table))

        yt.write_table(table, ["y=1\n"])
        self.check(["y=1\n"], yt.read_table(table))

        yt.write_table(table, StringIO("y=1\n"), raw=True, format=yt.DsvFormat())
        self.check(["y=1\n"], yt.read_table(table))

        response_parameters = {}
        yt.read_table(table, response_parameters=response_parameters)
        assert {"start_row_index": 0, "approximate_row_count": 1} == response_parameters

        yt.write_table(table, [{"y": "1"}], raw=False)
        assert [{"y": "1"}] == list(yt.read_table(table, raw=False))

    def test_empty_table(self):
        dir = TEST_DIR + "/dir"
        table = dir + "/table"

        with pytest.raises(yt.YtError):
            yt.create_table(table)
        with pytest.raises(yt.YtError):
            yt.records_count(table)

        yt.create_table(table, recursive=True)
        assert yt.records_count(table) == 0
        self.check([], yt.read_table(table, format=yt.DsvFormat()))

        yt.run_erase(table)
        assert yt.records_count(table) == 0

        yt.remove(dir, recursive=True)
        with pytest.raises(yt.YtError):
            yt.create_table(table)

    def test_create_temp_table(self):
        table = yt.create_temp_table(path=TEST_DIR)
        assert table.startswith(TEST_DIR)

        table = yt.create_temp_table(path=TEST_DIR, prefix="prefix")
        assert table.startswith(TEST_DIR + "/prefix")

    def test_write_many_chunks(self):
        yt.config.WRITE_BUFFER_SIZE = 1
        table = TEST_DIR + "/table"
        yt.write_table(table, ["x=1\n", "y=2\n", "z=3\n"])
        yt.write_table(table, ["x=1\n", "y=2\n", "z=3\n"])
        yt.write_table(table, ["x=1\n", "y=2\n", "z=3\n"])

    def test_binary_data_with_dsv(self):
        record = {"\tke\n\\\\y=": "\\x\\y\tz\n"}

        table = TEST_DIR + "/table"
        yt.write_table(table, map(yt.dumps_row, [record]))
        assert [record] == map(yt.loads_row, yt.read_table(table))

    def test_mount_unmount(self):
        if yt.config["api_version"] == "v2":
            pytest.skip()

        table = TEST_DIR + "/table"
        yt.create_table(table)
        yt.set(table + "/@schema", [{"name": name, "type": "string"} for name in ["x", "y"]])
        yt.set(table + "/@key_columns", ["x"])

        tablet_id = yt.create("tablet_cell", attributes={"size": 1})
        while yt.get("//sys/tablet_cells/{0}/@health".format(tablet_id)) != 'good':
            time.sleep(0.1)

        yt.mount_table(table)
        while yt.get("{0}/@tablets/0/state".format(table)) != 'mounted':
            time.sleep(0.1)

        yt.unmount_table(table)
        while yt.get("{0}/@tablets/0/state".format(table)) != 'unmounted':
            time.sleep(0.1)

    @pytest.mark.skipif('os.environ.get("BUILD_ENABLE_LLVM", None) == "NO"')
    def test_select(self):
        if yt.config["api_version"] == "v2":
            pytest.skip()

        table = TEST_DIR + "/table"

        def select():
            return list(yt.select_rows("* from [{0}]".format(table),
                                       format=yt.YsonFormat(format="text", process_table_index=False), raw=False))

        yt.remove(table, force=True)
        yt.create_table(table)
        yt.run_sort(table, sort_by=["x"])

        yt.set(table + "/@schema", [{"name": name, "type": "int64"} for name in ["x", "y", "z"]])
        yt.set(table + "/@key_columns", ["x"])

        assert [] == select()

        yt.write_table(yt.TablePath(table, append=True, sorted_by=["x"]),
                       ["{x=1;y=2;z=3}"], format=yt.YsonFormat())

        assert [{"x": 1, "y": 2, "z": 3}] == select()

    def test_insert_lookup_delete(self):
        if yt.config["api_version"] == "v2":
            pytest.skip()

        yt.config["tabular_data_format"] = None
        try:
            # Name must differ with name of table in select test because of metadata caches
            table = TEST_DIR + "/table2"
            yt.remove(table, force=True)
            yt.create_table(table)
            yt.set(table + "/@schema", [{"name": name, "type": "string"} for name in ["x", "y"]])
            yt.set(table + "/@key_columns", ["x"])

            tablet_id = yt.create("tablet_cell", attributes={"size": 1})
            while yt.get("//sys/tablet_cells/{0}/@health".format(tablet_id)) != 'good':
                time.sleep(0.1)

            yt.mount_table(table)
            while yt.get("{0}/@tablets/0/state".format(table)) != 'mounted':
                time.sleep(0.1)

            yt.insert_rows(table, [{"x": "a", "y": "b"}])
            assert [{"x": "a", "y": "b"}] == list(yt.select_rows("* from [{0}]".format(table), raw=False))

            yt.insert_rows(table, [{"x": "c", "y": "d"}])
            assert [{"x": "c", "y": "d"}] == list(yt.lookup_rows(table, [{"x": "c"}]))

            yt.delete_rows(table, [{"x": "a"}])
            assert [{"x": "c", "y": "d"}] == list(yt.select_rows("* from [{0}]".format(table), raw=False))
        finally:
            yt.config["tabular_data_format"] = yt.format.DsvFormat()

    def test_start_row_index(self):
        table = TEST_DIR + "/table"

        yt.write_table(yt.TablePath(table, sorted_by=["a"]), ["a=b\n", "a=c\n", "a=d\n"])

        rsp = yt.read_table(table)
        assert rsp.response_parameters == {"start_row_index": 0L,
                                           "approximate_row_count": 3L}

        rsp = yt.read_table(yt.TablePath(table, start_index=1))
        assert rsp.response_parameters == {"start_row_index": 1L,
                                           "approximate_row_count": 2L}

        rsp = yt.read_table(yt.TablePath(table, lower_key=["d"]))
        assert rsp.response_parameters == \
            {"start_row_index": 2L,
             # When reading with key limits row count is estimated rounded up to the chunk row count.
             "approximate_row_count": 3L}

        rsp = yt.read_table(yt.TablePath(table, lower_key=["x"]))
        assert rsp.response_parameters == {"start_row_index": 0L,
                                           "approximate_row_count": 0L}

    def test_table_index(self):
        dsv = yt.format.DsvFormat(enable_table_index=True, table_index_column="TableIndex")
        schemaful_dsv = yt.format.SchemafulDsvFormat(columns=['1', '2', '3'],
                                                     enable_table_index=True,
                                                     table_index_column="_table_index_")

        src_table_a = TEST_DIR + '/in_table_a'
        src_table_b = TEST_DIR + '/in_table_b'
        dst_table_a = TEST_DIR + '/out_table_a'
        dst_table_b = TEST_DIR + '/out_table_b'
        dst_table_ab = TEST_DIR + '/out_table_ab'

        len_a = 5
        len_b = 3

        yt.create_table(src_table_a, recursive=True, ignore_existing=True)
        yt.create_table(src_table_b, recursive=True, ignore_existing=True)
        yt.write_table(src_table_a, "1=a\t2=a\t3=a\n" * len_a, format=dsv)
        yt.write_table(src_table_b, "1=b\t2=b\t3=b\n" * len_b, format=dsv)

        assert yt.records_count(src_table_a) == len_a
        assert yt.records_count(src_table_b) == len_b

        def mix_table_indexes(row):
            row["_table_index_"] = row["TableIndex"]
            yield row
            row["_table_index_"] = 2
            yield row

        yt.table_commands.run_map(binary=mix_table_indexes,
                                  source_table=[src_table_a, src_table_b],
                                  destination_table=[dst_table_a, dst_table_b, dst_table_ab],
                                  input_format=dsv,
                                  output_format=schemaful_dsv)
        assert yt.records_count(dst_table_b) == len_b
        assert yt.records_count(dst_table_a) == len_a
        assert yt.records_count(dst_table_ab) == len_a + len_b
        for table in (dst_table_a, dst_table_b, dst_table_ab):
            row = yt.read_table(table, raw=False).next()
            for field in ("@table_index", "TableIndex", "_table_index_"):
                assert field not in row

    def test_erase(self):
        table = TEST_DIR + "/table"
        yt.write_table(table, self.get_temp_dsv_records())
        assert yt.records_count(table) == 10
        yt.run_erase(TablePath(table, start_index=0, end_index=5))
        assert yt.records_count(table) == 5
        yt.run_erase(TablePath(table, start_index=0, end_index=5))
        assert yt.records_count(table) == 0

    def test_read_with_ranges(self):
        table = TEST_DIR + "/table"
        yt.write_table(table, ["y=w3\n", "x=b\ty=w1\n", "x=a\ty=w2\n"])
        yt.run_sort(table, sort_by=["x", "y"])

        def read_table(**kwargs):
            return list(yt.read_table(TablePath(table, **kwargs), raw=False))

        assert read_table(lower_key="a", upper_key="d") == [{"x": "a", "y": "w2"},
                                                            {"x": "b", "y": "w1"}]
        assert read_table(columns=["y"]) == [{"y": "w" + str(i)} for i in [3, 2, 1]]
        assert read_table(lower_key="a", end_index=2, columns=["x"]) == [{"x": "a"}]
        assert read_table(start_index=0, upper_key="b") == [{"y": "w3"}, {"x": "a", "y": "w2"}]
        assert read_table(start_index=1, columns=["x"]) == [{"x": "a"}, {"x": "b"}]

        assert list(yt.read_table(table + "{y}[:#2]")) == ["y=w3\n", "y=w2\n"]
        assert list(yt.read_table(table + "[#1:]")) == ["x=a\ty=w2\n", "x=b\ty=w1\n"]

        assert list(yt.read_table("<ranges=[{"
                                  "lower_limit={key=[b]}"
                                  "}]>" + table)) == ["x=b\ty=w1\n"]
        assert list(yt.read_table("<ranges=[{"
                                  "upper_limit={row_index=2}"
                                  "}]>" + table)) == ["y=w3\n", "x=a\ty=w2\n"]

        with pytest.raises(yt.YtError):
            yt.read_table(TablePath(table, lower_key="a", start_index=1))
        with pytest.raises(yt.YtError):
            yt.read_table(TablePath(table, upper_key="c", end_index=1))
        yt.write_table(table, ["x=b\n", "x=a\n", "x=c\n"])
        with pytest.raises(yt.YtError):
            yt.read_table(TablePath(table, lower_key="a"))

    def test_huge_table(self):
        table = TEST_DIR + "/table"
        power = 3
        records = imap(yt.dumps_row, ({"k": i, "s": i * i, "v": "long long string with strange symbols"
                                                                " #*@*&^$#%@(#!@:L|L|KL..,,.~`"}
                                      for i in xrange(10 ** power)))
        yt.write_table(table, yt.StringIterIO(records))

        assert yt.records_count(table) == 10 ** power

        records_count = 0
        for _ in yt.read_table(table):
            records_count += 1
        assert records_count == 10 ** power

    ###
    ### test_operations
    ###

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
        self.check(["x=1\n", "y=2\n"], yt.read_table(res_table))

        yt.run_merge(tableX, res_table)
        assert not parse_bool(yt.get_attribute(res_table, "sorted"))
        self.check(["x=1\n"], yt.read_table(res_table))

        yt.run_sort(tableX, sort_by="x")
        yt.run_merge(tableX, res_table)
        assert parse_bool(yt.get_attribute(res_table, "sorted"))
        self.check(["x=1\n"], yt.read_table(res_table))

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
        self.check(["x=1\n", "x=2\n"], yt.read_table(table))
        yt.run_sort(table, sort_by=["x"])
        with pytest.raises(yt.YtError):
            yt.run_reduce("cat", table, [], reduce_by=["x"])

        yt.run_reduce("cat", table, table, reduce_by=["x"])
        self.check(["x=1\n", "x=2\n"], yt.read_table(table))

        with pytest.raises(yt.YtError):
            yt.run_map("cat", table, table, table_writer={"max_row_weight": 1})

        yt.run_map("grep 2", table, other_table)
        self.check(["x=2\n"], yt.read_table(other_table))

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
                   files=_get_test_file_path("capitalize_b.py"))
        records = yt.read_table(other_table, raw=False)
        assert sorted([rec["b"] for rec in records]) == ["IGNAT", "MAX", "NAME"]
        assert sorted([rec["c"] for rec in records]) == []

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

        table = TEST_DIR + "/table"

        yt.write_table(table, ["x=1\n", "y=2\n"])
        yt.run_map(change_x, table, table, format=None)
        self.check(yt.read_table(table), ["x=2\n", "y=2\n"])

        yt.write_table(table, ["x=1\n", "y=2\n"])
        yt.run_map(change_x, table, table)
        self.check(yt.read_table(table), ["x=2\n", "y=2\n"])

        for mode in ["method", "staticmethod", "classmethod"]:
            yt.write_table(table, ["x=1\n", "y=2\n"])
            yt.run_map(ChangeX__(mode), table, table)
            self.check(yt.read_table(table), ["x=2\n", "y=2\n"])

        yt.write_table(table, ["x=1\n", "y=2\n"])
        yt.run_map(TMapperWithMetaclass().map, table, table)
        self.check(yt.read_table(table), ["x=2\n", "y=2\n"])

        yt.write_table(table, ["x=2\n", "x=2\ty=2\n"])
        yt.run_sort(table, sort_by=["x"])
        yt.run_reduce(sum_y, table, table, reduce_by=["x"])
        self.check(yt.read_table(table), ["y=3\tx=2\n"])

        yt.write_table(table, ["x=1\n", "y=2\n"])
        yt.run_map(change_field, table, table)
        self.check(yt.read_table(table), ["z=8\n", "z=8\n"])

    def test_cross_format_operation(self):
        @yt.raw
        def reformat(rec):
            values = rec.strip().split("\t", 2)
            yield "\t".join("=".join([k, v]) for k, v in zip(["k", "s", "v"], values)) + "\n"

        yt.config["tabular_data_format"] = yt.format.YamrFormat(has_subkey=True)
        try:
            table = TEST_DIR + "/table"
            other_table = TEST_DIR + "/other_table"
            yt.write_table(table, ["0\ta\tA\n", "1\tb\tB\n"])
            yt.run_map(reformat, table, other_table, output_format=yt.format.DsvFormat())
            assert sorted(yt.read_table(other_table, format=yt.format.DsvFormat())) == \
                   ["k=0\ts=a\tv=A\n", "k=1\ts=b\tv=B\n"]
        finally:
            yt.config["tabular_data_format"] = yt.format.DsvFormat()

    def test_python_operations_io(self):
        """ All access (except read-only) to stdin/out during the operation should be disabled """
        table = TEST_DIR + "/table_io_test"

        yt.write_table(table, ["x=1\n", "y=2\n"])

        import sys

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
                   files=_get_test_file_path("many_output.py"))

        for table in output_tables:
            assert yt.records_count(table) == 1
        self.check(["x=1\ty=1\n", "x=10\ty=10\n"], yt.read_table(append_table))

    def test_attached_mode(self):
        table = TEST_DIR + "/table"

        yt.config["detached"] = 0
        try:
            yt.write_table(table, ["x=1\n"])
            yt.run_map("cat", table, table)
            self.check(yt.read_table(table), ["x=1\n"])
            yt.run_merge(table, table)
            self.check(yt.read_table(table), ["x=1\n"])
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
            time.sleep(2.5)
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
        self.check(["x=1\n", "y=2\n"], sorted(list(yt.read_table(table))))

    def test_reduce_differently_sorted_table(self):
        table = TEST_DIR + "/table"
        other_table = TEST_DIR + "/other_table"
        yt.create("table", table)
        yt.run_sort(table, sort_by=["a", "b"])

        with pytest.raises(yt.YtError):
            yt.run_reduce("cat", source_table=table, destination_table=other_table, reduce_by=["c"])

    def test_reduce_with_output_sorted(self):
        table = TEST_DIR + "/table"
        output_table = TEST_DIR + "/output_table"

        yt.write_table(table, ["x=2\n", "x=3\n", "x=1\n"])
        yt.run_sort(table, sort_by=["x"])

        yt.run_reduce("cat", table, "<sorted_by=[x]>" + output_table, reduce_by=["x"])
        assert yt.is_sorted(output_table)

    @add_failed_operation_stderrs_to_error_message
    def test_yamred_dsv(self):
        def foo(rec):
            yield rec

        table = TEST_DIR + "/table"
        yt.write_table(table, ["x=1\ty=2\n"])

        yt.run_map(foo, table, table,
                   input_format=yt.create_format("<key_column_names=[\"y\"]>yamred_dsv"),
                   output_format=yt.YamrFormat(has_subkey=False, lenval=False))
        self.check(["key=2\tvalue=x=1\n"], sorted(list(yt.read_table(table))))

    def test_schemaful_dsv(self):
        def foo(rec):
            yield rec

        table = TEST_DIR + "/table"
        yt.write_table(table, ["x=1\ty=2\n", "x=\\n\tz=3\n"])
        self.check(["1\n", "\\n\n"],
                   sorted(list(yt.read_table(table, format=yt.SchemafulDsvFormat(columns=["x"])))))

        yt.run_map(foo, table, table, format=yt.SchemafulDsvFormat(columns=["x"]))
        self.check(["x=1\n", "x=\\n\n"], sorted(list(yt.read_table(table))))

    ###
    ### test_misc
    ###

    def test_yt_binary(self):
        env = self.get_environment()
        if yt.config["api_version"] == "v2":
            env["FALSE"] = '"false"'
            env["TRUE"] = '"true"'
        else:
            env["FALSE"] = '%false'
            env["TRUE"] = '%true'

        current_dir = os.path.dirname(os.path.abspath(__file__))
        proc = subprocess.Popen(
            os.path.join(current_dir, "../test_yt.sh"),
            shell=True,
            env=env)
        proc.communicate()
        assert proc.returncode == 0

    def check_command(self, command, post_action=None, check_action=None, final_action=None):
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
            yt.config.RETRY = True
            assert result == run_command()
            yt.config.RETRY = None
            if check_action is not None:
                assert check_action()

        if final_action is not None:
            final_action(result)

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
        self.check_command(
            lambda: yt.start_transaction(parent_tx),
            None,
            lambda: len(yt.get("//sys/transactions/{0}/@nested_transaction_ids".format(parent_tx))) == 1)

        id = yt.start_transaction()
        self.check_command(lambda: yt.abort_transaction(id))

        id = yt.start_transaction()
        self.check_command(lambda: yt.commit_transaction(id))

        self.check_command(lambda: yt.move(test_dir, test_dir2))

    def test_scheduler_mutation_id(self):
        def abort(operation_id):
            yt.abort_operation(operation_id)
            time.sleep(1.0) # Wait for aborting transactions

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

            operations_count = yt.get("//sys/operations/@count")

            self.check_command(
                lambda: yson.loads(yt.driver.make_request(command, params)),
                None,
                lambda: yt.get("//sys/operations/@count") == operations_count + 1,
                abort)

    def test_read_with_retries(self):
        old_value = yt.config["read_retries"]["enable"]
        yt.config["read_retries"]["enable"] = True
        yt.config._ENABLE_READ_TABLE_CHAOS_MONKEY = True
        try:
            table = TEST_DIR + "/table"

            with pytest.raises(yt.YtError):
                yt.read_table(table)

            yt.create_table(table)
            self.check([], list(yt.read_table(table, raw=False)))
            assert "" == yt.read_table(table).read()

            yt.write_table(table, ["x=1\n", "y=2\n"])
            self.check(["x=1\n", "y=2\n"], list(yt.read_table(table)))

            #rsp = yt.read_table(table)
            #assert rsp.next() == "x=1\n"
            #yt.write_table(table, ["x=1\n", "y=3\n"])
            #assert rsp.next() == "y=2\n"
            #rsp.close()

            rsp = yt.read_table(table, raw=False)
            # y != 3 because rsp.close() aborts inner write_table() transaction
            # TODO(asaitgalin): snapshot transaction in read_table() should not be put into transaction stack
            assert [("x", "1"), ("y", "2")] == sorted([x.items()[0] for x in rsp])

            response_parameters = {}
            rsp = yt.read_table(table, response_parameters=response_parameters)
            assert {"start_row_index": 0, "approximate_row_count": 2} == response_parameters
            rsp.close()

        finally:
            yt.config._ENABLE_READ_TABLE_CHAOS_MONKEY = False
            yt.config["read_retries"]["enable"] = old_value

    def test_http_retries(self):
        old_request_retry_timeout = yt.config["proxy"]["request_retry_timeout"]
        yt.config._ENABLE_HTTP_CHAOS_MONKEY = True
        yt.config["proxy"]["request_retry_timeout"] = 1000
        try:
            yt.get("/")
            yt.list("/")
            yt.exists("/")
            yt.exists(TEST_DIR)
            yt.exists(TEST_DIR + "/some_node")
            yt.set(TEST_DIR + "/some_node", {})
            yt.exists(TEST_DIR + "/some_node")
            yt.list(TEST_DIR)
        finally:
            yt.config._ENABLE_HTTP_CHAOS_MONKEY = False
            yt.config["proxy"]["request_retry_timeout"] = old_request_retry_timeout

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

    def test_client(self):
        client = Yt(config=self.config)

        other_client = Yt(config=self.config)
        other_client.config["tabular_data_format"] = yt.JsonFormat()

        assert client.get("/")
        client.create("table", "//tmp/in")
        client.write_table("//tmp/in", ["a=b\n"])
        assert "a=b\n" == client.read_table("//tmp/in", raw=True).read()
        assert client.exists("//tmp/in")
        client.run_map("cat", "//tmp/in", "//tmp/out")
        assert client.exists("//tmp/out")
        with client.Transaction():
            yt.set("//@attr", 10)
            assert yt.exists("//@attr")

        assert other_client.get("/")
        assert '{"a":"b"}\n' == other_client.read_table("//tmp/in", raw=True).read()

    def test_get_user_name(self):
        if yt.config["api_version"] == "v2":
            assert get_user_name("") == None
        else:
            # With disabled authentication in proxy it always return root
            assert get_user_name("") == "root"

        #assert get_user_name("") == None
        #assert get_user_name("12345") == None

        #token = "".join(["a"] * 16)
        #yt.set("//sys/tokens/" + token, "user")
        #assert get_user_name(token) == "user"

    def test_old_config_options(self):
        yt.config.http.PROXY = yt.config.http.PROXY
        yt.config.http.PROXY_SUFFIX = yt.config.http.PROXY_SUFFIX
        yt.config.http.TOKEN = yt.config.http.TOKEN
        yt.config.http.TOKEN_PATH = yt.config.http.TOKEN_PATH
        yt.config.http.USE_TOKEN = yt.config.http.USE_TOKEN
        yt.config.http.ACCEPT_ENCODING = yt.config.http.ACCEPT_ENCODING
        yt.config.http.CONTENT_ENCODING = yt.config.http.CONTENT_ENCODING
        yt.config.http.REQUEST_RETRY_TIMEOUT = yt.config.http.REQUEST_RETRY_TIMEOUT
        yt.config.http.REQUEST_RETRY_COUNT = yt.config.http.REQUEST_RETRY_COUNT
        yt.config.http.REQUEST_BACKOFF = yt.config.http.REQUEST_BACKOFF
        yt.config.http.FORCE_IPV4 = yt.config.http.FORCE_IPV4
        yt.config.http.FORCE_IPV6 = yt.config.http.FORCE_IPV6
        yt.config.http.HEADER_FORMAT = yt.config.http.HEADER_FORMAT

        yt.config.VERSION = yt.config.VERSION
        yt.config.OPERATION_LINK_PATTERN = yt.config.OPERATION_LINK_PATTERN

        yt.config.DRIVER_CONFIG = yt.config.DRIVER_CONFIG
        yt.config.DRIVER_CONFIG_PATH = yt.config.DRIVER_CONFIG_PATH

        yt.config.USE_HOSTS = yt.config.USE_HOSTS
        yt.config.HOSTS = yt.config.HOSTS
        yt.config.HOST_BAN_PERIOD = yt.config.HOST_BAN_PERIOD

        yt.config.ALWAYS_SET_EXECUTABLE_FLAG_TO_FILE = yt.config.ALWAYS_SET_EXECUTABLE_FLAG_TO_FILE
        yt.config.USE_MAPREDUCE_STYLE_DESTINATION_FDS = yt.config.USE_MAPREDUCE_STYLE_DESTINATION_FDS
        yt.config.TREAT_UNEXISTING_AS_EMPTY = yt.config.TREAT_UNEXISTING_AS_EMPTY
        yt.config.DELETE_EMPTY_TABLES = yt.config.DELETE_EMPTY_TABLES
        yt.config.USE_YAMR_SORT_REDUCE_COLUMNS = yt.config.USE_YAMR_SORT_REDUCE_COLUMNS
        yt.config.REPLACE_TABLES_WHILE_COPY_OR_MOVE = yt.config.REPLACE_TABLES_WHILE_COPY_OR_MOVE
        yt.config.CREATE_RECURSIVE = yt.config.CREATE_RECURSIVE
        yt.config.THROW_ON_EMPTY_DST_LIST = yt.config.THROW_ON_EMPTY_DST_LIST
        yt.config.RUN_MAP_REDUCE_IF_SOURCE_IS_NOT_SORTED = yt.config.RUN_MAP_REDUCE_IF_SOURCE_IS_NOT_SORTED
        yt.config.USE_NON_STRICT_UPPER_KEY = yt.config.USE_NON_STRICT_UPPER_KEY
        yt.config.CHECK_INPUT_FULLY_CONSUMED = yt.config.CHECK_INPUT_FULLY_CONSUMED
        yt.config.FORCE_DROP_DST = yt.config.FORCE_DROP_DST

        yt.config.OPERATION_STATE_UPDATE_PERIOD = yt.config.OPERATION_STATE_UPDATE_PERIOD
        yt.config.STDERR_LOGGING_LEVEL = yt.config.STDERR_LOGGING_LEVEL
        yt.config.IGNORE_STDERR_IF_DOWNLOAD_FAILED = yt.config.IGNORE_STDERR_IF_DOWNLOAD_FAILED
        yt.config.READ_BUFFER_SIZE = yt.config.READ_BUFFER_SIZE
        yt.config.MEMORY_LIMIT = yt.config.MEMORY_LIMIT

        yt.config.FILE_STORAGE = yt.config.FILE_STORAGE
        yt.config.TEMP_TABLES_STORAGE = yt.config.TEMP_TABLES_STORAGE
        yt.config.LOCAL_TMP_DIR = yt.config.LOCAL_TMP_DIR
        yt.config.REMOVE_TEMP_FILES = yt.config.REMOVE_TEMP_FILES

        yt.config.KEYBOARD_ABORT = yt.config.KEYBOARD_ABORT

        yt.config.MERGE_INSTEAD_WARNING = yt.config.MERGE_INSTEAD_WARNING
        yt.config.MIN_CHUNK_COUNT_FOR_MERGE_WARNING = yt.config.MIN_CHUNK_COUNT_FOR_MERGE_WARNING
        yt.config.MAX_CHUNK_SIZE_FOR_MERGE_WARNING  = yt.config.MAX_CHUNK_SIZE_FOR_MERGE_WARNING

        yt.config.PREFIX = yt.config.PREFIX

        yt.config.TRANSACTION_TIMEOUT = yt.config.TRANSACTION_TIMEOUT
        yt.config.TRANSACTION_SLEEP_PERIOD = yt.config.TRANSACTION_SLEEP_PERIOD
        yt.config.OPERATION_GET_STATE_RETRY_COUNT = yt.config.OPERATION_GET_STATE_RETRY_COUNT

        yt.config.RETRY_READ = yt.config.RETRY_READ
        yt.config.USE_RETRIES_DURING_WRITE = yt.config.USE_RETRIES_DURING_WRITE
        yt.config.USE_RETRIES_DURING_UPLOAD = yt.config.USE_RETRIES_DURING_UPLOAD

        yt.config.CHUNK_SIZE = yt.config.CHUNK_SIZE

        yt.config.PYTHON_FUNCTION_SEARCH_EXTENSIONS = yt.config.PYTHON_FUNCTION_SEARCH_EXTENSIONS
        yt.config.PYTHON_FUNCTION_MODULE_FILTER = yt.config.PYTHON_FUNCTION_MODULE_FILTER
        yt.config.PYTHON_DO_NOT_USE_PYC = yt.config.PYTHON_DO_NOT_USE_PYC
        yt.config.PYTHON_CREATE_MODULES_ARCHIVE = yt.config.PYTHON_CREATE_MODULES_ARCHIVE

        yt.config.DETACHED = yt.config.DETACHED

        yt.config.format.TABULAR_DATA_FORMAT = yt.config.format.TABULAR_DATA_FORMAT

        yt.config.MEMORY_LIMIT = 1024 * 1024 * 1024
        yt.config.POOL = "pool"
        yt.config.INTERMEDIATE_DATA_ACCOUNT = "tmp"
        # Reset spec options
        yt.config["spec_defaults"] = {}

    def test_special_config_options(self):
        # Special shortcuts (manually backported)
        # MERGE_INSTEAD_WARNING
        yt.config.MERGE_INSTEAD_WARNING = True
        yt.config["auto_merge_output"]["action"] = "none"
        assert not yt.config.MERGE_INSTEAD_WARNING
        yt.config.MERGE_INSTEAD_WARNING = True
        assert yt.config["auto_merge_output"]["action"] == "merge"
        yt.config["auto_merge_output"]["action"] = "log"
        assert not yt.config.MERGE_INSTEAD_WARNING

        env_merge_option = os.environ.get("YT_MERGE_INSTEAD_WARNING", None)
        try:
            os.environ["YT_MERGE_INSTEAD_WARNING"] = "1"
            yt.config._update_from_env()
            assert yt.config["auto_merge_output"]["action"] == "merge"
            os.environ["YT_MERGE_INSTEAD_WARNING"] = "0"
            yt.config._update_from_env()
            assert yt.config["auto_merge_output"]["action"] == "log"
        finally:
            if env_merge_option is not None:
                os.environ["YT_MERGE_INSTEAD_WARNING"] = env_merge_option
            update(yt.config.config, self.config)

    def test_config(self):
        yt.write_table("//tmp/in", ["a=b\n"])

        old_format = yt.config["tabular_data_format"]
        yt.config.update_config({"tabular_data_format": yt.JsonFormat()})

        assert '{"a":"b"}\n' == yt.read_table("//tmp/in", raw=True).read()

        yt.config["tabular_data_format"] = old_format

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


class TestNativeModeV2(NativeModeTester):
    @classmethod
    def setup_class(cls):
        super(TestNativeModeV2, cls).setup_class({"api_version": "v2"})
        yt.config.COMMANDS = None

    @classmethod
    def teardown_class(cls):
        super(TestNativeModeV2, cls).teardown_class()

class TestNativeModeV3(NativeModeTester):
    @classmethod
    def setup_class(cls):
        super(TestNativeModeV3, cls).setup_class({"api_version": "v3", "proxy": {"header_format": "yson"}})
        yt.config.COMMANDS = None

    @classmethod
    def teardown_class(cls):
        super(TestNativeModeV3, cls).teardown_class()

class TestNativeModeBindings(NativeModeTester):
    @classmethod
    def setup_class(cls):
        super(TestNativeModeBindings, cls).setup_class({
            "backend": "native",
            "api_version": "v3"
        })

        # It may be missing in the python job
        import yt_driver_bindings
        yt_driver_bindings.configure_logging(cls.env.driver_logging_config)

    @classmethod
    def teardown_class(cls):
        super(TestNativeModeBindings, cls).teardown_class()

