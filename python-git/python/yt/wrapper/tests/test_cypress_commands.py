# -*- coding: utf-8 -*-

from yt.wrapper.client import Yt
from yt.wrapper.common import parse_bool
import yt.packages.simplejson as json
import yt.yson as yson
import yt.wrapper as yt

from helpers import TEST_DIR

import time
import pytest

@pytest.mark.usefixtures("yt_env")
class TestCypressCommands(object):
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

        res = set([TEST_DIR, TEST_DIR + "/dir",
                   TEST_DIR + "/dir/other_dir",
                   TEST_DIR + "/dir/table",
                   TEST_DIR + "/file"])
        assert set(yt.search(TEST_DIR)) == res
        yt.set_attribute(TEST_DIR + "/dir", "opaque", True)
        assert set(yt.search(TEST_DIR)) == res
        yt.remove(TEST_DIR + "/dir/@opaque")

        assert set(yt.search(TEST_DIR, depth_bound=1)) == set([TEST_DIR, TEST_DIR + "/dir",
                                                               TEST_DIR + "/file"])
        assert set(yt.search(TEST_DIR, exclude=[TEST_DIR + "/dir"])) == set([TEST_DIR, TEST_DIR + "/file"])

        res = yt.search(TEST_DIR, map_node_order=lambda path, object: sorted(object))
        assert list(res) == [TEST_DIR, TEST_DIR + "/dir", TEST_DIR + "/dir/other_dir",
                             TEST_DIR + "/dir/table", TEST_DIR + "/file"]

        assert set(yt.search(TEST_DIR, node_type="file")) == set([TEST_DIR + "/file"])

        assert set(yt.search(TEST_DIR, node_type="table",
                             path_filter=lambda x: x.find("dir") != -1)) == set([TEST_DIR + "/dir/table"])

        def subtree_filter(path, obj):
            is_in_dir = path.find("dir") != -1
            is_file = obj.attributes["type"] == "file"
            return not is_in_dir and not is_file

        assert list(yt.search(TEST_DIR, subtree_filter=subtree_filter)) == [TEST_DIR]

        # Search empty tables
        res = yt.search(TEST_DIR, attributes=["row_count"],
                        object_filter=lambda x: x.attributes.get("row_count", -1) == 0)
        assert sorted(list(res)) == sorted([yson.to_yson_type(TEST_DIR + "/dir/table",
                                                              {"row_count": 0})])

        # Search in list nodes
        list_node = TEST_DIR + "/list_node"
        yt.set(list_node, ["x"])
        yt.create_table(list_node + "/end")
        yt.create_table(list_node + "/end")
        assert set(yt.search(list_node, node_type="table")) == set([list_node + "/1", list_node + "/2"])
        assert list(yt.search(list_node, list_node_order=lambda p, obj: [2, 0, 1])) == \
               [list_node] + ["{0}/{1}".format(list_node, i) for i in [2, 0, 1]]
        assert "//sys/accounts/tmp" in yt.search("//sys", node_type="account")

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
        yt.write_table(table, ["x=1\ty=1\n", "x=2\ty=2\n"], format="dsv")
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
        yt.create_table(table)
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
            yt.copy([], table)
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

        yt.copy(other_table, TEST_DIR + "/tmp1", preserve_account=True)
        assert yt.exists(TEST_DIR + "/tmp1")
        yt.move(TEST_DIR + "/tmp1", TEST_DIR + "/tmp2", preserve_account=True)
        assert yt.exists(TEST_DIR + "/tmp2")

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
        try:
            yt.lock(dir, waitable=True)
            #with pytest.raises(yt.YtError):
            #    yt.lock(dir, waitable=True)
            assert yt.lock(dir, waitable=True) == "0-0-0-0"

            yt.config.TRANSACTION = "0-0-0-0"
            with pytest.raises(yt.YtError):
                with yt.Transaction():
                    yt.lock(dir, waitable=True, wait_for=1000)
        finally:
            yt.config.TRANSACTION = "0-0-0-0"
            yt.abort_transaction(tx)

        tx = yt.start_transaction(timeout=2000)
        yt.config.TRANSACTION = tx
        client = Yt(config=yt.config.config)
        try:
            assert yt.lock(dir) != "0-0-0-0"
            with client.Transaction():
                assert client.lock(dir, waitable=True, wait_for=3000) != "0-0-0-0"
        finally:
            yt.config.TRANSACTION = "0-0-0-0"
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

        yt.write_table(table, ["x=1\ty=2\n", "x=3\ty=1\n", "x=2\ty=3\n"],
                       format="dsv")
        yt.run_sort(table, sort_by=["y"])

        yt.copy(table, other_table)
        assert yt.is_sorted(other_table)
        assert is_sorted_by_y(other_table)

        yt.move(table, another_table)
        assert yt.is_sorted(another_table)
        assert is_sorted_by_y(another_table)

    def test_utf8(self):
        yt.create("table", TEST_DIR + "/table", attributes={"attr": u"капуста"})

