# -*- coding: utf-8 -*-

from __future__ import print_function

from .conftest import authors
from .helpers import TEST_DIR, set_config_option, inject_http_error

import yt.json_wrapper as json
import yt.yson as yson

try:
    from yt.packages.six import PY3
except ImportError:
    from six import PY3

from yt.common import datetime_to_string, YtResponseError
import yt.wrapper.cli_impl as cli_impl
import yt.wrapper as yt
from yt.wrapper.schema import TableSchema, ColumnSchema

import yt.type_info as typing

from flaky import flaky

import copy
import sys
import time
import pytest
import datetime


@pytest.mark.usefixtures("yt_env_with_rpc")
class TestCypressCommands(object):

    @authors("ignat")
    def test_ypath(self):
        path = yt.TablePath("<append=%false;sort-by=[key]>//my/table")
        assert str(path) == "//my/table"
        assert repr(path).endswith("//my/table")
        assert not path.append
        assert path.attributes == {"append": False, "sort_by": ["key"]}

        path = yt.TablePath("<append=%false;sort-by=[key]>//my/table", append=True, attributes={"sort_by": ["subkey"]})
        assert str(path) == "//my/table"
        assert repr(path).endswith("//my/table")
        assert path.append
        assert path.attributes == {"append": True, "sort_by": ["subkey"]}

        path = yt.TablePath("#123", ranges=[])
        assert str(path) == "#123"
        assert repr(path).endswith("#123")
        assert path.append is None
        assert path.attributes == {"ranges": []}

    @authors("levysotsky")
    def test_table_schema(self):
        schema = TableSchema(unique_keys=True) \
            .add_column(ColumnSchema("a", typing.String, sort_order="ascending")) \
            .add_column("b", typing.Struct["field1": typing.Optional[typing.Yson], "field2": typing.Int8])
        schema.strict = False
        schema.add_column("c", typing.Null)
        path = yt.TablePath(TEST_DIR + "/my/table", schema=schema)
        yt.create("table", path, recursive=True, attributes={"schema": schema})
        schema_from_attr = TableSchema.from_yson_type(yt.get(path + "/@schema"))
        assert schema == schema_from_attr
        wrong_schema = copy.deepcopy(schema)
        wrong_schema.add_column("new", typing.Int64)
        assert wrong_schema != schema
        assert wrong_schema != schema_from_attr

    @pytest.mark.opensource
    @authors("asaitgalin")
    def test_get_set_exists(self):
        assert yt.get("/")
        assert len(yt.list("/")) > 1
        with pytest.raises(yt.YtError):
            yt.get("//none")

        assert yt.exists("/")
        assert yt.exists(TEST_DIR)
        assert not yt.exists(TEST_DIR + "/some_node")

        with pytest.raises(yt.YtError):
            yt.set(TEST_DIR + "/some_node/embedded_node", {}, force=True)
        yt.set(TEST_DIR + "/some_node", {}, force=True)

        assert yt.exists(TEST_DIR + "/some_node")

        yt.set_attribute(TEST_DIR + "/some_node", "attr", 1)
        assert yt.get(TEST_DIR + "/some_node", attributes=["attr", "other_attr"])\
            .attributes == {"attr": 1}

        assert json.loads(yt.get(TEST_DIR, format=yt.format.JsonFormat())) == {"some_node": {}}
        assert json.loads(yt.get(TEST_DIR, format="json")) == {"some_node": {}}

        yt.set_attribute(TEST_DIR + "/some_node", "attribut_na_russkom", u"Привет")
        if PY3:
            assert yt.get(TEST_DIR + "/some_node/@attribut_na_russkom") == u"Привет"
        else:
            assert yt.get(TEST_DIR + "/some_node/@attribut_na_russkom") == u"Привет".encode("utf-8")

        yt.set(TEST_DIR, b'{"other_node": {}}', format="json", force=True)
        assert yt.get(TEST_DIR) == {"other_node": {}}
        assert json.loads(yt.get(TEST_DIR, format="json")) == {"other_node": {}}

    @authors("asaitgalin")
    def test_remove(self):
        for recursive in [False, True]:
            with pytest.raises(yt.YtError):
                yt.remove(TEST_DIR + "/some_node", recursive=recursive)
            yt.remove(TEST_DIR + "/some_node", recursive=recursive, force=True)

        for force in [False, True]:
            yt.set(TEST_DIR + "/some_node", {}, force=True)
            yt.remove(TEST_DIR + "/some_node",
                      recursive=True,
                      force=force)

        yt.set_attribute(TEST_DIR, "my_attr", "my_value")
        assert yt.get_attribute(TEST_DIR, "my_attr") == "my_value"
        yt.remove_attribute(TEST_DIR, "my_attr")
        assert yt.get_attribute(TEST_DIR, "my_attr", None) is None

    @authors("asaitgalin")
    def test_mkdir(self):
        yt.mkdir(TEST_DIR, recursive=True)
        with pytest.raises(yt.YtError):
            yt.mkdir(TEST_DIR)

        with pytest.raises(yt.YtError):
            yt.mkdir(TEST_DIR + "/x/y")
        yt.mkdir(TEST_DIR + "/x")
        yt.mkdir(TEST_DIR + "/x/y/z", recursive=True)

    @authors("ignat", "ostyakov")
    @pytest.mark.parametrize("enable_batch_mode", [False, True])
    def test_search(self, enable_batch_mode):
        yt.mkdir(TEST_DIR + "/dir/other_dir", recursive=True)
        yt.create("table", TEST_DIR + "/dir/table")
        yt.write_file(TEST_DIR + "/file", b"")

        res = [TEST_DIR, TEST_DIR + "/dir", TEST_DIR + "/dir/other_dir", TEST_DIR + "/dir/table", TEST_DIR + "/file"]
        assert list(yt.search(TEST_DIR, enable_batch_mode=enable_batch_mode)) == res
        yt.set_attribute(TEST_DIR + "/dir", "opaque", True)

        assert set(list(yt.search(TEST_DIR, enable_batch_mode=enable_batch_mode))) == set(res)
        assert set(list(yt.search(TEST_DIR, enable_batch_mode=enable_batch_mode, map_node_order=None))) == set(res)
        yt.remove(TEST_DIR + "/dir/@opaque")

        assert list(yt.search(TEST_DIR, depth_bound=1, enable_batch_mode=enable_batch_mode)) == sorted([
            TEST_DIR,
            TEST_DIR + "/dir",
            TEST_DIR + "/file"])
        assert list(yt.search(TEST_DIR, exclude=[TEST_DIR + "/dir"],
                              enable_batch_mode=enable_batch_mode)) == sorted([TEST_DIR, TEST_DIR + "/file"])

        res = yt.search(TEST_DIR, map_node_order=lambda path, object: sorted(object),
                        enable_batch_mode=enable_batch_mode)
        assert list(res) == [TEST_DIR, TEST_DIR + "/dir", TEST_DIR + "/dir/other_dir",
                             TEST_DIR + "/dir/table", TEST_DIR + "/file"]

        assert list(yt.search(TEST_DIR, node_type="file",
                              enable_batch_mode=enable_batch_mode)) == [TEST_DIR + "/file"]

        assert list(yt.search(TEST_DIR, node_type="table", enable_batch_mode=enable_batch_mode,
                              path_filter=lambda x: x.find("dir") != -1)) == [TEST_DIR + "/dir/table"]

        def subtree_filter(path, obj):
            is_in_dir = path.find("dir") != -1
            is_file = obj.attributes["type"] == "file"
            return not is_in_dir and not is_file

        assert list(yt.search(TEST_DIR, subtree_filter=subtree_filter,
                              enable_batch_mode=enable_batch_mode)) == [TEST_DIR]

        # Search empty tables
        res = yt.search(TEST_DIR, attributes=["row_count"], enable_batch_mode=enable_batch_mode,
                        object_filter=lambda x: x.attributes.get("row_count", -1) == 0)
        assert sorted(res) == sorted([yson.to_yson_type(TEST_DIR + "/dir/table",
                                                        {"row_count": 0})])

        # Search in list nodes
        list_node = TEST_DIR + "/list_node"
        yt.set(list_node, ["x"])
        yt.create("table", list_node + "/end")
        yt.create("table", list_node + "/end")
        assert list(yt.search(list_node, enable_batch_mode=enable_batch_mode,
                              node_type="table")) == sorted([list_node + "/1", list_node + "/2"])
        assert list(yt.search(list_node, list_node_order=lambda p, obj: [2, 0, 1],
                              enable_batch_mode=enable_batch_mode)) == \
               [list_node] + ["{0}/{1}".format(list_node, i) for i in [2, 0, 1]]
        assert "//sys/accounts/tmp" in yt.search("//sys", node_type="account", enable_batch_mode=enable_batch_mode)

        yt.mkdir(TEST_DIR + "/dir_with_slash")
        yt.mkdir(TEST_DIR + "/dir_with_slash" + "/dir_\\\\_x")
        yt.set(TEST_DIR + "/dir_with_slash" + "/dir_\\\\_x" + "/@opaque", True)
        yt.mkdir(TEST_DIR + "/dir_with_slash" + "/dir_\\\\_x" + "/inner_dir")
        assert ["dir_\\_x"] == yt.list(TEST_DIR + "/dir_with_slash")
        assert [TEST_DIR + "/dir_with_slash",
                TEST_DIR + "/dir_with_slash" + "/dir_\\\\_x",
                TEST_DIR + "/dir_with_slash" + "/dir_\\\\_x" + "/inner_dir"] \
               == list(yt.search(TEST_DIR + "/dir_with_slash", enable_batch_mode=enable_batch_mode))

        yt.create("map_node", TEST_DIR + "/search_test")
        yt.create("table", TEST_DIR + "/search_test/search_test_table")
        yt.link(TEST_DIR + "/search_test/search_test_table", TEST_DIR + "/search_test/link_to_table")
        yt.remove(TEST_DIR + "/search_test/search_test_table")

        assert list(yt.search(TEST_DIR + "/search_test", follow_links=True)) == \
            [TEST_DIR + "/search_test", TEST_DIR + "/search_test/link_to_table"]
        assert list(yt.search(TEST_DIR + "/search_test", follow_links=False)) == \
            [TEST_DIR + "/search_test", TEST_DIR + "/search_test/link_to_table"]

        yt.mkdir(TEST_DIR + "/search_test/test_dir")
        yt.create("table", TEST_DIR + "/search_test/test_dir/table")
        yt.link(TEST_DIR + "/search_test/test_dir", TEST_DIR + "/search_test/test_dir_link")

        for opaque in (True, False):
            for link_opaque in (True, False):
                print("OPAQUE", opaque, "LINK_OPAQUE", link_opaque, file=sys.stderr)
                yt.set(TEST_DIR + "/search_test/test_dir/@opaque", opaque)
                yt.set(TEST_DIR + "/search_test/test_dir_link&/@opaque", link_opaque)

                correct_result = [
                    TEST_DIR + "/search_test", TEST_DIR + "/search_test/link_to_table",
                    TEST_DIR + "/search_test/test_dir", TEST_DIR + "/search_test/test_dir/table",
                    TEST_DIR + "/search_test/test_dir_link", TEST_DIR + "/search_test/test_dir_link", TEST_DIR + "/search_test/test_dir_link/table"]
                if opaque:
                    correct_result[3:5] = correct_result[4:2:-1]
                assert list(yt.search(TEST_DIR + "/search_test", follow_links=True)) == correct_result

                correct_result = [
                    TEST_DIR + "/search_test", TEST_DIR + "/search_test/link_to_table",
                    TEST_DIR + "/search_test/test_dir", TEST_DIR + "/search_test/test_dir/table",
                    TEST_DIR + "/search_test/test_dir_link"]
                if opaque:
                    correct_result[3:5] = correct_result[4:2:-1]
                assert list(yt.search(TEST_DIR + "/search_test", follow_links=False)) == correct_result

        assert list(yt.search(TEST_DIR + "/search_test/test_dir_link", follow_links=True)) == \
            [TEST_DIR + "/search_test/test_dir_link", TEST_DIR + "/search_test/test_dir_link/table"]
        assert list(yt.search(TEST_DIR + "/search_test/test_dir_link", follow_links=False)) == \
            [TEST_DIR + "/search_test/test_dir_link", TEST_DIR + "/search_test/test_dir_link/table"]

        assert list(yt.search(TEST_DIR + "/search_test/test_dir_link&", follow_links=True)) == \
            [TEST_DIR + "/search_test/test_dir_link&"]
        assert list(yt.search(TEST_DIR + "/search_test/test_dir_link&", follow_links=False)) == \
            [TEST_DIR + "/search_test/test_dir_link&"]

        assert list(yt.search())

        yt.create("map_node", TEST_DIR + "/search_test_with_binary")
        yt.create("map_node", TEST_DIR + "/search_test_with_binary/uhu_\\xf3")
        assert list(yt.search(TEST_DIR + "/search_test_with_binary")) == [
            TEST_DIR + "/search_test_with_binary",
            TEST_DIR + "/search_test_with_binary/uhu_\\xf3",
        ]

    @authors("asaitgalin")
    def test_create(self):
        with pytest.raises(yt.YtError):
            yt.create("map_node", TEST_DIR + "/map", attributes={"type": "table"})

        yt.create("map_node", TEST_DIR + "/dir")
        revision = yt.get(TEST_DIR + "/dir/@revision")

        with pytest.raises(yt.YtError):
            yt.create("map_node", TEST_DIR + "/dir")

        yt.create("map_node", TEST_DIR + "/dir", ignore_existing=True)
        assert revision == yt.get(TEST_DIR + "/dir/@revision")

        yt.create("map_node", TEST_DIR + "/dir", force=True)
        assert revision != yt.get(TEST_DIR + "/dir/@revision")

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

    @authors("asaitgalin")
    def test_attributes_commands(self):
        table = TEST_DIR + "/table_with_attributes"
        yt.write_table(table, [{"x": 1, "y": 1}, {"x": 2, "y": 2}], format="dsv")
        assert yt.row_count(table) == 2
        assert not yt.is_sorted(table)

        assert yt.get_attribute(table, "non_existing", default=0) == 0

        yt.set_attribute(table, "my_attribute", {"000": 10})
        with pytest.raises(yt.YtError):
            yt.get_attribute(table, "my_attribute/000")
        with pytest.raises(yt.YtError):
            yt.set_attribute(table, "my_attribute/000", 20)

        assert yt.get_attribute(table, "user_attribute_keys") == ["my_attribute"]
        assert yt.get(table + "/@my_attribute") == {"000": 10}

        dir_name = TEST_DIR + "/dir"
        yt.create("map_node", dir_name, attributes={"attr": 1})
        yt.set_attribute(dir_name, "second_attr", "str")
        assert yt.has_attribute(dir_name, "second_attr")
        assert yt.get(dir_name, attributes=["attr", "second_attr"]).attributes == \
            {"attr": 1, "second_attr": "str"}

        result = list(yt.search(table, node_type="table", attributes=("my_attribute", )))
        assert len(result) == 1
        assert str(result[0]) == table
        assert result[0].attributes["my_attribute"] == {"000": 10}

    @authors("asaitgalin")
    def test_link(self):
        table = TEST_DIR + "/table_with_attributes"
        link = TEST_DIR + "/table_link"
        yt.create("table", table)
        yt.link(table, link)
        assert not yt.get_attribute(link + "&", "broken")
        assert yt.get_attribute(link + "&", "target_path") == table

        with pytest.raises(yt.YtError):
            yt.link(table, link)
        yt.link(table, link, ignore_existing=True)

        expected = link

        other_link = TEST_DIR + "/other_link"
        yt.link(link, other_link, recursive=False)

        assert yt.get_attribute(other_link + "&", "target_path") == expected
        yt.remove(other_link, force=True)
        yt.link(link, other_link, recursive=True)
        assert yt.get_attribute(other_link + "&", "target_path") == expected

    @authors("asaitgalin")
    def test_list(self):
        tables = ["{0}/{1}".format(TEST_DIR, name) for name in ("a", "b", "c")]
        for table in tables:
            yt.create("table", table)
        assert yt.list(TEST_DIR) == sorted(["a", "b", "c"])
        assert yt.list(TEST_DIR, absolute=True) == \
            sorted(["{0}/{1}".format(TEST_DIR, x) for x in ("a", "b", "c")])
        yt.mkdir(TEST_DIR + "/subdir")
        yt.create("table", TEST_DIR + "/subdir/table")

        result = yt.list(TEST_DIR + "/subdir", attributes=["type"])[0]
        assert str(result) == "table"
        assert result.attributes == {"type": "table"}

        with pytest.raises(yt.YtError):
            yt.list(TEST_DIR + "/subdir", absolute=True, format="json")

    @authors("asaitgalin")
    def test_get_type(self):
        table = TEST_DIR + "/table"
        map_node = TEST_DIR + "/map_node"
        yt.create("table", table)
        yt.create("map_node", map_node)
        assert yt.get_type(table) == "table"
        assert yt.get_type(map_node) == "map_node"

    @authors("asaitgalin")
    def test_simple_copy_move(self):
        table = TEST_DIR + "/table"
        dir = TEST_DIR + "/dir"
        other_table = dir + "/other_table"
        yt.create("table", table)
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

        yt.write_table(table, [{"a": "b"}])
        with pytest.raises(yt.YtError):
            yt.copy(table, other_table)
        yt.copy(table, other_table, ignore_existing=True)
        assert list(yt.read_table(other_table, format=yt.format.DsvFormat())) == []
        with pytest.raises(yt.YtError):
            yt.copy(table, other_table, ignore_existing=True, force=True)
        yt.copy(table, other_table, force=True)
        assert yt.exists(table)
        assert yt.exists(other_table)
        assert list(yt.read_table(other_table, format=yt.format.DsvFormat())) == [{"a": "b"}]

        # Just checking that the parameter exists.
        yt.copy(other_table, TEST_DIR + "/quota_check", pessimistic_quota_check=True)
        yt.move(TEST_DIR + "/quota_check", TEST_DIR + "/no_quota_check", pessimistic_quota_check=False)

        # Remove it after fixes in move
        with pytest.raises(yt.YtError):
            yt.move(table, other_table)

        yt.remove(other_table)
        yt.move(table, other_table)
        assert not yt.exists(table)
        assert yt.exists(other_table)

        yt.copy(other_table, TEST_DIR + "/tmp1", preserve_account=True)
        assert yt.exists(TEST_DIR + "/tmp1")
        yt.move(TEST_DIR + "/tmp1", TEST_DIR + "/tmp2", preserve_account=True)
        assert yt.exists(TEST_DIR + "/tmp2")

        yt.copy(other_table, TEST_DIR + "/d1/d2/table", recursive=True)
        assert yt.exists(other_table)
        assert yt.exists(TEST_DIR + "/d1/d2/table")

        yt.move(TEST_DIR + "/d1/d2/table", TEST_DIR + "/d3/d4/table", recursive=True)
        assert not yt.exists(TEST_DIR + "/d1/d2/table")
        assert yt.exists(TEST_DIR + "/d3/d4/table")

        yt.create("table", TEST_DIR + "/ttt", attributes={"expiration_time": str(datetime.datetime(year=2030, month=1, day=1))})
        yt.copy(TEST_DIR + "/ttt", TEST_DIR + "/ttt_copied", preserve_expiration_time=True, preserve_creation_time=True)

        assert yt.get(TEST_DIR + "/ttt/@expiration_time") == yt.get(TEST_DIR + "/ttt_copied/@expiration_time")
        assert yt.get(TEST_DIR + "/ttt/@creation_time") == yt.get(TEST_DIR + "/ttt_copied/@creation_time")

    @authors("asaitgalin", "ignat")
    @flaky(max_runs=5)
    def test_transactions(self):
        table = TEST_DIR + "/transaction_test_table"

        yt.create("table", table)
        yt.write_table(table, [{"x": 1}])

        def read_table(client=None):
            return list(yt.read_table(table, client=client))

        new_client = yt.client.Yt(token=yt.config["token"], config=yt.config.config)

        with yt.Transaction():
            yt.write_table(table, [{"x": 2}])
            assert read_table(new_client) == [{"x": 1}]

        assert read_table(new_client) == [{"x": 2}]

        with pytest.raises(yt.YtError):
            yt.ping_transaction("incorrect")

        with pytest.raises(yt.YtError):
            yt.ping_transaction("4-3-2-1")

        with pytest.raises(yt.YtError):
            with yt.Transaction(timeout=2000, ping=False):
                yt.write_table(table, [{"x": 3}])
                time.sleep(3)

        assert read_table() == [{"x": 2}]
        assert read_table(new_client) == [{"x": 2}]

        with yt.Transaction(timeout=1000):
            yt.write_table(table, [{"x": 3}])
            time.sleep(3)

        assert read_table() == [{"x": 3}]
        assert read_table(new_client) == [{"x": 3}]

        with yt.Transaction():
            yt.write_table(table, [{"x": 5}])
            time.sleep(3)
            assert read_table(new_client) == [{"x": 3}]

        assert read_table() == [{"x": 5}]
        assert read_table(new_client) == [{"x": 5}]

        try:
            with yt.Transaction(timeout=3000) as tx:
                transaction_id = tx.transaction_id
                raise yt.YtError("test error")
        except yt.YtError:
            pass

        assert not yt.exists("//sys/transactions/" + transaction_id)

        with yt.Transaction() as t:
            with pytest.raises(RuntimeError):
                t.abort()
            with pytest.raises(RuntimeError):
                t.commit()

        tx_id = yt.start_transaction(deadline=datetime.datetime.utcnow() + datetime.timedelta(seconds=5))
        assert yt.exists("#" + tx_id)
        time.sleep(6)
        assert not yt.exists("#" + tx_id)

        tx_id = yt.start_transaction(deadline=datetime_to_string(datetime.datetime.utcnow() + datetime.timedelta(seconds=5)))
        assert yt.exists("#" + tx_id)
        time.sleep(6)
        assert not yt.exists("#" + tx_id)

    @authors("asaitgalin")
    @pytest.mark.skipif("True")  # Enable when st/YT-4182 is done.
    def test_signal_in_transactions(self):
        new_client = yt.YtClient(token=yt.config["token"], config=yt.config.config)

        yt.config["transaction_use_signal_if_ping_failed"] = True
        old_request_timeout = yt.config["proxy"]["request_timeout"]
        yt.config["proxy"]["request_timeout"] = 3000.0
        try:
            caught = False
            try:
                with yt.Transaction() as tx:
                    new_client.abort_transaction(tx.transaction_id)
                    time.sleep(5.0)
            except yt.YtTransactionPingError:
                caught = True

            assert caught

            caught = False
            try:
                with yt.Transaction() as tx1:
                    with yt.Transaction():
                        with yt.Transaction():
                            new_client.abort_transaction(tx1.transaction_id)
                            time.sleep(5.0)
            except yt.YtTransactionPingError:
                caught = True

            assert caught
        finally:
            yt.config["transaction_use_signal_if_ping_failed"] = False
            yt.config["proxy"]["request_timeout"] = old_request_timeout

    @authors("ignat")
    def test_ping_failed_mode_pass(self):
        new_client = yt.YtClient(token=yt.config["token"], config=yt.config.config)

        with set_config_option("ping_failed_mode", "pass"):
            try:
                with yt.Transaction() as tx:
                    new_client.abort_transaction(tx.transaction_id)
                    time.sleep(5.0)
            except yt.YtResponseError as err:
                assert err.is_no_such_transaction()

    @authors("ignat")
    def test_lock(self):
        dir = TEST_DIR + "/dir"

        yt.mkdir(dir)
        assert len(yt.get(dir + "/@locks")) == 0

        with yt.Transaction():
            yt.lock(dir)
            assert len(yt.get(dir + "/@locks")) == 1

        assert len(yt.get(dir + "/@locks")) == 0
        with yt.Transaction():
            yt.lock(dir, waitable=True)
            yt.lock(dir, waitable=True, wait_for=1000)
            assert len(yt.get(dir + "/@locks")) == 2

        tx = yt.start_transaction()
        yt.config.COMMAND_PARAMS["transaction_id"] = tx
        try:
            yt.lock(dir, waitable=True)
            assert len(yt.get(dir + "/@locks")) == 1

            yt.config.COMMAND_PARAMS["transaction_id"] = "0-0-0-0"
            with pytest.raises(yt.YtError):
                with yt.Transaction():
                    yt.lock(dir, waitable=True, wait_for=1000)
        finally:
            yt.config.COMMAND_PARAMS["transaction_id"] = "0-0-0-0"
            yt.abort_transaction(tx)

        tx = yt.start_transaction(timeout=60*1000)
        client = yt.YtClient(config=yt.config.config)
        client.COMMAND_PARAMS["transaction_id"] = tx
        try:
            lock_result = client.lock(dir, waitable=True, wait_for=4000)
            lock_id = lock_result["lock_id"] if yt.config["api_version"] == "v4" else lock_result
            assert lock_id != "0-0-0-0"
            with client.Transaction():
                lock_result = client.lock(dir, waitable=True, wait_for=4000)
                lock_id = lock_result["lock_id"] if yt.config["api_version"] == "v4" else lock_result
                assert lock_id != "0-0-0-0"
        finally:
            client.abort_transaction(tx)

    @authors("ignat")
    def test_shared_key_attribute_locks(self):
        dir = TEST_DIR + "/dir"
        yt.create("map_node", dir)

        tx_id = yt.start_transaction()
        with yt.Transaction(transaction_id=tx_id):
            yt.lock(dir, mode="shared", attribute_key="my_attr")
            yt.set(TEST_DIR + "/@my_attr", 10)

            yt.lock(dir, mode="shared", child_key="child")
            yt.create("table", TEST_DIR + "/child")

        yt.set(TEST_DIR + "/@other_attr", 20)
        with pytest.raises(yt.YtError):
            yt.set(TEST_DIR + "/@my_attr", 30)

        yt.create("table", TEST_DIR + "/other_child")
        with pytest.raises(yt.YtError):
            yt.set(TEST_DIR + "/child", {}, force=True)

        yt.commit_transaction(tx_id)
        yt.set(TEST_DIR + "/@my_attr", 30)
        yt.remove(TEST_DIR + "/child")

    @authors("asaitgalin")
    def test_copy_move_sorted_table(self):
        def is_sorted_by_y(table_path):
            sorted_by = yt.get_attribute(table_path, "sorted_by", None)
            if sorted_by is None:
                sorted_by = yt.get_attribute(table_path, "key_columns", None)
            return sorted_by == ["y"]

        table = TEST_DIR + "/table"
        other_table = TEST_DIR + "/other_table"
        another_table = TEST_DIR + "/another_table"

        yt.write_table(table, [{"x": 1, "y": 2}, {"x": 3, "y": 1}, {"x": 2, "y": 3}])
        yt.run_sort(table, sort_by=["y"])

        yt.copy(table, other_table)
        assert yt.is_sorted(other_table)
        assert is_sorted_by_y(other_table)

        yt.move(table, another_table)
        assert yt.is_sorted(another_table)
        assert is_sorted_by_y(another_table)

    @authors("asaitgalin")
    def test_utf8(self):
        yt.create("table", TEST_DIR + "/table", attributes={"attr": u"капуста"})

    @authors("ignat")
    def test_concatenate(self):
        tableA = TEST_DIR + "/tableA"
        tableB = TEST_DIR + "/tableB"
        output_table = TEST_DIR + "/outputTable"

        yt.write_table(tableA, [{"x": 1, "y": 2}])
        yt.write_table(tableB, [{"x": 10, "y": 20}])
        yt.concatenate([tableA, tableB], output_table)

        assert [{"x": 1, "y": 2}, {"x": 10, "y": 20}] == list(yt.read_table(output_table))

        fileA = TEST_DIR + "/fileA"
        fileB = TEST_DIR + "/fileB"
        output_file = TEST_DIR + "/outputFile"

        yt.write_file(fileA, b"Hello")
        yt.write_file(fileB, b"World")
        yt.concatenate([fileA, fileB], output_file)

        assert b"HelloWorld" == yt.read_file(output_file).read()

        with pytest.raises(yt.YtError):
            yt.concatenate([], tableA)

        with pytest.raises(yt.YtError):
            yt.concatenate([fileA, tableB], output_table)

        with pytest.raises(yt.YtError):
            yt.concatenate([TEST_DIR, tableB], output_table)

    @authors("ostyakov")
    def test_set_recursive(self):
        yt.create("map_node", TEST_DIR + "/node")

        with pytest.raises(yt.YtError):
            yt.set(TEST_DIR + "/node/node2/node3/node4", 1)

        with pytest.raises(yt.YtError):
            yt.set(TEST_DIR + "/node/node2/node3/node4/@attr", 1, recursive=True)

        yt.set(TEST_DIR + "/node/node2/node3/node4", 1, recursive=True)
        assert yt.get(TEST_DIR + "/node/node2/node3/node4") == 1

    @authors("ignat")
    def test_commands_with_json_format(self):
        yt.set(TEST_DIR + "/some_node", {}, force=True)
        assert json.loads(yt.get(TEST_DIR, format=yt.format.JsonFormat())) == {"some_node": {}}
        assert json.loads(yt.get(TEST_DIR, format="json")) == {"some_node": {}}

        yt.set(TEST_DIR, b'{"other_node": {}}', format="json", force=True)
        assert yt.get(TEST_DIR) == {"other_node": {}}
        assert json.loads(yt.get(TEST_DIR, format="json")) == {"other_node": {}}

        tables = ["{0}/{1}".format(TEST_DIR, name) for name in ("a", "b", "c")]
        for table in tables:
            yt.create("table", table)
        with pytest.raises(yt.YtError):
            yt.list(TEST_DIR + "/subdir", absolute=True, format="json")

    @authors("ignat")
    def test_create_with_utf8(self):
        yt.create("table", TEST_DIR + "/table", attributes={"attr": u"капуста"})
        if PY3:
            assert u"капуста" == yt.get(TEST_DIR + "/table/@attr")
        else:
            assert u"капуста".encode("utf-8") == yt.get(TEST_DIR + "/table/@attr")

    @authors("ignat")
    def test_non_utf8_node(self):
        value = {b"\xee": yson.to_yson_type(b"\xff", attributes={"encoding": "custom"})}
        value_bytes = {b"\xee": yson.to_yson_type(b"\xff", attributes={b"encoding": b"custom"})}

        # Map Node case.
        yt.set(TEST_DIR + "/some_node", value, force=True)

        search_result = [
            TEST_DIR,
            TEST_DIR + "/some_node",
            # YPath escapes non-ascii symbols.
            TEST_DIR + "/some_node/\\xee",
        ]
        search_result_bytes = [x.encode("ascii") for x in search_result]

        # JSON applies encode_utf8 decoding on the server side.
        assert yt.get(TEST_DIR + "/some_node", format="json") == b'{"\xc3\xae":"\xc3\xbf"}'

        assert yt.get(TEST_DIR + "/some_node", attributes=["encoding"]) == value
        assert list(yt.search(TEST_DIR)) == search_result

        with set_config_option("structured_data_format", yt.YsonFormat(encoding=None)):
            assert yt.get(TEST_DIR + "/some_node", attributes=["encoding"]) == value_bytes
            assert list(yt.search(TEST_DIR)) == search_result_bytes

        with set_config_option("structured_data_format", yt.YsonFormat(encoding="utf-8")):
            if PY3:
                assert yt.get(TEST_DIR + "/some_node", attributes=["encoding"]) == value
            else:
                with pytest.raises(yt.YtError):
                    yt.get(TEST_DIR + "/some_node")

        with set_config_option("structured_data_format", yt.JsonFormat(encoding=None)):
            assert yt.get(TEST_DIR + "/some_node", attributes=["encoding"]) == value_bytes
            assert list(yt.search(TEST_DIR)) == search_result_bytes

        # Document case.
        yt.create("document", TEST_DIR + "/some_document", attributes={"value": value})
        assert yt.get(TEST_DIR + "/some_node", attributes=["encoding"]) == value

        with set_config_option("structured_data_format", yt.YsonFormat(encoding=None)):
            assert yt.get(TEST_DIR + "/some_node", attributes=["encoding"]) == value_bytes

    @authors("ignat")
    def test_utf8_node(self):
        unicode_symbol = json.loads('"\\u00A9"')
        value = {unicode_symbol: unicode_symbol}
        value_bytes = {unicode_symbol.encode("utf-8"): unicode_symbol.encode("utf-8")}

        search_result = [
            TEST_DIR,
            yt.ypath_join(TEST_DIR, "some_node"),
            yt.ypath_join(TEST_DIR, "some_node", yt.escape_ypath_literal(unicode_symbol, encoding="utf-8")),
        ]

        # Map Node case.
        if PY3:
            yt.set(TEST_DIR + "/some_node", value, force=True)
            assert yt.get(TEST_DIR + "/some_node") == value
        else:
            yt.set(TEST_DIR + "/some_node", value, force=True)
            with set_config_option("structured_data_format", yt.YsonFormat(encoding="utf-8")):
                yt.set(TEST_DIR + "/some_node", value, force=True)
            assert yt.get(TEST_DIR + "/some_node") == value_bytes

        # JSON applies encode_utf8 decoding on the server side.
        assert yt.get(TEST_DIR + "/some_node", format="json") == b'{"\xc3\x82\xc2\xa9":"\xc3\x82\xc2\xa9"}'

        assert search_result == list(yt.search(TEST_DIR))

        # NB: JsonFormat is used since 'Encoding parameter is not supported for Python 2'.
        with set_config_option("structured_data_format", yt.JsonFormat(encoding="utf-8")):
            assert search_result == list(yt.search(TEST_DIR))
            assert yt.get(TEST_DIR + "/some_node") == value
        with set_config_option("structured_data_format", yt.YsonFormat(encoding=None)):
            assert [item.encode("ascii") for item in search_result] == list(yt.search(TEST_DIR))
            assert yt.get(TEST_DIR + "/some_node") == value_bytes

    @authors("ignat")
    def test_set_remove_attribute_recursively(self):
        yt.create("table", yt.ypath_join(TEST_DIR, "table"))
        yt.create("file", yt.ypath_join(TEST_DIR, "file"))
        yt.create("map_node", yt.ypath_join(TEST_DIR, "dir"))
        yt.create("document", yt.ypath_join(TEST_DIR, "dir/document"))
        yt.link(yt.ypath_join(TEST_DIR, "table"), yt.ypath_join(TEST_DIR, "dir/link"))

        paths = [
            yt.ypath_join(TEST_DIR, "table"),
            yt.ypath_join(TEST_DIR, "file"),
            yt.ypath_join(TEST_DIR, "dir"),
            yt.ypath_join(TEST_DIR, "dir/document"),
            yt.ypath_join(TEST_DIR, "dir/link")
        ]

        cli_impl._set_attribute(TEST_DIR, "my_attr", "my_value", recursive=True)
        for path in paths:
            assert yt.get(path + "&/@my_attr") == "my_value"

        cli_impl._set_attribute(yt.ypath_join(TEST_DIR, "dir"), "my_attr", {"key": 10}, recursive=True)
        for path in paths[:2]:
            assert yt.get(path + "&/@my_attr") == "my_value"
        for path in paths[2:]:
            assert yt.get(path + "&/@my_attr") == {"key": 10}

        cli_impl._remove_attribute(TEST_DIR, "my_attr", recursive=True)
        for path in paths:
            assert not yt.exists(path + "&/@my_attr")

    @authors("aleexfi")
    def test_get_table_schema(self):
        expected_schema = (
            TableSchema(strict=True)
            .add_column("first", typing.String, sort_order="ascending")
            .add_column("second", typing.Bool)
        )
        path = yt.ypath.ypath_join(TEST_DIR, "/table")
        yt.create("table", path, recursive=True, attributes={"schema": expected_schema})
        retrieved_schema = yt.get_table_schema(path)
        assert expected_schema == retrieved_schema


@pytest.mark.usefixtures("test_environment_multicell")
class TestCypressCommandsMulticell(object):

    PORTAL1_ENTRANCE = '//tmp/some_portal'
    TMP_DIR = '//tmp/tmp_dir'
    PORTAL1_CELL_ID = 2

    def setup(self):
        assert self._create_portal(self.PORTAL1_ENTRANCE, self.PORTAL1_CELL_ID)
        yt.create("map_node", self.TMP_DIR)

    def teardown(self):
        for t in yt.list(self.PORTAL1_ENTRANCE):
            yt.remove(self.PORTAL1_ENTRANCE + "/" + t, force=True, recursive=True)
        yt.remove(self.TMP_DIR, force=True, recursive=True)

    def _create_portal(self, path_in, portal_cell_tag_id):
        if yt.exists(path_in):
            return True
        entrance_id = yt.create("portal_entrance", path_in, attributes={"exit_cell_tag": portal_cell_tag_id})
        assert yt.get(path_in + "&/@type") == "portal_entrance"
        assert yt.get(path_in + "&/@path") == path_in

        assert yt.exists("//sys/portal_entrances/{}".format(entrance_id))

        exit_id = yt.get(path_in + "&/@exit_node_id")
        assert yt.get("#{}/@type".format(exit_id)) == "portal_exit"
        assert yt.get("#{}/@entrance_node_id".format(exit_id)) == entrance_id
        assert yt.get("#{}/@inherit_acl".format(exit_id))
        assert yt.get("#{}/@path".format(exit_id)) == path_in

        assert yt.exists("//sys/portal_exits/{}".format(exit_id))

        return True

    @authors("denvr")
    def test_portal_copy_retries(self):
        table_beyond_portal = self.PORTAL1_ENTRANCE + "/dark_table"
        table_beyond_portal_2 = self.PORTAL1_ENTRANCE + "/darkest_table"
        table_no_portal = self.TMP_DIR + "/regula_table"
        yt.create("table", table_beyond_portal)
        yt.write_table(table_beyond_portal, [{"foo": "bar"}, {"foo": "qwe"}])
        assert len(list(yt.read_table(table_beyond_portal))) == 2
        yt.copy(table_beyond_portal, table_no_portal)
        assert len(list(yt.read_table(table_no_portal))) == 2
        yt.remove(table_no_portal)

        with pytest.raises(YtResponseError) as ex:
            yt.copy(table_beyond_portal, table_no_portal, enable_cross_cell_copying=False)
        assert ex.value.is_prohibited_cross_cell_copy()
        assert not yt.exists(table_no_portal)

        yt.copy(table_beyond_portal, table_no_portal, enable_cross_cell_copying=True)
        assert len(list(yt.read_table(table_no_portal))) == 2
        yt.remove(table_no_portal)

        client = yt.YtClient(config=yt.config.config)

        # copy
        client.copy(table_beyond_portal, table_beyond_portal_2)
        assert client.exists(table_beyond_portal)
        assert client.exists(table_beyond_portal_2)
        client.remove(table_beyond_portal_2)

        # copy with retry at start
        client.config["proxy"]["retries"]["count"] = 3  # even than "interrupt_every"
        with inject_http_error(client, filter_url="api/v4/start_transaction", interrupt_every=0) as transaction_calls_cnt:
            with inject_http_error(client, filter_url="api/v4/copy", interrupt_from=0, interrupt_till=3, interrupt_every=2, raise_connection_reset=True) as copy_calls_cnt:
                # 1 attempt (enable_cross_cell_copying=False) - fail by test
                # 2 retry in make_formatted_request (enable_cross_cell_copying=False) - fail by server
                # 3 switch to transaction mode with enable_cross_cell_copying=True - ok
                client.copy(table_beyond_portal, table_no_portal)
        assert transaction_calls_cnt.filtered_total_calls == 1
        assert copy_calls_cnt.filtered_total_calls == 3
        assert copy_calls_cnt.filtered_raises == 1
        assert len(list(client.read_table(table_no_portal))) == 2
        client.remove(table_no_portal)

        # copy with retry inside transaction
        client.config["proxy"]["retries"]["count"] = 3  # even than "interrupt_every"
        with inject_http_error(client, filter_url="api/v4/start_transaction", interrupt_every=0) as transaction_calls_cnt:
            with inject_http_error(client, filter_url="api/v4/copy", interrupt_from=1, interrupt_till=3, interrupt_every=2, raise_connection_reset=True) as copy_calls_cnt:
                # 1 attempt with enable_cross_cell_copying=False - fail by server
                # 2 switch to transaction mode with enable_cross_cell_copying=True - fail by test
                # 3 retry with new transaction switch to transaction mode with enable_cross_cell_copying=True - ok
                client.copy(table_beyond_portal, table_no_portal)
        assert transaction_calls_cnt.filtered_total_calls == 2
        assert copy_calls_cnt.filtered_total_calls == 3
        assert copy_calls_cnt.filtered_raises == 1
        assert len(list(client.read_table(table_no_portal))) == 2
        client.remove(table_no_portal)

        # move
        client.move(table_beyond_portal, table_beyond_portal_2)
        assert not client.exists(table_beyond_portal)
        assert client.exists(table_beyond_portal_2)
        client.move(table_beyond_portal_2, table_beyond_portal)

        client.config["proxy"]["retries"]["count"] = 3  # even than "interrupt_every"
        with inject_http_error(client, filter_url="api/v4/move", interrupt_from=0, interrupt_till=3, interrupt_every=2, raise_connection_reset=True) as calls_cnt:
            client.move(table_beyond_portal, table_no_portal)
        assert not client.exists(table_beyond_portal)
        assert calls_cnt.filtered_total_calls == 3
        assert calls_cnt.filtered_raises == 1
        assert len(list(client.read_table(table_no_portal))) == 2

    @authors("denvr")
    def test_portal_copy_flags(self):
        table_beyond_portal = self.PORTAL1_ENTRANCE + "/dark_table_2"
        yt.create("table", table_beyond_portal)
        yt.write_table(table_beyond_portal, [{"foo": "bar"}, {"foo": "qwe"}])

        table_no_portal = self.TMP_DIR + "/regula_table_2"
        yt.create("table", table_no_portal)
        assert yt.row_count(table_beyond_portal) == 2
        assert yt.row_count(table_no_portal) == 0

        with pytest.raises(YtResponseError) as ex:
            yt.copy(table_beyond_portal, table_no_portal)
        assert ex.value.contains_text("already exists")
        assert yt.row_count(table_no_portal) == 0

        yt.copy(table_beyond_portal, table_no_portal, force=True)
        assert yt.row_count(table_beyond_portal) == 2
        assert yt.row_count(table_no_portal) == 2

    @authors("denvr")
    def test_portal_copy_batch(self):
        table_beyond_portal = self.PORTAL1_ENTRANCE + "/dark_table_3"
        yt.create("table", table_beyond_portal)
        yt.write_table(table_beyond_portal, [{"foo": "bar"}, {"foo": "qwe"}])

        table_no_portal = self.TMP_DIR + "/regula_table_3"

        client = yt.YtClient(config=yt.config.config)
        batch_client = client.create_batch_client()
        res = batch_client.copy(table_beyond_portal, table_no_portal)
        batch_client.commit_batch()
        assert res.get_error() is None
        assert res.is_ok()

        assert yt.row_count(table_beyond_portal) == 2
        assert yt.row_count(table_no_portal) == 2
