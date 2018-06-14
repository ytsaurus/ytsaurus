from .helpers import TEST_DIR, set_config_option

from yt.wrapper.table import TablePath
import yt.wrapper.http_helpers as http

from yt.packages.six.moves import xrange

import yt.wrapper as yt

import pytest
import time
import tempfile
from copy import deepcopy

@pytest.mark.usefixtures("yt_env")
class TestClient(object):
    def setup(self):
        yt.create("user", attributes={"name": "tester"})
        yt.create("group", attributes={"name": "testers"})

    def teardown(self):
        yt.remove("//sys/users/tester", force=True)
        yt.remove("//sys/groups/testers", force=True)

    def test_client(self, yt_env):
        client = yt.YtClient(config=yt.config.config)

        other_client = yt.YtClient(config=yt.config.config)
        other_client.config["proxy"]["force_ipv4"] = True
        other_client.config["tabular_data_format"] = yt.JsonFormat()

        deepcopy(client)

        with set_config_option("proxy/url", None):
            if yt.config["backend"] != "native":
                assert client.get_user_name("") == "root"

            client.set(TEST_DIR + "/node", "abc")
            assert client.get(TEST_DIR + "/node") == "abc"
            with pytest.raises(yt.YtError):
                client.get_attribute(TEST_DIR + "/node", "missing_attribute")

            assert client.list(TEST_DIR) == ["node"]

            client.remove(TEST_DIR + "/node")
            assert not client.exists(TEST_DIR + "/node")

            client.mkdir(TEST_DIR + "/folder")
            assert client.get_type(TEST_DIR + "/folder") == "map_node"

            table = TEST_DIR + "/table"
            client.create("table", table)
            client.write_table(table, [{"a": "b"}])
            assert b"a=b\n" == client.read_table(table, format=yt.DsvFormat(), raw=True).read()

            assert set(client.search(TEST_DIR)) == {TEST_DIR, TEST_DIR + "/folder", table}

            other_table = TEST_DIR + "/other_table"
            client.copy(table, other_table)
            assert b"a=b\n" == client.read_table(other_table, format=yt.DsvFormat(), raw=True).read()
            client.move(table, TEST_DIR + "/moved_table")
            assert b"a=b\n" == client.read_table(TEST_DIR + "/moved_table", format=yt.DsvFormat(), raw=True).read()
            assert not client.exists(table)

            client.link(other_table, TEST_DIR + "/table_link")
            assert client.get_attribute(TEST_DIR + "/table_link&", "target_path") == \
                   client.get_attribute(other_table, "path")
            assert client.has_attribute(TEST_DIR + "/table_link&", "broken")

            client.set_attribute(other_table, "test_attr", "value")
            for attribute in ["id", "test_attr"]:
                assert attribute in client.list_attributes(other_table)

            assert not client.exists(client.find_free_subpath(TEST_DIR))

            assert client.check_permission("tester", "write", "//sys")["action"] == "deny"

            client.add_member("tester", "testers")
            assert client.get_attribute("//sys/groups/testers", "members") == ["tester"]
            client.remove_member("tester", "testers")
            assert client.get_attribute("//sys/groups/testers", "members") == []

            client.create("table", TEST_DIR + "/table")
            assert client.exists(TEST_DIR + "/table")

            temp_table = client.create_temp_table(TEST_DIR)
            assert client.get_type(temp_table) == "table"
            assert client.is_empty(temp_table)

            client.write_table(temp_table, [{"a": i} for i in xrange(10)])
            client.run_sort(temp_table, sort_by=["x"])
            assert client.is_sorted(temp_table)

            client.run_erase(TablePath(temp_table, start_index=0, end_index=5, client=client))
            assert client.row_count(temp_table) == 5

            client.run_map("cat", other_table, TEST_DIR + "/map_output", format=yt.DsvFormat())
            assert b"a=b\n" == client.read_table(other_table, format=yt.DsvFormat(), raw=True).read()

            client.write_table(TEST_DIR + "/first", [{"x": 1}])
            client.write_table(TEST_DIR + "/second", [{"x": 2}])
            client.run_merge([TEST_DIR + "/first", TEST_DIR + "/second"], TEST_DIR + "/merged_table")
            assert list(client.read_table(TEST_DIR + "/merged_table")) == [{"x": 1}, {"x": 2}]

            client.run_reduce("head -n 3", temp_table, TEST_DIR + "/reduce_output", reduce_by=["x"], format=yt.DsvFormat())
            assert client.row_count(TEST_DIR + "/reduce_output") == 3

            client.write_table("<sorted_by=[x]>" + TEST_DIR + "/first", [{"x": 1}, {"x": 2}])
            client.write_table("<sorted_by=[x]>" + TEST_DIR + "/second", [{"x": 2}, {"x": 3}])
            client.run_join_reduce("cat", [TEST_DIR + "/first", "<foreign=true>" + TEST_DIR + "/second"],
                TEST_DIR + "/join_output", join_by=["x"], format=yt.DsvFormat())
            assert client.row_count(TEST_DIR + "/join_output") == 3

            mr_operation = client.run_map_reduce("cat", "head -n 3", temp_table, TEST_DIR + "/mapreduce_output",
                                                 reduce_by=["x"], format=yt.DsvFormat())
            assert client.get_operation_state(mr_operation.id) == "completed"
            assert client.row_count(TEST_DIR + "/mapreduce_output") == 3

            with client.Transaction():
                client.set("//@attr", 10)
                assert client.exists("//@attr")

            tx = client.start_transaction(timeout=5000)
            with client.PingTransaction(tx, delay=1):
                assert client.exists("//sys/transactions/{0}".format(tx))
                client.COMMAND_PARAMS["transaction_id"] = tx
                assert client.lock(table) != "0-0-0-0"
                client.COMMAND_PARAMS["transaction_id"] = "0-0-0-0"

            client.ping_transaction(tx)
            client.abort_transaction(tx)
            with pytest.raises(yt.YtError):
                client.commit_transaction(tx)

            op = client.run_map("sleep 10; cat", temp_table, table, sync=False, format=yt.DsvFormat())
            assert not client.get_operation_state(op.id).is_unsuccessfully_finished()
            assert op.get_attributes()["state"] != "failed"
            time.sleep(0.5)
            client.suspend_operation(op.id)
            time.sleep(2.5)
            client.resume_operation(op.id)
            time.sleep(2.5)
            client.abort_operation(op.id)
            # Second abort on aborted operation should be silent
            client.abort_operation(op.id)
            assert op.get_progress()["total"] != 0
            assert op.get_stderrs() == []

            client.write_file(TEST_DIR + "/file", b"0" * 1000)
            assert client.read_file(TEST_DIR + "/file").read() == b"0" * 1000
            with pytest.raises(yt.YtError):
                client.smart_upload_file("/unexisting")

            assert other_client.get("/")
            assert b'{"a":"b"}\n' == other_client.read_table(other_table, raw=True).read()

            with client.TempTable(TEST_DIR) as table:
                assert client.exists(table)
            assert not client.exists(table)

    def test_default_api_version(self):
        if yt.config["backend"] != "native":
            client = yt.YtClient(proxy=yt.config["proxy"]["url"])
            client.get("/")
            assert client._api_version == "v3"

    def test_client_with_unknown_api_version(self):
        client = yt.YtClient(config=yt.config.config)
        client.config["api_version"] = None
        client.config["default_api_version_for_http"] = None
        if client.config["backend"] == "native":
            pytest.skip()
        client.get("/")
        assert client._api_version == "v3"

    def test_get_user_name(self):
        if yt.config["backend"] != "native":
            # With disabled authentication in proxy it always return root
            assert yt.get_user_name("") == "root"

        #assert get_user_name("") == None
        #assert get_user_name("12345") == None

        #token = "".join(["a"] * 16)
        #yt.set("//sys/tokens/" + token, "user")
        #assert get_user_name(token) == "user"

    def test_get_token(self):
        client = yt.YtClient(token="a" * 32)
        client.config["enable_token"] = True
        client.config["cache_token"] = False

        assert http.get_token(client=client) == "a" * 32

        _, filename = tempfile.mkstemp()
        with open(filename, "w") as fout:
            fout.write("b" * 32)
        client.config["token"] = None
        client.config["token_path"] = filename
        assert http.get_token(client=client) == "b" * 32

        assert http.get_token(client=yt.YtClient(config={"token": "abacaba"})) == "abacaba"
        with pytest.raises(yt.YtTokenError):
            http.get_token(client=yt.YtClient(config={"token": "\x01\x02"}))

        assert http.get_token(client=yt.YtClient(config={"token": ""})) == None
