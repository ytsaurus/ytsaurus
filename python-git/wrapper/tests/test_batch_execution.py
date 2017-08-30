from .helpers import TEST_DIR

from yt.wrapper.common import parse_bool
from yt.wrapper.batch_helpers import create_batch_client
from yt.wrapper.batch_response import apply_function_to_result
from yt.wrapper.batch_execution import YtBatchRequestFailedError

import yt.wrapper as yt

from yt.packages.six.moves import xrange

import time
import pytest

@pytest.mark.usefixtures("yt_env")
class TestBatchExecution(object):
    def setup(self):
        client = create_batch_client()
        client.create("user", attributes={"name": "batch_tester"})
        client.create("account", attributes={"name": "batch_tester"})
        client.create("group", attributes={"name": "batch_testers"})
        client.create("group", attributes={"name": "batch_super_testers"})
        client.commit_batch()

    def teardown(self):
        client = create_batch_client()
        client.remove("//sys/users/batch_tester", force=True)
        client.remove("//sys/groups/batch_testers", force=True)
        client.remove("//sys/accounts/batch_tester", force=True)
        client.remove("//sys/groups/batch_super_testers", force=True)
        client.commit_batch()

    def test_unsupported_commands(self):
        with pytest.raises(AttributeError):
            client = create_batch_client()
            client.write_table(TEST_DIR + "/batch_node/table", [{"a": 1}])
            client.commit_batch()

        with pytest.raises(Exception):
            client = create_batch_client()
            batch_node = client.get(TEST_DIR + "/batch_node")
            client.set(TEST_DIR + "/batch_node2", batch_node)
            client.commit_batch()

    def test_cypress_commands(self):
        table = TEST_DIR + "/batch_node/table"
        other_table = TEST_DIR + "/batch_node/other_table"
        map_node = TEST_DIR + "/batch_node/map_node"
        link = TEST_DIR + "/batch_node/table_link"

        client = create_batch_client()
        client.mkdir(TEST_DIR + "/batch", recursive=True)
        client.set(TEST_DIR + "/batch_node", {"attr": 1})
        client.set(TEST_DIR + "/batch_node2", {})
        client.commit_batch()

        mkdir_response = client.mkdir(TEST_DIR + "/batch")
        batch_node = client.get(TEST_DIR + "/batch_node")
        exists_result = client.exists(TEST_DIR + "/batch")
        client.commit_batch()

        assert batch_node.get_result() == {"attr": 1}
        assert mkdir_response.get_result() is None
        assert not mkdir_response.is_ok()
        assert exists_result.get_result()

        client.set_attribute(TEST_DIR + "/batch_node2", "attr", 1)
        client.remove(TEST_DIR + "/batch")
        client.create("file", TEST_DIR + "/test_file", recursive=True)
        client.create_table(table)
        client.commit_batch()

        node = client.get(TEST_DIR + "/batch_node2", attributes=["attr", "other_attr"])
        copy_result = client.copy(table, table)
        client.copy(table, other_table)
        client.commit_batch()

        assert node.get_result().attributes == {"attr": 1}
        assert not copy_result.is_ok()
        assert yt.exists(table)
        assert yt.exists(other_table)

        yt.remove(other_table)

        client.move(table, other_table)
        client.commit_batch()

        assert not yt.exists(table)
        assert yt.exists(other_table)

        yt.create_table(table)

        client.link(table, link)
        client.create("map_node", map_node)
        client.commit_batch()

        assert not parse_bool(yt.get_attribute(link + "&", "broken"))
        assert yt.get_attribute(link + "&", "target_path") == table

        table_type = client.get_type(table)
        map_node_type = client.get_type(map_node)
        client.commit_batch()

        assert table_type.get_result() == "table"
        assert map_node_type.get_result() == "map_node"

        tables = ["{0}/table_{1}".format(TEST_DIR + "/batch_node_list", str(i)) for i in xrange(10)]
        for table in tables:
            client.create_table(table, recursive=True)
        client.commit_batch()

        list_result = client.list(TEST_DIR + "/batch_node_list")
        client.commit_batch()

        assert set(list_result.get_result()) == set(("table_" + str(i) for i in xrange(10)))

    def test_acl_commands(self):
        client = create_batch_client()
        permissions_read = client.check_permission("batch_tester", "read", "//sys")
        permissions_write = client.check_permission("batch_tester", "write", "//sys")
        client.commit_batch()
        assert permissions_read.get_result()["action"] == "allow"
        assert permissions_write.get_result()["action"] == "deny"

        client.add_member("batch_tester", "batch_testers")
        client.commit_batch()

        assert yt.get_attribute("//sys/groups/batch_testers", "members") == ["batch_tester"]

        client.remove_member("batch_tester", "batch_testers")
        client.commit_batch()

        assert yt.get_attribute("//sys/groups/batch_testers", "members") == []

    def test_table_commands(self, yt_env):
        table = TEST_DIR + "/batch_node/test_table"

        client = create_batch_client()

        client.create_table(table, recursive=True)
        client.commit_batch()

        assert yt.exists(table)

        row_count = client.row_count(table)
        is_sorted = client.is_sorted(table)
        is_empty = client.is_empty(table)
        client.commit_batch()

        assert row_count.get_result() == 0
        assert is_empty.get_result()
        assert not is_sorted.get_result()

        yt.remove(table)
        if yt_env.version < "0.18":
            yt.create_table(table, attributes={"dynamic": True})
            yt.set(table + "/@schema", [{"name": name, "type": "string"} for name in ["x", "y"]])
            yt.set(table + "/@key_columns", ["x"])
        else:
            yt.create_table(table, attributes={
                "dynamic": True,
                "schema": [
                    {"name": "x", "type": "string", "sort_order": "ascending"},
                    {"name": "y", "type": "string"}
                ]})
        tablet_id = yt.create("tablet_cell", attributes={"size": 1})
        while yt.get("//sys/tablet_cells/{0}/@health".format(tablet_id)) != "good":
            time.sleep(0.1)

        client.mount_table(table)
        client.commit_batch()

        while yt.get("{0}/@tablets/0/state".format(table)) != "mounted":
            time.sleep(0.1)

        with pytest.raises(yt.YtError):
            client.unmount_table(table, sync=True)

        client.unmount_table(table)
        client.commit_batch()

        while yt.get("{0}/@tablets/0/state".format(table)) != "unmounted":
            time.sleep(0.1)

    def test_transactions(self):
        table = TEST_DIR + "/batch_node/test_transaction_table"
        new_client = yt.YtClient(token=yt.config["token"], config=yt.config)

        with yt.Transaction():
            client = create_batch_client()
            client.create_table(table, recursive=True)
            client.commit_batch()

            assert not new_client.exists(table)

    def test_commit(self):
        client = create_batch_client()
        client.mkdir(TEST_DIR + "/batch_commit", recursive=True)
        client.commit_batch()
        exist_result = client.exists(TEST_DIR + "/batch_commit")
        client.commit_batch()
        client.commit_batch()
        assert exist_result.get_result()

    def test_raise_errors(self):
        yt.mkdir(TEST_DIR + "/raise_error", recursive=True)
        with pytest.raises(YtBatchRequestFailedError):
            client = create_batch_client(raise_errors=True)
            client.mkdir(TEST_DIR + "/raise_error")
            client.mkdir(TEST_DIR + "/raise_error/dir/dir")
            client.commit_batch()

    def test_retries(self):
        yt.config._ENABLE_HEAVY_REQUEST_CHAOS_MONKEY = True
        try:
            client = create_batch_client()
            client.mkdir(TEST_DIR + "/batch_retries", recursive=True)
            client.commit_batch()
            assert yt.exists(TEST_DIR + "/batch_retries")

            client = create_batch_client()
            client.remove(TEST_DIR + "/batch_retries")
            client.commit_batch()
            assert not yt.exists(TEST_DIR + "/batch_retries")
        finally:
            yt.config._ENABLE_HEAVY_REQUEST_CHAOS_MONKEY = False

    def test_batch_response(self):
        table = TEST_DIR + "/test_batch_response_table"

        client = create_batch_client()
        create_result = client.create("table", table, recursive=True)

        with pytest.raises(yt.YtError):
            create_result.get_result()

        with pytest.raises(yt.YtError):
            create_result.get_error()

        with pytest.raises(yt.YtError):
            create_result.is_ok()

        result = []
        def test_result_function(output, error):
            assert error is None
            result.append(output)
            return output, error

        apply_function_to_result(test_result_function, create_result, include_error=True)
        client.commit_batch()

        create_result.get_result()
        assert result[0] is not None
