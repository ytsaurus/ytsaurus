from .conftest import authors
from .helpers import TEST_DIR, set_config_option

from yt.common import YtError

import yt.wrapper.http_helpers as http

try:
    from yt.packages.six.moves import xrange
    from yt.packages.six.moves.urllib.parse import urlparse
except ImportError:
    from six.moves import xrange
    from six.moves.urllib.parse import urlparse

import yt.type_info as type_info
import yt.wrapper as yt

import pytest
import time
import tempfile
import requests_mock
from copy import deepcopy


@pytest.mark.usefixtures("yt_env")
class TestClient(object):
    def setup(self):
        yt.create("user", attributes={"name": "tester"})
        yt.create("group", attributes={"name": "testers"})

    def teardown(self):
        yt.remove("//sys/users/tester", force=True)
        yt.remove("//sys/groups/testers", force=True)

    @authors("asaitgalin")
    def test_client(self, yt_env):
        client = yt.YtClient(config=yt.config.config)

        other_client = yt.YtClient(config=yt.config.config)
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

            client.run_erase(yt.TablePath(temp_table, start_index=0, end_index=5, client=client))
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
            client.run_join_reduce(
                "cat",
                [TEST_DIR + "/first", "<foreign=true>" + TEST_DIR + "/second"],
                TEST_DIR + "/join_output",
                join_by=["x"],
                format=yt.DsvFormat())
            assert client.row_count(TEST_DIR + "/join_output") == 3

            mr_operation = client.run_map_reduce("cat", "head -n 3", temp_table, TEST_DIR + "/mapreduce_output",
                                                 reduce_by=["x"], format=yt.DsvFormat())
            assert client.get_operation_state(mr_operation.id) == "completed"
            assert client.row_count(TEST_DIR + "/mapreduce_output") == 3

            with client.Transaction() as tx:
                client.set("//@attr", 10)
                assert client.exists("//@attr")
                assert client.get_current_transaction_id() == tx.transaction_id

            tx = client.start_transaction(timeout=5000)
            with client.PingTransaction(tx, ping_period=1000, ping_timeout=1000):
                assert client.exists("//sys/transactions/{0}".format(tx))
                client.COMMAND_PARAMS["transaction_id"] = tx
                assert client.lock(table) != "0-0-0-0"
                client.COMMAND_PARAMS["transaction_id"] = "0-0-0-0"

            client.ping_transaction(tx)
            client.abort_transaction(tx)
            with pytest.raises(yt.YtError):
                client.commit_transaction(tx)

            transaction_id = client.start_transaction(timeout=60 * 1000)
            with client.Transaction(transaction_id=transaction_id):
                transaction_filepath = TEST_DIR + '/transaction_file'
                client.write_file(transaction_filepath, b'smth')

            assert not client.exists(transaction_filepath)

            last_ping_time = client.get_attribute(
                '//sys/transactions/{}'.format(transaction_id),
                'last_ping_time',
            )
            with client.Transaction(transaction_id=transaction_id, timeout=60 * 1000, acquire=True):
                while last_ping_time == client.get_attribute(
                    '//sys/transactions/{}'.format(transaction_id),
                    'last_ping_time',
                ):
                    time.sleep(1)

            assert client.exists(transaction_filepath)

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
            assert op.get_jobs_with_error_or_stderr() == []

            client.write_file(TEST_DIR + "/file", b"0" * 1000)
            assert client.read_file(TEST_DIR + "/file").read() == b"0" * 1000
            with pytest.raises(yt.YtError):
                client.smart_upload_file("/unexisting")

            assert other_client.get("/")
            assert b'{"a":"b"}\n' == other_client.read_table(other_table, raw=True).read()

            with client.TempTable(TEST_DIR) as table:
                assert client.exists(table)
            assert not client.exists(table)

            expected_schema = (
                yt.schema.TableSchema(strict=True)
                .add_column("first", type_info.String, sort_order="ascending")
                .add_column("second", type_info.Bool)
            )
            path = yt.ypath.ypath_join(TEST_DIR, "/table-with-schema")
            client.create("table", path, recursive=True, attributes={"schema": expected_schema})
            retrieved_schema = client.get_table_schema(path)
            assert expected_schema == retrieved_schema

    @authors("asaitgalin", "ignat")
    def test_default_api_version(self):
        if yt.config["backend"] != "native":
            client = yt.YtClient(proxy=yt.config["proxy"]["url"])
            client.get("/")
            assert client._api_version == "v4"

    @authors("asaitgalin")
    def test_client_with_unknown_api_version(self):
        client = yt.YtClient(config=yt.config.config)
        client.config["api_version"] = None
        client.config["default_api_version_for_http"] = None
        if client.config["backend"] == "native":
            pytest.skip()
        client.get("/")
        assert client._api_version == "v3"

    @authors("asaitgalin", "ignat")
    def test_get_user_name(self):
        if yt.config["backend"] != "native":
            # With disabled authentication in proxy it always return root
            assert yt.get_user_name("") == "root"

        # assert get_user_name("") == None
        # assert get_user_name("12345") == None

        # token = "".join(["a"] * 16)
        # yt.set("//sys/tokens/" + token, "user")
        # assert get_user_name(token) == "user"

    @authors("asaitgalin", "ignat")
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

        assert http.get_token(client=yt.YtClient(config={"token": ""})) is None

    @authors("denvr")
    def test_client_proxy_url(self):
        if yt.config["backend"] != "native":
            url_original_parts = urlparse('//{}'.format(yt.config["proxy"]["url"]))
            client = yt.YtClient(proxy="http://{}:{}".format(url_original_parts.hostname, url_original_parts.port or ""))
            assert client.get("/")
            client = None

            client = yt.YtClient(proxy="https://{}:{}".format(url_original_parts.hostname, url_original_parts.port or ""))

            mocked_adapter_https = requests_mock.Adapter()
            mocked_adapter_http = requests_mock.Adapter()
            yt.http_helpers._get_session(client=client).mount("http://", mocked_adapter_http)
            yt.http_helpers._get_session(client=client).mount("https://", mocked_adapter_https)
            mocked_adapter_https.register_uri(
                "GET",
                requests_mock.ANY,
                text='[{"name": "get", "input_type": "null", "output_type": "structured", "is_volatile": false, "is_heavy": false}]')

            try:
                client.get("/")
            except Exception:
                pass

            assert len(mocked_adapter_http.request_history) == 0
            assert len(mocked_adapter_https.request_history) >= 1
            assert mocked_adapter_https.request_history[0].url.startswith("https://")

    @authors("verytable", "denvr")
    def test_get_proxy_url(self):
        for test_name, proxy_config, expected_url in [
            (
                "cluster_name",
                {"url": "cluster1"},
                "cluster1.yt.yandex.net",
            ),
            (
                "localhost_w_port",
                {"url": "localhost:23924"},
                "localhost:23924",
            ),
            (
                "proxy_fqdn",
                {"url": "sas4-5340-proxy-cluster1.man-pre.yp-c.yandex.net:80"},
                "sas4-5340-proxy-cluster1.man-pre.yp-c.yandex.net:80",
            ),
            (
                "tvm_only",
                {"url": "cluster1", "tvm_only": True},
                "tvm.cluster1.yt.yandex.net:{}".format(http.TVM_ONLY_HTTP_PROXY_PORT),
            ),
            (
                "default_suffix",
                {"url": "cluster1", "default_suffix": ".imaginary.yt.yandex.net"},
                "cluster1.imaginary.yt.yandex.net",
            ),
            (
                "ipv4",
                {"url": "127.0.0.1"},
                "127.0.0.1"
            ),
            (
                "ipv4 with port",
                {"url": "127.0.0.1:23924"},
                "127.0.0.1:23924"
            ),
            (
                "ipv6",
                {"url": "[::1]"},
                "[::1]"
            ),
            (
                "ipv4-mapped",
                {"url": "[::ffff:127.0.0.1]"},
                "[::ffff:127.0.0.1]"
            ),
            (
                "ipv6 with port",
                {"url": "[::1]:23924"},
                "[::1]:23924"
            ),
        ]:
            client = yt.YtClient(config={"proxy": proxy_config})
            assert http.get_proxy_url(client=client) == expected_url, test_name

        client = yt.YtClient()
        with pytest.raises(YtError):
            http.get_proxy_url(client=client)

    @authors("denvr")
    def test_get_proxy_address_url(self):
        config_copy = deepcopy(yt.config.config)
        for test_name, proxy_config, expected_url in [
            (
                "localhost",
                {"url": "localhost"},
                "http://localhost",
            ),
            (
                "localhost w port",
                {"url": "localhost:23924"},
                "http://localhost:23924",
            ),
            (
                "short name w port",
                {"url": "hostname:123"},
                "http://hostname:123",
            ),
            (
                "short_host_w_schame",
                {"url": "https://cluster1"},
                "https://cluster1",
            ),
            (
                "cluster name",
                {"url": "cluster1"},
                "http://cluster1.yt.yandex.net",
            ),
            (
                "proxy_fqdn",
                {"url": "sas4-5340-proxy-cluster1.man-pre.yp-c.yandex.net:80"},
                "http://sas4-5340-proxy-cluster1.man-pre.yp-c.yandex.net:80",
            ),
            (
                "tvm_only",
                {"url": "cluster1", "tvm_only": True},
                "http://tvm.cluster1.yt.yandex.net:{}".format(http.TVM_ONLY_HTTP_PROXY_PORT),
            ),
            (
                "tvm_only_https",
                {"url": "https://cluster1", "tvm_only": True},
                "https://tvm.cluster1:{}".format(http.TVM_ONLY_HTTPS_PROXY_PORT),
            ),
            (
                "tvm_only_http",
                {"url": "http://cluster1", "tvm_only": True},
                "http://tvm.cluster1:{}".format(http.TVM_ONLY_HTTP_PROXY_PORT),
            ),
            (
                "default_suffix",
                {"url": "cluster1", "default_suffix": ".imaginary.yt.yandex.net"},
                "http://cluster1.imaginary.yt.yandex.net",
            ),
            (
                "cluster_name config https",
                {"url": "cluster1", "prefer_https": True},
                "https://cluster1.yt.yandex.net",
            ),
            (
                "localhost",
                {"url": "localhost", "prefer_https": True},
                "https://localhost",
            ),
            (
                "localhost override",
                {"url": "http://localhost", "prefer_https": True},
                "http://localhost",
            ),
            (
                "cluster_name url priority over config 1",
                {"url": "http://cluster1.yt.domain.net", "prefer_https": True},
                "http://cluster1.yt.domain.net",
            ),
            (
                "cluster_name url priority over config 2",
                {"url": "https://cluster1.yt.domain.net", "prefer_https": False},
                "https://cluster1.yt.domain.net",
            ),
            (
                "proxy_fqdn config https",
                {"url": "sas4-5340-proxy-cluster1.man-pre.yp-c.domain.net:80", "prefer_https": True},
                "https://sas4-5340-proxy-cluster1.man-pre.yp-c.domain.net:80",
            ),
            (
                "tvm_only config https",
                {"url": "cluster1", "tvm_only": True, "prefer_https": True},
                "https://tvm.cluster1.yt.yandex.net:{}".format(http.TVM_ONLY_HTTPS_PROXY_PORT),
            ),
            (
                "default_suffix config https",
                {"url": "cluster1", "default_suffix": ".imaginary.yt.cluster.net", "prefer_https": True},
                "https://cluster1.imaginary.yt.cluster.net",
            ),
            (
                "ipv4",
                {"url": "127.0.0.1"},
                "http://127.0.0.1"
            ),
            (
                "ipv4 with port",
                {"url": "127.0.0.1:23924"},
                "http://127.0.0.1:23924"
            ),
            (
                "ipv4 with scheme and port",
                {"url": "https://127.0.0.1:23924"},
                "https://127.0.0.1:23924"
            ),
            (
                "ipv6",
                {"url": "[::1]"},
                "http://[::1]"
            ),
            (
                "ipv4-mapped",
                {"url": "[::ffff:127.0.0.1]"},
                "http://[::ffff:127.0.0.1]"
            ),
            (
                "ipv6 with port",
                {"url": "[::1]:23924"},
                "http://[::1]:23924"
            ),
            (
                "ipv6 with scheme and port",
                {"url": "https://[::1]:23924"},
                "https://[::1]:23924"
            ),
        ]:
            client = yt.YtClient(config={"proxy": proxy_config})
            assert http.get_proxy_address_url(client=client) == expected_url, test_name
            for f in ["url", "default_suffix", "prefer_https", "tvm_only"]:
                yt.config["proxy"][f] = proxy_config.get(f, config_copy["proxy"][f])
            assert http.get_proxy_address_url(client=None) == expected_url, test_name
            for f in ["url", "default_suffix", "prefer_https", "tvm_only"]:
                yt.config["proxy"][f] = config_copy["proxy"][f]

        # check empty
        client = yt.YtClient()
        assert http.get_proxy_address_url(required=False, client=client) is None
        with pytest.raises(YtError):
            http.get_proxy_address_url(client=client)

        # check custom
        client = yt.YtClient(config={"proxy": {"url": "test1:555", "prefer_https": True}})
        assert http.get_proxy_address_url(client=client, replace_host=None) == "https://test1:555"
        assert http.get_proxy_address_url(client=client, replace_host="custom") == "https://custom.yt.yandex.net"
        assert http.get_proxy_address_url(client=client, replace_host="custom:666") == "https://custom:666"
        assert http.get_proxy_address_url(client=client, replace_host="http://scheme_override") == "http://scheme_override"
        assert http.get_proxy_address_url(client=client, replace_host="custom", add_path="aaa/bbb") == "https://custom.yt.yandex.net/aaa/bbb"
        assert http.get_proxy_address_url(client=client, replace_host="custom", add_path="/aaa/bbb") == "https://custom.yt.yandex.net/aaa/bbb"

        assert http.get_proxy_address_netloc(client=client, replace_host=None) == "test1:555"
        assert http.get_proxy_address_netloc(client=client, replace_host="custom") == "custom.yt.yandex.net"
        assert http.get_proxy_address_netloc(client=client, replace_host="custom.aaa.ru:123") == "custom.aaa.ru:123"
        assert http.get_proxy_address_netloc(client=client, replace_host="http://custom.aaa.ru") == "custom.aaa.ru"

        client = yt.YtClient(config={"proxy": {"url": "https://secuered.host:555", "prefer_https": None}})
        assert http.get_proxy_address_url(client=client, replace_host="must_secured.host") == "https://must_secured.host"

        client = yt.YtClient(config={"proxy": {"url": "secured_tvm_cluster", "tvm_only": True, "prefer_https": True}})
        assert http.get_proxy_address_url(client=client) == "https://tvm.secured_tvm_cluster.yt.yandex.net:9443"
        assert http.get_proxy_address_url(client=client, replace_host="heavy.proxy.full.fqdn") == "https://heavy.proxy.full.fqdn:9443"

        client = yt.YtClient(config={"proxy": {"url": "secured_tvm_cluster", "tvm_only": True, "prefer_https": False}})
        assert http.get_proxy_address_url(client=client) == "http://tvm.secured_tvm_cluster.yt.yandex.net:9026"
        assert http.get_proxy_address_url(client=client, replace_host="heavy.proxy.full.fqdn") == "http://heavy.proxy.full.fqdn:9026"
