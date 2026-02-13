from .proxy_format_config import _TestProxyFormatConfigBase

from yt_env_setup import YTEnvSetup, Restarter, MASTERS_SERVICE, NODES_SERVICE, is_asan_build

from yt_helpers import profiler_factory, read_structured_log, write_log_barrier

from yt.environment.tls_helpers import (
    create_certificate,
    get_certificate_fingerprint,
    get_server_certificate,
)

from yt_commands import (
    authors, wait, wait_no_assert, create, ls, get, set, remove, map,
    create_user, create_proxy_role, issue_token, make_ace,
    create_access_control_object_namespace, create_access_control_object,
    with_breakpoint, wait_breakpoint, print_debug, raises_yt_error,
    read_table, write_table, add_member, Operation, discover_proxies,
    start_transaction,
)

from yt.common import YtResponseError, YtError
import yt.packages.requests as requests
import yt.yson as yson
import yt_error_codes

from yt_driver_bindings import Driver

import pytest

from copy import deepcopy
from datetime import datetime, timedelta
from typing import Any
import collections
import json
import os
import struct
import socket
import time
import uuid


def try_parse_yt_error_headers(rsp):
    if "X-YT-Error" in rsp.headers:
        assert "X-YT-Framing" not in rsp.headers
        raise YtResponseError(json.loads(rsp.headers.get("X-YT-Error")))
    rsp.raise_for_status()


def try_parse_yt_error_trailers(rsp):
    trailers = rsp.trailers()
    if trailers is not None and "X-YT-Error" in trailers:
        assert "X-YT-Framing" not in trailers
        raise YtResponseError(json.loads(trailers.get("X-YT-Error")))

##################################################################


class HttpProxyTestBase(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 5
    NUM_SCHEDULERS = 1
    ENABLE_HTTP_PROXY = True
    ENABLE_RPC_PROXY = True
    NUM_SECONDARY_MASTER_CELLS = 2
    NUM_HTTP_PROXIES = 1
    NUM_RPC_PROXIES = 2

    DELTA_HTTP_PROXY_CONFIG = {
        "coordinator": {
            "heartbeat_interval": 100,
            "death_age": 500,
            "cypress_timeout": 50,
        },
        "api": {
            "force_tracing": True,
        },
        "access_checker": {
            "enabled": True,
            "cache": {
                "expire_after_access_time": 100,
            },
        },
        "cancel_fiber_on_connection_close": True,
    }

    MASTER_CELL_DESCRIPTORS = {
        "11": {"roles": ["chunk_host"]},
        "12": {"roles": ["chunk_host"]},
    }

    USER = "root"
    PATH = "//tmp/test"
    PARAMS = {"path": PATH, "output_format": "yson"}

    def _get_proxy_address(self):
        return "http://" + self.Env.get_proxy_address()

    def _get_https_proxy_url(self):
        return self.Env.get_https_proxy_url()

    def _get_https_proxy_address(self):
        return self.Env.get_http_proxy_address(https=True)

    def _get_https_ca_cert(self):
        return self.Env.yt_config.public_ca_cert

    def _get_proxy_cert_path(self, index=0):
        proxy_config = self.Env.configs["http_proxy"][index]
        proxy_cert = proxy_config["https_server"]["credentials"]["cert_chain"]["file_name"]
        proxy_cert_key = proxy_config["https_server"]["credentials"]["private_key"]["file_name"]
        return proxy_cert, proxy_cert_key

    def _get_build_snapshot_url(self):
        return self._get_proxy_address() + "/api/v4/build_snapshot"

    def _get_master_exit_read_only_url(self):
        return self._get_proxy_address() + "/api/v4/master_exit_read_only"

    def _get_master_address(self):
        return ls("//sys/primary_masters", suppress_transaction_coordinator_sync=True)[0]

    def _get_hydra_monitoring(self, master=None):
        if master is None:
            master = self._get_master_address()
        return get(
            "//sys/primary_masters/{}/orchid/monitoring/hydra".format(master),
            suppress_transaction_coordinator_sync=True,
            default={},
        )

    def _execute_command(self, http_method, command_name, params=PARAMS, user="root", data=None):
        headers = {
            "X-YT-Parameters": yson.dumps(params),
            "X-YT-Header-Format": "<format=text>yson",
            "X-YT-Output-Format": "<format=text>yson",
            "X-YT-User-Name": user,
        }
        rsp = requests.request(
            http_method,
            "{}/api/v4/{}".format(self._get_proxy_address(), command_name),
            headers=headers,
            data=data,
        )

        try_parse_yt_error_headers(rsp)
        return rsp


class TestHttpProxy(HttpProxyTestBase):
    def teardown_method(self, method):
        for proxy in ls("//sys/http_proxies"):
            set("//sys/http_proxies/{}/@role".format(proxy), "data")
        super(TestHttpProxy, self).teardown_method(method)

    @authors("prime")
    def test_ping(self):
        rsp = requests.get(self._get_proxy_address() + "/ping")
        rsp.raise_for_status()

    @authors("prime")
    def test_version(self):
        rsp = requests.get(self._get_proxy_address() + "/version")
        rsp.raise_for_status()

    @authors("prime")
    def test_service(self):
        service = requests.get(self._get_proxy_address() + "/service").json()
        assert "version" in service
        assert "start_time" in service

    @authors("levysotsky")
    def test_hosts(self):
        proxy = ls("//sys/http_proxies")[0]

        def get_yson(url):
            return yson.loads(requests.get(url).content)

        assert get_yson(self._get_proxy_address() + "/hosts") == [proxy]
        assert get_yson(self._get_proxy_address() + "/hosts?role=data") == [proxy]
        assert get_yson(self._get_proxy_address() + "/hosts?role=control") == []

        def make_failing_request_and_check_counter(counter):
            url = self._get_proxy_address() + "/api/v3/find_meaning_of_life"
            requests.get(url)
            return counter.get_delta() > 0

        profiler = profiler_factory().at_http_proxy(proxy, fixed_tags={"http_code": "404"})
        data_http_code_counter = profiler.counter("http_proxy/http_code_count", tags={"proxy_role": "data"})

        wait(lambda: make_failing_request_and_check_counter(data_http_code_counter))

        set("//sys/http_proxies/" + proxy + "/@role", "control")

        def check_role_updated():
            return get_yson(self._get_proxy_address() + "/hosts") == [] and \
                get_yson(self._get_proxy_address() + "/hosts?role=data") == [] and \
                get_yson(self._get_proxy_address() + "/hosts?role=control") == [proxy]

        # Wait until the proxy entry will be updated on the coordinator.
        wait(check_role_updated)

        control_http_code_counter = profiler.counter("http_proxy/http_code_count", tags={"proxy_role": "control"})

        wait(lambda: make_failing_request_and_check_counter(control_http_code_counter))

        hosts = requests.get(self._get_proxy_address() + "/hosts/all").json()
        assert len(hosts) == 1
        assert not hosts[0]["banned"]

    @authors("rp-1", "nadya73")
    def test_symbolic_error_code(self):
        proxy = ls("//sys/http_proxies")[0]

        profiler = profiler_factory().at_http_proxy(proxy)
        api_error_count_counter = profiler.counter(
            "http_proxy/api_error_count",
            tags={"error_code": "NYT::NYTree::EErrorCode::AlreadyExists"})

        url = self._get_proxy_address() + "/api/v4/create?path=//some_table&type=table"
        # Create table.
        requests.post(url)
        # Table with this name already exists.
        requests.post(url)

        wait(lambda: api_error_count_counter.get_delta() > 0)

    def _execute_with_ip_and_port(self, http_method, command_name, params, user="root", data=None,
                                  api_version="v4", fake_ip="", fake_port=""):
        headers = {
            "X-YT-Parameters": yson.dumps(object=params, yson_type="node"),
            "X-YT-Header-Format": yson.dumps(object="yson", yson_type="node"),
            "X-YT-Output-Format": yson.dumps(object="yson", yson_type="node"),
            "X-YT-Input-Format":  yson.dumps(object="yson", yson_type="node"),
            "X-YT-User-Name": user,
            "X-Forwarded-For-Y": fake_ip,
            "X-Source-Port-Y": fake_port,
        }
        rsp = requests.request(
            http_method,
            f"{self._get_proxy_address()}/api/{api_version}/{command_name}",
            headers=headers,
            data=data
        )
        try_parse_yt_error_headers(rsp)
        return rsp

    @authors("theevilbird")
    @pytest.mark.parametrize("api_queries", [
        {
            "version": "v3",
            "start_tx": "start_tx",
            "ping_tx": "ping_tx"
        },
        {
            "version": "v4",
            "start_tx": "start_transaction",
            "ping_tx": "ping_transaction"
        }
    ])
    def test_transaction_pinger_ip(self, api_queries):
        fake_pinger_ip = "my_fake_ip"
        fake_ports = ["1337", "1338", "1339"]

        def get_expected_fake_ip(index):
            return f"[{fake_pinger_ip}]:{fake_ports[index]}"

        def get_tx_id(rsp):
            if api_queries["version"] == "v3":
                return yson.loads(rsp.content)
            elif api_queries["version"] == "v4":
                return yson.loads(rsp.content)["transaction_id"]

        rsp = self._execute_with_ip_and_port(
            "POST",
            api_queries["start_tx"],
            {},
            user="root",
            api_version=api_queries["version"],
            fake_ip=fake_pinger_ip,
            fake_port=fake_ports[0]
        )
        tx = get_tx_id(rsp)
        assert get(f"//sys/transactions/{tx}/@last_ping_address") == get_expected_fake_ip(0)

        self._execute_with_ip_and_port(
            "POST",
            api_queries["ping_tx"],
            {"transaction_id": tx},
            user="root",
            api_version=api_queries["version"],
            fake_ip=fake_pinger_ip,
            fake_port=fake_ports[0]
        )
        assert get(f"//sys/transactions/{tx}/@last_ping_address") == get_expected_fake_ip(0)

        rsp = self._execute_with_ip_and_port(
            "POST",
            api_queries["start_tx"],
            {"transaction_id": tx, "ping_ancestor_transactions": "false"},
            user="root",
            api_version=api_queries["version"],
            fake_ip=fake_pinger_ip,
            fake_port=fake_ports[1]
        )
        tx1 = get_tx_id(rsp)
        assert get(f"//sys/transactions/{tx}/@last_ping_address") == get_expected_fake_ip(0)
        assert get(f"//sys/transactions/{tx1}/@last_ping_address") == get_expected_fake_ip(1)

        self._execute_with_ip_and_port(
            "POST",
            api_queries["ping_tx"],
            {"transaction_id": tx1, "ping_ancestor_transactions": "false"},
            user="root",
            api_version=api_queries["version"],
            fake_ip=fake_pinger_ip,
            fake_port=fake_ports[1]
        )
        assert get(f"//sys/transactions/{tx}/@last_ping_address") == get_expected_fake_ip(0)
        assert get(f"//sys/transactions/{tx1}/@last_ping_address") == get_expected_fake_ip(1)

        self._execute_with_ip_and_port(
            "POST",
            api_queries["ping_tx"],
            {"transaction_id": tx1, "ping_ancestor_transactions": "true"},
            user="root",
            api_version=api_queries["version"],
            fake_ip=fake_pinger_ip,
            fake_port=fake_ports[2]
        )

        # Expect any of two ips because start_tx repinging transaction
        assert get(f"//sys/transactions/{tx}/@last_ping_address") in [get_expected_fake_ip(0), get_expected_fake_ip(2)]
        assert get(f"//sys/transactions/{tx1}/@last_ping_address") == get_expected_fake_ip(2)

        rsp = self._execute_with_ip_and_port(
            "POST",
            api_queries["start_tx"],
            {},
            user="root",
            api_version=api_queries["version"],
        )
        tx2 = get_tx_id(rsp)
        assert get(f"//sys/transactions/{tx2}/@last_ping_address") != yson.YsonEntity()

    @authors("theevilbird")
    @pytest.mark.parametrize("api_queries", [
        {
            "version": "v4",
            "start_tx": "start_transaction",
            "ping_tx": "ping_transaction"
        }
    ])
    def test_pinger_ip_table_commands(self, api_queries):
        fake_pinger_ip = "my_fake_ip"
        fake_ports = ["1337", "1338", "1339"]

        def get_expected_fake_ip(index):
            return f"[{fake_pinger_ip}]:{fake_ports[index]}"

        def get_tx_id(rsp):
            if api_queries["version"] == "v3":
                return yson.loads(rsp.content)
            elif api_queries["version"] == "v4":
                return yson.loads(rsp.content)["transaction_id"]

        self._execute_with_ip_and_port(
            "POST",
            "create",
            {"path": "//tmp/table", "type": "table"},
            user="root",
        )

        rsp = self._execute_with_ip_and_port(
            "POST",
            "start_transaction",
            {},
            user="root",
            fake_ip=fake_pinger_ip,
            fake_port=fake_ports[1]
        )

        tx = get_tx_id(rsp)
        assert get(f"//sys/transactions/{tx}/@last_ping_address") == get_expected_fake_ip(1)

        content = [
            {"a": "1", "b": "2"},
            {"a": "3", "b": "4"},
        ]
        self._execute_with_ip_and_port(
            "put",
            "write_table",
            {"path": "//tmp/table"},
            user="root",
            data=yson.dumps(object=content, yson_type="list_fragment"),
            fake_ip=fake_pinger_ip,
            fake_port=fake_ports[2]
        )
        assert get(f"//sys/transactions/{tx}/@last_ping_address") in [get_expected_fake_ip(1), get_expected_fake_ip(2)]

    @authors("aleksandr.gaev")
    def test_cluster_connection(self):
        def get_cluster_connection(path):
            return requests.get(self._get_proxy_address() + path)

        url = "{}/api/v3/get?path=//sys/@cluster_connection".format(self._get_proxy_address())
        api_result = requests.get(url)
        driver_result = get("//sys/@cluster_connection", is_raw=True, output_format="json")

        assert json.loads(api_result.content) == json.loads(driver_result)

        assert get_cluster_connection("/cluster_connection").content == api_result.content
        assert get_cluster_connection("/cluster_connection/").content == api_result.content
        assert get_cluster_connection("/cluster_connection/abcd").status_code == 404
        assert get_cluster_connection("/cluster_connection?abcd=efhg").content == api_result.content

    @authors("prime")
    def test_supported_api_versions(self):
        assert ["v3", "v4"] == requests.get(self._get_proxy_address() + "/api").json()

    @authors("prime")
    def test_discover_versions_v2(self):
        # Give all components some time to be considered online.
        time.sleep(5)

        rsp = requests.get(self._get_proxy_address() + "/internal/discover_versions/v2")
        rsp.raise_for_status()

        versions = rsp.json()
        assert "details" in versions
        assert "summary" in versions

        print_debug(f"Collected component versions: {versions}")

        counts = collections.Counter()

        for instance in versions["details"]:
            assert "address" in instance
            assert "start_time" in instance
            assert "type" in instance
            assert "version" in instance

            if "state" in instance:
                assert instance["state"] == "online"

            counts[instance["type"]] += 1

        summary = versions["summary"]
        # All components run on the same version + there is a total summary.
        assert len(summary) == 2
        for version_summary in summary.values():
            for type, component_summary in version_summary.items():
                assert component_summary["total"] == counts[type]
                assert component_summary["banned"] == 0
                assert component_summary["offline"] == 0

        assert counts["primary_master"] == 1
        assert counts["secondary_master"] == 2
        assert counts["cluster_node"] == 5
        assert counts["scheduler"] == 1
        assert counts["controller_agent"] == 1
        assert counts["http_proxy"] == 1
        assert counts["rpc_proxy"] == 2

    @authors("prime")
    def test_cache_control(self):
        rsp = requests.get(self._get_proxy_address() + "/api/v4/get?path=//@")
        rsp.raise_for_status()

        assert rsp.headers["cache-control"] == "no-store"

    @authors("prime")
    def test_dynamic_config(self):
        monitoring_port = self.Env.configs["http_proxy"][0]["monitoring_port"]
        config_url = "http://localhost:{}/orchid/dynamic_config_manager/effective_config".format(monitoring_port)

        set("//sys/http_proxies/@config", {"tracing": {"user_sample_rate": {"prime": 1.0}}})

        def config_updated():
            config = requests.get(config_url).json()
            return "prime" in config["tracing"]["user_sample_rate"]

        wait(config_updated)

    @authors("prime")
    def test_taken_port(self):
        pytest.skip()

        monitoring_port = self.Env.configs["node"][0]["monitoring_port"]

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        with Restarter(self.Env, NODES_SERVICE):
            s.bind(("127.0.0.1", monitoring_port))
            s.listen(1)

    @authors("greatkorn")
    def test_kill_nodes(self):
        http_proxy_name = "test_http_proxy.ytsaurus.tech"
        rpc_proxy_name = "test_rpc_proxy.ytsaurus.tech"
        create("map_node", f"//sys/http_proxies/{http_proxy_name}")
        set(
            f"//sys/http_proxies/{http_proxy_name}/@liveness",
            {"updated_at": "2010-06-24T11:23:30.156098Z"},
        )
        set(f"//sys/http_proxies/{http_proxy_name}/@start_time", "2009-06-19T16:39:02.171721Z")
        set(f"//sys/http_proxies/{http_proxy_name}/@version", "19.5.30948-master-ya~c9facaeaca")
        create("map_node", f"//sys/rpc_proxies/{rpc_proxy_name}")
        set(
            f"//sys/rpc_proxies/{rpc_proxy_name}/@start_time",
            "2009-06-19T16:39:02.171721Z",
        )
        set(
            f"//sys/rpc_proxies/{rpc_proxy_name}/@version",
            "19.5.30948-master-ya~c9facaeaca",
        )

        rsp = requests.get(self._get_proxy_address() + "/internal/discover_versions/v2")
        rsp.raise_for_status()

        status = rsp.json()
        for proxy in status["details"]:
            if proxy["address"] in (http_proxy_name, rpc_proxy_name):
                assert proxy.get("state") == "offline"

        remove(f"//sys/http_proxies/{http_proxy_name}")
        remove(f"//sys/rpc_proxies/{rpc_proxy_name}")

    @authors("greatkorn")
    def test_structured_logs(self):
        proxy_address = "localhost:" + str(self.Env.configs["http_proxy"][0]["rpc_port"])

        from_barrier = write_log_barrier(proxy_address)
        client = self.Env.create_client()
        client.list("//sys")
        to_barrier = write_log_barrier(proxy_address)

        log_path = self.path_to_run + "/logs/http-proxy-0.json.log"
        events = read_structured_log(log_path, from_barrier, to_barrier)

        has_log_entry = False
        for event in events:
            if event.get("path") == "//sys":
                has_log_entry = True
        assert has_log_entry

    @authors("greatkorn")
    def test_fail_logging(self):
        requests.get(self._get_proxy_address() + "/api/v2/get")

    @authors("alexkolodezny")
    def test_banned_proxy(self):
        proxy = ls("//sys/http_proxies")[0]
        set("//sys/http_proxies/" + proxy + "/@banned", True)
        wait(lambda: not requests.get(self._get_proxy_address() + "/ping").ok)

        set("//sys/http_proxies/" + proxy + "/@banned", False)
        wait(lambda: requests.get(self._get_proxy_address() + "/ping").ok)

    @authors("alexkolodezny")
    def test_proxy_unavailable_on_master_failure(self):
        with Restarter(self.Env, MASTERS_SERVICE):
            wait(lambda: not requests.get(self._get_proxy_address() + "/ping").ok)

        wait(lambda: requests.get(self._get_proxy_address() + "/ping").ok)

    @authors("rp-1")
    def test_error_format(self):
        proxy_address = self._get_proxy_address()

        url = proxy_address + "/api/v4/get?path=//does_not_exist/@"

        headers = {
            "X-YT-Header-Format": "<format=text>yson",
            "X-YT-Error-Format": "<annotate_with_types=%true>json",
        }

        rsp = requests.get(url, headers=headers)

        error_json = json.loads(rsp.headers["X-YT-Error"])

        assert "$type" in error_json["attributes"]["tid"]
        assert "$value" in error_json["attributes"]["tid"]

    @authors("rp-1")
    def test_error_format_type(self):
        proxy_address = self._get_proxy_address()

        url = proxy_address + "/api/v4/get?path=//does_not_exist/@"

        headers = {
            "X-YT-Header-Format": "<format=text>yson",
            "X-YT-Error-Format": "<format=text>yson",
        }

        rsp = requests.get(url, headers=headers)

        assert rsp.headers["X-YT-Error-Content-Type"] == "application/x-yt-yson-text"

    @authors("rp-1")
    def test_error_web_json(self):
        proxy_address = self._get_proxy_address()

        url = proxy_address + "/api/v4/get?path=//does_not_exist/@"

        headers = {
            "X-YT-Header-Format": "<format=text>yson",
            "X-YT-Error-Format": "web_json",
        }

        rsp = requests.get(url, headers=headers)

        tid = json.loads(rsp.headers["X-YT-Error"])["attributes"]["tid"]

        if isinstance(tid, int):
            assert tid <= 2 ** 53 - 1
        else:
            assert "$type" in tid
            assert "$value" in tid

    @authors("ermolovd")
    def test_request_cancellation(self):
        user = "test_request_cancellation"
        dir_path = "//tmp/test_request_cancellation"
        create_user(user)

        create("map_node", dir_path)
        set(f"{dir_path}/@test_attr", [])

        address = self.Env.get_http_proxy_address()
        host, port = address.split(":")
        port = int(port)

        def make_request(count, correlation_id=None):
            params = {
                "concurrency": 1,
                "requests": [
                    {
                        "command": "set",
                        "parameters": {"path": f"{dir_path}/@test_attr/end"},
                        "input": 1,
                    }
                ] * count,
            }

            body = yson.dumps(params, yson_format="text").decode("utf-8")

            if correlation_id is None:
                correlation_id = str(uuid.uuid4())

            return (
                "POST /api/v4/execute_batch HTTP/1.1\r\n"
                f"Host: {address}\r\n"
                f"X-YT-Header-Format: <format=text>yson\r\n"
                f"X-YT-Output-Format: <format=text>yson\r\n"
                f"X-YT-Input-Format: <format=text>yson\r\n"
                f"X-YT-User-Name: {user}\r\n"
                f"X-YT-Correlation-Id: {correlation_id}\r\n"
                f"Content-Length: {len(body)}\r\n"
                f"Connection: close\r\n"
                "\r\n"
                f"{body}"
            )

        def recv_all(sock, bufsize=4096):
            chunks = []
            while True:
                data = sock.recv(bufsize)
                if not data:
                    break
                chunks.append(data)
            return b"".join(chunks)

        def first_request():
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((host, port))
            request = make_request(100)
            sock.sendall(request.encode("ascii"))
            bytes_response = recv_all(sock)
            assert b"200 OK" in bytes_response
        first_request()
        assert get(f"{dir_path}/@test_attr") == [1] * 100

        set(f"//sys/users/{user}/@write_request_rate_limit", 1)
        set(f"{dir_path}/@test_attr", [])

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((host, port))
        correlation_id = "111-222-333-" + str(uuid.uuid4())
        request = make_request(1024, correlation_id=correlation_id)
        sock.sendall(request.encode("ascii"))
        time.sleep(1)
        sock.close()

        prev_len = 0
        for _ in range(10):
            time.sleep(2)
            cur = get(f"{dir_path}/@test_attr")
            if len(cur) == 1024:
                raise AssertionError(f"Write request limiter didn't work for request {correlation_id}")
            if len(cur) == prev_len:
                break
            prev_len = len(cur)
        else:
            raise AssertionError(f"Request {correlation_id} doesn't seem to be canceled")


@pytest.mark.enabled_multidaemon
class TestHttpProxyMemoryDrop(HttpProxyTestBase):
    ENABLE_MULTIDAEMON = True

    DELTA_HTTP_PROXY_CONFIG = {
        "memory_limits": {
            "total": 1000000000,
        }
    }

    @authors("nadya73")
    def test_basic(self):
        monitoring_port = self.Env.configs["http_proxy"][0]["monitoring_port"]
        config_url = "http://localhost:{}/orchid/dynamic_config_manager/effective_config".format(monitoring_port)

        def config_updated(expected_total_memory_limit_ratio):
            config = requests.get(config_url).json()
            return config.get("api", {}).get("default_memory_limit_ratios", {}).get("total_memory_limit_ratio", 0.0) == expected_total_memory_limit_ratio

        create("table", "//tmp/test")

        wait(lambda: requests.get(f"{self._get_proxy_address()}/api/v4/get?path=//@").ok)

        # Total memory limit was not reached.
        self._execute_command("GET", "get", {"path": "//@"})
        self._execute_command("GET", "read_table", {"path": "//tmp/test"})

        total_memory_limit_ratio = 0.001
        set("//sys/http_proxies/@config", {
            "api": {
                "default_memory_limit_ratios": {
                    "total_memory_limit_ratio": total_memory_limit_ratio
                }
            }
        })
        wait(lambda: config_updated(total_memory_limit_ratio))

        # Total memory limit was not reached.
        self._execute_command("GET", "get", {"path": "//@"})
        self._execute_command("GET", "read_table", {"path": "//tmp/test"})

        total_memory_limit_ratio = 0.0005
        heavy_request_memory_limit_ratio = 0.0
        set("//sys/http_proxies/@config", {
            "api": {
                "default_memory_limit_ratios": {
                    "total_memory_limit_ratio": total_memory_limit_ratio,
                    "heavy_request_memory_limit_ratio": heavy_request_memory_limit_ratio,
                }
            }
        })
        wait(lambda: config_updated(total_memory_limit_ratio))

        # Heavy request limit does not affect get request.
        self._execute_command("GET", "get", {"path": "//@"})

        # Heavy request limit was reached.
        with raises_yt_error("Request is dropped due to high memory pressure") as err:
            self._execute_command("GET", "read_table", {"path": "//tmp/test"})
        assert err[0].is_rpc_unavailable()

        total_memory_limit_ratio = 0.0000001
        set("//sys/http_proxies/@config", {
            "api": {
                "default_memory_limit_ratios": {
                    "total_memory_limit_ratio": total_memory_limit_ratio
                }
            }
        })
        wait(lambda: config_updated(total_memory_limit_ratio))

        with raises_yt_error("Request is dropped due to high memory pressure") as err:
            self._execute_command("GET", "get", {"path": "//@"})
        assert err[0].is_rpc_unavailable()

        with raises_yt_error("Request is dropped due to high memory pressure") as err:
            self._execute_command("GET", "read_table", {"path": "//tmp/test"})
        assert err[0].is_rpc_unavailable()

        def role_config_updated(expected_total_memory_limit_ratio):
            config = requests.get(config_url).json()
            return config.get("api", {}).get("role_to_memory_limit_ratios", {}).get("data", {}).get("total_memory_limit_ratio", 0.0) == expected_total_memory_limit_ratio

        total_memory_limit_ratio = 0.001
        heavy_request_memory_limit_ratio = 0.001
        set("//sys/http_proxies/@config", {
            "api": {
                "default_memory_limit_ratios": {
                    "total_memory_limit_ratio": 0.0000001
                },
                "role_to_memory_limit_ratios": {
                    "data": {
                        "total_memory_limit_ratio": total_memory_limit_ratio,
                        "heavy_request_memory_limit_ratio": heavy_request_memory_limit_ratio,
                    }
                }
            }
        })
        wait(lambda: role_config_updated(total_memory_limit_ratio))

        # Total memory limit for data role was not reached.
        self._execute_command("GET", "get", {"path": "//@"})
        self._execute_command("GET", "read_table", {"path": "//tmp/test"})


@pytest.mark.enabled_multidaemon
class TestHttpProxyUserMemoryDrop(HttpProxyTestBase):
    ENABLE_MULTIDAEMON = True

    @authors("nadya02")
    @pytest.mark.timeout(120)
    def test_concurrency_limit(self):
        create_user("nadya")

        create("table", "//tmp/test")

        proxy_name = ls("//sys/http_proxies")[0]
        # No limits.
        self._execute_command("GET", "read_table", {"path": "//tmp/test"}, user="nadya")

        set("//sys/http_proxies/@config", {
            "api": {
                "user_to_concurrency_limit_ratio" : {
                    "nadya": 0.0,
                },
            }
        })

        def role_config_updated(expected):
            config = get("//sys/http_proxies/" + proxy_name + "/orchid/dynamic_config_manager/effective_config")
            return config.get("api", {}).get("user_to_concurrency_limit_ratio") == expected

        wait(lambda: role_config_updated({
            "nadya": 0.0,
        }))

        self._execute_command("GET", "read_table", {"path": "//tmp/test"})

        with raises_yt_error("Received response with error"):
            self._execute_command("GET", "read_table", {"path": "//tmp/test"}, user="nadya")

    @authors("nadya02")
    @pytest.mark.timeout(120)
    def test_specific_user_drop(self):
        create_user("nadya")

        def get_yson(url):
            return yson.loads(requests.get(url).content)

        if get_yson(self._get_proxy_address() + "/hosts?role=data") != []:
            create_proxy_role("ml", "http")
        proxy_name = ls("//sys/http_proxies")[0]

        create("table", "//tmp/test")

        proxy = ls("//sys/http_proxies")[0]

        set("//sys/http_proxies/" + proxy + "/@role", "ml")

        def get_yson(url):
            return yson.loads(requests.get(url).content)

        def check_role_updated():
            return get_yson(self._get_proxy_address() + "/hosts") == [] and \
                get_yson(self._get_proxy_address() + "/hosts?role=data") == []

        wait(check_role_updated)

        set("//sys/http_proxies/@config", {
            "api": {
                "default_memory_limit_ratios": {
                    "total_memory_limit_ratio": 0.98
                }
            }
        })

        def empty_config_updated():
            config = get("//sys/http_proxies/" + proxy_name + "/orchid/dynamic_config_manager/effective_config")
            # Default value - 0.9.
            return config.get("api", {}).get("default_memory_limit_ratios", {}).get("total_memory_limit_ratio", None) == 0.98

        wait(empty_config_updated)

        # No limits.
        self._execute_command("GET", "read_table", {"path": "//tmp/test"}, user="nadya")

        set("//sys/http_proxies/@config", {
            "api": {
                "role_to_memory_limit_ratios" : {
                    "ml" : {
                        "user_to_memory_limit_ratio": {
                            "nadya": 0.0,
                        }
                    },
                },
            }
        })

        def role_config_updated(expected):
            config = get("//sys/http_proxies/" + proxy_name + "/orchid/dynamic_config_manager/effective_config")
            return config.get("api", {}).get("role_to_memory_limit_ratios") == expected

        wait(lambda: role_config_updated({
            "ml": {
                "user_to_memory_limit_ratio": {
                    "nadya": 0.0,
                },
                "total_memory_limit_ratio": 0.9,
                "heavy_request_memory_limit_ratio": 0.9,
            }
        }))

        with raises_yt_error("Request is dropped due to high memory pressure"):
            self._execute_command("GET", "read_table", {"path": "//tmp/test"}, user="nadya")

        set("//sys/http_proxies/@config", {
            "api": {
                "default_memory_limit_ratios": {
                    "user_to_memory_limit_ratio": {
                        "nadya": 1.0,
                    },
                },
                "role_to_memory_limit_ratios" : {
                },
            }
        })

        wait(lambda: role_config_updated({}))

        # No limits.
        self._execute_command("GET", "read_table", {"path": "//tmp/test"}, user="nadya")

        set("//sys/http_proxies/@config", {
            "api": {
                "default_memory_limit_ratios": {
                    "user_to_memory_limit_ratio": {
                        "nadya": 0.0,
                    },
                },
                "role_to_memory_limit_ratios": {
                },
            }
        })

        def default_config_updated(expected):
            config = get("//sys/http_proxies/" + proxy_name + "/orchid/dynamic_config_manager/effective_config")
            return config.get("api", {}).get("default_memory_limit_ratios") == expected

        wait(lambda: default_config_updated({
            "user_to_memory_limit_ratio": {
                "nadya": 0.0,
            },
            "total_memory_limit_ratio": 0.9,
            "heavy_request_memory_limit_ratio": 0.9,
        }))

        with raises_yt_error("Request is dropped due to high memory pressure"):
            self._execute_command("GET", "read_table", {"path": "//tmp/test"}, user="nadya")

    @authors("nadya02")
    @pytest.mark.timeout(120)
    def test_role_default_user_drop(self):

        def get_yson(url):
            return yson.loads(requests.get(url).content)

        if get_yson(self._get_proxy_address() + "/hosts?role=data") != []:
            create_proxy_role("ml", "http")

        proxy_name = ls("//sys/http_proxies")[0]

        create("table", "//tmp/test")

        proxy = ls("//sys/http_proxies")[0]

        set("//sys/http_proxies/" + proxy + "/@role", "ml")

        def check_role_updated():
            return get_yson(self._get_proxy_address() + "/hosts") == [] and \
                get_yson(self._get_proxy_address() + "/hosts?role=data") == []

        wait(check_role_updated)

        set("//sys/http_proxies/@config", {
            "api": {
                "role_to_memory_limit_ratios" : {
                    "ml" : {
                        "default_user_memory_limit_ratio": 0.0
                    }
                }
            }
        })

        def config_updated():
            config = get("//sys/http_proxies/" + proxy_name + "/orchid/dynamic_config_manager/effective_config")
            return config.get("api", {}).get("role_to_memory_limit_ratios", {}).get("ml", {}).get("default_user_memory_limit_ratio", {}) == 0.0

        wait(config_updated)

        with raises_yt_error("Request is dropped due to high memory pressure"):
            self._execute_command("GET", "read_table", {"path": "//tmp/test"})

    @authors("nadya02")
    @pytest.mark.timeout(120)
    def test_default_user_drop(self):
        proxy_name = ls("//sys/http_proxies")[0]

        create("table", "//tmp/test")

        set("//sys/http_proxies/@config", {
            "api": {
                "default_memory_limit_ratios": {
                    "default_user_memory_limit_ratio": 0.0,
                },
            },
        })

        def config_updated():
            config = get("//sys/http_proxies/" + proxy_name + "/orchid/dynamic_config_manager/effective_config")
            return config.get("api", {}).get("default_memory_limit_ratios", {}).get("default_user_memory_limit_ratio", None) == 0.0

        wait(config_updated)

        with raises_yt_error("Request is dropped due to high memory pressure"):
            self._execute_command("GET", "read_table", {"path": "//tmp/test"})


@pytest.mark.enabled_multidaemon
class TestHttpProxyPoolMetrics(HttpProxyTestBase):
    @authors("nadya02")
    @pytest.mark.timeout(120)
    def test_memory_pool_metrics(self):
        http_proxies = ls("//sys/http_proxies")
        assert len(http_proxies) == 1

        create_user("nadya")

        create("table", "//tmp/test")

        self._execute_command("GET", "read_table", {"path": "//tmp/test"}, user="nadya")
        time.sleep(5)

        profiler = profiler_factory().at_http_proxy(http_proxies[0])
        pool_limit_memory_usage_gauge = profiler.gauge("http_proxy/memory_usage/pool_limit")

        metrics = pool_limit_memory_usage_gauge.get_all()
        metric_count = 0
        has_heavy_request_tag = False
        for metric in metrics:
            if metric["tags"]:
                if "pool" in metric["tags"] and metric["tags"]["pool"] == "nadya":
                    metric_count += 1
                    if "category" in metric["tags"] and metric["tags"]["category"] == "heavy_request":
                        has_heavy_request_tag = True

        assert has_heavy_request_tag
        assert metric_count < 5


class TestFullDiscoverVersions(HttpProxyTestBase):
    ENABLE_MULTIDAEMON = False  # Cell balancer crashes in multidaemon mode.
    NUM_DISCOVERY_SERVERS = 1
    NUM_TIMESTAMP_PROVIDERS = 1
    NUM_MASTER_CACHES = 1
    NUM_CELL_BALANCERS = 1
    ENABLE_BUNDLE_CONTROLLER = True
    NUM_QUEUE_AGENTS = 1
    NUM_TABLET_BALANCERS = 1
    NUM_REPLICATED_TABLE_TRACKERS = 1

    @authors("koloshmet")
    def test_discover_versions_v2(self):
        # Give all components some time to be considered online.
        time.sleep(5)

        rsp = requests.get(self._get_proxy_address() + "/internal/discover_versions/v2")
        rsp.raise_for_status()

        versions = rsp.json()
        assert "details" in versions
        assert "summary" in versions

        print_debug(f"Collected component versions: {versions}")

        counts = collections.Counter()

        for instance in versions["details"]:
            assert "address" in instance
            assert "start_time" in instance
            assert "type" in instance
            assert "version" in instance

            if "state" in instance:
                assert instance["state"] == "online"

            counts[instance["type"]] += 1

        summary = versions["summary"]
        # All components run on the same version + there is a total summary.
        assert len(summary) == 2
        for version_summary in summary.values():
            for type, component_summary in version_summary.items():
                assert component_summary["total"] == counts[type]
                assert component_summary["banned"] == 0
                assert component_summary["offline"] == 0

        assert counts["primary_master"] == 1
        assert counts["secondary_master"] == 2
        assert counts["cluster_node"] == 5
        assert counts["scheduler"] == 1
        assert counts["controller_agent"] == 1
        assert counts["http_proxy"] == 1
        assert counts["rpc_proxy"] == 2
        assert counts["discovery"] == 1
        assert counts["timestamp_provider"] == 1
        assert counts["master_cache"] == 1
        assert counts["bundle_controller"] == 1
        assert counts["queue_agent"] == 1
        assert counts["tablet_balancer"] == 1
        assert counts["replicated_table_tracker"] == 1


@pytest.mark.enabled_multidaemon
class TestCypressProxyDiscoverVersions(HttpProxyTestBase):
    ENABLE_MULTIDAEMON = True

    USE_SEQUOIA = True
    NUM_CLOCKS = 1
    NUM_REMOTE_CLUSTERS = 1
    USE_SEQUOIA_REMOTE_0 = False

    @authors("koloshmet")
    def test_discover_versions_v2(self):
        # Give all components some time to be considered online.
        time.sleep(5)

        rsp = requests.get(self._get_proxy_address() + "/internal/discover_versions/v2")
        rsp.raise_for_status()

        versions = rsp.json()
        assert "details" in versions
        assert "summary" in versions

        print_debug(f"Collected component versions: {versions}")

        counts = collections.Counter()

        for instance in versions["details"]:
            assert "address" in instance
            assert "start_time" in instance
            assert "type" in instance
            assert "version" in instance

            if "state" in instance:
                assert instance["state"] == "online"

            counts[instance["type"]] += 1

        summary = versions["summary"]
        # All components run on the same version + there is a total summary.
        assert len(summary) == 2
        for version_summary in summary.values():
            for type, component_summary in version_summary.items():
                assert component_summary["total"] == counts[type]
                assert component_summary["banned"] == 0
                assert component_summary["offline"] == 0

        assert counts["primary_master"] == 1
        assert counts["secondary_master"] == 2
        assert counts["cluster_node"] == 5
        assert counts["scheduler"] == 1
        assert counts["http_proxy"] == 1
        assert counts["rpc_proxy"] == 2
        assert counts["timestamp_provider"] == 1
        assert counts["cypress_proxy"] == 1


class TestSolomonProxy(HttpProxyTestBase):
    # Instances of the same component have different monitoring ports, since everything is exposed on localhost.
    # For this reason we are only testing components that have a single configured instance ¯\_(ツ)_/¯.
    # Let's explicitly configure them that way.
    NUM_MASTERS = 1
    NUM_SCHEDULERS = 1
    NUM_HTTP_PROXIES = 1
    NUM_RPC_PROXIES = 1

    # Just to make the test run faster.
    NUM_SECONDARY_MASTER_CELLS = 0

    DELTA_HTTP_PROXY_CONFIG = {
        "solomon_proxy": {
            "public_component_names": ["rpc_proxy", "primary_master", "http_proxy"],
            # We will configure the endpoint providers later, since monitoring ports are not yet generated at this point.
        }
    }

    ENABLE_MULTIDAEMON = False  # Solomon test.

    # This is sad, but I don't see any other way to retrieve the generated monitoring ports during cluster configuration.
    @classmethod
    def apply_config_patches(cls, configs, ytserver_version, cluster_index, cluster_path):
        super(TestSolomonProxy, cls).apply_config_patches(configs, ytserver_version, cluster_index, cluster_path)

        primary_master_config = list(configs["master"].values())[0][0]
        rpc_proxy_config = configs["rpc_proxy"][0]
        http_proxy_config = configs["http_proxy"][0]
        scheduler_config = configs["scheduler"][0]

        configs["http_proxy"][0]["solomon_proxy"]["endpoint_providers"] = [
            {
                "component_type": "primary_master",
                "monitoring_port": primary_master_config["monitoring_port"],
                # We use this to distinguish instances based on their name.
                "include_port_in_instance_name": True,
            },
            {
                "component_type": "rpc_proxy",
                "monitoring_port": rpc_proxy_config["monitoring_port"],
                "include_port_in_instance_name": True,
            },
            {
                "component_type": "http_proxy",
                "monitoring_port": http_proxy_config["monitoring_port"],
                "include_port_in_instance_name": True,
            },
            # Not declared public above!
            {
                "component_type": "scheduler",
                "monitoring_port": scheduler_config["monitoring_port"],
                "include_port_in_instance_name": True,
            },
        ]

        # This isn't very pretty either, but it will do for now.
        # TODO(achulkov2): Initialize host for all components similar to jaeger init?
        primary_master_config["solomon_exporter"]["host"] = "master.yt.test"
        rpc_proxy_config["solomon_exporter"]["host"] = "rpc-proxy.yt.test"
        http_proxy_config["solomon_exporter"]["host"] = "http-proxy.yt.test"
        scheduler_config["solomon_exporter"]["host"] = "scheduler.yt.test"

    @staticmethod
    def filter_sensors(sensors, sensor_name=None, host=None):
        def match(sensor, sensor_name, host):
            sensor_filter = sensor_name is None or sensor_name in sensor["labels"]["sensor"]
            host_filter = host is None or host in sensor["labels"]["host"]
            return all([sensor_filter, host_filter])

        return [sensor for sensor in sensors if match(sensor, sensor_name, host)]

    def get_sensors_raw(self, **kwargs):
        rsp = requests.get(f"{self._get_proxy_address()}/solomon_proxy/sensors", **kwargs)
        try_parse_yt_error_headers(rsp)
        return rsp

    def get_sensors(self, **kwargs):
        rsp = self.get_sensors_raw(**kwargs)
        return rsp.json().get("sensors", [])

    # We use the build version sensor as an indicator sensor.
    @staticmethod
    def get_instance_count(sensors, sensor_name="build.version"):
        return len(TestSolomonProxy.filter_sensors(sensors, sensor_name=sensor_name))

    @authors("achulkov2")
    def test_basic(self):
        sensors = self.get_sensors()

        # Sensors for schedulers are not returned, since they are declared public.
        assert len(self.filter_sensors(sensors, host="scheduler")) == 0

        build_version_sensors = self.filter_sensors(sensors, sensor_name="build.version")
        # Sensors for schedulers are not returned, since they are declared public.
        assert len(build_version_sensors) == 3

        # No labels should be lost!
        assert self.filter_sensors(build_version_sensors, host="http-proxy")[0]["labels"]["proxy_role"] == "data"
        assert self.filter_sensors(build_version_sensors, host="rpc-proxy")[0]["labels"]["proxy_role"] == "default"

    @authors("achulkov2")
    def test_filters(self):
        # Component name.
        rpc_proxy_sensors = self.get_sensors(params={"component": "rpc_proxies"})
        assert self.filter_sensors(rpc_proxy_sensors, host="rpc-proxy") == rpc_proxy_sensors

        # Instance name.
        http_proxy_instance_sensors = self.get_sensors(params={"instance": f"localhost:{self.Env.configs['http_proxy'][0]['port']}"})
        assert self.filter_sensors(http_proxy_instance_sensors, host="http-proxy") == http_proxy_instance_sensors
        assert self.get_instance_count(http_proxy_instance_sensors) == 1

        # Instance labels.
        rpc_proxy_address = ls("//sys/rpc_proxies")[0]

        set(f"//sys/rpc_proxies/{rpc_proxy_address}/@banned", True)
        wait(lambda: self.get_instance_count(self.get_sensors(params={"instance_banned": "0"})) == 2)

        set(f"//sys/rpc_proxies/{rpc_proxy_address}/@banned", False)
        wait(lambda: self.get_instance_count(self.get_sensors(params={"instance_banned": "0"})) == 3)

        # Solomon shards (which are also instance labels).
        assert self.get_instance_count(self.get_sensors(params={"instance_shard": "all"})) == 3
        assert self.get_instance_count(self.get_sensors(params={"instance_shard": "we-miss-prime"})) == 0

    @authors("achulkov2")
    def test_sharding(self):
        first_shard_size = self.get_instance_count(self.get_sensors(params={"shard_index": 0, "shard_count": 2}))
        second_shard_size = self.get_instance_count(self.get_sensors(params={"shard_index": 1, "shard_count": 2}))
        assert first_shard_size + second_shard_size == 3

    @authors("achulkov2")
    def test_formats(self):
        # Json (default).
        assert self.get_instance_count(self.get_sensors(headers={"Accept": "application/json"})) == 3

        # Prometheus.
        prometheus_sensors_rsp = self.get_sensors_raw(headers={"Accept": "text/plain"})
        assert len([line for line in prometheus_sensors_rsp.text.split("\n") if "# TYPE" not in line and "build_version" in line]) == 3
        # No counter-to-rate transformation for prometheus format.
        assert len([line for line in prometheus_sensors_rsp.text.split("\n") if "_rate" in line and "_rate_limit" not in line]) == 0

        # Spack (only check for errors).
        self.get_sensors_raw(headers={"Accept": "application/x-solomon-spack"})
        self.get_sensors_raw(headers={"Accept": "application/x-solomon-spack", "Accept-Encoding": "zstd"})

    @authors("achulkov2")
    def test_errors(self):
        with raises_yt_error("Invalid sharding configuration"):
            self.get_sensors(params={"shard_index": 5, "shard_count": 2})

        # This also checks that solomon parameters are actually forwarded to endpoints.
        # An error is thrown in case all endpoint respond with errors.
        with raises_yt_error("Could not pull sensors from any endpoint"):
            self.get_sensors(params={"period": "5s"})


@pytest.mark.enabled_multidaemon
class HttpProxyAccessCheckerTestBase(HttpProxyTestBase):
    DELTA_HTTP_PROXY_CONFIG = {
        "access_checker": {
            "enabled": True,
            "cache": {
                "expire_after_access_time": 100,
            },
        },
    }
    ENABLE_MULTIDAEMON = True

    @authors("nadya02")
    def test_user_access_validator(self):
        proxy_address = self._get_proxy_address()

        def check_access(proxy_address, path="/", status_code=200, error_code=None, user=None):
            url = "{}/api/v4/get?path={}".format(proxy_address, path)
            headers = {}
            if user:
                headers["X-YT-User-Name"] = user

            rsp = requests.get(url, headers=headers)

            if error_code is not None :
                assert json.loads(rsp.content)["code"] == error_code

            return rsp.status_code == status_code

        create_user("u")

        set("//sys/users/u/@banned", True)
        wait(lambda: check_access(proxy_address, status_code=403, user="u"))

        set("//sys/users/u/@banned", False)
        wait(lambda: check_access(proxy_address, status_code=200, user="u"))

    @authors("gritukan", "verytable")
    def test_access_checker(self):
        def check_access(proxy_address, user):
            url = "{}/api/v3/get?path=//sys/@config".format(proxy_address)
            rsp = requests.get(url, headers={"X-YT-User-Name": user})
            assert rsp.status_code == 200 or rsp.status_code == 403
            return rsp.status_code == 200

        self.create_proxy_role_namespace()

        create_user("u")
        self.create_proxy_role("r1")
        self.create_proxy_role("r2")

        self.set_acl("r1", [make_ace("deny", "u", "use")])
        self.set_acl("r2", [make_ace("allow", "u", "use")])

        proxy = ls("//sys/http_proxies")[0]
        proxy_address = self._get_proxy_address()

        # "u" is not allowed to use proxies with role "r1".
        set("//sys/http_proxies/" + proxy + "/@role", "r1")
        wait(lambda: not check_access(proxy_address, "u"))

        # "u" is allowed to use proxies with role "r2".
        set("//sys/http_proxies/" + proxy + "/@role", "r2")
        wait(lambda: check_access(proxy_address, "u"))

        # Now "u" is not allowed to use proxies with role "r2".
        self.set_acl("r2", [make_ace("deny", "u", "use")])
        wait(lambda: not check_access(proxy_address, "u"))

        # There is no node for proxy role "r3". By default we allow access to
        # proxies with unknown role.
        set("//sys/http_proxies/" + proxy + "/@role", "r3")
        wait(lambda: check_access(proxy_address, "u"))

        # Set proxy role back to "r2". User "u" still can't use it.
        set("//sys/http_proxies/" + proxy + "/@role", "r2")
        wait(lambda: not check_access(proxy_address, "u"))

        # Disable access checker via dynamic config. Now "u" can use proxy.
        set("//sys/http_proxies/@config", {"access_checker": {"enabled": False}})
        wait(lambda: check_access(proxy_address, "u"))

        # Enable access checker via dynamic config. And "u" is banned again.
        set("//sys/http_proxies/@config", {"access_checker": {"enabled": True}})
        wait(lambda: not check_access(proxy_address, "u"))


@pytest.mark.enabled_multidaemon
class TestHttpProxyAccessChecker(HttpProxyAccessCheckerTestBase):
    ENABLE_MULTIDAEMON = True

    def create_proxy_role_namespace(self):
        create("http_proxy_role_map", "//sys/http_proxy_roles")

    def create_proxy_role(self, name):
        create_proxy_role(name, "http")

    def set_acl(self, role, acl):
        set(f"//sys/http_proxy_roles/{role}/@acl", acl)


@pytest.mark.enabled_multidaemon
class TestHttpProxyAccessCheckerWithAco(HttpProxyAccessCheckerTestBase):
    ENABLE_MULTIDAEMON = True

    @classmethod
    def setup_class(cls):
        cls.DELTA_HTTP_PROXY_CONFIG["access_checker"].update({
            "use_access_control_objects": True,
            "path_prefix": "//sys/access_control_object_namespaces/http_proxy_roles",
        })
        super().setup_class()

    def create_proxy_role_namespace(self):
        create_access_control_object_namespace("http_proxy_roles")

    def create_proxy_role(self, name):
        create_access_control_object(name, "http_proxy_roles")

    def set_acl(self, role, acl):
        set(f"//sys/access_control_object_namespaces/http_proxy_roles/{role}/principal/@acl", acl)


@pytest.mark.enabled_multidaemon
class TestHttpProxyRoleFromStaticConfig(HttpProxyTestBase):
    DELTA_HTTP_PROXY_CONFIG = {
        "role": "ab"
    }

    ENABLE_MULTIDAEMON = True

    @authors("nadya73")
    def test_role(self):
        proxy = ls("//sys/http_proxies")[0]
        role = get("//sys/http_proxies/" + proxy + "/@role")
        assert role == "ab"

    @authors("nadya73")
    def test_hosts(self):
        proxy = ls("//sys/http_proxies")[0]

        def get_yson(url):
            return yson.loads(requests.get(url).content)

        assert get_yson(self._get_proxy_address() + "/hosts") == []
        assert get_yson(self._get_proxy_address() + "/hosts?role=data") == []
        assert get_yson(self._get_proxy_address() + "/hosts?role=ab") == [proxy]


@pytest.mark.enabled_multidaemon
class TestHttpProxyAuth(HttpProxyTestBase):
    ENABLE_MULTIDAEMON = True

    @classmethod
    def setup_class(cls):
        cls.DELTA_HTTP_PROXY_CONFIG["auth"] = {
            "enable_authentication": True,
        }
        super(TestHttpProxyAuth, cls).setup_class()

    def create_user_with_token(self, user):
        create_user(user)
        token, _ = issue_token(user)
        return token

    @authors("ermolovd")
    def test_get_current_user(self):
        token = self.create_user_with_token("test_get_current_user")

        url = f"{self._get_proxy_address()}/api/v4/get_current_user"
        headers = {
            "Authorization": f"OAuth {token}",
        }
        rsp = requests.get(url, headers=headers)
        assert rsp.status_code == 200, f"Proxy returned {rsp.status_code} response: {rsp.content}"
        assert rsp.json()["user"] == "test_get_current_user"

    @authors("mpereskokova")
    def test_access_on_behalf_of_the_user(self):
        proxy_address = self._get_proxy_address()

        def check_access(proxy_address, path="/", status_code=200, error_code=None, user=None, token=None):
            url = "{}/api/v4/get?path={}".format(proxy_address, path)
            headers = {}
            if user:
                headers["X-YT-User-Name"] = user
            if token:
                headers["Authorization"] = "OAuth " + token
            rsp = requests.get(url, headers=headers)

            if error_code is not None :
                assert json.loads(rsp.content)["code"] == error_code

            assert rsp.status_code in [200, 400, 401]
            return rsp.status_code == status_code

        yql_agent_token = self.create_user_with_token("yql_agent")
        test_user_token = self.create_user_with_token("test_user")

        wait(lambda: check_access(proxy_address, status_code=200, token=yql_agent_token))
        wait(lambda: check_access(proxy_address, status_code=200, token=yql_agent_token, user="test_user"))
        wait(lambda: check_access(proxy_address, status_code=401, token=test_user_token, user="yql_agent"))

        node = "//tmp/dir"
        create(
            "map_node",
            node,
            attributes={"acl": [
                {"action": "deny", "subjects": ["yql_agent"], "permissions": ["read"]},
                {"action": "allow", "subjects": ["test_user"], "permissions": ["read"]},
            ]},
        )
        wait(lambda: check_access(proxy_address, path=node, status_code=400, error_code=yt_error_codes.AuthorizationErrorCode, token=yql_agent_token))
        wait(lambda: check_access(proxy_address, path=node, status_code=200, token=yql_agent_token, user="test_user"))

        # Now the user should be allowed to impersonate others.
        add_member("test_user", "superusers")
        # While yql_agent is not, even though it is explicitly whitelisted.
        set("//sys/users/yql_agent/@banned", True)
        wait(lambda: check_access(proxy_address, status_code=401, token=yql_agent_token, user="test_user"))
        wait(lambda: check_access(proxy_address, status_code=200, token=test_user_token, user="root"))

        # Superusers can be banned too!
        set("//sys/users/test_user/@banned", True)
        wait(lambda: check_access(proxy_address, status_code=401, token=test_user_token, user="root"))


@pytest.mark.enabled_multidaemon
class TestHttpProxyFraming(HttpProxyTestBase):
    SUSPENDING_TABLE = "//tmp/suspending_table"
    DELAY_BEFORE_COMMAND = 10 * 1000
    KEEP_ALIVE_PERIOD = 0.1 * 1000
    # CLIENT_TIMEOUT << DELAY_BEFORE_COMMAND to catch framing bugs
    # CLIENT_TIMEOUT >> KEEP_ALIVE_PERIOD to avoid false test failures
    CLIENT_TIMEOUT = 1 * 1000
    DELTA_HTTP_PROXY_CONFIG = {
        "api": {
            "testing": {
                "delay_before_command": {
                    "get": {
                        "delay": DELAY_BEFORE_COMMAND,
                        "parameter_path": "/path",
                        "substring": SUSPENDING_TABLE,
                    },
                    "get_table_columnar_statistics": {
                        "delay": DELAY_BEFORE_COMMAND,
                        "parameter_path": "/paths/0",
                        "substring": SUSPENDING_TABLE,
                    },
                    "partition_tables": {
                        "delay": DELAY_BEFORE_COMMAND,
                        "parameter_path": "/paths/0",
                        "substring": SUSPENDING_TABLE,
                    },
                    "read_table": {
                        "delay": DELAY_BEFORE_COMMAND,
                        "parameter_path": "/path",
                        "substring": SUSPENDING_TABLE,
                    },
                }
            },
        },
    }

    FRAME_TAG_TO_NAME = {
        0x01: "data",
        0x02: "keep_alive",
    }
    ENABLE_MULTIDAEMON = True

    @classmethod
    def _unframe_content(cls, content):
        result = []
        i = 0
        while i < len(content):
            tag = content[i]
            i += 1
            assert tag in cls.FRAME_TAG_TO_NAME
            name = cls.FRAME_TAG_TO_NAME[tag]
            if name == "data":
                (length,) = struct.unpack("<i", content[i:i + 4])
                i += 4
                assert i + length <= len(content)
                frame = content[i:i + length]
                i += length
            else:
                frame = None
            result.append((name, frame))
        return result

    def setup_method(self, method):
        super(TestHttpProxyFraming, self).setup_method(method)
        monitoring_port = self.Env.configs["http_proxy"][0]["monitoring_port"]
        config_url = "http://localhost:{}/orchid/dynamic_config_manager/effective_config".format(monitoring_port)
        set(
            "//sys/http_proxies/@config",
            {"api": {"framing": {"keep_alive_period": self.KEEP_ALIVE_PERIOD}}},
        )

        @wait_no_assert
        def config_updated():
            config = requests.get(config_url).json()
            value = config \
                .get("api", {}) \
                .get("framing", {}) \
                .get("keep_alive_period", 0.0)
            assert value == self.KEEP_ALIVE_PERIOD

    def _execute_command(self, http_method, command_name, params, extra_headers={}):
        headers = {
            "X-YT-Accept-Framing": "1",
            "X-YT-Parameters": yson.dumps(params),
            "X-YT-Header-Format": "<format=text>yson",
            "X-YT-Output-Format": "<format=text>yson",
        }
        headers.update(extra_headers)
        start = datetime.now()
        rsp = requests.request(
            http_method,
            "{}/api/v4/{}".format(self._get_proxy_address(), command_name),
            headers=headers,
            timeout=timedelta(milliseconds=self.CLIENT_TIMEOUT).total_seconds(),
        )
        try_parse_yt_error_headers(rsp)

        assert "X-YT-Framing" in rsp.headers
        unframed_content = self._unframe_content(rsp.content)
        try_parse_yt_error_trailers(rsp)

        keep_alive_frame_count = sum(name == "keep_alive" for name, frame in unframed_content)
        assert keep_alive_frame_count >= self.DELAY_BEFORE_COMMAND / self.KEEP_ALIVE_PERIOD - 7
        assert datetime.now() - start > timedelta(milliseconds=self.DELAY_BEFORE_COMMAND)
        actual_response = b""
        for name, frame in unframed_content:
            if name == "data":
                actual_response += frame
        return actual_response

    @authors("levysotsky")
    def test_get(self):
        create("table", self.SUSPENDING_TABLE)
        write_table(self.SUSPENDING_TABLE, [{"foo": "bar"}, {"foo": "baz"}, {"foo": "qux"}])
        attribute_value = {"x": 1, "y": "qux"}
        set(self.SUSPENDING_TABLE + "/@foobar", attribute_value)
        params = {
            "path": self.SUSPENDING_TABLE + "/@foobar",
        }
        response = self._execute_command("GET", "get", params)
        response_yson = yson.loads(response)
        assert {"value": attribute_value} == response_yson

    @authors("levysotsky")
    @pytest.mark.parametrize("use_compression", [True, False])
    def test_read_table(self, use_compression):
        create("table", self.SUSPENDING_TABLE)
        rows_chunk = [{"foo": "bar"}, {"foo": "baz"}, {"foo": "qux"}] * 100
        chunk_count = 100
        for _ in range(chunk_count):
            write_table("<append=%true>" + self.SUSPENDING_TABLE, rows_chunk)
        params = {
            "path": self.SUSPENDING_TABLE,
            "output_format": "yson",
        }
        headers = {}
        if not use_compression:
            headers["Accept-Encoding"] = "identity"
        response = self._execute_command("GET", "read_table", params, extra_headers=headers)
        response_yson = list(yson.loads(response, yson_type="list_fragment"))
        assert rows_chunk * chunk_count == response_yson

        params = {
            "path": self.SUSPENDING_TABLE + "-nonexistent",
            "output_format": "yson",
        }
        with pytest.raises(YtResponseError):
            self._execute_command("GET", "read_table", params, extra_headers=headers)

    @authors("levysotsky", "denvid")
    def test_get_table_columnar_statistics(self):
        create("table", self.SUSPENDING_TABLE)
        write_table(self.SUSPENDING_TABLE, [{"column_1": 1, "column_2": "foo"}])
        params = {
            "paths": [self.SUSPENDING_TABLE + "{column_1}"],
        }
        response = self._execute_command("GET", "get_table_columnar_statistics", params)
        statistics = yson.loads(response)
        assert len(statistics) == 1
        assert statistics[0].get("column_data_weights") == {"column_1": 8}
        assert statistics[0].get("column_min_values") == {"column_1": 1}
        assert statistics[0].get("column_max_values") == {"column_1": 1}
        assert statistics[0].get("column_non_null_value_counts") == {"column_1": 1}
        assert statistics[0].get("chunk_row_count") == 1
        assert statistics[0].get("legacy_chunk_row_count") == 0

        remove(self.SUSPENDING_TABLE)
        with pytest.raises(YtResponseError):
            self._execute_command("GET", "get_table_columnar_statistics", params)

    @authors("galtsev")
    @pytest.mark.parametrize("encoding", ["identity", "gzip", "deflate", "brotly"])
    def test_partition_tables(self, encoding):
        create("table", self.SUSPENDING_TABLE)
        write_table(self.SUSPENDING_TABLE, [{"column_1": 1, "column_2": "foo"}])
        params = {
            "paths": [self.SUSPENDING_TABLE + "{column_1}"],
            "data_weight_per_partition": 1 << 30,
        }
        headers = {
            "Accept-Encoding": encoding,
        }
        response = self._execute_command("GET", "partition_tables", params, extra_headers=headers)
        partitioning_response = yson.loads(response)
        assert len(partitioning_response) == 1
        assert "partitions" in partitioning_response
        assert "table_ranges" in partitioning_response["partitions"][0]

        remove(self.SUSPENDING_TABLE)
        with pytest.raises(YtResponseError):
            self._execute_command("GET", "get_table_columnar_statistics", params, extra_headers=headers)


@pytest.mark.enabled_multidaemon
class TestHttpProxyJobShellAudit(HttpProxyTestBase):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1
    ENABLE_HTTP_PROXY = True
    NUM_HTTP_PROXIES = 1

    DELTA_NODE_CONFIG = {
        "exec_node": {
            "job_proxy": {
                "test_poll_job_shell": True,
            },
        },
    }

    USE_PORTO = True
    ENABLE_MULTIDAEMON = True

    @classmethod
    def modify_http_proxy_config(cls, config, cluster_index, multidaemon_config, proxy_index):
        if "logging" in config:
            config["logging"]["flush_period"] = 100
            config["logging"]["rules"].append(
                {
                    "min_level": "info",
                    "writers": ["job_shell"],
                    "include_categories": ["JobShell"],
                    "message_format": "structured",
                }
            )
            config["logging"]["writers"]["job_shell"] = {
                "type": "file",
                "file_name": os.path.join(cls.path_to_run, f"logs/job-shell-{proxy_index}.json.log"),
                "accepted_message_format": "structured",
            }

        multidaemon_config["logging"]["flush_period"] = 100
        multidaemon_config["logging"]["rules"].append(
            {
                "min_level": "info",
                "writers": ["job_shell"],
                "include_categories": ["JobShell"],
                "message_format": "structured",
            }
        )
        multidaemon_config["logging"]["writers"]["job_shell"] = {
            "type": "file",
            "file_name": os.path.join(cls.path_to_run, "logs/job-shell-0.json.log"),
            "accepted_message_format": "structured",
        }

    @authors("psushin")
    def test_job_shell_logging(self):
        create("table", "//tmp/t_in", force=True)
        write_table("//tmp/t_in", [{"a": "b"}])
        op = map(
            track=False,
            label="poll_job_shell",
            in_="//tmp/t_in",
            command=with_breakpoint("cat > /dev/null; BREAKPOINT"))

        jobs = wait_breakpoint()

        headers = {
            "X-YT-Parameters": yson.dumps({
                "job_id": jobs[0],
                "parameters": {
                    "operation": "spawn",
                    "term": "screen-256color",
                    "height": 50,
                    "width": 132,
                },
            }),
            "X-YT-Header-Format": "<format=text>yson",
            "X-YT-Output-Format": "<format=text>yson",
        }

        rsp = requests.request(
            "post",
            "{}/api/v4/{}".format(self._get_proxy_address(), "poll_job_shell"),
            headers=headers
        )
        try_parse_yt_error_headers(rsp)

        result = yson.loads(rsp.content)
        shell_id = result["result"]["shell_id"]

        path = os.path.join(self.path_to_run, "logs/job-shell-0.json.log")

        def has_shell_start_record():
            if not os.path.exists(path):
                return False
            with open(path) as f:
                for line_json in f.readlines():
                    if json.loads(line_json.strip())["shell_id"] == shell_id:
                        return True
            return False

        wait(lambda: has_shell_start_record(), iter=120, sleep_backoff=1.0)

        op.abort()


@pytest.mark.enabled_multidaemon
class TestHttpProxyFormatConfig(HttpProxyTestBase, _TestProxyFormatConfigBase):
    NUM_TEST_PARTITIONS = 6
    ENABLE_MULTIDAEMON = True

    def setup_method(self, method):
        super(TestHttpProxyFormatConfig, self).setup_method(method)
        monitoring_port = self.Env.configs["http_proxy"][0]["monitoring_port"]
        config_url = "http://localhost:{}/orchid/dynamic_config_manager/effective_config".format(monitoring_port)
        set("//sys/http_proxies/@config", {"api": {"formats": self.FORMAT_CONFIG}})

        def config_updated():
            config = requests.get(config_url).json()
            return config \
                .get("api", {}) \
                .get("formats", {}) \
                .get("yamred_dsv", {}) \
                .get("user_overrides", {}) \
                .get("good_user", False)

        wait(config_updated)

    def _execute_command(self, http_method, command_name, params, user="root", data=None,
                         header_format="yson", input_format="yson", output_format="yson",
                         api_version="v4"):
        headers = {
            "X-YT-Parameters": self._write_format(header_format, params, tabular=False),
            "X-YT-Header-Format": self._write_format("yson", header_format, tabular=False),
            "X-YT-Output-Format": self._write_format(header_format, output_format, tabular=False),
            "X-YT-Input-Format": self._write_format(header_format, input_format, tabular=False),
            "X-YT-User-Name": user,
        }
        rsp = requests.request(
            http_method,
            "{address}/api/{api_version}/{command_name}".format(
                address=self._get_proxy_address(),
                api_version=api_version,
                command_name=command_name,
            ),
            headers=headers,
            data=data,
        )
        try_parse_yt_error_headers(rsp)
        return rsp

    def _do_run_operation(self, op_type, spec, user, use_start_op):
        params = {"spec": spec, "operation_type": op_type}
        command = "start_operation" if use_start_op else op_type
        api_version = "v4" if use_start_op else "v3"
        rsp = self._execute_command(
            "post",
            command,
            params,
            user=user,
            api_version=api_version,
        )
        result = yson.loads(rsp.content)
        op = Operation()
        op.id = result if api_version == "v3" else result["operation_id"]
        return op

    def _test_format_enable_general(self, format, user, enable):
        create("table", "//tmp/t", force=True)
        content = self.TABLE_CONTENT_TWO_COLUMNS
        write_table("//tmp/t", content)

        manager = self._get_context_manager(enable)

        with manager():
            rsp = self._execute_command(
                "get",
                "read_table",
                {"path": "//tmp/t"},
                user=user,
                output_format=format,
            )
            assert self._parse_format(format, rsp.content) == content

        with manager():
            rsp = self._execute_command(
                "get",
                "read_table",
                {"path": "//tmp/t", "output_format": format},
                user=user,
            )
            assert self._parse_format(format, rsp.content) == content

        with manager():
            self._execute_command(
                "put",
                "write_table",
                {"path": "//tmp/t"},
                user=user,
                input_format=format,
                data=self._write_format(format, content),
            )
            assert read_table("//tmp/t") == content

        with manager():
            self._execute_command(
                "put",
                "write_table",
                {"path": "//tmp/t", "input_format": format},
                user=user,
                data=self._write_format(format, content),
            )
            assert read_table("//tmp/t") == content

    def _test_format_enable(self, format, user, enable):
        self._test_format_enable_general(format, user, enable)
        self._test_format_enable_operations(format, user, enable)

    @authors("levysotsky")
    def test_header_format_enable(self):
        create_user("no_json_user")
        manager = self._get_context_manager(enable=False)
        with manager():
            self._execute_command(
                "get",
                "get",
                {"path": "//tmp/t/@type"},
                user="no_json_user",
                header_format="json",
                output_format=self.YSON,
            )

    def _test_format_defaults_cypress(self, format, user, content, expected_content):
        set("//tmp/@attr", content, force=True)

        rsp = self._execute_command(
            "GET",
            "get",
            {"path": "//tmp/@attr"},
            user=user,
            output_format=format,
        )
        actual_content = rsp.content
        assert actual_content == expected_content

        rsp = self._execute_command(
            "GET",
            "get",
            {"path": "//tmp/@attr", "output_format": format},
            user=user,
        )
        actual_content = rsp.content
        assert actual_content == expected_content

    def _test_format_defaults(self, format, user, content, expected_format):
        assert str(expected_format) == "yson"
        yson_format = expected_format.attributes.get("format", "text")
        expected_content_cypress = yson.dumps({"value": content}, yson_format=yson_format)
        expected_content_operation = yson.dumps(content, yson_format=yson_format, yson_type="list_fragment")
        self._test_format_defaults_cypress(format, user, content, expected_content_cypress)
        self._test_format_defaults_operations(format, user, content, expected_content_operation)


@pytest.mark.enabled_multidaemon
class TestHttpProxyBuildSnapshotBase(HttpProxyTestBase):
    NUM_SCHEDULERS = 0

    DELTA_MASTER_CONFIG = {
        "hydra_manager": {
            "build_snapshot_delay": 10000,
        },
    }

    DELTA_HTTP_PROXY_CONFIG = {
        "coordinator": {
            "heartbeat_interval": 100,
            "cypress_timeout": 50,
            "death_age": 500,
        },
    }

    ENABLE_MULTIDAEMON = True

    def _build_snapshot(self, set_read_only):
        params = {
            "cell_id": self.Env.configs["master"][0]["primary_master"]["cell_id"],
            "set_read_only": set_read_only,
            "wait_for_snapshot_completion": False,
        }
        headers = {
            "X-YT-Parameters": yson.dumps(params),
            "X-YT-Header-Format": "<format=text>yson",
            "X-YT-Output-Format": "<format=text>yson",
        }

        rsp = requests.post(self._get_build_snapshot_url(), headers=headers)
        rsp.raise_for_status()

        return yson.loads(rsp.content)["snapshot_id"]

    def _master_exit_read_only(self):
        params = {
            "retry": True,
        }
        headers = {
            "X-YT-Parameters": yson.dumps(params),
            "X-YT-Header-Format": "<format=text>yson",
            "X-YT-Output-Format": "<format=text>yson",
        }

        rsp = requests.post(self._get_master_exit_read_only_url(), headers=headers)
        rsp.raise_for_status()

    def _build_snapshot_and_check(self, set_read_only):
        master = self._get_master_address()

        def _check_not_building_snapshot():
            monitoring = self._get_hydra_monitoring(master)
            return "building_snapshot" in monitoring and not monitoring["building_snapshot"]
        wait(_check_not_building_snapshot)

        snapshot_id = self._build_snapshot(set_read_only)

        def _check_building_snapshot():
            monitoring = self._get_hydra_monitoring(master)
            return "building_snapshot" in monitoring and monitoring["building_snapshot"]
        wait(_check_building_snapshot)

        def _check_snapshot_built():
            monitoring = self._get_hydra_monitoring(master)
            return "building_snapshot" in monitoring and "last_snapshot_id" in monitoring and \
                   not monitoring["building_snapshot"] and monitoring["last_snapshot_id"] == snapshot_id
        wait(_check_snapshot_built)

        if set_read_only:
            def _check_read_only():
                monitoring = self._get_hydra_monitoring(master)
                return monitoring["read_only"]
            wait(_check_read_only)


@pytest.mark.enabled_multidaemon
class TestHttpProxyBuildSnapshotNoReadonly(TestHttpProxyBuildSnapshotBase):
    ENABLE_MULTIDAEMON = True

    @authors("babenko")
    def test_no_read_only(self):
        self._build_snapshot_and_check(False)


class TestHttpProxyBuildSnapshotReadonly(TestHttpProxyBuildSnapshotBase):
    ENABLE_MULTIDAEMON = False  # There are component restarts.

    def _check_no_read_only(self):
        monitoring = self._get_hydra_monitoring()
        return "active" in monitoring and monitoring["active"] and not monitoring["read_only"]

    @authors("babenko")
    def test_read_only(self):
        self._build_snapshot_and_check(True)

        with Restarter(self.Env, MASTERS_SERVICE):
            pass

        self._master_exit_read_only()

        wait(lambda: self._check_no_read_only())

    @authors("aleksandra-zh")
    def test_read_only_proxy_availability(self):
        def check_proxies_online():
            rsp = requests.get(self._get_proxy_address() + "/hosts/all")
            rsp.raise_for_status()

            proxies = rsp.json()
            assert len(proxies) > 0
            for proxy in proxies:
                assert not proxy["dead"]

            rsp = requests.get(self._get_proxy_address() + "/internal/discover_versions/v2")
            rsp.raise_for_status()

            versions = rsp.json()
            assert "details" in versions
            assert all(instance["state"] == "online" for instance in versions["details"] if instance["type"] == "http_proxy")

        self._build_snapshot_and_check(True)

        time.sleep(2)

        rsp = requests.get(self._get_proxy_address() + "/ping")
        rsp.raise_for_status()

        check_proxies_online()

        with Restarter(self.Env, MASTERS_SERVICE):
            pass

        time.sleep(2)
        check_proxies_online()

        self._master_exit_read_only()

        wait(lambda: self._check_no_read_only())


@pytest.mark.skipif(is_asan_build(), reason="Memory allocation is not reported under ASAN")
@pytest.mark.enabled_multidaemon
class TestHttpProxyHeapUsageStatisticsBase(HttpProxyTestBase):
    NUM_HTTP_PROXIES = 1
    ENABLE_MULTIDAEMON = True

    def enable_allocation_tags(self, proxy):
        set(f"//sys/{proxy}/@config", {
            "api": {
                "enable_allocation_tags": True,
            },
        })
        wait(lambda: get(f"//sys/{proxy}/@config")["api"]["enable_allocation_tags"])

    def check_memory_usage(self, memory_usage, command):
        tags = ["user", "command"]
        if not memory_usage or len(memory_usage) < len(tags):
            return False

        for tag in tags:
            assert tag in memory_usage

        assert self.USER in memory_usage["user"].keys()
        assert command in memory_usage["command"].keys()

        assert memory_usage["user"][self.USER] > 5 * 1024 ** 2
        assert memory_usage["command"][command] > 5 * 1024 ** 2

        return True


@pytest.mark.skipif(is_asan_build(), reason="Memory allocation is not reported under ASAN")
class TestHttpProxyHeapUsageStatistics(TestHttpProxyHeapUsageStatisticsBase):
    DELTA_HTTP_PROXY_CONFIG = {
        "heap_profiler": {
            "snapshot_update_period": 50,
        },
        "api": {
            "testing": {
                "heap_profiler": {
                    "allocation_size": 50 * 1024 ** 2,
                    "allocation_release_delay" : 120 * 1000,
                },
            },
        },
    }
    ENABLE_MULTIDAEMON = False  # Checks profiling.

    @authors("ni-stoiko")
    @pytest.mark.timeout(120)
    def test_heap_usage_statistics(self):
        http_proxies = ls("//sys/http_proxies")
        assert len(http_proxies) == 1
        http_proxy_agent_orchid = f"//sys/http_proxies/{http_proxies[0]}/orchid/http_proxy"

        self.enable_allocation_tags("http_proxies")
        time.sleep(1)

        create("table", self.PATH)
        write_table(f"<append=%true>{self.PATH}", [{"key": "x"}])

        self._execute_command("GET", "read_table")
        time.sleep(1)
        wait(lambda: self.check_memory_usage(
            get(http_proxy_agent_orchid + "/heap_usage"),
            "read_table"))

    @authors("ni-stoiko")
    @pytest.mark.timeout(120)
    def test_heap_usage_gauges(self):
        self.enable_allocation_tags("http_proxies")
        time.sleep(1)

        http_proxies = ls("//sys/http_proxies")
        assert len(http_proxies) == 1

        create("table", self.PATH)
        write_table(f"<append=%true>{self.PATH}", [{"key": "x"}])

        self._execute_command("GET", "read_table")

        profiler = profiler_factory().at_http_proxy(http_proxies[0])
        command_memory_usage_gauge = profiler.gauge("memory/heap_usage/command")
        user_memory_usage_gauge = profiler.gauge("memory/heap_usage/user")

        def check(statistics, tag, memory=5 * 1024 ** 2):
            for stat in statistics:
                if stat["tags"] and tag == stat["tags"]:
                    return stat["value"] > memory
            return False

        wait(lambda: check(command_memory_usage_gauge.get_all(), {"command": "read_table"}))
        wait(lambda: check(user_memory_usage_gauge.get_all(), {"user": self.USER}))


@pytest.mark.skipif(is_asan_build(), reason="Memory allocation is not reported under ASAN")
@pytest.mark.enabled_multidaemon
class TestHttpProxyNullApiTestingOptions(TestHttpProxyHeapUsageStatisticsBase):
    DELTA_HTTP_PROXY_CONFIG = {
        "heap_profiler": {
            "snapshot_update_period": 50,
        },
    }
    ENABLE_MULTIDAEMON = True

    @authors("ni-stoiko")
    def test_null_api_testing_options(self):
        self.enable_allocation_tags("http_proxies")
        create("table", self.PATH)
        write_table(f"<append=%true>{self.PATH}", [{"key": "x"}])
        self._execute_command("GET", "read_table")


class TestHttpProxySignaturesBase(HttpProxyTestBase):
    DELTA_HTTP_PROXY_CONFIG = {
        "signature_components": {
            "validation": {
                "cypress_key_reader": dict(),
            },
            "generation": {
                "cypress_key_writer": dict(),
                "key_rotator": dict(),
                "generator": dict(),
            },
        },
    }

    # NB(pavook): to avoid owner collision.
    NUM_HTTP_PROXIES = 1

    OWNERS_PATH = "//sys/public_keys/by_owner"


def deep_update(source: dict[Any, Any], overrides: dict[Any, Any]) -> dict[Any, Any]:
    """
    Update a nested dictionary.
    """
    result = source.copy()
    for key, value in overrides.items():
        if isinstance(value, dict) and value:
            returned = deep_update(source.get(key, {}), value)
            result[key] = returned
        else:
            result[key] = value
    return result


class TestHttpProxySignatures(TestHttpProxySignaturesBase):
    @classmethod
    def setup_class(cls):
        super(TestHttpProxySignatures, cls).setup_class()
        # Wait until the keys are generated.
        # With very low probability, if the command gets through between the transaction
        # commit and actual key emplace, this won't be enough: the key still won't be available.
        # But this probability should be neglectable, as some network packeting should happen before the signatures
        # even start generating.
        wait(lambda: len(ls(cls.OWNERS_PATH)) > 0)

    @authors("ermolovd")
    def test_partition_tables_with_modified_cookie(self):
        create_user("user1")
        create_user("user2")

        table = "//tmp/table"
        create("table", table)
        write_table(table, [{"a": "123456789"}] * 1024)
        partitions = self._execute_command("GET", "partition_tables", {
            "paths": [table],
            "data_weight_per_partition": 1024 * 10 // 3,
            "enable_cookies": True,
            "username": "user1",
        }, user="user1")
        partitions = yson.loads(partitions.content)
        cookie = yson.get_bytes(partitions["partitions"][0]["cookie"])

        self._execute_command("GET", "read_table_partition", {"cookie": cookie}, user="user1")

        assert b"user1" in cookie
        cookie = cookie.replace(b"user1", b"user2")
        with raises_yt_error("Signature validation failed"):
            self._execute_command("GET", "read_table_partition", {"cookie": cookie}, user="user2")

    @authors("pavook")
    @pytest.mark.timeout(60)
    def test_distributed_write_with_modified_cookie(self):
        table_path = "//tmp/distributed_table"
        modified_path = "//tmp/pwntributed_table"
        assert len(table_path) == len(modified_path)

        create("table", table_path, schema=[{"name": "v1", "type": "string", "sort_order": "ascending"}])
        res = self._execute_command(
            "POST",
            "start_distributed_write_session",
            {"path": table_path})

        assert bytes(table_path, "utf-8") in res.content
        yson_res = yson.loads(res.content.replace(bytes(table_path, "utf-8"), bytes(modified_path, "utf-8")))
        session = yson_res["session"]
        with raises_yt_error("Signature validation failed"):
            self._execute_command("POST", "finish_distributed_write_session", {"session": session, "results": []})

    @authors("pavook")
    def disabled_test_shuffle_with_modified_handle(self):  # TODO(pavook): enable when shuffle is supported via HTTP.
        account = "intermediate"
        modified_account = "pwnedmediate"
        assert len(account) == len(modified_account)

        parent_transaction = start_transaction(timeout=60000)
        res = self._execute_command("POST", "start_shuffle", {
            "account": account,
            "partition_count": 1,
            "parent_transaction_id": parent_transaction,
        })

        shuffle_handle = res.content
        assert bytes(account, "utf-8") in shuffle_handle
        modified_handle = shuffle_handle.replace(bytes(account, "utf-8"), bytes(modified_account, "utf-8"))
        rows = [{"key": 0, "value": 1}]
        yson_rows = yson.dumps(rows, yson_type="list_fragment")

        self._execute_command("POST", "write_shuffle_data", {"signed_shuffle_handle": shuffle_handle}, data=yson_rows)
        with raises_yt_error("Signature validation failed"):
            self._execute_command("POST", "write_shuffle_data", {"signed_shuffle_handle": modified_handle}, data=yson_rows)

        assert yson.loads(self._execute_command("GET", "read_shuffle_data").content, yson_type="list_fragment") == yson_rows
        with raises_yt_error("Signature validation failed"):
            self._execute_command("GET", "read_shuffle_data").content


class TestHttpProxySignaturesKeyRotation(TestHttpProxySignaturesBase):
    DELTA_HTTP_PROXY_CONFIG = deep_update(TestHttpProxySignaturesBase.DELTA_HTTP_PROXY_CONFIG, {
        "signature_components": {
            "generation": {
                "key_rotator": {
                    "key_rotation_options": {
                        "period": "200ms",
                    },
                },
            },
        },
    })

    @authors("pavook")
    @pytest.mark.timeout(60)
    def test_public_key_rotates(self):
        wait(lambda: ls(self.OWNERS_PATH))
        owner = ls(self.OWNERS_PATH)[0]
        wait(lambda: len(ls(f"{self.OWNERS_PATH}/{owner}")) > 1)

    @authors("pavook")
    def test_dynamic_config(self):
        wait(lambda: ls(self.OWNERS_PATH))
        new_path = "//tmp/dynamic_test_public_keys"
        create("map_node", new_path)
        set(
            "//sys/http_proxies/@config",
            deep_update(self.DELTA_HTTP_PROXY_CONFIG, {
                "signature_components": {
                    "generation": {
                        "cypress_key_writer": {
                            "path": new_path,
                        },
                        "key_rotator": {
                            "key_rotation_options": {
                                "period": "5s",
                            },
                        },
                    },
                },
            }))
        wait(lambda: ls(new_path))


class TestHttpsProxy(HttpProxyTestBase):
    ENABLE_TLS = True

    DELTA_HTTP_PROXY_CONFIG = {
        "https_server": {
            "credentials": {
                "update_period": 1000,
            },
        },
    }

    ENABLE_MULTIDAEMON = False  # Some problems with TLS.

    @authors("khlebnikov")
    def test_ping_https(self):
        # verification against system ca bundle: /etc/ssl/certs/ca-certificates.crt
        with pytest.raises(requests.exceptions.SSLError):
            rsp = requests.get(self._get_https_proxy_url() + "/ping")
            rsp.raise_for_status()

    @authors("khlebnikov")
    def test_ping_https_verify_false(self):
        rsp = requests.get(self._get_https_proxy_url() + "/ping", verify=False)
        rsp.raise_for_status()

    @authors("khlebnikov")
    def test_ping_verify_ca(self):
        rsp = requests.get(self._get_https_proxy_url() + "/ping", verify=self._get_https_ca_cert())
        rsp.raise_for_status()

    @authors("khlebnikov")
    def test_certificate_update(self):
        proxy_cert, proxy_cert_key = self._get_proxy_cert_path()

        old_fingerprint = get_certificate_fingerprint(proxy_cert)
        print_debug("Old certificate fingerprint: {}", old_fingerprint)

        def current_fingerprint():
            return get_certificate_fingerprint(cert_content=get_server_certificate(self._get_https_proxy_address()))

        assert current_fingerprint() == old_fingerprint

        create_certificate(
            ca_cert=self.Env.yt_config.public_ca_cert,
            ca_cert_key=self.Env.yt_config.public_ca_cert_key,
            cert=proxy_cert,
            cert_key=proxy_cert_key,
            names=[self.Env.yt_config.fqdn, self.Env.yt_config.cluster_name],
        )

        new_fingerprint = get_certificate_fingerprint(proxy_cert)
        print_debug("New certificate fingerprint: {}", new_fingerprint)

        assert new_fingerprint != old_fingerprint

        wait(lambda: current_fingerprint() == new_fingerprint)

        rsp = requests.get(self._get_https_proxy_url() + "/ping", verify=self._get_https_ca_cert())
        rsp.raise_for_status()


@pytest.mark.enabled_multidaemon
class TestHttpProxyDiscovery(YTEnvSetup):
    ENABLE_HTTP_PROXY = True
    ENABLE_RPC_PROXY = True
    ENABLE_CHYT_HTTP_PROXIES = True

    NUM_HTTP_PROXIES = 2
    NUM_RPC_PROXIES = 1

    ENABLE_MULTIDAEMON = True

    def setup_method(self, method):
        super(TestHttpProxyDiscovery, self).setup_method(method)
        driver_config = deepcopy(self.Env.configs["driver"])
        driver_config["api_version"] = 4
        self.driver = Driver(driver_config)

    @classmethod
    def modify_http_proxy_config(cls, config, cluster_index, multidaemon_config, proxy_index):
        config["addresses"] = [["default", "localhost"], ["fastbone", "fb-localhost"]]

    @authors("nadya73")
    def test_addresses(self):
        proxy = ls("//sys/http_proxies")[0]

        addresses = get("//sys/http_proxies/" + proxy + "/@addresses")
        for listener in ("http", "chyt_http"):
            assert listener in addresses
            assert "default" in addresses[listener]
            assert "fastbone" in addresses[listener]
        assert proxy == addresses["http"]["default"]

    @authors("nadya73")
    def test_discovery(self):
        configured_proxy_addresses = sorted(self.Env.get_http_proxy_addresses())
        configured_proxy_addresses_fb = ["fb-" + address for address in configured_proxy_addresses]
        configured_monitoring_addresses = sorted(self.Env.get_http_proxy_monitoring_addresses())

        for test_name, request, expected_addresses in [
            (
                "defaults", {}, configured_proxy_addresses,
            ),
            (
                "explicit_address_type",
                {},
                configured_proxy_addresses,
            ),
            (
                "explicit_params",
                {"network_name": "default"},
                configured_proxy_addresses,
            ),
            (
                "explicit_params",
                {"network_name": "fastbone"},
                configured_proxy_addresses_fb,
            ),
            (
                "monitoring_addresses",
                {"address_type": "monitoring_http", "network_name": "default"},
                configured_monitoring_addresses,
            ),
        ]:
            proxies = discover_proxies(type_="http", driver=self.driver, **request)
            assert sorted(proxies) == expected_addresses, test_name

    @authors("nadya73")
    def test_invalid_address_type(self):
        with pytest.raises(YtError):
            discover_proxies(type_="http", driver=self.driver, address_type="invalid")

    @authors("nadya73")
    def test_invalid_network_name(self):
        proxies = discover_proxies(type_="http", driver=self.driver, network_name="invalid")
        assert len(proxies) == 0

    @authors("epsilond1")
    def test_chyt_http_address(self):
        proxies = discover_proxies(type_="http", driver=self.driver, address_type="chyt_http")
        assert len(proxies) != 0


@pytest.mark.enabled_multidaemon
class TestHttpProxyDiscoveryRoleFromStaticConfig(YTEnvSetup):
    ENABLE_HTTP_PROXY = True
    ENABLE_RPC_PROXY = True

    NUM_HTTP_PROXIES = 2
    NUM_RPC_PROXIES = 1

    DELTA_HTTP_PROXY_CONFIG = {
        "role": "ab",
    }

    ENABLE_MULTIDAEMON = True

    def setup_method(self, method):
        super(TestHttpProxyDiscoveryRoleFromStaticConfig, self).setup_method(method)
        driver_config = deepcopy(self.Env.configs["driver"])
        driver_config["api_version"] = 4
        self.driver = Driver(driver_config)

    @authors("nadya73")
    def test_role(self):
        proxy = ls("//sys/http_proxies")[0]

        role = get("//sys/http_proxies/" + proxy + "/@role")
        assert role == "ab"

    @authors("nadya73")
    def test_discovery(self):
        configured_proxy_addresses = sorted(self.Env.get_http_proxy_addresses())

        proxies = discover_proxies(type_="http", driver=self.driver, **{})
        assert sorted(proxies) == []

        proxies = discover_proxies(type_="http", driver=self.driver, **{"role": "ab"})
        assert sorted(proxies) == configured_proxy_addresses


@pytest.mark.enabled_multidaemon
class TestHttpProxyDiscoveryBalancers(YTEnvSetup):
    ENABLE_HTTP_PROXY = True
    ENABLE_RPC_PROXY = True

    NUM_HTTP_PROXIES = 2
    NUM_RPC_PROXIES = 1

    ENABLE_MULTIDAEMON = True

    def setup_method(self, method):
        super(TestHttpProxyDiscoveryBalancers, self).setup_method(method)
        driver_config = deepcopy(self.Env.configs["driver"])
        driver_config["api_version"] = 4
        driver_config["proxy_discovery_cache"] = {
            "expire_after_access_time": 0,
        }
        self.driver = Driver(driver_config)

    @authors("nadya73")
    def test_discovery(self):
        configured_proxy_addresses = sorted(self.Env.get_http_proxy_addresses())
        configured_monitoring_addresses = sorted(self.Env.get_http_proxy_monitoring_addresses())

        balancer_first = 'default-balancer.com:9013'
        balancer_second = 'default-balancer-2.com:9013'
        set(
            "//sys/http_proxies/@balancers",
            {
                "data": {
                    "http": {
                        "default": [balancer_first, balancer_second]
                    }
                }
            },
        )

        proxies = discover_proxies(type_="http", driver=self.driver, **{})
        assert proxies == [balancer_first, balancer_second]

        proxies = discover_proxies(type_="http", driver=self.driver, **{"ignore_balancers": True})
        assert sorted(proxies) == configured_proxy_addresses

        proxies = discover_proxies(type_="http", driver=self.driver, **{"address_type": "monitoring_http"})
        assert sorted(proxies) == configured_monitoring_addresses

    @authors("nadya73")
    def test_invalid_address_type(self):
        with pytest.raises(YtError):
            discover_proxies(type_="http", driver=self.driver, address_type="invalid")

    @authors("nadya73")
    def test_invalid_network_name(self):
        proxies = discover_proxies(type_="http", driver=self.driver, network_name="invalid")
        assert len(proxies) == 0
