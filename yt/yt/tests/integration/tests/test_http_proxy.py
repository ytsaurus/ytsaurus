from proxy_format_config import _TestProxyFormatConfigBase

from yt_env_setup import YTEnvSetup, wait, Restarter, MASTERS_SERVICE
from yt_commands import *
from yt_helpers import Metric

from yt.common import YtResponseError

import yt.packages.requests as requests

import json
import struct
from datetime import datetime, timedelta


def try_parse_yt_error(rsp):
    if "X-YT-Error" in rsp.headers:
        raise YtResponseError(json.loads(rsp.headers.get("X-YT-Error")))
    rsp.raise_for_status()

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

    DELTA_PROXY_CONFIG = {
        "coordinator": {
            "heartbeat_interval": 100,
        },
        "api": {
            "force_tracing": True,
        },
    }

    def _get_proxy_address(self):
        return "http://" + self.Env.get_proxy_address()

    def _get_build_snapshot_url(self):
        return self._get_proxy_address() + "/api/v4/build_snapshot"

    def _get_master_address(self):
        return ls("//sys/primary_masters")[0]

    def _get_hydra_monitoring(self, master=None):
        if master is None:
            master = self._get_master_address()
        return get(
            "//sys/primary_masters/{}/orchid/monitoring/hydra".format(master),
            default={},
        )


class TestHttpProxy(HttpProxyTestBase):
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
        proxy = ls("//sys/proxies")[0]

        def get_yson(url):
            return yson.loads(requests.get(url).text)

        assert [proxy] == get_yson(self._get_proxy_address() + "/hosts")
        assert [proxy] == get_yson(self._get_proxy_address() + "/hosts?role=data")
        assert [] == get_yson(self._get_proxy_address() + "/hosts?role=control")

        def make_request_and_check_metric(metric):
            url = self._get_proxy_address() + "/api/v3/get?path=//sys/@config"
            requests.get(url)
            return metric.update().get(verbose=True) > 0

        data_metric = Metric.at_proxy(
            proxy,
            "http_proxy/http_code_count",
            with_tags={"http_code": "200", "proxy_role": "data"},
            aggr_method="last",
        )

        wait(lambda: make_request_and_check_metric(data_metric))

        set("//sys/proxies/" + proxy + "/@role", "control")

        # Wait until the proxy entry will be updated on the coordinator.
        wait(lambda: [] == get_yson(self._get_proxy_address() + "/hosts"))
        assert [] == get_yson(self._get_proxy_address() + "/hosts?role=data")
        assert [proxy] == get_yson(self._get_proxy_address() + "/hosts?role=control")

        control_metric = Metric.at_proxy(
            proxy,
            "http_proxy/http_code_count",
            with_tags={"http_code": "200", "proxy_role": "control"},
            aggr_method="last",
        )

        wait(lambda: make_request_and_check_metric(control_metric))

        hosts = requests.get(self._get_proxy_address() + "/hosts/all").json()
        assert len(hosts) == 1
        assert not hosts[0]["banned"]

    @authors("prime")
    def test_supported_api_versions(self):
        assert ["v3", "v4"] == requests.get(self._get_proxy_address() + "/api").json()

    @authors("prime")
    def test_discover_versions(self):
        rsp = requests.get(self._get_proxy_address() + "/internal/discover_versions").json()
        service = requests.get(self._get_proxy_address() + "/service").json()

        assert len(rsp["primary_masters"]) == 1
        assert len(rsp["secondary_masters"]) == 2
        assert len(rsp["nodes"]) == 5
        assert len(rsp["schedulers"]) == 1
        assert len(rsp["controller_agents"]) == 1
        assert len(rsp["http_proxies"]) == 1
        assert len(rsp["rpc_proxies"]) == 2
        for component in rsp:
            for instant in rsp[component]:
                assert "version" in rsp[component][instant]
                assert "start_time" in rsp[component][instant]
                assert "version" in service

    @authors("prime")
    def test_discover_versions_v2(self):
        rsp = requests.get(self._get_proxy_address() + "/internal/discover_versions/v2")
        rsp.raise_for_status()

        versions = rsp.json()
        assert "details" in versions
        assert "summary" in versions

    @authors("prime")
    def test_cache_control(self):
        rsp = requests.get(self._get_proxy_address() + "/api/v4/get?path=//@")
        rsp.raise_for_status()

        assert rsp.headers["cache-control"] == "no-store"

    @authors("prime")
    def test_dynamic_config(self):
        monitoring_port = self.Env.configs["http_proxy"][0]["monitoring_port"]
        config_url = "http://localhost:{}/orchid/coordinator/dynamic_config".format(monitoring_port)

        set("//sys/proxies/@config", {"tracing": {"user_sample_rate": {"prime": 1.0}}})

        def config_updated():
            config = requests.get(config_url).json()
            return "prime" in config["tracing"]["user_sample_rate"]

        wait(config_updated)

    @authors("greatkorn")
    def test_kill_nodes(self):
        create("map_node", "//sys/proxies/test_http_proxy")
        set(
            "//sys/proxies/test_http_proxy/@liveness",
            {"updated_at": "2010-06-24T11:23:30.156098Z"},
        )
        set("//sys/proxies/test_http_proxy/@start_time", "2009-06-19T16:39:02.171721Z")
        set("//sys/proxies/test_http_proxy/@version", "19.5.30948-master-ya~c9facaeaca")
        create("map_node", "//sys/rpc_proxies/test_rpc_proxy")
        set(
            "//sys/rpc_proxies/test_rpc_proxy/@start_time",
            "2009-06-19T16:39:02.171721Z",
        )
        set(
            "//sys/rpc_proxies/test_rpc_proxy/@version",
            "19.5.30948-master-ya~c9facaeaca",
        )

        rsp = requests.get(self._get_proxy_address() + "/internal/discover_versions/v2")
        rsp.raise_for_status()

        status = rsp.json()
        for proxy in status["details"]:
            if proxy["address"] in ("test_http_proxy", "test_rpc_proxy"):
                assert proxy.get("state") == "offline"

    @authors("greatkorn")
    def test_structured_logs(self):
        client = self.Env.create_client()
        client.list("//sys")

        log_path = self.path_to_run + "/logs/http-proxy-0.json.log"
        wait(lambda: os.path.exists(log_path), "Cannot find proxy's structured log")

        def logs_updated():
            flag = False
            with open(log_path, "r") as fd:
                for line in fd:
                    line_json = json.loads(line)
                    if line_json.get("path") == "//sys":
                        flag |= line_json["command"] == "list"
            return flag

        wait(logs_updated)

    @authors("greatkorn")
    def test_fail_logging(self):
        requests.get(self._get_proxy_address() + "/api/v2/get")


class TestHttpProxyFraming(HttpProxyTestBase):
    SUSPENDING_TABLE = "//tmp/suspending_table"
    DELAY_BEFORE_COMMAND = 10 * 1000
    KEEP_ALIVE_PERIOD = 1 * 1000
    DELTA_PROXY_CONFIG = {
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

    @classmethod
    def _unframe_content(cls, content):
        result = []
        i = 0
        while i < len(content):
            tag = ord(content[i])
            i += 1
            assert tag in cls.FRAME_TAG_TO_NAME
            name = cls.FRAME_TAG_TO_NAME[tag]
            if name == "data":
                (length,) = struct.unpack("<i", content[i : i + 4])
                i += 4
                assert i + length <= len(content)
                frame = content[i : i + length]
                i += length
            else:
                frame = None
            result.append((name, frame))
        return result

    def setup(self):
        monitoring_port = self.Env.configs["http_proxy"][0]["monitoring_port"]
        config_url = "http://localhost:{}/orchid/coordinator/dynamic_config".format(monitoring_port)
        set(
            "//sys/proxies/@config",
            {"framing": {"keep_alive_period": self.KEEP_ALIVE_PERIOD}},
        )
        wait(lambda: requests.get(config_url).json()["framing"]["keep_alive_period"] == self.KEEP_ALIVE_PERIOD)

    def _execute_command(self, http_method, command_name, params):
        headers = {
            "X-YT-Accept-Framing": "1",
            "X-YT-Parameters": yson.dumps(params),
            "X-YT-Header-Format": "<format=text>yson",
            "X-YT-Output-Format": "<format=text>yson",
        }
        start = datetime.now()
        rsp = requests.request(
            http_method,
            "{}/api/v4/{}".format(self._get_proxy_address(), command_name),
            headers=headers,
        )
        rsp.raise_for_status()
        assert "X-YT-Framing" in rsp.headers
        unframed_content = self._unframe_content(rsp.content)
        keep_alive_frame_count = sum(name == "keep_alive" for name, frame in unframed_content)
        assert keep_alive_frame_count >= self.DELAY_BEFORE_COMMAND / self.KEEP_ALIVE_PERIOD - 3
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
    def test_read_table(self):
        create("table", self.SUSPENDING_TABLE)
        rows = [{"foo": "bar"}, {"foo": "baz"}, {"foo": "qux"}]
        write_table(self.SUSPENDING_TABLE, rows)
        params = {
            "path": self.SUSPENDING_TABLE,
            "output_format": "yson",
        }
        response = self._execute_command("GET", "read_table", params)
        response_yson = list(yson.loads(response, yson_type="list_fragment"))
        assert rows == response_yson

    @authors("levysotsky")
    def test_get_table_columnar_statistics(self):
        create("table", self.SUSPENDING_TABLE)
        write_table(self.SUSPENDING_TABLE, [{"column_1": 1, "column_2": "foo"}])
        params = {
            "paths": [self.SUSPENDING_TABLE + "{column_1}"],
        }
        response = self._execute_command("GET", "get_table_columnar_statistics", params)
        statistics = yson.loads(response)
        assert len(statistics) == 1
        assert "column_data_weights" in statistics[0]


class TestHttpProxyFormatConfig(HttpProxyTestBase, _TestProxyFormatConfigBase):
    def setup(self):
        monitoring_port = self.Env.configs["http_proxy"][0]["monitoring_port"]
        config_url = "http://localhost:{}/orchid/coordinator/dynamic_config".format(monitoring_port)
        set("//sys/proxies/@config", {"formats": self.FORMAT_CONFIG})

        def config_updated():
            config = requests.get(config_url).json()
            return config \
                .get("formats", {}) \
                .get("yamred_dsv", {}) \
                .get("user_overrides", {}) \
                .get("good_user", False)

        wait(config_updated)

    def _execute_command(self, http_method, command_name, params, user="root", data=None,
                         header_format="yson", input_format="yson", output_format="yson",
                         api_version="v4"):
        headers = {
            "X-YT-Parameters": yson.dumps(params),
            "X-YT-Header-Format": yson.dumps(header_format),
            "X-YT-Output-Format": yson.dumps(output_format),
            "X-YT-Input-Format": yson.dumps(input_format),
            "X-YT-Testing-User-Name": user,
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
        try_parse_yt_error(rsp)
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
            self._execute_command(
                "put",
                "write_table",
                {"path": "//tmp/t"},
                user=user,
                input_format=format,
                data=self._write_format(format, content),
            )
            assert read_table("//tmp/t") == content

    def _test_format_enable(self, format, user, enable):
        self._test_format_enable_general(format, user, enable)
        self._test_format_enable_operations(format, user, enable)

    def _test_format_defaults_cypress(self, format, user, content, expected_content):
        set("//tmp/list_node", content)

        rsp = self._execute_command(
            "GET",
            "get",
            {"path": "//tmp/list_node"},
            user=user,
            output_format=format,
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


class TestHttpProxyBuildSnapshotBase(HttpProxyTestBase):
    NUM_SCHEDULERS = 0
    DELTA_MASTER_CONFIG = {
        "hydra_manager": {
            "build_snapshot_delay": 10000,
        },
    }

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

        return yson.loads(rsp.text)["snapshot_id"]

    def _wait_for_snapshot_state(self, building_snapshot=None, last_snapshot_id=None):
        master = self._get_master_address()

        def predicate():
            monitoring = self._get_hydra_monitoring(master)
            return (building_snapshot is None or monitoring.get("building_snapshot", None) == building_snapshot) and (
                last_snapshot_id is None or monitoring.get("last_snapshot_id", None) == last_snapshot_id
            )

        wait(predicate)


class TestHttpProxyBuildSnapshotNoReadonly(TestHttpProxyBuildSnapshotBase):
    @authors("babenko")
    def test_no_read_only(self):
        self._wait_for_snapshot_state(False, -1)
        snapshot_id = self._build_snapshot(False)
        self._wait_for_snapshot_state(True, -1)
        self._wait_for_snapshot_state(False, snapshot_id)


class TestHttpProxyBuildSnapshotReadonly(TestHttpProxyBuildSnapshotBase):
    @authors("babenko")
    def test_read_only(self):
        self._wait_for_snapshot_state(False, -1)
        self._build_snapshot(True)
        self._wait_for_snapshot_state(True, -1)
        self._wait_for_snapshot_state(False)

        wait(lambda: self._get_hydra_monitoring().get("read_only", None))

        with Restarter(self.Env, MASTERS_SERVICE):
            pass

        wait(lambda: not self._get_hydra_monitoring().get("read_only", None))
