from yt_env_setup import YTEnvSetup, wait, Restarter, MASTERS_SERVICE
from yt_commands import *
from yt_helpers import Metric

import yt.packages.requests as requests

import json
import struct

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

    def proxy_address(self):
        return "http://" + self.Env.get_proxy_address()

    def build_snapshot_url(self):
        return self.proxy_address() + "/api/v4/build_snapshot"

    def discover_master(self):
        return ls("//sys/primary_masters")[0]

    def hydra_monitor(self, master=None):
        if master is None:
            master = self.discover_master()
        return get("//sys/primary_masters/{}/orchid/monitoring/hydra".format(master))


class TestHttpProxy(HttpProxyTestBase):
    @authors("prime")
    def test_ping(self):
        rsp = requests.get(self.proxy_address() + "/ping")
        rsp.raise_for_status()

    @authors("prime")
    def test_version(self):
        rsp = requests.get(self.proxy_address() + "/version")
        rsp.raise_for_status()

    @authors("prime")
    def test_service(self):
        service = requests.get(self.proxy_address() + "/service").json()
        assert "version" in service
        assert "start_time" in service

    @authors("levysotsky")
    def test_hosts(self):
        proxy = ls("//sys/proxies")[0]

        def get_yson(url):
            return yson.loads(requests.get(url).text)

        assert [proxy] == get_yson(self.proxy_address() + "/hosts")
        assert [proxy] == get_yson(self.proxy_address() + "/hosts?role=data")
        assert [] == get_yson(self.proxy_address() + "/hosts?role=control")

        def make_request_and_check_metric(metric):
            url = self.proxy_address() + "/api/v3/get?path=//sys/@config"
            requests.get(url)
            return metric.update().get(verbose=True) > 0

        data_metric = Metric.at_proxy(
            proxy,
            "http_proxy/http_code_count",
            with_tags={"http_code": "200", "proxy_role" : "data"},
            aggr_method="last")

        wait(lambda: make_request_and_check_metric(data_metric))

        set("//sys/proxies/" + proxy + "/@role", "control")

        # Wait until the proxy entry will be updated on the coordinator.
        wait(lambda: [] == get_yson(self.proxy_address() + "/hosts"))
        assert [] == get_yson(self.proxy_address() + "/hosts?role=data")
        assert [proxy] == get_yson(self.proxy_address() + "/hosts?role=control")

        control_metric = Metric.at_proxy(
            proxy,
            "http_proxy/http_code_count",
            with_tags={"http_code": "200", "proxy_role" : "control"},
            aggr_method="last")

        wait(lambda: make_request_and_check_metric(control_metric))

        hosts = requests.get(self.proxy_address() + "/hosts/all").json()
        assert len(hosts) == 1
        assert not hosts[0]["banned"]

    @authors("prime")
    def test_supported_api_versions(self):
        assert ["v3", "v4"] == requests.get(self.proxy_address() + "/api").json()

    @authors("prime")
    def test_discover_versions(self):
        rsp = requests.get(self.proxy_address() + "/internal/discover_versions").json()
        service = requests.get(self.proxy_address() + "/service").json()

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
        rsp = requests.get(self.proxy_address() + "/internal/discover_versions/v2")
        rsp.raise_for_status()

        versions = rsp.json()
        assert "details" in versions
        assert "summary" in versions

    @authors("prime")
    def test_cache_control(self):
        rsp = requests.get(self.proxy_address() + "/api/v4/get?path=//@")
        rsp.raise_for_status()

        assert rsp.headers['cache-control'] == 'no-cache'

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
        set("//sys/proxies/test_http_proxy/@liveness", {"updated_at" : "2010-06-24T11:23:30.156098Z"})
        set("//sys/proxies/test_http_proxy/@start_time", "2009-06-19T16:39:02.171721Z")
        set("//sys/proxies/test_http_proxy/@version", "19.5.30948-master-ya~c9facaeaca")
        create("map_node", "//sys/rpc_proxies/test_rpc_proxy")
        set("//sys/rpc_proxies/test_rpc_proxy/@start_time", "2009-06-19T16:39:02.171721Z")
        set("//sys/rpc_proxies/test_rpc_proxy/@version", "19.5.30948-master-ya~c9facaeaca")

        rsp = requests.get(self.proxy_address() + "/internal/discover_versions/v2")
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
        requests.get(self.proxy_address() + "/api/v2/get")


class TestHttpProxyFraming(HttpProxyTestBase):
    SUSPENDING_ATTRIBUTE = "magic_suspending_attribute"
    DELTA_PROXY_CONFIG = {
        "api": {
            "testing": {
                "delay_inside_get": {
                    "delay": 10 * 1000,
                    "path": "//tmp/t1/@" + SUSPENDING_ATTRIBUTE,
                }
            },
            "framing_keep_alive_period": 1 * 1000,
        },
    }

    FRAME_TAG_TO_NAME = {
        0x01 : "data",
        0x02 : "keep_alive",
    }

    @classmethod
    def _unframe_content(cls, content):
        result = []
        i = 0
        while i  < len(content):
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

    @authors("levysotsky")
    def test_framing(self):
        create("table", "//tmp/t1")
        write_table("//tmp/t1", [{"foo": "bar"}, {"foo": "baz"}, {"foo": "qux"}])
        attribute_value = {"x": 1, "y": "qux"}
        set("//tmp/t1/@" + self.SUSPENDING_ATTRIBUTE, attribute_value)
        params = {
            "path": "//tmp/t1/@" + self.SUSPENDING_ATTRIBUTE,
        }
        headers = {
            "X-YT-Accept-Framing": "1",
            'X-YT-Parameters': yson.dumps(params),
            'X-YT-Header-Format': "<format=text>yson",
            'X-YT-Output-Format': "<format=text>yson"
        }
        rsp = requests.get(self.proxy_address() + "/api/v4/get", headers=headers)
        rsp.raise_for_status()
        assert "X-YT-Framing" in rsp.headers
        unframed_content = self._unframe_content(rsp.content)
        keep_alive_frame_count = sum(name == "keep_alive" for name, frame in unframed_content)
        assert keep_alive_frame_count >= 5
        actual_response = b""
        for name, frame in unframed_content:
            if name == "data":
                actual_response += frame
        response_yson = yson.loads(actual_response)
        assert {"value": attribute_value} == response_yson


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
            "wait_for_snapshot_completion": False
        }
        headers = {
            'X-YT-Parameters': yson.dumps(params),
            'X-YT-Header-Format': "<format=text>yson",
            'X-YT-Output-Format': "<format=text>yson"
        }

        rsp = requests.post(self.build_snapshot_url(), headers=headers)
        rsp.raise_for_status()

        return yson.loads(rsp.text)["snapshot_id"]

    def _wait_for_snapshot_state(self, building_snapshot=None, last_snapshot_id=None):
        master = self.discover_master()

        def predicate():
            monitor = self.hydra_monitor(master)
            flag_building = building_snapshot is None or monitor["building_snapshot"] == building_snapshot
            flag_last = last_snapshot_id is None or monitor["last_snapshot_id"] == last_snapshot_id
            return flag_building and flag_last

        wait(predicate, "Expected state is not reached")


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

        assert self.hydra_monitor()["read_only"]

        with Restarter(self.Env, MASTERS_SERVICE):
            pass
