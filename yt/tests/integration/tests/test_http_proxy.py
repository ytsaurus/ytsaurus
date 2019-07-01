from yt_env_setup import YTEnvSetup, wait
from yt_commands import *

import yt.packages.requests as requests
import json

##################################################################

class TestHttpProxy(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 5
    NUM_SCHEDULERS = 1
    ENABLE_PROXY = True
    ENABLE_RPC_PROXY = True
    NUM_SECONDARY_MASTER_CELLS = 2
    NUM_HTTP_PROXIES = 1
    NUM_RPC_PROXIES = 2

    DELTA_PROXY_CONFIG = {
        "coordination": {
            "heartbeat_interval": 100,
        },
    }

    def proxy_address(self):
        return "http://" + self.Env.get_proxy_address()

    def test_ping(self):
        rsp = requests.get(self.proxy_address() + "/ping")
        rsp.raise_for_status()

    def test_version(self):
        rsp = requests.get(self.proxy_address() + "/version")
        rsp.raise_for_status()

    def test_service(self):
        service = requests.get(self.proxy_address() + "/service").json()
        assert "version" in service
        assert "start_time" in service

    def test_hosts(self):
        proxy = ls("//sys/proxies")[0]

        def get_yson(url):
            return yson.loads(requests.get(url).text)

        assert [proxy] == get_yson(self.proxy_address() + "/hosts")
        assert [proxy] == get_yson(self.proxy_address() + "/hosts?role=data")
        assert [] == get_yson(self.proxy_address() + "/hosts?role=control")

        set("//sys/proxies/" + proxy + "/@role", "control")

        # Wait until the proxy entry will be updated on the coordinator.
        wait(lambda: [] == get_yson(self.proxy_address() + "/hosts"))
        assert [] == get_yson(self.proxy_address() + "/hosts?role=data")
        assert [proxy] == get_yson(self.proxy_address() + "/hosts?role=control")

        hosts = requests.get(self.proxy_address() + "/hosts/all").json()
        assert len(hosts) == 1
        assert not hosts[0]["banned"]

    def test_supported_api_versions(self):
        assert ["v3", "v4"] == requests.get(self.proxy_address() + "/api").json()

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

    def test_discover_versions_v2(self):
        rsp = requests.get(self.proxy_address() + "/internal/discover_versions/v2")
        rsp.raise_for_status()

        versions = rsp.json()
        assert "details" in versions
        assert "summary" in versions

    def test_dynamic_config(self):
        monitoring_port = self.Env.configs["http_proxy"][0]["monitoring_port"]
        config_url = "http://localhost:{}/orchid/coordinator/dynamic_config".format(monitoring_port)

        set("//sys/proxies/@config", {"tracing": {"user_sample_rate": {"prime": 1.0}}})

        def config_updated():
            config = requests.get(config_url).json()
            return "prime" in config["tracing"]["user_sample_rate"]

        wait(config_updated)

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