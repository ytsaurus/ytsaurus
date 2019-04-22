from yt_env_setup import YTEnvSetup, wait
from yt_commands import *

import yt.packages.requests as requests

##################################################################

class TestHttpProxy(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 5
    NUM_SCHEDULERS = 1
    ENABLE_PROXY = True
    ENABLE_RPC_PROXY = True
    NUM_SECONDARY_MASTER_CELLS = 2

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

        set("//sys/proxies/@config", {"tracing_user_sample_probability": {"prime": 1.0}})

        def config_updated():
            config = requests.get(config_url).json()
            return "prime" in config["tracing_user_sample_probability"]

        wait(config_updated)
