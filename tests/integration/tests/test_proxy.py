from yt_env_setup import YTEnvSetup, wait
from yt_commands import *

import yt.packages.requests as requests

##################################################################

class TestHttpProxy(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 5
    NUM_SCHEDULERS = 1
    ENABLE_PROXY = True
    ENABLE_RPC_PROXY = True
    NUM_SECONDARY_MASTER_CELLS = 2

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
        assert 1 == len(requests.get(self.proxy_address() + "/hosts").text.split())

        hosts = requests.get(self.proxy_address() + "/hosts/all").json()
        assert len(hosts) == 1
        assert not hosts[0]["banned"]

    def test_supported_api_versions(self):
        assert ["v2", "v3", "v4"] == requests.get(self.proxy_address() + "/api").json()

    def test_discover_versions(self):
        rsp = requests.get(self.proxy_address() + "/api/v3/_discover_versions").json()
        service = requests.get(self.proxy_address() + "/service").json()
        assert len(rsp["primary_masters"]) == 3
        assert len(rsp["secondary_masters"]) == 2 * 3
        assert len(rsp["nodes"]) == 5
        assert len(rsp["schedulers"]) == 1
        for component in rsp:
            for instant in rsp[component]:
                assert "version" in rsp[component][instant]
                assert "start_time" in rsp[component][instant]
                assert "version" in service
