import urllib2
import time
import json

import pytest

from yt_env_setup import YTEnvSetup, wait
from yt_commands import *

##################################################################

class TestMonitoring(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 1
    NUM_SCHEDULERS = 1

    def get_json(self, port, query):
        return json.loads(urllib2.urlopen("http://localhost:{}/orchid{}".format(port, query)).read())
    
    @pytest.mark.parametrize("component", ["master", "scheduler", "node"])
    def test_component_http_monitoring(self, component):
        http_port = self.Env.configs[component][0]["monitoring_port"]

        root_orchid = self.get_json(http_port, "")
        assert "config" in root_orchid

        config_orchid = self.get_json(http_port, "/config")
        assert "monitoring_port" in config_orchid

        def monitoring_orchid_ready():
            try:
                self.get_json(http_port, "/profiling/logging/backlog_events")
                return True
            except urllib2.HTTPError:
                return False

        wait(monitoring_orchid_ready)

        profiling_orchid = self.get_json(http_port,
            "/profiling/logging/backlog_events?from_time={}".format(int(time.time()) * 1000000))

        assert len(profiling_orchid) > 0

        with pytest.raises(urllib2.HTTPError):
            self.get_json(http_port, "/profiling/logging/backlog_events?from_time=abc")
