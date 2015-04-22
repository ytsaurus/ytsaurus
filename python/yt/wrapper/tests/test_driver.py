import yt.wrapper.driver
import yt.wrapper.config

import time

def test_heavy_proxies():
    yt.wrapper.driver.get_hosts = lambda client: ["host1", "host2"]
    assert yt.wrapper.driver.get_heavy_proxy(client=None) == "host1"

    yt.wrapper.driver.ban_host("host1", client=None)
    assert yt.wrapper.driver.get_heavy_proxy(client=None) == "host2"
    
    yt.wrapper.driver.ban_host("host2", client=None)
    assert yt.wrapper.driver.get_heavy_proxy(client=None) == "host1"

    yt.wrapper.driver.get_hosts = lambda client: ["host2", "host3"]
    yt.wrapper.config._HOST_BAN_PERIOD = 10
    time.sleep(0.01)
    
    assert yt.wrapper.driver.get_heavy_proxy(client=None) == "host2"
