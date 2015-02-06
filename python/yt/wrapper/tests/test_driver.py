import yt.wrapper.driver
import yt.wrapper.config

import time

def test_heavy_proxies():
    yt.wrapper.http_driver.get_hosts = lambda client: ["host1", "host2"]
    assert yt.wrapper.http_driver.get_heavy_proxy(client=None) == "host1"

    yt.wrapper.http_driver.ban_host("host1", client=None)
    assert yt.wrapper.http_driver.get_heavy_proxy(client=None) == "host2"
    
    yt.wrapper.http_driver.ban_host("host2", client=None)
    assert yt.wrapper.http_driver.get_heavy_proxy(client=None) == "host1"

    yt.wrapper.http_driver.get_hosts = lambda client: ["host2", "host3"]
    yt.wrapper.config.HOST_BAN_PERIOD = 10
    time.sleep(0.01)
    
    assert yt.wrapper.http_driver.get_heavy_proxy(client=None) == "host2"

def teardown_function(function):
    reload(yt.wrapper.http_driver)
    reload(yt.wrapper.config)
