import yt.wrapper.driver
import yt.wrapper.config

import time

def test_heavy_proxies():
    from yt.wrapper.http_driver import HeavyProxyProvider
    from socket import error as SocketError

    yt.wrapper.config["proxy"]["number_of_top_proxies_for_random_choice"] = 1

    provider = HeavyProxyProvider(None)
    provider._discover_heavy_proxies = lambda: ["host1", "host2"]
    assert provider() == "host1"

    provider.on_error_occured(SocketError())
    assert provider() == "host2"

    provider.on_error_occured(SocketError())
    assert provider() == "host1"

    provider._discover_heavy_proxies = lambda: ["host2", "host3"]
    yt.wrapper.config.HOST_BAN_PERIOD = 10
    time.sleep(0.01)

    assert provider() == "host2"

def teardown_function(function):
    reload(yt.wrapper.http_driver)
    reload(yt.wrapper.config)
