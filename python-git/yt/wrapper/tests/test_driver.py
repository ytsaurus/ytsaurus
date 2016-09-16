import yt.wrapper.driver
import yt.wrapper.config

from yt.packages.six.moves import reload_module

import time

def test_heavy_proxies():
    from yt.wrapper.http_driver import HeavyProxyProvider
    from socket import error as SocketError

    yt.wrapper.config["proxy"]["number_of_top_proxies_for_random_choice"] = 1

    provider = HeavyProxyProvider(None, "light_proxy")
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

    provider._discover_heavy_proxies = lambda: []
    assert provider() == "light_proxy"

def teardown_function(function):
    reload_module(yt.wrapper.http_driver)
    reload_module(yt.wrapper.config)
