from .helpers import TEST_DIR

from yt.wrapper.common import parse_bool

import yt.yson as yson

from yt.packages.six.moves import reload_module

import yt.wrapper as yt

import time
import pytest

def test_heavy_proxies():
    from yt.wrapper.http_driver import HeavyProxyProvider
    from socket import error as SocketError

    yt.config["proxy"]["number_of_top_proxies_for_random_choice"] = 1

    provider = HeavyProxyProvider(None)
    provider._get_light_proxy = lambda: "light_proxy"
    provider._discover_heavy_proxies = lambda: ["host1", "host2"]
    assert provider() == "host1"

    provider.on_error_occured(SocketError())
    assert provider() == "host2"

    provider.on_error_occured(SocketError())
    assert provider() == "host1"

    provider._discover_heavy_proxies = lambda: ["host2", "host3"]
    yt.wrapper.config["proxy"]["proxy_ban_timeout"] = 10
    time.sleep(0.01)

    assert provider() == "host2"

    provider._discover_heavy_proxies = lambda: []
    assert provider() == "light_proxy"

def teardown_function(function):
    import yt.wrapper.http_driver
    import yt.wrapper.config
    reload_module(yt.wrapper.http_driver)
    reload_module(yt.wrapper.config)

@pytest.mark.usefixtures("yt_env")
def test_process_params():
    schema = yson.YsonList([{"name": "k", "type": "int64", "sort_order": "ascending"}])
    schema.attributes["unique_keys"] = True

    table = TEST_DIR + "/dynamic_table"
    yt.create_table(table, attributes={"schema": schema})
    assert parse_bool(yt.get(table + "/@schema/@unique_keys"))
