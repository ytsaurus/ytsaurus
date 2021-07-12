from .conftest import authors
from .helpers import TEST_DIR, get_test_file_path, get_python, wait

from yt.wrapper.common import update_inplace

import yt.yson as yson

from yt.packages.six.moves import reload_module

import yt.wrapper as yt
import yt.subprocess_wrapper as subprocess

import os
import sys
import time
import pytest
import signal
from copy import deepcopy

@authors("ignat")
def test_heavy_proxies():
    from yt.wrapper.http_driver import HeavyProxyProvider
    from socket import error as SocketError

    config = deepcopy(yt.config.config)
    try:
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
        yt.config["proxy"]["proxy_ban_timeout"] = 10
        time.sleep(0.01)

        assert provider() == "host2"

        provider._discover_heavy_proxies = lambda: []
        assert provider() == "light_proxy"
    finally:
        reload_module(yt.http_driver)
        reload_module(yt.config)
        update_inplace(yt.config.config, config)

@authors("ignat")
@pytest.mark.usefixtures("yt_env")
def test_sanitize_structure():
    schema = yson.YsonList([{"name": "k", "type": "int64", "sort_order": "ascending"}])
    schema.attributes["unique_keys"] = True

    table = TEST_DIR + "/dynamic_table"
    yt.create("table", table, attributes={"schema": schema})
    assert yt.get(table + "/@schema/@unique_keys")

@authors("asaitgalin")
@pytest.mark.usefixtures("yt_env_with_rpc")
@pytest.mark.only_backend("rpc")
def test_catching_sigint(yt_env_with_rpc):
    if yt.config["backend"] != "rpc":
        pytest.skip()

    driver_config_path = yt_env_with_rpc.env.config_paths["rpc_driver"]
    driver_logging_config_path = yt_env_with_rpc.env.config_paths["driver_logging"]
    binary = get_test_file_path("driver_catch_sigint.py")

    process = subprocess.Popen(
        [get_python(), binary, driver_config_path, driver_logging_config_path],
        stderr=sys.stderr)

    time.sleep(2)
    if process.poll() is not None:
        assert False, "Process finished early"

    wait(lambda: yt.exists("//tmp/test_file"))
    wait(lambda: yt.get("//tmp/test_file/@uncompressed_data_size") == 50 * 1000 * 1000)
    time.sleep(3)

    os.kill(process.pid, signal.SIGINT)
    try:
        process.wait(5)
    except:
        os.kill(process.pid, signal.SIGKILL)
        assert False, "Process hanged up for more than 5 seconds on SIGINT"

    binary = get_test_file_path("driver_read_request_catch_sigint.py")
    process = subprocess.Popen([get_python(), binary])

    time.sleep(3)
    os.kill(process.pid, signal.SIGINT)
    try:
        process.wait(5)
    except:
        os.kill(process.pid, signal.SIGKILL)
        assert False, "Process hanged up for more than 5 seconds on SIGINT"
