# coding=utf-8

from yt_odin.logserver import FULLY_AVAILABLE_STATE, PARTIALLY_AVAILABLE_STATE, UNAVAILABLE_STATE
from yt_odin.test_helpers import make_check_dir, configure_and_run_checks


def test_discovery_servers(yt_env):
    yt_client = yt_env.yt_client

    proxy_url = yt_client.config["proxy"]["url"]
    checks_path = make_check_dir("discovery")

    yt_client.create("map_node", "//sys/discovery_servers")
    storage = configure_and_run_checks(proxy_url, checks_path)
    assert abs(storage.get_service_states("discovery")[-1] - UNAVAILABLE_STATE) <= 0.001

    yt_client.create("map_node", "//sys/discovery_servers/d1")
    storage = configure_and_run_checks(proxy_url, checks_path)
    assert abs(storage.get_service_states("discovery")[-1] - UNAVAILABLE_STATE) <= 0.001

    yt_client.create("map_node", "//sys/discovery_servers/d1/orchid")
    storage = configure_and_run_checks(proxy_url, checks_path)
    assert abs(storage.get_service_states("discovery")[-1] - FULLY_AVAILABLE_STATE) <= 0.001

    yt_client.create("map_node", "//sys/discovery_servers/d2")
    yt_client.create("map_node", "//sys/discovery_servers/d3")
    storage = configure_and_run_checks(proxy_url, checks_path)
    assert abs(storage.get_service_states("discovery")[-1] - UNAVAILABLE_STATE) <= 0.001

    yt_client.create("map_node", "//sys/discovery_servers/d2/orchid")
    storage = configure_and_run_checks(proxy_url, checks_path)
    assert abs(storage.get_service_states("discovery")[-1] - PARTIALLY_AVAILABLE_STATE) <= 0.001

    yt_client.create("map_node", "//sys/discovery_servers/d3/orchid")
    storage = configure_and_run_checks(proxy_url, checks_path)
    assert abs(storage.get_service_states("discovery")[-1] - FULLY_AVAILABLE_STATE) <= 0.001
