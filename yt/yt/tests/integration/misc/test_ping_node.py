from yt_env_setup import YTEnvSetup

from yt_commands import authors, ping_node, print_debug, raises_yt_error
from yt.wrapper.driver import make_formatted_request

import random


##################################################################


class TestPingNode(YTEnvSetup):
    ENABLE_RPC_PROXY = True
    ENABLE_HTTP_PROXY = True
    NUM_MASTERS = 1
    NUM_MASTER_CACHES = 1
    NUM_CLOCKS = 1
    NUM_TIMESTAMP_PROVIDERS = 1
    NUM_QUEUE_AGENTS = 1
    NUM_DISCOVERY_SERVERS = 1
    NUM_SCHEDULERS = 1
    NUM_CONTROLLER_AGENTS = 1
    NUM_NODES = 1
    NUM_RPC_PROXIES = 1
    NUM_HTTP_PROXIES = 1

    @authors("khlebnikov")
    def test_ping_node(self):
        addresses = []

        def add_service(service, count):
            assert count > 0, service
            for index in range(count):
                addresses.append(self.Env.get_node_address(index, config_service=service))

        for service in ["master", "master_cache", "clock", "timestamp_provider", "queue_agent",
                        "scheduler", "controller_agent", "node", "rpc_proxy", "http_proxy"]:
            add_service(service, getattr(self.Env.yt_config, service + "_count"))

        add_service("discovery", self.Env.yt_config.discovery_server_count)

        http_client = self.Env.create_client()

        def http_ping_node(node_address, **kwargs):
            kwargs["node_address"] = node_address
            return make_formatted_request("ping_node", kwargs, format=None, client=http_client)

        for node_address in addresses:
            def check(result):
                assert result["latency"] >= 0
                assert len(result["chain"]) == 0

            print_debug("ping_node", node_address)
            check(ping_node(node_address))
            check(ping_node(node_address, payload_size=1024))
            check(http_ping_node(node_address))
            check(http_ping_node(node_address, payload_size=1024))

        for _ in range(10):
            def check(result):
                assert result["latency"] >= 0
                assert len(result["chain"]) == 10
                for step in result["chain"]:
                    assert 0 <= step["latency"] <= result["latency"]

            node_address = random.choice(addresses)
            chain_addresses = random.choices(addresses, k=10)
            print_debug("ping_node", node_address, chain_addresses)
            check(ping_node(node_address, chain_addresses=chain_addresses))
            check(ping_node(random.choice(addresses), chain_addresses=chain_addresses, payload_size=1024))
            check(http_ping_node(node_address, chain_addresses=chain_addresses))
            check(http_ping_node(node_address, chain_addresses=chain_addresses, payload_size=1024))

        with raises_yt_error("Ping node chain is too long"):
            ping_node(addresses[0], chain_addresses=addresses[:1]*11)
        with raises_yt_error("Ping node chain is too long"):
            http_ping_node(addresses[0], chain_addresses=addresses[:1]*11)
        with raises_yt_error("Ping node payload is too large"):
            ping_node(addresses[0], payload_size=1+(10<<20))
        with raises_yt_error("Ping node payload is too large"):
            http_ping_node(addresses[0], payload_size=1+(10<<20))


class TestPingNodeTls(TestPingNode):
    ENABLE_TLS = True
