from copy import deepcopy
from hashlib import sha1

from yt_env_setup import YTEnvSetup
from yt_commands import authors, get, create_user, set, ls, discover_proxies

import yatest.common
import yatest.common.network

from yt.wrapper import YtClient, tvm, config
from yt.common import YtError, update_inplace

from yt_driver_bindings import Driver

import tvmauth

import pytest
import requests
import json


##################################################################


PRIME_USER_TICKET = "3:user:CA0Q__________9_GhgKAwi5YBC5YBoGeXQ6YXBpINKF2MwEKAE:" \
    "BWkhjQvaA-vyqaGE_6GeqssoEleSAejzRhCW_UP360CckT5S1ZrMgvUQmSwWvKgH6nc9WnVSWUhtLS5p" \
    "_k0neyiHPThVVtp7_tpurqp7QQa-yuE6_cFHvLaoLcCxnZQtWcaSx0YlKqlXR7spcovnbYMGT0iy8LC-S8c0frdrHFU"


def auth_config(bb_port, tvm_port):
    return {
        "enable_authentication": True,
        "cypress_token_authenticator": {
            "secure": True
        },
        "blackbox_service": {
            "host": "localhost",
            "port": bb_port,
            "secure": False,
            "request_timeout": 1000,
        },
        "blackbox_ticket_authenticator": {
            "scopes": ["yt:api"],
            "enable_scope_check": True,
        },
        "blackbox_token_authenticator": {
            "scope": "yt:api",
            "get_user_ticket": False,
        },
        "tvm_service": {
            "use_tvm_tool": True,
            "tvm_tool_self_alias": "yt",
            "tvm_tool_auth_token": "e152bb86666565ee6619c15f60156cd6",
            "tvm_tool_port": tvm_port,

            "client_enable_user_ticket_checking": True,
            "client_enable_service_ticket_fetching": True,
            "client_enable_service_ticket_checking": True,

            # "client_self_id": 12345,
            # "client_dst_map": {
            #     "blackbox": 223,
            # },
            # "client_self_secret": "fake_secret",
        },
    }


class TestAuthBase(YTEnvSetup):
    ENABLE_HTTP_PROXY = True
    ENABLE_RPC_PROXY = True
    ENABLE_TVM_ONLY_PROXIES = True

    DELTA_PROXY_CONFIG = {}
    DELTA_RPC_PROXY_CONFIG = {}

    @classmethod
    def setup_class(cls):
        bb_port = yatest.common.network.PortManager().get_port()
        cls.fake_bb = yatest.common.execute([
            yatest.common.binary_path("yt/yt/tests/integration/fake_blackbox/fake_blackbox"),
            "--port", str(bb_port)
        ], wait=False)

        tvm_config_path = yatest.common.output_path("tvm.conf")
        with open(tvm_config_path, "w") as f:
            json.dump({
                "BbEnvType": 1,
                "clients": {
                    "yt": {
                        "secret": "fake_secret",
                        "self_tvm_id": 12345,
                        "dsts": {
                            "blackbox": {
                                "dst_id": 100500
                            },
                        }
                    },
                    "this_test": {
                        "secret": "fake_secret",
                        "self_tvm_id": 90231,
                        "dsts": {
                            "ytproxy": {
                                "dst_id": 12345
                            }
                        }
                    },
                }
            }, f)

        tvm_port = yatest.common.network.PortManager().get_port()
        cls.tvm_port = tvm_port
        cls.fake_tvm = yatest.common.execute([
            yatest.common.binary_path("passport/infra/daemons/tvmtool/cmd/tvmtool"),
            "--unittest",
            "--port", str(tvm_port),
            "--config", tvm_config_path,
            "--auth", "e152bb86666565ee6619c15f60156cd6"
        ], wait=False)

        cls.DELTA_PROXY_CONFIG["auth"] = auth_config(bb_port, tvm_port)
        cls.DELTA_RPC_PROXY_CONFIG.update(auth_config(bb_port, tvm_port))

        super(TestAuthBase, cls).setup_class()

    @classmethod
    def teardown_class(cls):
        cls.fake_bb.kill()
        cls.fake_tvm.kill()

        super(TestAuthBase, cls).teardown_class()

    def setup_method(self, method):
        super(TestAuthBase, self).setup_method(method)
        create_user("prime")
        create_user("tvm:90231")
        set("//sys/tokens/" + sha1(b"cypress_token").hexdigest(), "prime")

    def _create_yt_client(self, tvm_only=False, config=None):
        return YtClient(proxy=self.Env.get_http_proxy_address(tvm_only=tvm_only), config=config)

    def _create_yt_rpc_client(self, tvm_only=False, config=None):
        proxy = self.Env.get_rpc_proxy_address(tvm_only=tvm_only)
        default_config = {
            "backend": "rpc",
            "driver_config": {
                "connection_type": "rpc",
                "proxy_addresses": [proxy],
            },
        }

        if config is not None:
            update_inplace(default_config, config)

        return YtClient(proxy=None, config=default_config)


class TestAuth(TestAuthBase):
    @authors("prime", "verytable")
    def test_http_proxy_invalid_token(self):
        yc = self._create_yt_client(config={"token": "bad_token"})

        with pytest.raises(YtError):
            yc.create("map_node", "//tmp/bad_token")

    @authors("prime", "verytable")
    def test_rpc_proxy_invalid_token(self):
        yc = self._create_yt_rpc_client(config={"token": "bad_token"})

        with pytest.raises(YtError):
            yc.create("map_node", "//tmp/bad_token_rpc")

    @authors("prime", "verytable")
    def test_http_proxy_oauth_token_from_cypress(self):
        yc = self._create_yt_client(config={"token": "cypress_token"})

        yc.create("map_node", "//tmp/cypress_token")
        assert get("//tmp/cypress_token/@owner") == "prime"

        yc = self._create_yt_client(tvm_only=True, config={"token": "cypress_token"})
        with pytest.raises(YtError):
            yc.create("map_node", "//tmp/cypress_token_tvm_only")

    @authors("prime", "verytable")
    def test_rpc_proxy_oauth_token_from_cypress(self):
        yc = self._create_yt_rpc_client(config={"token": "cypress_token"})

        yc.create("map_node", "//tmp/cypress_token_rpc")
        assert get("//tmp/cypress_token_rpc/@owner") == "prime"

        yc = self._create_yt_rpc_client(tvm_only=True, config={"token": "cypress_token"})
        with pytest.raises(YtError):
            yc.create("map_node", "//tmp/cypress_token_rpc_tvm_only")

    @authors("prime", "verytable")
    def test_http_proxy_oauth_token_from_bb(self):
        yc = self._create_yt_client(config={"token": "bb_token"})

        yc.create("map_node", "//tmp/bb_token")
        assert get("//tmp/bb_token/@owner") == "prime"

        yc = self._create_yt_client(tvm_only=True, config={"token": "bb_token"})
        with pytest.raises(YtError):
            yc.create("map_node", "//tmp/bb_token_tvm_only")

    @authors("prime", "verytable")
    def test_rpc_proxy_oauth_token_from_bb(self):
        yc = self._create_yt_rpc_client(config={"token": "bb_token"})

        yc.create("map_node", "//tmp/bb_token_rpc")
        assert get("//tmp/bb_token_rpc/@owner") == "prime"

        yc = self._create_yt_rpc_client(tvm_only=True, config={"token": "bb_token"})
        with pytest.raises(YtError):
            yc.create("map_node", "//tmp/bb_token_rpc_tvm_only")

    @authors("prime", "verytable")
    def test_http_proxy_user_ticket(self):
        for tvm_only in [False, True]:
            path = "//tmp/user_ticket" + ["", "_tvm_only"][tvm_only]
            rsp = requests.post(
                "http://{}/api/v4/create".format(self.Env.get_http_proxy_address(tvm_only=tvm_only)),
                json={
                    "path": path,
                    "type": "map_node",
                },
                headers={
                    "X-Ya-User-Ticket": PRIME_USER_TICKET,
                })
            rsp.raise_for_status()

            assert get(path+"/@owner") == "prime"

    @authors("verytable")
    def test_config_deepcopy(self):
        tvm_client = tvmauth.TvmClient(
            tvmauth.TvmToolClientSettings(
                self_alias="this_test",
                auth_token='e152bb86666565ee6619c15f60156cd6',
                port=self.tvm_port,
            )
        )
        tvm_auth = tvm.ServiceTicketAuth(tvm_client)
        tvm_auth.override_proxy_tvm_id(12345)

        yc = self._create_yt_client(config={"tvm_auth": tvm_auth})

        # Config must be deepcopy-able.
        deepcopy(config.get_config(yc))

    @authors("prime", "verytable")
    def test_http_proxy_service_ticket(self):
        tvm_client = tvmauth.TvmClient(
            tvmauth.TvmToolClientSettings(
                self_alias="this_test",
                auth_token='e152bb86666565ee6619c15f60156cd6',
                port=self.tvm_port,
            )
        )
        tvm_auth = tvm.ServiceTicketAuth(tvm_client)
        tvm_auth.override_proxy_tvm_id(12345)

        for tvm_only in [False, True]:
            yc = self._create_yt_client(tvm_only=tvm_only, config={"tvm_auth": tvm_auth})

            path = "//tmp/service_ticket" + ["", "_tvm_only"][tvm_only]
            yc.create("map_node", path)

            assert get(path+"/@owner") == "tvm:90231"

    @authors("prime", "verytable")
    def test_rpc_proxy_service_ticket(self):
        tvm_client = tvmauth.TvmClient(
            tvmauth.TvmToolClientSettings(
                self_alias="this_test",
                auth_token='e152bb86666565ee6619c15f60156cd6',
                port=self.tvm_port,
            )
        )
        tvm_auth = tvm.ServiceTicketAuth(tvm_client)
        tvm_auth.override_proxy_tvm_id(12345)

        for tvm_only in [False, True]:
            yc = self._create_yt_rpc_client(tvm_only=tvm_only, config={"tvm_auth": tvm_auth})

            path = "//tmp/service_ticket_rpc" + ["", "_tvm_only"][tvm_only]
            yc.create("map_node", path)

            assert get(path+"/@owner") == "tvm:90231"


class TestTvmOnlyRpcProxyDiscovery(TestAuthBase):
    NUM_RPC_PROXIES = 2

    def setup_method(self, method):
        super(TestTvmOnlyRpcProxyDiscovery, self).setup_method(method)
        driver_config = deepcopy(self.Env.configs["driver"])
        driver_config["api_version"] = 4
        self.driver = Driver(driver_config)

    @authors("verytable")
    def test_addresses(self):
        proxy = ls("//sys/rpc_proxies")[0]

        addresses = get("//sys/rpc_proxies/"+proxy+"/@addresses")
        assert "tvm_only_internal_rpc" in addresses
        assert "default" in addresses["tvm_only_internal_rpc"]

    @authors("verytable")
    def test_discovery(self):
        configured_proxy_addresses = sorted(self.Env.get_rpc_proxy_addresses())
        configured_tvm_only_proxy_addresses = sorted(self.Env.get_rpc_proxy_addresses(tvm_only=True))

        for test_name, request, expected_addresses in [
            (
                "defaults", {}, configured_proxy_addresses,
            ),
            (
                "explicit_address_type",
                {"address_type": "tvm_only_internal_rpc"},
                configured_tvm_only_proxy_addresses,
            ),
            (
                "explicit_params",
                {"address_type": "tvm_only_internal_rpc", "network_name": "default"},
                configured_tvm_only_proxy_addresses,
            ),
        ]:
            proxies = discover_proxies(type_="rpc", driver=self.driver, **request)
            assert sorted(proxies) == expected_addresses, test_name
