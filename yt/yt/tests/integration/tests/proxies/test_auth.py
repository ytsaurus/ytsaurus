from hashlib import sha1

from yt_env_setup import YTEnvSetup
from yt_commands import authors, get, create_user, set

import yatest.common
import yatest.common.network

import requests
import json


##################################################################


PRIME_USER_TICKET = "3:user:CA0Q__________9_GhgKAwi5YBC5YBoGeXQ6YXBpINKF2MwEKAE:" \
    "BWkhjQvaA-vyqaGE_6GeqssoEleSAejzRhCW_UP360CckT5S1ZrMgvUQmSwWvKgH6nc9WnVSWUhtLS5p" \
    "_k0neyiHPThVVtp7_tpurqp7QQa-yuE6_cFHvLaoLcCxnZQtWcaSx0YlKqlXR7spcovnbYMGT0iy8LC-S8c0frdrHFU"

TVM_90231_SERVICE_TICKET = "3:serv:CBAQ__________9_IgcI98AFELlg:" \
    "Bc8SQuXdMvo8p0ETQdb2fSFQDAaRAjhGN0Bu_lwCOjeQynyqRirtquzy-r_Gcr" \
    "_bJhbBYeK8vBAGYs0MHLqma0eMqw7HnUHJfwu6i1DQvPSRwtzzbPX5zAIqYwK1I6zteEkvy" \
    "-bYqpaVdmhIvonh6UdS49VaFIzcYC2ClcSg5sg"


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


class TestAuth(YTEnvSetup):
    ENABLE_HTTP_PROXY = True
    ENABLE_RPC_PROXY = True

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
                    }
                }
            }, f)

        tvm_port = yatest.common.network.PortManager().get_port()
        cls.fake_tvm = yatest.common.execute([
            yatest.common.binary_path("passport/go/daemons/tvmtool/cmd/tvmtool"),
            "--unittest",
            "--port", str(tvm_port),
            "--config", tvm_config_path,
            "--auth", "e152bb86666565ee6619c15f60156cd6"
        ], wait=False)

        cls.DELTA_PROXY_CONFIG["auth"] = auth_config(bb_port, tvm_port)
        cls.DELTA_RPC_PROXY_CONFIG["auth"] = auth_config(bb_port, tvm_port)

        super(TestAuth, cls).setup_class()

    @classmethod
    def teardown_class(cls):
        cls.fake_bb.kill()
        cls.fake_tvm.kill()

        super(TestAuth, cls).teardown_class()

    def setup_method(self, method):
        super(TestAuth, self).setup_method(method)
        create_user("prime")
        create_user("tvm:90231")
        set("//sys/tokens/" + sha1(b"cypress_token").hexdigest(), "prime")

    @authors("prime")
    def test_http_proxy_invalid_token(self):
        rsp = requests.post(
            "http://{}/api/v4/create".format(self.Env.get_proxy_address()),
            json={
                "path": "//tmp/bad_token",
                "type": "map_node",
            },
            headers={
                "Authorization": "OAuth bad_token",
            })

        assert rsp.status_code == 401

    @authors("prime")
    def test_http_proxy_oauth_token_from_cypress(self):
        rsp = requests.post(
            "http://{}/api/v4/create".format(self.Env.get_proxy_address()),
            json={
                "path": "//tmp/cypress_token",
                "type": "map_node",
            },
            headers={
                "Authorization": "OAuth cypress_token",
            })
        rsp.raise_for_status()

        assert get("//tmp/cypress_token/@owner") == "prime"

    @authors("prime")
    def test_http_proxy_oauth_token_from_bb(self):
        rsp = requests.post(
            "http://{}/api/v4/create".format(self.Env.get_proxy_address()),
            json={
                "path": "//tmp/bb_token",
                "type": "map_node",
            },
            headers={
                "Authorization": "OAuth bb_token",
            })
        rsp.raise_for_status()

        assert get("//tmp/bb_token/@owner") == "prime"

    @authors("prime")
    def test_user_ticket(self):
        rsp = requests.post(
            "http://{}/api/v4/create".format(self.Env.get_proxy_address()),
            json={
                "path": "//tmp/user_ticket",
                "type": "map_node",
            },
            headers={
                "X-Ya-User-Ticket": PRIME_USER_TICKET,
            })
        rsp.raise_for_status()

        assert get("//tmp/user_ticket/@owner") == "prime"

    @authors("prime")
    def test_service_ticket(self):
        rsp = requests.post(
            "http://{}/api/v4/create".format(self.Env.get_proxy_address()),
            json={
                "path": "//tmp/service_ticket",
                "type": "map_node",
            },
            headers={
                "X-Ya-Service-Ticket": TVM_90231_SERVICE_TICKET,
            })
        rsp.raise_for_status()

        assert get("//tmp/service_ticket/@owner") == "tvm:90231"
