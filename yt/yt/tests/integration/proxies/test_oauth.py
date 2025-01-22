from conftest_lib.conftest import mock_server  # noqa

from yt_env_setup import YTEnvSetup
from yt_commands import authors, create_user, remove_user, wait
from yt.environment.helpers import OpenPortIterator

import pytest
import requests
import os


def auth_config(port):
    return {
        "enable_authentication": True,
        "oauth_service": {
            "host": "127.0.0.1",
            "port": port,
            "secure": False,
            "user_info_endpoint": "user_info",
            "user_info_login_field": "login",
            "user_info_subject_field": "sub",
            "user_info_error_field": "error",
            "login_transformations": [
                {
                    "match_pattern": r"(.*)@one-company\.(.*)",
                    "replacement": r"\1"
                },
                {
                    "match_pattern": r"(.*)@second-company\.(.*)",
                    "replacement": r"\2-nosokhvost-\1"
                },
                {
                    "match_pattern": r"(.*)@@(.*)\.(.*)",
                    "replacement": r"\2-\1"
                },
            ],
        },
        "oauth_cookie_authenticator": {
            "cache": {
                "cache_ttl": "5s",
                "optimistic_cache_ttl": "1m",
                "error_ttl": "1s",
            },
        },
        "oauth_token_authenticator": {
            "cache": {
                "cache_ttl": "5s",
                "optimistic_cache_ttl": "1m",
                "error_ttl": "1s",
            },
        },
        "cypress_user_manager": {
            "cache": {
                "cache_ttl": "5s",
                "optimistic_cache_ttl": "1m",
                "error_ttl": "1s",
            },
        },
    }


@pytest.mark.enabled_multidaemon
class TestOAuthBase(YTEnvSetup):
    NUM_MASTERS = 1

    ENABLE_HTTP_PROXY = True
    NUM_HTTP_PROXIES = 1
    DELTA_PROXY_CONFIG = {}
    DELTA_PROXY_AUTH_CONFIG = {}
    ENABLE_MULTIDAEMON = True

    @classmethod
    def setup_class(cls):
        cls.open_port_iterator = OpenPortIterator(
            port_locks_path=os.environ.get("YT_LOCAL_PORT_LOCKS_PATH", None))
        cls.mock_server_port = next(cls.open_port_iterator)

        cls.DELTA_PROXY_CONFIG["auth"] = auth_config(cls.mock_server_port)
        cls.DELTA_PROXY_CONFIG["auth"].update(cls.DELTA_PROXY_AUTH_CONFIG)
        super(TestOAuthBase, cls).setup_class()

    # This is annoying, but we set blackbox_token_authenticator in default_config for
    # some reason. This way we avoid 10 second timeouts in each authentication call.
    @classmethod
    def modify_proxy_config(cls, multidaemon_config, config):
        super(TestOAuthBase, cls).modify_proxy_config(multidaemon_config, config)

        for proxy_config in config:
            if proxy_config.get("auth", {}).get("blackbox_token_authenticator"):
                del proxy_config["auth"]["blackbox_token_authenticator"]

    @classmethod
    def teardown_class(cls):
        pass

    def setup_method(self, method):
        super(TestOAuthBase, self).setup_method(method)
        create_user("u")

    def _get_proxy_address(self):
        return "http://" + self.Env.get_proxy_address()

    def _make_request(self, token=None, cookie=None, user=None):
        headers = {}
        if not user:
            user = "u"
        if token:
            headers["Authorization"] = f"OAuth {token}:{user}"
        elif cookie:
            headers["Cookie"] = f"access_token={cookie}:{user}"

        rsp = requests.get(
            self._get_proxy_address() + "/auth/whoami",
            headers=headers)
        return rsp

    def _check_allow(self, token=None, cookie=None, user=None):
        rsp = self._make_request(token, cookie, user)
        rsp.raise_for_status()
        return rsp

    def _wait_allow(self, token=None, cookie=None, user=None):
        wait(lambda: self._make_request(token, cookie, user).ok)

    def _check_deny(self, token=None, cookie=None, user=None):
        rsp = self._make_request(token, cookie, user)
        return rsp.status_code == 401

    @pytest.fixture(autouse=True)
    def setup_route(self, mock_server):  # noqa
        mock_server.bad_then_good_token_counter = 0
        mock_server.good_then_bad_token_counter = 0

        @mock_server.handler("/user_info")
        def user_info_handler(request, **kwargs):
            auth_header = request.headers.get("authorization")
            if auth_header is None:
                return mock_server.make_response(json={"error": "no authorization header provided"}, status=403)

            if ":" in auth_header:
                bearer_token, user = auth_header.split(":", 1)
            else:
                bearer_token = auth_header
                user = "u"

            if bearer_token == "Bearer retry_token":
                return mock_server.make_response(json={"error": "server error"}, status=500)

            if bearer_token == "Bearer bad_then_good_token":
                mock_server.bad_then_good_token_counter += 1
                if mock_server.bad_then_good_token_counter == 1:
                    return mock_server.make_response(json={"error": "invalid token"}, status=403)
                else:
                    return mock_server.make_response(json={"login": user, "sub": "42"})

            if bearer_token == "Bearer good_then_bad_token":
                mock_server.good_then_bad_token_counter += 1
                if mock_server.good_then_bad_token_counter == 1:
                    return mock_server.make_response(json={"login": user, "sub": "42"})
                else:
                    return mock_server.make_response(json={"error": "invalid token"}, status=403)

            if bearer_token != "Bearer good_token":
                return mock_server.make_response(json={"error": "invalid token"}, status=403)

            return mock_server.make_response(json={"login": user, "sub": "42"})


@pytest.mark.enabled_multidaemon
class TestOAuth(TestOAuthBase):
    ENABLE_MULTIDAEMON = True

    @authors("kaikash", "ignat")
    def test_http_proxy_invalid_token(self):
        assert self._check_deny()
        assert self._check_deny(token="bad_token")

    @authors("kaikash", "ignat")
    def test_http_proxy_invalid_cookie(self):
        assert self._check_deny(cookie="bad_token")

    @authors("kaikash", "ignat")
    def test_http_proxy_good_token(self):
        rsp = self._check_allow(token="good_token")
        data = rsp.json()
        assert data["login"] == "u"
        assert data["realm"] == "oauth:token"

    @authors("kaikash", "ignat")
    def test_http_proxy_good_cookie(self):
        rsp = self._check_allow(cookie="good_token")
        data = rsp.json()
        assert data["login"] == "u"
        assert data["realm"] == "oauth:cookie"

    @authors("kaikash", "ignat")
    def test_http_user_create(self):
        rsp = self._check_allow(token="good_token", user="new_user")
        data = rsp.json()
        assert data["login"] == "new_user"
        assert data["realm"] == "oauth:token"

    @authors("kaikash", "ignat")
    def test_http_proxy_oauth_server_error(self):
        assert self._check_deny(cookie="retry_token")

    @authors("achulkov2", "nadya73")
    def test_login_transformation(self):
        def check_user(user, expected_login):
            rsp = self._check_allow(token="good_token", user=user)
            data = rsp.json()
            assert data["login"] == expected_login
            assert data["realm"] == "oauth:token"

        check_user("achulkov2@one-company.de", "achulkov2")
        check_user("max42@second-company.be", "be-nosokhvost-max42")
        check_user("john@@third-company.fr", "third-company-john")
        check_user("fourth@fourth-company.com", "fourth@fourth-company.com")
        check_user("fifth", "fifth")

    @authors("aleksandr.gaev", "nadya73")
    def test_cache_invalidation_bad_then_good_token(self):
        assert self._check_deny(token="bad_then_good_token")
        self._wait_allow(token="bad_then_good_token")

    @authors("aleksandr.gaev", "nadya73")
    def test_cache_invalidation_bad_then_good_cookie(self):
        assert self._check_deny(cookie="bad_then_good_token")
        self._wait_allow(cookie="bad_then_good_token")

    @authors("aleksandr.gaev", "nadya73")
    def test_cache_invalidation_good_then_bad_token(self):
        assert self._check_allow(token="good_then_bad_token")
        wait(lambda: self._check_deny(token="good_then_bad_token"))

    @authors("aleksandr.gaev", "nadya73")
    def test_cache_invalidation_good_then_bad_cookie(self):
        assert self._check_allow(cookie="good_then_bad_token")
        wait(lambda: self._check_deny(cookie="good_then_bad_token"))


@pytest.mark.enabled_multidaemon
class TestOAuthWithDisabledUserCreation(TestOAuthBase):
    ENABLE_MULTIDAEMON = True

    DELTA_PROXY_AUTH_CONFIG = {
        "oauth_cookie_authenticator": {
            "cache": {
                "cache_ttl": "5s",
                "optimistic_cache_ttl": "1m",
                "error_ttl": "1s",
            },
            "create_user_if_not_exists": False,
        },
        "oauth_token_authenticator": {
            "cache": {
                "cache_ttl": "5s",
                "optimistic_cache_ttl": "1m",
                "error_ttl": "1s",
            },
            "create_user_if_not_exists": False,
        },
    }

    @authors("aleksandr.gaev", "nadya73")
    def test_unknown_user(self):
        assert self._check_deny(token="bad_token", user="unknown_user")
        assert self._check_deny(cookie="bad_token", user="unknown_user")

        assert self._check_deny(token="good_token", user="unknown_user")
        assert self._check_deny(cookie="good_token", user="unknown_user")

    @authors("aleksandr.gaev", "nadya73")
    def test_known_user(self):
        create_user("known_user")

        assert self._check_deny(token="bad_token", user="known_user")
        assert self._check_deny(cookie="bad_token", user="known_user")

        rsp = self._check_allow(token="good_token", user="known_user")
        data = rsp.json()
        assert data["login"] == "known_user"
        assert data["realm"] == "oauth:token"

        rsp = self._check_allow(cookie="good_token", user="known_user")
        data = rsp.json()
        assert data["login"] == "known_user"
        assert data["realm"] == "oauth:cookie"

    @authors("aleksandr.gaev", "nadya73")
    def test_user_cache_invalidation(self):
        assert self._check_deny(token="bad_token", user="new_user")
        assert self._check_deny(cookie="bad_token", user="new_user")
        assert self._check_deny(token="good_token", user="new_user")
        assert self._check_deny(cookie="good_token", user="new_user")

        create_user("new_user")

        assert self._check_deny(token="bad_token", user="new_user")
        assert self._check_deny(cookie="bad_token", user="new_user")

        self._wait_allow(token="good_token", user="new_user")
        rsp = self._check_allow(token="good_token", user="new_user")
        data = rsp.json()
        assert data["login"] == "new_user"
        assert data["realm"] == "oauth:token"

        self._wait_allow(cookie="good_token", user="new_user")
        rsp = self._check_allow(cookie="good_token", user="new_user")
        data = rsp.json()
        assert data["login"] == "new_user"
        assert data["realm"] == "oauth:cookie"

        remove_user("new_user")

        assert self._check_deny(token="bad_token", user="new_user")
        assert self._check_deny(cookie="bad_token", user="new_user")

        wait(lambda: self._check_deny(token="good_token", user="new_user"))
        wait(lambda: self._check_deny(cookie="good_token", user="new_user"))
