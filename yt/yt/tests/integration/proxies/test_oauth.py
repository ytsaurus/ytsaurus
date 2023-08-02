from conftest_lib.conftest import mock_server  # noqa

from yt_env_setup import YTEnvSetup
from yt_commands import authors, create_user
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
            "user_info_error_field": "error"
        },
        "oauth_cookie_authenticator": {},
        "oauth_token_authenticator": {},
        "cypress_user_manager": {}
    }


class TestOAuthBase(YTEnvSetup):
    ENABLE_HTTP_PROXY = True
    DELTA_PROXY_CONFIG = {}

    @classmethod
    def setup_class(cls):
        cls.open_port_iterator = OpenPortIterator(
            port_locks_path=os.environ.get("YT_LOCAL_PORT_LOCKS_PATH", None))
        cls.mock_server_port = next(cls.open_port_iterator)

        cls.DELTA_PROXY_CONFIG["auth"] = auth_config(cls.mock_server_port)
        super(TestOAuthBase, cls).setup_class()

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

    def _check_deny(self, token=None, cookie=None, user=None):
        rsp = self._make_request(token, cookie, user)
        return rsp.status_code == 401


class TestOAuth(TestOAuthBase):
    @pytest.fixture(autouse=True)
    def setup_route(self, mock_server):  # noqa
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

            if bearer_token != "Bearer good_token":
                return mock_server.make_response(json={"error": "invalid token"}, status=403)

            return mock_server.make_response(json={"login": user, "sub": "42"})

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
