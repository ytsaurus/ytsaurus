from yt_env_setup import YTEnvSetup
from yt_commands import (
    authors, create_user, issue_token, revoke_token, wait, get, set_user_password
)

import requests
import hashlib


##################################################################

class TestCypressTokenAuthBase(YTEnvSetup):
    NUM_MASTERS = 1

    ENABLE_HTTP_PROXY = True
    NUM_HTTP_PROXIES = 1

    def _get_proxy_address(self):
        return "http://" + self.Env.get_proxy_address()

    def _make_request(self, path, token=None):
        headers = {}
        if token:
            headers = {"Authorization": "OAuth " + token}
        return requests.get(
            self._get_proxy_address() + path,
            headers=headers)

    def _check_allow(self, path="/api/v4/get?path=//@", token=None):
        rsp = self._make_request(path, token)
        rsp.raise_for_status()

    def _check_deny(self, path="/api/v4/get?path=//@", token=None):
        rsp = self._make_request(path, token)
        return rsp.status_code == 401

    def _compute_sha256(self, value):
        return hashlib.sha256(value.encode("utf-8")).hexdigest()


class TestCypressTokenAuth(TestCypressTokenAuthBase):
    DELTA_PROXY_CONFIG = {
        "auth": {
            "enable_authentication": True,
            "cypress_token_authenticator": {
                "cache": {
                    "cache_ttl": "5s",
                    "optimistic_cache_ttl": "1m",
                    "error_ttl": "1s",
                },
            },
        },
    }

    @authors("gritukan")
    def test_simple(self):
        create_user("u")
        t, t_hash = issue_token("u")
        assert get("//sys/cypress_tokens/@count") == 1

        self._check_allow(token=t)
        assert self._check_deny()
        assert self._check_deny(token="xxx")

        revoke_token("u", t_hash)
        assert get("//sys/cypress_tokens/@count") == 0

        wait(lambda: self._check_deny(token=t))

    @authors("aleksandr.gaev")
    def test_revoke_token(self):
        create_user("u2")
        t, t_hash = issue_token("u2")
        self._check_allow(token=t)  # Activate cache.
        revoke_token("u2", t_hash)
        wait(lambda: self._check_deny(token=t))


class TestAuthenticationCommands(TestCypressTokenAuthBase):
    DELTA_PROXY_CONFIG = {
        "auth": {
            "enable_authentication": True,
        },
    }

    @authors("aleksandr.gaev")
    def test_authentication_commands(self):
        create_user("u3")
        t, _ = issue_token("u3")
        _, t2_sha256 = issue_token("u3")
        p1_sha256 = self._compute_sha256("p1")
        p2_sha256 = self._compute_sha256("p2")

        # Passwordless user should not be able to change password
        assert not self._make_request(f"/api/v4/set_user_password?user=u3&new_password_sha256={p2_sha256}", t).ok

        set_user_password("u3", "p1")

        # Test failure without providing password.
        assert self._make_request(f"/api/v4/set_user_password?user=u3&new_password_sha256={p2_sha256}", t).json()["message"] == "User provided invalid password"
        assert self._make_request("/api/v4/issue_token?user=u3", t).json()["message"] == "User provided invalid password"
        assert self._make_request(f"/api/v4/revoke_token?user=u3&token_sha256={t2_sha256}", t).json()["message"] == "User provided invalid password"
        assert self._make_request("/api/v4/list_user_tokens?user=u3", t).json()["message"] == "User provided invalid password"

        # Test success with providing password.
        self._make_request(f"/api/v4/set_user_password?user=u3&current_password_sha256={p1_sha256}&new_password_sha256={p2_sha256}", t).raise_for_status()
        self._make_request(f"/api/v4/issue_token?user=u3&password_sha256={p2_sha256}", t).raise_for_status()
        self._make_request(f"/api/v4/revoke_token?user=u3&token_sha256={t2_sha256}&password_sha256={p2_sha256}", t).raise_for_status()
        self._make_request(f"/api/v4/list_user_tokens?user=u3&password_sha256={p2_sha256}", t).raise_for_status()


class TestAuthenticationCommandsWithNoPassword(TestCypressTokenAuthBase):
    DELTA_PROXY_CONFIG = {
        "driver": {
            "require_password_in_authentication_commands": False,
        },
        "auth": {
            "enable_authentication": True,
        },
    }

    @authors("aleksandr.gaev")
    def test_authentication_commands_without_password(self):
        create_user("u")
        t, _ = issue_token("u")
        _, t2_sha256 = issue_token("u")
        _, t3_sha256 = issue_token("u")
        p1_sha256 = self._compute_sha256("p1")
        p2_sha256 = self._compute_sha256("p2")

        # Passwordless user should be able to change password
        self._make_request(f"/api/v4/set_user_password?user=u&new_password_sha256={p2_sha256}", t).raise_for_status()

        set_user_password("u", "p3")

        # Test success without providing password.
        self._make_request(f"/api/v4/set_user_password?user=u&new_password_sha256={p1_sha256}", t).raise_for_status()
        self._make_request("/api/v4/issue_token?user=u", t).raise_for_status()
        self._make_request(f"/api/v4/revoke_token?user=u&token_sha256={t2_sha256}", t).raise_for_status()
        self._make_request("/api/v4/list_user_tokens?user=u", t).raise_for_status()

        # Test success with providing password.
        self._make_request(f"/api/v4/set_user_password?user=u&current_password_sha256={p1_sha256}&new_password_sha256={p2_sha256}", t).raise_for_status()
        self._make_request(f"/api/v4/issue_token?user=u&password_sha256={p2_sha256}", t).raise_for_status()
        self._make_request(f"/api/v4/revoke_token?user=u&token_sha256={t3_sha256}&password_sha256={p2_sha256}", t).raise_for_status()
        self._make_request(f"/api/v4/list_user_tokens?user=u&password_sha256={p2_sha256}", t).raise_for_status()
