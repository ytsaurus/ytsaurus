from yt_env_setup import YTEnvSetup
from yt_commands import (
    authors, create_user, issue_token, revoke_token, list_user_tokens,
    wait, get, set, set_user_password, create, raises_yt_error
)
from yt.common import YtResponseError
from yt.environment.helpers import assert_items_equal

import pytest

import requests
import hashlib


##################################################################

@pytest.mark.enabled_multidaemon
class TestCypressTokenAuthBase(YTEnvSetup):
    NUM_MASTERS = 1

    ENABLE_HTTP_PROXY = True
    NUM_HTTP_PROXIES = 1

    ENABLE_MULTIDAEMON = True

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


@pytest.mark.enabled_multidaemon
class TestCypressTokenAuth(TestCypressTokenAuthBase):
    DELTA_PROXY_CONFIG = {
        "auth": {
            "enable_authentication": True,
            "cypress_token_authenticator": {
                "cache": {
                    "cache_ttl": "5s",
                    "optimistic_cache_ttl": "5s",
                    "error_ttl": "1s",
                },
            },
        },
    }
    ENABLE_MULTIDAEMON = True

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

    @authors("pavel-bash")
    def test_list_user_tokens(self):
        create_user("u1")
        _, t1_hash = issue_token("u1")
        _, t2_hash = issue_token("u1")
        assert_items_equal(list_user_tokens("u1"), [t1_hash, t2_hash])

        create_user("u2")
        _, t3_hash = issue_token("u2")
        assert_items_equal(list_user_tokens("u2"), [t3_hash])

    @authors("pavel-bash")
    def test_old_user_attribute_is_not_created(self):
        create_user("u1")
        _, t_hash = issue_token("u1")
        with raises_yt_error("Attribute \"user\" is not found"):
            get(f"//sys/cypress_tokens/{t_hash}/@user")

    @authors("pavel-bash")
    def test_correct_user_id_in_token(self):
        create_user("u1")
        user1_id = get("//sys/users/u1/@id")
        _, t1_hash = issue_token("u1")
        assert get(f"//sys/cypress_tokens/{t1_hash}/@user_id") == user1_id

        create_user("u2")
        user2_id = get("//sys/users/u2/@id")
        _, t2_hash = issue_token("u2")
        assert get(f"//sys/cypress_tokens/{t2_hash}/@user_id") == user2_id

        assert user1_id != user2_id

    @authors("pavel-bash")
    def test_username_to_user_id_backward_compatibility_issue_revoke(self):
        # In this test we're manually issuing the token using the old schema;
        # the authentication and token revocation should still succeed.
        create_user("u1")
        token = "XXX"
        token_hash = hashlib.sha256(token.encode("utf-8")).hexdigest()
        create("file", f"//sys/cypress_tokens/{token_hash}", attributes={"user": "u1", "token_prefix": ""})
        self._check_allow(token=token)

        revoke_token("u1", token_hash)
        wait(lambda: self._check_deny(token=token))

    @authors("pavel-bash")
    def test_username_to_user_id_backward_compatibility_list_mixed(self):
        # Tokens from the old schema and from the new schema must both be listed.
        create_user("u1")
        token_manual = "XXX"
        token_manual_hash = hashlib.sha256(token_manual.encode("utf-8")).hexdigest()
        create("file", f"//sys/cypress_tokens/{token_manual_hash}", attributes={"user": "u1", "token_prefix": ""})

        _, token_usual_hash = issue_token("u1")

        assert_items_equal(list_user_tokens("u1"), [token_manual_hash, token_usual_hash])


@pytest.mark.enabled_multidaemon
class TestCypressTokenAuthWithoutCache(TestCypressTokenAuthBase):
    DELTA_PROXY_CONFIG = {
        "auth": {
            "enable_authentication": True,
            "cypress_token_authenticator": {
                "cache": {
                    "cache_ttl": "1ms",
                    "optimistic_cache_ttl": "1ms",
                    "error_ttl": "1ms",
                },
            },
        },
    }
    ENABLE_MULTIDAEMON = True

    @authors("pavel-bash")
    def test_user_rename(self):
        # This test does not pass when the cache is used; at least, until we introduce the logic
        # of authentication cache invalidation.
        create_user("u1")
        t, _ = issue_token("u1")
        self._check_allow(token=t)

        set("//sys/users/u1/@name", "u2")
        self._check_allow(token=t)


@pytest.mark.enabled_multidaemon
class TestAuthenticationCommands(TestCypressTokenAuthBase):
    DELTA_PROXY_CONFIG = {
        "auth": {
            "enable_authentication": True,
        },
    }
    ENABLE_MULTIDAEMON = True

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


@pytest.mark.enabled_multidaemon
class TestAuthenticationCommandsWithNoPassword(TestCypressTokenAuthBase):
    DELTA_PROXY_CONFIG = {
        "driver": {
            "require_password_in_authentication_commands": False,
        },
        "auth": {
            "enable_authentication": True,
        },
    }
    ENABLE_MULTIDAEMON = True

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
