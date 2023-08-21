from yt_env_setup import YTEnvSetup
from yt_commands import (
    authors, create_user, issue_token, revoke_token, wait, get,
)

import requests


##################################################################


class TestCypressTokenAuth(YTEnvSetup):
    NUM_MASTERS = 1

    ENABLE_HTTP_PROXY = True
    NUM_HTTP_PROXIES = 1

    DELTA_PROXY_CONFIG = {
        "auth": {
            "enable_authentication": True,
            "cypress_token_authenticator": {
                "cache": {
                    "cache_ttl": 100,
                    "optimistic_cache_ttl": 100,
                    "error_ttl": 100,
                },
            },
        },
    }

    def _get_proxy_address(self):
        return "http://" + self.Env.get_proxy_address()

    def _make_request(self, token=None):
        headers = {}
        if token:
            headers = {"Authorization": "OAuth " + token}
        return requests.get(
            self._get_proxy_address() + "/api/v4/get?path=//@",
            headers=headers)

    def _check_allow(self, token=None):
        rsp = self._make_request(token)
        rsp.raise_for_status()

    def _check_deny(self, token=None):
        rsp = self._make_request(token)
        return rsp.status_code == 401

    @authors("gritukan")
    def test_simple(self):
        create_user("u")
        t, t_hash = issue_token("u")
        assert get("//sys/cypress_tokens/@count") == 1

        self._check_allow(t)
        assert self._check_deny()
        assert self._check_deny("xxx")

        revoke_token("u", t_hash)
        assert get("//sys/cypress_tokens/@count") == 0

        wait(lambda: self._check_deny(t))
