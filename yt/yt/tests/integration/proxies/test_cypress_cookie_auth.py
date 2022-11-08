from yt_env_setup import YTEnvSetup, Restarter, HTTP_PROXIES_SERVICE
from yt_commands import (
    authors, get, create_user, exists,
    print_debug, set_user_password,
)

import dateutil
import datetime
import requests
import time

from requests.auth import HTTPBasicAuth


##################################################################


class TestCypressCookieAuth(YTEnvSetup):
    NUM_MASTERS = 1

    ENABLE_HTTP_PROXY = True
    NUM_HTTP_PROXIES = 1

    DELTA_PROXY_CONFIG = {
        "auth": {
            "enable_authentication": True,
            "cypress_cookie_manager": {
                "cookie_generator": {
                    "cookie_expiration_timeout": 4000,
                    "cookie_renewal_period": 2000,
                },
                "cookie_store": {
                    "full_fetch_period": 200,
                },
                "cookie_authenticator": {
                    "cache": {
                        "cache_ttl": 100,
                        "optimistic_cache_ttl": 100,
                        "error_ttl": 100,
                    },
                },
            },
        },
    }

    def _get_proxy_address(self):
        return "http://" + self.Env.get_proxy_address()

    def _check_login_page(self, rsp):
        assert rsp.status_code == 401
        print_debug(rsp.content)
        assert rsp.headers["WWW-Authenticate"] == "Basic"

    def _try_login(self, user, password):
        auth = HTTPBasicAuth(user, password)
        return requests.get(self._get_proxy_address() + "/login", auth=auth)

    def _login(self, user, password):
        rsp = self._try_login(user, password)
        return rsp.cookies["YTCypressCookie"]

    def _make_request(self, cookie=None):
        cookies = {}
        if cookie:
            cookies = {"YTCypressCookie": cookie}
        return requests.get(
            self._get_proxy_address() + "/api/v4/get?path=//@",
            cookies=cookies)

    def _check_allow(self, cookie=None):
        rsp = self._make_request(cookie)
        rsp.raise_for_status()

    def _check_deny(self, cookie=None):
        rsp = self._make_request(cookie)
        assert rsp.status_code == 401
        assert "YTCypressCookie" not in rsp.cookies

    @authors("gritukan")
    def test_login_401(self):
        def check(path_suffix):
            rsp = requests.get(self._get_proxy_address() + path_suffix)
            self._check_login_page(rsp)

        check("/login")
        check("/login/")
        check("/login/foo?bar=a&baz=b")

    @authors("gritukan")
    def test_cookie_in_cypress(self):
        create_user("u")
        set_user_password("u", "1234")
        password_revision = get("//sys/users/u/@password_revision")

        cookie = self._login("u", "1234")
        assert get(f"//sys/cypress_cookies/{cookie}/value") == cookie
        assert get(f"//sys/cypress_cookies/{cookie}/user") == "u"
        assert get(f"//sys/cypress_cookies/{cookie}/password_revision") == password_revision
        assert exists(f"//sys/cypress_cookies/{cookie}/@expiration_time")

        time.sleep(4.2)

        assert not exists(f"//sys/cypress_cookies/{cookie}")

    @authors("gritukan")
    def test_cookie_format(self):
        create_user("u")
        set_user_password("u", "u")
        rsp = self._try_login("u", "u")
        header = rsp.headers["Set-Cookie"]
        cookie, expire, secure, http_only, path = header.split(';')

        assert cookie.startswith("YTCypressCookie=")
        cookie = cookie[len("YTCypressCookie="):]
        assert exists(f"//sys/cypress_cookies/{cookie}")

        assert expire.startswith(" Expires=")
        expire_time = dateutil.parser.parse(expire[len(" Expires="):])
        now = datetime.datetime.now(datetime.timezone.utc)
        expiration_timeout = expire_time - now
        assert expiration_timeout >= datetime.timedelta(seconds=0)
        assert expiration_timeout <= datetime.timedelta(seconds=4)

        assert secure == " Secure"
        assert http_only == " HttpOnly"
        assert path == " Path=/"

    @authors("gritukan")
    def test_login_failed(self):
        # No such user.
        self._check_login_page(self._try_login("v", "1234"))

        create_user("u")
        # User has no password set.
        self._check_login_page(self._try_login("u", ""))

        set_user_password("u", "1234")
        # Invalid password.
        self._check_login_page(self._try_login("u", "123"))

    @authors("gritukan")
    def test_weird_password(self):
        PASSWORD = "  :  -_-"

        create_user("u")
        set_user_password("u", PASSWORD)
        self._login("u", PASSWORD)

    @authors("gritukan")
    def test_request_with_cookie(self):
        create_user("u")
        set_user_password("u", "1234")
        cookie = self._login("u", "1234")
        rsp = self._make_request(cookie)
        rsp.raise_for_status()

    @authors("gritukan")
    def test_request_with_invalid_cookie(self):
        # No cookie.
        self._check_deny()

        # Non-existent cookie.
        self._check_deny("ABCDDCBA")

        # Password changed.
        create_user("u")
        set_user_password("u", "1234")
        cookie = self._login("u", "1234")
        self._check_allow(cookie)
        set_user_password("u", "1235")
        # Create another cookie that should not be used for rotation on failure.
        self._login("u", "1235")
        time.sleep(0.5)
        self._check_deny(cookie)

        # Cookie expired.
        cookie = self._login("u", "1235")
        self._check_allow(cookie)
        time.sleep(2)
        # Create another cookie that should not be used for rotation on failure.
        self._login("u", "1235")
        time.sleep(2.2)
        self._check_deny(cookie)

    @authors("gritukan")
    def test_cookie_rotation(self):
        create_user("u")
        set_user_password("u", "1234")
        cookie1 = self._login("u", "1234")

        time.sleep(2.2)
        rsp = self._make_request(cookie1)
        cookie2 = rsp.cookies["YTCypressCookie"]
        assert cookie2 != cookie1

        assert \
            dateutil.parser.parse(get(f"//sys/cypress_cookies/{cookie2}/expires_at")) > \
            dateutil.parser.parse(get(f"//sys/cypress_cookies/{cookie1}/expires_at"))

        # Both cookies are still valid.
        self._check_allow(cookie1)
        self._check_allow(cookie2)

    @authors("gritukan")
    def test_periodic_cookie_fetch(self):
        create_user("u")
        set_user_password("u", "1234")
        cookie1 = self._login("u", "1234")
        time.sleep(2.2)
        cookie2 = self._login("u", "1234")

        assert \
            dateutil.parser.parse(get(f"//sys/cypress_cookies/{cookie2}/expires_at")) > \
            dateutil.parser.parse(get(f"//sys/cypress_cookies/{cookie1}/expires_at"))

        with Restarter(self.Env, HTTP_PROXIES_SERVICE):
            pass

        # Wait for all cookies fetch.
        time.sleep(0.5)

        rsp = self._make_request(cookie1)
        assert rsp.cookies["YTCypressCookie"] == cookie2
