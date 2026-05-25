from yt_env_setup import YTEnvSetup
from yt_commands import authors, get, set, create_user, remove

import os
import requests
import socket
import threading

from requests.auth import HTTPBasicAuth


##################################################################
# Minimal Python mock LDAP server
##################################################################

def _ber_length(n):
    if n < 128:
        return bytes([n])
    if n < 256:
        return bytes([0x81, n])
    return bytes([0x82, (n >> 8) & 0xff, n & 0xff])


def _ber_tlv(tag, value):
    if isinstance(value, str):
        value = value.encode()
    return bytes([tag]) + _ber_length(len(value)) + bytes(value)


def _ber_seq(body):
    return _ber_tlv(0x30, body)


def _ber_int(n):
    return bytes([0x02, 0x01, n])


def _ber_str(s):
    if isinstance(s, str):
        s = s.encode()
    return _ber_tlv(0x04, s)


def _ber_enum(n):
    return bytes([0x0a, 0x01, n])


def _ldap_result(code):
    return _ber_enum(code) + _ber_str(b"") + _ber_str(b"")


def _ldap_message(msg_id, op):
    return _ber_seq(_ber_int(msg_id) + op)


def _bind_response(msg_id, code):
    return _ldap_message(msg_id, _ber_tlv(0x61, _ldap_result(code)))


def _search_result_entry(msg_id, dn):
    body = _ber_str(dn) + _ber_tlv(0x30, b"")
    return _ldap_message(msg_id, _ber_tlv(0x64, body))


def _search_result_done(msg_id, code=0):
    return _ldap_message(msg_id, _ber_tlv(0x65, _ldap_result(code)))


class _BerReader:
    def __init__(self, data):
        self._data = data
        self._pos = 0

    def _read_length(self):
        first = self._data[self._pos]
        self._pos += 1
        if first < 0x80:
            return first
        n = first & 0x7f
        length = 0
        for _ in range(n):
            length = (length << 8) | self._data[self._pos]
            self._pos += 1
        return length

    def read_tlv(self):
        tag = self._data[self._pos]
        self._pos += 1
        length = self._read_length()
        value = self._data[self._pos:self._pos + length]
        self._pos += length
        return tag, value

    def read_int(self):
        tag, value = self.read_tlv()
        assert tag == 0x02
        result = 0
        for b in value:
            result = (result << 8) | b
        return result

    def read_octet_string(self):
        tag, value = self.read_tlv()
        assert tag in (0x04, 0x80), f"expected OCTET STRING, got {tag:#x}"
        return value

    def at_end(self):
        return self._pos >= len(self._data)


class MockLdapServer:
    LDAP_SUCCESS = 0
    LDAP_INVALID_CREDENTIALS = 49

    def __init__(self, admin_dn, admin_password, users):
        """
        users: dict of uid -> {"dn": str, "password": str}
        """
        self._admin_dn = admin_dn.encode() if isinstance(admin_dn, str) else admin_dn
        self._admin_password = admin_password.encode() if isinstance(admin_password, str) else admin_password
        self._users = {
            uid: {
                "dn": u["dn"].encode() if isinstance(u["dn"], str) else u["dn"],
                "password": u["password"].encode() if isinstance(u["password"], str) else u["password"],
            }
            for uid, u in users.items()
        }
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind(("127.0.0.1", 0))
        self._sock.listen(10)
        self._port = self._sock.getsockname()[1]
        self._thread = None

    @property
    def port(self):
        return self._port

    def start(self):
        self._thread = threading.Thread(target=self._accept_loop, daemon=True)
        self._thread.start()

    def stop(self):
        try:
            self._sock.close()
        except OSError:
            pass
        if self._thread:
            self._thread.join(timeout=2)

    def _accept_loop(self):
        while True:
            try:
                client, _ = self._sock.accept()
            except OSError:
                break
            t = threading.Thread(target=self._handle_conn, args=(client,), daemon=True)
            t.start()

    def _recv_exactly(self, conn, n):
        data = b""
        while len(data) < n:
            chunk = conn.recv(n - len(data))
            if not chunk:
                return None
            data += chunk
        return data

    def _read_ldap_message(self, conn):
        header = self._recv_exactly(conn, 2)
        if not header:
            return None
        if header[0] != 0x30:
            return None
        first_len = header[1]
        if first_len < 0x80:
            body_len = first_len
        else:
            n_bytes = first_len & 0x7f
            extra = self._recv_exactly(conn, n_bytes)
            if not extra:
                return None
            body_len = 0
            for b in extra:
                body_len = (body_len << 8) | b
        return self._recv_exactly(conn, body_len)

    def _handle_conn(self, conn):
        try:
            while True:
                body = self._read_ldap_message(conn)
                if body is None:
                    break
                r = _BerReader(body)
                msg_id = r.read_int()
                op_tag, op_data = r.read_tlv()

                if op_tag == 0x60:  # BindRequest
                    response = self._handle_bind(msg_id, op_data)
                elif op_tag == 0x63:  # SearchRequest
                    response = self._handle_search(msg_id, op_data)
                elif op_tag == 0x42:  # UnbindRequest
                    break
                else:
                    break

                if response:
                    conn.sendall(response)
        finally:
            conn.close()

    def _handle_bind(self, msg_id, data):
        r = _BerReader(data)
        r.read_tlv()  # version
        dn = r.read_octet_string()
        pw = r.read_octet_string()

        if dn == self._admin_dn and pw == self._admin_password:
            return _bind_response(msg_id, self.LDAP_SUCCESS)
        for user in self._users.values():
            if dn == user["dn"] and pw == user["password"]:
                return _bind_response(msg_id, self.LDAP_SUCCESS)
        return _bind_response(msg_id, self.LDAP_INVALID_CREDENTIALS)

    def _handle_search(self, msg_id, data):
        r = _BerReader(data)
        for _ in range(6):
            r.read_tlv()  # baseObject, scope, derefAliases, sizeLimit, timeLimit, typesOnly
        filter_tag, filter_data = r.read_tlv()
        uid = self._extract_uid(filter_tag, filter_data)

        if uid and uid in self._users:
            dn = self._users[uid]["dn"]
            return _search_result_entry(msg_id, dn) + _search_result_done(msg_id, self.LDAP_SUCCESS)
        return _search_result_done(msg_id, self.LDAP_SUCCESS)

    def _extract_uid(self, tag, data):
        if tag == 0xa3:  # equalityMatch [3] CONSTRUCTED
            r = _BerReader(data)
            attr = r.read_octet_string().decode().lower()
            value = r.read_octet_string().decode()
            if attr == "uid":
                return value
        elif tag == 0xa0:  # and [0] CONSTRUCTED
            r = _BerReader(data)
            while not r.at_end():
                ft, fd = r.read_tlv()
                uid = self._extract_uid(ft, fd)
                if uid:
                    return uid
        return None


##################################################################
# Tests
##################################################################

ADMIN_DN = "cn=admin,dc=example,dc=com"
ADMIN_PASSWORD = "admin_secret"
ADMIN_PASSWORD_ENV_VAR = "YT_TEST_LDAP_ADMIN_PASSWORD"

LDAP_USERS = {
    "alice": {"dn": "cn=alice,ou=users,dc=example,dc=com", "password": "alice_pass"},
    "bob":   {"dn": "cn=bob,ou=users,dc=example,dc=com",   "password": "bob_pass"},
}


class TestLdapAuth(YTEnvSetup):
    NUM_MASTERS = 1

    ENABLE_HTTP_PROXY = True
    NUM_HTTP_PROXIES = 1

    ENABLE_MULTIDAEMON = False

    # Filled in setup_class, before the cluster starts.
    _ldap_server = None

    DELTA_HTTP_PROXY_CONFIG = {
        "auth": {
            "enable_authentication": True,
            "cypress_password_authenticator": {
                "enabled": False,
            },
            "cypress_cookie_manager": {
                "cookie_generator": {
                    # Short LDAP cookie TTL so expiry tests are fast.
                    "ldap_cookie_expiration_timeout": 4000,
                    "cookie_expiration_timeout": 90000,
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

    @classmethod
    def setup_class(cls):
        cls._ldap_server = MockLdapServer(ADMIN_DN, ADMIN_PASSWORD, LDAP_USERS)
        cls._ldap_server.start()
        os.environ[ADMIN_PASSWORD_ENV_VAR] = ADMIN_PASSWORD
        super().setup_class()

    @classmethod
    def teardown_class(cls):
        super().teardown_class()
        if cls._ldap_server:
            cls._ldap_server.stop()

    def setup_method(self, method):
        super().setup_method(method)
        # LDAP users must also exist in Cypress so the proxy can fetch ldap_password_revision.
        for uid in LDAP_USERS:
            create_user(uid)

    def teardown_method(self, method):
        super().teardown_method(method)
        remove("//sys/cypress_cookies/*", force=True)

    @classmethod
    def modify_http_proxy_config(cls, config, cluster_index, multidaemon_config, proxy_index):
        config.setdefault("auth", {})["ldap_service"] = {
            "host": "127.0.0.1",
            "port": cls._ldap_server.port,
            "admin_dn": ADMIN_DN,
            "admin_password_env_var": ADMIN_PASSWORD_ENV_VAR,
            "search_base": "dc=example,dc=com",
            "search_filter": "(uid={login})",
            "request_timeout": "5s",
            "encryption": "none",
        }

    def _proxy(self):
        return "http://" + self.Env.get_proxy_address()

    def _login(self, user, password):
        rsp = requests.get(
            self._proxy() + "/login",
            auth=HTTPBasicAuth(user, password))
        return rsp

    def _get_cookie(self, user, password):
        rsp = self._login(user, password)
        assert rsp.status_code == 200, f"login failed: {rsp.text}"
        return rsp.cookies["YTCypressCookie"]

    @authors("nadya73")
    def test_successful_login(self):
        cookie = self._get_cookie("alice", "alice_pass")
        assert cookie

    @authors("nadya73")
    def test_second_user(self):
        cookie = self._get_cookie("bob", "bob_pass")
        assert cookie

    @authors("nadya73")
    def test_wrong_password(self):
        rsp = self._login("alice", "wrong_pass")
        assert rsp.status_code == 401

    @authors("nadya73")
    def test_unknown_user(self):
        rsp = self._login("charlie", "any_pass")
        assert rsp.status_code == 401

    @authors("nadya73")
    def test_empty_password(self):
        rsp = self._login("alice", "")
        assert rsp.status_code == 401

    @authors("nadya73")
    def test_ldap_password_revision_attribute(self):
        """Cookie should use ldap_password_revision, not password_revision."""
        set("//sys/users/alice/@ldap_password_revision", 42)

        cookie = self._get_cookie("alice", "alice_pass")

        revision = get(f"//sys/cypress_cookies/{cookie}/password_revision")
        assert revision == 42

    @authors("nadya73")
    def test_cookie_source_is_ldap(self):
        """Cookie created via LDAP login must have auth_source=ldap."""
        cookie = self._get_cookie("alice", "alice_pass")

        auth_source = get(f"//sys/cypress_cookies/{cookie}/auth_source")
        assert auth_source == "ldap"

    @authors("nadya73")
    def test_ldap_cookie_not_renewed(self):
        """LDAP cookies must not be auto-renewed (renewal would bypass short TTL)."""
        import time

        cookie = self._get_cookie("alice", "alice_pass")

        # Wait into renewal window (> 2s after issue).
        time.sleep(2.2)

        rsp = requests.get(
            self._proxy() + "/api/v4/get?path=//@",
            cookies={"YTCypressCookie": cookie})

        # Request should succeed (cookie still valid) but no new cookie issued.
        assert rsp.status_code == 200
        assert "YTCypressCookie" not in rsp.cookies, \
            "LDAP cookie must not be auto-renewed"
