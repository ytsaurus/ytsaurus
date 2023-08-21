from yt_env_setup import YTEnvSetup

from yt_commands import (
    authors, create, get, set, ls, exists,
    create_proxy_role, remove_proxy_role, create_user, create_group,
    make_ace, add_member, check_permission)

from yt.common import YtError

import pytest

##################################################################


class TestProxyRoles(YTEnvSetup):
    NUM_MASTERS = 1

    def setup_class(cls):
        super(TestProxyRoles, cls).setup_class()
        create("http_proxy_role_map", "//sys/http_proxy_roles")
        create("rpc_proxy_role_map", "//sys/rpc_proxy_roles")

    @authors("gritukan")
    @pytest.mark.parametrize("proxy_kind", ["http", "rpc"])
    def test_simple(self, proxy_kind):
        opposite_kind = "rpc" if proxy_kind == "http" else "http"

        create_proxy_role("r", proxy_kind)
        assert ls("//sys/{}_proxy_roles".format(proxy_kind)) == ["r"]
        assert ls("//sys/{}_proxy_roles".format(opposite_kind)) == []
        assert get("//sys/{}_proxy_roles/r/@name".format(proxy_kind)) == "r"
        assert get("//sys/{}_proxy_roles/r/@proxy_kind".format(proxy_kind)) == proxy_kind

        with pytest.raises(YtError):
            create_proxy_role("r", proxy_kind)
        with pytest.raises(YtError):
            set("//sys/{}_proxy_roles/r/@name".format(proxy_kind), "foo")
        with pytest.raises(YtError):
            set("//sys/{}_proxy_roles/r/@proxy_kind".format(proxy_kind), opposite_kind)
        with pytest.raises(YtError):
            get("//sys/{}_proxy_roles/foo/@acl".format(proxy_kind))

        remove_proxy_role("r", proxy_kind)
        assert ls("//sys/{}_proxy_roles".format(proxy_kind)) == []
        with pytest.raises(YtError):
            remove_proxy_role("r", proxy_kind)

    @authors("gritukan")
    def test_invalid(self):
        with pytest.raises(YtError):
            create_proxy_role("", "http")
        with pytest.raises(YtError):
            create_proxy_role("r", "foo")

    @authors("gritukan")
    def test_acl(self):
        create_proxy_role("r", "http")
        create_user("u1")
        create_user("u2")
        create_group("g")

        set("//sys/http_proxy_roles/r/@acl", [make_ace("allow", "u1", "use")])
        assert check_permission("u1", "use", "//sys/http_proxy_roles/r")["action"] == "allow"

        set("//sys/http_proxy_roles/r/@acl", [make_ace("allow", "g", "use")])
        assert check_permission("u1", "use", "//sys/http_proxy_roles/r")["action"] == "deny"
        assert check_permission("u2", "use", "//sys/http_proxy_roles/r")["action"] == "deny"
        add_member("u2", "g")
        assert check_permission("u2", "use", "//sys/http_proxy_roles/r")["action"] == "allow"

    @authors("gritukan")
    def test_namesakes(self):
        create_proxy_role("r", "http")
        create_proxy_role("r", "rpc")

        create_user("u")
        set("//sys/http_proxy_roles/r/@acl", [make_ace("deny", "u", "use")])
        set("//sys/rpc_proxy_roles/r/@acl", [make_ace("allow", "u", "use")])
        assert check_permission("u", "use", "//sys/http_proxy_roles/r")["action"] == "deny"
        assert check_permission("u", "use", "//sys/rpc_proxy_roles/r")["action"] == "allow"

        remove_proxy_role("r", "http")
        assert not exists("//sys/http_proxy_roles/r")
        assert exists("//sys/rpc_proxy_roles/r")
