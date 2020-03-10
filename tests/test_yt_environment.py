from yt.test_helpers import unorderable_list_difference

import pytest


def _check_lists_equal(expected, actual):
    missing, unexpected = unorderable_list_difference(expected, actual)
    assert len(missing) == 0
    assert len(unexpected) == 0


@pytest.mark.usefixtures("yp_env")
class TestYTEnvironment(object):
    def test_accounts(self, yp_env):
        yt_client = yp_env.yt_client
        assert "yp" in yt_client.get("//sys/accounts")
        assert yt_client.check_permission("yp", "use", "//sys/accounts/yp")["action"] == "allow"

    def test_users(self, yp_env):
        assert "yp" in yp_env.yt_client.get("//sys/users")

    def test_groups(self, yp_env):
        assert "yp-devs" in yp_env.yt_client.get("//sys/groups")

    def test_acls(self, yp_env):
        yt_client = yp_env.yt_client

        expected_yp_effective_acls = [
            {
                "action": "allow",
                "inheritance_mode": "object_and_descendants",
                "subjects": ["yp"],
                "permissions": ["read", "write"],
            },
            {
                "action": "allow",
                "inheritance_mode": "object_and_descendants",
                "subjects": ["yp"],
                "permissions": ["read", "write", "remove", "mount"],
            },
            {
                "action": "allow",
                "inheritance_mode": "object_and_descendants",
                "subjects": ["admins"],
                "permissions": ["read", "write", "administer", "remove", "mount"],
            },
            {
                "action": "allow",
                "inheritance_mode": "object_and_descendants",
                "subjects": ["yp-devs"],
                "permissions": ["read", "write", "administer", "remove", "mount"],
            },
        ]

        assert yt_client.get("//yp/@inherit_acl") == False  # noqa
        _check_lists_equal(expected_yp_effective_acls, yp_env.yt_client.get("//yp/@effective_acl"))

        expected_yp_tokens_effective_acls = [
            {
                "action": "allow",
                "inheritance_mode": "object_and_descendants",
                "subjects": ["yp"],
                "permissions": ["read", "write"],
            },
            {
                "action": "allow",
                "inheritance_mode": "object_and_descendants",
                "subjects": ["admins"],
                "permissions": ["read", "write", "administer", "remove"],
            },
            {
                "action": "allow",
                "inheritance_mode": "object_and_descendants",
                "subjects": ["yp-devs"],
                "permissions": ["read", "write", "administer", "remove"],
            },
        ]

        assert yt_client.get("//yp/tokens/@inherit_acl") == False  # noqa
        _check_lists_equal(
            expected_yp_tokens_effective_acls, yp_env.yt_client.get("//yp/tokens/@effective_acl")
        )
