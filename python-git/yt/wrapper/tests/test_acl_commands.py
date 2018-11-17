import yt.wrapper as yt

import pytest

@pytest.mark.usefixtures("yt_env_with_rpc")
class TestAclCommands(object):
    def setup(self):
        yt.create("user", attributes={"name": "tester"})
        yt.create("account", attributes={"name": "tester"})
        yt.create("group", attributes={"name": "testers"})
        yt.create("group", attributes={"name": "super_testers"})

    def teardown(self):
        yt.remove("//sys/users/tester", force=True)
        yt.remove("//sys/groups/testers", force=True)
        yt.remove("//sys/accounts/tester", force=True)
        yt.remove("//sys/groups/super_testers", force=True)

    def test_check_permission(self, yt_env):
        assert yt.check_permission("tester", "read", "//sys")["action"] == "allow"
        assert yt.check_permission("tester", "write", "//sys")["action"] == "deny"
        assert yt.check_permission("root", "write", "//sys")["action"] == "allow"
        assert yt.check_permission("root", "administer", "//home")["action"] == "allow"
        assert yt.check_permission("root", "use", "//home")["action"] == "allow"
        permissions = ["read", "write", "administer", "remove"]
        yt.create("map_node", "//home/tester", attributes={"inherit_acl": "false",
            "acl": [{"action": "allow",
                     "subjects": ["tester"],
                     "permissions": permissions}]})
        try:
            for permission in permissions:
                assert yt.check_permission("tester", permission, "//home/tester")["action"] == "allow"
        finally:
            yt.remove("//home/tester", force=True)

    def test_add_remove_member(self):
        yt.add_member("tester", "testers")
        assert yt.get_attribute("//sys/groups/testers", "members") == ["tester"]
        assert set(yt.get_attribute("//sys/users/tester", "member_of")) == {"users", "testers"}
        assert set(yt.get_attribute("//sys/users/tester", "member_of_closure")) == {"users", "testers", "everyone"}

        yt.remove_member("tester", "testers")
        assert yt.get_attribute("//sys/groups/testers", "members") == []
        assert "testers" not in yt.get_attribute("//sys/users/tester", "member_of")

        yt.add_member("testers", "super_testers")
        assert yt.get_attribute("//sys/groups/testers", "member_of") == ["super_testers"]
        assert yt.get_attribute("//sys/groups/super_testers", "members") == ["testers"]
        yt.add_member("tester", "testers")
        assert "super_testers" in yt.get_attribute("//sys/users/tester", "member_of_closure")
        yt.remove_member("tester", "testers")

        yt.remove_member("testers", "super_testers")
        assert yt.get_attribute("//sys/groups/super_testers", "members") == []
        assert "super_testers" not in yt.get_attribute("//sys/groups/testers", "member_of")

