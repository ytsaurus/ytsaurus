import pytest

from yt_env_setup import YTEnvSetup
from yt_commands import *

from time import sleep

from yt.environment.helpers import assert_items_equal

##################################################################

class TestUsers(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 0

    def test_user_ban1(self):
        create_user("u")

        assert not get("//sys/users/u/@banned")
        get("//tmp", authenticated_user="u")

        set("//sys/users/u/@banned", True)
        assert get("//sys/users/u/@banned")
        with pytest.raises(YtError): get("//tmp", authenticated_user="u")

        set("//sys/users/u/@banned", False)
        assert not get("//sys/users/u/@banned")

        get("//tmp", authenticated_user="u")

    def test_user_ban2(self):
        with pytest.raises(YtError): set("//sys/users/root/@banned", True)

    def test_request_rate_limit1(self):
        create_user("u")
        with pytest.raises(YtError): set("//sys/users/u/@read_request_rate_limit", -1)
        with pytest.raises(YtError): set("//sys/users/u/@write_request_rate_limit", -1)

    def test_request_rate_limit2(self):
        create_user("u")
        set("//sys/users/u/@request_rate_limit", 1)

    def test_request_queue_size_limit1(self):
        create_user("u")
        with pytest.raises(YtError): set("//sys/users/u/@request_queue_size_limit", -1)

    def test_request_queue_size_limit2(self):
        create_user("u")
        set("//sys/users/u/@request_queue_size_limit", 1)

    def test_request_queue_size_limit3(self):
        create_user("u")
        set("//sys/users/u/@request_queue_size_limit", 0)
        with pytest.raises(YtError): ls("/", authenticated_user="u")
        set("//sys/users/u/@request_queue_size_limit", 1)
        ls("/", authenticated_user="u")

    def test_access_counter1(self):
        create_user("u")
        assert get("//sys/users/u/@request_count") == 0

        ls("//tmp", authenticated_user="u")
        sleep(1.0)
        assert get("//sys/users/u/@request_count") == 1

    def test_access_counter2(self):
        create_user('u')
        with pytest.raises(YtError): set('//sys/users/u/@request_count', -1.0)

    def test_access_counter3(self):
        create_user('u')

        assert get("//sys/users/u/@request_count") == 0

        # Transaction ping is not accounted in request counter
        tx = start_transaction()
        ping_transaction(tx, authenticated_user='u')

        assert get("//sys/users/u/@request_count") == 0

    def test_builtin_init(self):
        assert_items_equal(get("//sys/groups/everyone/@members"),
            ["users", "guest"])
        assert_items_equal(get("//sys/groups/users/@members"),
            ["superusers", "owner", "application_operations"])
        assert_items_equal(get("//sys/groups/superusers/@members"),
            ["root", "scheduler", "job", "replicator", "file_cache", "application_operations", "operations_cleaner"])

        assert_items_equal(get("//sys/users/root/@member_of"), ["superusers"])
        assert_items_equal(get("//sys/users/guest/@member_of"), ["everyone"])
        assert_items_equal(get("//sys/users/scheduler/@member_of"), ["superusers"])
        assert_items_equal(get("//sys/users/job/@member_of"), ["superusers"])
        assert_items_equal(get("//sys/users/replicator/@member_of"), ["superusers"])
        assert_items_equal(get("//sys/users/file_cache/@member_of"), ["superusers"])
        assert_items_equal(get("//sys/users/application_operations/@member_of"), ["superusers", "users"])
        assert_items_equal(get("//sys/users/operations_cleaner/@member_of"), ["superusers"])

        assert_items_equal(get("//sys/users/root/@member_of_closure"), ["superusers", "users", "everyone"])
        assert_items_equal(get("//sys/users/guest/@member_of_closure"), ["everyone"])
        assert_items_equal(get("//sys/users/scheduler/@member_of_closure"), ["superusers", "users", "everyone"])
        assert_items_equal(get("//sys/users/job/@member_of_closure"), ["superusers", "users", "everyone"])
        assert_items_equal(get("//sys/users/replicator/@member_of_closure"), ["superusers", "users", "everyone"])
        assert_items_equal(get("//sys/users/file_cache/@member_of_closure"), ["superusers", "users", "everyone"])
        assert_items_equal(get("//sys/users/application_operations/@member_of_closure"), ["superusers", "users", "everyone"])
        assert_items_equal(get("//sys/users/operations_cleaner/@member_of_closure"), ["superusers", "users", "everyone"])

    def test_create_user1(self):
        create_user("max")
        assert get("//sys/users/max/@name") == "max"
        assert "max" in get("//sys/groups/users/@members")
        assert get("//sys/users/max/@member_of") == ["users"]

    def test_create_user2(self):
        create_user("max")
        with pytest.raises(YtError): create_user("max")
        with pytest.raises(YtError): create_group("max")

    def test_create_group1(self):
        create_group("devs")
        assert get("//sys/groups/devs/@name") == "devs"

    def test_create_group2(self):
        create_group("devs")
        with pytest.raises(YtError): create_user("devs")
        with pytest.raises(YtError): create_group("devs")

    def test_user_remove_builtin(self):
        with pytest.raises(YtError): remove_user("root")
        with pytest.raises(YtError): remove_user("guest")

    def test_group_remove_builtin(self):
        with pytest.raises(YtError): remove_group("everyone")
        with pytest.raises(YtError): remove_group("users")

    def test_membership1(self):
        create_user("max")
        create_group("devs")
        add_member("max", "devs")
        assert get("//sys/groups/devs/@members") == ["max"]
        assert get("//sys/groups/devs/@members") == ["max"]

    def test_membership2(self):
        create_user("u1")
        create_user("u2")
        create_group("g1")
        create_group("g2")

        add_member("u1", "g1")
        add_member("g2", "g1")
        add_member("u2", "g2")

        assert sorted(get("//sys/groups/g1/@members")) == sorted(["u1", "g2"])
        assert get("//sys/groups/g2/@members") == ["u2"]

        assert sorted(get("//sys/users/u1/@member_of")) == sorted(["g1", "users"])
        assert sorted(get("//sys/users/u2/@member_of")) == sorted(["g2", "users"])

        assert sorted(get("//sys/users/u1/@member_of_closure")) == sorted(["g1", "users", "everyone"])
        assert sorted(get("//sys/users/u2/@member_of_closure")) == sorted(["g1", "g2", "users", "everyone"])

        remove_member("g2", "g1")

        assert get("//sys/groups/g1/@members") == ["u1"]
        assert get("//sys/groups/g2/@members") == ["u2"]

        assert sorted(get("//sys/users/u1/@member_of")) == sorted(["g1", "users"])
        assert sorted(get("//sys/users/u2/@member_of")) == sorted(["g2", "users"])

        assert sorted(get("//sys/users/u1/@member_of_closure")) == sorted(["g1", "users", "everyone"])
        assert sorted(get("//sys/users/u2/@member_of_closure")) == sorted(["g2", "users", "everyone"])

    def test_membership3(self):
        create_group("g1")
        create_group("g2")
        create_group("g3")

        add_member("g2", "g1")
        add_member("g3", "g2")
        with pytest.raises(YtError): add_member("g1", "g3")

    def test_membership4(self):
        create_user("u")
        create_group("g")
        add_member("u", "g")
        remove_user("u")
        assert get("//sys/groups/g/@members") == []

    def test_membership5(self):
        create_user("u")
        create_group("g")
        add_member("u", "g")
        assert sorted(get("//sys/users/u/@member_of")) == sorted(["g", "users"])
        remove_group("g")
        assert get("//sys/users/u/@member_of") == ["users"]

    def test_membership6(self):
        create_user("u")
        create_group("g")

        with pytest.raises(YtError): remove_member("u", "g")

        add_member("u", "g")
        with pytest.raises(YtError): add_member("u", "g")

    def test_membership7(self):
        create_group("g")
        with pytest.raises(YtError): add_member("g", "g")

    def test_modify_builtin(self):
        create_user("u")
        with pytest.raises(YtError): remove_member("u", "everyone")
        with pytest.raises(YtError): remove_member("u", "users")
        with pytest.raises(YtError): add_member("u", "everyone")
        with pytest.raises(YtError): add_member("u", "users")

    def test_create_banned_user(self):
        create_user("u", attributes={"banned": True})
        users = ls("//sys/users", attributes=["banned"])
        assert get("//sys/users/u/@banned")
        found = False
        for item in users:
            if str(item) == "u":
                assert item.attributes["banned"]
                found = True
        assert found

    def test_remove_group(self):
        create_user("u")
        create_group("g")
        add_member("u", "g")

        assert sorted(get("//sys/users/u/@member_of")) == sorted(["g", "users"])
        assert sorted(get("//sys/users/u/@member_of_closure")) == sorted(["g", "users", "everyone"])

        remove_group("g")

        assert get("//sys/users/u/@member_of") == ["users"]
        assert sorted(get("//sys/users/u/@member_of_closure")) == sorted(["users", "everyone"])

    def test_usable_accounts(self):
        create_user("u")

        create_account("a1")
        create_account("a2")

        assert sorted(get("//sys/users/u/@usable_accounts")) == ["intermediate", "tmp"] # these are defaults

        set("//sys/accounts/a1/@acl", [make_ace("allow", "u", "use")])

        assert sorted(get("//sys/users/u/@usable_accounts")) == ["a1", "intermediate", "tmp"]

        create_group("g")

        acl = [make_ace("allow", "u", "use")]
        set("//sys/accounts/a2/@acl", [make_ace("allow", "g", "use")])

        assert sorted(get("//sys/users/u/@usable_accounts")) == ["a1", "intermediate", "tmp"]

        add_member("u", "g")

        assert sorted(get("//sys/users/u/@usable_accounts")) == ["a1", "a2", "intermediate", "tmp"]

##################################################################

class TestUsersRpcProxy(TestUsers):
    NUM_MASTERS = 3
    NUM_NODES = 5
    NUM_SCHEDULERS = 1
    USE_DYNAMIC_TABLES = True
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True
    ENABLE_PROXY = True


##################################################################

class TestUsersMulticell(TestUsers):
    NUM_SECONDARY_MASTER_CELLS = 2
