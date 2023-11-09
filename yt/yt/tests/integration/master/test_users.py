from yt_env_setup import YTEnvSetup, Restarter, MASTERS_SERVICE

from yt_commands import (
    authors, wait, create, ls, get, set, exists, remove,
    create_account, create_network_project,
    create_user, create_group, create_tablet_cell_bundle, make_ace,
    add_member, remove_member, remove_group, remove_user,
    remove_network_project, start_transaction, raises_yt_error,
    set_user_password, issue_token, revoke_token, list_user_tokens,
    build_snapshot,
)

import yt_error_codes

from yt_helpers import profiler_factory

from yt.environment.helpers import assert_items_equal
from yt.common import YtError

import pytest
import builtins
import datetime


##################################################################


class TestUsers(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 0

    DELTA_MASTER_CONFIG = {
        "object_service": {
            "sticky_user_error_expire_time": 0
        }
    }

    @authors("babenko", "ignat")
    def test_user_ban1(self):
        create_user("u")

        assert not get("//sys/users/u/@banned")
        get("//tmp", authenticated_user="u")

        set("//sys/users/u/@banned", True)
        assert get("//sys/users/u/@banned")
        with pytest.raises(YtError):
            get("//tmp", authenticated_user="u")

        set("//sys/users/u/@banned", False)
        assert not get("//sys/users/u/@banned")

        get("//tmp", authenticated_user="u")

    @authors("babenko", "ignat", "gritukan")
    def test_ban_superuser(self):
        def is_banned(user):
            try:
                get("//tmp", authenticated_user=user)
                return False
            except YtError:
                return True

        create_user("u")
        add_member("u", "superusers")
        assert not is_banned("u")
        set("//sys/users/u/@banned", True)
        assert is_banned("u")

        assert not is_banned("root")
        with pytest.raises(YtError):
            set("//sys/users/root/@banned", True)
        assert not is_banned("root")

    @authors("babenko")
    def test_request_rate_limit1(self):
        create_user("u")
        with pytest.raises(YtError):
            set("//sys/users/u/@read_request_rate_limit", -1)
        with pytest.raises(YtError):
            set("//sys/users/u/@write_request_rate_limit", -1)

    @authors("babenko")
    def test_request_rate_limit2(self):
        create_user("u")
        set("//sys/users/u/@request_rate_limit", 1)

    @authors("babenko")
    def test_request_queue_size_limit1(self):
        create_user("u")
        with pytest.raises(YtError):
            set("//sys/users/u/@request_queue_size_limit", -1)

    @authors("babenko")
    def test_request_queue_size_limit2(self):
        create_user("u")
        set("//sys/users/u/@request_queue_size_limit", 1)

    @authors("babenko")
    def test_request_queue_size_limit3(self):
        create_user("u")
        set("//sys/users/u/@request_queue_size_limit", 0)
        with pytest.raises(YtError):
            ls("/", authenticated_user="u")
        set("//sys/users/u/@request_queue_size_limit", 1)
        ls("/", authenticated_user="u")

    @authors("aozeritsky")
    def test_request_limits_per_cell(self):
        create_user("u")
        set("//sys/users/u/@request_limits/read_request_rate/default", 1337)
        assert get("//sys/users/u/@request_limits/read_request_rate/default") == 1337

        set("//sys/users/u/@request_limits/read_request_rate/per_cell", {"10": 1338})
        assert get("//sys/users/u/@request_limits/read_request_rate/per_cell/10") == 1338

    @authors("babenko")
    def test_builtin_init(self):
        assert_items_equal(get("//sys/groups/everyone/@members"), ["users", "guest"])
        assert_items_equal(
            get("//sys/groups/users/@members"),
            ["superusers", "owner"],
        )
        assert_items_equal(
            get("//sys/groups/superusers/@members"),
            [
                "root",
                "scheduler",
                "job",
                "replicator",
                "file_cache",
                "operations_cleaner",
                "operations_client",
                "tablet_cell_changelogger",
                "tablet_cell_snapshotter",
                "table_mount_informer",
                "alien_cell_synchronizer",
                "queue_agent",
                "tablet_balancer",
            ],
        )

        assert_items_equal(get("//sys/users/root/@member_of"), ["superusers"])
        assert_items_equal(get("//sys/users/guest/@member_of"), ["everyone"])
        assert_items_equal(get("//sys/users/scheduler/@member_of"), ["superusers"])
        assert_items_equal(get("//sys/users/job/@member_of"), ["superusers"])
        assert_items_equal(get("//sys/users/replicator/@member_of"), ["superusers"])
        assert_items_equal(get("//sys/users/file_cache/@member_of"), ["superusers"])
        assert_items_equal(get("//sys/users/operations_cleaner/@member_of"), ["superusers"])
        assert_items_equal(get("//sys/users/tablet_cell_changelogger/@member_of"), ["superusers"])
        assert_items_equal(get("//sys/users/tablet_cell_snapshotter/@member_of"), ["superusers"])
        assert_items_equal(get("//sys/users/table_mount_informer/@member_of"), ["superusers"])
        assert_items_equal(get("//sys/users/queue_agent/@member_of"), ["superusers"])
        assert_items_equal(get("//sys/users/tablet_balancer/@member_of"), ["superusers"])

        assert_items_equal(
            get("//sys/users/root/@member_of_closure"),
            ["superusers", "users", "everyone"],
        )
        assert_items_equal(get("//sys/users/guest/@member_of_closure"), ["everyone"])
        assert_items_equal(
            get("//sys/users/scheduler/@member_of_closure"),
            ["superusers", "users", "everyone"],
        )
        assert_items_equal(
            get("//sys/users/job/@member_of_closure"),
            ["superusers", "users", "everyone"],
        )
        assert_items_equal(
            get("//sys/users/replicator/@member_of_closure"),
            ["superusers", "users", "everyone"],
        )
        assert_items_equal(
            get("//sys/users/file_cache/@member_of_closure"),
            ["superusers", "users", "everyone"],
        )
        assert_items_equal(
            get("//sys/users/operations_cleaner/@member_of_closure"),
            ["superusers", "users", "everyone"],
        )
        assert_items_equal(
            get("//sys/users/tablet_cell_changelogger/@member_of_closure"),
            ["superusers", "users", "everyone"],
        )
        assert_items_equal(
            get("//sys/users/tablet_cell_snapshotter/@member_of_closure"),
            ["superusers", "users", "everyone"],
        )
        assert_items_equal(
            get("//sys/users/table_mount_informer/@member_of_closure"),
            ["superusers", "users", "everyone"],
        )
        assert_items_equal(
            get("//sys/users/queue_agent/@member_of_closure"),
            ["superusers", "users", "everyone"],
        )
        assert_items_equal(
            get("//sys/users/tablet_balancer/@member_of_closure"),
            ["superusers", "users", "everyone"],
        )

    @authors("babenko", "ignat")
    def test_create_user1(self):
        create_user("max")
        assert get("//sys/users/max/@name") == "max"
        assert "max" in get("//sys/groups/users/@members")
        assert get("//sys/users/max/@member_of") == ["users"]

    @authors("babenko", "ignat")
    def test_create_user2(self):
        create_user("max")
        with pytest.raises(YtError):
            create_user("max")
        with pytest.raises(YtError):
            create_group("max")

    @authors("babenko", "ignat")
    def test_create_group1(self):
        create_group("devs")
        assert get("//sys/groups/devs/@name") == "devs"

    @authors("babenko", "ignat")
    def test_create_group2(self):
        create_group("devs")
        with pytest.raises(YtError):
            create_user("devs")
        with pytest.raises(YtError):
            create_group("devs")

    @authors("babenko", "ignat")
    def test_user_remove_builtin(self):
        with pytest.raises(YtError):
            remove_user("root")
        with pytest.raises(YtError):
            remove_user("guest")

    @authors("babenko", "ignat")
    def test_group_remove_builtin(self):
        with pytest.raises(YtError):
            remove_group("everyone")
        with pytest.raises(YtError):
            remove_group("users")

    @authors("ignat")
    def test_membership1(self):
        create_user("max")
        create_group("devs")
        add_member("max", "devs")

        assert get("//sys/groups/devs/@members") == ["max"]
        assert get("//sys/groups/devs/@members") == ["max"]

        with raises_yt_error(yt_error_codes.IsAlreadyPresentInGroup):
            add_member("max", "devs")

    @authors("asaitgalin", "babenko", "ignat")
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

    @authors("babenko", "ignat")
    def test_membership3(self):
        create_group("g1")
        create_group("g2")
        create_group("g3")

        add_member("g2", "g1")
        add_member("g3", "g2")
        with pytest.raises(YtError):
            add_member("g1", "g3")

    @authors("ignat")
    def test_membership4(self):
        create_user("u")
        create_group("g")
        add_member("u", "g")
        remove_user("u")
        assert get("//sys/groups/g/@members") == []

    @authors("ignat")
    def test_membership5(self):
        create_user("u")
        create_group("g")
        add_member("u", "g")
        assert sorted(get("//sys/users/u/@member_of")) == sorted(["g", "users"])
        remove_group("g")
        assert get("//sys/users/u/@member_of") == ["users"]

    @authors("babenko", "ignat")
    def test_membership6(self):
        create_user("u")
        create_group("g")

        with pytest.raises(YtError):
            remove_member("u", "g")

        add_member("u", "g")
        with pytest.raises(YtError):
            add_member("u", "g")

    @authors("babenko")
    def test_membership7(self):
        create_group("g")
        with pytest.raises(YtError):
            add_member("g", "g")

    @authors("ignat")
    def test_modify_builtin(self):
        create_user("u")
        with pytest.raises(YtError):
            remove_member("u", "everyone")
        with pytest.raises(YtError):
            remove_member("u", "users")
        with pytest.raises(YtError):
            add_member("u", "everyone")
        with pytest.raises(YtError):
            add_member("u", "users")

    @authors("babenko")
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

    @authors("asaitgalin", "babenko")
    def test_remove_group(self):
        create_user("u")
        create_group("g")
        add_member("u", "g")

        assert sorted(get("//sys/users/u/@member_of")) == sorted(["g", "users"])
        assert sorted(get("//sys/users/u/@member_of_closure")) == sorted(["g", "users", "everyone"])

        remove_group("g")

        assert get("//sys/users/u/@member_of") == ["users"]
        assert sorted(get("//sys/users/u/@member_of_closure")) == sorted(["users", "everyone"])

    @authors("prime")
    def test_prerequisite_transactions(self):
        create_group("g8")

        with pytest.raises(YtError):
            add_member("root", "g8", prerequisite_transaction_ids=["a-b-c-d"])

        with pytest.raises(YtError):
            remove_member("root", "g8", prerequisite_transaction_ids=["a-b-c-d"])

        tx = start_transaction()
        add_member("root", "g8", prerequisite_transaction_ids=[tx])
        remove_member("root", "g8", prerequisite_transaction_ids=[tx])

    @authors("shakurov")
    def test_usable_accounts(self):
        create_user("u")

        create_account("a1")
        create_account("a2")

        assert sorted(get("//sys/users/u/@usable_accounts")) == [
            "intermediate",
            "tmp",
        ]  # these are defaults

        set("//sys/accounts/a1/@acl", [make_ace("allow", "u", "use")])

        assert sorted(get("//sys/users/u/@usable_accounts")) == [
            "a1",
            "intermediate",
            "tmp",
        ]

        create_group("g")

        set("//sys/accounts/a2/@acl", [make_ace("allow", "g", "use")])

        assert sorted(get("//sys/users/u/@usable_accounts")) == [
            "a1",
            "intermediate",
            "tmp",
        ]

        add_member("u", "g")

        assert sorted(get("//sys/users/u/@usable_accounts")) == [
            "a1",
            "a2",
            "intermediate",
            "tmp",
        ]

    @authors("s-v-m")
    def test_usable_tablet_cell_bundles(self):
        create_user("u")
        create_tablet_cell_bundle("tcb1")
        create_tablet_cell_bundle("tcb2")
        set("//sys/tablet_cell_bundles/tcb1/@acl", [make_ace("allow", "u", "use")])
        assert sorted(get("//sys/users/u/@usable_tablet_cell_bundles")) == ['default', "sequoia", "tcb1"]
        create_group("g")
        add_member("u", "g")
        set("//sys/tablet_cell_bundles/tcb2/@acl", [make_ace("allow", "g", "use")])
        assert sorted(get("//sys/users/u/@usable_tablet_cell_bundles")) == ['default', "sequoia", "tcb1", "tcb2"]

    @authors("babenko", "kiselyovp")
    def test_delayed_membership_closure(self):
        create_group("g1")
        create_group("g2")
        create_user("u")
        add_member("g1", "g2")

        set(
            "//sys/@config/security_manager/membership_closure_recomputation_period",
            3000,
        )
        set(
            "//sys/@config/security_manager/enable_delayed_membership_closure_recomputation",
            True,
        )
        add_member("u", "g1")

        wait(lambda: builtins.set(["g1", "g2"]) <= builtins.set(get("//sys/users/u/@member_of_closure")))

    @authors("gritukan")
    def test_network_projects(self):
        create_network_project("a")

        with pytest.raises(YtError):
            create_network_project("a")

        set("//sys/network_projects/a/@project_id", 123)
        assert get("//sys/network_projects/a/@project_id") == 123

        with pytest.raises(YtError):
            set("//sys/network_projects/a/@project_id", "abc")

        with pytest.raises(YtError):
            set("//sys/network_projects/a/@project_id", -1)

        set("//sys/network_projects/a/@name", "b")
        assert not exists("//sys/network_projects/a")
        assert get("//sys/network_projects/b/@project_id") == 123

        remove_network_project("b")
        assert not exists("//sys/network_projects/b")

    @authors("gritukan")
    def test_network_projects_acl(self):
        create_user("u1")
        create_user("u2")

        create_network_project("a")

        set("//sys/network_projects/a/@acl", [make_ace("allow", "u1", "use")])
        assert sorted(get("//sys/users/u1/@usable_network_projects")) == ["a"]
        assert get("//sys/users/u2/@usable_network_projects") == []

        create_group("g")
        set("//sys/network_projects/a/@acl", [make_ace("allow", "g", "use")])
        add_member("u2", "g")
        assert sorted(get("//sys/users/u2/@usable_network_projects")) == ["a"]

    @authors("ifsmirnov")
    def test_create_non_external_table(self):
        create("table", "//tmp/t1", attributes={"external": False})

        create_user("u")
        with pytest.raises(YtError):
            create(
                "table",
                "//tmp/t2",
                attributes={"external": False},
                authenticated_user="u",
            )

        set("//sys/users/u/@allow_external_false", True)
        create("table", "//tmp/t3", attributes={"external": False}, authenticated_user="u")

        set("//sys/users/u/@allow_external_false", False)
        with pytest.raises(YtError):
            create(
                "table",
                "//tmp/t4",
                attributes={"external": False},
                authenticated_user="u",
            )

    @authors("aleksandra-zh")
    def test_distributed_throttler_simple(self):
        create_user("u")

        set("//sys/@config/security_manager/enable_distributed_throttler", True)
        get("//tmp", authenticated_user="u")

        set("//sys/@config/security_manager/enable_distributed_throttler", False)
        get("//tmp", authenticated_user="u")

    @authors("aleksandra-zh")
    def test_distributed_throttler_profiler(self):
        create_user("u")

        set("//sys/@config/security_manager/enable_distributed_throttler", True)
        get("//tmp", authenticated_user="u")

        master_address = ls("//sys/primary_masters")[0]
        profiler = profiler_factory().at_primary_master(master_address)

        def get_throttler_gauge(name):
            # Provoke limits update
            get("//tmp", authenticated_user="u")

            path = "security/distributed_throttler/{}".format(name)
            gauge = profiler.gauge(name=path, fixed_tags={"throttler_id": "u:request_count:Read"}).get()
            if gauge is None:
                return 0
            return gauge

        wait(lambda: get_throttler_gauge("usage") > 0)
        wait(lambda: get_throttler_gauge("limit") > 0)

        for _ in range(5):
            with Restarter(self.Env, MASTERS_SERVICE):
                pass

            wait(lambda: get_throttler_gauge("usage") > 0)
            wait(lambda: get_throttler_gauge("limit") > 0)

    @authors("gritukan")
    def test_recomute_membership_closure_on_group_destruction(self):
        create_user("u")
        create_group("g1")
        create_group("g2")

        add_member("u", "g1")
        add_member("g1", "g2")

        assert_items_equal(get("//sys/users/u/@member_of_closure"), ["g1", "g2", "everyone", "users"])
        assert_items_equal(get("//sys/groups/g1/@member_of_closure"), ["g2"])

        # Disable periodic membership closure recomputation.
        set("//sys/@config/security_manager/enable_delayed_membership_closure_recomputation", True)
        set("//sys/@config/security_manager/membership_closure_recomputation_period", 10**8)

        remove_group("g2")

        assert_items_equal(get("//sys/users/u/@member_of_closure"), ["g1", "everyone", "users"])
        assert_items_equal(get("//sys/groups/g1/@member_of_closure"), [])

    @authors("gritukan")
    def test_set_user_password(self):
        if self.DRIVER_BACKEND == "rpc":
            return

        create_user("u")
        create_user("v")

        assert not exists("//sys/users/u/@hashed_password")
        assert not exists("//sys/users/u/@password_salt")
        assert not exists("//sys/users/u/@password")
        rev1 = get("//sys/users/u/@password_revision")

        set_user_password("u", "admin")
        enc2 = get("//sys/users/u/@hashed_password")
        assert len(enc2) == 64
        salt2 = get("//sys/users/u/@password_salt")
        assert len(salt2) == 32
        rev2 = get("//sys/users/u/@password_revision")
        assert rev2 > rev1

        with raises_yt_error("User provided invalid password"):
            set_user_password("u", "admin2", authenticated_user="u")
        with raises_yt_error("User provided invalid password"):
            set_user_password("u", "admin2", "123456", authenticated_user="u")
        with raises_yt_error("Password change can be performed either"):
            set_user_password("u", "admin2", "admin", authenticated_user="v")
        set_user_password("u", "admin2", "admin", authenticated_user="u")

        enc3 = get("//sys/users/u/@hashed_password")
        assert enc3 != enc2
        salt3 = get("//sys/users/u/@password_salt")
        assert salt3 != salt2
        rev3 = get("//sys/users/u/@password_revision")
        assert rev3 > rev2

        add_member("v", "superusers")

        # Same password, another salt.
        set_user_password("u", "admin2", authenticated_user="v")
        set("//sys/users/u/@password", "admin")
        assert get("//sys/users/u/@hashed_password") != enc3
        assert get("//sys/users/u/@password_salt") != salt3
        rev3 = get("//sys/users/u/@password_revision")
        assert rev3 > rev2

        remove("//sys/users/u/@hashed_password")
        remove("//sys/users/u/@password_salt")
        assert not exists("//sys/users/u/@hashed_password")
        assert not exists("//sys/users/u/@password_salt")
        rev4 = get("//sys/users/u/@password_revision")
        assert rev4 > rev3

    @authors("gritukan")
    def test_tokens(self):
        if self.DRIVER_BACKEND == "rpc":
            return

        create_user("u")
        create_user("v")
        set_user_password("u", "u")
        set_user_password("v", "v")

        _, t1_hash = issue_token("u")
        assert get(f"//sys/cypress_tokens/{t1_hash}/@user") == "u"
        assert_items_equal(list_user_tokens("u"), [t1_hash])
        assert list_user_tokens("v") == []

        _, t2_hash = issue_token("u", "u", authenticated_user="u")
        assert_items_equal(list_user_tokens("u"), [t1_hash, t2_hash])

        with raises_yt_error("User provided invalid password"):
            issue_token("u", "a", authenticated_user="u")
        with raises_yt_error("Token issuance can be performed"):
            issue_token("u", "v", authenticated_user="v")

        with raises_yt_error("Provided token is not recognized"):
            revoke_token("u", "xxx", "u", authenticated_user="u")
        with raises_yt_error("User provided invalid password"):
            revoke_token("u", t1_hash, "a", authenticated_user="u")
        with raises_yt_error("Provided token is not recognized"):
            revoke_token("v", t1_hash, "v", authenticated_user="v")
        with raises_yt_error("Token revokation can be performed"):
            revoke_token("u", t1_hash, "v", authenticated_user="v")
        assert_items_equal(list_user_tokens("u"), [t1_hash, t2_hash])

        revoke_token("u", t1_hash)
        assert_items_equal(list_user_tokens("u"), [t2_hash])

        revoke_token("u", t2_hash, "u", authenticated_user="u")
        assert list_user_tokens("u") == []

    @authors("shakurov")
    def test_user_request_profiling(self):
        master_address = ls("//sys/primary_masters")[0]
        profiler = profiler_factory().at_primary_master(master_address)

        def check_profiling_counters(user_name, should_exist):
            read_time = profiler.counter("security/user_read_time", tags={"user": user_name})
            write_time = profiler.counter("security/user_write_time", tags={"user": user_name})
            read_request_count = profiler.counter("security/user_read_request_count", tags={"user": user_name})
            write_request_count = profiler.counter("security/user_write_request_count", tags={"user": user_name})
            request_count = profiler.counter("security/user_request_count", tags={"user": user_name})
            request_queue_size = profiler.counter("security/user_request_queue_size", tags={"user": user_name})

            wait(lambda: (read_time.get() is not None) == should_exist)
            wait(lambda: (write_time.get() is not None) == should_exist)
            wait(lambda: (write_request_count.get() is not None) == should_exist)
            wait(lambda: (read_request_count.get() is not None) == should_exist)
            wait(lambda: (request_count.get() is not None) == should_exist)
            wait(lambda: (request_queue_size.get() is not None) == should_exist)

        create_user("u")
        check_profiling_counters("u", True)

        set("//sys/users/u/@name", "v")
        assert not exists("//sys/users/u")
        assert exists("//sys/users/v")

        check_profiling_counters("u", False)
        check_profiling_counters("v", True)

        build_snapshot(cell_id=None)

        # Shutdown masters and wait a bit.
        with Restarter(self.Env, MASTERS_SERVICE):
            pass

        check_profiling_counters("u", False)
        check_profiling_counters("v", True)

    def _get_last_seen_time(self, username):
        last_seen_yson = get(f"//sys/users/{username}/@last_seen_time")
        return datetime.datetime.strptime(last_seen_yson, "%Y-%m-%dT%H:%M:%S.%fZ")

    @authors("cherepashka")
    def test_last_seen_time_is_increasing(self):
        create_user("u")
        wait(lambda: self._get_last_seen_time("u").year > 1970, timeout=10)
        last_seen = self._get_last_seen_time("u")

        for table_ind in range(5):
            create("table", f"//tmp/t{table_ind}", authenticated_user="u")
            wait(lambda: self._get_last_seen_time("u") > last_seen, timeout=2)
            assert self._get_last_seen_time("u") - last_seen < datetime.timedelta(seconds=2)
            last_seen = self._get_last_seen_time("u")

        last_seen = self._get_last_seen_time("u")
        set("//sys/users/u/@name", "v", authenticated_user="u")
        wait(lambda: self._get_last_seen_time("v") > last_seen, timeout=2)


##################################################################


class TestUsersRpcProxy(TestUsers):
    NUM_MASTERS = 1
    NUM_NODES = 5
    NUM_SCHEDULERS = 1
    USE_DYNAMIC_TABLES = True
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True
    ENABLE_HTTP_PROXY = True


##################################################################


class TestUsersMulticell(TestUsers):
    NUM_SECONDARY_MASTER_CELLS = 2

    @authors("aleksandra-zh")
    def test_request_limit_cell_names(self):
        create_user("u")
        set("//sys/@config/multicell_manager/cell_descriptors/10", {"name": "Julia"})
        set("//sys/users/u/@request_limits/read_request_rate/per_cell", {"Julia": 100, "11": 200})
        assert get("//sys/users/u/@request_limits/read_request_rate/per_cell") == {"Julia": 100, "11": 200}

        set("//sys/@config/multicell_manager/cell_descriptors/11", {"name": "George"})
        assert get("//sys/users/u/@request_limits/read_request_rate/per_cell") == {"Julia": 100, "George": 200}

    @authors("cherepashka")
    def test_last_seen_via_visit_portal(self):
        # Make sure secondary cell can host portal entries.
        set("//sys/@config/multicell_manager/cell_descriptors", {"11": {"roles": ["cypress_node_host"]}})

        create_user("u")
        wait(lambda: self._get_last_seen_time("u").year > 1970, timeout=10)
        last_seen = self._get_last_seen_time("u")

        create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 11})
        create("table", "//tmp/p/t", authenticated_user="u")
        wait(lambda: self._get_last_seen_time("u") > last_seen, timeout=2)
        assert self._get_last_seen_time("u") - last_seen < datetime.timedelta(seconds=2)
