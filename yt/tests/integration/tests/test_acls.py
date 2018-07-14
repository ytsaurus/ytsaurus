import pytest

from yt_env_setup import YTEnvSetup, unix_only
from yt_commands import *

from yt.environment.helpers import assert_items_equal

##################################################################

class TestAcls(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    def test_empty_names_fail(self):
        with pytest.raises(YtError): create_user("")
        with pytest.raises(YtError): create_group("")

    def test_default_acl_sanity(self):
        create_user("u")
        with pytest.raises(YtError): set("/", {}, authenticated_user="u")
        with pytest.raises(YtError): set("//sys", {}, authenticated_user="u")
        with pytest.raises(YtError): set("//sys/a", "b", authenticated_user="u")
        with pytest.raises(YtError): set("/a", "b", authenticated_user="u")
        with pytest.raises(YtError): remove("//sys", authenticated_user="u")
        with pytest.raises(YtError): remove("//sys/tmp", authenticated_user="u")
        with pytest.raises(YtError): remove("//sys/home", authenticated_user="u")
        with pytest.raises(YtError): set("//sys/home/a", "b", authenticated_user="u")
        set("//tmp/a", "b", authenticated_user="u")
        ls("//tmp", authenticated_user="guest")
        with pytest.raises(YtError): set("//tmp/c", "d", authenticated_user="guest")

    def _test_denying_acl(self, rw_path, rw_user, acl_path, acl_subject):
        set(rw_path, "b", authenticated_user=rw_user)
        assert get(rw_path, authenticated_user=rw_user) == "b"

        set(rw_path, "c", authenticated_user=rw_user)
        assert get(rw_path, authenticated_user=rw_user) == "c"

        set(acl_path + "/@acl/end", make_ace("deny", acl_subject, ["write", "remove"]))
        with pytest.raises(YtError): set(rw_path, "d", authenticated_user=rw_user)
        assert get(rw_path, authenticated_user=rw_user) == "c"

        remove(acl_path + "/@acl/-1")
        set(acl_path + "/@acl/end", make_ace("deny", acl_subject, ["read", "write", "remove"]))
        with pytest.raises(YtError): get(rw_path, authenticated_user=rw_user)
        with pytest.raises(YtError): set(rw_path, "d", authenticated_user=rw_user)

    def test_denying_acl1(self):
        create_user("u")
        self._test_denying_acl("//tmp/a", "u", "//tmp/a", "u")

    def test_denying_acl2(self):
        create_user("u")
        create_group("g")
        add_member("u", "g")
        self._test_denying_acl("//tmp/a", "u", "//tmp/a", "g")

    def test_denying_acl3(self):
        create_user("u")
        set("//tmp/p", {})
        self._test_denying_acl("//tmp/p/a", "u", "//tmp/p", "u")

    def _test_allowing_acl(self, rw_path, rw_user, acl_path, acl_subject):
        set(rw_path, "a")

        with pytest.raises(YtError): set(rw_path, "b", authenticated_user=rw_user)

        set(acl_path + "/@acl/end", make_ace("allow", acl_subject, ["write", "remove"]))
        set(rw_path, "c", authenticated_user=rw_user)

        remove(acl_path + "/@acl/-1")
        set(acl_path + "/@acl/end", make_ace("allow", acl_subject, ["read"]))
        with pytest.raises(YtError): set(rw_path, "d", authenticated_user=rw_user)

    def test_allowing_acl1(self):
        self._test_allowing_acl("//tmp/a", "guest", "//tmp/a", "guest")

    def test_allowing_acl2(self):
        create_group("g")
        add_member("guest", "g")
        self._test_allowing_acl("//tmp/a", "guest", "//tmp/a", "g")

    def test_allowing_acl3(self):
        set("//tmp/p", {})
        self._test_allowing_acl("//tmp/p/a", "guest", "//tmp/p", "guest")

    def test_schema_acl1(self):
        create_user("u")
        create("table", "//tmp/t1", authenticated_user="u")
        set("//sys/schemas/table/@acl/end", make_ace("deny", "u", "create"))
        with pytest.raises(YtError): create("table", "//tmp/t2", authenticated_user="u")

    def test_schema_acl2(self):
        create_user("u")
        start_transaction(authenticated_user="u")
        set("//sys/schemas/transaction/@acl/end", make_ace("deny", "u", "create"))
        with pytest.raises(YtError): start_transaction(authenticated_user="u")

    def test_user_destruction(self):
        old_acl = get("//tmp/@acl")

        create_user("u")
        set("//tmp/@acl/end", make_ace("deny", "u", "write"))

        remove_user("u")
        assert get("//tmp/@acl") == old_acl

    def test_group_destruction(self):
        old_acl = get("//tmp/@acl")

        create_group("g")
        set("//tmp/@acl/end", make_ace("deny", "g", "write"))

        remove_group("g")
        assert get("//tmp/@acl") == old_acl

    def test_account_acl(self):
        create_account("a")
        create_user("u")

        with pytest.raises(YtError):
            create("table", "//tmp/t", authenticated_user="u", attributes={"account": "a"})

        create("table", "//tmp/t", authenticated_user="u")
        assert get("//tmp/t/@account") == "tmp"

        with pytest.raises(YtError):
            set("//tmp/t/@account", "a", authenticated_user="u")

        set("//sys/accounts/a/@acl/end", make_ace("allow", "u", "use"))
        set("//tmp/t/@acl/end", make_ace("deny", "u", "administer"))
        set("//tmp/t/@account", "a", authenticated_user="u")
        assert get("//tmp/t/@account") == "a"

    def test_init_acl_in_create(self):
        create_user("u1")
        create_user("u2")
        create("table", "//tmp/t", attributes={
            "inherit_acl": False,
            "acl" : [make_ace("allow", "u1", "write")]})
        set("//tmp/t/@x", 1, authenticated_user="u1")
        with pytest.raises(YtError): set("//tmp/t/@x", 1, authenticated_user="u2")

    def test_init_acl_in_set(self):
        create_user("u1")
        create_user("u2")
        value = yson.YsonInt64(10)
        value.attributes["acl"] = [make_ace("allow", "u1", "write")]
        value.attributes["inherit_acl"] = False
        set("//tmp/t", value)
        set("//tmp/t/@x", 1, authenticated_user="u1")
        with pytest.raises(YtError): set("//tmp/t/@x", 1, authenticated_user="u2")

    def _prepare_scheduler_test(self):
        create_user("u")
        create_account("a")

        create("table", "//tmp/t1")
        write_table("//tmp/t1", {"a" : "b"})

        create("table", "//tmp/t2")

        # just a sanity check
        map(in_="//tmp/t1", out="//tmp/t2", command="cat", authenticated_user="u")

    @unix_only
    def test_scheduler_in_acl(self):
        self._prepare_scheduler_test()
        set("//tmp/t1/@acl/end", make_ace("deny", "u", "read"))
        with pytest.raises(YtError): map(in_="//tmp/t1", out="//tmp/t2", command="cat", authenticated_user="u")

    @unix_only
    def test_scheduler_out_acl(self):
        self._prepare_scheduler_test()
        set("//tmp/t2/@acl/end", make_ace("deny", "u", "write"))
        with pytest.raises(YtError): map(in_="//tmp/t1", out="//tmp/t2", command="cat", authenticated_user="u")

    @unix_only
    def test_scheduler_account_quota(self):
        self._prepare_scheduler_test()
        set("//tmp/t2/@account", "a")
        set("//sys/accounts/a/@acl/end", make_ace("allow", "u", "use"))
        set_account_disk_space_limit("a", 0)
        with pytest.raises(YtError): map(in_="//tmp/t1", out="//tmp/t2", command="cat", authenticated_user="u")

    def test_scheduler_operation_abort_acl(self):
        self._prepare_scheduler_test()
        create_user("u1")
        op = map(
            dont_track=True,
            in_="//tmp/t1",
            out="//tmp/t2",
            command="cat; while true; do sleep 1; done",
            authenticated_user="u")
        with pytest.raises(YtError): op.abort(authenticated_user="u1")
        op.abort(authenticated_user="u")

    def test_scheduler_operation_abort_by_owners(self):
        self._prepare_scheduler_test()
        create_user("u1")
        op = map(
            dont_track=True,
            in_="//tmp/t1",
            out="//tmp/t2",
            command="cat; while true; do sleep 1; done",
            authenticated_user="u",
            spec={"owners": ["u1"]})
        op.abort(authenticated_user="u1")

    def test_inherit1(self):
        set("//tmp/p", {})
        set("//tmp/p/@inherit_acl", False)

        create_user("u")
        with pytest.raises(YtError): set("//tmp/p/a", "b", authenticated_user="u")
        with pytest.raises(YtError): ls("//tmp/p", authenticated_user="u")

        set("//tmp/p/@acl/end", make_ace("allow", "u", ["read", "write"]))
        set("//tmp/p/a", "b", authenticated_user="u")
        assert ls("//tmp/p", authenticated_user="u") == ["a"]
        assert get("//tmp/p/a", authenticated_user="u") == "b"

    def test_create_in_tx1(self):
        create_user("u")
        tx = start_transaction()
        create("table", "//tmp/a", tx=tx, authenticated_user="u")
        assert read_table("//tmp/a", tx=tx, authenticated_user="u") == []

    def test_create_in_tx2(self):
        create_user("u")
        tx = start_transaction()
        create("table", "//tmp/a/b/c", recursive=True, tx=tx, authenticated_user="u")
        assert read_table("//tmp/a/b/c", tx=tx, authenticated_user="u") == []

    @pytest.mark.xfail(run = False, reason = "In progress")
    def test_snapshot_remove(self):
        set("//tmp/a", {"b" : {"c" : "d"}})
        path = "#" + get("//tmp/a/b/c/@id")
        create_user("u")
        assert get(path, authenticated_user="u") == "d"
        tx = start_transaction()
        lock(path, mode="snapshot", tx=tx)
        assert get(path, authenticated_user="u", tx=tx) == "d"
        remove("//tmp/a")
        assert get(path, authenticated_user="u", tx=tx) == "d"

    @pytest.mark.xfail(run = False, reason = "In progress")
    def test_snapshot_no_inherit(self):
        set("//tmp/a", "b")
        assert get("//tmp/a/@inherit_acl")
        tx = start_transaction()
        lock("//tmp/a", mode="snapshot", tx=tx)
        assert not get("//tmp/a/@inherit_acl", tx=tx)

    def test_administer_permission1(self):
        create_user("u")
        create("table", "//tmp/t")
        with pytest.raises(YtError): set("//tmp/t/@acl", [], authenticated_user="u")

    def test_administer_permission2(self):
        create_user("u")
        create("table", "//tmp/t")
        set("//tmp/t/@acl/end", make_ace("allow", "u", "administer"))
        set("//tmp/t/@acl", [], authenticated_user="u")

    def test_administer_permission3(self):
        create("table", "//tmp/t")
        create_user("u")

        acl = [make_ace("allow", "u", "administer"), make_ace("deny", "u", "write")]
        set("//tmp/t/@acl", acl)

        set("//tmp/t/@inherit_acl", False, authenticated_user="u")
        set("//tmp/t/@acl", acl, authenticated_user="u")
        remove("//tmp/t/@acl/1", authenticated_user="u")

    def test_administer_permission4(self):
        create("table", "//tmp/t")
        create_user("u")

        acl = [make_ace("deny", "u", "administer")]
        set("//tmp/t/@acl", acl)

        with pytest.raises(YtError):
            set("//tmp/t/@acl", [], authenticated_user="u")
        with pytest.raises(YtError):
            set("//tmp/t/@inherit_acl", False, authenticated_user="u")

    def test_user_rename_success(self):
        create_user("u1")
        set("//sys/users/u1/@name", "u2")
        assert get("//sys/users/u2/@name") == "u2"

    def test_user_rename_fail(self):
        create_user("u1")
        create_user("u2")
        with pytest.raises(YtError):
            set("//sys/users/u1/@name", "u2")

    def test_deny_create(self):
        create_user("u")
        with pytest.raises(YtError):
            create("account_map", "//tmp/accounts", authenticated_user="u")

    def test_deny_copy_src(self):
        create_user("u")
        with pytest.raises(YtError):
            copy("//sys", "//tmp/sys", authenticated_user="u")

    def test_deny_copy_dst(self):
        create_user("u")
        create("table", "//tmp/t")
        with pytest.raises(YtError):
            copy("//tmp/t", "//sys/t", authenticated_user="u", preserve_account=True)

    def test_document1(self):
        create_user("u")
        create("document", "//tmp/d")
        set("//tmp/d", {"foo":{}})
        set("//tmp/d/@inherit_acl", False)

        assert get("//tmp", authenticated_user="u") == {"d": None}
        with pytest.raises(YtError): get("//tmp/d", authenticated_user="u") == {"foo": {}}
        with pytest.raises(YtError): get("//tmp/d/@value", authenticated_user="u")
        with pytest.raises(YtError): get("//tmp/d/foo", authenticated_user="u")
        with pytest.raises(YtError): set("//tmp/d/foo", {}, authenticated_user="u")
        with pytest.raises(YtError): set("//tmp/d/@value", {}, authenticated_user="u")
        with pytest.raises(YtError): set("//tmp/d", {"foo":{}}, authenticated_user="u")
        assert ls("//tmp", authenticated_user="u") == ["d"]
        with pytest.raises(YtError): ls("//tmp/d", authenticated_user="u")
        with pytest.raises(YtError): ls("//tmp/d/foo", authenticated_user="u")
        assert exists("//tmp/d", authenticated_user="u")
        with pytest.raises(YtError): exists("//tmp/d/@value", authenticated_user="u")
        with pytest.raises(YtError): exists("//tmp/d/foo", authenticated_user="u")
        with pytest.raises(YtError): remove("//tmp/d/foo", authenticated_user="u")
        with pytest.raises(YtError): remove("//tmp/d", authenticated_user="u")

    def test_document2(self):
        create_user("u")
        create("document", "//tmp/d")
        set("//tmp/d", {"foo":{}})
        set("//tmp/d/@inherit_acl", False)
        set("//tmp/d/@acl/end", make_ace("allow", "u", "read"))

        assert get("//tmp", authenticated_user="u") == {"d": None}
        assert get("//tmp/d", authenticated_user="u") == {"foo": {}}
        assert get("//tmp/d/@value", authenticated_user="u") == {"foo": {}}
        assert get("//tmp/d/foo", authenticated_user="u") == {}
        with pytest.raises(YtError): set("//tmp/d/foo", {}, authenticated_user="u")
        with pytest.raises(YtError): set("//tmp/d/@value", {}, authenticated_user="u")
        with pytest.raises(YtError): set("//tmp/d", {"foo":{}}, authenticated_user="u")
        assert ls("//tmp", authenticated_user="u") == ["d"]
        assert ls("//tmp/d", authenticated_user="u") == ["foo"]
        assert ls("//tmp/d/foo", authenticated_user="u") == []
        assert exists("//tmp/d", authenticated_user="u")
        assert exists("//tmp/d/@value", authenticated_user="u")
        assert exists("//tmp/d/foo", authenticated_user="u")
        with pytest.raises(YtError): remove("//tmp/d/foo", authenticated_user="u")
        with pytest.raises(YtError): remove("//tmp/d", authenticated_user="u")

    def test_document3(self):
        create_user("u")
        create("document", "//tmp/d")
        set("//tmp/d", {"foo":{}})
        set("//tmp/d/@inherit_acl", False)
        set("//tmp/d/@acl/end", make_ace("allow", "u", ["read", "write", "remove"]))

        assert get("//tmp", authenticated_user="u") == {"d": None}
        assert get("//tmp/d", authenticated_user="u") == {"foo": {}}
        assert get("//tmp/d/@value", authenticated_user="u") == {"foo": {}}
        assert get("//tmp/d/foo", authenticated_user="u") == {}
        set("//tmp/d/foo", {}, authenticated_user="u")
        set("//tmp/d/@value", {}, authenticated_user="u")
        set("//tmp/d", {"foo":{}}, authenticated_user="u")
        assert ls("//tmp", authenticated_user="u") == ["d"]
        assert ls("//tmp/d", authenticated_user="u") == ["foo"]
        assert ls("//tmp/d/foo", authenticated_user="u") == []
        assert exists("//tmp/d", authenticated_user="u")
        assert exists("//tmp/d/@value", authenticated_user="u")
        assert exists("//tmp/d/foo", authenticated_user="u")
        remove("//tmp/d/foo", authenticated_user="u")
        remove("//tmp/d", authenticated_user="u")

    def test_copy_account1(self):
        create_account("a")
        create_user("u")

        set("//tmp/x", {})
        set("//tmp/x/@account", "a")

        with pytest.raises(YtError):
            copy("//tmp/x", "//tmp/y", authenticated_user="u", preserve_account=True)

    def test_copy_account2(self):
        create_account("a")
        create_user("u")
        set("//sys/accounts/a/@acl/end", make_ace("allow", "u", "use"))

        set("//tmp/x", {})
        set("//tmp/x/@account", "a")

        copy("//tmp/x", "//tmp/y", authenticated_user="u", preserve_account=True)
        assert get("//tmp/y/@account") == "a"

    def test_copy_account3(self):
        create_account("a")
        create_user("u")

        set("//tmp/x", {"u": "v"})
        set("//tmp/x/u/@account", "a")

        with pytest.raises(YtError):
            copy("//tmp/x", "//tmp/y", authenticated_user="u", preserve_account=True)

    def test_copy_non_writable_src(self):
        # YT-4175
        create_user("u")
        set("//tmp/s", {"x": {"a": 1}})
        set("//tmp/s/@acl/end", make_ace("allow", "u", ["read", "write", "remove"]))
        set("//tmp/s/x/@acl", [make_ace("deny", "u", ["write", "remove"])])
        copy("//tmp/s/x", "//tmp/s/y", authenticated_user="u")
        assert get("//tmp/s/y", authenticated_user="u") == get("//tmp/s/x", authenticated_user="u")

    def test_copy_and_move_require_read_on_source(self):
        create_user("u")
        set("//tmp/s", {"x": {}})
        set("//tmp/s/@acl/end", make_ace("allow", "u", ["read", "write", "remove"]))
        set("//tmp/s/x/@acl", [make_ace("deny", "u", "read")])
        with pytest.raises(YtError): copy("//tmp/s/x", "//tmp/s/y", authenticated_user="u")
        with pytest.raises(YtError): move("//tmp/s/x", "//tmp/s/y", authenticated_user="u")

    def test_copy_and_move_require_write_on_target_parent(self):
        create_user("u")
        set("//tmp/s", {"x": {}})
        set("//tmp/s/@acl/end", make_ace("allow", "u", ["read", "remove"]))
        set("//tmp/s/@acl/end", make_ace("deny", "u", ["write"]))
        with pytest.raises(YtError): copy("//tmp/s/x", "//tmp/s/y", authenticated_user="u")
        with pytest.raises(YtError): move("//tmp/s/x", "//tmp/s/y", authenticated_user="u")

    def test_copy_and_move_requires_remove_on_target_if_exists(self):
        create_user("u")
        set("//tmp/s", {"x": {}, "y": {}})
        set("//tmp/s/@acl/end", make_ace("allow", "u", ["read", "write", "remove"]))
        set("//tmp/s/y/@acl", [make_ace("deny", "u", "remove")])
        with pytest.raises(YtError): copy("//tmp/s/x", "//tmp/s/y", force=True, authenticated_user="u")
        with pytest.raises(YtError): move("//tmp/s/x", "//tmp/s/y", force=True, authenticated_user="u")

    def test_move_requires_remove_on_self_and_write_on_self_parent(self):
        create_user("u")
        set("//tmp/s", {"x": {}})
        set("//tmp/s/@acl", [make_ace("allow", "u", ["read", "write", "remove"])])
        set("//tmp/s/x/@acl", [make_ace("deny", "u", "remove")])
        with pytest.raises(YtError): move("//tmp/s/x", "//tmp/s/y", authenticated_user="u")
        set("//tmp/s/x/@acl", [])
        set("//tmp/s/@acl", [make_ace("allow", "u", ["read", "remove"]), make_ace("deny", "u", "write")])
        with pytest.raises(YtError): move("//tmp/s/x", "//tmp/s/y", authenticated_user="u")
        set("//tmp/s/@acl", [make_ace("allow", "u", ["read", "write", "remove"])])
        move("//tmp/s/x", "//tmp/s/y", authenticated_user="u")

    def test_superusers(self):
        create("table", "//sys/protected")
        create_user("u")
        with pytest.raises(YtError):
            remove("//sys/protected", authenticated_user="u")
        add_member("u", "superusers")
        remove("//sys/protected", authenticated_user="u")

    def test_remove_self_requires_permission(self):
        create_user("u")
        set("//tmp/x", {})
        set("//tmp/x/y", {})

        set("//tmp/x/@inherit_acl", False)
        with pytest.raises(YtError): remove("//tmp/x", authenticated_user="u")
        with pytest.raises(YtError): remove("//tmp/x/y", authenticated_user="u")

        set("//tmp/x/@acl", [make_ace("allow", "u", "write")])
        set("//tmp/x/y/@acl", [make_ace("deny", "u", "remove")])
        with pytest.raises(YtError): remove("//tmp/x", authenticated_user="u")
        with pytest.raises(YtError): remove("//tmp/x/y", authenticated_user="u")

        set("//tmp/x/y/@acl", [make_ace("allow", "u", "remove")])
        with pytest.raises(YtError): remove("//tmp/x", authenticated_user="u")
        remove("//tmp/x/y", authenticated_user="u")
        with pytest.raises(YtError): remove("//tmp/x", authenticated_user="u")

        set("//tmp/x/@acl", [make_ace("allow", "u", "remove")])
        remove("//tmp/x", authenticated_user="u")

    def test_remove_recursive_requires_permission(self):
        create_user("u")
        set("//tmp/x", {})
        set("//tmp/x/y", {})

        set("//tmp/x/@inherit_acl", False)
        with pytest.raises(YtError): remove("//tmp/x/*", authenticated_user="u")
        set("//tmp/x/@acl", [make_ace("allow", "u", "write")])
        set("//tmp/x/y/@acl", [make_ace("deny", "u", "remove")])
        with pytest.raises(YtError): remove("//tmp/x/*", authenticated_user="u")
        set("//tmp/x/y/@acl", [make_ace("allow", "u", "remove")])
        remove("//tmp/x/*", authenticated_user="u")

    def test_set_self_requires_remove_permission(self):
        create_user("u")
        set("//tmp/x", {})
        set("//tmp/x/y", {})

        set("//tmp/x/@inherit_acl", False)
        with pytest.raises(YtError): set("//tmp/x", {}, authenticated_user="u")
        with pytest.raises(YtError): set("//tmp/x/y", {}, authenticated_user="u")

        set("//tmp/x/@acl", [make_ace("allow", "u", "write")])
        set("//tmp/x/y/@acl", [make_ace("deny", "u", "remove")])
        set("//tmp/x/y", {}, authenticated_user="u")
        with pytest.raises(YtError): set("//tmp/x", {}, authenticated_user="u")

        set("//tmp/x/@acl", [make_ace("allow", "u", "write")])
        set("//tmp/x/y/@acl", [make_ace("allow", "u", "remove")])
        set("//tmp/x/y", {}, authenticated_user="u")
        set("//tmp/x", {}, authenticated_user="u")

    def test_guest_can_remove_users_groups_accounts(self):
        create_user("u")
        create_group("g")
        create_account("a")

        with pytest.raises(YtError): remove("//sys/users/u", authenticated_user="guest")
        with pytest.raises(YtError): remove("//sys/groups/g", authenticated_user="guest")
        with pytest.raises(YtError): remove("//sys/accounts/a", authenticated_user="guest")

        set("//sys/schemas/user/@acl/end", make_ace("allow", "guest", "remove"))
        set("//sys/schemas/group/@acl/end", make_ace("allow", "guest", "remove"))
        set("//sys/schemas/account/@acl/end", make_ace("allow", "guest", "remove"))

        remove("//sys/users/u", authenticated_user="guest")
        remove("//sys/groups/g", authenticated_user="guest")
        remove("//sys/accounts/a", authenticated_user="guest")

        remove("//sys/schemas/user/@acl/-1")
        remove("//sys/schemas/group/@acl/-1")
        remove("//sys/schemas/account/@acl/-1")

    def test_set_acl_upon_construction(self):
        create_user("u")
        create("table", "//tmp/t", attributes={
            "acl": [make_ace("allow", "u", "write")],
            "inherit_acl": False})
        assert len(get("//tmp/t/@acl")) == 1


    def test_group_write_acl(self):
        create_user("u")
        create_group("g")
        with pytest.raises(YtError): add_member("u", "g", authenticated_user="guest")
        set("//sys/groups/g/@acl/end", make_ace("allow", "guest", "write"))
        add_member("u", "g", authenticated_user="guest")

    def test_user_remove_acl(self):
        create_user("u")
        with pytest.raises(YtError): remove("//sys/users/u", authenticated_user="guest")
        set("//sys/users/u/@acl/end", make_ace("allow", "guest", "remove"))
        remove("//sys/users/u", authenticated_user="guest")

    def test_group_remove_acl(self):
        create_group("g")
        with pytest.raises(YtError): remove("//sys/groups/g", authenticated_user="guest")
        set("//sys/groups/g/@acl/end", make_ace("allow", "guest", "remove"))
        remove("//sys/groups/g", authenticated_user="guest")


    def test_default_inheritance(self):
        create("map_node", "//tmp/m", attributes={"acl": [make_ace("allow", "guest", "remove")]})
        assert get("//tmp/m/@acl/0/inheritance_mode") == "object_and_descendants"

    def test_descendants_only_inheritance(self):
        create("map_node", "//tmp/m", attributes={"acl": [make_ace("allow", "guest", "remove", "descendants_only")]})
        create("map_node", "//tmp/m/s")
        create("map_node", "//tmp/m/s/r")
        assert check_permission("guest", "remove", "//tmp/m/s/r")["action"] == "allow"
        assert check_permission("guest", "remove", "//tmp/m/s")["action"] == "allow"
        assert check_permission("guest", "remove", "//tmp/m")["action"] == "deny"

    def test_object_only_inheritance(self):
        create("map_node", "//tmp/m", attributes={"acl": [make_ace("allow", "guest", "remove", "object_only")]})
        create("map_node", "//tmp/m/s")
        assert check_permission("guest", "remove", "//tmp/m/s")["action"] == "deny"
        assert check_permission("guest", "remove", "//tmp/m")["action"] == "allow"

    def test_immediate_descendants_only_inheritance(self):
        create("map_node", "//tmp/m", attributes={"acl": [make_ace("allow", "guest", "remove", "immediate_descendants_only")]})
        create("map_node", "//tmp/m/s")
        create("map_node", "//tmp/m/s/r")
        assert check_permission("guest", "remove", "//tmp/m/s/r")["action"] == "deny"
        assert check_permission("guest", "remove", "//tmp/m/s")["action"] == "allow"
        assert check_permission("guest", "remove", "//tmp/m")["action"] == "deny"

    def test_banned_user_permission(self):
        create_user("u")
        set("//sys/users/u/@banned", True)
        assert check_permission("u", "read", "//tmp")["action"] == "deny"

    def test_read_from_cache(self):
        create_user("u")
        set("//tmp/a", "b")
        set("//tmp/a/@acl/end", make_ace("deny", "u", "read"))
        with pytest.raises(YtError): get("//tmp/a", authenticated_user="u")
        with pytest.raises(YtError): get("//tmp/a", authenticated_user="u", read_from="cache")

    def test_check_permission_for_virtual_maps(self):
        assert check_permission("guest", "read", "//sys/chunks")["action"] == "allow"

    def test_owner_user(self):
        create("map_node", "//tmp/x")
        create_user("u1")
        create_user("u2")
        create("table", "//tmp/x/1", authenticated_user="u1")
        create("table", "//tmp/x/2", authenticated_user="u2")
        assert get("//tmp/x/1/@owner") == "u1"
        assert get("//tmp/x/2/@owner") == "u2"
        set("//tmp/x/@inherit_acl", False)
        set("//tmp/x/@acl", [make_ace("allow", "owner", "remove")])
        assert check_permission("u2", "remove", "//tmp/x/1")["action"] == "deny"
        assert check_permission("u1", "remove", "//tmp/x/2")["action"] == "deny"
        assert check_permission("u1", "remove", "//tmp/x/1")["action"] == "allow"
        assert check_permission("u2", "remove", "//tmp/x/2")["action"] == "allow"

    def test_owner_group(self):
        create("map_node", "//tmp/x")
        create_user("u1")
        create_user("u2")
        create_group("g")
        add_member("u1", "g")
        create("table", "//tmp/x/1")
        set("//tmp/x/1/@owner", "g")
        assert get("//tmp/x/1/@owner") == "g"
        set("//tmp/x/@inherit_acl", False)
        set("//tmp/x/@acl", [make_ace("allow", "owner", "remove")])
        assert check_permission("u1", "remove", "//tmp/x/1")["action"] == "allow"
        assert check_permission("u2", "remove", "//tmp/x/1")["action"] == "deny"

    def test_no_owner_auth(self):
        with pytest.raises(YtError): get("//tmp", authenticated_user="owner")

    def test_list_with_attr_yt_7165(self):
        create_user("u")
        create("map_node", "//tmp/x")
        set("//tmp/x/@acl", [make_ace("allow", "u", "read", "object_only")])
        set("//tmp/x/@inherit_acl", False)
        create("map_node", "//tmp/x/y")
        set("//tmp/x/y/@attr", "value")
        assert "attr" not in ls("//tmp/x", attributes=["attr"], authenticated_user="u")[0].attributes

    def test_safe_mode(self):
        create_user("u")
        with pytest.raises(YtError):
            set("//sys/@config/enable_safe_mode", True, authenticated_user="u")
        set("//sys/@config/enable_safe_mode", True)
        create("table", "//tmp/t1")
        with pytest.raises(YtError):
            create("table", "//tmp/t2", authenticated_user="u")
        with pytest.raises(YtError):
            start_transaction(authenticated_user="u")

    def test_check_permission_by_acl(self):
        create_user("u1")
        create_user("u2")
        assert check_permission_by_acl("u1", "remove", [{"subjects": ["u1"], "permissions": ["remove"], "action": "allow"}])["action"] == "allow"
        assert check_permission_by_acl("u1", "remove", [{"subjects": ["u2"], "permissions": ["remove"], "action": "allow"}])["action"] == "deny"
        assert check_permission_by_acl(None, "remove", [{"subjects": ["u2"], "permissions": ["remove"], "action": "allow"}])["action"] == "allow"

    def test_effective_acl(self):
        create_user("u")
        for ch in "abcd":
            for idx in range(1,5):
                create_user(ch + str(idx))

        create("map_node", "//tmp/a")
        create("map_node", "//tmp/a/b")
        create("map_node", "//tmp/a/b/c")
        create("map_node", "//tmp/a/b/c/d")

        set("//tmp/a/@inherit_acl", False)

        set("//tmp/a/@acl", [
            make_ace("allow", "a1", "read", "immediate_descendants_only"),
            make_ace("allow", "a2", "read", "object_only"),
            make_ace("allow", "a3", "read", "object_and_descendants"),
            make_ace("allow", "a4", "read", "descendants_only")])

        set("//tmp/a/b/@acl", [
            make_ace("allow", "b1", "read", "immediate_descendants_only"),
            make_ace("allow", "b2", "read", "object_only"),
            make_ace("allow", "b3", "read", "object_and_descendants"),
            make_ace("allow", "b4", "read", "descendants_only")])

        set("//tmp/a/b/c/@acl", [
            make_ace("allow", "c1", "read", "immediate_descendants_only"),
            make_ace("allow", "c2", "read", "object_only"),
            make_ace("allow", "c3", "read", "object_and_descendants"),
            make_ace("allow", "c4", "read", "descendants_only")])

        set("//tmp/a/b/c/d/@acl", [
            make_ace("allow", "d1", "read", "immediate_descendants_only"),
            make_ace("allow", "d2", "read", "object_only"),
            make_ace("allow", "d3", "read", "object_and_descendants"),
            make_ace("allow", "d4", "read", "descendants_only")])

        assert_items_equal(get("//tmp/a/@effective_acl"), [
            make_ace("allow", "a2", "read", "object_only"),
            make_ace("allow", "a3", "read", "object_and_descendants")])

        assert_items_equal(get("//tmp/a/b/@effective_acl"), [
            make_ace("allow", "a1", "read", "object_only"),
            make_ace("allow", "a3", "read", "object_and_descendants"),
            make_ace("allow", "a4", "read", "object_and_descendants"),
            make_ace("allow", "b2", "read", "object_only"),
            make_ace("allow", "b3", "read", "object_and_descendants")])

        assert_items_equal(get("//tmp/a/b/c/@effective_acl"), [
            make_ace("allow", "a3", "read", "object_and_descendants"),
            make_ace("allow", "a4", "read", "object_and_descendants"),
            make_ace("allow", "b1", "read", "object_only"),
            make_ace("allow", "b3", "read", "object_and_descendants"),
            make_ace("allow", "b4", "read", "object_and_descendants"),
            make_ace("allow", "c2", "read", "object_only"),
            make_ace("allow", "c3", "read", "object_and_descendants")])

        assert_items_equal(get("//tmp/a/b/c/d/@effective_acl"), [
            make_ace("allow", "a3", "read", "object_and_descendants"),
            make_ace("allow", "a4", "read", "object_and_descendants"),
            make_ace("allow", "b3", "read", "object_and_descendants"),
            make_ace("allow", "b4", "read", "object_and_descendants"),
            make_ace("allow", "c1", "read", "object_only"),
            make_ace("allow", "c3", "read", "object_and_descendants"),
            make_ace("allow", "c4", "read", "object_and_descendants"),
            make_ace("allow", "d2", "read", "object_only"),
            make_ace("allow", "d3", "read", "object_and_descendants")])

##################################################################

class TestAclsMulticell(TestAcls):
    NUM_SECONDARY_MASTER_CELLS = 2
