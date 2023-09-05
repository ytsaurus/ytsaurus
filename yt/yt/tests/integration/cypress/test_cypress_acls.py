from yt_env_setup import YTEnvSetup, wait

from yt_commands import (
    authors, create, ls, get, set, copy, move, remove, create_domestic_medium,
    exists, multiset_attributes, create_account,
    create_user, create_group, make_ace, check_permission, check_permission_by_acl, add_member, remove_group, remove_user, start_transaction, lock,
    read_table, write_table, alter_table,
    map, set_account_disk_space_limit, raises_yt_error,
    gc_collect, build_snapshot,
    create_access_control_object_namespace, create_access_control_object,)

from yt_type_helpers import make_schema

from yt_helpers import skip_if_renaming_disabled

from yt.environment.helpers import assert_items_equal
from yt.common import YtError
import yt.yson as yson

import pytest

##################################################################


class CheckPermissionBase(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3

    @authors("s-v-m")
    def test_subject_aliases_acls(self):
        create_user("u1")
        create_user("u2")
        create_group("g1")
        create_group("g2")
        add_member("u1", "g1")
        add_member("u2", "g2")
        set("//sys/groups/g1/@aliases", ["ga1", "ga2"])
        with raises_yt_error('Alias "ga1" already exists'):
            set("//sys/groups/g2/@aliases", ["ga1", "ga3"])
        with raises_yt_error('Alias "ga4" listed more than once'):
            set("//sys/groups/g1/@aliases", ["ga4", "ga4"])
        set("//sys/users/u1/@aliases", ["ua1", "ua2"])

        create("table", "//tmp/t")
        set("//tmp/t/@inherit_acl", False)
        assert check_permission("u1", "write", "//tmp/t")["action"] == "deny"

        set("//tmp/t/@acl", [make_ace("allow", "ga1", "write")])
        assert check_permission("u1", "write", "//tmp/t")["action"] == "allow"
        assert check_permission("u2", "write", "//tmp/t")["action"] == "deny"

        set("//tmp/t/@acl", [make_ace("allow", "ua1", "write")])
        assert check_permission("u1", "write", "//tmp/t")["action"] == "allow"
        assert check_permission("u2", "write", "//tmp/t")["action"] == "deny"

    @authors("s-v-m")
    def test_subject_aliases_misc(self):
        create_user("user")
        set("//sys/users/user/@aliases", ["alias_to_user"])
        assert "alias_to_user" not in ls("//sys/users")
        assert get("//sys/users/alias_to_user/@name") == "user"

        create_group("group")
        set("//sys/groups/group/@aliases", ["alias_to_group"])
        assert "alias_to_group" not in ls("//sys/groups")
        assert get("//sys/groups/alias_to_group/@name") == "group"

        add_member("alias_to_user", "alias_to_group")
        assert "user" in get("//sys/groups/group/@members")

        create("table", "//tmp/t")
        set("//tmp/t/@owner", "alias_to_user")
        assert get("//tmp/t/@owner") == "user"

    @authors("kiselyovp")
    def test_descendants_only_inheritance(self):
        create(
            "map_node",
            "//tmp/m",
            attributes={"acl": [make_ace("allow", "guest", "remove", "descendants_only")]},
        )
        create("map_node", "//tmp/m/s")
        create("map_node", "//tmp/m/s/r")
        assert check_permission("guest", "remove", "//tmp/m/s/r")["action"] == "allow"
        assert check_permission("guest", "remove", "//tmp/m/s")["action"] == "allow"
        assert check_permission("guest", "remove", "//tmp/m")["action"] == "deny"

    @authors("kiselyovp")
    def test_object_only_inheritance(self):
        create(
            "map_node",
            "//tmp/m",
            attributes={"acl": [make_ace("allow", "guest", "remove", "object_only")]},
        )
        create("map_node", "//tmp/m/s")
        assert check_permission("guest", "remove", "//tmp/m/s")["action"] == "deny"
        assert check_permission("guest", "remove", "//tmp/m")["action"] == "allow"

    @authors("kiselyovp")
    def test_immediate_descendants_only_inheritance(self):
        create(
            "map_node",
            "//tmp/m",
            attributes={"acl": [make_ace("allow", "guest", "remove", "immediate_descendants_only")]},
        )
        create("map_node", "//tmp/m/s")
        create("map_node", "//tmp/m/s/r")
        assert check_permission("guest", "remove", "//tmp/m/s/r")["action"] == "deny"
        assert check_permission("guest", "remove", "//tmp/m/s")["action"] == "allow"
        assert check_permission("guest", "remove", "//tmp/m")["action"] == "deny"

    @authors("kiselyovp", "gritukan")
    @pytest.mark.parametrize("superuser", [False, True])
    def test_banned_user_permission(self, superuser):
        create_user("u")
        if superuser:
            add_member("u", "superusers")
        set("//sys/users/u/@banned", True)
        assert check_permission("u", "read", "//tmp")["action"] == "deny"

    @authors("kiselyovp")
    def test_check_permission_for_virtual_maps(self):
        assert check_permission("guest", "read", "//sys/chunks")["action"] == "allow"

    @authors("kiselyovp")
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

    @authors("kiselyovp")
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

    @authors("kiselyovp")
    def test_check_permission_by_acl(self):
        create_user("u1")
        create_user("u2")
        assert (
            check_permission_by_acl(
                "u1",
                "remove",
                [{"subjects": ["u1"], "permissions": ["remove"], "action": "allow"}],
            )["action"]
            == "allow"
        )
        assert (
            check_permission_by_acl(
                "u1",
                "remove",
                [{"subjects": ["u2"], "permissions": ["remove"], "action": "allow"}],
            )["action"]
            == "deny"
        )
        assert (
            check_permission_by_acl(
                None,
                "remove",
                [{"subjects": ["u2"], "permissions": ["remove"], "action": "allow"}],
            )["action"]
            == "allow"
        )

    @authors("kiselyovp", "levysotsky")
    @pytest.mark.parametrize("acl_path", ["//tmp/dir/t", "//tmp/dir"])
    @pytest.mark.parametrize("rename_columns", [False, True])
    def test_check_permission_for_columnar_acl(self, acl_path, rename_columns):
        skip_if_renaming_disabled(self.Env)

        create_user("u1")
        create_user("u2")
        create_user("u3")
        create_user("u4")

        create(
            "table",
            "//tmp/dir/t",
            attributes={
                "schema": [
                    {"name": "a", "type": "string"},
                    {"name": "b", "type": "string"},
                    {"name": "c", "type": "string"},
                ]
            },
            recursive=True,
        )

        if rename_columns:
            new_schema = [
                {"name": "a", "type": "string"},
                {"name": "b_new", "type": "string", "stable_name": "b"},
                {"name": "c_new", "type": "string", "stable_name": "c"},
            ]
            alter_table("//tmp/dir/t", schema=new_schema)
            names = ["a", "b_new", "c_new"]
        else:
            names = ["a", "b", "c"]

        set(
            acl_path + "/@acl",
            [
                make_ace("deny", "u1", "read", columns=names[0]),
                make_ace("allow", "u2", "read", columns=names[1]),
                make_ace("deny", "u4", "read"),
            ],
        )

        response1 = check_permission("u1", "read", "//tmp/dir/t", columns=names)
        assert response1["action"] == "allow"
        assert response1["columns"][0]["action"] == "deny"
        assert response1["columns"][1]["action"] == "deny"
        assert response1["columns"][2]["action"] == "allow"

        response2 = check_permission("u2", "read", "//tmp/dir/t", columns=names)
        assert response2["action"] == "allow"
        assert response2["columns"][0]["action"] == "deny"
        assert response2["columns"][1]["action"] == "allow"
        assert response2["columns"][2]["action"] == "allow"

        response3 = check_permission("u3", "read", "//tmp/dir/t", columns=names)
        assert response3["action"] == "allow"
        assert response3["columns"][0]["action"] == "deny"
        assert response3["columns"][1]["action"] == "deny"
        assert response3["columns"][2]["action"] == "allow"

        response4 = check_permission("u4", "read", "//tmp/dir/t", columns=names)
        assert response4["action"] == "deny"

    @authors("babenko", "levysotsky")
    @pytest.mark.parametrize("rename_columns", [False, True])
    def test_inaccessible_columns_yt_11619(self, rename_columns):
        create_user("u")

        create(
            "table",
            "//tmp/t",
            attributes={
                "schema": [
                    {"name": "a", "type": "string"},
                    {"name": "b", "type": "string"},
                    {"name": "c", "type": "string"},
                ],
                "acl": [
                    make_ace("deny", "u", "read", columns="a"),
                    make_ace("deny", "u", "read", columns="b"),
                ],
            },
            recursive=True,
        )

        if rename_columns:
            write_table("//tmp/t", [{"a": "a", "b": "b", "c": "c"}])

            new_schema = [
                {"name": "a", "type": "string"},
                {"name": "b_new", "type": "string", "stable_name": "b"},
                {"name": "c_new", "type": "string", "stable_name": "c"},
            ]
            alter_table("//tmp/t", schema=new_schema)

            write_table("<append=%true>//tmp/t", [{"a": "a", "b_new": "b_new", "c_new": "c_new"}])
            names = ["a", "b_new", "c_new"]
        else:
            write_table("//tmp/t", [
                {"a": "a", "b": "b", "c": "c"},
                {"a": "a", "b": "b_new", "c": "c_new"},
            ])
            names = ["a", "b", "c"]

        response_parameters = {}
        rows = read_table(
            "//tmp/t{{ {} }}".format(",".join(names)),
            omit_inaccessible_columns=True,
            response_parameters=response_parameters,
            authenticated_user="u",
        )
        if rename_columns:
            assert rows == [{"b_new": "b", "c_new": "c"}, {"b_new": "b_new", "c_new": "c_new"}]
            assert_items_equal(response_parameters["omitted_inaccessible_columns"], [b"a"])
        else:
            assert rows == [{"c": "c"}, {"c": "c_new"}]
            assert_items_equal(response_parameters["omitted_inaccessible_columns"], [b"a", b"b"])


class TestCypressAcls(CheckPermissionBase):
    NUM_TEST_PARTITIONS = 2
    NUM_SCHEDULERS = 1
    USE_MASTER_CACHE = True

    @authors("babenko", "ignat")
    def test_empty_names_fail(self):
        with pytest.raises(YtError):
            create_user("")
        with pytest.raises(YtError):
            create_group("")

    @authors("babenko")
    def test_default_acl_sanity(self):
        create_user("u")
        with pytest.raises(YtError):
            set("/", {}, authenticated_user="u")
        with pytest.raises(YtError):
            set("//sys", {}, authenticated_user="u")
        with pytest.raises(YtError):
            set("//sys/a", "b", authenticated_user="u")
        with pytest.raises(YtError):
            set("/a", "b", authenticated_user="u")
        with pytest.raises(YtError):
            remove("//sys", authenticated_user="u")
        with pytest.raises(YtError):
            remove("//sys/tmp", authenticated_user="u")
        with pytest.raises(YtError):
            remove("//sys/home", authenticated_user="u")
        with pytest.raises(YtError):
            set("//sys/home/a", "b", authenticated_user="u")
        set("//tmp/a", "b", authenticated_user="u")
        ls("//tmp", authenticated_user="guest")
        with pytest.raises(YtError):
            set("//tmp/c", "d", authenticated_user="guest")

    def _test_denying_acl(self, rw_path, rw_user, acl_path, acl_subject):
        set(rw_path, "b", authenticated_user=rw_user)
        assert get(rw_path, authenticated_user=rw_user) == "b"

        set(rw_path, "c", authenticated_user=rw_user)
        assert get(rw_path, authenticated_user=rw_user) == "c"

        set(acl_path + "/@acl/end", make_ace("deny", acl_subject, ["write", "remove"]))
        with pytest.raises(YtError):
            set(rw_path, "d", authenticated_user=rw_user)
        assert get(rw_path, authenticated_user=rw_user) == "c"

        remove(acl_path + "/@acl/-1")
        set(
            acl_path + "/@acl/end",
            make_ace("deny", acl_subject, ["read", "write", "remove"]),
        )
        with pytest.raises(YtError):
            get(rw_path, authenticated_user=rw_user)
        with pytest.raises(YtError):
            set(rw_path, "d", authenticated_user=rw_user)

    @authors("babenko", "ignat")
    def test_denying_acl1(self):
        create_user("u")
        self._test_denying_acl("//tmp/a", "u", "//tmp/a", "u")

    @authors("babenko", "ignat")
    def test_denying_acl2(self):
        create_user("u")
        create_group("g")
        add_member("u", "g")
        self._test_denying_acl("//tmp/a", "u", "//tmp/a", "g")

    @authors("babenko")
    def test_denying_acl3(self):
        create_user("u")
        set("//tmp/p", {})
        self._test_denying_acl("//tmp/p/a", "u", "//tmp/p", "u")

    def _test_allowing_acl(self, rw_path, rw_user, acl_path, acl_subject):
        set(rw_path, "a")

        with pytest.raises(YtError):
            set(rw_path, "b", authenticated_user=rw_user)

        set(acl_path + "/@acl/end", make_ace("allow", acl_subject, ["write", "remove"]))
        set(rw_path, "c", authenticated_user=rw_user)

        remove(acl_path + "/@acl/-1")
        set(acl_path + "/@acl/end", make_ace("allow", acl_subject, ["read"]))
        with pytest.raises(YtError):
            set(rw_path, "d", authenticated_user=rw_user)

    @authors("babenko", "ignat")
    def test_allowing_acl1(self):
        self._test_allowing_acl("//tmp/a", "guest", "//tmp/a", "guest")

    @authors("babenko", "ignat")
    def test_allowing_acl2(self):
        create_group("g")
        add_member("guest", "g")
        self._test_allowing_acl("//tmp/a", "guest", "//tmp/a", "g")

    @authors("babenko", "ignat")
    def test_allowing_acl3(self):
        set("//tmp/p", {})
        self._test_allowing_acl("//tmp/p/a", "guest", "//tmp/p", "guest")

    @authors("babenko")
    def test_schema_acl1(self):
        create_user("u")
        create("table", "//tmp/t1", authenticated_user="u")
        set("//sys/schemas/table/@acl/end", make_ace("deny", "u", "create"))
        with pytest.raises(YtError):
            create("table", "//tmp/t2", authenticated_user="u")

    @authors("babenko")
    def test_schema_acl2(self):
        create_user("u")
        start_transaction(authenticated_user="u")
        set("//sys/schemas/transaction/@acl/end", make_ace("deny", "u", "create"))
        with pytest.raises(YtError):
            start_transaction(authenticated_user="u")

    @authors("babenko", "ignat")
    def test_user_destruction(self):
        old_acl = get("//tmp/@acl")
        create("map_node", "//tmp/dir")
        set("//tmp/dir/@acl", old_acl)
        set("//tmp/dir/@inherit_acl", False)

        create_user("u")
        set("//tmp/dir/@acl/end", make_ace("deny", "u", "write"))

        remove_user("u")
        assert get("//tmp/dir/@acl") == old_acl

    @authors("babenko", "ignat")
    def test_group_destruction(self):
        old_acl = get("//tmp/@acl")
        create("map_node", "//tmp/dir")
        set("//tmp/dir/@acl", old_acl)
        set("//tmp/dir/@inherit_acl", False)

        create_group("g")
        set("//tmp/dir/@acl/end", make_ace("deny", "g", "write"))

        remove_group("g")
        assert get("//tmp/dir/@acl") == old_acl

    @authors("babenko", "ignat")
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

    @authors("babenko")
    def test_init_acl_in_create(self):
        create_user("u1")
        create_user("u2")
        create(
            "table",
            "//tmp/t",
            attributes={
                "inherit_acl": False,
                "acl": [make_ace("allow", "u1", "write")],
            },
        )
        set("//tmp/t/@x", 1, authenticated_user="u1")
        with pytest.raises(YtError):
            set("//tmp/t/@x", 1, authenticated_user="u2")

    @authors("babenko")
    def test_init_acl_in_set(self):
        create_user("u1")
        create_user("u2")
        value = yson.YsonInt64(10)
        value.attributes["acl"] = [make_ace("allow", "u1", "write")]
        value.attributes["inherit_acl"] = False
        set("//tmp/t", value)
        set("//tmp/t/@x", 1, authenticated_user="u1")
        with pytest.raises(YtError):
            set("//tmp/t/@x", 1, authenticated_user="u2")

    def _prepare_scheduler_test(self):
        create_user("u")
        create_account("a")

        create("table", "//tmp/t1")
        write_table("//tmp/t1", {str(i): i for i in range(20)})

        create("table", "//tmp/t2")

        # just a sanity check
        map(in_="//tmp/t1", out="//tmp/t2", command="cat", authenticated_user="u")

    @authors("babenko")
    def test_scheduler_in_acl(self):
        self._prepare_scheduler_test()
        set("//tmp/t1/@acl/end", make_ace("deny", "u", "read"))
        with pytest.raises(YtError):
            map(in_="//tmp/t1", out="//tmp/t2", command="cat", authenticated_user="u")

    @authors("babenko")
    def test_scheduler_out_acl(self):
        self._prepare_scheduler_test()
        set("//tmp/t2/@acl/end", make_ace("deny", "u", "write"))
        with pytest.raises(YtError):
            map(in_="//tmp/t1", out="//tmp/t2", command="cat", authenticated_user="u")

    @authors("babenko")
    def test_scheduler_account_quota(self):
        self._prepare_scheduler_test()
        set("//tmp/t2/@account", "a")
        set("//sys/accounts/a/@acl/end", make_ace("allow", "u", "use"))
        set_account_disk_space_limit("a", 0)
        with pytest.raises(YtError):
            map(in_="//tmp/t1", out="//tmp/t2", command="cat", authenticated_user="u")

    @authors("acid")
    def test_scheduler_operation_abort_by_owners(self):
        self._prepare_scheduler_test()
        create_user("u1")
        op = map(
            track=False,
            in_="//tmp/t1",
            out="//tmp/t2",
            command="cat; while true; do sleep 1; done",
            authenticated_user="u",
            spec={"owners": ["u1"]},
        )
        op.abort(authenticated_user="u1")

    @authors("babenko")
    def test_inherit1(self):
        set("//tmp/p", {})
        set("//tmp/p/@inherit_acl", False)

        create_user("u")
        with pytest.raises(YtError):
            set("//tmp/p/a", "b", authenticated_user="u")
        with pytest.raises(YtError):
            ls("//tmp/p", authenticated_user="u")

        set("//tmp/p/@acl/end", make_ace("allow", "u", ["read", "write"]))
        set("//tmp/p/a", "b", authenticated_user="u")
        assert ls("//tmp/p", authenticated_user="u") == ["a"]
        assert get("//tmp/p/a", authenticated_user="u") == "b"

    @authors("shakurov")
    def test_create_with_replace(self):
        create_user("u")

        # Deny writing.
        create("map_node", "//tmp/a", attributes={"acl": [make_ace("deny", "u", "write")]})
        create(
            "map_node",
            "//tmp/a/b",
            attributes={"acl": [make_ace("allow", "u", "write")]},
        )

        with pytest.raises(YtError):
            create("table", "//tmp/a/b", force=True, authenticated_user="u")
        set("//tmp/a/@acl", [make_ace("allow", "u", "write")])
        create("table", "//tmp/a/b", force=True, authenticated_user="u")

        # Allow write but deny remove.
        create("map_node", "//tmp/c", attributes={"acl": [make_ace("allow", "u", "write")]})
        create(
            "map_node",
            "//tmp/c/d",
            attributes={
                "acl": [
                    make_ace("allow", "u", "write"),
                    make_ace("deny", "u", "remove"),
                ]
            },
        )

        with pytest.raises(YtError):
            create("table", "//tmp/c", force=True, authenticated_user="u")
        set(
            "//tmp/c/d/@acl",
            [make_ace("allow", "u", "write"), make_ace("allow", "u", "remove")],
        )
        create("table", "//tmp/c", force=True, authenticated_user="u")

    @authors("babenko")
    def test_create_in_tx1(self):
        create_user("u")
        tx = start_transaction()
        create("table", "//tmp/a", tx=tx, authenticated_user="u")
        assert read_table("//tmp/a", tx=tx, authenticated_user="u") == []

    @authors("babenko")
    def test_create_in_tx2(self):
        create_user("u")
        tx = start_transaction()
        create("table", "//tmp/a/b/c", recursive=True, tx=tx, authenticated_user="u")
        assert read_table("//tmp/a/b/c", tx=tx, authenticated_user="u") == []

    @authors("babenko", "ignat")
    @pytest.mark.xfail(run=False, reason="In progress")
    def test_snapshot_remove(self):
        set("//tmp/a", {"b": {"c": "d"}})
        path = "#" + get("//tmp/a/b/c/@id")
        create_user("u")
        assert get(path, authenticated_user="u") == "d"
        tx = start_transaction()
        lock(path, mode="snapshot", tx=tx)
        assert get(path, authenticated_user="u", tx=tx) == "d"
        remove("//tmp/a")
        assert get(path, authenticated_user="u", tx=tx) == "d"

    @authors("babenko", "ignat")
    @pytest.mark.xfail(run=False, reason="In progress")
    def test_snapshot_no_inherit(self):
        set("//tmp/a", "b")
        assert get("//tmp/a/@inherit_acl")
        tx = start_transaction()
        lock("//tmp/a", mode="snapshot", tx=tx)
        assert not get("//tmp/a/@inherit_acl", tx=tx)

    @authors("babenko", "ignat")
    def test_administer_permission1(self):
        create_user("u")
        create("table", "//tmp/t")
        with pytest.raises(YtError):
            set("//tmp/t/@acl", [], authenticated_user="u")

    @authors("babenko", "ignat")
    def test_administer_permission2(self):
        create_user("u")
        create("table", "//tmp/t")
        set("//tmp/t/@acl/end", make_ace("allow", "u", "administer"))
        set("//tmp/t/@acl", [], authenticated_user="u", force=True)

    @authors("babenko")
    def test_administer_permission3(self):
        create("table", "//tmp/t")
        create_user("u")

        acl = [make_ace("allow", "u", "administer"), make_ace("deny", "u", "write")]
        set("//tmp/t/@acl", acl)

        set("//tmp/t/@inherit_acl", False, authenticated_user="u")
        set("//tmp/t/@acl", acl, authenticated_user="u")
        remove("//tmp/t/@acl/1", authenticated_user="u")

    @authors("babenko")
    def test_administer_permission4(self):
        create("table", "//tmp/t")
        create_user("u")

        acl = [make_ace("deny", "u", "administer")]
        set("//tmp/t/@acl", acl)

        with pytest.raises(YtError):
            set("//tmp/t/@acl", [], authenticated_user="u")
        with pytest.raises(YtError):
            set("//tmp/t/@inherit_acl", False, authenticated_user="u")

    @authors("shakurov")
    def test_administer_permission5(self):
        create_user("u")

        create("map_node", "//tmp/dir1")
        create(
            "map_node",
            "//tmp/dir2",
            attributes={
                "acl": [
                    make_ace("deny", "u", "write"),
                    make_ace("allow", "u", "administer"),
                ]
            },
        )
        create(
            "map_node",
            "//tmp/dir3",
            attributes={
                "acl": [
                    make_ace("allow", "u", "write"),
                    make_ace("deny", "u", "administer"),
                ]
            },
        )
        create(
            "map_node",
            "//tmp/dir4",
            attributes={
                "acl": [
                    make_ace("allow", "u", "write"),
                    make_ace("allow", "u", "administer"),
                ]
            },
        )

        read_acl = [make_ace("allow", "u", "read")]

        with pytest.raises(YtError):
            create(
                "table",
                "//tmp/dir1/t1",
                attributes={"acl": read_acl},
                authenticated_user="u",
            )
        with pytest.raises(YtError):
            create(
                "table",
                "//tmp/dir2/t1",
                attributes={"acl": read_acl},
                authenticated_user="u",
            )
        with pytest.raises(YtError):
            create(
                "table",
                "//tmp/dir3/t1",
                attributes={"acl": read_acl},
                authenticated_user="u",
            )
        create(
            "table",
            "//tmp/dir4/t1",
            attributes={"acl": read_acl},
            authenticated_user="u",
        )

        with pytest.raises(YtError):
            create(
                "table",
                "//tmp/dir1/t2",
                attributes={"inherit_acl": False},
                authenticated_user="u",
            )
        with pytest.raises(YtError):
            create(
                "table",
                "//tmp/dir2/t2",
                attributes={"inherit_acl": False},
                authenticated_user="u",
            )
        with pytest.raises(YtError):
            create(
                "table",
                "//tmp/dir3/t2",
                attributes={"inherit_acl": False},
                authenticated_user="u",
            )
        create(
            "table",
            "//tmp/dir4/t2",
            attributes={"inherit_acl": False},
            authenticated_user="u",
        )

    @authors("babenko", "ignat")
    def test_user_rename_success(self):
        create_user("u1")
        set("//sys/users/u1/@name", "u2")
        assert get("//sys/users/u2/@name") == "u2"

    @authors("babenko", "ignat")
    def test_user_rename_fail(self):
        create_user("u1")
        create_user("u2")
        with pytest.raises(YtError):
            set("//sys/users/u1/@name", "u2")

    @authors("babenko", "ignat")
    def test_deny_create(self):
        create_user("u")
        with pytest.raises(YtError):
            create("account_map", "//tmp/accounts", authenticated_user="u")

    @authors("babenko", "ignat")
    def test_deny_copy_src(self):
        create_user("u")
        with pytest.raises(YtError):
            copy("//sys", "//tmp/sys", authenticated_user="u")

    @authors("babenko", "ignat")
    def test_deny_copy_dst(self):
        create_user("u")
        create("table", "//tmp/t")
        with pytest.raises(YtError):
            copy("//tmp/t", "//sys/t", authenticated_user="u", preserve_account=True)

    @authors("babenko")
    def test_document1(self):
        create_user("u")
        create("document", "//tmp/d")
        set("//tmp/d", {"foo": {}})
        set("//tmp/d/@inherit_acl", False)

        assert get("//tmp", authenticated_user="u") == {"d": None}
        with pytest.raises(YtError):
            get("//tmp/d", authenticated_user="u") == {"foo": {}}
        with pytest.raises(YtError):
            get("//tmp/d/@value", authenticated_user="u")
        with pytest.raises(YtError):
            get("//tmp/d/foo", authenticated_user="u")
        with pytest.raises(YtError):
            set("//tmp/d/foo", {}, authenticated_user="u")
        with pytest.raises(YtError):
            set("//tmp/d/@value", {}, authenticated_user="u")
        with pytest.raises(YtError):
            set("//tmp/d", {"foo": {}}, authenticated_user="u")
        assert ls("//tmp", authenticated_user="u") == ["d"]
        with pytest.raises(YtError):
            ls("//tmp/d", authenticated_user="u")
        with pytest.raises(YtError):
            ls("//tmp/d/foo", authenticated_user="u")
        assert exists("//tmp/d", authenticated_user="u")
        with pytest.raises(YtError):
            exists("//tmp/d/@value", authenticated_user="u")
        with pytest.raises(YtError):
            exists("//tmp/d/foo", authenticated_user="u")
        with pytest.raises(YtError):
            remove("//tmp/d/foo", authenticated_user="u")
        with pytest.raises(YtError):
            remove("//tmp/d", authenticated_user="u")

    @authors("babenko")
    def test_document2(self):
        create_user("u")
        create("document", "//tmp/d")
        set("//tmp/d", {"foo": {}})
        set("//tmp/d/@inherit_acl", False)
        set("//tmp/d/@acl/end", make_ace("allow", "u", "read"))

        assert get("//tmp", authenticated_user="u") == {"d": None}
        assert get("//tmp/d", authenticated_user="u") == {"foo": {}}
        assert get("//tmp/d/@value", authenticated_user="u") == {"foo": {}}
        assert get("//tmp/d/foo", authenticated_user="u") == {}
        with pytest.raises(YtError):
            set("//tmp/d/foo", {}, authenticated_user="u")
        with pytest.raises(YtError):
            set("//tmp/d/@value", {}, authenticated_user="u")
        with pytest.raises(YtError):
            set("//tmp/d", {"foo": {}}, authenticated_user="u")
        assert ls("//tmp", authenticated_user="u") == ["d"]
        assert ls("//tmp/d", authenticated_user="u") == ["foo"]
        assert ls("//tmp/d/foo", authenticated_user="u") == []
        assert exists("//tmp/d", authenticated_user="u")
        assert exists("//tmp/d/@value", authenticated_user="u")
        assert exists("//tmp/d/foo", authenticated_user="u")
        with pytest.raises(YtError):
            remove("//tmp/d/foo", authenticated_user="u")
        with pytest.raises(YtError):
            remove("//tmp/d", authenticated_user="u")

    @authors("babenko")
    def test_document3(self):
        create_user("u")
        create("document", "//tmp/d")
        set("//tmp/d", {"foo": {}})
        set("//tmp/d/@inherit_acl", False)
        set("//tmp/d/@acl/end", make_ace("allow", "u", ["read", "write", "remove"]))

        assert get("//tmp", authenticated_user="u") == {"d": None}
        assert get("//tmp/d", authenticated_user="u") == {"foo": {}}
        assert get("//tmp/d/@value", authenticated_user="u") == {"foo": {}}
        assert get("//tmp/d/foo", authenticated_user="u") == {}
        set("//tmp/d/foo", {}, authenticated_user="u")
        set("//tmp/d/@value", {}, authenticated_user="u")
        set("//tmp/d", {"foo": {}}, authenticated_user="u")
        assert ls("//tmp", authenticated_user="u") == ["d"]
        assert ls("//tmp/d", authenticated_user="u") == ["foo"]
        assert ls("//tmp/d/foo", authenticated_user="u") == []
        assert exists("//tmp/d", authenticated_user="u")
        assert exists("//tmp/d/@value", authenticated_user="u")
        assert exists("//tmp/d/foo", authenticated_user="u")
        remove("//tmp/d/foo", authenticated_user="u")
        remove("//tmp/d", authenticated_user="u")

    @authors("babenko", "ignat")
    def test_copy_account1(self):
        create_account("a")
        create_user("u")

        set("//tmp/x", {})
        set("//tmp/x/@account", "a")

        with pytest.raises(YtError):
            copy("//tmp/x", "//tmp/y", authenticated_user="u", preserve_account=True)

    @authors("babenko", "ignat")
    def test_copy_account2(self):
        create_account("a")
        create_user("u")
        set("//sys/accounts/a/@acl/end", make_ace("allow", "u", "use"))

        set("//tmp/x", {})
        set("//tmp/x/@account", "a")

        copy("//tmp/x", "//tmp/y", authenticated_user="u", preserve_account=True)
        assert get("//tmp/y/@account") == "a"

    @authors("babenko", "ignat")
    def test_copy_account3(self):
        create_account("a")
        create_user("u")

        set("//tmp/x", {"u": "v"})
        set("//tmp/x/u/@account", "a")

        with pytest.raises(YtError):
            copy("//tmp/x", "//tmp/y", authenticated_user="u", preserve_account=True)

    @authors("sandello", "babenko")
    def test_copy_non_writable_src(self):
        # YT-4175
        create_user("u")
        set("//tmp/s", {"x": {"a": 1}})
        set("//tmp/s/@acl/end", make_ace("allow", "u", ["read", "write", "remove"]))
        set("//tmp/s/x/@acl", [make_ace("deny", "u", ["write", "remove"])])
        copy("//tmp/s/x", "//tmp/s/y", authenticated_user="u")
        assert get("//tmp/s/y", authenticated_user="u") == get("//tmp/s/x", authenticated_user="u")

    @authors("sandello", "babenko")
    def test_copy_and_move_require_read_on_source(self):
        create_user("u")
        set("//tmp/s", {"x": {}})
        set("//tmp/s/@acl/end", make_ace("allow", "u", ["read", "write", "remove"]))
        set("//tmp/s/x/@acl", [make_ace("deny", "u", "read")])
        with pytest.raises(YtError):
            copy("//tmp/s/x", "//tmp/s/y", authenticated_user="u")
        with pytest.raises(YtError):
            move("//tmp/s/x", "//tmp/s/y", authenticated_user="u")

    @authors("sandello", "babenko")
    def test_copy_and_move_require_write_on_target_parent(self):
        create_user("u")
        set("//tmp/s", {"x": {}})
        set("//tmp/s/@acl/end", make_ace("allow", "u", ["read", "remove"]))
        set("//tmp/s/@acl/end", make_ace("deny", "u", ["write"]))
        with pytest.raises(YtError):
            copy("//tmp/s/x", "//tmp/s/y", authenticated_user="u")
        with pytest.raises(YtError):
            move("//tmp/s/x", "//tmp/s/y", authenticated_user="u")

    @authors("sandello", "babenko")
    def test_copy_and_move_requires_remove_on_target_if_exists(self):
        create_user("u")
        set("//tmp/s", {"x": {}, "y": {}})
        set("//tmp/s/@acl/end", make_ace("allow", "u", ["read", "write", "remove"]))
        set("//tmp/s/y/@acl", [make_ace("deny", "u", "remove")])
        with pytest.raises(YtError):
            copy("//tmp/s/x", "//tmp/s/y", force=True, authenticated_user="u")
        with pytest.raises(YtError):
            move("//tmp/s/x", "//tmp/s/y", force=True, authenticated_user="u")

    @authors("sandello", "babenko")
    def test_move_requires_remove_on_self_and_write_on_self_parent(self):
        create_user("u")
        set("//tmp/s", {"x": {}})
        set("//tmp/s/@acl", [make_ace("allow", "u", ["read", "write", "remove"])])
        set("//tmp/s/x/@acl", [make_ace("deny", "u", "remove")])
        with pytest.raises(YtError):
            move("//tmp/s/x", "//tmp/s/y", authenticated_user="u")
        set("//tmp/s/x/@acl", [])
        set(
            "//tmp/s/@acl",
            [
                make_ace("allow", "u", ["read", "remove"]),
                make_ace("deny", "u", "write"),
            ],
        )
        with pytest.raises(YtError):
            move("//tmp/s/x", "//tmp/s/y", authenticated_user="u")
        set("//tmp/s/@acl", [make_ace("allow", "u", ["read", "write", "remove"])])
        move("//tmp/s/x", "//tmp/s/y", authenticated_user="u")

    @authors("babenko", "ignat")
    def test_superusers(self):
        create("table", "//sys/protected")
        create_user("u")
        with pytest.raises(YtError):
            remove("//sys/protected", authenticated_user="u")
        add_member("u", "superusers")
        remove("//sys/protected", authenticated_user="u")

    @authors("babenko")
    def test_remove_self_requires_permission(self):
        create_user("u")
        set("//tmp/x", {}, force=True)
        set("//tmp/x/y", {}, force=True)

        set("//tmp/x/@inherit_acl", False)
        with pytest.raises(YtError):
            remove("//tmp/x", authenticated_user="u")
        with pytest.raises(YtError):
            remove("//tmp/x/y", authenticated_user="u")

        set("//tmp/x/@acl", [make_ace("allow", "u", "write")])
        set("//tmp/x/y/@acl", [make_ace("deny", "u", "remove")])
        with pytest.raises(YtError):
            remove("//tmp/x", authenticated_user="u")
        with pytest.raises(YtError):
            remove("//tmp/x/y", authenticated_user="u")

        set("//tmp/x/y/@acl", [make_ace("allow", "u", "remove")])
        with pytest.raises(YtError):
            remove("//tmp/x", authenticated_user="u")
        remove("//tmp/x/y", authenticated_user="u")
        with pytest.raises(YtError):
            remove("//tmp/x", authenticated_user="u")

        set("//tmp/x/@acl", [make_ace("allow", "u", "remove")])
        remove("//tmp/x", authenticated_user="u")

    @authors("babenko")
    def test_remove_recursive_requires_permission(self):
        create_user("u")
        set("//tmp/x", {}, force=True)
        set("//tmp/x/y", {}, force=True)

        set("//tmp/x/@inherit_acl", False)
        with pytest.raises(YtError):
            remove("//tmp/x/*", authenticated_user="u")
        set("//tmp/x/@acl", [make_ace("allow", "u", "write")])
        set("//tmp/x/y/@acl", [make_ace("deny", "u", "remove")])
        with pytest.raises(YtError):
            remove("//tmp/x/*", authenticated_user="u")
        set("//tmp/x/y/@acl", [make_ace("allow", "u", "remove")])
        remove("//tmp/x/*", authenticated_user="u")

    @authors("babenko")
    def test_set_self_requires_remove_permission(self):
        create_user("u")
        set("//tmp/x", {}, force=True)
        set("//tmp/x/y", {}, force=True)

        set("//tmp/x/@inherit_acl", False)
        with pytest.raises(YtError):
            set("//tmp/x", {}, authenticated_user="u", force=True)
        with pytest.raises(YtError):
            set("//tmp/x/y", {}, authenticated_user="u", force=True)

        set("//tmp/x/@acl", [make_ace("allow", "u", "write")])
        set("//tmp/x/y/@acl", [make_ace("deny", "u", "remove")])
        set("//tmp/x/y", {}, authenticated_user="u", force=True)
        with pytest.raises(YtError):
            set("//tmp/x", {}, authenticated_user="u", force=True)

        set("//tmp/x/@acl", [make_ace("allow", "u", "write")])
        set("//tmp/x/y/@acl", [make_ace("allow", "u", "remove")])
        set("//tmp/x/y", {}, authenticated_user="u", force=True)
        set("//tmp/x", {}, authenticated_user="u", force=True)

    @authors("babenko")
    def test_guest_can_remove_users_groups(self):
        create_user("u")
        create_group("g")

        with pytest.raises(YtError):
            remove_user("u", sync=False, authenticated_user="guest")
        with pytest.raises(YtError):
            remove("//sys/groups/g", authenticated_user="guest")

        set("//sys/schemas/user/@acl/end", make_ace("allow", "guest", "remove"))
        set("//sys/schemas/group/@acl/end", make_ace("allow", "guest", "remove"))

        remove_user("u", authenticated_user="guest")
        remove("//sys/groups/g", authenticated_user="guest")

        remove("//sys/schemas/user/@acl/-1")
        remove("//sys/schemas/group/@acl/-1")

    @authors("babenko")
    def test_set_acl_upon_construction(self):
        create_user("u")
        create(
            "table",
            "//tmp/t",
            attributes={"acl": [make_ace("allow", "u", "write")], "inherit_acl": False},
        )
        assert len(get("//tmp/t/@acl")) == 1

    @authors("babenko")
    def test_group_write_acl(self):
        create_user("u")
        create_group("g")
        with pytest.raises(YtError):
            add_member("u", "g", authenticated_user="guest")
        set("//sys/groups/g/@acl/end", make_ace("allow", "guest", "write"))
        add_member("u", "g", authenticated_user="guest")

    @authors("babenko")
    def test_user_remove_acl(self):
        create_user("u")
        with pytest.raises(YtError):
            remove_user("u", sync=False, authenticated_user="guest")
        set("//sys/users/u/@acl/end", make_ace("allow", "guest", "remove"))
        remove_user("u", authenticated_user="guest")

    @authors("babenko")
    def test_group_remove_acl(self):
        create_group("g")
        with pytest.raises(YtError):
            remove("//sys/groups/g", authenticated_user="guest")
        set("//sys/groups/g/@acl/end", make_ace("allow", "guest", "remove"))
        remove("//sys/groups/g", authenticated_user="guest")

    @authors("babenko")
    def test_default_inheritance(self):
        create(
            "map_node",
            "//tmp/m",
            attributes={"acl": [make_ace("allow", "guest", "remove")]},
        )
        assert get("//tmp/m/@acl/0/inheritance_mode") == "object_and_descendants"

    @authors("babenko")
    def test_read_from_per_user_cache(self):
        create_user("u")
        set("//tmp/a", "b")
        set("//tmp/a/@acl/end", make_ace("deny", "u", "read"))
        with pytest.raises(YtError):
            get("//tmp/a", authenticated_user="u")
        with pytest.raises(YtError):
            get("//tmp/a", authenticated_user="u", read_from="cache")

    @authors("babenko")
    def test_read_from_global_cache(self):
        create_user("u")
        set("//tmp/a", "b")
        set("//tmp/a/@acl/end", make_ace("deny", "u", "read"))
        assert get("//tmp/a", read_from="cache", disable_per_user_cache=True) == "b"
        with pytest.raises(YtError):
            get("//tmp/a", authenticated_user="u")
        assert get("//tmp/a", authenticated_user="u", read_from="cache", disable_per_user_cache=True) == "b"

    @authors("babenko")
    def test_no_owner_auth(self):
        with pytest.raises(YtError):
            get("//tmp", authenticated_user="owner")

    @authors("babenko")
    def test_list_with_attr_yt_7165(self):
        create_user("u")
        create("map_node", "//tmp/x")
        set("//tmp/x/@acl", [make_ace("allow", "u", "read", "object_only")])
        set("//tmp/x/@inherit_acl", False)
        create("map_node", "//tmp/x/y")
        set("//tmp/x/y/@attr", "value")
        assert "attr" not in ls("//tmp/x", attributes=["attr"], authenticated_user="u")[0].attributes

    @authors("savrus")
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
        with pytest.raises(YtError):
            check_permission("u", "read", "//tmp")

    @authors("levysotsky")
    def test_effective_acl(self):
        create_user("u")
        for ch in "abcd":
            for idx in range(1, 5):
                create_user(ch + str(idx))

        create("map_node", "//tmp/a")
        create("map_node", "//tmp/a/b")
        create("map_node", "//tmp/a/b/c")
        create("map_node", "//tmp/a/b/c/d")

        set("//tmp/a/@inherit_acl", False)

        set(
            "//tmp/a/@acl",
            [
                make_ace("allow", "a1", "read", "immediate_descendants_only"),
                make_ace("allow", "a2", "read", "object_only"),
                make_ace("allow", "a3", "read", "object_and_descendants"),
                make_ace("allow", "a4", "read", "descendants_only"),
            ],
        )

        set(
            "//tmp/a/b/@acl",
            [
                make_ace("allow", "b1", "read", "immediate_descendants_only"),
                make_ace("allow", "b2", "read", "object_only"),
                make_ace("allow", "b3", "read", "object_and_descendants"),
                make_ace("allow", "b4", "read", "descendants_only"),
            ],
        )

        set(
            "//tmp/a/b/c/@acl",
            [
                make_ace("allow", "c1", "read", "immediate_descendants_only"),
                make_ace("allow", "c2", "read", "object_only"),
                make_ace("allow", "c3", "read", "object_and_descendants"),
                make_ace("allow", "c4", "read", "descendants_only"),
            ],
        )

        set(
            "//tmp/a/b/c/d/@acl",
            [
                make_ace("allow", "d1", "read", "immediate_descendants_only"),
                make_ace("allow", "d2", "read", "object_only"),
                make_ace("allow", "d3", "read", "object_and_descendants"),
                make_ace("allow", "d4", "read", "descendants_only"),
            ],
        )

        assert_items_equal(
            get("//tmp/a/@effective_acl"),
            [
                make_ace("allow", "a2", "read", "object_only"),
                make_ace("allow", "a3", "read", "object_and_descendants"),
            ],
        )

        assert_items_equal(
            get("//tmp/a/b/@effective_acl"),
            [
                make_ace("allow", "a1", "read", "object_only"),
                make_ace("allow", "a3", "read", "object_and_descendants"),
                make_ace("allow", "a4", "read", "object_and_descendants"),
                make_ace("allow", "b2", "read", "object_only"),
                make_ace("allow", "b3", "read", "object_and_descendants"),
            ],
        )

        assert_items_equal(
            get("//tmp/a/b/c/@effective_acl"),
            [
                make_ace("allow", "a3", "read", "object_and_descendants"),
                make_ace("allow", "a4", "read", "object_and_descendants"),
                make_ace("allow", "b1", "read", "object_only"),
                make_ace("allow", "b3", "read", "object_and_descendants"),
                make_ace("allow", "b4", "read", "object_and_descendants"),
                make_ace("allow", "c2", "read", "object_only"),
                make_ace("allow", "c3", "read", "object_and_descendants"),
            ],
        )

        assert_items_equal(
            get("//tmp/a/b/c/d/@effective_acl"),
            [
                make_ace("allow", "a3", "read", "object_and_descendants"),
                make_ace("allow", "a4", "read", "object_and_descendants"),
                make_ace("allow", "b3", "read", "object_and_descendants"),
                make_ace("allow", "b4", "read", "object_and_descendants"),
                make_ace("allow", "c1", "read", "object_only"),
                make_ace("allow", "c3", "read", "object_and_descendants"),
                make_ace("allow", "c4", "read", "object_and_descendants"),
                make_ace("allow", "d2", "read", "object_only"),
                make_ace("allow", "d3", "read", "object_and_descendants"),
            ],
        )

    @authors("babenko")
    def test_read_table_with_denied_columns(self):
        create_user("u")

        create(
            "table",
            "//tmp/t",
            attributes={
                "schema": [
                    {"name": "public", "type": "string"},
                    {"name": "secret", "type": "string"},
                ],
                "acl": [
                    make_ace("deny", "u", "read", columns="secret"),
                ],
            },
        )
        write_table("//tmp/t", [{"public": "a", "secret": "b"}])

        assert read_table("//tmp/t{public}") == [{"public": "a"}]

        with pytest.raises(YtError):
            read_table("//tmp/t", authenticated_user="u")
        with pytest.raises(YtError):
            read_table("//tmp/t{secret}", authenticated_user="u")

    @authors("babenko")
    def test_columnar_acl_sanity(self):
        create_user("u")
        create(
            "table",
            "//tmp/t",
            attributes={"acl": [make_ace("allow", "u", "read", columns="secret")]},
        )
        with pytest.raises(YtError):
            create(
                "table",
                "//tmp/t",
                attributes={"acl": [make_ace("allow", "u", "write", columns="secret")]},
            )

    @authors("babenko")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    @pytest.mark.parametrize("strict", [False, True])
    def test_read_table_with_omitted_columns(self, optimize_for, strict):
        create_user("u")

        schema = yson.YsonList([{"name": "public", "type": "string"}, {"name": "secret", "type": "string"}])
        schema.attributes["strict"] = strict

        create(
            "table",
            "//tmp/t",
            attributes={
                "optimize_for": optimize_for,
                "schema": schema,
                "acl": [
                    make_ace("deny", "u", "read", columns="secret"),
                ],
            },
        )

        row = {"public": "a", "secret": "b"}
        if not strict:
            row["other"] = "c"

        public_row = row
        public_row.pop("secret", None)

        write_table("//tmp/t", [row])

        def do(path, expected_row, expected_omitted_columns):
            response_parameters = {}
            rows = read_table(
                path,
                omit_inaccessible_columns=True,
                response_parameters=response_parameters,
                authenticated_user="u",
            )
            assert rows == [expected_row]
            assert response_parameters["omitted_inaccessible_columns"] == expected_omitted_columns

        do("//tmp/t{public}", {"public": "a"}, [])
        do("//tmp/t", public_row, [b"secret"])
        do("//tmp/t{secret}", {}, [b"secret"])

    @authors("babenko")
    def test_map_table_with_denied_columns(self):
        create_user("u")

        create(
            "table",
            "//tmp/t_in",
            attributes={
                "schema": [
                    {"name": "public", "type": "string"},
                    {"name": "secret", "type": "string"},
                ],
                "acl": [
                    make_ace("deny", "u", "read", columns="secret"),
                ],
            },
        )
        write_table("//tmp/t_in", [{"public": "a", "secret": "b"}])

        create("table", "//tmp/t_out")

        map(
            in_="//tmp/t_in{public}",
            out="//tmp/t_out",
            command="cat",
            authenticated_user="u",
        )
        assert read_table("//tmp/t_out") == [{"public": "a"}]

        with pytest.raises(YtError):
            map(
                in_="//tmp/t_in{secret}",
                out="//tmp/t_out",
                command="cat",
                authenticated_user="u",
            )
        with pytest.raises(YtError):
            map(
                in_="//tmp/t_in",
                out="//tmp/t_out",
                command="cat",
                authenticated_user="u",
            )

    @authors("babenko")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    @pytest.mark.parametrize("strict", [False, True])
    def test_map_table_with_omitted_columns(self, optimize_for, strict):
        create_user("u")
        create(
            "table",
            "//tmp/t_in",
            attributes={
                "optimize_for": optimize_for,
                "schema": make_schema(
                    [
                        {"name": "public", "type": "string"},
                        {"name": "secret", "type": "string"},
                    ],
                    strict=strict,
                ),
                "acl": [make_ace("deny", "u", "read", columns="secret")],
            },
        )

        row = {"public": "a", "secret": "b"}
        if not strict:
            row["other"] = "c"

        public_row = row
        public_row.pop("secret", None)

        write_table("//tmp/t_in", [row])

        def do(input_path, expected_row, expect_alert):
            create("table", "//tmp/t_out", force=True)
            op = map(
                in_=input_path,
                out="//tmp/t_out",
                command="cat",
                authenticated_user="u",
                spec={"omit_inaccessible_columns": True},
            )
            alerts = op.get_alerts()
            if expect_alert:
                assert len(alerts) == 1
                assert alerts["omitted_inaccessible_columns_in_input_tables"]["attributes"]["input_tables"] == [
                    {"path": "//tmp/t_in", "columns": ["secret"]}
                ]
            else:
                assert len(alerts) == 0
            assert read_table("//tmp/t_out") == [expected_row]

        do("//tmp/t_in{public}", {"public": "a"}, False)
        do("//tmp/t_in", public_row, True)
        do("//tmp/t_in{secret}", {}, True)

    @authors("shakurov")
    def test_orphaned_node(self):
        create_user("u")

        create(
            "map_node",
            "//tmp/d",
            attributes={"inherit_acl": False, "acl": [make_ace("allow", "u", "read")]},
        )

        table_id = create("table", "//tmp/d/t")

        tx = start_transaction()
        lock("//tmp/d/t", mode="snapshot", tx=tx)

        assert check_permission("u", "read", "#" + table_id)["action"] == "allow"

        remove("//tmp/d/t")

        assert check_permission("u", "read", "#" + table_id)["action"] == "allow"

    def _test_columnar_acl_copy_yt_12749(self, src_dir, dst_dir):
        create(
            "table",
            src_dir + "/t1",
            attributes={
                "schema": [
                    {"name": "a", "type": "string"},
                    {"name": "b", "type": "string"},
                ],
                "acl": [
                    make_ace("deny", "u1", "read", columns="b"),
                ],
            },
        )

        create(
            "table",
            src_dir + "/t2",
            attributes={
                "schema": [
                    {"name": "a", "type": "string"},
                    {"name": "b", "type": "string"},
                ],
                "acl": [
                    make_ace("allow", "u2", "read", columns="b"),
                ],
            },
        )

        # Explicitly denied.
        with pytest.raises(YtError):
            copy(src_dir + "/t1", dst_dir + "/t1_copy", authenticated_user="u1")

        # No matching ACE.
        with pytest.raises(YtError):
            copy(src_dir + "/t2", dst_dir + "/t2_copy", authenticated_user="u1")

        remove(src_dir + "/t1")
        remove(src_dir + "/t2")

    @authors("shakurov")
    def test_columnar_acl_copy_yt_12749(self):
        create_user("u1")
        create_user("u2")

        self._test_columnar_acl_copy_yt_12749("//tmp", "//tmp")

    @authors("shakurov")
    def test_special_acd_holders(self):
        create_user("u1")

        create_access_control_object_namespace("cats")
        create_access_control_object("garfield", "cats")
        set("//sys/access_control_object_namespaces/cats/garfield/@acl/end",
            make_ace("allow", "u1", "read"))

        remove("//sys/access_control_object_namespaces/cats/garfield")
        gc_collect()

        # Must not crash.
        build_snapshot(cell_id=None, set_read_only=False)
        get("//sys/users/u1/@")

    @authors("don-dron")
    def test_create_access_control_objects_with_ignore_existing(self):
        create_access_control_object_namespace("cats", ignore_existing=True)
        create_access_control_object("garfield", "cats", ignore_existing=True)

        remove("//sys/access_control_object_namespaces/cats/garfield")
        remove("//sys/access_control_object_namespaces/cats")

    @authors("shakurov")
    def test_access_control_object_recursive_get(self):
        create_access_control_object_namespace("cats")
        create_access_control_object("garfield", "cats")

        assert "principal" in get("//sys/access_control_object_namespaces/cats/garfield")

    @authors("shakurov")
    def test_access_control_object_revision(self):
        create_access_control_object_namespace("cats")
        create_access_control_object("garfield", "cats")

        old_revision = get("//sys/access_control_object_namespaces/cats/garfield/@revision")
        old_attribute_revision = get("//sys/access_control_object_namespaces/cats/garfield/@attribute_revision")
        old_content_revision = get("//sys/access_control_object_namespaces/cats/garfield/@content_revision")
        assert old_revision == max(old_attribute_revision, old_content_revision)

        # Removing a non-existent attribute should not affect revision.
        with pytest.raises(YtError):
            remove("//sys/access_control_object_namespaces/cats/garfield/@foo")

        assert get("//sys/access_control_object_namespaces/cats/garfield/@attribute_revision") == old_attribute_revision

        set("//sys/access_control_object_namespaces/cats/garfield/@foo", "bar")
        new_attribute_revision = get("//sys/access_control_object_namespaces/cats/garfield/@attribute_revision")
        new_revision = get("//sys/access_control_object_namespaces/cats/garfield/@attribute_revision")
        assert new_revision == new_attribute_revision
        assert new_attribute_revision > old_attribute_revision
        old_attribute_revision = new_attribute_revision

        create_user("u1")
        set("//sys/access_control_object_namespaces/cats/garfield/@acl/end", make_ace("allow", "u1", "read"))
        new_attribute_revision = get("//sys/access_control_object_namespaces/cats/garfield/@attribute_revision")
        assert new_attribute_revision > old_attribute_revision

    @authors("kvk1920")
    def test_medium_permission_validation(self):
        create_domestic_medium("prohibited")
        create_user("u")

        with raises_yt_error("Access denied"):
            create("table", "//tmp/t", attributes={"primary_medium": "prohibited"}, authenticated_user="u")

    def __test_set_attributes_request(self, path, key, value, user, force=False):
        set(path + "/@" + key,
            value,
            authenticated_user=user,
            force=force)

    def __test_multiset_attributes_request(self, path, key, value, user, force=False):
        multiset_attributes(
            path + "/@",
            {key: value},
            authenticated_user=user,
            force=force)

    @authors("vovamelnikov")
    @pytest.mark.parametrize("request_type", ["set", "multiset"])
    @pytest.mark.parametrize("key", ["acl", "inherit_acl"])
    def test_irreversible_modification(self, request_type, key):
        set("//sys/@config/security_manager/forbid_irreversible_changes", True)
        create_user("u")

        create(
            "map_node",
            "//tmp/dir",
            attributes={
                "acl": [
                    make_ace("allow", "u", ["administer", "read", "write"]),
                ],
            }
        )
        create(
            "map_node",
            "//tmp/dir/subdir1",
        )

        request_func = {
            "set": self.__test_set_attributes_request,
            "multiset": self.__test_multiset_attributes_request,
        }[request_type]

        path, value = {
            "acl": ("//tmp/dir", [make_ace("deny", "u", "administer")]),
            "inherit_acl": ("//tmp/dir/subdir1", False),
        }[key]

        with pytest.raises(YtError, match="It will be impossible for this user to revert this modification"):
            request_func(
                path=path,
                key=key,
                value=value,
                user='u',
                force=False)

        request_func(
            path=path,
            key=key,
            value=value,
            user='u',
            force=True)

    @authors("vovamelnikov")
    @pytest.mark.parametrize("request_type", ["set", "multiset"])
    def test_irreversible_owner_modification(self, request_type):
        set("//sys/@config/security_manager/forbid_irreversible_changes", True)
        create_user("u1")
        create_user("u2")

        create(
            "map_node",
            "//tmp/dir",
            attributes={
                "acl": [
                    make_ace("allow", "everyone", ["administer", "write"]),
                    make_ace("deny", "owner", ["administer", "write"]),
                ],
                "owner": "u1"
            }
        )

        request_func = {
            "set": self.__test_set_attributes_request,
            "multiset": self.__test_multiset_attributes_request,
        }[request_type]

        with pytest.raises(YtError, match="It will be impossible for this user to revert this modification"):
            request_func(
                path="//tmp/dir",
                key="owner",
                value="u2",
                user="u2",
                force=False)

        request_func(
            path="//tmp/dir",
            key="owner",
            value="u2",
            user="u2",
            force=True)

##################################################################


class TestCypressAclsMulticell(TestCypressAcls):
    NUM_TEST_PARTITIONS = 3
    NUM_SECONDARY_MASTER_CELLS = 2


class TestCheckPermissionRpcProxy(CheckPermissionBase):
    DRIVER_BACKEND = "rpc"
    ENABLE_HTTP_PROXY = True
    ENABLE_RPC_PROXY = True


class TestCypressAclsPortal(TestCypressAclsMulticell):
    NUM_TEST_PARTITIONS = 3
    NUM_SECONDARY_MASTER_CELLS = 3
    ENABLE_TMP_PORTAL = True

    @authors("shakurov")
    def test_columnar_acl_copy_yt_12749(self):
        set(
            "//sys/@config/multicell_manager/cell_descriptors",
            {
                "11": {"roles": ["cypress_node_host"]},
                "12": {"roles": ["cypress_node_host"]},
                "13": {"roles": ["chunk_host"]},
            },
        )

        super(TestCypressAclsPortal, self).test_columnar_acl_copy_yt_12749()

        create("portal_entrance", "//portals/p", attributes={"exit_cell_tag": 12})
        self._test_columnar_acl_copy_yt_12749("//tmp", "//portals/p")
        self._test_columnar_acl_copy_yt_12749("//portals/p", "//tmp")

    @authors("shakurov")
    def test_special_acd_holders(self):
        super(TestCypressAclsPortal, self).test_special_acd_holders()

        create("portal_entrance", "//portals/p1", attributes={"exit_cell_tag": 12})
        set("//portals/p1&/@acl/end", make_ace("allow", "u1", "read"))
        portal_exit_id = get("//portals/p1/@id")

        remove("//portals/p1")
        gc_collect()

        wait(lambda: not exists("#" + portal_exit_id))

        # Must not crash.
        build_snapshot(cell_id=None, set_read_only=False)
        get("//sys/users/u1/@")
