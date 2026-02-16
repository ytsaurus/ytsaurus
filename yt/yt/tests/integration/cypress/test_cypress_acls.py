from yt_env_setup import YTEnvSetup, wait

from yt_commands import (
    assert_yt_error, authors, create, ls, get, set, copy, move, remove, create_domestic_medium, exists, multiset_attributes,
    create_account, create_user, create_group, make_ace, check_permission, check_permission_by_acl, add_member,
    remove_group, remove_user, start_transaction, lock, read_table, write_table, alter_table, map,
    set_account_disk_space_limit, raises_yt_error, gc_collect, build_snapshot, create_access_control_object_namespace,
    create_access_control_object, get_active_primary_master_leader_address, concatenate, get_driver,
    abort_transaction, unlock,
)

from yt_type_helpers import make_schema

from yt_helpers import profiler_factory

from yt_sequoia_helpers import not_implemented_in_sequoia

from yt.environment.helpers import assert_items_equal
from yt.common import YtError
import yt.yson as yson

import pytest

##################################################################


def make_rl_ace(users, row_access_predicate=None, mode=None, permission="read"):
    ace = make_ace("allow", users, permission)
    if row_access_predicate is not None:
        ace["row_access_predicate"] = row_access_predicate
    if mode is not None:
        ace["inapplicable_row_access_predicate_mode"] = mode
    return ace


@pytest.mark.enabled_multidaemon
class TestCheckPermissionProfiling(YTEnvSetup):
    ENABLE_MULTIDAEMON = True
    NUM_MASTERS = 1
    NUM_NODES = 3

    @authors("h0pless")
    def test_check_permission_profilers(self):
        create_user("grudgebearer")
        resulting_acl = [make_ace("allow", "grudgebearer", ["administer", "read", "write", "create"])]
        for i in range(1, 5):
            create_user(f"elf{i}")
            resulting_acl.append(make_ace("deny", f"elf{i}", ["administer", "read", "write", "create"]))
            create_user(f"human{i}")
            resulting_acl.append(make_ace("deny", f"human{i}", ["administer", "read", "write", "create"]))
            create_user(f"dwarf{i}")
            resulting_acl.append(make_ace("allow", f"dwarf{i}", ["read", "write"]))

        create(
            "map_node",
            "//tmp/grudges",
            attributes={
                "acl": resulting_acl,
            },
        )

        for grudge_counter in range(1, 50):
            create(
                "map_node",
                f"//tmp/grudges/grudge_{grudge_counter}",
                authenticated_user="grudgebearer",
            )

        leader_address = get_active_primary_master_leader_address(self)
        profiler = profiler_factory().at_primary_master(leader_address)
        check_permission_time = profiler.counter("permission_validation/check_permission_wall_time").get()
        assert check_permission_time != 0
        acl_iteration_time = profiler.counter("permission_validation/acl_iteration_wall_time").get()
        assert acl_iteration_time != 0


##################################################################


class CheckPermissionBase(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3

    @authors("danilalexeev")
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

    @authors("danilalexeev")
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

    @authors("danilalexeev")
    def test_descendants_only_inheritance(self):
        create(
            "map_node",
            "//tmp/m",
            attributes={"acl": [make_ace("allow", "guest", "remove", "descendants_only")]},
        )
        create("map_node", "//tmp/m/s")
        create("document", "//tmp/m/s/r")
        assert check_permission("guest", "remove", "//tmp/m/s/r")["action"] == "allow"
        assert check_permission("guest", "remove", "//tmp/m/s")["action"] == "allow"
        assert check_permission("guest", "remove", "//tmp/m")["action"] == "deny"

    @authors("danilalexeev")
    def test_object_only_inheritance(self):
        create(
            "map_node",
            "//tmp/m",
            attributes={"acl": [make_ace("allow", "guest", "remove", "object_only")]},
        )
        create("map_node", "//tmp/m/s")
        assert check_permission("guest", "remove", "//tmp/m/s")["action"] == "deny"
        assert check_permission("guest", "remove", "//tmp/m")["action"] == "allow"

    @authors("danilalexeev")
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

    @authors("danilalexeev")
    def test_check_permission_for_virtual_maps(self):
        assert check_permission("guest", "read", "//sys/chunks")["action"] == "allow"

    @authors("kiselyovp")
    @not_implemented_in_sequoia
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
    @not_implemented_in_sequoia
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

    @authors("danilalexeev")
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

    @authors("danilalexeev")
    @pytest.mark.parametrize("acl_path", ["//tmp/dir/t", "//tmp/dir"])
    @pytest.mark.parametrize("rename_columns", [False, True])
    def test_check_permission_for_columnar_acl(self, acl_path, rename_columns):
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


@pytest.mark.enabled_multidaemon
class TestCypressAcls(CheckPermissionBase):
    ENABLE_MULTIDAEMON = True
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

    @authors("danilalexeev")
    def test_denying_acl1(self):
        create_user("u")
        self._test_denying_acl("//tmp/a", "u", "//tmp/a", "u")

    @authors("danilalexeev")
    def test_denying_acl2(self):
        create_user("u")
        create_group("g")
        add_member("u", "g")
        self._test_denying_acl("//tmp/a", "u", "//tmp/a", "g")

    @authors("danilalexeev")
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

    @authors("danilalexeev")
    def test_allowing_acl1(self):
        self._test_allowing_acl("//tmp/a", "guest", "//tmp/a", "guest")

    @authors("danilalexeev")
    def test_allowing_acl2(self):
        create_group("g")
        add_member("guest", "g")
        self._test_allowing_acl("//tmp/a", "guest", "//tmp/a", "g")

    @authors("danilalexeev")
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

    @authors("kvk1920")
    def test_schema_acl2(self):
        create_user("u")
        start_transaction(authenticated_user="u")
        set("//sys/schemas/transaction/@acl/end", make_ace("deny", "u", "create"))
        with raises_yt_error(
                "Access denied for user \"u\": \"create\" permission for \"transaction\" "
                "schema is denied for \"u\" by ACE at \"transaction\" schema"):
            start_transaction(authenticated_user="u")

    @authors("danilalexeev")
    def test_user_destruction(self):
        old_acl = get("//tmp/@acl")
        create("map_node", "//tmp/dir")
        set("//tmp/dir/@acl", old_acl)
        set("//tmp/dir/@inherit_acl", False)

        create_user("u")
        set("//tmp/dir/@acl/end", make_ace("deny", "u", "write"))

        remove_user("u")
        assert get("//tmp/dir/@acl") == old_acl

    @authors("danilalexeev")
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

    @authors("danilalexeev")
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

    @authors("danilalexeev")
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

    @authors("babenko", "danilalexeev")
    def test_scheduler_in_acl(self):
        self._prepare_scheduler_test()
        set("//tmp/t1/@acl/end", make_ace("deny", "u", "read"))
        with pytest.raises(YtError):
            map(in_="//tmp/t1", out="//tmp/t2", command="cat", authenticated_user="u")

    @authors("babenko", "danilalexeev")
    def test_scheduler_out_acl(self):
        self._prepare_scheduler_test()
        set("//tmp/t2/@acl/end", make_ace("deny", "u", "write"))
        with pytest.raises(YtError):
            map(in_="//tmp/t1", out="//tmp/t2", command="cat", authenticated_user="u")

    @authors("babenko", "danilalexeev")
    def test_scheduler_account_quota(self):
        self._prepare_scheduler_test()
        set("//tmp/t2/@account", "a")
        set("//sys/accounts/a/@acl/end", make_ace("allow", "u", "use"))
        set_account_disk_space_limit("a", 0)
        with pytest.raises(YtError):
            map(in_="//tmp/t1", out="//tmp/t2", command="cat", authenticated_user="u")

    @authors("acid", "danilalexeev")
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

    @authors("danilalexeev")
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

    @authors("shakurov", "danilalexeev")
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

    @authors("danilalexeev")
    def test_create_in_tx1(self):
        create_user("u")
        tx = start_transaction()
        create("table", "//tmp/a", tx=tx, authenticated_user="u")
        assert read_table("//tmp/a", tx=tx, authenticated_user="u") == []

    @authors("danilalexeev")
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

    @authors("danilalexeev")
    def test_administer_permission1(self):
        create_user("u")
        create("table", "//tmp/t")
        with pytest.raises(YtError):
            set("//tmp/t/@acl", [], authenticated_user="u")

    @authors("danilalexeev")
    def test_administer_permission2(self):
        create_user("u")
        create("table", "//tmp/t")
        set("//tmp/t/@acl/end", make_ace("allow", "u", "administer"))
        set("//tmp/t/@acl", [], authenticated_user="u", force=True)

    @authors("danilalexeev")
    def test_administer_permission3(self):
        create("table", "//tmp/t")
        create_user("u")

        acl = [make_ace("allow", "u", "administer"), make_ace("deny", "u", "write")]
        set("//tmp/t/@acl", acl)

        set("//tmp/t/@inherit_acl", False, authenticated_user="u")
        set("//tmp/t/@acl", acl, authenticated_user="u")
        remove("//tmp/t/@acl/1", authenticated_user="u")

    @authors("danilalexeev")
    def test_administer_permission4(self):
        create("table", "//tmp/t")
        create_user("u")

        acl = [make_ace("deny", "u", "administer")]
        set("//tmp/t/@acl", acl)

        with pytest.raises(YtError):
            set("//tmp/t/@acl", [], authenticated_user="u")
        with pytest.raises(YtError):
            set("//tmp/t/@inherit_acl", False, authenticated_user="u")

    @authors("shakurov", "danilalexeev")
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

    @authors("danilalexeev")
    def test_user_rename_success(self):
        create_user("u1")
        set("//sys/users/u1/@name", "u2")
        assert get("//sys/users/u2/@name") == "u2"

    @authors("danilalexeev")
    def test_user_rename_fail(self):
        create_user("u1")
        create_user("u2")
        with pytest.raises(YtError):
            set("//sys/users/u1/@name", "u2")

    @authors("danilalexeev")
    def test_user_rename_ace(self):
        create_user("u1")
        create("map_node", "//tmp/d", attributes={
            "inherit_acl": False,
            "acl": [make_ace("allow", "u1", "read")]
        })
        set("//sys/users/u1/@name", "u2")
        wait(lambda: get("//tmp/d/@acl/0/subjects/0") == "u2")
        assert get("//tmp/d", authenticated_user="u2") == {}

    @authors("babenko", "ignat")
    def test_deny_create(self):
        create_user("u")
        with pytest.raises(YtError):
            create("account_map", "//tmp/accounts", authenticated_user="u")

    @authors("danilalexeev")
    def test_deny_copy_src(self):
        create_user("u")
        with pytest.raises(YtError):
            copy("//sys", "//tmp/sys", authenticated_user="u")

    @authors("danilalexeev")
    def test_deny_copy_dst(self):
        create_user("u")
        create("table", "//tmp/t")
        with pytest.raises(YtError):
            copy("//tmp/t", "//sys/t", authenticated_user="u", preserve_account=True)

    @authors("danilalexeev")
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

    @authors("danilalexeev")
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

    @authors("danilalexeev")
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

    @authors("danilalexeev")
    def test_copy_non_writable_src(self):
        # YT-4175
        create_user("u")
        set("//tmp/s", {"x": {"a": 1}})
        set("//tmp/s/@acl/end", make_ace("allow", "u", ["read", "write", "remove"]))
        set("//tmp/s/x/@acl", [make_ace("deny", "u", ["write", "remove"])])
        copy("//tmp/s/x", "//tmp/s/y", authenticated_user="u")
        assert get("//tmp/s/y", authenticated_user="u") == get("//tmp/s/x", authenticated_user="u")

    @authors("danilalexeev")
    def test_copy_and_move_require_read_on_source(self):
        create_user("u")
        set("//tmp/s", {"x": {}})
        set("//tmp/s/@acl/end", make_ace("allow", "u", ["read", "write", "remove"]))
        set("//tmp/s/x/@acl", [make_ace("deny", "u", "read")])
        with pytest.raises(YtError):
            copy("//tmp/s/x", "//tmp/s/y", authenticated_user="u")
        with pytest.raises(YtError):
            move("//tmp/s/x", "//tmp/s/y", authenticated_user="u")

    @authors("danilalexeev")
    def test_copy_and_move_require_write_on_target_parent(self):
        create_user("u")
        set("//tmp/s", {"x": {}})
        set("//tmp/s/@acl/end", make_ace("allow", "u", ["read", "remove"]))
        set("//tmp/s/@acl/end", make_ace("deny", "u", ["write"]))
        with pytest.raises(YtError):
            copy("//tmp/s/x", "//tmp/s/y", authenticated_user="u")
        with pytest.raises(YtError):
            move("//tmp/s/x", "//tmp/s/y", authenticated_user="u")

    @authors("danilalexeev")
    def test_copy_and_move_requires_remove_on_target_if_exists(self):
        create_user("u")
        set("//tmp/s", {"x": {}, "y": {}})
        set("//tmp/s/@acl/end", make_ace("allow", "u", ["read", "write", "remove"]))
        set("//tmp/s/y/@acl", [make_ace("deny", "u", "remove")])
        with pytest.raises(YtError):
            copy("//tmp/s/x", "//tmp/s/y", force=True, authenticated_user="u")
        with pytest.raises(YtError):
            move("//tmp/s/x", "//tmp/s/y", force=True, authenticated_user="u")

    @authors("danilalexeev")
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

    @authors("danilalexeev")
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

    @authors("danilalexeev")
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

    @authors("danilalexeev")
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

    @authors("babenko", "danilalexeev")
    def test_read_from_per_user_cache(self):
        create_user("u")
        set("//tmp/a", "b")
        set("//tmp/a/@acl/end", make_ace("deny", "u", "read"))
        with pytest.raises(YtError):
            get("//tmp/a", authenticated_user="u")
        with pytest.raises(YtError):
            get("//tmp/a", authenticated_user="u", read_from="cache")

    @authors("babenko", "danilalexeev")
    def test_read_from_global_cache(self):
        create_user("u")
        set("//tmp/a", "b")
        set("//tmp/a/@acl/end", make_ace("deny", "u", "read"))
        assert get("//tmp/a", read_from="cache", disable_per_user_cache=True) == "b"
        with pytest.raises(YtError):
            get("//tmp/a", authenticated_user="u")
        assert get("//tmp/a", authenticated_user="u", read_from="cache", disable_per_user_cache=True) == "b"

    @authors("danilalexeev")
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
        assert check_permission("u", "read", "//tmp/x/y")["action"] == "deny"
        assert "attr" not in ls("//tmp/x", attributes=["attr"], authenticated_user="u")[0].attributes

    @authors("danilalexeev")
    def test_get_with_denied_subtree(self):
        create_user("u")
        set("//tmp", {"foo": {"var": 1}, "baz": {"bar": 2}}, force=True)
        set("//tmp/foo/@inherit_acl", False)
        assert get("//tmp", authenticated_user="u") == {"foo": yson.YsonEntity(), "baz": {"bar": 2}}
        set("//tmp/baz/bar/@inherit_acl", False)
        tt = get("//tmp", attributes=["id"], authenticated_user="u")
        assert "id" not in tt["foo"].attributes
        assert "id" in tt["baz"].attributes
        assert "id" not in tt["baz"]["bar"].attributes

    @authors("danilalexeev")
    def test_get_with_inheritence_modes(self):
        create_user("u")
        set("//tmp", {"foo": {"bar": {"var": 2}}}, force=True)
        set("//tmp/foo/@inherit_acl", False)
        set("//tmp/foo/@acl/end", make_ace("allow", ["u"], "read", "descendants_only"))
        assert get("//tmp/foo/bar", authenticated_user="u") == {"var": 2}

        set("//tmp/foo/bar/@acl/end", make_ace("deny", ["u"], "read", "immediate_descendants_only"))
        assert get("//tmp/foo/bar", authenticated_user="u") == {"var": yson.YsonEntity()}

        set("//tmp/foo/@acl/end", make_ace("allow", ["u"], "read", "object_and_descendants"))
        assert get("//tmp/foo", authenticated_user="u") == {"bar": {"var": yson.YsonEntity()}}

    @authors("savrus")
    @not_implemented_in_sequoia
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

    @authors("danilalexeev")
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

    @authors("babenko", "danilalexeev")
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

    @authors("babenko", "danilalexeev")
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

    @authors("babenko", "danilalexeev")
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

    @authors("babenko", "danilalexeev")
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

    @authors("babenko", "danilalexeev")
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

    @authors("shakurov", "danilalexeev")
    @pytest.mark.parametrize("removal", ["abort", "unlock"])
    def test_orphaned_node(self, removal):
        if removal == "unlock":
            # Using unlock() on snapshot branch leads to
            # "Non-zero reference counter after object removal" alert.
            # TODO(kvk1920): investigate it.
            pytest.skip()

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

        assert check_permission("u", "read", "#" + table_id, tx=tx)["action"] == "allow"
        assert check_permission("u", "read", "#" + table_id)["action"] == "allow"

        if removal == "unlock":
            unlock(f"#{table_id}", tx=tx)
        else:
            abort_transaction(tx)
        gc_collect()

        if removal == "unlock":
            with raises_yt_error(f"No such object {table_id}"):
                check_permission("u", "read", f"#{table_id}", tx=tx)
        with raises_yt_error(f"No such object {table_id}"):
            check_permission("u", "read", f"#{table_id}")

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
    @not_implemented_in_sequoia
    def test_columnar_acl_copy_yt_12749(self):
        create_user("u1")
        create_user("u2")

        self._test_columnar_acl_copy_yt_12749("//tmp", "//tmp")

    @authors("shakurov", "danilalexeev")
    def test_special_acd_holders(self):
        create_user("u1")

        create_access_control_object_namespace("cats")
        create_access_control_object("garfield", "cats")
        set("//sys/access_control_object_namespaces/cats/garfield/@acl/end",
            make_ace("allow", "u1", "read"))

        remove("//sys/access_control_object_namespaces/cats/garfield")
        gc_collect()

        # Must not crash.
        build_snapshot(cell_id=get("//sys/@cell_id"), set_read_only=False)
        get("//sys/users/u1/@")

    @authors("shakurov", "danilalexeev")
    def test_create_access_control_objects_with_ignore_existing(self):
        create_access_control_object_namespace("cats", ignore_existing=True)
        create_access_control_object("garfield", "cats", ignore_existing=True)

        remove("//sys/access_control_object_namespaces/cats/garfield")
        remove("//sys/access_control_object_namespaces/cats")

    @authors("shakurov", "danilalexeev")
    def test_access_control_object_recursive_get(self):
        create_access_control_object_namespace("cats")
        create_access_control_object("garfield", "cats")

        assert "principal" in get("//sys/access_control_object_namespaces/cats/garfield")

    @authors("shakurov", "danilalexeev")
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
        set(path + "/@" + key, value, authenticated_user=user, force=force)

    def __test_multiset_attributes_request(self, path, key, value, user, force=False):
        multiset_attributes(path + "/@", {key: value}, authenticated_user=user, force=force)

    @authors("vovamelnikov")
    @not_implemented_in_sequoia
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
            },
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

        with raises_yt_error("It will be impossible for this user to revert this modification"):
            request_func(path=path, key=key, value=value, user='u', force=False)

        request_func(path=path, key=key, value=value, user='u', force=True)

    @authors("vovamelnikov")
    @not_implemented_in_sequoia
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
                "owner": "u1",
            },
        )

        request_func = {
            "set": self.__test_set_attributes_request,
            "multiset": self.__test_multiset_attributes_request,
        }[request_type]

        with raises_yt_error("It will be impossible for this user to revert this modification"):
            request_func(path="//tmp/dir", key="owner", value="u2", user="u2", force=False)

        request_func(path="//tmp/dir", key="owner", value="u2", user="u2", force=True)

    @authors("h0pless", "danilalexeev")
    def test_attribute_based_access_control(self):
        create_user("smoothie_lover")
        create_user("kvas_enjoyer")
        set("//sys/users/smoothie_lover/@tags", ["smoothie", "drinks", "smoothie"])

        tags = get("//sys/users/smoothie_lover/@tags")
        assert {i for i in tags} == {"smoothie", "drinks"}
        assert len(tags) == 2

        create(
            "map_node",
            "//tmp/drinks_discussion",
            attributes={
                "inherit_acl": False,
                "acl": [
                    make_ace("allow", "everyone", ["administer", "read", "write", "create"], subject_tag_filter="drinks"),
                ],
            },
        )

        create(
            "map_node",
            "//tmp/drinks_discussion/smoothie_topic",
            attributes={
                "acl": [
                    make_ace("deny", "everyone", ["administer", "read", "write", "create"], subject_tag_filter="!smoothie"),
                ],
            },
            authenticated_user="smoothie_lover",
        )

        # Missing tag "drinks".
        with raises_yt_error("Access denied for user"):
            create(
                "map_node",
                "//tmp/drinks_discussion/carbonated_drinks_topic",
                authenticated_user="kvas_enjoyer",
            )
        set("//sys/users/kvas_enjoyer/@tags", ["drinks"])

        # Even though user has no "kvas" or "beer" tag, they still can give access to it.
        create(
            "map_node",
            "//tmp/drinks_discussion/carbonated_drinks_topic",
            attributes={
                "acl": [
                    make_ace("allow", "everyone", ["administer", "read", "write", "create"], subject_tag_filter="beer|kvas"),
                ],
            },
            authenticated_user="kvas_enjoyer",
        )

        # Because user has "drinks" role inherited from above user has access to the node.
        create(
            "map_node",
            "//tmp/drinks_discussion/carbonated_drinks_topic/top_10_kvas",
            authenticated_user="kvas_enjoyer",
        )
        set("//tmp/drinks_discussion/carbonated_drinks_topic/@inherit_acl", False)

        # With inherit_acl set to false user loses the ability to create anything there without "kvas" or "beer" roles.
        with raises_yt_error("Access denied for user"):
            create(
                "map_node",
                "//tmp/drinks_discussion/carbonated_drinks_topic/best_kvas_in_my_oblast",
                authenticated_user="kvas_enjoyer",
            )
        set("//sys/users/kvas_enjoyer/@tags/end", "kvas")

        # And now role tags are satisfied!
        create(
            "map_node",
            "//tmp/drinks_discussion/carbonated_drinks_topic/best_kvas_in_my_oblast",
            authenticated_user="kvas_enjoyer",
        )

        # And finally check that filtering by "!" works.
        create(
            "map_node",
            "//tmp/drinks_discussion/smoothie_topic/my_favorite_smoothie_place",
            authenticated_user="smoothie_lover",
        )
        with raises_yt_error("Access denied for user"):
            create(
                "map_node",
                "//tmp/drinks_discussion/smoothie_topic/top_10_kvas",
                authenticated_user="kvas_enjoyer",
            )

    @authors("danilalexeev")
    def test_user_tags_limits(self):
        create_user("u")

        set("//sys/@config/security_manager/max_user_tag_size", 500)
        set("//sys/@config/security_manager/max_user_tag_count", 4)

        with raises_yt_error("user tag size limit exceeded"):
            set("//sys/users/u/@tags", ["tag_big" * 100])

        with raises_yt_error("user tags count limit exceeded"):
            set("//sys/users/u/@tags", [f"tag_{i}" for i in range(10)])

    @authors("danilalexeev")
    def test_subject_tag_filters_limits(self):
        set("//sys/@config/security_manager/max_subject_tag_filter_size", 40)

        create(
            "map_node",
            "//tmp/dir",
            attributes={
                "acl": [
                    make_ace("deny", "everyone", ["administer", "read", "write", "create"], subject_tag_filter="baz"),
                ],
            },
        )

        with raises_yt_error("tag filter size limit exceeded"):
            set("//tmp/dir/@acl/0/subject_tag_filter", "&".join([f"tag_{i}" for i in range(10)]))

    @authors("h0pless")
    @not_implemented_in_sequoia
    def test_disable_subject_tag_filters(self):
        create_user("George50")
        set("//sys/users/George50/@tags", ['cool', 'lonely'])

        create(
            "map_node",
            "//tmp/cool_dir",
            attributes={
                "acl": [
                    make_ace("allow", "everyone", ["administer", "read", "write", "create"], subject_tag_filter="cool"),
                    make_ace("deny", "everyone", ["administer", "read", "write", "create"], subject_tag_filter="!lonely"),
                ],
            },
        )
        create(
            "map_node",
            "//tmp/uncool_dir",
            attributes={
                "acl": [
                    make_ace("allow", "everyone", ["administer", "read", "write", "create"], subject_tag_filter="!cool"),
                ],
            },
        )
        set("//tmp/cool_dir/@inherit_acl", False)
        set("//tmp/uncool_dir/@inherit_acl", False)

        # Subject tag filter is active.
        create(
            "table",
            "//tmp/cool_dir/cool_table",
            authenticated_user="George50",
        )
        with raises_yt_error("Access denied for user"):
            create(
                "table",
                "//tmp/uncool_dir/cool_table",
                authenticated_user="George50",
            )

        set("//sys/@config/security_manager/enable_subject_tag_filters", False)
        create(
            "table",
            "//tmp/cool_dir/anohter_cool_table",
            authenticated_user="George50",
        )
        create(
            "table",
            "//tmp/uncool_dir/another_cool_table",
            authenticated_user="George50",
        )

    @authors("kivedernikov")
    @not_implemented_in_sequoia
    def test_permissions_concatenate(self):
        create_user("u1")
        create_group("g1")
        add_member("u1", "g1")

        create(
            "table",
            "//tmp/t",
            attributes={
                "schema": [
                    {"name": "a", "type": "string"},
                    {"name": "b", "type": "string"},
                    {"name": "c", "type": "string"},
                ]
            },
            recursive=True,
        )

        create(
            "table",
            "//tmp/t2",
            attributes={
                "schema": [
                    {"name": "a", "type": "string"},
                    {"name": "b", "type": "string"},
                    {"name": "c", "type": "string"},
                ]
            },
            recursive=True,
        )

        concatenate(["//tmp/t"], "//tmp/t2", authenticated_user="u1")

        set(
            "//tmp/t/@acl",
            [
                make_ace("allow", "u1", "read", columns=["a", "b"]),
                make_ace("deny", "u1", "read", columns=["c"]),
            ],
        )

        response1 = check_permission("u1", "read", "//tmp/t", columns=["a", "b", "c"])
        assert response1["columns"][0]["action"] == "allow"
        assert response1["columns"][1]["action"] == "allow"
        assert response1["columns"][2]["action"] == "deny"

        with pytest.raises(YtError):
            concatenate(["//tmp/t"], "//tmp/t2", authenticated_user="u1")

        set(
            "//tmp/t/@acl",
            [
                make_ace("allow", "u1", ["read", "write", "remove"]),
            ],
        )

        concatenate(["//tmp/t"], "//tmp/t2", authenticated_user="u1")

    @authors("coteeq")
    @not_implemented_in_sequoia
    def test_full_read_validation(self):
        create_user("u")
        create("table", "//tmp/t")

        with raises_yt_error('ACE with "full_read" permission may have only "allow" action'):
            set("//tmp/t/@acl", [make_ace("deny", "u", "full_read")])

        with raises_yt_error('ACE with "full_read" permission may have only "allow" action'):
            set("//tmp/t/@acl", [make_ace("deny", "u", ["read", "full_read"])])

        with raises_yt_error('ACE specifying columns may contain only "read" permission'):
            set("//tmp/t/@acl", [make_ace("deny", "u", "full_read", columns=["a"])])

    @authors("coteeq")
    @not_implemented_in_sequoia
    def test_full_read_simple(self):
        create_user("u")
        create_user("restricted")

        create(
            "table",
            "//tmp/t",
            attributes={
                "schema": [
                    {"name": "public", "type": "string"},
                    {"name": "private", "type": "int64"},
                ],
                "acl": [
                    make_ace("allow", "u", "read", columns=["private"]),
                ],
                "inherit_acl": False,
            }
        )

        with raises_yt_error('Access denied for user "restricted"'):
            copy("//tmp/t", "//tmp/t_copy", authenticated_user="restricted")

        set("//tmp/t/@acl/end", make_ace("allow", "restricted", "full_read"))

        # full_read should grant read.
        get("//tmp/t/@acl", authenticated_user="restricted")
        read_table("//tmp/t", authenticated_user="restricted")

        # full_read should also grant full_read.
        copy("//tmp/t", "//tmp/t_copy", authenticated_user="restricted")

    @authors("coteeq")
    @not_implemented_in_sequoia
    def test_full_read_and_deny_read(self):
        create_user("u")

        create(
            "table",
            "//tmp/t",
            attributes={
                "schema": [
                    {"name": "public", "type": "string"},
                    {"name": "private", "type": "int64"},
                ],
                "acl": [
                    make_ace("allow", "u", "full_read"),
                    make_ace("deny", "u", "read"),
                ],
                "inherit_acl": False,
            }
        )

        # full_read must not override deny on read.
        with raises_yt_error('Access denied for user "u"'):
            get("//tmp/t/@acl", authenticated_user="u")
        with raises_yt_error('Access denied for user "u"'):
            copy("//tmp/t", "//tmp/t_copy", authenticated_user="u")

    @authors("coteeq")
    @not_implemented_in_sequoia
    def test_absence_of_read_does_not_mention_full_read(self):
        # NB(coteeq): This is a test for the behaviour that is designed to
        # decrease entropy with the access control rules.
        # When the user tries to full_read a table, on which they do not even
        # have a basic read, we should ask them to get basic read first and
        # hope that basic read will be enough.
        #
        # Otherwise, if we mention full_read in the error, the user will
        # immediately rush to get full_read, which they probably do not need.
        create_user("u")

        create(
            "table",
            "//tmp/t",
            attributes={
                "inherit_acl": False,
            }
        )

        with raises_yt_error('"read" permission for node //tmp/t'):
            copy("//tmp/t", "//tmp/t_copy", authenticated_user="u")

    @authors("coteeq")
    @not_implemented_in_sequoia
    def test_alter_requires_full_read(self):
        create_user("u")
        create_user("u_with_partial_read")
        create_user("u_with_explicit_full_read")

        create(
            "table",
            "//tmp/t",
            attributes={
                "inherit_acl": False,
                "schema": [
                    {"name": "public", "type": "string"},
                    {"name": "private", "type": "int64"},
                ],
            }
        )

        new_schema = [
            {"name": "public", "type": "string"},
            {"name": "not_private_anymore", "stable_name": "private", "type": "int64"},
        ]

        set("//tmp/t/@acl", [
            make_ace("allow", "u", "write"),
        ])

        # Missing any read.
        with raises_yt_error('"read" permission for node //tmp/t'):
            alter_table("//tmp/t", schema=new_schema, authenticated_user="u")

        set("//tmp/t/@acl", [
            make_ace("allow", "u", ["read", "write"]),
            make_ace("allow", "u_with_partial_read", "read"),
            make_ace("allow", "u_with_partial_read", "read", columns=["private"]),
        ])

        # Can't read column 'private'.
        with raises_yt_error('"full_read" permission for node //tmp/t'):
            alter_table("//tmp/t", schema=new_schema, authenticated_user="u")

        set("//tmp/t/@acl", [
            make_ace("allow", "u", ["read", "write"]),
            make_rl_ace("u", row_access_predicate='public = ""'),
        ])

        # Has non-trivial RL ACE.
        with raises_yt_error('"read" permission for node //tmp/t'):
            alter_table("//tmp/t", schema=new_schema, authenticated_user="u")

        set("//tmp/t/@acl", [
            make_ace("allow", "u", ["read", "write"]),
            make_ace("allow", "u_with_partial_read", "read"),
            make_ace("allow", ["u", "u_with_partial_read"], "read", columns=["private"]),
        ])

        # Finally has full_read.
        alter_table("//tmp/t", schema=new_schema, authenticated_user="u")

        new_schema_2 = [
            {"name": "public", "type": "string"},
            {"name": "not_private_anymore_2", "stable_name": "private", "type": "int64"},
        ]

        set("//tmp/t/@acl", [
            make_ace("allow", "u", ["read", "write"]),
            make_ace("allow", "u_with_explicit_full_read", ["full_read", "write"]),
            make_ace("allow", "u_with_partial_read", ["read", "write"]),
            make_ace("allow", "u_with_partial_read", "read", columns=["not_private_anymore"]),
        ])

        # Sanity check that columnar ACL is working as expected.
        with raises_yt_error('"full_read" permission for node //tmp/t'):
            alter_table("//tmp/t", schema=new_schema_2, authenticated_user="u")

        # Explicit full_read.
        alter_table("//tmp/t", schema=new_schema_2, authenticated_user="u_with_explicit_full_read")


@pytest.mark.enabled_multidaemon
class TestRowAcls(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SECONDARY_MASTER_CELLS = 1

    MASTER_CELL_DESCRIPTORS = {
        "11": {"roles": ["chunk_host"]},
    }

    def _read(self, user, path="//tmp/t", omit_inaccessible_rows=True, **kwargs):
        return read_table(
            path,
            authenticated_user=user,
            omit_inaccessible_rows=omit_inaccessible_rows,
            **kwargs,
        )

    def _rows(self, *int_seq):
        return [
            {"col1": i, "col2": f"val_{i}"}
            for i in int_seq
        ]

    def _create_and_write_table(self, acl, optimize_for="scan", schema=None, sorted=False):
        sort_order = {"sort_order": "ascending"} if sorted else {}
        create(
            "table",
            "//tmp/t",
            attributes={
                "inherit_acl": False,
                "acl": acl,
                "schema": schema or [
                    {"name": "col1", "type": "int64", **sort_order},
                    {"name": "col2", "type": "string"},
                ],
                "optimize_for": optimize_for,
            },
        )
        write_table("//tmp/t", self._rows(*range(2, 10)))

    @authors("coteeq")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_row_ace(self, optimize_for):
        create_user("prime_manager")
        create_user("even_manager")
        create_user("everything_manager")
        create_user("everything_manager_via_row_access_predicate")
        create_user("only_generic_read")
        create_user("no_read")

        acl = [
            make_rl_ace("prime_manager", "col1 in (2, 3, 5, 7)"),
            make_rl_ace("even_manager", "col1 % 2 = 0"),
            make_ace("allow", "everything_manager", "full_read"),
            make_rl_ace("everything_manager_via_row_access_predicate", "true"),
            make_rl_ace(["prime_manager", "even_manager", "only_generic_read", "everything_manager_via_row_access_predicate"]),
        ]

        self._create_and_write_table(acl, optimize_for)

        for user in ["prime_manager", "even_manager", "everything_manager", "everything_manager_via_row_access_predicate", "only_generic_read"]:
            get("//tmp/t/@optimize_for", authenticated_user=user)

        with raises_yt_error("Access denied for user"):
            get("//tmp/t/@optimize_for", authenticated_user="no_read")

        for user in ["prime_manager", "even_manager", "only_generic_read", "no_read", "everything_manager_via_row_access_predicate"]:
            with raises_yt_error("Access denied for user"):
                copy("//tmp/t", f"//tmp/t_copy_{user}", authenticated_user=user)
            with raises_yt_error("Access denied for user"):
                concatenate(["//tmp/t"], f"//tmp/t_concat_{user}", authenticated_user=user)

        copy("//tmp/t", "//tmp/t2_copy", authenticated_user="everything_manager")
        concatenate(["//tmp/t"], "//tmp/t2_copy", authenticated_user="everything_manager")

        assert self._read("prime_manager") == self._rows(2, 3, 5, 7)
        assert self._read("even_manager") == self._rows(2, 4, 6, 8)
        assert self._read("everything_manager") == self._rows(*range(2, 10))
        assert self._read("everything_manager_via_row_access_predicate") == self._rows(*range(2, 10))
        assert self._read("only_generic_read") == []

        # Just check for sanity.
        with raises_yt_error():
            self._read("no_read")

    @authors("coteeq")
    def test_has_row_level_ace_attribute(self):
        create_user("u")

        def _create(path, acl=[], inherit_acl=False):
            create(
                "table",
                path,
                attributes={
                    "inherit_acl": inherit_acl,
                    "acl": acl,
                    "schema": [
                        {"name": "key", "type": "int64"},
                        {"name": "value", "type": "string"},
                    ],
                    "external_cell_tag": 11,
                },
            )

        _create(
            "//tmp/with_rls",
            acl=[
                make_rl_ace("u", "key in (2, 3, 5, 7)"),
                make_ace("allow", "u", "read"),
            ],
        )
        _create(
            "//tmp/without_rls",
            acl=[
                make_ace("allow", "u", "read"),
            ],
        )
        create("map_node", "//tmp/dir")
        set(
            "//tmp/dir/@acl",
            [
                make_rl_ace("u", "key in (2, 3, 5, 7)"),
                make_ace("allow", "u", "read"),
            ],
        )
        _create(
            "//tmp/dir/inherited_rls",
            inherit_acl=True,
        )

        # Regular get.
        assert get("//tmp/with_rls/@has_row_level_ace")
        assert get("//tmp/with_rls/@has_row_level_ace", authenticated_user="u")
        assert not get("//tmp/without_rls/@has_row_level_ace")
        assert get("//tmp/dir/inherited_rls/@has_row_level_ace")

        # Assert opaqueness.
        assert get("//tmp/with_rls/@")["has_row_level_ace"] == yson.YsonEntity()

        # Get by id.
        def get_by_id(path, **kwargs):
            id = get(f"{path}/@id")
            return get(f"#{id}/@has_row_level_ace", **kwargs)

        assert get_by_id("//tmp/with_rls")
        assert get_by_id("//tmp/with_rls", authenticated_user="u")
        assert not get_by_id("//tmp/without_rls")
        assert get_by_id("//tmp/dir/inherited_rls")

        # Get directly to the external cell.
        id = get("//tmp/with_rls/@id")
        external_cell_tag = get("//tmp/with_rls/@external_cell_tag")
        driver = get_driver(external_cell_tag - 10)
        with raises_yt_error("Attribute \"has_row_level_ace\" is not found"):
            assert get("#" + id + "/@has_row_level_ace", driver=driver)

    @authors("coteeq")
    def test_row_ace_validation(self):
        create_user("u")

        def make_bad_ace(action, permissions, row_access_predicate, inapplicable_row_access_predicate_mode=None, columns=None):
            ace = make_ace(action, "u", permissions, columns=columns)
            if row_access_predicate is not None:
                ace["row_access_predicate"] = row_access_predicate
            if inapplicable_row_access_predicate_mode is not None:
                ace["inapplicable_row_access_predicate_mode"] = inapplicable_row_access_predicate_mode
            return ace

        bad_aces = []
        bad_aces.extend([
            (make_bad_ace("deny", "read", "a = 2"), 'ACE specifying "row_access_predicate" may have only "allow" action'),
        ])
        bad_aces.extend([
            (make_bad_ace("allow", permission, "a = 2"), 'ACE specifying "row_access_predicate" may contain only "read" permission')
            for permission in ["write", "use", "administer", "create", "remove", "mount", "manage", "modify_children"]
        ] + [
            (make_bad_ace("allow", ["write", "modify_children"], "a = 2"), 'ACE specifying "row_access_predicate" may contain only "read" permission'),
        ])
        bad_aces.extend([
            (make_bad_ace("allow", "read", None, "ignore"), '"inapplicable_row_access_predicate_mode" can only be specified if "row_access_predicate" is specified'),
        ])
        bad_aces.extend([
            (make_bad_ace("allow", "read", "a = 2", columns=["col"]), 'Single ACE must not contain both "columns" and "row_access_predicate"'),
        ])

        bad_aces.extend([
            (make_bad_ace("allow", ["read", "full_read"], "a = 2"), 'ACE with "full_read" permission may not specify "row_access_predicate" or "columns"'),
            (make_bad_ace("allow", ["full_read"], "a = 2"), 'ACE with "full_read" permission may not specify "row_access_predicate" or "columns"'),
        ])

        for bad_ace, expected_error in bad_aces:
            with raises_yt_error(expected_error):
                create("table", "//tmp/t", attributes={"acl": [bad_ace]})

    def _prepare_check_permission(self, user, permission="read"):
        create_user("u0")
        create_user("u1")
        create_user("u2")
        create_user("u3")
        create_user("writer")

        acl = [
            make_rl_ace("u0", "a < 3"),
            make_rl_ace(["u0", "u1"], "b == 2"),
            make_rl_ace(["u0", "u1"], "c == \"asdf\"", "ignore"),
            make_ace("allow", ["u2"], "full_read"),
            make_rl_ace(["u3"]),
            make_ace("allow", "writer", "write"),
        ]

        create("table", "//tmp/t", attributes={"acl": acl})
        return check_permission(user, permission, "//tmp/t")

    @authors("coteeq")
    def test_check_permission_u0(self):
        response = self._prepare_check_permission("u0")
        response["row_level_acl"].sort(key=lambda descriptor: descriptor.get("row_access_predicate", ""))

        assert response["row_level_acl"][0]["row_access_predicate"] == "a < 3"
        assert response["row_level_acl"][1]["row_access_predicate"] == "b == 2"
        assert response["row_level_acl"][2]["row_access_predicate"] == "c == \"asdf\""

        assert "inapplicable_row_access_predicate_mode" not in response["row_level_acl"][0]
        assert "inapplicable_row_access_predicate_mode" not in response["row_level_acl"][1]
        assert response["row_level_acl"][2]["inapplicable_row_access_predicate_mode"] == "ignore"

    @authors("coteeq")
    def test_check_permission_u1(self):
        response = self._prepare_check_permission("u1")
        response["row_level_acl"].sort(key=lambda descriptor: descriptor["row_access_predicate"])

        assert response["row_level_acl"][0]["row_access_predicate"] == "b == 2"
        assert response["row_level_acl"][1]["row_access_predicate"] == "c == \"asdf\""

        assert "inapplicable_row_access_predicate_mode" not in response["row_level_acl"][0]
        assert response["row_level_acl"][1]["inapplicable_row_access_predicate_mode"] == "ignore"

    @authors("coteeq")
    def test_check_permission_u2(self):
        response = self._prepare_check_permission("u2")
        assert "row_level_acl" not in response

    @authors("coteeq")
    def test_check_permission_u3(self):
        response = self._prepare_check_permission("u3")
        assert response["row_level_acl"] == []

    @authors("coteeq")
    def test_check_permission_writer(self):
        response = self._prepare_check_permission("writer", permission="write")
        assert "row_level_acl" not in response

    @authors("coteeq")
    @pytest.mark.parametrize("mode", ["ignore", "fail"])
    @pytest.mark.parametrize("invalid_reason", ["non_existent_column", "column_type_invalid", "not_boolean", "syntax"])
    def test_invalid_row_access_predicate(self, mode, invalid_reason):
        create_user("u")

        row_access_predicate = None
        expected_error = None
        if invalid_reason == "non_existent_column":
            row_access_predicate = "non_existent = 2"
            expected_error = "Undefined reference"
        elif invalid_reason == "column_type_invalid":
            row_access_predicate = "col1 = \"str\""
            expected_error = "Type mismatch in expression"
        elif invalid_reason == "not_boolean":
            row_access_predicate = "col1 + 1"
            expected_error = "result type to be boolean"
        else:
            row_access_predicate = ")col1 == 2("
            expected_error = "syntax error"

        self._create_and_write_table(
            [
                make_rl_ace("u"),
                make_rl_ace("u", row_access_predicate, mode=mode),
            ],
        )

        if mode == "fail":
            try:
                self._read("u")
                assert False, "Did not raise exception"
            except Exception as e:
                assert_yt_error(e, expected_error)
                assert_yt_error(e, "and ACE has inapplicable_row_access_predicate_mode=fail")
        else:
            assert self._read("u") == []

    @authors("coteeq")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_adjust_columns(self, optimize_for):
        create_user("u")

        self._create_and_write_table(
            [
                make_rl_ace("u"),
                make_rl_ace("u", "col1 < 5"),
            ],
            optimize_for,
        )

        assert self._read("u", path="//tmp/t{col2}") == [{"col2": row["col2"]} for row in self._rows(2, 3, 4)]

    @authors("coteeq")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_missing_in_chunk_column(self, optimize_for):
        create_user("u")

        self._create_and_write_table(
            [],
            optimize_for,
        )

        new_schema = [
            {"name": "col1", "type": "int64"},
            {"name": "col2", "type": "string"},
            {"name": "new_col", "type": "string"},
        ]

        alter_table("//tmp/t", schema=new_schema)

        set("//tmp/t/@acl/end", make_rl_ace("u"))
        set("//tmp/t/@acl/end", make_rl_ace("u", "is_null(new_col) and col1 < 5"))

        assert self._read("u") == self._rows(2, 3, 4)

    @authors("coteeq")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_rename_columns(self, optimize_for):
        create_user("u")

        self._create_and_write_table(
            [
                make_rl_ace("u"),
                make_rl_ace("u", "col1 < 5"),
            ],
            optimize_for,
        )

        new_schema_1 = [
            {"name": "forget_me", "type": "int64", "stable_name": "col1"},
            {"name": "col2", "type": "string"},
            {"name": "col1", "type": "string", "stable_name": "new_col"},
        ]

        alter_table("//tmp/t", schema=new_schema_1)

        # col1 is now string and row_access_predicate is not applicable.
        with raises_yt_error():
            assert self._read("u")

        new_schema_2 = [
            {"name": "forget_me", "type": "int64", "stable_name": "col1"},
            {"name": "col2", "type": "string"},
            {"name": "new_col", "type": "string", "stable_name": "new_col"},
            {"name": "col1", "type": "int64", "stable_name": "even_newer_col"},
        ]
        alter_table("//tmp/t", schema=new_schema_2)

        write_table("<append=%true>//tmp/t", {"col1": 12, "col2": "12"})

        # col1 is now null for the first chunk and `null < 5` is always true.
        assert self._read("u") == [
            {"forget_me": stable["col1"], "col2": stable["col2"]}
            for stable in self._rows(*range(2, 10))
        ]

    @authors("coteeq")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    @pytest.mark.xfail(reason="This will be kinda hard :(")
    def test_read_ranges_index(self, optimize_for):
        create_user("u")

        self._create_and_write_table(
            [
                make_rl_ace("u"),
                make_rl_ace("u", "col1 % 2 = 0"),
            ],
            optimize_for,
        )

        assert self._read("u", "//tmp/t[#2]") == self._rows(6)

    @authors("coteeq")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_read_ranges_keys(self, optimize_for):
        create_user("u")

        self._create_and_write_table(
            [
                make_rl_ace("u"),
                make_rl_ace("u", "col1 % 2 = 0"),
            ],
            optimize_for,
            schema=[
                {"name": "col1", "type": "int64", "sort_order": "ascending"},
                {"name": "col2", "type": "string"},
            ]
        )

        assert self._read("u", "//tmp/t[4]") == self._rows(4)
        assert self._read("u", "//tmp/t[5]") == []
        assert self._read("u", "//tmp/t[4:8]") == self._rows(4, 6)

    @authors("coteeq")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_multiple_aces(self, optimize_for):
        create_user("u")

        self._create_and_write_table(
            [
                make_rl_ace("u"),
                make_rl_ace("u", "col1 = 4"),
                make_rl_ace("u", "col1 = 5"),
                make_rl_ace("u", "col1 = 6"),
            ],
            optimize_for,
        )

        assert self._read("u") == self._rows(4, 5, 6)

    @authors("coteeq")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    @pytest.mark.parametrize("use_columns", [False, True], ids=["use_columns", "no_use_columns"])
    def test_extra_columns(self, optimize_for, use_columns):
        create_user("u")

        self._create_and_write_table(
            [
                make_rl_ace("u"),
                make_rl_ace("u", "col1 = 4"),
                make_rl_ace("u", "col1 = 5"),
            ],
            optimize_for,
        )

        path = "//tmp/t{col1,col2}" if use_columns else "//tmp/t"
        actual = self._read(
            "u",
            path=path,
            control_attributes={
                "enable_row_index": True,
            },
        )
        # Drop control attributes.
        actual = [row for row in actual if not isinstance(row, yson.yson_types.YsonEntity)]
        assert actual == self._rows(4, 5)

    @authors("coteeq")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_rls_does_not_affect_writes(self, optimize_for):
        create_user("u")
        create_user("writer")

        self._create_and_write_table(
            [
                make_rl_ace(["u", "writer"]),
                make_rl_ace("u", "col1 = 4"),
                make_rl_ace("u", "col1 = 5"),
                make_ace("allow", "writer", "write"),
            ],
            optimize_for,
        )

        with raises_yt_error("\"write\" permission for node"):
            write_table("<append=%true>//tmp/t", self._rows(10, 11), authenticated_user="u")

        write_table("<append=%true>//tmp/t", self._rows(10, 11), authenticated_user="writer")
        # Check that 'writer' has row-level acl in effect.
        assert self._read("writer") == []

    @authors("coteeq")
    def test_row_count_attribute(self):
        create_user("u")
        create_user("data_owner")

        self._create_and_write_table(
            [
                make_rl_ace("u"),
                make_rl_ace("u", "col1 = 4"),
                make_rl_ace("u", "col1 = 5"),
                make_ace("allow", "data_owner", "full_read"),
            ],
            "lookup",
        )

        with raises_yt_error("Attribute \"row_count\" is not found"):
            get("//tmp/t/@row_count", authenticated_user="u")

        # Even owner cannot see @row_count.
        with raises_yt_error("Attribute \"row_count\" is not found"):
            get("//tmp/t/@row_count", authenticated_user="data_owner")

        # Request as root.
        get("//tmp/t/@row_count")


##################################################################


@pytest.mark.enabled_multidaemon
class TestCypressAclsMulticell(TestCypressAcls):
    ENABLE_MULTIDAEMON = True
    NUM_TEST_PARTITIONS = 3
    NUM_SECONDARY_MASTER_CELLS = 2

    MASTER_CELL_DESCRIPTORS = {
        "11": {"roles": ["chunk_host"]},
        "12": {"roles": ["chunk_host"]},
    }


@pytest.mark.enabled_multidaemon
class TestCheckPermissionRpcProxy(CheckPermissionBase):
    ENABLE_MULTIDAEMON = True
    DRIVER_BACKEND = "rpc"
    ENABLE_HTTP_PROXY = True
    ENABLE_RPC_PROXY = True


@pytest.mark.enabled_multidaemon
class TestCypressAclsPortal(TestCypressAclsMulticell):
    ENABLE_MULTIDAEMON = True
    NUM_TEST_PARTITIONS = 3
    NUM_SECONDARY_MASTER_CELLS = 3
    ENABLE_TMP_PORTAL = True

    MASTER_CELL_DESCRIPTORS = {
        "11": {"roles": ["cypress_node_host"]},
        "12": {"roles": ["chunk_host", "cypress_node_host"]},
        "13": {"roles": ["chunk_host"]},
    }

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
        build_snapshot(cell_id=get("//sys/@cell_id"), set_read_only=False)
        get("//sys/users/u1/@")


################################################################################


@pytest.mark.enabled_multidaemon
class TestCypressAclsSequoia(TestCypressAclsMulticell):
    ENABLE_MULTIDAEMON = True
    USE_SEQUOIA = True
    ENABLE_CYPRESS_TRANSACTIONS_IN_SEQUOIA = True
    ENABLE_TMP_ROOTSTOCK = True
    NUM_TEST_PARTITIONS = 3
    NUM_SECONDARY_MASTER_CELLS = 3
    MASTER_CELL_DESCRIPTORS = {
        "10": {"roles": ["sequoia_node_host"]},
        # Master cell with tag 11 is reserved for portals.
        "11": {"roles": ["chunk_host", "cypress_node_host"]},
        "12": {"roles": ["sequoia_node_host"]},
        "13": {"roles": ["chunk_host"]},
    }
