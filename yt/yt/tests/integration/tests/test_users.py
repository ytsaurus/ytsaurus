from yt_env_setup import YTEnvSetup

from yt_commands import (  # noqa
    authors, print_debug, wait, wait_assert, wait_breakpoint, release_breakpoint, with_breakpoint,
    events_on_fs, reset_events_on_fs,
    create, ls, get, set, copy, move, remove, link, exists, concatenate,
    create_account, create_network_project, create_tmpdir, create_user, create_group,
    create_pool, create_pool_tree, remove_pool_tree,
    create_data_center, create_rack, create_table,
    create_tablet_cell_bundle, remove_tablet_cell_bundle, create_table_replica,
    make_ace, check_permission, add_member, remove_member, remove_group, remove_user,
    remove_network_project,
    make_batch_request, execute_batch, get_batch_error,
    start_transaction, abort_transaction, commit_transaction, lock,
    insert_rows, select_rows, lookup_rows, delete_rows, trim_rows, alter_table,
    read_file, write_file, read_table, write_table, write_local_file, read_blob_table,
    map, reduce, map_reduce, join_reduce, merge, vanilla, sort, erase, remote_copy,
    run_test_vanilla, run_sleeping_vanilla,
    abort_job, list_jobs, get_job, abandon_job, interrupt_job,
    get_job_fail_context, get_job_input, get_job_stderr, get_job_spec,
    dump_job_context, poll_job_shell,
    abort_op, complete_op, suspend_op, resume_op,
    get_operation, list_operations, clean_operations,
    get_operation_cypress_path, scheduler_orchid_pool_path,
    scheduler_orchid_default_pool_tree_path, scheduler_orchid_operation_path,
    scheduler_orchid_default_pool_tree_config_path, scheduler_orchid_path,
    scheduler_orchid_node_path, scheduler_orchid_pool_tree_config_path, scheduler_orchid_pool_tree_path,
    mount_table, unmount_table, freeze_table, unfreeze_table, reshard_table,
    wait_for_tablet_state,
    sync_create_cells, sync_mount_table, sync_unmount_table,
    sync_freeze_table, sync_unfreeze_table, sync_reshard_table,
    sync_flush_table, sync_compact_table, sync_remove_tablet_cells,
    sync_reshard_table_automatic, sync_balance_tablet_cells,
    get_first_chunk_id, get_singular_chunk_id, get_chunk_replication_factor, multicell_sleep,
    update_nodes_dynamic_config, update_controller_agent_config,
    update_op_parameters, enable_op_detailed_logs,
    set_node_banned, set_banned_flag, set_account_disk_space_limit,
    check_all_stderrs,
    create_test_tables, create_dynamic_table, PrepareTables,
    get_statistics, get_recursive_disk_space, get_chunk_owner_disk_space,
    make_random_string, raises_yt_error,
    build_snapshot, is_multicell,
    get_driver, Driver, execute_command)

from yt.environment.helpers import assert_items_equal
from yt.common import YtError

import pytest

import __builtin__


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

    @authors("babenko", "ignat")
    def test_user_ban2(self):
        with pytest.raises(YtError):
            set("//sys/users/root/@banned", True)

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

        set("//sys/users/u/@request_limits/read_request_rate/per_cell", {"0": 1338})
        assert get("//sys/users/u/@request_limits/read_request_rate/per_cell/0") == 1338

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
        assert sorted(get("//sys/users/u/@usable_tablet_cell_bundles")) == ['default', "tcb1"]
        create_group("g")
        add_member("u", "g")
        set("//sys/tablet_cell_bundles/tcb2/@acl", [make_ace("allow", "g", "use")])
        assert sorted(get("//sys/users/u/@usable_tablet_cell_bundles")) == ['default', "tcb1", "tcb2"]

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

        wait(lambda: __builtin__.set(["g1", "g2"]) <= __builtin__.set(get("//sys/users/u/@member_of_closure")))

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
    def test_distributed_throttler(self):
        create_user("u")

        set("//sys/@config/security_manager/enable_distributed_throttler", True)
        get("//tmp", authenticated_user="u")

        set("//sys/@config/security_manager/enable_distributed_throttler", False)
        get("//tmp", authenticated_user="u")


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
        set("//sys/@config/multicell_manager/cell_descriptors/0", {"name": "Julia"})
        set("//sys/users/u/@request_limits/read_request_rate/per_cell", {"Julia": 100, "1": 200})
        assert get("//sys/users/u/@request_limits/read_request_rate/per_cell") == {"Julia": 100, "1": 200}

        set("//sys/@config/multicell_manager/cell_descriptors/1", {"name": "George"})
        assert get("//sys/users/u/@request_limits/read_request_rate/per_cell") == {"Julia": 100, "George": 200}
