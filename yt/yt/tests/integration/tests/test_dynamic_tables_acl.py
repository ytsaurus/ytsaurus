from test_sorted_dynamic_tables import TestSortedDynamicTablesBase
from test_ordered_dynamic_tables import TestOrderedDynamicTablesBase

from yt_commands import (  # noqa
    authors, print_debug, wait, wait_assert, wait_breakpoint, release_breakpoint, with_breakpoint,
    events_on_fs, reset_events_on_fs,
    create, ls, get, set, copy, move, remove, link, exists, concatenate,
    create_account, create_network_project, create_tmpdir, create_user, create_group,
    create_pool, create_pool_tree, remove_pool_tree,
    create_data_center, create_rack, create_table,
    make_ace, check_permission, add_member,
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
    mount_table, wait_for_tablet_state,
    sync_create_cells, sync_mount_table, sync_unmount_table,
    sync_freeze_table, sync_unfreeze_table, sync_reshard_table,
    sync_flush_table, sync_compact_table,
    get_first_chunk_id, get_singular_chunk_id, get_chunk_replication_factor, multicell_sleep,
    update_nodes_dynamic_config, update_controller_agent_config,
    update_op_parameters, enable_op_detailed_logs,
    set_node_banned, set_banned_flag, set_account_disk_space_limit,
    check_all_stderrs,
    create_test_tables, create_dynamic_table, PrepareTables,
    get_statistics, get_recursive_disk_space, get_chunk_owner_disk_space,
    make_random_string, raises_yt_error,
    build_snapshot,
    get_driver, Driver, execute_command)

from yt.environment.helpers import assert_items_equal
from yt.common import YtError

import pytest

################################################################################


class TestSortedDynamicTablesAcl(TestSortedDynamicTablesBase):
    USE_PERMISSION_CACHE = False

    SIMPLE_SCHEMA = [
        {"name": "key", "type": "int64", "sort_order": "ascending"},
        {"name": "value", "type": "string"},
    ]
    COLUMNAR_SCHEMA = [
        {"name": "key", "type": "int64", "sort_order": "ascending"},
        {"name": "value1", "type": "string"},
        {"name": "value2", "type": "string"},
        {"name": "value3", "type": "string"},
    ]

    def _prepare_env(self):
        if not exists("//sys/users/u"):
            create_user("u")
        if get("//sys/tablet_cells/@count") == 0:
            sync_create_cells(1)

    def _prepare_allowed(self, permission, table="//tmp/t"):
        self._prepare_env()
        self._create_simple_table(table, schema=self.SIMPLE_SCHEMA)
        sync_mount_table(table)
        set(table + "/@inherit_acl", False)
        set(table + "/@acl", [make_ace("allow", "u", permission)])

    def _prepare_denied(self, permission, table="//tmp/t"):
        self._prepare_env()
        self._create_simple_table(table, schema=self.SIMPLE_SCHEMA)
        sync_mount_table(table)
        set(table + "/@acl", [make_ace("deny", "u", permission)])

    def _prepare_columnar(self):
        create_user("u1")
        create_user("u2")
        sync_create_cells(1)
        self._create_simple_table("//tmp/t", schema=self.COLUMNAR_SCHEMA)
        sync_mount_table("//tmp/t")
        set(
            "//tmp/t/@acl",
            [
                make_ace("allow", ["u1", "u2"], "read"),
                make_ace("deny", "u1", "read", columns=["value1"]),
                make_ace("allow", "u2", "read", columns=["value1"]),
                make_ace("deny", "u2", "read", columns=["value2"]),
            ],
        )

    @authors("babenko")
    def test_select_allowed(self):
        self._prepare_allowed("read")
        insert_rows("//tmp/t", [{"key": 1, "value": "test"}])
        expected = [{"key": 1, "value": "test"}]
        actual = select_rows("* from [//tmp/t]", authenticated_user="u")
        assert_items_equal(actual, expected)

    @authors("babenko")
    def test_select_with_join_allowed(self):
        self._prepare_allowed("read", "//tmp/t1")
        self._prepare_allowed("read", "//tmp/t2")
        insert_rows("//tmp/t1", [{"key": 1, "value": "test1"}])
        insert_rows("//tmp/t2", [{"key": 1, "value": "test2"}])
        expected = [{"key": 1, "value1": "test1", "value2": "test2"}]
        actual = select_rows(
            "t1.key as key, t1.value as value1, t2.value as value2 "
            "from [//tmp/t1] as t1 join [//tmp/t2] as t2 on t1.key = t2.key",
            authenticated_user="u",
        )
        assert_items_equal(actual, expected)

    @authors("babenko")
    def test_select_with_join_denied(self):
        self._prepare_allowed("read", "//tmp/t1")
        self._prepare_denied("read", "//tmp/t2")
        insert_rows("//tmp/t1", [{"key": 1, "value": "test1"}])
        insert_rows("//tmp/t2", [{"key": 1, "value": "test2"}])
        with pytest.raises(YtError):
            select_rows(
                "t1.key as key, t1.value as value1, t2.value as value2 "
                "from [//tmp/t1] as t1 join [//tmp/t2] as t2 on t1.key = t2.key",
                authenticated_user="u",
            )

    @authors("babenko")
    def test_select_denied(self):
        self._prepare_denied("read")
        with pytest.raises(YtError):
            select_rows("* from [//tmp/t]", authenticated_user="u")

    @authors("babenko")
    def test_lookup_allowed(self):
        self._prepare_allowed("read")
        insert_rows("//tmp/t", [{"key": 1, "value": "test"}])
        expected = [{"key": 1, "value": "test"}]
        actual = lookup_rows("//tmp/t", [{"key": 1}], authenticated_user="u")
        assert_items_equal(actual, expected)

    @authors("babenko")
    def test_lookup_denied(self):
        self._prepare_denied("read")
        insert_rows("//tmp/t", [{"key": 1, "value": "test"}])
        with pytest.raises(YtError):
            lookup_rows("//tmp/t", [{"key": 1}], authenticated_user="u")

    @authors("babenko")
    def test_columnar_lookup_denied(self):
        self._prepare_columnar()
        insert_rows("//tmp/t", [{"key": 1, "value1": "a", "value2": "b", "value3": "c"}])
        with pytest.raises(YtError):
            lookup_rows("//tmp/t", [{"key": 1}], authenticated_user="u1")
        with pytest.raises(YtError):
            lookup_rows("//tmp/t", [{"key": 2}], authenticated_user="u1")
        with pytest.raises(YtError):
            lookup_rows(
                "//tmp/t",
                [{"key": 1}],
                column_names=["value1"],
                authenticated_user="u1",
            )
        with pytest.raises(YtError):
            lookup_rows(
                "//tmp/t",
                [{"key": 1}],
                column_names=["value2"],
                authenticated_user="u2",
            )

    @authors("babenko")
    def test_columnar_lookup_allowed(self):
        self._prepare_columnar()
        insert_rows("//tmp/t", [{"key": 1, "value1": "a", "value2": "b", "value3": "c"}])
        assert (
            lookup_rows(
                "//tmp/t",
                [{"key": 1}],
                column_names=["key", "value3"],
                authenticated_user="u1",
            )
            == [{"key": 1, "value3": "c"}]
        )
        assert (
            lookup_rows(
                "//tmp/t",
                [{"key": 1}],
                column_names=["key", "value1"],
                authenticated_user="u2",
            )
            == [{"key": 1, "value1": "a"}]
        )

    @authors("babenko")
    def test_columnar_select_denied(self):
        self._prepare_columnar()
        insert_rows("//tmp/t", [{"key": 1, "value1": "a", "value2": "b", "value3": "c"}])
        with pytest.raises(YtError):
            select_rows("* from [//tmp/t]", authenticated_user="u1")
        with pytest.raises(YtError):
            select_rows("value1 from [//tmp/t]", authenticated_user="u1")
        with pytest.raises(YtError):
            select_rows("value2 from [//tmp/t]", authenticated_user="u2")

    @authors("babenko")
    def test_columnar_select_allowed(self):
        self._prepare_columnar()
        insert_rows("//tmp/t", [{"key": 1, "value1": "a", "value2": "b", "value3": "c"}])
        assert select_rows("key, value3 from [//tmp/t] where key = 1", authenticated_user="u1") == [
            {"key": 1, "value3": "c"}
        ]
        assert select_rows("key, value1 from [//tmp/t] where key = 1", authenticated_user="u2") == [
            {"key": 1, "value1": "a"}
        ]

    @authors("babenko")
    def test_insert_allowed(self):
        self._prepare_allowed("write")
        insert_rows("//tmp/t", [{"key": 1, "value": "test"}], authenticated_user="u")
        expected = [{"key": 1, "value": "test"}]
        actual = lookup_rows("//tmp/t", [{"key": 1}])
        assert_items_equal(actual, expected)

    @authors("babenko")
    def test_insert_denied(self):
        self._prepare_denied("write")
        with pytest.raises(YtError):
            insert_rows("//tmp/t", [{"key": 1, "value": "test"}], authenticated_user="u")

    @authors("babenko")
    def test_delete_allowed(self):
        self._prepare_allowed("write")
        insert_rows("//tmp/t", [{"key": 1, "value": "test"}])
        delete_rows("//tmp/t", [{"key": 1}], authenticated_user="u")
        expected = []
        actual = lookup_rows("//tmp/t", [{"key": 1}])
        assert_items_equal(actual, expected)

    @authors("babenko")
    def test_delete_denied(self):
        self._prepare_denied("write")
        with pytest.raises(YtError):
            delete_rows("//tmp/t", [{"key": 1}], authenticated_user="u")


class TestSortedDynamicTablesAclMulticell(TestSortedDynamicTablesAcl):
    NUM_SECONDARY_MASTER_CELLS = 2


class TestSortedDynamicTablesAclRpcProxy(TestSortedDynamicTablesAcl):
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True


class TestSortedDynamicTablesAclPortal(TestSortedDynamicTablesAclMulticell):
    ENABLE_TMP_PORTAL = True


################################################################################


class TestOrderedDynamicTablesAcl(TestOrderedDynamicTablesBase):
    USE_PERMISSION_CACHE = False

    def _prepare_allowed(self, permission):
        create_user("u")
        sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        sync_mount_table("//tmp/t")
        set("//tmp/t/@inherit_acl", False)
        set("//tmp/t/@acl", [make_ace("allow", "u", permission)])

    def _prepare_denied(self, permission):
        create_user("u")
        sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        sync_mount_table("//tmp/t")
        set("//tmp/t/@acl", [make_ace("deny", "u", permission)])

    @authors("babenko")
    def test_select_allowed(self):
        self._prepare_allowed("read")
        rows = [{"a": 1}]
        insert_rows("//tmp/t", rows)
        assert select_rows("a from [//tmp/t]", authenticated_user="u") == rows

    @authors("babenko")
    def test_select_denied(self):
        self._prepare_denied("read")
        with pytest.raises(YtError):
            select_rows("* from [//tmp/t]", authenticated_user="u")

    @authors("babenko")
    def test_insert_allowed(self):
        self._prepare_allowed("write")
        rows = [{"a": 1}]
        insert_rows("//tmp/t", rows, authenticated_user="u")
        assert select_rows("a from [//tmp/t]") == rows

    @authors("babenko")
    def test_insert_denied(self):
        self._prepare_denied("write")
        with pytest.raises(YtError):
            insert_rows("//tmp/t", [{"a": 1}], authenticated_user="u")

    @authors("babenko")
    def test_trim_allowed(self):
        self._prepare_allowed("write")
        rows = [{"a": 1}, {"a": 2}]
        insert_rows("//tmp/t", rows)
        trim_rows("//tmp/t", 0, 1, authenticated_user="u")
        assert select_rows("a from [//tmp/t]") == rows[1:]

    @authors("babenko")
    def test_trim_denied(self):
        self._prepare_denied("write")
        insert_rows("//tmp/t", [{"a": 1}, {"a": 2}])
        with pytest.raises(YtError):
            trim_rows("//tmp/t", 0, 1, authenticated_user="u")


class TestOrderedDynamicTablesAclMulticell(TestOrderedDynamicTablesAcl):
    NUM_SECONDARY_MASTER_CELLS = 2


class TestOrderedDynamicTablesAclRpcProxy(TestOrderedDynamicTablesAcl):
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True


class TestOrderedDynamicTablesAclPortal(TestOrderedDynamicTablesAclMulticell):
    ENABLE_TMP_PORTAL = True
