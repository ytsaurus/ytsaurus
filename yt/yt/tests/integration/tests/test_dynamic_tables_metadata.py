from test_sorted_dynamic_tables import TestSortedDynamicTablesBase

from yt_env_setup import skip_if_rpc_driver_backend

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
    mount_table, remount_table, reshard_table, wait_for_tablet_state,
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
    build_snapshot, gc_collect, clear_metadata_caches,
    get_driver, Driver, execute_command)

from yt.common import YtError
from yt.environment.helpers import assert_items_equal

import pytest

##################################################################


class TestSortedDynamicTablesMetadataCaching(TestSortedDynamicTablesBase):
    USE_MASTER_CACHE = True

    DELTA_DRIVER_CONFIG = {
        "max_rows_per_write_request": 2,
        "table_mount_cache": {
            "expire_after_successful_update_time": 60000,
            "refresh_time": 60000,
            "expire_after_failed_update_time": 1000,
            "expire_after_access_time": 300000,
        },
    }

    DELTA_NODE_CONFIG = {"tablet_node": {"tablet_snapshot_eviction_timeout": 0}}

    # Reimplement dynamic table commands without calling clear_metadata_caches()

    def _mount_table(self, path, **kwargs):
        kwargs["path"] = path
        return execute_command("mount_table", kwargs)

    def _unmount_table(self, path, **kwargs):
        kwargs["path"] = path
        return execute_command("unmount_table", kwargs)

    def _reshard_table(self, path, arg, **kwargs):
        kwargs["path"] = path
        kwargs["pivot_keys"] = arg
        return execute_command("reshard_table", kwargs)

    def _sync_mount_table(self, path, **kwargs):
        self._mount_table(path, **kwargs)
        print_debug("Waiting for tablets to become mounted...")
        wait_for_tablet_state(path, "mounted", **kwargs)

    def _sync_unmount_table(self, path, **kwargs):
        self._unmount_table(path, **kwargs)
        print_debug("Waiting for tablets to become unmounted...")
        wait_for_tablet_state(path, "unmounted", **kwargs)

    @authors("savrus")
    def test_select_with_expired_schema(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        self._reshard_table("//tmp/t", [[], [1]])
        self._sync_mount_table("//tmp/t")
        rows = [{"key": i, "value": str(i)} for i in xrange(2)]
        insert_rows("//tmp/t", rows)
        assert_items_equal(select_rows("* from [//tmp/t]"), rows)
        self._sync_unmount_table("//tmp/t")
        alter_table(
            "//tmp/t",
            schema=[
                {"name": "key", "type": "int64", "sort_order": "ascending"},
                {"name": "key2", "type": "int64", "sort_order": "ascending"},
                {"name": "value", "type": "string"},
            ],
        )
        self._sync_mount_table("//tmp/t")
        expected = [{"key": i, "key2": None, "value": str(i)} for i in xrange(2)]
        assert_items_equal(select_rows("* from [//tmp/t]"), expected)

    @authors("savrus")
    def test_lookup_from_removed_table(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t2")
        self._sync_mount_table("//tmp/t2")
        rows = [{"key": i, "value": str(i)} for i in xrange(2)]

        # Do lookup to clear metadata and master cache.
        # Unfortunately master cache has old schema and it is retreived in driver where key is constructed.
        # Client invalidate&retry doesn't rebuild driver's key so this lookup has no chances to be completed.
        try:
            lookup_rows("//tmp/t2", [{"key": 0}])
        except YtError:
            pass

        insert_rows("//tmp/t2", rows)
        assert_items_equal(select_rows("* from [//tmp/t2]"), rows)
        remove("//tmp/t2")
        self._create_simple_table("//tmp/t2")
        self._sync_mount_table("//tmp/t2")
        actual = lookup_rows("//tmp/t2", [{"key": 0}])
        assert actual == []


class TestSortedDynamicTablesMetadataCaching2(TestSortedDynamicTablesMetadataCaching):
    USE_MASTER_CACHE = False

    @authors("savrus")
    @skip_if_rpc_driver_backend
    def test_metadata_cache_invalidation(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t1", enable_compaction_and_partitioning=False)
        self._sync_mount_table("//tmp/t1")

        rows = [{"key": i, "value": str(i)} for i in xrange(3)]
        keys = [{"key": row["key"]} for row in rows]
        insert_rows("//tmp/t1", rows)
        assert_items_equal(lookup_rows("//tmp/t1", keys), rows)

        self._sync_unmount_table("//tmp/t1")
        with pytest.raises(YtError):
            lookup_rows("//tmp/t1", keys)
        clear_metadata_caches()
        self._sync_mount_table("//tmp/t1")

        assert_items_equal(lookup_rows("//tmp/t1", keys), rows)

        self._sync_unmount_table("//tmp/t1")
        with pytest.raises(YtError):
            select_rows("* from [//tmp/t1]")
        clear_metadata_caches()
        self._sync_mount_table("//tmp/t1")

        assert_items_equal(select_rows("* from [//tmp/t1]"), rows)

        def reshard_mounted_table(path, pivots):
            self._sync_unmount_table(path)
            self._reshard_table(path, pivots)
            self._sync_mount_table(path)

        reshard_mounted_table("//tmp/t1", [[], [1]])
        assert_items_equal(lookup_rows("//tmp/t1", keys), rows)

        reshard_mounted_table("//tmp/t1", [[], [1], [2]])
        assert_items_equal(select_rows("* from [//tmp/t1]"), rows)

        reshard_mounted_table("//tmp/t1", [[]])
        rows = [{"key": i, "value": str(i + 1)} for i in xrange(3)]
        with pytest.raises(YtError):
            insert_rows("//tmp/t1", rows)
        insert_rows("//tmp/t1", rows)

        insert_rows("//tmp/t1", rows)
        assert_items_equal(lookup_rows("//tmp/t1", keys), rows)


##################################################################


class TestSortedDynamicTablesMetadataCachingMulticell(TestSortedDynamicTablesMetadataCaching):
    NUM_SECONDARY_MASTER_CELLS = 2


class TestSortedDynamicTablesMetadataCachingMulticell2(TestSortedDynamicTablesMetadataCaching2):
    NUM_SECONDARY_MASTER_CELLS = 2


###################################################################


class TestSortedDynamicTablesMetadataCachingRpcProxy(TestSortedDynamicTablesMetadataCaching):
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True


class TestSortedDynamicTablesMetadataCachingRpcProxy2(TestSortedDynamicTablesMetadataCaching2):
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True
