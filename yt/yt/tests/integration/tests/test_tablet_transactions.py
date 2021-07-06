from yt_env_setup import YTEnvSetup

from yt_commands import (  # noqa
    authors, print_debug, wait, wait_assert, wait_breakpoint, release_breakpoint, with_breakpoint,
    events_on_fs, reset_events_on_fs,
    create, ls, get, set, copy, move, remove, link, exists, concatenate,
    create_account, create_network_project, create_tmpdir, create_user, create_group,
    create_pool, create_pool_tree, remove_pool_tree,
    create_data_center, create_rack, create_table,
    create_tablet_cell_bundle, remove_tablet_cell_bundle, create_table_replica,
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
    get_driver, execute_command)

from yt.common import YtError

import pytest

##################################################################


class TestTabletTransactions(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 5
    NUM_SCHEDULERS = 0
    USE_DYNAMIC_TABLES = True

    def _create_table(self, path):
        create(
            "table",
            path,
            attributes={
                "dynamic": True,
                "schema": [
                    {"name": "key", "type": "int64", "sort_order": "ascending"},
                    {"name": "value", "type": "string"},
                ],
            },
        )

    @authors("sandello")
    def test_sticky_tablet_transactions(self):
        sync_create_cells(1)
        self._create_table("//tmp/t")
        sync_mount_table("//tmp/t")

        def _keys(i, j):
            return [{"key": x} for x in range(i, j)]

        def _rows(i, j):
            return [{"key": x, "value": str(x)} for x in xrange(i, j)]

        assert select_rows("* from [//tmp/t]") == []

        tx1 = start_transaction(type="tablet")
        insert_rows("//tmp/t", _rows(0, 1), tx=tx1)

        tx2 = start_transaction(type="tablet")
        delete_rows("//tmp/t", _keys(0, 1), tx=tx2)

        # cannot see transaction effects until not committed
        assert select_rows("* from [//tmp/t]") == []
        assert lookup_rows("//tmp/t", _keys(0, 1)) == []

        commit_transaction(tx1)
        assert select_rows("* from [//tmp/t]") == _rows(0, 1)
        assert lookup_rows("//tmp/t", _keys(0, 1)) == _rows(0, 1)

        # cannot see unsynchronized transaction effects
        assert select_rows("* from [//tmp/t]", tx=tx2) == []
        assert lookup_rows("//tmp/t", _keys(0, 1), tx=tx2) == []

        # cannot commit transaction twice
        with pytest.raises(YtError):
            commit_transaction(tx1)

        # cannot commit conflicting transaction
        with pytest.raises(YtError):
            commit_transaction(tx2)
