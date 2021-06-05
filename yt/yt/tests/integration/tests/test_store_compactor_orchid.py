from test_sorted_dynamic_tables import TestSortedDynamicTablesBase

from yt_commands import (  # noqa
    authors, print_debug, wait, wait_assert, wait_breakpoint, release_breakpoint, with_breakpoint,
    events_on_fs, reset_events_on_fs,
    create, ls, get, set, copy, move, remove, link, exists,
    create_account, create_network_project, create_tmpdir, create_user, create_group,
    create_pool, create_pool_tree, remove_pool_tree,
    create_data_center, create_rack, create_table,
    create_tablet_cell_bundle,
    make_ace, check_permission, add_member,
    make_batch_request, execute_batch, get_batch_error,
    start_transaction, abort_transaction, commit_transaction, lock,
    insert_rows, select_rows, lookup_rows, delete_rows, trim_rows, lock_rows, alter_table,
    read_file, write_file, read_table, write_table, write_local_file,
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
    mount_table, remount_table, reshard_table, generate_timestamp,
    wait_for_tablet_state, wait_for_cells,
    sync_create_cells, sync_mount_table, sync_unmount_table,
    sync_freeze_table, sync_unfreeze_table, sync_reshard_table,
    sync_flush_table, sync_compact_table,
    get_first_chunk_id, get_singular_chunk_id, get_chunk_replication_factor, multicell_sleep,
    update_nodes_dynamic_config, update_controller_agent_config,
    update_op_parameters, enable_op_detailed_logs,
    set_node_banned, set_banned_flag, set_account_disk_space_limit,
    check_all_stderrs,
    create_test_tables, create_dynamic_table, PrepareTables,
    get_statistics, get_tablet_leader_address,
    make_random_string, raises_yt_error,
    build_snapshot,
    get_driver, Driver, execute_command,
    AsyncLastCommittedTimestamp, MinTimestamp)

#################################################################


class TestStoreCompactorOrchid(TestSortedDynamicTablesBase):
    NUM_NODES = 3
    USE_DYNAMIC_TABLES = True

    @authors("akozhikhov")
    def test_compaction_orchid(self):
        NUM_TABLES = 3L
        nodes = ls("//sys/cluster_nodes")
        for node in nodes[1:]:
            set("//sys/cluster_nodes/{0}/@disable_tablet_cells".format(node), True)
        node = nodes[0]

        sync_create_cells(1)

        for table_idx in range(NUM_TABLES):
            create_dynamic_table(
                "//tmp/t{0}".format(table_idx),
                schema=[
                    {"name": "key", "type": "int64", "sort_order": "ascending"},
                    {"name": "value", "type": "string"},
                ],
            )

        empty_task_dict = {
            "task_count": 0,
            "finished_task_count": 0,
            "pending_tasks": [],
            "finished_tasks": [],
        }
        assert get("//sys/cluster_nodes/{0}/orchid/store_compactor/compaction_tasks".format(node)) == empty_task_dict
        assert get("//sys/cluster_nodes/{0}/orchid/store_compactor/partitioning_tasks".format(node)) == empty_task_dict

        for table_idx in range(NUM_TABLES):
            set(
                "//tmp/t{0}/@enable_compaction_and_partitioning".format(table_idx),
                False,
            )
            sync_mount_table("//tmp/t{0}".format(table_idx))

        for table_idx in range(NUM_TABLES):
            for i in range(5):
                insert_rows(
                    "//tmp/t{0}".format(table_idx),
                    [{"key": j, "value": str(j)} for j in range(i * 10, (i + 1) * 10)],
                )
                sync_flush_table("//tmp/t{0}".format(table_idx))
            set("//tmp/t{0}/@enable_compaction_and_partitioning".format(table_idx), True)
            remount_table("//tmp/t{0}".format(table_idx))

        tablets = [get("//tmp/t{0}/@tablets/0".format(tablet_idx)) for tablet_idx in range(NUM_TABLES)]
        tablet_ids = [tablets[i]["tablet_id"] for i in range(NUM_TABLES)]
        tablet_ids.sort()

        def _compaction_task_finished():
            compaction_tasks = get("//sys/cluster_nodes/{0}/orchid/store_compactor/compaction_tasks".format(node))
            for task in compaction_tasks["finished_tasks"]:
                # We don't want to predict priorities in integration test
                del task["task_priority"]
                del task["partition_id"]
                del task["mount_revision"]
            compaction_tasks["finished_tasks"].sort(key=lambda x: x["tablet_id"])

            expected_compaction_tasks = {
                "task_count": 0,
                "finished_task_count": NUM_TABLES,
                "pending_tasks": [],
                "finished_tasks": [{"tablet_id": tablet_ids[i], "store_count": 5} for i in range(NUM_TABLES)],
            }

            return compaction_tasks == expected_compaction_tasks

        wait(lambda: _compaction_task_finished())
        assert get("//sys/cluster_nodes/{0}/orchid/store_compactor/partitioning_tasks".format(node)) == empty_task_dict

    @authors("akozhikhov")
    def test_partitioning_orchid(self):
        nodes = ls("//sys/cluster_nodes")
        for node in nodes[1:]:
            set("//sys/cluster_nodes/{0}/@disable_tablet_cells".format(node), True)
        node = nodes[0]

        sync_create_cells(1)
        self._create_simple_table("//tmp/t")

        self._create_partitions(partition_count=2)

        # Now add store to eden
        sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", [{"key": i} for i in xrange(2, 6)])
        sync_flush_table("//tmp/t")
        assert len(get("//tmp/t/@chunk_ids")) == 3
        assert get("#{}/@eden".format(get("//tmp/t/@chunk_ids/{0}".format(2))))

        set("//tmp/t/@enable_compaction_and_partitioning", True)
        set("//tmp/t/@forced_compaction_revision", 1)
        remount_table("//tmp/t")

        def _partition_task_finished():
            expected_partition_task = {
                "task_count": 0,
                "finished_task_count": 1,
                "pending_tasks": [],
                "finished_tasks": [{"tablet_id": get("//tmp/t/@tablets/0/tablet_id"), "store_count": 1}],
            }
            partition_task = get("//sys/cluster_nodes/{0}/orchid/store_compactor/partitioning_tasks".format(node))
            if partition_task["finished_task_count"] == 0:
                return False
            del partition_task["finished_tasks"][0]["task_priority"]
            del partition_task["finished_tasks"][0]["partition_id"]
            del partition_task["finished_tasks"][0]["mount_revision"]
            return partition_task == expected_partition_task

        wait(lambda: _partition_task_finished())
