from test_sorted_dynamic_tables import TestSortedDynamicTablesBase

from yt_env_setup import wait

from yt_commands import (  # noqa
    authors, print_debug, wait, wait_assert, wait_breakpoint, release_breakpoint, with_breakpoint,
    events_on_fs, reset_events_on_fs,
    create, ls, get, set, copy, move, remove, link, exists, concatenate,
    create_account, create_network_project, create_tmpdir, create_user, create_group,
    create_pool, create_pool_tree, remove_pool_tree,
    create_data_center, create_rack, create_table,
    create_tablet_cell_bundle, remove_tablet_cell,
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
    get_driver, Driver, execute_command, generate_uuid)

from yt_helpers import Profiler

from yt.yson import YsonEntity
from yt.environment.helpers import assert_items_equal

import pytest

import time

##################################################################


class TestDynamicTablesProfiling(TestSortedDynamicTablesBase):
    DELTA_NODE_CONFIG = {"cluster_connection": {"timestamp_provider": {"update_period": 100}}}

    @authors("gridem")
    def test_sorted_tablet_node_profiling(self):
        sync_create_cells(1)
        create_user("u")

        table_path = "//tmp/{}".format(generate_uuid())
        self._create_simple_table(table_path, dynamic_store_auto_flush_period=None)
        get(table_path + "/@dynamic_store_auto_flush_period")
        sync_mount_table(table_path)

        tablet_profiling = self._get_table_profiling(table_path, user="u")

        def get_all_counters(count_name):
            all_counters = (
                tablet_profiling.get_counter("lookup/" + count_name),
                tablet_profiling.get_counter("lookup/unmerged_" + count_name),
                tablet_profiling.get_counter("select/" + count_name),
                tablet_profiling.get_counter("select/unmerged_" + count_name),
                tablet_profiling.get_counter("write/" + count_name),
                tablet_profiling.get_counter("commit/" + count_name),
            )
            print_debug("all_counters = {}".format(all_counters))
            return all_counters

        assert get_all_counters("row_count") == (0, 0, 0, 0, 0, 0)
        assert get_all_counters("data_weight") == (0, 0, 0, 0, 0, 0)
        assert tablet_profiling.get_counter("lookup/cpu_time") == 0
        assert tablet_profiling.get_counter("select/cpu_time") == 0

        rows = [{"key": 1, "value": "2"}]
        keys = [{"key": 1}]
        insert_rows(table_path, rows, authenticated_user="u")

        wait(
            lambda: get_all_counters("row_count") == (0, 0, 0, 0, 1, 1)
            and get_all_counters("data_weight") == (0, 0, 0, 0, 10, 10)
            and tablet_profiling.get_counter("lookup/cpu_time") == 0
            and tablet_profiling.get_counter("select/cpu_time") == 0
        )

        actual = lookup_rows(table_path, keys, authenticated_user="u")
        assert_items_equal(actual, rows)

        wait(
            lambda: get_all_counters("row_count") == (1, 1, 0, 0, 1, 1)
            and get_all_counters("data_weight") == (10, 25, 0, 0, 10, 10)
            and tablet_profiling.get_counter("lookup/cpu_time") > 0
            and tablet_profiling.get_counter("select/cpu_time") == 0
        )

        actual = select_rows("* from [{}]".format(table_path), authenticated_user="u")
        assert_items_equal(actual, rows)

        wait(
            lambda: get_all_counters("row_count") == (1, 1, 1, 1, 1, 1)
            and get_all_counters("data_weight") == (10, 25, 10, 25, 10, 10)
            and tablet_profiling.get_counter("lookup/cpu_time") > 0
            and tablet_profiling.get_counter("select/cpu_time") > 0
        )

    @authors("gridem")
    def test_sorted_default_enabled_tablet_node_profiling(self):
        sync_create_cells(1)

        table_path = "//tmp/{}".format(generate_uuid())
        self._create_simple_table(table_path, dynamic_store_auto_flush_period=None)
        sync_mount_table(table_path)

        table_profiling = self._get_table_profiling(table_path)

        def get_all_counters(count_name):
            return (
                table_profiling.get_counter("lookup/" + count_name),
                table_profiling.get_counter("write/" + count_name),
                table_profiling.get_counter("commit/" + count_name),
            )

        assert get_all_counters("row_count") == (0, 0, 0)
        assert get_all_counters("data_weight") == (0, 0, 0)
        assert table_profiling.get_counter("lookup/cpu_time") == 0

        rows = [{"key": 1, "value": "2"}]
        keys = [{"key": 1}]
        insert_rows(table_path, rows)

        wait(
            lambda: get_all_counters("row_count") == (0, 1, 1)
            and get_all_counters("data_weight") == (0, 10, 10)
            and table_profiling.get_counter("lookup/cpu_time") == 0
        )

        actual = lookup_rows(table_path, keys)
        assert_items_equal(actual, rows)

        wait(
            lambda: get_all_counters("row_count") == (1, 1, 1)
            and get_all_counters("data_weight") == (10, 10, 10)
            and table_profiling.get_counter("lookup/cpu_time") > 0
        )

    @authors("iskhakovt")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    @pytest.mark.parametrize("in_memory_mode", ["none", "compressed"])
    def test_data_weight_performance_counters(self, optimize_for, in_memory_mode):
        sync_create_cells(1)
        self._create_simple_table(
            "//tmp/t",
            optimize_for=optimize_for,
            in_memory_mode=in_memory_mode,
            dynamic_store_auto_flush_period=YsonEntity(),
        )
        sync_mount_table("//tmp/t")

        path = "//tmp/t/@tablets/0/performance_counters"

        insert_rows("//tmp/t", [{"key": 0, "value": "hello"}])

        wait(lambda: get(path + "/dynamic_row_write_data_weight_count") > 0)

        select_rows("* from [//tmp/t]")

        # Dynamic read must change, lookup must not change
        wait(lambda: get(path + "/dynamic_row_read_data_weight_count") > 0)
        assert get(path + "/dynamic_row_lookup_data_weight_count") == 0

        lookup_rows("//tmp/t", [{"key": 0}])

        # Dynamic read lookup change, read must not change
        wait(lambda: get(path + "/dynamic_row_lookup_data_weight_count") > 0)
        assert get(path + "/dynamic_row_read_data_weight_count") == get(path + "/dynamic_row_lookup_data_weight_count")

        # Static read/lookup must not change
        assert get(path + "/static_chunk_row_read_data_weight_count") == 0
        assert get(path + "/static_chunk_row_lookup_data_weight_count") == 0

        sync_flush_table("//tmp/t")

        select_rows("* from [//tmp/t]")

        # Static read must change, lookup must not change
        wait(lambda: get(path + "/static_chunk_row_read_data_weight_count") > 0)
        assert get(path + "/static_chunk_row_lookup_data_weight_count") == 0

        lookup_rows("//tmp/t", [{"key": 0}])

        # Static lookup must change, read must not change
        wait(lambda: get(path + "/static_chunk_row_lookup_data_weight_count") > 0)
        assert get(path + "/static_chunk_row_read_data_weight_count") == get(
            path + "/static_chunk_row_lookup_data_weight_count"
        )

    @authors("prime")
    def test_bundle_solomon_tag(self):
        default_cell = sync_create_cells(1)[0]

        def get_solomon_tags(cell_id):
            node_address = get("#%s/@peers/0/address" % cell_id)
            return get("//sys/cluster_nodes/%s/orchid/monitoring/solomon/dynamic_tags" % node_address)

        wait(lambda: get_solomon_tags(default_cell) == {"tablet_cell_bundle": "default"})

        create_tablet_cell_bundle("b1", attributes={})
        bundle_cells = sync_create_cells(20, tablet_cell_bundle="b1")

        wait(lambda: get_solomon_tags(default_cell) == {})
        remove_tablet_cell(default_cell)

        for cell_id in bundle_cells:
            wait(lambda: get_solomon_tags(cell_id) == {"tablet_cell_bundle": "b1"})

        set("//sys/tablet_cell_bundles/b1/@dynamic_options/solomon_tag", "tag1")

        for cell_id in bundle_cells:
            wait(lambda: get_solomon_tags(cell_id) == {"tablet_cell_bundle": "tag1"})

    @authors("prime")
    def test_profiling_path_letters(self):
        sync_create_cells(1)

        create("map_node", "//tmp/path_letters")
        set("//tmp/path_letters/@profiling_mode", "path_letters")

        self._create_simple_table("//tmp/path_letters/t0")
        self._create_simple_table("//tmp/path_letters/t1")
        sync_mount_table("//tmp/path_letters/t0")
        sync_mount_table("//tmp/path_letters/t1")

        insert_rows("//tmp/path_letters/t0", [{"key": 0, "value": "hello"}])
        time.sleep(2)

        profiler = Profiler.at_tablet_node("//tmp/path_letters/t0")

        counter = profiler.get("write/row_count", {"table_path": "//tmp/path_letters/t_"})
        assert counter == 1
