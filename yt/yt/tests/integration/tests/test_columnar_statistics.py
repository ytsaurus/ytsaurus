from yt_env_setup import wait, YTEnvSetup, Restarter, CONTROLLER_AGENTS_SERVICE

from yt_commands import (  # noqa
    authors, print_debug, wait, retry, wait_assert, wait_breakpoint, release_breakpoint, with_breakpoint,
    events_on_fs, reset_events_on_fs,
    create, ls, get, set, copy, move, remove, link, exists, concatenate,
    create_account, remove_account,
    create_network_project, create_tmpdir, create_user, create_group, create_medium,
    create_pool, create_pool_tree, remove_pool_tree,
    create_data_center, create_rack, create_table, create_proxy_role,
    create_tablet_cell_bundle, remove_tablet_cell_bundle, create_tablet_cell, create_table_replica,
    make_ace, check_permission, add_member, remove_member, remove_group, remove_user,
    remove_network_project,
    make_batch_request, execute_batch, get_batch_error,
    start_transaction, abort_transaction, commit_transaction, lock,
    externalize, internalize,
    insert_rows, select_rows, lookup_rows, delete_rows, trim_rows, alter_table,
    read_file, write_file, read_table, write_table, write_local_file, read_blob_table,
    read_journal, write_journal, truncate_journal, wait_until_sealed,
    map, reduce, map_reduce, join_reduce, merge, vanilla, sort, erase, remote_copy,
    run_test_vanilla, run_sleeping_vanilla,
    abort_job, list_jobs, get_job, abandon_job, interrupt_job,
    get_job_fail_context, get_job_input, get_job_stderr, get_job_spec, get_job_input_paths,
    dump_job_context, poll_job_shell,
    abort_op, complete_op, suspend_op, resume_op,
    get_operation, list_operations, clean_operations,
    get_operation_cypress_path, scheduler_orchid_pool_path,
    scheduler_orchid_default_pool_tree_path, scheduler_orchid_operation_path,
    scheduler_orchid_default_pool_tree_config_path, scheduler_orchid_path,
    scheduler_orchid_node_path, scheduler_orchid_pool_tree_config_path, scheduler_orchid_pool_tree_path,
    mount_table, unmount_table, freeze_table, unfreeze_table, reshard_table, remount_table, generate_timestamp,
    reshard_table_automatic, wait_for_tablet_state, wait_for_cells,
    get_tablet_infos, get_table_pivot_keys, get_tablet_leader_address,
    get_table_columnar_statistics,
    sync_create_cells, sync_mount_table, sync_unmount_table,
    sync_freeze_table, sync_unfreeze_table, sync_reshard_table,
    sync_flush_table, sync_compact_table, sync_remove_tablet_cells,
    sync_reshard_table_automatic, sync_balance_tablet_cells,
    get_first_chunk_id, get_singular_chunk_id, get_chunk_replication_factor, multicell_sleep,
    update_nodes_dynamic_config, update_controller_agent_config,
    update_op_parameters, enable_op_detailed_logs,
    set_node_banned, set_banned_flag,
    set_account_disk_space_limit, set_node_decommissioned,
    get_account_disk_space, get_account_committed_disk_space,
    check_all_stderrs,
    create_test_tables, create_dynamic_table, PrepareTables,
    get_statistics, get_recursive_disk_space, get_chunk_owner_disk_space, cluster_resources_equal,
    make_random_string, raises_yt_error,
    build_snapshot, build_master_snapshots,
    gc_collect, is_multicell, clear_metadata_caches,
    get_driver, Driver, execute_command, generate_uuid,
    AsyncLastCommittedTimestamp, MinTimestamp)

from yt.common import YtError

import pytest


class TestColumnarStatistics(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1
    USE_DYNAMIC_TABLES = True

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "enable_map_job_size_adjustment": False,
            "max_user_file_table_data_weight": 2000,
            "operation_options": {
                "spec_template": {
                    "use_columnar_statistics": True,
                },
            },
            "tagged_memory_statistics_update_period": 100,
        },
    }

    def _expect_statistics(
        self,
        lower_row_index,
        upper_row_index,
        columns,
        expected_data_weights,
        expected_timestamp_weight=None,
        expected_legacy_data_weight=0,
        fetcher_mode="from_nodes",
        table="//tmp/t",
    ):
        path = '["{0}{{{1}}}[{2}:{3}]";]'.format(
            table,
            columns,
            "#" + str(lower_row_index) if lower_row_index is not None else "",
            "#" + str(upper_row_index) if upper_row_index is not None else "",
        )
        statistics = get_table_columnar_statistics(path, fetcher_mode=fetcher_mode)[0]
        assert statistics["legacy_chunks_data_weight"] == expected_legacy_data_weight
        assert statistics["column_data_weights"] == dict(zip(columns.split(","), expected_data_weights))
        if expected_timestamp_weight is not None:
            assert statistics["timestamp_total_weight"] == expected_timestamp_weight

    def _expect_multi_statistics(
        self,
        paths,
        lower_row_indices,
        upper_row_indices,
        all_columns,
        all_expected_data_weights,
        all_expected_timestamp_weight=None,
    ):
        assert len(paths) == len(all_columns)
        for index in range(len(paths)):
            paths[index] = "{0}{{{1}}}[{2}:{3}]".format(
                paths[index],
                all_columns[index],
                "#" + str(lower_row_indices[index]) if lower_row_indices[index] is not None else "",
                "#" + str(upper_row_indices[index]) if upper_row_indices[index] is not None else "",
            )
        yson_paths = "["
        for path in paths:
            yson_paths += '"' + path + '";'
        yson_paths += "]"
        allStatistics = get_table_columnar_statistics(yson_paths)
        assert len(allStatistics) == len(all_expected_data_weights)
        for index in range(len(allStatistics)):
            assert allStatistics[index]["legacy_chunks_data_weight"] == 0
            assert allStatistics[index]["column_data_weights"] == dict(
                zip(all_columns[index].split(","), all_expected_data_weights[index])
            )
            if all_expected_timestamp_weight is not None:
                assert allStatistics[index] == all_expected_timestamp_weight[index]

    def _create_simple_dynamic_table(self, path, optimize_for="lookup"):
        create(
            "table",
            path,
            attributes={
                "schema": [
                    {"name": "key", "type": "int64", "sort_order": "ascending"},
                    {"name": "value", "type": "string"},
                ],
                "dynamic": True,
                "optimize_for": optimize_for,
            },
        )

    @authors("max42")
    def test_get_table_columnar_statistics(self):
        create("table", "//tmp/t")
        write_table("<append=%true>//tmp/t", [{"a": "x" * 100, "b": 42}, {"c": 1.2}])
        write_table("<append=%true>//tmp/t", [{"a": "x" * 200}, {"c": True}])
        write_table("<append=%true>//tmp/t", [{"b": None, "c": 0}, {"a": "x" * 1000}])
        with pytest.raises(YtError):
            get_table_columnar_statistics('["//tmp/t";]')
        self._expect_statistics(2, 2, "a,b,c", [0, 0, 0])
        self._expect_statistics(0, 6, "a,b,c", [1300, 8, 17])
        self._expect_statistics(0, 6, "a,c,x", [1300, 17, 0])
        self._expect_statistics(1, 5, "a,b,c", [1300, 8, 17])
        self._expect_statistics(2, 5, "a", [1200])
        self._expect_statistics(1, 4, "", [])

    @authors("gritukan")
    def test_get_table_approximate_statistics(self):
        def make_table(column_weights):
            if exists("//tmp/t"):
                remove("//tmp/t")
            create("table", "//tmp/t")
            if column_weights:
                write_table(
                    "//tmp/t",
                    [{"x{}".format(i): "a" * column_weights[i] for i in range(len(column_weights))}],
                )

        make_table([255, 12, 45, 1, 0])
        self._expect_statistics(
            0,
            1,
            "x0,x1,x2,x3,x4,zzz",
            [255, 12, 45, 1, 0, 0],
            fetcher_mode="from_master",
        )

        create("table", "//tmp/t2")
        remote_copy(
            in_="//tmp/t",
            out="//tmp/t2",
            spec={"cluster_connection": self.__class__.Env.configs["driver"]},
        )
        self._expect_statistics(
            0,
            1,
            "x0,x1,x2,x3,x4,zzz",
            [255, 12, 45, 1, 0, 0],
            fetcher_mode="from_master",
            table="//tmp/t2",
        )

        make_table([510, 12, 13, 1, 0])
        self._expect_statistics(0, 1, "x0,x1,x2,x3,x4", [510, 12, 14, 2, 0], fetcher_mode="from_master")

        make_table([256, 12, 13, 1, 0])
        self._expect_statistics(0, 1, "x0,x1,x2,x3,x4", [256, 12, 14, 2, 0], fetcher_mode="from_master")

        make_table([1])
        self._expect_statistics(0, 1, "", [], fetcher_mode="from_master")

        make_table([])
        self._expect_statistics(0, 1, "x", [0], fetcher_mode="from_master")

        set("//sys/@config/chunk_manager/max_heavy_columns", 1)
        make_table([255, 42])
        self._expect_statistics(0, 1, "x0,x1,zzz", [255, 255, 255], fetcher_mode="from_master")

        set("//sys/@config/chunk_manager/max_heavy_columns", 0)
        make_table([256, 42])
        self._expect_statistics(
            0,
            1,
            "x0,x1,zzz",
            [0, 0, 0],
            fetcher_mode="from_master",
            expected_legacy_data_weight=299,
        )
        self._expect_statistics(0, 1, "x0,x1,zzz", [256, 42, 0], fetcher_mode="fallback")

    @authors("dakovalkov")
    def test_get_table_columnar_statistics_multi(self):
        create("table", "//tmp/t")
        write_table("<append=%true>//tmp/t", [{"a": "x" * 10, "b": 42}, {"c": 1.2}])
        write_table("<append=%true>//tmp/t", [{"a": "x" * 20}, {"c": True}])
        write_table("<append=%true>//tmp/t", [{"b": None, "c": 0}, {"a": "x" * 100}])

        create("table", "//tmp/t2")
        write_table("<append=%true>//tmp/t2", [{"a": "x" * 100, "b": 42}, {"c": 1.2}])
        write_table("<append=%true>//tmp/t2", [{"a": "x" * 200}, {"c": True}])
        write_table("<append=%true>//tmp/t2", [{"b": None, "c": 0}, {"a": "x" * 1000}])

        paths = []
        lower_row_indices = []
        upper_row_indices = []
        all_columns = []
        all_expected_data_weights = []

        paths.append("//tmp/t")
        lower_row_indices.append(2)
        upper_row_indices.append(2)
        all_columns.append("a,b,c")
        all_expected_data_weights.append([0, 0, 0])

        paths.append("//tmp/t2")
        lower_row_indices.append(0)
        upper_row_indices.append(6)
        all_columns.append("a,b,c")
        all_expected_data_weights.append([1300, 8, 17])

        paths.append("//tmp/t")
        lower_row_indices.append(0)
        upper_row_indices.append(6)
        all_columns.append("a,c,x")
        all_expected_data_weights.append([130, 17, 0])

        paths.append("//tmp/t2")
        lower_row_indices.append(1)
        upper_row_indices.append(5)
        all_columns.append("a,b,c")
        all_expected_data_weights.append([1300, 8, 17])

        paths.append("//tmp/t")
        lower_row_indices.append(2)
        upper_row_indices.append(5)
        all_columns.append("a")
        all_expected_data_weights.append([120])

        paths.append("//tmp/t2")
        lower_row_indices.append(1)
        upper_row_indices.append(4)
        all_columns.append("")
        all_expected_data_weights.append([])

        self._expect_multi_statistics(
            paths,
            lower_row_indices,
            upper_row_indices,
            all_columns,
            all_expected_data_weights,
        )

    @authors("max42")
    def test_map_thin_column(self):
        create("table", "//tmp/t", attributes={"optimize_for": "scan"})
        create("table", "//tmp/d")
        for i in range(10):
            write_table(
                "<append=%true>//tmp/t",
                [{"a": "x" * 90, "b": "y" * 10} for j in range(100)],
            )
        assert get("//tmp/t/@data_weight") == 101 * 10 ** 3
        self._expect_statistics(0, 1000, "a,b", [90 * 10 ** 3, 10 * 10 ** 3])
        op = map(
            in_="//tmp/t{b}",
            out="//tmp/d",
            spec={"data_weight_per_job": 1000},
            command="echo '{a=1}'",
        )
        op.track()
        assert 9 <= get("//tmp/d/@chunk_count") <= 11

    @authors("max42")
    def test_sorted_merge_thin_column(self):
        create(
            "table",
            "//tmp/t",
            attributes={
                "optimize_for": "scan",
                "schema": [
                    {"name": "a", "sort_order": "ascending", "type": "string"},
                    {"name": "b", "type": "string"},
                ],
            },
        )
        create("table", "//tmp/d")
        for i in range(10):
            write_table(
                "<append=%true>//tmp/t",
                [{"a": "x" * 90, "b": "y" * 10} for j in range(100)],
            )
        assert get("//tmp/t/@data_weight") == 101 * 10 ** 3
        self._expect_statistics(0, 1000, "a,b", [90 * 10 ** 3, 10 * 10 ** 3])
        op = merge(in_="//tmp/t{b}", out="//tmp/d", spec={"data_weight_per_job": 1000})
        op.track()
        assert 9 <= get("//tmp/d/@chunk_count") <= 11

    @authors("max42")
    def test_map_thin_column_dynamic(self):
        sync_create_cells(1)
        self._create_simple_dynamic_table("//tmp/t")
        set("//tmp/t/@enable_dynamic_store_read", True)
        set("//tmp/t/@optimize_for", "scan")
        set("//tmp/t/@enable_compaction_and_partitioning", False)
        sync_mount_table("//tmp/t")
        for i in range(10):
            insert_rows(
                "//tmp/t",
                [{"key": j, "value": "y" * 80} for j in range(i * 100, (i + 1) * 100)],
            )
            sync_flush_table("//tmp/t")
        create("table", "//tmp/d")
        wait(lambda: get("//tmp/t/@chunk_count") == 12)
        assert get("//tmp/t/@data_weight") == (8 + (80 + 8) + 8) * 10 ** 3
        self._expect_statistics(
            None,
            None,
            "key,value",
            [8 * 10 ** 3, (80 + 8) * 10 ** 3],
            expected_timestamp_weight=(8 * 1000),
        )
        op = map(
            in_="//tmp/t{key}",
            out="//tmp/d",
            spec={"data_weight_per_job": 1600},
            command="echo '{a=1}'",
        )
        op.track()
        assert 9 <= get("//tmp/d/@chunk_count") <= 11

    @authors("max42")
    def test_empty_column_selector(self):
        create("table", "//tmp/t")
        create("table", "//tmp/d")
        write_table("<append=%true>//tmp/t", [{"a": "x" * 100}] * 100)
        op = merge(in_="//tmp/t{}", out="//tmp/d", spec={"data_weight_per_job": 10})
        op.track()
        assert 9 <= get("//tmp/d/@chunk_count") <= 11

    @authors("max42")
    def test_table_file_in_sandbox(self):
        create("table", "//tmp/t")
        s = "x" * 100
        for i in range(5):
            write_table("<append=%true>//tmp/t", [{"a": s, "b": s, "c": s, "d": s, "e": s}])
        with pytest.raises(YtError):
            vanilla(
                spec={
                    "tasks": {
                        "task": {
                            "job_count": 1,
                            "command": "exit",
                            "file_paths": ["<format=dsv>//tmp/t"],
                        },
                    },
                }
            )

        vanilla(
            spec={
                "tasks": {
                    "task": {
                        "job_count": 1,
                        "command": "exit",
                        "file_paths": ["<format=dsv>//tmp/t[#1:#4]"],
                    },
                },
            }
        )

        vanilla(
            spec={
                "tasks": {
                    "task": {
                        "job_count": 1,
                        "command": "exit",
                        "file_paths": ["<format=dsv>//tmp/t{b,c,d}"],
                    },
                },
            }
        )

    @authors("max42")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_dynamic_tables(self, optimize_for):
        sync_create_cells(1)
        self._create_simple_dynamic_table("//tmp/t")
        set("//tmp/t/@optimize_for", optimize_for)

        sync_mount_table("//tmp/t")
        rows = [{"key": i, "value": str(i) * 1000} for i in range(10)]
        insert_rows("//tmp/t", rows)
        sync_flush_table("//tmp/t")

        self._expect_statistics(None, None, "key,value", [80, 10080], expected_timestamp_weight=(8 * 10))

        rows = [{"key": i, "value": str(i // 2) * 1000} for i in range(10)]
        insert_rows("//tmp/t", rows)
        sync_flush_table("//tmp/t")

        self._expect_statistics(None, None, "key,value", [160, 20160], expected_timestamp_weight=(8 * 20))

        sync_compact_table("//tmp/t")

        self._expect_statistics(None, None, "key,value", [80, 20160], expected_timestamp_weight=(8 * 20))

        rows = [{"key": i} for i in range(10)]
        delete_rows("//tmp/t", rows)
        sync_flush_table("//tmp/t")

        self._expect_statistics(None, None, "key,value", [160, 20160], expected_timestamp_weight=(8 * 30))

        sync_compact_table("//tmp/t")

        self._expect_statistics(None, None, "key,value", [80, 20160], expected_timestamp_weight=(8 * 30))

    @authors("max42")
    def test_fetch_cancelation(self):
        create("table", "//tmp/t", attributes={"optimize_for": "scan"})
        create("table", "//tmp/d")
        for i in range(10):
            write_table(
                "<append=%true>//tmp/t",
                [{"a": "x" * 90, "b": "y" * 10} for j in range(100)],
            )

        # Restart controller agent to ensure our operation taking memory tagged statistics slot 0.
        with Restarter(self.Env, [CONTROLLER_AGENTS_SERVICE]):
            pass

        controller_agents = ls("//sys/controller_agents/instances")
        assert len(controller_agents) == 1
        controller_agent_orchid = "//sys/controller_agents/instances/{}/orchid/controller_agent".format(
            controller_agents[0]
        )

        op = map(
            track=False,
            in_="//tmp/t{b}",
            out="//tmp/d",
            spec={
                "data_weight_per_job": 1000,
                "testing": {"cancellation_stage": "columnar_statistics_fetch"},
            },
            command="echo '{a=1}'",
        )

        with raises_yt_error("Test operation failure"):
            op.track()

        def operation_disposed():
            entry = get(controller_agent_orchid + "/tagged_memory_statistics/0")
            if entry["operation_id"] != op.id:
                return False
            return not entry["alive"]

        wait(operation_disposed)

    @authors("gritukan")
    def test_estimated_input_statistics(self):
        create(
            "table",
            "//tmp/in",
            attributes={"optimize_for": "scan", "compression_codec": "none"},
        )
        create("table", "//tmp/out")
        for i in range(10):
            write_table(
                "<append=%true>//tmp/in",
                [{"a": "x" * 90, "b": "y" * 10} for j in range(100)],
            )

        op = map(in_="//tmp/in{b}", out="//tmp/out", command="echo '{a=1}'")
        op.track()

        statistics = get(op.get_path() + "/@progress/estimated_input_statistics")
        assert 10000 <= statistics["uncompressed_data_size"] <= 12000
        assert 10000 <= statistics["compressed_data_size"] <= 12000


##################################################################


class TestColumnarStatisticsRpcProxy(TestColumnarStatistics):
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True
