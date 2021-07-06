from test_sorted_dynamic_tables import TestSortedDynamicTablesBase

from yt_env_setup import wait

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
    get_driver, execute_command, generate_uuid,
    AsyncLastCommittedTimestamp, MinTimestamp)

from yt_type_helpers import make_schema

import yt.yson as yson

from time import sleep, time

################################################################################


class TestCompactionPartitioning(TestSortedDynamicTablesBase):
    @authors("ifsmirnov")
    def test_partition_balancer_chunk_view(self):
        [cell_id] = sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        set("//tmp/t/@chunk_writer", {"block_size": 64})
        set("//tmp/t/@compression_codec", "none")
        set("//tmp/t/@tablet_balancer_config/enable_auto_reshard", False)
        set("//tmp/t/@enable_compaction_and_partitioning", False)
        set("//tmp/t/@max_partition_data_size", 640)
        set("//tmp/t/@desired_partition_data_size", 512)
        set("//tmp/t/@min_partition_data_size", 256)
        sync_mount_table("//tmp/t")

        insert_rows("//tmp/t", [{"key": i, "value": str(i)} for i in xrange(20)])
        sync_unmount_table("//tmp/t")
        sync_reshard_table("//tmp/t", [[], [1], [18]])
        sync_mount_table("//tmp/t")

        tablet_id = get("//tmp/t/@tablets/1/tablet_id")
        address = get_tablet_leader_address(tablet_id)
        orchid = self._find_tablet_orchid(address, tablet_id)
        assert len(orchid["partitions"]) == 1

        build_snapshot(cell_id=cell_id)

        peer = get("//sys/tablet_cells/{}/@peers/0/address".format(cell_id))
        set("//sys/cluster_nodes/{}/@banned".format(peer), True)
        wait(lambda: get("//sys/tablet_cells/{}/@health".format(cell_id)) == "good")

        set("//tmp/t/@enable_compaction_and_partitioning", True)
        remount_table("//tmp/t")
        address = get_tablet_leader_address(tablet_id)
        wait(lambda: len(self._find_tablet_orchid(address, tablet_id)["partitions"]) > 1)

    @authors("savrus")
    def test_partition_balancer(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        set("//tmp/t/@max_partition_data_size", 640)
        set("//tmp/t/@desired_partition_data_size", 512)
        set("//tmp/t/@min_partition_data_size", 256)
        set("//tmp/t/@compression_codec", "none")
        set("//tmp/t/@chunk_writer", {"block_size": 64})
        sync_mount_table("//tmp/t")

        tablet_id = get("//tmp/t/@tablets/0/tablet_id")
        address = get_tablet_leader_address(tablet_id)
        orchid = self._find_tablet_orchid(address, tablet_id)
        assert len(orchid["partitions"]) == 1

        insert_rows("//tmp/t", [{"key": i, "value": str(i)} for i in xrange(16)])
        sync_flush_table("//tmp/t")
        wait(lambda: len(self._find_tablet_orchid(address, tablet_id)["partitions"]) > 1)

    @authors("ifsmirnov")
    def test_partitioning_with_alter(self):
        # Creating two chunks with @eden=%false:
        # - [1]
        # - [1;1]
        # Two partitions should be created upon mount.
        sync_create_cells(1)
        schema = [
            {"name": "k1", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "int64"},
        ]
        self._create_simple_table("//tmp/t", schema=schema)
        set("//tmp/t/@min_partition_data_size", 1)

        # Create [1] chunk.
        sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", [{"k1": 1}])
        sync_unmount_table("//tmp/t")
        set("//tmp/t/@forced_compaction_revision", 1)
        chunk_id = get("//tmp/t/@chunk_ids/0")
        sync_mount_table("//tmp/t")
        wait(lambda: get("//tmp/t/@chunk_ids/0") != chunk_id)
        assert not get("#{}/@eden".format(get("//tmp/t/@chunk_ids/0")))

        # Create [1;1] chunk.
        sync_unmount_table("//tmp/t")
        schema = schema[:1] + [{"name": "k2", "type": "int64", "sort_order": "ascending"}] + schema[1:]
        alter_table("//tmp/t", schema=schema)
        sync_reshard_table("//tmp/t", [[], [1, 1]])
        sync_mount_table("//tmp/t", first_tablet_index=1, last_tablet_index=1)
        insert_rows("//tmp/t", [{"k1": 1, "k2": 1}])
        sync_unmount_table("//tmp/t")
        set("//tmp/t/@forced_compaction_revision", 1)
        chunk_id = get("//tmp/t/@chunk_ids/1")
        sync_mount_table("//tmp/t", first_tablet_index=1, last_tablet_index=1)
        wait(lambda: get("//tmp/t/@chunk_ids/1") != chunk_id)
        assert not get("#{}/@eden".format(get("//tmp/t/@chunk_ids/1")))

        sync_unmount_table("//tmp/t")
        set("//tmp/t/@enable_compaction_and_partitioning", False)
        sync_reshard_table("//tmp/t", [[]])
        sync_mount_table("//tmp/t")
        wait(lambda: get("//tmp/t/@tablet_statistics/partition_count") > 0)
        assert get("//tmp/t/@tablet_statistics/partition_count") == 2

        expected = [
            {"k1": 1L, "k2": yson.YsonEntity(), "value": yson.YsonEntity()},
            {"k1": 1L, "k2": 1L, "value": yson.YsonEntity()},
        ]

        assert list(lookup_rows("//tmp/t", [{"k1": 1}, {"k1": 1, "k2": 1}])) == expected
        assert list(select_rows("* from [//tmp/t] order by k1,k2 limit 100")) == expected

    @authors("akozhikhov")
    def test_overlapping_store_count(self):
        # Create 3 chunks [{2}, {3}], [{4}, {5}] and [{6}, {7}].
        # Then create two chunks in eden [{3}, {4}] and [{5}, {6}], which don't overlap.
        sync_create_cells(1)
        self._create_simple_table("//tmp/t", attributes={"enable_lsm_verbose_logging": True})

        sync_mount_table("//tmp/t")

        def _check(stores, overlaps):
            sync_mount_table("//tmp/t")
            tablet_id = get("//tmp/t/@tablets/0/tablet_id")
            address = get_tablet_leader_address(tablet_id)
            wait(lambda: stores == len(self._find_tablet_orchid(address, tablet_id)["eden"]["stores"]))
            wait(lambda: overlaps == get("//tmp/t/@tablet_statistics/overlapping_store_count"))
            sync_unmount_table("//tmp/t")

        _check(stores=1, overlaps=1)

        self._create_partitions(partition_count=3)

        _check(stores=1, overlaps=2)

        sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", [{"key": i} for i in xrange(3, 5)])
        sync_flush_table("//tmp/t")

        _check(stores=2, overlaps=3)

        sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", [{"key": i} for i in xrange(5, 7)])
        sync_flush_table("//tmp/t")

        _check(stores=3, overlaps=3)

    @authors("babenko")
    def test_store_rotation(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t")

        set("//tmp/t/@max_dynamic_store_row_count", 10)
        sync_mount_table("//tmp/t")

        tablet_id = get("//tmp/t/@tablets/0/tablet_id")
        address = get_tablet_leader_address(tablet_id)

        rows = [{"key": i, "value": str(i)} for i in xrange(10)]
        insert_rows("//tmp/t", rows)

        def check():
            tablet_data = self._find_tablet_orchid(address, tablet_id)
            return (
                len(tablet_data["eden"]["stores"]) == 1
                and len(tablet_data["partitions"]) == 1
                and len(tablet_data["partitions"][0]["stores"]) == 1
            )

        wait(lambda: check())

    @authors("akozhikhov")
    def test_small_chunks_partition_scenario(self):
        # Create three chunks and check number of partitions.
        sync_create_cells(1)
        schema = make_schema(
            [
                {"name": "k1", "type": "int64", "sort_order": "ascending"},
                {"name": "value", "type": "string"},
            ],
            unique_keys=True,
        )
        create("table", "//tmp/t", attributes={"schema": schema})

        for key in range(3):
            write_table("<append=%true>//tmp/t", [{"k1": key}])

        alter_table("//tmp/t", dynamic=True)

        def _check(expected_partitions):
            sync_mount_table("//tmp/t")
            wait(lambda: get("//tmp/t/@tablet_statistics/partition_count") > 0)
            assert get("//tmp/t/@tablet_statistics/partition_count") == expected_partitions
            sync_unmount_table("//tmp/t")

        set("//tmp/t/@enable_compaction_and_partitioning", False)
        _check(expected_partitions=1)
        set("//tmp/t/@min_partition_data_size", 1)
        _check(expected_partitions=3)

    @authors("akozhikhov")
    def test_partitioning_with_chunk_views(self):
        # Creating two chunks [{0}, {1}, {2}] and [{2}, {3}] and check whether they become partitioned.
        sync_create_cells(1)
        self._create_simple_table("//tmp/t", attributes={"enable_lsm_verbose_logging": True})

        self._create_partitions(partition_count=2, do_overlap=True)

        sync_mount_table("//tmp/t")
        wait(lambda: get("//tmp/t/@tablet_statistics/partition_count") > 0)
        assert get("//tmp/t/@tablet_statistics/partition_count") == 2
        sync_unmount_table("//tmp/t")

        assert len(get("//tmp/t/@chunk_ids")) == 2

        expected = [
            {"pivot_key": [], "min_key": [0], "upper_bound_key": [2]},
            {
                "pivot_key": [2],
                "min_key": [2],
                "upper_bound_key": [3, yson.YsonEntity()],
            },
        ]

        sync_mount_table(
            "//tmp/t",
        )
        tablet_id = get("//tmp/t/@tablets/0/tablet_id")
        address = get_tablet_leader_address(tablet_id)
        orchid = self._find_tablet_orchid(address, tablet_id)
        for partition_idx in range(len(orchid["partitions"])):
            partition = orchid["partitions"][partition_idx]
            assert partition["pivot_key"] == expected[partition_idx]["pivot_key"]
            assert len(partition["stores"]) == 1
            store_values = dict(partition["stores"].values()[0].iteritems())
            assert store_values["min_key"] == expected[partition_idx]["min_key"]
            assert store_values["upper_bound_key"] == expected[partition_idx]["upper_bound_key"]

    @authors("ifsmirnov")
    def test_mount_chunk_view_YT_12532(self):
        sync_create_cells(1)
        schema = make_schema(
            [
                {"name": "key", "type": "int64", "sort_order": "ascending"},
                {"name": "value", "type": "string"},
            ],
            unique_keys=True,
        )
        self._create_simple_table("//tmp/t", dynamic=False, schema=schema)

        # Chunks:     [0 1 2] [3 4 5] [6 7 8]
        # Tablet #1:     [       |       )

        write_table("<append=%true>//tmp/t", [{"key": i} for i in range(0, 3)])
        write_table("<append=%true>//tmp/t", [{"key": i} for i in range(3, 6)])
        write_table("<append=%true>//tmp/t", [{"key": i} for i in range(6, 9)])

        alter_table("//tmp/t", dynamic=True)
        set("//tmp/t/@min_partition_data_size", 1)
        sync_reshard_table("//tmp/t", [[], [1], [4], [7]])
        sync_reshard_table("//tmp/t", [[1]], first_tablet_index=1, last_tablet_index=2)
        sync_mount_table("//tmp/t")

        tablet_id = get("//tmp/t/@tablets/1/tablet_id")
        address = get_tablet_leader_address(tablet_id)
        orchid = self._find_tablet_orchid(address, tablet_id)
        partitions = orchid["partitions"]
        assert len(partitions) == 3
        assert partitions[0]["pivot_key"] == [1]
        assert partitions[1]["pivot_key"] == [3]
        assert partitions[2]["pivot_key"] == [6]

    @authors("ifsmirnov")
    def test_expired_partition(self):
        cell_id = sync_create_cells(1)[0]

        schema = make_schema(
            [
                {"name": "key", "type": "int64", "sort_order": "ascending"},
                {"name": "value", "type": "string"},
            ],
            unique_keys=True,
        )
        self._create_simple_table(
            "//tmp/t",
            schema=schema,
            dynamic=False,
            min_data_ttl=15000,
            max_data_ttl=15000,
            min_data_versions=0,
            max_data_versions=1,
            # NB: compressed size of each chunk is 38.
            min_partition_data_size=60,
            desired_partition_data_size=75,
            max_partition_data_size=90,
            min_compaction_store_count=10,
            max_compaction_store_count=10,
        )

        start_time = time()

        def _sleep_until(instant):
            now = time()
            if now - start_time < instant:
                sleep(instant - (now - start_time))

        rows = [{"key": i, "value": str(i)} for i in range(6)]

        # Expire at 15.
        write_table("<append=%true>//tmp/t", [rows[0]])
        write_table("<append=%true>//tmp/t", [rows[1]])
        _sleep_until(5)
        # Expire at 20.
        write_table("<append=%true>//tmp/t", [rows[2]])
        write_table("<append=%true>//tmp/t", [rows[3]])
        _sleep_until(10)
        # Expire at 25.
        write_table("<append=%true>//tmp/t", [rows[4]])
        write_table("<append=%true>//tmp/t", [rows[5]])

        alter_table("//tmp/t", dynamic=True)
        sync_mount_table("//tmp/t")
        tablet_id = get("//tmp/t/@tablets/0/tablet_id")
        address = get_tablet_leader_address(tablet_id)

        def _get_partitions():
            return get(
                "//sys/cluster_nodes/{}/orchid/tablet_cells/{}/tablets/{}/partitions".format(
                    address, cell_id, tablet_id
                )
            )

        expected_partitions = [[2, 2, 2], [2, 2], [2], []]
        expected_values = [rows, rows[2:], rows[4:], []]

        def _check(expected, actual):
            while expected and expected[0] != actual:
                expected.pop(0)
            if not expected:
                assert False

        for deadline in (0, 18, 23):
            _sleep_until(deadline)
            # We may encounter an empty partition if its chunks were dropped
            # but the partition itself has not yet been split.
            _check(
                expected_partitions,
                [len(x["stores"]) for x in _get_partitions() if x["stores"]],
            )
            _check(expected_values, sorted(list(select_rows("* from [//tmp/t]"))))


################################################################################


class TestCompactionPartitioningMulticell(TestCompactionPartitioning):
    NUM_SECONDARY_MASTER_CELLS = 2
