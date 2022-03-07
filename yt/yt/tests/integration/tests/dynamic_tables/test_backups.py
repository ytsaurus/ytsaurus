from yt_commands import (
    authors, get, insert_rows, select_rows, mount_table, reshard_table, sync_create_cells,
    remove, sync_mount_table, sync_flush_table, sync_freeze_table, sync_unmount_table,
    create_table_backup, restore_table_backup, raises_yt_error, update_nodes_dynamic_config,
    wait, start_transaction, commit_transaction, print_debug, lookup_rows,
    generate_timestamp, set, sync_compact_table, read_table, merge, create)

import yt_error_codes

from test_dynamic_tables import DynamicTablesBase

from yt.environment.helpers import assert_items_equal
from yt.common import YtResponseError
import yt.yson as yson
from time import time, sleep

import pytest

##################################################################


class EmptyDynamicStoreIdPoolException(Exception):
    pass

##################################################################


@authors("ifsmirnov")
class TestBackups(DynamicTablesBase):
    NUM_TEST_PARTITIONS = 2

    NUM_SCHEDULERS = 1

    def test_basic_backup(self):
        sync_create_cells(1)
        self._create_sorted_table("//tmp/t", dynamic_store_auto_flush_period=yson.YsonEntity())
        sync_mount_table("//tmp/t")
        rows = [{"key": 1, "value": "a"}]
        insert_rows("//tmp/t", rows)
        assert get("//tmp/t/@backup_state") == "none"

        create_table_backup(["//tmp/t", "//tmp/bak"])
        assert get("//tmp/bak/@tablet_backup_state") == "backup_completed"
        assert get("//tmp/bak/@backup_state") == "backup_completed"

        with raises_yt_error():
            restore_table_backup(["//tmp/bak", "//tmp/res"])

        sync_flush_table("//tmp/t")

        with raises_yt_error():
            mount_table("//tmp/bak")
        with raises_yt_error():
            reshard_table("//tmp/bak", [[], [1], [2]])

        restore_table_backup(["//tmp/bak", "//tmp/res"])
        assert get("//tmp/res/@tablet_backup_state") == "none"
        assert get("//tmp/res/@backup_state") == "restored_with_restrictions"
        sync_mount_table("//tmp/res")
        assert_items_equal(select_rows("* from [//tmp/res]"), rows)

    def test_checkpoint_timestamp_workflow(self):
        sync_create_cells(1)
        self._create_sorted_table("//tmp/t", dynamic_store_auto_flush_period=yson.YsonEntity())
        sync_mount_table("//tmp/t")
        rows = [{"key": 1, "value": "a"}]
        insert_rows("//tmp/t", rows)
        tablet_id = get("//tmp/t/@tablets/0/tablet_id")

        update_nodes_dynamic_config({
            "tablet_node": {
                "backup_manager": {
                    "checkpoint_feasibility_check_batch_period": 3000,
                }
            }
        })

        response = create_table_backup(
            ["//tmp/t", "//tmp/bak"],
            checkpoint_timestamp_delay=5000,
            return_response=True)

        def _get_backup_stage():
            return get("//sys/tablets/{}/orchid/backup_stage".format(tablet_id))

        wait(lambda: _get_backup_stage() != "none")
        wait(lambda: _get_backup_stage() == "timestamp_received")
        wait(lambda: _get_backup_stage() == "feasibility_confirmed")
        response.wait()
        assert response.is_ok()
        assert _get_backup_stage() == "none"

        assert get("//tmp/bak/@tablet_backup_state") == "backup_completed"
        assert get("//tmp/bak/@backup_state") == "backup_completed"

    def test_checkpoint_timestamp_rejected(self):
        sync_create_cells(1)

        self._create_sorted_table("//tmp/t", dynamic_store_auto_flush_period=yson.YsonEntity())
        sync_mount_table("//tmp/t")
        rows = [{"key": 1, "value": "a"}]
        insert_rows("//tmp/t", rows)

        create_table_backup(["//tmp/t", "//tmp/bak"])
        remove("//tmp/bak")

        update_nodes_dynamic_config({
            "tablet_node": {
                "backup_manager": {
                    "checkpoint_feasibility_check_batch_period": 3000,
                }
            }
        })

        with raises_yt_error(yt_error_codes.BackupCheckpoinRejected):
            create_table_backup(["//tmp/t", "//tmp/bak"], checkpoint_timestamp_delay=0)

    def _insert_into_multiple_tables(self, tables, rows):
        tx = start_transaction(type="tablet", verbose=False)
        print_debug("Inserting rows (Rows: {}, Tables: {}, TransactionId: {})".format(
            rows, tables, tx))
        for table in tables:
            insert_rows(table, rows, transaction_id=tx, verbose=False)
        commit_transaction(tx, verbose=False)
        generate_timestamp()

    def test_backup_multiple_tables(self):
        table_count = 3
        source_tables = ["//tmp/t_" + str(i) for i in range(table_count)]
        backup_tables = ["//tmp/bak_" + str(i) for i in range(table_count)]
        restored_tables = ["//tmp/res_" + str(i) for i in range(table_count)]

        sync_create_cells(table_count)

        for table in source_tables:
            self._create_sorted_table(
                table,
                dynamic_store_auto_flush_period=2000,
                dynamic_store_flush_period_splay=0)
            sync_mount_table(table)

        for i in range(10):
            self._insert_into_multiple_tables(source_tables, [{"key": i, "value": str(i)}])

        response = create_table_backup(
            *zip(source_tables, backup_tables),
            checkpoint_timestamp_delay=5000,
            return_response=True)

        i = 10
        start_time = time()
        while time() - start_time < 10:
            self._insert_into_multiple_tables(source_tables, [{"key": i, "value": str(i)}])
            i += 1

        response.wait()
        assert response.is_ok()

        for table in source_tables:
            sync_freeze_table(table)
        restore_table_backup(*zip(backup_tables, restored_tables))

        rowsets = []
        for table in restored_tables:
            assert get(table + "/@backup_state") == "restored_with_restrictions"
            sync_mount_table(table)
            rowsets.append(list(select_rows("* from [{}] order by key limit 1000000".format(table))))

        assert rowsets[0] == rowsets[1]
        assert rowsets[0] == rowsets[2]

    @pytest.mark.parametrize(
        ("merge_rows_on_flush", "flush_period", "auto_compaction_period"),
        [
            [True, 2000, yson.YsonEntity()],
            [True, yson.YsonEntity(), yson.YsonEntity()],
            [False, 2000, 1],
            [True, 2000, 1],
        ])
    def test_merge_rows_on_flush_and_compaction_disabled(self, merge_rows_on_flush, flush_period, auto_compaction_period):
        sync_create_cells(1)

        self._create_sorted_table(
            "//tmp/t",
            dynamic_store_auto_flush_period=flush_period,
            dynamic_store_flush_period_splay=0,
            merge_rows_on_flush=merge_rows_on_flush,
            auto_compaction_period=auto_compaction_period,
            min_data_ttl=0,
            min_data_versions=0)
        sync_mount_table("//tmp/t")

        self._create_sorted_table("//tmp/model")
        sync_mount_table("//tmp/model")

        for i in range(10):
            self._insert_into_multiple_tables(["//tmp/t", "//tmp/model"], [{"key": 1, "value": str(i)}])

        response = create_table_backup(
            ["//tmp/t", "//tmp/bak"],
            checkpoint_timestamp_delay=5000,
            return_response=True)

        i = 10
        start_time = time()
        while time() - start_time < 10:
            self._insert_into_multiple_tables(["//tmp/t", "//tmp/model"], [{"key": 1, "value": str(i)}])
            i += 1

        response.wait()
        assert response.is_ok()
        ts = get("//tmp/bak/@backup_checkpoint_timestamp")

        sync_freeze_table("//tmp/t")
        restore_table_backup(["//tmp/bak", "//tmp/res"])
        sync_mount_table("//tmp/res")

        expected = lookup_rows("//tmp/model", [{"key": 1}], timestamp=ts)
        actual = lookup_rows("//tmp/res", [{"key": 1}])
        assert expected == actual

    @pytest.mark.parametrize("in_memory_mode", ["none", "uncompressed"])
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_clip_timestamp_various_chunk_formats(self, optimize_for, in_memory_mode):
        cell_id = sync_create_cells(1)[0]
        tablet_node = get("#{}/@peers/0/address".format(cell_id))
        set("//sys/cluster_nodes/{}/@disable_write_sessions".format(tablet_node), True)

        self._create_sorted_table(
            "//tmp/t",
            dynamic_store_auto_flush_period=yson.YsonEntity(),
            in_memory_mode=in_memory_mode,
            optimize_for=optimize_for)
        sync_mount_table("//tmp/t")

        rows = [
            {"key": 1, "value": "foo"},
            {"key": 2, "value": "bar"},
            {"key": 2, "value": "baz"},
            {"key": 3, "value": "qux"},
        ]
        keys = [{"key": 1}, {"key": 2}, {"key": 3}]

        insert_rows("//tmp/t", [rows[0]])
        ts_after_first_row = generate_timestamp()
        insert_rows("//tmp/t", [rows[1]])

        ts_before = generate_timestamp()
        create_table_backup(["//tmp/t", "//tmp/bak"])
        ts_after = generate_timestamp()

        insert_rows("//tmp/t", [rows[2]])
        insert_rows("//tmp/t", [rows[3]])

        sync_freeze_table("//tmp/t")
        restore_table_backup(["//tmp/bak", "//tmp/res"])
        assert get("//tmp/res/@in_memory_mode") == in_memory_mode
        assert get("//tmp/res/@optimize_for") == optimize_for
        sync_mount_table("//tmp/res")
        wait(lambda: get("//tmp/res/@preload_state") == "complete")

        checkpoint_ts = get("//tmp/bak/@backup_checkpoint_timestamp")
        assert ts_before < checkpoint_ts < ts_after

        def _check_with_timestamp(ts, expected):
            assert lookup_rows("//tmp/res", keys, timestamp=ts) == expected
            assert_items_equal(list(select_rows("* from [//tmp/res]", timestamp=ts)), expected)
            assert read_table("<timestamp={}>//tmp/res".format(ts)) == expected
            merge(in_="<timestamp={}>//tmp/res".format(ts), out="//tmp/dump", mode="ordered")
            assert read_table("//tmp/dump") == expected

        def _check():
            assert lookup_rows("//tmp/res", keys) == rows[:2]
            assert_items_equal(list(select_rows("* from [//tmp/res]")), rows[:2])
            assert read_table("//tmp/res") == rows[:2]
            create("table", "//tmp/dump", force=True)
            merge(in_="//tmp/res", out="//tmp/dump", mode="ordered")
            assert read_table("//tmp/dump") == rows[:2]

            _check_with_timestamp(ts_after_first_row, rows[:1])
            _check_with_timestamp(generate_timestamp(), rows[:2])

        set("//tmp/res/@enable_data_node_lookup", True)
        sync_unmount_table("//tmp/res")
        sync_mount_table("//tmp/res")
        wait(lambda: get("//tmp/res/@preload_state") == "complete")

        # No need to run all heavy checks.
        assert lookup_rows("//tmp/res", keys) == rows[:2]
        assert lookup_rows("//tmp/res", keys, timestamp=ts_after_first_row) == rows[:1]
        assert lookup_rows("//tmp/res", keys, timestamp=generate_timestamp()) == rows[:2]

        set("//tmp/res/@enable_data_node_lookup", False)
        sync_unmount_table("//tmp/res")
        sync_mount_table("//tmp/res")
        wait(lambda: get("//tmp/res/@preload_state") == "complete")

        sync_compact_table("//tmp/res")
        _check()

    @pytest.mark.flaky(
        max_runs=5,
        rerun_filter=lambda err, *args: issubclass(err[0], EmptyDynamicStoreIdPoolException))
    def test_backup_multiple_tables_ordered(self):
        table_count = 3
        source_tables = ["//tmp/t_" + str(i) for i in range(table_count)]
        backup_tables = ["//tmp/bak_" + str(i) for i in range(table_count)]
        restored_tables = ["//tmp/res_" + str(i) for i in range(table_count)]

        sync_create_cells(table_count)

        for table in source_tables:
            self._create_ordered_table(
                table,
                dynamic_store_auto_flush_period=2000,
                dynamic_store_flush_period_splay=0,
                commit_ordering="strong")
            sync_mount_table(table)

        for i in range(10):
            tx = start_transaction(type="tablet")
            for table in source_tables:
                insert_rows(table, [{"key": i, "value": str(i)}], transaction_id=tx)
            commit_transaction(tx)

        response = create_table_backup(
            *zip(source_tables, backup_tables),
            checkpoint_timestamp_delay=5000,
            return_response=True)

        cur_time = time()
        for i in range(10, 1000000):
            if time() - cur_time > 10:
                break
            print_debug("Inserting {}".format(i))
            tx = start_transaction(type="tablet")
            for table in source_tables:
                insert_rows(table, [{"key": i, "value": str(i)}], transaction_id=tx)
            commit_transaction(tx)

        response.wait()

        # This kind of race is infrequent but possible.
        if not response.is_ok():
            error = YtResponseError(response.error())
            if error.contains_text("cannot perform backup cutoff due to empty dynamic store id pool"):
                raise EmptyDynamicStoreIdPoolException()
            raise error

        for table in source_tables:
            sync_freeze_table(table)
        restore_table_backup(*zip(backup_tables, restored_tables))

        rowsets = []
        for table in restored_tables:
            assert get(table + "/@backup_state") == "restored_with_restrictions"
            sync_mount_table(table)
            rowsets.append(list(select_rows("* from [{}] order by key limit 1000000".format(table))))

        assert rowsets[0] == rowsets[1]
        assert rowsets[0] == rowsets[2]

        for table in source_tables:
            assert len(list(select_rows("* from [{}]".format(table)))) > len(rowsets[0])

    def test_checkpoint_rejected_by_transaction(self):
        sync_create_cells(1)
        self._create_ordered_table("//tmp/t", commit_ordering="strong")
        sync_mount_table("//tmp/t")

        update_nodes_dynamic_config({
            "tablet_node": {
                "backup_manager": {
                    "checkpoint_feasibility_check_batch_period": 10000,
                }
            }
        })

        response = create_table_backup(
            ["//tmp/t", "//tmp/bak"],
            checkpoint_timestamp_delay=0,
            return_response=True)

        tablet_id = get("//tmp/t/@tablets/0/tablet_id")
        wait(lambda: get("//sys/tablets/{}/orchid/backup_stage".format(tablet_id))
                == "timestamp_received")

        # Wait for current timestamp to exceed checkpoint timestamp.
        sleep(2)
        insert_rows("//tmp/t", [{"key": 1, "value": "foo"}])

        wait(lambda: response.is_set())
        assert not response.is_ok()
        error = YtResponseError(response.error())
        assert error.contains_text(
            "Failed to confirm checkpoint timestamp in time due to a transaction with later timestamp")

##################################################################


@authors("ifsmirnov")
class TestBackupsMulticell(TestBackups):
    NUM_SECONDARY_MASTER_CELLS = 2
