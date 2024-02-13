from yt_commands import (
    authors, get, insert_rows, select_rows, mount_table, sync_reshard_table, sync_create_cells,
    remove, exists, sync_mount_table, sync_flush_table, sync_freeze_table, sync_unmount_table,
    create_table_backup, restore_table_backup, raises_yt_error, update_nodes_dynamic_config,
    wait, start_transaction, commit_transaction, print_debug, lookup_rows,
    generate_timestamp, set, sync_compact_table, read_table, merge, create,
    get_driver, sync_enable_table_replica, create_table_replica, sync_disable_table_replica,
    sync_alter_table_replica_mode, remount_table, get_tablet_infos, alter_table_replica,
    write_table, remote_copy, alter_table, copy, move, delete_rows, disable_write_sessions_on_node,
    create_account)

import yt_error_codes

from yt_dynamic_tables_base import DynamicTablesBase
from .test_replicated_dynamic_tables import TestReplicatedDynamicTablesBase

from yt.environment.helpers import assert_items_equal
from yt.test_helpers import are_items_equal
from yt.common import YtResponseError
import yt.yson as yson
from time import time, sleep
from collections import Counter
from random import sample, randint
import builtins

import pytest

##################################################################


class BackupKnowinglyFailedException(Exception):
    pass

##################################################################


@authors("ifsmirnov")
class TestBackups(DynamicTablesBase):
    NUM_TEST_PARTITIONS = 3

    NUM_SCHEDULERS = 1

    ENABLE_BULK_INSERT = True

    def test_basic_backup(self):
        sync_create_cells(1)
        self._create_sorted_table("//tmp/t", dynamic_store_auto_flush_period=yson.YsonEntity())
        sync_mount_table("//tmp/t")
        rows = [{"key": 1, "value": "a"}]
        insert_rows("//tmp/t", rows)
        assert get("//tmp/t/@backup_state") == "none"

        set("//tmp/t/@mount_config/enable_store_flush", False)
        remount_table("//tmp/t")

        create_table_backup(["//tmp/t", "//tmp/bak"])
        assert get("//tmp/bak/@tablet_backup_state") == "backup_completed"
        assert get("//tmp/bak/@backup_state") == "backup_completed"

        with raises_yt_error():
            restore_table_backup(["//tmp/bak", "//tmp/res"])

        set("//tmp/t/@mount_config/enable_store_flush", True)
        remount_table("//tmp/t")
        sync_flush_table("//tmp/t")

        with raises_yt_error():
            mount_table("//tmp/bak")
        with raises_yt_error():
            sync_reshard_table("//tmp/bak", [[], [1], [2]])

        restore_table_backup(["//tmp/bak", "//tmp/res"])
        assert get("//tmp/res/@tablet_backup_state") == "none"
        assert get("//tmp/res/@backup_state") == "none"
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
        wait(lambda: _get_backup_stage() == "none")

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

        with raises_yt_error(yt_error_codes.BackupCheckpointRejected):
            create_table_backup(["//tmp/t", "//tmp/bak"], checkpoint_timestamp_delay=0)

    def _insert_into_multiple_tables(self, tables, rows):
        tx = start_transaction(type="tablet", verbose=False)
        print_debug("Inserting rows (Rows: {}, Tables: {}, TransactionId: {})".format(
            rows, tables, tx))
        for table in tables:
            insert_rows(table, rows, transaction_id=tx, verbose=False)
        commit_transaction(tx, verbose=False)

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
            assert get(table + "/@backup_state") == "none"
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
        disable_write_sessions_on_node(tablet_node, "test clip timestamp various chunk formats")

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
        rerun_filter=lambda err, *args: issubclass(err[0], BackupKnowinglyFailedException))
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
                raise BackupKnowinglyFailedException()
            raise error

        for table in source_tables:
            sync_freeze_table(table)
        restore_table_backup(*zip(backup_tables, restored_tables))

        rowsets = []
        for table in restored_tables:
            assert get(table + "/@backup_state") == "none"
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
        wait(lambda: get("//sys/tablets/{}/orchid/backup_stage".format(tablet_id)) == "timestamp_received")

        # Wait for current timestamp to exceed checkpoint timestamp.
        sleep(2)
        insert_rows("//tmp/t", [{"key": 1, "value": "foo"}])

        wait(lambda: response.is_set())
        assert not response.is_ok()
        error = YtResponseError(response.error())
        assert error.contains_text(
            "Failed to confirm checkpoint timestamp in time due to a transaction with later timestamp")

    @authors("akozhikhov")
    def test_backup_hunks_simple(self):
        schema = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "string", "max_inline_hunk_size": 1},
        ]

        sync_create_cells(1)
        self._create_sorted_table("//tmp/t", schema=schema, dynamic_store_auto_flush_period=yson.YsonEntity())
        sync_mount_table("//tmp/t")
        rows = [{"key": 1, "value": "aa"}]
        insert_rows("//tmp/t", rows)
        sync_flush_table("//tmp/t")
        src_hunk_chunk_ids = self._get_hunk_chunk_ids("//tmp/t")
        assert len(src_hunk_chunk_ids) == 1

        create_table_backup(["//tmp/t", "//tmp/bak"])
        assert get("//tmp/bak/@tablet_backup_state") == "backup_completed"
        assert get("//tmp/bak/@backup_state") == "backup_completed"

        restore_table_backup(["//tmp/bak", "//tmp/res"])
        assert get("//tmp/res/@tablet_backup_state") == "none"
        assert get("//tmp/res/@backup_state") == "none"
        sync_mount_table("//tmp/res")
        assert_items_equal(select_rows("* from [//tmp/res]"), rows)
        dst_hunk_chunk_ids = self._get_hunk_chunk_ids("//tmp/res")
        assert src_hunk_chunk_ids == dst_hunk_chunk_ids

    @authors("akozhikhov")
    @pytest.mark.parametrize("remove_backup_first", [False, True])
    @pytest.mark.parametrize("erasure", [False, True])
    def test_backup_hunks_restore_after_flush(self, remove_backup_first, erasure):
        schema = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "string", "max_inline_hunk_size": 1},
        ]

        sync_create_cells(1)
        self._create_sorted_table("//tmp/t",
                                  schema=schema,
                                  dynamic_store_auto_flush_period=yson.YsonEntity(),
                                  enable_compaction_and_partitioning=False)
        if erasure:
            set("//tmp/t/@erasure_codec", "isa_reed_solomon_6_3")
            set("//tmp/t/@hunk_erasure_codec", "isa_reed_solomon_6_3")
        sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", [{"key": 1, "value": "aa"}])
        sync_flush_table("//tmp/t")
        hunk_chunk_id_1 = self._get_hunk_chunk_ids("//tmp/t")
        assert len(hunk_chunk_id_1) == 1
        hunk_chunk_id_1 = hunk_chunk_id_1[0]

        set("//tmp/t/@mount_config/enable_store_flush", False)
        remount_table("//tmp/t")

        insert_rows("//tmp/t", [{"key": 2, "value": "bb"}])

        create_table_backup(["//tmp/t", "//tmp/bak"])
        assert get("//tmp/bak/@tablet_backup_state") == "backup_completed"
        assert get("//tmp/bak/@backup_state") == "backup_completed"

        set("//tmp/t/@mount_config/enable_store_flush", True)
        remount_table("//tmp/t")
        sync_flush_table("//tmp/t")
        hunk_chunk_id_2 = [hunk_chunk_id for hunk_chunk_id in self._get_hunk_chunk_ids("//tmp/t") if hunk_chunk_id != hunk_chunk_id_1]
        assert len(hunk_chunk_id_2) == 1
        hunk_chunk_id_2 = hunk_chunk_id_2[0]

        insert_rows("//tmp/t", [{"key": 3, "value": "cc"}])
        sync_flush_table("//tmp/t")
        hunk_chunk_id_3 = [hunk_chunk_id for hunk_chunk_id in self._get_hunk_chunk_ids("//tmp/t") if hunk_chunk_id not in [hunk_chunk_id_1, hunk_chunk_id_2]]
        assert len(hunk_chunk_id_3) == 1
        hunk_chunk_id_3 = hunk_chunk_id_3[0]

        sync_unmount_table("//tmp/t")
        remove("//tmp/t")
        wait(lambda: not exists("#{}".format(hunk_chunk_id_3)))
        assert exists("#{}".format(hunk_chunk_id_2))
        assert exists("#{}".format(hunk_chunk_id_1))

        restore_table_backup(["//tmp/bak", "//tmp/res"])
        assert get("//tmp/res/@tablet_backup_state") == "none"
        assert get("//tmp/res/@backup_state") == "none"
        sync_mount_table("//tmp/res")
        assert_items_equal(select_rows("* from [//tmp/res]"), [{"key": 1, "value": "aa"}, {"key": 2, "value": "bb"}])
        dst_hunk_chunk_ids = self._get_hunk_chunk_ids("//tmp/res")
        assert [hunk_chunk_id_1, hunk_chunk_id_2] == dst_hunk_chunk_ids

        if remove_backup_first:
            remove("//tmp/bak")
            sleep(5)
            assert exists("#{}".format(hunk_chunk_id_2))
            assert exists("#{}".format(hunk_chunk_id_1))
            assert_items_equal(select_rows("* from [//tmp/res]"), [{"key": 1, "value": "aa"}, {"key": 2, "value": "bb"}])

            set("//tmp/res/@mount_config/enable_store_flush", True)
            remount_table("//tmp/res")
            sync_unmount_table("//tmp/res")
            remove("//tmp/res")
        else:
            set("//tmp/res/@mount_config/enable_store_flush", True)
            remount_table("//tmp/res")
            sync_unmount_table("//tmp/res")
            remove("//tmp/res")
            sleep(5)
            assert exists("#{}".format(hunk_chunk_id_2))
            assert exists("#{}".format(hunk_chunk_id_1))

            remove("//tmp/bak")

        wait(lambda: not exists("#{}".format(hunk_chunk_id_2)))
        wait(lambda: not exists("#{}".format(hunk_chunk_id_1)))

    def test_ordered_at_least(self):
        tablet_count = 10
        sync_create_cells(tablet_count)
        schema = [
            {"name": "$timestamp", "type": "uint64"},
            {"name": "key", "type": "int64"},
            {"name": "value", "type": "string"},
        ]
        self._create_ordered_table("//tmp/t", tablet_count=tablet_count, schema=schema)
        sync_mount_table("//tmp/t")

        for i in range(10):
            indexes = sorted(sample(range(tablet_count), randint(1, tablet_count)))
            insert_rows("//tmp/t", [{"key": i, "value": str(i), "$tablet_index": j} for j in indexes])

        response = create_table_backup(
            ["//tmp/t", "//tmp/bak", {"ordered_mode": "at_least"}],
            checkpoint_timestamp_delay=5000,
            return_response=True)

        write_responses = []
        cur_time = time()
        for i in range(10, 10000):
            if time() - cur_time > 4:
                break
            indexes = sorted(sample(range(tablet_count), randint(1, tablet_count)))
            write_responses.append(insert_rows(
                "//tmp/t",
                [{"key": i, "value": str(i), "$tablet_index": j} for j in indexes],
                return_response=True))
            # Our poor driver is so busy writing rows that cannot even finish its backup tasks.
            # Give it a rest.
            if i % 5 == 0:
                sleep(0.1)

        for rsp in write_responses:
            rsp.wait()
            if not rsp.is_ok():
                raise YtResponseError(rsp.error())

        response.wait()
        if not response.is_ok():
            raise YtResponseError(response.error())

        checkpoint_ts = get("//tmp/bak/@backup_checkpoint_timestamp")
        required = [[] for i in range(tablet_count)]
        for row in select_rows("* from[//tmp/t]", verbose=False):
            if row["$timestamp"] <= checkpoint_ts:
                required[row["$tablet_index"]].append(row["key"])

        sync_freeze_table("//tmp/t")
        restore_table_backup(["//tmp/bak", "//tmp/res"])
        sync_mount_table("//tmp/res")

        actual = [[] for i in range(tablet_count)]
        rows = select_rows("* from [//tmp/res]", verbose=False)
        for row in rows:
            actual[row["$tablet_index"]].append(row["key"])

        for r, a in zip(required, actual):
            r = builtins.set(r)
            a = builtins.set(a)
            if not r.issubset(a):
                print_debug(r)
                print_debug(a)
                assert r.issubset(a)

    @pytest.mark.parametrize("empty", [True, False])
    @pytest.mark.parametrize("frozen", [True, False])
    def test_unmounted(self, empty, frozen):
        sync_create_cells(1)
        self._create_ordered_table("//tmp/ordered")
        self._create_sorted_table("//tmp/sorted")

        if not empty:
            for table in "//tmp/ordered", "//tmp/sorted":
                sync_mount_table(table)
                insert_rows(table, [{"key": 1}])
                sync_unmount_table(table)

        if frozen:
            for table in "//tmp/ordered", "//tmp/sorted":
                sync_mount_table(table, freeze=True)

        create_table_backup(
            ["//tmp/sorted", "//tmp/sorted_bak"],
            ["//tmp/ordered", "//tmp/ordered_bak", {"ordered_mode": "at_least"}])
        restore_table_backup(
            ["//tmp/sorted_bak", "//tmp/sorted_res"],
            ["//tmp/ordered_bak", "//tmp/ordered_res"])

        for table in "//tmp/ordered_res", "//tmp/sorted_res":
            sync_mount_table(table)
            assert len(list(read_table(table))) == (0 if empty else 1)
            assert len(list(select_rows("* from [{}]".format(table)))) == (0 if empty else 1)

    def test_cannot_set_enable_dynamic_store_read(self):
        sync_create_cells(1)
        self._create_sorted_table("//tmp/t")
        sync_mount_table("//tmp/t")
        create_table_backup(["//tmp/t", "//tmp/bak"], checkpoint_timestamp_delay=2000)

        with raises_yt_error():
            set("//tmp/bak/@enable_dynamic_store_read", True)
        with raises_yt_error():
            set("//tmp/bak/@enable_dynamic_store_read", False)

    def test_bulk_insert_into_backup_table(self):
        sync_create_cells(1)
        self._create_sorted_table("//tmp/t")
        sync_mount_table("//tmp/t")
        normal_rows = [{"key": 1, "value": "foo"}]
        bulk_rows = [{"key": 2, "value": "bar"}]
        extra_rows = [{"key": 3, "value": "baz"}]
        insert_rows("//tmp/t", normal_rows)
        create_table_backup(["//tmp/t", "//tmp/bak"], checkpoint_timestamp_delay=2000)
        insert_rows("//tmp/t", extra_rows)
        sync_freeze_table("//tmp/t")

        with raises_yt_error():
            remote_copy(
                in_="//tmp/t",
                out="//tmp/bak",
                spec={"cluster_name": "primary"})

        restore_table_backup(["//tmp/bak", "//tmp/res"])
        sync_mount_table("//tmp/res")

        create("table", "//tmp/data")
        write_table("//tmp/data", bulk_rows)
        merge(in_="//tmp/data", out="<append=%true>//tmp/res", mode="ordered")

        assert_items_equal(list(select_rows("* from [//tmp/res]")), normal_rows + bulk_rows)
        assert read_table("//tmp/res") == normal_rows + bulk_rows
        assert_items_equal(list(select_rows("* from [//tmp/t]")), normal_rows + extra_rows)
        assert read_table("//tmp/t") == normal_rows + extra_rows

    @pytest.mark.parametrize("sorted", [True, False])
    @pytest.mark.parametrize("empty", [True, False])
    def test_frozen_tables(self, sorted, empty):
        sync_create_cells(2)
        if sorted:
            self._create_sorted_table("//tmp/t", pivot_keys=[[], [1]])
        else:
            self._create_ordered_table("//tmp/t", tablet_count=2, commit_ordering="strong")

        sync_mount_table("//tmp/t")

        rows = [{"key": 0, "value": "0"}, {"key": 1, "value": "1"}]
        if not sorted:
            for row in rows:
                row["$tablet_index"] = row["key"]

        if not empty:
            insert_rows("//tmp/t", rows)

        tablet_ids = [t["tablet_id"] for t in get("//tmp/t/@tablets")]
        sync_freeze_table("//tmp/t", first_tablet_index=0, last_tablet_index=0)

        response = create_table_backup(
            ["//tmp/t", "//tmp/bak"],
            checkpoint_timestamp_delay=5000,
            return_response=True)

        def _get_backup_stage(index):
            return get("//sys/tablets/{}/orchid/backup_stage".format(tablet_ids[index]))

        wait(lambda: _get_backup_stage(0) == "feasibility_confirmed")
        wait(lambda: _get_backup_stage(1) == "feasibility_confirmed")

        response.wait()
        assert response.is_ok()
        assert get("//tmp/bak/@tablet_backup_state") == "backup_completed"
        assert get("//tmp/bak/@backup_state") == "backup_completed"

    def test_static_table(self):
        create("table", "//tmp/t")
        with raises_yt_error():
            create_table_backup(["//tmp/t", "//tmp/bak"])

    def test_alter_to_static(self):
        sync_create_cells(1)
        self._create_ordered_table("//tmp/t")
        sync_mount_table("//tmp/t")
        rows = [{"key": i, "value": str(i)} for i in range(100)]
        insert_rows("//tmp/t", rows[:50])
        create_table_backup(
            ["//tmp/t", "//tmp/bak", {"ordered_mode": "at_least"}],
            checkpoint_timestamp_delay=2000)
        insert_rows("//tmp/t", rows[50:])
        sync_flush_table("//tmp/t")

        with raises_yt_error():
            alter_table("//tmp/bak", dynamic=False)

        restore_table_backup(["//tmp/bak", "//tmp/res"])
        alter_table("//tmp/res", dynamic=False)
        assert read_table("//tmp/res") == rows[:50]

    @pytest.mark.parametrize("sorted", [True, False])
    def test_clone_backup(self, sorted):
        sync_create_cells(1)
        if sorted:
            self._create_sorted_table("//tmp/t")
        else:
            self._create_ordered_table(
                "//tmp/t",
                replication_factor=10,
                chunk_writer={"upload_replication_factor": 10})
        sync_mount_table("//tmp/t")

        rows = [{"key": i, "value": str(i)} for i in range(100)]
        insert_rows("//tmp/t", rows[:50])
        create_table_backup(
            ["//tmp/t", "//tmp/bak", {"ordered_mode": "at_least"}],
            checkpoint_timestamp_delay=2000)
        insert_rows("//tmp/t", rows[50:])

        copy("//tmp/bak", "//tmp/bak_copy")
        move("//tmp/bak", "//tmp/bak_move")

        assert get("//tmp/bak_copy/@backup_state") == "backup_completed"
        assert get("//tmp/bak_move/@backup_state") == "backup_completed"
        assert get("//tmp/bak_copy/@tablet_backup_state") == "backup_completed"
        assert get("//tmp/bak_move/@tablet_backup_state") == "backup_completed"
        assert get("//tmp/bak_copy/@backup_checkpoint_timestamp") > 0
        assert get("//tmp/bak_move/@backup_checkpoint_timestamp") > 0

        if not sorted:
            set("//tmp/t/@chunk_writer/upload_replication_factor", 1)
            remount_table("//tmp/t")
        sync_freeze_table("//tmp/t")

        for iter in range(2):
            restore_table_backup(["//tmp/bak_copy", "//tmp/res_copy"])
            assert read_table("//tmp/res_copy") == rows[:50]
            if not sorted:
                set("//tmp/res_copy/@chunk_writer/upload_replication_factor", 1)
            sync_mount_table("//tmp/res_copy")
            assert_items_equal(select_rows("key, value from [//tmp/res_copy]"), rows[:50])
            if sorted:
                set("//tmp/res_copy/@min_data_ttl", 0)
                remount_table("//tmp/res_copy")
                delete_rows("//tmp/res_copy", [{"key": k["key"]} for k in rows[:50]])
                sync_flush_table("//tmp/res_copy")
                sync_compact_table("//tmp/res_copy")
                assert get("//tmp/res_copy/@chunk_count") == 2  # dynamic stores
            else:
                insert_rows("//tmp/res_copy", [{"key": 1234, "value": "barbarbar"}])
                sync_flush_table("//tmp/res_copy")

            restore_table_backup(["//tmp/bak_move", "//tmp/res_move"])
            assert read_table("//tmp/res_move") == rows[:50]
            sync_mount_table("//tmp/res_move")
            assert_items_equal(select_rows("key, value from [//tmp/res_move]"), rows[:50])
            sync_unmount_table("//tmp/res_move")

            move("//tmp/res_move", "//tmp/res_move2")
            assert read_table("//tmp/res_move2") == rows[:50]
            sync_mount_table("//tmp/res_move2")
            assert_items_equal(select_rows("key, value from [//tmp/res_move2]"), rows[:50])

            remove("//tmp/res_copy")
            remove("//tmp/res_move2")

    def test_preserve_account_on_backup_restore(self):
        original_account = "test_original_account"
        backup_account = "test_backup_account"
        create_account(original_account)
        create_account(backup_account)
        set(f"//sys/accounts/{original_account}/@resource_limits/tablet_count", 5)
        set(f"//sys/accounts/{backup_account}/@resource_limits/tablet_count", 5)

        sync_create_cells(1)
        self._create_sorted_table("//tmp/t", account=original_account)

        create_table_backup(
            ["//tmp/t", "//tmp/bak"],
            checkpoint_timestamp_delay=3000,
            preserve_account=True)

        assert get("//tmp/bak/@account") == original_account
        set("//tmp/bak/@account", backup_account)

        restore_table_backup(
            ["//tmp/bak", "//tmp/res"],
            mount=False,
            preserve_account=True)

        assert get("//tmp/res/@account") == backup_account

##################################################################


@authors("ifsmirnov")
class TestReplicatedTableBackups(TestReplicatedDynamicTablesBase):
    NUM_SCHEDULERS = 1
    ENABLE_BULK_INSERT = True

    def _create_tables(self, replica_modes, mount=True, **attributes):
        self._create_replicated_table("//tmp/t", mount=mount, **attributes)
        for i, mode in enumerate(replica_modes):
            replica_path = "//tmp/r" + str(i + 1)
            replica_id = create_table_replica(
                "//tmp/t",
                self.REPLICA_CLUSTER_NAME,
                replica_path,
                mode=mode)
            self._create_replica_table(replica_path, replica_id, mount=mount, **attributes)
            sync_enable_table_replica(replica_id)

    def _make_backup_manifest(self, table_count_or_list):
        if type(table_count_or_list) is int:
            table_list = range(1, table_count_or_list + 1)
        else:
            table_list = table_count_or_list
        return {
            "primary": [("//tmp/t", "//tmp/bak")],
            self.REPLICA_CLUSTER_NAME: [
                ("//tmp/r" + str(i), "//tmp/bak" + str(i))
                for i in table_list
            ]
        }

    def _make_restore_manifest(self, table_count_or_list):
        if type(table_count_or_list) is int:
            table_list = range(1, table_count_or_list + 1)
        else:
            table_list = table_count_or_list
        return {
            "primary": [("//tmp/bak", "//tmp/res")],
            self.REPLICA_CLUSTER_NAME: [
                ("//tmp/bak" + str(i), "//tmp/res" + str(i))
                for i in table_list
            ]
        }

    def _get_replica_info(self, tablet_info, replica_id):
        for replica_info in tablet_info["replica_infos"]:
            if replica_info["replica_id"] == replica_id:
                return replica_info
        assert False

    @pytest.mark.flaky(max_runs=5)
    @pytest.mark.parametrize("driver_cluster", ["primary", "remote_0"])
    def test_replicated_tables_backup(self, driver_cluster):
        self._create_cells()
        self._create_tables(["sync", "async"])

        for i in range(10):
            insert_rows("//tmp/t", [{"key": i, "value1": str(i)}])

        response = create_table_backup(
            self._make_backup_manifest(2),
            return_response=True,
            checkpoint_timestamp_delay=3000,
            driver=get_driver(cluster=driver_cluster))

        cur_time = time()
        for i in range(10, 50000):
            if time() - cur_time > 6:
                break
            insert_rows("//tmp/t", [{"key": i, "value1": str(i)}])

        response.wait()

        if not response.is_ok():
            raise YtResponseError(response.error())

        assert get("//tmp/bak/@type") == "replicated_table"
        assert get("//tmp/bak/@backup_state") == "backup_completed"
        replicas = get("//tmp/bak/@replicas")

        assert get("//tmp/bak1/@backup_state", driver=self.replica_driver) == "backup_completed"
        replica_id = get("//tmp/bak1/@upstream_replica_id", driver=self.replica_driver)
        assert replicas[replica_id]["replica_path"] == "//tmp/bak1"
        assert replicas[replica_id]["mode"] == "sync"
        assert replicas[replica_id]["state"] == "disabled"

        assert get("//tmp/bak2/@backup_state", driver=self.replica_driver) == "backup_completed"
        replica_id = get("//tmp/bak2/@upstream_replica_id", driver=self.replica_driver)
        assert replicas[replica_id]["replica_path"] == "//tmp/bak2"
        assert replicas[replica_id]["mode"] == "async"
        assert replicas[replica_id]["state"] == "disabled"

        sync_flush_table("//tmp/t")
        sync_flush_table("//tmp/r1", driver=self.replica_driver)
        sync_flush_table("//tmp/r2", driver=self.replica_driver)

        restore_table_backup(
            self._make_restore_manifest(2),
            driver=get_driver(cluster=driver_cluster))

        assert get("//tmp/res/@type") == "replicated_table"
        assert get("//tmp/res/@backup_state") == "none"
        replicas = get("//tmp/res/@replicas")

        assert get("//tmp/res1/@backup_state", driver=self.replica_driver) == "none"
        replica_id = get("//tmp/res1/@upstream_replica_id", driver=self.replica_driver)
        assert replicas[replica_id]["replica_path"] == "//tmp/res1"
        assert replicas[replica_id]["mode"] == "sync"
        assert replicas[replica_id]["state"] == "disabled"
        sync_enable_table_replica(replica_id)

        assert get("//tmp/res2/@backup_state", driver=self.replica_driver) == "none"
        replica_id = get("//tmp/res2/@upstream_replica_id", driver=self.replica_driver)
        assert replicas[replica_id]["replica_path"] == "//tmp/res2"
        assert replicas[replica_id]["mode"] == "async"
        assert replicas[replica_id]["state"] == "disabled"
        sync_enable_table_replica(replica_id)
        async_replica_id = replica_id

        sync_mount_table("//tmp/res")
        sync_mount_table("//tmp/res1", driver=self.replica_driver)
        sync_mount_table("//tmp/res2", driver=self.replica_driver)

        row_count = get("//tmp/res/@tablets/0/flushed_row_count")
        sync_rows = list(select_rows(
            "* from [//tmp/res1] order by key limit 10000",
            driver=self.replica_driver))
        wait(lambda: get(
            "#{}/@tablets/0/current_replication_row_index".format(async_replica_id)) == row_count)

        async_rows = list(select_rows(
            "* from [//tmp/res2] order by key limit 10000",
            driver=self.replica_driver))

        assert sync_rows == async_rows
        assert list(select_rows("* from [//tmp/res] order by key limit 10000")) == sync_rows

    def test_cannot_modify_backup_table_replicas(self):
        self._create_cells()
        self._create_tables(["sync"])
        create_table_backup(
            self._make_backup_manifest(1),
            checkpoint_timestamp_delay=2000)

        with raises_yt_error():
            create_table_replica("//tmp/bak", self.REPLICA_CLUSTER_NAME, "//tmp/aaa")

        replica_id = get("//tmp/bak1/@upstream_replica_id", driver=self.replica_driver)
        with raises_yt_error():
            alter_table_replica(replica_id, mode="async")

    def test_cannot_modify_replica_during_backup(self):
        self._create_cells()
        self._create_tables(["sync"])
        replica_id = get("//tmp/r1/@upstream_replica_id", driver=self.replica_driver)
        response = create_table_backup(
            self._make_backup_manifest(1),
            checkpoint_timestamp_delay=5000,
            return_response=True)
        wait(lambda: get("//tmp/t/@tablet_backup_state") == "checkpoint_requested")

        with raises_yt_error():
            alter_table_replica(replica_id, mode="async")

        with raises_yt_error():
            remove("#" + replica_id)

        response.wait()
        if not response.is_ok():
            raise YtResponseError(response.error())

        alter_table_replica(replica_id, mode="async")

    def test_disabled_replica(self):
        self._create_cells()
        self._create_tables(["async"])
        replica_id = get("//tmp/r1/@upstream_replica_id", driver=self.replica_driver)
        set("//tmp/t/@replication_throttler", {"limit": 2000})
        remount_table("//tmp/t")

        rows = [{"key": i, "value1": "A" * 1000, "value2": i * 100} for i in range(15)]
        for row in rows:
            insert_rows("//tmp/t", [row], require_sync_replica=False)

        def _get_replica_row_count(table):
            return len(list(select_rows(f"* from [{table}]", driver=self.replica_driver)))

        wait(lambda: _get_replica_row_count("//tmp/r1") >= 5)
        sync_disable_table_replica(replica_id)

        create_table_backup(self._make_backup_manifest(1))
        sync_flush_table("//tmp/t")
        sync_flush_table("//tmp/r1", driver=self.replica_driver)

        restore_table_backup(self._make_restore_manifest(1))
        sync_mount_table("//tmp/res")
        sync_mount_table("//tmp/res1", driver=self.replica_driver)

        written_row_count = _get_replica_row_count("//tmp/res1")
        tablet_info = get_tablet_infos("//tmp/res", [0])["tablets"][0]
        replica_info = tablet_info["replica_infos"][0]
        replicated_row_count = replica_info["current_replication_row_index"]
        assert written_row_count == replicated_row_count

        assert get("//tmp/res/@replication_throttler/limit") == 2000
        set("//tmp/res/@replication_throttler/limit", 10000)
        remount_table("//tmp/res")

        cloned_replica_id = get("//tmp/res1/@upstream_replica_id", driver=self.replica_driver)
        sync_enable_table_replica(replica_id)
        sleep(1)
        assert get("#" + cloned_replica_id + "/@state") == "disabled"
        assert _get_replica_row_count("//tmp/res1") == written_row_count

        sync_enable_table_replica(cloned_replica_id)
        wait(lambda: are_items_equal(
            rows,
            list(select_rows("* from [//tmp/res1]", driver=self.replica_driver))))

    @pytest.mark.flaky(
        max_runs=5,
        rerun_filter=lambda err, *args: issubclass(err[0], BackupKnowinglyFailedException))
    def test_lagging_async_replica(self):
        self._create_cells(5)
        self._create_tables(["async", "async"], mount=False)

        def _make_pivots(n, c):
            keys = sorted(sample(range(c), n - 1))
            pivots = [[]] + [[x] for x in keys]
            print_debug(pivots)
            return pivots

        tablet_count = 10
        key_count = 500
        sync_reshard_table("//tmp/t", _make_pivots(tablet_count, key_count))
        sync_reshard_table("//tmp/r1", _make_pivots(tablet_count, key_count), driver=self.replica_driver)
        sync_reshard_table("//tmp/r2", _make_pivots(tablet_count, key_count), driver=self.replica_driver)
        sync_mount_table("//tmp/r1", driver=self.replica_driver)
        sync_mount_table("//tmp/r2", driver=self.replica_driver)

        for i in range(tablet_count):
            set("//tmp/t/@replication_throttler", {"limit": randint(1000, 20000)})
            sync_mount_table("//tmp/t", first_tablet_index=i, last_tablet_index=i)

        def _write_rows_async(table, rows):
            print_debug("Inserting {} rows ({} .. {})".format(
                len(rows), rows[0], rows[-1]))
            responses = []
            for row in rows:
                responses.append(insert_rows(
                    table,
                    [row],
                    require_sync_replica=False,
                    verbose=False,
                    return_response=True))
            for response in responses:
                response.wait()
                assert response.is_ok()

        rows = [{"key": i, "value1": "A" * 1000, "value2": i * 100} for i in range(0, key_count, 2)]
        rows += [{"key": i, "value1": "A" * 1000, "value2": i * 100} for i in range(1, key_count, 2)]
        bad_rows = [{"key": i, "value1": "bad", "value2": -1} for i in range(key_count)]
        _write_rows_async("//tmp/t", rows[:key_count//2])

        try:
            create_table_backup(self._make_backup_manifest(2), checkpoint_timestamp_delay=2000)
        except YtResponseError as e:
            if e.contains_text("Backup aborted due to overlapping replication transaction"):
                raise BackupKnowinglyFailedException()
            raise e

        _write_rows_async("//tmp/t", bad_rows)
        remove("//tmp/t/@replication_throttler")
        remount_table("//tmp/t")
        wait(lambda: are_items_equal(
            bad_rows,
            list(select_rows("* from [//tmp/r1]", driver=self.replica_driver, verbose=False))))
        wait(lambda: are_items_equal(
            bad_rows,
            list(select_rows("* from [//tmp/r2]", driver=self.replica_driver, verbose=False))))
        sync_freeze_table("//tmp/t")
        sync_freeze_table("//tmp/r1", driver=self.replica_driver)
        sync_freeze_table("//tmp/r2", driver=self.replica_driver)

        restore_table_backup(self._make_restore_manifest(2))

        set("//tmp/res/@replication_throttler", {"limit": 0})
        sync_mount_table("//tmp/res")
        sync_mount_table("//tmp/res1", driver=self.replica_driver)
        sync_mount_table("//tmp/res2", driver=self.replica_driver)
        tablet_infos = get_tablet_infos("//tmp/res", list(range(tablet_count)))["tablets"]

        # For each replica, check that replicated row count equals written row count.
        for table in ("//tmp/res1", "//tmp/res2"):
            replica_id = get(table + "/@upstream_replica_id", driver=self.replica_driver)
            written_row_count = get(table + "/@chunk_row_count", driver=self.replica_driver)
            written_row_count = len(list(select_rows(
                f"key from [{table}] order by key limit 10000",
                driver=self.replica_driver)))
            replicated_row_count = 0
            for tablet_info in tablet_infos:
                replica_info = self._get_replica_info(tablet_info, replica_id)
                replicated_row_count += replica_info["current_replication_row_index"]
            assert replicated_row_count == written_row_count

        for r in get("//tmp/res/@replicas"):
            sync_enable_table_replica(r)
        set("//tmp/res/@replication_throttler", {"limit": 250000})

        def _check(expected):
            tablet_infos = get_tablet_infos("//tmp/res", list(range(tablet_count)))["tablets"]
            total_replicated_row_count = Counter()
            for tablet_info in tablet_infos:
                for replica_info in tablet_info["replica_infos"]:
                    total_replicated_row_count[replica_info["replica_id"]] +=\
                        replica_info["current_replication_row_index"]
            return all(x == expected for x in total_replicated_row_count.values())
        set("//tmp/res/@replication_throttler", {"limit": 200000})
        remount_table("//tmp/res")
        wait(lambda: _check(key_count//2))

        _write_rows_async("//tmp/res", rows[key_count//2:])
        wait(lambda: _check(key_count))

        sync_alter_table_replica_mode(
            get("//tmp/res1/@upstream_replica_id", driver=self.replica_driver),
            mode="sync")

        assert_items_equal(list(select_rows("* from [//tmp/res]", verbose=False)), rows)

    def test_catching_up_sync_replica_fails(self):
        self._create_cells()
        self._create_tables(["async", "sync"])
        set("//tmp/t/@replication_throttler", {"limit": 500})
        remount_table("//tmp/t")
        for i in range(15):
            insert_rows(
                "//tmp/t",
                [{"key": i, "value1": "A" * 1000}],
                require_sync_replica=False)

        replica_id = get("//tmp/r1/@upstream_replica_id", driver=self.replica_driver)
        sync_alter_table_replica_mode(replica_id, "sync")

        tablet_info = get_tablet_infos("//tmp/t", [0])["tablets"][0]
        replica_info = self._get_replica_info(tablet_info, replica_id)
        assert replica_info["mode"] == "sync"
        assert replica_info["current_replication_row_index"] < tablet_info["total_row_count"]

        # Replica 1 of sync mode is not in sync and cannot be backed up.
        with raises_yt_error(yt_error_codes.BackupCheckpointRejected):
            create_table_backup(self._make_backup_manifest([1]))

        # But it should not prevent us from backing up only replica 2.
        create_table_backup(self._make_backup_manifest([2]))

    def test_bulk_insert_to_replicas(self):
        self._create_cells()
        self._create_tables(["sync", "async"])

        normal_rows = [{"key": i, "value1": str(i), "value2": i * 100} for i in range(10)]
        for row in normal_rows:
            insert_rows("//tmp/t", [row])

        bulk_rows = [{"key": i, "value1": str(i), "value2": i * 100} for i in range(10, 20)]
        create("table", "//tmp/data", driver=self.replica_driver)
        write_table("//tmp/data", bulk_rows, driver=self.replica_driver)

        for table in ["//tmp/r1", "//tmp/r2"]:
            set(table + "/@enable_compaction_and_partitioning", False, driver=self.replica_driver)
            remount_table(table, driver=self.replica_driver)
            merge(in_="//tmp/data", out="<append=%true>" + table, mode="ordered", driver=self.replica_driver)

        rows = normal_rows + bulk_rows
        assert_items_equal(list(select_rows("* from [//tmp/r1]", driver=self.replica_driver)), rows)
        wait(lambda: are_items_equal(list(select_rows("* from [//tmp/r2]", driver=self.replica_driver)), rows))

        create_table_backup(self._make_backup_manifest(2))
        sync_freeze_table("//tmp/t")
        sync_freeze_table("//tmp/r1", driver=self.replica_driver)
        sync_freeze_table("//tmp/r2", driver=self.replica_driver)

        restore_table_backup(self._make_restore_manifest(2))

    def test_frozen_replicated_table(self):
        self._create_cells()
        self._create_tables(["async"])
        set("//tmp/t/@replication_throttler", {"limit": 2000})
        remount_table("//tmp/t")

        rows = [{"key": i, "value1": "A" * 1000, "value2": i * 100} for i in range(15)]
        for row in rows:
            insert_rows("//tmp/t", [row], require_sync_replica=False)

        def _get_replica_row_count(table):
            return len(list(select_rows(f"* from [{table}]", driver=self.replica_driver)))

        wait(lambda: _get_replica_row_count("//tmp/r1") >= 5)
        sync_freeze_table("//tmp/t")

        create_table_backup(self._make_backup_manifest(1), checkpoint_timestamp_delay=2000)
        sync_flush_table("//tmp/r1", driver=self.replica_driver)

        set("//tmp/t/@replication_throttler/limit", 10000)
        remount_table("//tmp/t")
        wait(lambda: _get_replica_row_count("//tmp/r1") == len(rows))

        restore_table_backup(self._make_restore_manifest(1))
        sync_mount_table("//tmp/res")
        sync_mount_table("//tmp/res1", driver=self.replica_driver)

        written_row_count = _get_replica_row_count("//tmp/res1")
        tablet_info = get_tablet_infos("//tmp/res", [0])["tablets"][0]
        replica_info = tablet_info["replica_infos"][0]
        replicated_row_count = replica_info["current_replication_row_index"]
        assert written_row_count == replicated_row_count

        set("//tmp/res/@replication_throttler/limit", 10000)
        remount_table("//tmp/res")
        cloned_replica_id = get("//tmp/res1/@upstream_replica_id", driver=self.replica_driver)
        sync_enable_table_replica(cloned_replica_id)
        wait(lambda: are_items_equal(
            rows,
            list(select_rows("* from [//tmp/res1]", driver=self.replica_driver))))

    def test_preserve_timestamps(self):
        self._create_cells()
        self._create_replicated_table("//tmp/t")
        replica_id = create_table_replica(
            "//tmp/t",
            self.REPLICA_CLUSTER_NAME,
            "//tmp/r1",
            mode="sync",
            attributes={"preserve_timestamps": False})
        self._create_replica_table("//tmp/r1", replica_id)
        sync_enable_table_replica(replica_id)

        rows = [{"key": 1, "value1": "foo", "value2": 1234}]
        insert_rows("//tmp/t", rows)
        assert_items_equal(list(select_rows("* from [//tmp/r1]", driver=self.replica_driver)), rows)

        with raises_yt_error():
            create_table_backup(self._make_backup_manifest(1))

    def test_enable_restored_replicas(self):
        self._create_cells()
        self._create_tables(["sync", "async"])

        for r, attrs in get("//tmp/t/@replicas").items():
            if attrs["mode"] == "async":
                sync_disable_table_replica(r)

        create_table_backup(
            self._make_backup_manifest(2),
            checkpoint_timestamp_delay=3000)
        restore_table_backup(
            self._make_restore_manifest(2),
            mount=True,
            enable_replicas=True)

        wait(lambda: get("//tmp/t/@tablet_state") == "mounted")

        for r, attrs in get("//tmp/res/@replicas").items():
            if attrs["mode"] == "sync":
                wait(lambda: get(f"#{r}/@state") == "enabled")
            else:
                assert get(f"#{r}/@state") == "disabled"

    @authors("akozhikhov")
    def test_rtt_for_restored_table(self):
        self._create_cells()
        self._create_tables(["async"])

        set("//tmp/t/@replicated_table_options", {
            "min_sync_replica_count": 0,
            "max_sync_replica_count": 1,
        })
        assert get("//tmp/t/@replicated_table_options/min_sync_replica_count") == 0

        create_table_backup(self._make_backup_manifest(1))
        restore_table_backup(
            self._make_restore_manifest(1),
            mount=True,
            enable_replicas=True)

        assert get("//tmp/res/@replicated_table_options/min_sync_replica_count") == 0
        set("//tmp/res/@replicated_table_options/enable_replicated_table_tracker", True)

        replica_id = list(get("//tmp/res/@replicas").keys())[0]
        wait(lambda: get("#{0}/@mode".format(replica_id)) == "sync")

        set("//sys/@config/tablet_manager/replicated_table_tracker/replicator_hint/banned_replica_clusters",
            [self.REPLICA_CLUSTER_NAME])
        wait(lambda: get("#{0}/@mode".format(replica_id)) == "async")

        set("//sys/@config/tablet_manager/replicated_table_tracker/replicator_hint/banned_replica_clusters", [])
        wait(lambda: get("#{0}/@mode".format(replica_id)) == "sync")


##################################################################


@authors("ifsmirnov")
class TestBackupsMulticell(TestBackups):
    NUM_SECONDARY_MASTER_CELLS = 2


@authors("ifsmirnov")
class TestBackupsShardedTx(TestBackups):
    NUM_SECONDARY_MASTER_CELLS = 2
    MASTER_CELL_DESCRIPTORS = {
        "10": {"roles": ["cypress_node_host"]},
        "12": {"roles": ["transaction_coordinator"]},
    }

    def test_external(self):
        sync_create_cells(1)
        self._create_sorted_table("//tmp/t1", external=False)
        self._create_sorted_table("//tmp/t2", external=True)
        sync_mount_table("//tmp/t1")
        sync_mount_table("//tmp/t2")
        insert_rows("//tmp/t1", [{"key": 1, "value": "a"}])
        insert_rows("//tmp/t2", [{"key": 2, "value": "b"}])
        create_table_backup(["//tmp/t1", "//tmp/bak1"], ["//tmp/t2", "//tmp/bak2"])
        sync_freeze_table("//tmp/t1")
        sync_freeze_table("//tmp/t2")
        restore_table_backup(["//tmp/bak1", "//tmp/res1"], ["//tmp/bak2", "//tmp/res2"])
        sync_mount_table("//tmp/res1")
        sync_mount_table("//tmp/res2")
        assert_items_equal(select_rows("* from [//tmp/res1]"), [{"key": 1, "value": "a"}])
        assert_items_equal(select_rows("* from [//tmp/res2]"), [{"key": 2, "value": "b"}])


@authors("ifsmirnov")
class TestReplicatedTableBackupsMulticell(TestReplicatedTableBackups):
    NUM_SECONDARY_MASTER_CELLS = 2


@authors("ifsmirnov")
class TestReplicatedTableBackupsShardedTx(TestReplicatedTableBackups):
    NUM_SECONDARY_MASTER_CELLS = 2
    MASTER_CELL_DESCRIPTORS = {
        "10": {"roles": ["cypress_node_host"]},
        "12": {"roles": ["transaction_coordinator"]},
    }


@authors("dave11ar")
class TestBackupsRpcProxy(TestBackups):
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True
