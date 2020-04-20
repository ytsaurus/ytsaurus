import pytest

from test_dynamic_tables import DynamicTablesBase

from yt_env_setup import YTEnvSetup, unix_only, wait, parametrize_external, Restarter, NODES_SERVICE
from yt_commands import *
import yt.yson as yson

from yt.environment.helpers import assert_items_equal

from yt.test_helpers import assert_items_equal

from flaky import flaky

from time import sleep

from copy import deepcopy

import __builtin__

##################################################################

@authors("ifsmirnov")
class TestBulkInsert(DynamicTablesBase):
    NUM_MASTERS = 1
    NUM_NODES = 5
    NUM_SCHEDULERS = 1
    USE_DYNAMIC_TABLES = True
    ENABLE_BULK_INSERT = True

    def _create_simple_dynamic_table(self, path, sort_order="ascending", **attributes):
        if "schema" not in attributes:
            attributes.update({"schema": [
                {"name": "key", "type": "int64", "sort_order": sort_order},
                {"name": "value", "type": "string"}]
            })
        create_dynamic_table(path, **attributes)

    def _ypath_with_update_mode(self, path, update_mode):
        if update_mode == "append":
            return "<append=%true>" + path
        elif update_mode == "overwrite":
            return path
        else:
            assert False

    @parametrize_external
    @pytest.mark.parametrize("freeze", [True, False])
    def test_basic_bulk_insert(self, external, freeze):
        sync_create_cells(1)
        create("table", "//tmp/t_input")
        self._create_simple_dynamic_table("//tmp/t_output", external=external)
        sync_mount_table("//tmp/t_output", freeze=freeze)

        rows = [{"key": 1, "value": "1"}]
        write_table("//tmp/t_input", rows)

        ts_before = generate_timestamp()

        op = map(
            in_="//tmp/t_input",
            out="<append=true>//tmp/t_output",
            command="cat")

        assert "legacy_live_preview_suppressed" in op.get_alerts()
        assert "dynamic" in str(op.get_alerts()["legacy_live_preview_suppressed"])

        assert read_table("//tmp/t_output") == rows
        assert_items_equal(select_rows("* from [//tmp/t_output]"), rows)

        actual = lookup_rows("//tmp/t_output", [{"key": 1}], versioned=True)
        assert len(actual) == 1

        row = actual[0]
        assert len(row.attributes["write_timestamps"]) == 1
        ts_write = row.attributes["write_timestamps"][0]
        assert ts_write > ts_before
        assert ts_write < generate_timestamp()

        assert row["key"] == 1
        assert str(row["value"][0]) ==  "1"

        expected_chunk_count = 1 if freeze else 3
        wait(lambda: get("//tmp/t_output/@chunk_count") == expected_chunk_count)

        assert lookup_rows("//tmp/t_output", [{"key": 123}]) == []

    def test_not_sorted_output(self):
        sync_create_cells(1)
        create("table", "//tmp/t_input")
        self._create_simple_dynamic_table("//tmp/t_output")
        sync_mount_table("//tmp/t_output")

        rows = [
            {"key": 2, "value": "2"},
            {"key": 1, "value": "1"},
        ]
        write_table("//tmp/t_input", rows)

        with pytest.raises(YtError):
            map(
                in_="//tmp/t_input",
                out="<append=true>//tmp/t_output",
                command="cat",
                mode="ordered",
                spec={"max_failed_job_count": 1})

        wait(lambda: get("//tmp/t_output/@chunk_count") == 2)

    def test_not_unique_keys(self):
        sync_create_cells(1)
        create("table", "//tmp/t_input")
        self._create_simple_dynamic_table("//tmp/t_output")
        sync_mount_table("//tmp/t_output")

        rows = [
            {"key": 1, "value": "1"},
            {"key": 1, "value": "1"},
        ]
        write_table("//tmp/t_input", rows)

        with pytest.raises(YtError):
            map(
                in_="//tmp/t_input",
                out="<append=true>//tmp/t_output",
                command="cat",
                mode="ordered",
                spec={"max_failed_job_count": 1})

        wait(lambda: get("//tmp/t_output/@chunk_count") == 2)

    def test_write_to_unmounted(self):
        sync_create_cells(1)
        create("table", "//tmp/t_input")
        self._create_simple_dynamic_table("//tmp/t_output")

        with pytest.raises(YtError):
            map(
                in_="//tmp/t_input",
                out="<append=%true>//tmp/t_output",
                command="cat",
                spec={"max_failed_job_count": 1})

        assert get("//tmp/t_output/@chunk_count") == 0

    def test_write_to_mounted_tablets_of_partially_mounted_table(self):
        sync_create_cells(1)
        set("//sys/@config/tablet_manager/tablet_balancer/enable_tablet_balancer", False)
        create("table", "//tmp/t_input")
        self._create_simple_dynamic_table("//tmp/t_output")
        sync_reshard_table("//tmp/t_output", [[], [1]])
        sync_mount_table("//tmp/t_output", first_tablet_index=1, last_tablet_index=1)

        rows = [{"key": 1, "value": "1"}]
        write_table("//tmp/t_input", rows)

        with pytest.raises(YtError):
            map(
                in_="//tmp/t_input",
                out="<append=%true>//tmp/t_output",
                command="cat")

    @pytest.mark.xfail(run=False, reason="Duplicate output tables are not fully supported, YT-10326")
    def test_same_table_more_than_once(self):
        sync_create_cells(1)
        create("table", "//tmp/t_input")
        self._create_simple_dynamic_table("//tmp/t_output")
        sync_mount_table("//tmp/t_output")

        rows = [{"key": 1, "value": "1"}]
        write_table("//tmp/t_input", rows)

        map(
            in_="//tmp/t_input",
            out=["<append=%true>//tmp/t_output", "<append=%true>//tmp/t_output"],
            command="echo '{key=1;value=\"1\"}'; echo '{key=2;value=\"2\"}' >&4")

        # TODO(ifsmirnov): probably we should disallow writing twice to the same dynamic table.
        assert get("//tmp/t_output/@chunk_count") == 2
        assert read_table("//tmp/t_output") == [{"key": 1, "value": "1"}, {"key": 2, "value": "2"}]

    @parametrize_external
    def test_multiple_output_tables_get_same_timestamp(self, external):
        cells = sync_create_cells(2)
        create("table", "//tmp/t_input")
        if external:
            self._create_simple_dynamic_table("//tmp/t1", external_cell_tag=1)
            self._create_simple_dynamic_table("//tmp/t2", external_cell_tag=2)
        else:
            self._create_simple_dynamic_table("//tmp/t1", external=False)
            self._create_simple_dynamic_table("//tmp/t2", external=False)
        set("//tmp/t1/@enable_compaction_and_partitioning", False)
        set("//tmp/t2/@enable_compaction_and_partitioning", False)
        sync_mount_table("//tmp/t1", target_cell_ids=[cells[0]])
        sync_mount_table("//tmp/t2", target_cell_ids=[cells[1]])

        write_table("//tmp/t_input", [{"a": 1}])

        map(
            in_="//tmp/t_input",
            out=["<append=%true>//tmp/t1", "<append=%true>//tmp/t2"],
            command="echo '{key=1;value=\"1\"}'; echo '{key=2;value=\"2\"}' >&4")

        def _get_chunk_view(table):
            chunk_list_id = get("{}/@chunk_list_id".format(table))
            tree = get("#{}/@tree".format(chunk_list_id))
            for child in tree[0]:
                if child.attributes["type"] == "chunk_list":
                    chunk_view_id = child[0].attributes["id"]
                    return get("#{}/@".format(chunk_view_id))
            assert False

        def _read_row_timestamp(table, key, expected_value):
            actual = lookup_rows(table, [{"key": key}], versioned=True)
            row = actual[0]
            assert str(row["value"][0]) == expected_value
            return row.attributes["write_timestamps"][0]

        expected_ts = _get_chunk_view("//tmp/t1")["timestamp"]
        assert expected_ts == _get_chunk_view("//tmp/t2")["timestamp"]

        assert expected_ts == _read_row_timestamp("//tmp/t1", 1, "1")
        assert expected_ts == _read_row_timestamp("//tmp/t2", 2, "2")

        sync_unmount_table("//tmp/t1")
        sync_unmount_table("//tmp/t2")
        sync_mount_table("//tmp/t1")
        sync_mount_table("//tmp/t2")

        assert expected_ts == _read_row_timestamp("//tmp/t1", 1, "1")
        assert expected_ts == _read_row_timestamp("//tmp/t2", 2, "2")

    @parametrize_external
    def test_table_unlocked(self, external):
        sync_create_cells(1)
        create("table", "//tmp/t_input")
        self._create_simple_dynamic_table("//tmp/t_output", external=external)
        sync_mount_table("//tmp/t_output")

        rows = [
            {"key": 1, "value": "1"},
            {"key": 2, "value": "2"},
            {"key": 3, "value": "3"},
        ]

        insert_rows("//tmp/t_output", [rows[0]])

        write_table("//tmp/t_input", [rows[1]])

        map(
            in_="//tmp/t_input",
            out="<append=%true>//tmp/t_output",
            command="cat")

        tablet_id = get("//tmp/t_output/@tablets/0/tablet_id")
        address = get_tablet_leader_address(tablet_id)
        def wait_func():
            orchid = self._find_tablet_orchid(address, tablet_id)
            return len(orchid["dynamic_table_locks"]) == 0
        wait(wait_func)

        insert_rows("//tmp/t_output", [rows[2]])

        assert_items_equal(select_rows("* from [//tmp/t_output]"), rows)

        sync_compact_table("//tmp/t_output")
        assert read_table("//tmp/t_output") == rows

    @parametrize_external
    def test_subsequent_bulk_inserts(self, external):
        sync_create_cells(1)
        create("table", "//tmp/t_input")
        self._create_simple_dynamic_table("//tmp/t_output", external=external)
        sync_mount_table("//tmp/t_output")

        rows = [{"a": 1}]
        write_table("//tmp/t_input", rows)

        operations = []
        for i in range(3):
            command = "cat >/dev/null; echo '{{key={};value=\"{}\"}}'".format(i, str(i))
            op = map(
                in_="//tmp/t_input",
                out="<append=%true>//tmp/t_output",
                command=command)
            operations.append(op)

        assert read_table("//tmp/t_output") == [{"key": i, "value": str(i)} for i in range(len(operations))]
        assert_items_equal(select_rows("* from [//tmp/t_output]"), [{"key": i, "value": str(i)} for i in range(len(operations))])

    @parametrize_external
    def test_simultaneous_bulk_inserts(self, external):
        sync_create_cells(1)
        create("table", "//tmp/t_input")
        self._create_simple_dynamic_table("//tmp/t_output", external=external)
        sync_mount_table("//tmp/t_output")

        rows = [{"a": 1}]
        write_table("//tmp/t_input", rows)

        operations = []
        for i in range(10):
            command = "cat >/dev/null; echo '{{key={};value=\"{}\"}}'".format(i, str(i))
            op = map(
                in_="//tmp/t_input",
                out="<append=%true>//tmp/t_output",
                command=command,
                track=False)
            operations.append(op)

        for op in operations:
            op.wait_for_state("completed")

        assert read_table("//tmp/t_output") == [{"key": i, "value": str(i)} for i in range(len(operations))]
        assert_items_equal(select_rows("* from [//tmp/t_output]"), [{"key": i, "value": str(i)} for i in range(len(operations))])

    def test_timestamp_preserved_after_mount_unmount(self):
        sync_create_cells(1)
        create("table", "//tmp/t_input")
        self._create_simple_dynamic_table("//tmp/t_output")
        sync_mount_table("//tmp/t_output")

        rows = [{"key": 1, "value": "1"}]
        write_table("//tmp/t_input", rows)

        map(
            in_="//tmp/t_input",
            out="<append=true>//tmp/t_output",
            command="cat")

        def _get_timestamp():
            rows = lookup_rows("//tmp/t_output", [{"key": 1}], versioned=True)
            assert len(rows) == 1
            row = rows[0]
            assert len(row.attributes["write_timestamps"]) == 1
            return row.attributes["write_timestamps"][0]

        ts_write = _get_timestamp()
        sync_unmount_table("//tmp/t_output")
        sync_mount_table("//tmp/t_output")
        assert _get_timestamp() == ts_write

    def test_subtablet_chunk_list_is_pruned(self):
        sync_create_cells(1)
        create("table", "//tmp/t_input")
        self._create_simple_dynamic_table("//tmp/t_output")
        set("//tmp/t_output/@enable_compaction_and_partitioning", False)
        sync_mount_table("//tmp/t_output")

        rows = [{"key": 1, "value": "1"}]
        write_table("//tmp/t_input", rows)

        map(
            in_="//tmp/t_input",
            out="<append=true>//tmp/t_output",
            command="cat")

        root_chunk_list = get("//tmp/t_output/@chunk_list_id")
        tablet_chunk_list = get("#{}/@child_ids/0".format(root_chunk_list))
        child_ids = get("#{}/@child_ids".format(tablet_chunk_list))
        subtablets = [child_id for child_id in child_ids if get("#{}/@type".format(child_id)) == "chunk_list"]
        assert len(subtablets) == 1
        subtablet_chunk_list = subtablets[0]
        assert get("#{}/@statistics/chunk_list_count".format(tablet_chunk_list)) == 2
        assert get("#{}/@statistics/chunk_list_count".format(root_chunk_list)) == 3

        set("//tmp/t_output/@enable_compaction_and_partitioning", True)
        sync_compact_table("//tmp/t_output")

        wait(lambda: not exists("#{}".format(subtablet_chunk_list)))
        assert get("#{}/@statistics/chunk_list_count".format(tablet_chunk_list)) == 1
        assert get("#{}/@statistics/chunk_list_count".format(root_chunk_list)) == 2

    @pytest.mark.parametrize("ref_type", ["lock", "copy"])
    def test_chunk_list_statistics_after_cow(self, ref_type):
        sync_create_cells(1)
        create("table", "//tmp/t_input")
        self._create_simple_dynamic_table("//tmp/t_output")
        sync_mount_table("//tmp/t_output", freeze=True)

        rows = [{"key": 1, "value": "1"}]
        write_table("//tmp/t_input", rows)

        map(
            in_="//tmp/t_input",
            out="<append=true>//tmp/t_output",
            command="cat")

        if ref_type == "lock":
            tx = start_transaction(timeout=60000)
            lock("//tmp/t_output", mode="snapshot", tx=tx)
        else:
            copy("//tmp/t_output", "//tmp/t_copy")

        sync_compact_table("//tmp/t_output")

        root_chunk_list = get("//tmp/t_output/@chunk_list_id")
        tablet_chunk_list = get("#{}/@child_ids/0".format(root_chunk_list))
        assert get("#{}/@statistics/chunk_list_count".format(tablet_chunk_list)) == 1
        assert get("#{}/@statistics/chunk_list_count".format(root_chunk_list)) == 2

    def test_read_with_timestamp(self):
        sync_create_cells(1)
        create("table", "//tmp/t_input")
        self._create_simple_dynamic_table("//tmp/t_output")
        sync_mount_table("//tmp/t_output")

        old_ts = generate_timestamp()

        rows = [{"key": 1, "value": "1"}]
        write_table("//tmp/t_input", rows)

        map(
            in_="//tmp/t_input",
            out="<append=true>//tmp/t_output",
            command="cat")

        new_ts = generate_timestamp()

        sync_flush_table("//tmp/t_output")
        assert read_table("<timestamp={}>//tmp/t_output".format(old_ts)) == []
        assert read_table("<timestamp={}>//tmp/t_output".format(new_ts)) == rows

    def test_map_on_bulk_inserted_data(self):
        sync_create_cells(1)
        create("table", "//tmp/t_input")
        self._create_simple_dynamic_table("//tmp/t_output")
        sync_mount_table("//tmp/t_output")

        old_ts = generate_timestamp()

        rows = [{"key": 1, "value": "1"}]
        write_table("//tmp/t_input", rows)

        map(
            in_="//tmp/t_input",
            out="<append=true>//tmp/t_output",
            command="cat")

        new_ts = generate_timestamp()

        sync_flush_table("//tmp/t_output")

        def _verify(ts, expected):
            create("table", "//tmp/t_verify", force=True)
            map(
                in_="<timestamp={}>//tmp/t_output".format(ts),
                out="//tmp/t_verify",
                command="cat")
            assert read_table("//tmp/t_verify") == expected

        _verify(old_ts, [])
        _verify(new_ts, rows)

    @parametrize_external
    def test_chunk_teleportation(self, external):
        sync_create_cells(1)
        if external:
            self._create_simple_dynamic_table("//tmp/t_output", external_cell_tag=1)
        else:
            self._create_simple_dynamic_table("//tmp/t_output", external=False)
        set("//tmp/t_output/@enable_compaction_and_partitioning", False)
        sync_mount_table("//tmp/t_output")

        if external:
            create(
                "table",
                "//tmp/t_input",
                attributes={"schema": get("//tmp/t_output/@schema")},
                external_cell_tag=2)
        else:
            create(
                "table",
                "//tmp/t_input",
                attributes={"schema": get("//tmp/t_output/@schema")},
                external=False)

        rows = [
            {"key": 1, "value": "1"},
            {"key": 2, "value": "2"},
        ]
        write_table("<append=%true>//tmp/t_input", [rows[0]])
        write_table("<append=%true>//tmp/t_input", [rows[1]])

        merge(
            in_="//tmp/t_input",
            out="<append=%true>//tmp/t_output",
            mode="ordered")

        assert get("//tmp/t_output/@chunk_ids") == get("//tmp/t_input/@chunk_ids")
        assert read_table("//tmp/t_output") == rows
        assert_items_equal(select_rows("* from [//tmp/t_output]"), rows)
        lookup_result = lookup_rows("//tmp/t_output", [{"key": 1}, {"key": 2}], versioned=True)
        assert lookup_result[0].attributes["write_timestamps"] == lookup_result[1].attributes["write_timestamps"]

    def test_partially_sorted(self):
        sync_create_cells(1)
        create("table", "//tmp/t_input")
        self._create_simple_dynamic_table("//tmp/t_output")
        set("//tmp/t_output/@enable_compaction_and_partitioning", False)
        sync_mount_table("//tmp/t_output")

        rows = [
            {"key": 1, "value": "1"},
            {"key": 3, "value": "3"},
            {"key": 2, "value": "2"},
            {"key": 4, "value": "4"}
        ]

        write_table("<append=%true>//tmp/t_input", rows[:2])
        write_table("<append=%true>//tmp/t_input", rows[2:])

        # Two separate jobs process [1, 3] and [2, 4].
        map(
            in_="//tmp/t_input",
            out="<append=%true;partially_sorted=%true>//tmp/t_output",
            command="cat",
            spec={"job_count": 2})

        wait(lambda: get("//tmp/t_output/@chunk_count") == 4)
        assert read_table("//tmp/t_output") == sorted(rows)
        assert_items_equal(select_rows("* from [//tmp/t_output]"), sorted(rows))
        wait(lambda: get("//tmp/t_output/@tablet_statistics/overlapping_store_count") == 3)

        # Single job processing [1, 3, 2, 4] should fail.
        with raises_yt_error(SortOrderViolation):
            map(
                in_="//tmp/t_input",
                out="<append=%true;partially_sorted=%true>//tmp/t_output",
                command="cat",
                spec={"job_count": 1})

    @pytest.mark.parametrize("stage", ["stage5", "stage6"])
    def test_abort_operation(self, stage):
        sync_create_cells(1)
        create("table", "//tmp/t_input")
        self._create_simple_dynamic_table("//tmp/t_output")
        set("//tmp/t_output/@enable_store_rotation", False)
        sync_mount_table("//tmp/t_output")

        rows = [
            {"key": 1, "value": "1"},
            {"key": 2, "value": "2"},
            {"key": 3, "value": "3"},
        ]
        write_table("//tmp/t_input", [rows[0]])

        insert_rows("//tmp/t_output", [rows[1]])

        op = map(
            in_="//tmp/t_input",
            out="<append=true>//tmp/t_output",
            command="cat",
            spec={
                "testing": {
                    "delay_inside_operation_commit": 5000,
                    "delay_inside_operation_commit_stage": stage,
                },
            },
            track=False)

        op.wait_for_state("completing")
        tx = get(op.get_path() + "/@output_transaction_id")
        abort_transaction(tx)
        op.wait_for_state("failed")

        # The only present chunks are dynamic stores.
        assert get("//tmp/t_output/@chunk_count") == 2
        assert_items_equal(select_rows("* from [//tmp/t_output]"), [rows[1]])
        insert_rows("//tmp/t_output", [rows[2]])
        assert_items_equal(select_rows("* from [//tmp/t_output]"), rows[1:3])

    @pytest.mark.parametrize("update_mode", ["append", "overwrite"])
    @pytest.mark.parametrize("empty_output", [True, False])
    def test_competing_tablet_transaction_lost(self, update_mode, empty_output):
        cell_id = sync_create_cells(1)[0]
        node = get("#{}/@peers/0/address".format(cell_id))
        create("table", "//tmp/t_input")
        self._create_simple_dynamic_table("//tmp/t_output")
        sync_mount_table("//tmp/t_output")
        tablet_id = get("//tmp/t_output/@tablets/0/tablet_id")

        rows = [
            {"key": 1, "value": "1"},
            {"key": 2, "value": "2"},
        ]

        if not empty_output:
            write_table("//tmp/t_input", [rows[0]])

        tablet_tx = start_transaction(type="tablet")
        insert_rows("//tmp/t_output", [rows[1]], tx=tablet_tx)

        map(
            in_="//tmp/t_input",
            out=self._ypath_with_update_mode("//tmp/t_output", update_mode),
            command="cat")

        with pytest.raises(YtError):
            commit_transaction(tablet_tx)

        assert_items_equal(select_rows("* from [//tmp/t_output]"), [] if empty_output else rows[:1])

    @pytest.mark.parametrize("pivot_keys_before, pivot_keys_after", [
        [[[], [2], [4]], [[]]],
        [[[]], [[], [2], [4]]]
    ])
    def test_reshard_after_bulk_insert(self, pivot_keys_before, pivot_keys_after):
        sync_create_cells(1)
        create("table", "//tmp/t_input")
        self._create_simple_dynamic_table("//tmp/t_output")
        set("//tmp/t_output/@enable_compaction_and_partitioning", False)
        sync_reshard_table("//tmp/t_output", pivot_keys_before)
        sync_mount_table("//tmp/t_output")

        rows = [{"key": i, "value": str(i)} for i in range(10)]
        keys = [{"key": i} for i in range(10)]

        write_table("//tmp/t_input", rows)

        map(
            in_="//tmp/t_input",
            out="<append=%true>//tmp/t_output",
            command="cat")

        assert lookup_rows("//tmp/t_output", keys) == rows

        lookup_result = lookup_rows("//tmp/t_output", keys, versioned=True)

        sync_unmount_table("//tmp/t_output")
        sync_reshard_table("//tmp/t_output", pivot_keys_before)
        sync_mount_table("//tmp/t_output")

        assert lookup_result == lookup_rows("//tmp/t_output", keys, versioned=True)

    def test_chunk_views_with_distinct_tx_do_not_merge(self):
        sync_create_cells(1)
        create("table", "//tmp/t_input")
        self._create_simple_dynamic_table("//tmp/t_output")
        set("//tmp/t_output/@enable_compaction_and_partitioning", False)
        sync_mount_table("//tmp/t_output")

        write_table("//tmp/t_input", [{"key": 1, "value": "1"}])

        for i in range(2):
            map(
                in_="//tmp/t_input",
                out="<append=%true>//tmp/t_output",
                command="cat")

        lookup_result = lookup_rows("//tmp/t_output", [{"key": 1}], versioned=True)
        sync_unmount_table("//tmp/t_output")
        sync_reshard_table("//tmp/t_output", [[]])
        sync_mount_table("//tmp/t_output")
        assert lookup_result == lookup_rows("//tmp/t_output", [{"key": 1}], versioned=True)

        chunk_list_id = get("//tmp/t_output/@chunk_list_id")
        tablet_chunk_list_id = get("#{}/@child_ids/0".format(chunk_list_id))
        wait(lambda: len(get("#{}/@child_ids".format(tablet_chunk_list_id))) == 4)

    @pytest.mark.parametrize("in_memory_mode", ["none", "compressed"])
    def test_accounting(self, in_memory_mode):
        sync_create_cells(1)

        create_account("a")
        set("//sys/accounts/a/@resource_limits/tablet_count", 100)
        set("//sys/accounts/a/@resource_limits/tablet_static_memory", 10000)
        usage_before = get("//sys/accounts/a/@resource_usage")

        create("table", "//tmp/t_input")
        self._create_simple_dynamic_table("//tmp/t_output", account="a", in_memory_mode=in_memory_mode)
        sync_mount_table("//tmp/t_output")

        write_table("//tmp/t_input", [{"key": i, "value": str(i)} for i in range(100)])

        map(
            in_="//tmp/t_input",
            out="<append=%true>//tmp/t_output",
            command="cat")

        wait(lambda: get_account_disk_space("a") > 0)
        if in_memory_mode != "none":
            wait(lambda: get("//sys/accounts/a/@resource_usage/tablet_static_memory") > 0)

        # Overwrite table with less data, usage should decrease.
        disk_space_usage = get_account_disk_space("a")
        tablet_static_usage = get("//sys/accounts/a/@resource_usage/tablet_static_memory")
        map(
            in_="//tmp/t_input[:#10]",
            out="//tmp/t_output",
            command="cat")
        wait(lambda: 0 < get_account_disk_space("a") < disk_space_usage)
        if in_memory_mode != "none":
            wait(lambda: 0 < get("//sys/accounts/a/@resource_usage/tablet_static_memory") < tablet_static_usage)

        sync_unmount_table("//tmp/t_output")
        if in_memory_mode != "none":
            wait(lambda: get("//sys/accounts/a/@resource_usage/tablet_static_memory") == 0)

        remove("//tmp/t_output")
        wait(lambda: get("//sys/accounts/a/@resource_usage") == usage_before)

    def test_sorted_merge(self):
        sync_create_cells(1)
        create("table", "//tmp/t_input1")
        create("table", "//tmp/t_input2")
        self._create_simple_dynamic_table("//tmp/t_output")
        sync_mount_table("//tmp/t_output")

        rows = [
            {"key": 1, "value": "1"},
            {"key": 2, "value": "2"},
        ]
        write_table("//tmp/t_input1", rows[:1], sorted_by=["key"])
        write_table("//tmp/t_input2", rows[1:], sorted_by=["key"])

        merge(
            in_=["//tmp/t_input1", "//tmp/t_input2"],
            out="<append=%true>//tmp/t_output",
            mode="sorted")

        assert read_table("//tmp/t_output") == rows
        assert_items_equal(select_rows("* from [//tmp/t_output]"), rows)

    def test_sort(self):
        sync_create_cells(1)
        create("table", "//tmp/t_input")
        self._create_simple_dynamic_table("//tmp/t_output")
        sync_mount_table("//tmp/t_output")

        rows = [
            {"key": 2, "value": "2"},
            {"key": 1, "value": "1"},
        ]
        write_table("//tmp/t_input", rows)

        sort(
            in_="//tmp/t_input",
            out="<append=%true>//tmp/t_output",
            sort_by=["key"])

        assert read_table("//tmp/t_output") == sorted(rows)
        assert_items_equal(select_rows("* from [//tmp/t_output]"), sorted(rows))

    @pytest.mark.parametrize("config_source", ["default", "spec"])
    def test_table_writer_config(self, config_source):
        sync_create_cells(1)
        create("table", "//tmp/t_input")
        self._create_simple_dynamic_table("//tmp/t_output")
        sync_mount_table("//tmp/t_output")

        rows = [{"key": i, "value": str(i)} for i in range(100000)]
        write_table("//tmp/t_input", rows)

        if config_source == "spec":
            block_size = 2000
            spec={"job_io": {"dynamic_table_writer": {"block_size": block_size}}}
        else:
            block_size = 256 * 2**10
            spec = {}

        map(
            in_="//tmp/t_input",
            out="<append=%true>//tmp/t_output",
            command="cat",
            spec=spec)

        chunk_id = get("//tmp/t_output/@chunk_ids/0")
        assert block_size - 50 < get("#{}/@max_block_size".format(chunk_id)) < block_size + 50

    def test_no_user_transaction(self):
        sync_create_cells(1)
        create("table", "//tmp/t_input")
        self._create_simple_dynamic_table("//tmp/t_output")
        sync_mount_table("//tmp/t_output")

        write_table("//tmp/t_input", [{"key": 1, "value": "1"}])

        tx = start_transaction(timeout=60000)
        with pytest.raises(YtError):
            map(
                in_="//tmp/t_input",
                out="<append=%true>//tmp/t_output",
                command="cat",
                tx=tx)

    @pytest.mark.parametrize("atomicity", ["none", "full"])
    def test_atomicity_should_match(self, atomicity):
        sync_create_cells(1)
        create("table", "//tmp/t_input")
        self._create_simple_dynamic_table("//tmp/t_output")
        set("//tmp/t_output/@atomicity", atomicity)
        sync_mount_table("//tmp/t_output")

        write_table("//tmp/t_input", [{"key": 1, "value": "a"}])

        def _run(op_atomicity):
            map(
                in_="//tmp/t_input",
                out="<append=%true>//tmp/t_output",
                command="cat",
                spec={"atomicity": op_atomicity})

        _run(atomicity)
        with pytest.raises(YtError):
            _run("none" if atomicity == "full" else "full")

    @pytest.mark.xfail(run=False, reason="A bit of race here, fix is to be discussed")
    def test_atomicity_none(self):
        sync_create_cells(1)
        create("table", "//tmp/t_input")
        self._create_simple_dynamic_table("//tmp/t_output")
        set("//tmp/t_output/@atomicity", "none")
        sync_mount_table("//tmp/t_output")

        rows = [{"key": 1, "value": "1"}]

        write_table("//tmp/t_input", rows)

        map(
            in_="//tmp/t_input",
            out="<append=%true>//tmp/t_output",
            command="cat",
            spec={"atomicity": "none"})

        assert_items_equal(select_rows("* from [//tmp/t_output]"), rows)
        assert lookup_rows("//tmp/t_output", [{"key": 1}]) == rows
        assert read_table("//tmp/t_output") == rows

    @pytest.mark.parametrize("flush", [True, False])
    def test_overwrite(self, flush):
        sync_create_cells(1)
        create("table", "//tmp/t_input")
        self._create_simple_dynamic_table("//tmp/t_output", enable_dynamic_store_read=False)
        if not flush:
            set("//tmp/t_output/@enable_store_rotation", False)
        sync_mount_table("//tmp/t_output")

        rows = [
            {"key": 1, "value": "1"},
            {"key": 2, "value": "2"},
        ]
        write_table("//tmp/t_input", rows[:1])
        insert_rows("//tmp/t_output", rows[1:])

        if flush:
            sync_flush_table("//tmp/t_output")

        map(
            in_="//tmp/t_input",
            out="//tmp/t_output",
            command="cat")

        assert read_table("//tmp/t_output") == rows[:1]
        assert_items_equal(select_rows("* from [//tmp/t_output]"), rows[:1])

    def test_overlapping_store_count_limit(self):
        sync_create_cells(1)
        create("table", "//tmp/t_input")
        self._create_simple_dynamic_table("//tmp/t_output")
        set("//tmp/t_output/@max_overlapping_store_count", 3);
        set("//tmp/t_output/@enable_compaction_and_partitioning", False);
        sync_mount_table("//tmp/t_output")

        write_table("//tmp/t_input", [{"key": 1, "value": "1"}])

        def _run_op():
            map(
                in_="//tmp/t_input",
                out="<append=%true>//tmp/t_output",
                command="cat")

        _run_op()
        _run_op()
        wait(lambda: get("//tmp/t_output/@tablet_statistics/overlapping_store_count") == 3)
        with pytest.raises(YtError):
            _run_op()

    def test_per_user_permit(self):
        sync_create_cells(1)
        create("table", "//tmp/t_input")
        self._create_simple_dynamic_table("//tmp/t_output")
        sync_mount_table("//tmp/t_output")

        write_table("//tmp/t_input", [{"key": 1, "value": "1"}])

        def _set_global_permit(value):
            set("//sys/controller_agents/config/enable_bulk_insert_for_everyone", value)
            for instance in ls("//sys/controller_agents/instances"):
                def _wait_func():
                    config = get("//sys/controller_agents/instances/{}/orchid/controller_agent/config".format(instance))
                    return config.get("enable_bulk_insert_for_everyone", False) == value
                wait(_wait_func)

        def _run_op():
            map(
                in_="//tmp/t_input",
                out="<append=%true>//tmp/t_output",
                command="cat",
                authenticated_user="u")

        try:
            create_user("u")

            _run_op()

            _set_global_permit(False)
            with pytest.raises(YtError):
                _run_op()

            set("//sys/users/u/@enable_bulk_insert", True)
            _run_op()
        finally:
            _set_global_permit(True)

##################################################################

@authors("ifsmirnov")
class TestUnversionedUpdateFormat(DynamicTablesBase):
    NUM_MASTERS = 1
    NUM_NODES = 5
    NUM_SCHEDULERS = 1
    USE_DYNAMIC_TABLES = True
    ENABLE_BULK_INSERT = True

    MISSING_FLAG = 0x1
    AGGREGATE_FLAG = 0x2

    WRITE_CHANGE_TYPE = yson.YsonUint64(0)
    DELETE_CHANGE_TYPE = yson.YsonUint64(1)

    def _create_simple_dynamic_table(self, path, sort_order="ascending", **attributes):
        if "schema" not in attributes:
            attributes.update({"schema": [
                {"name": "key", "type": "int64", "sort_order": sort_order},
                {"name": "value", "type": "string"}]
            })
        create_dynamic_table(path, **attributes)

    def _make_value(self, column_name, value, aggregate=False, treat_empty_flags_as_null=False):
        if value is None:
            # Treat as missing.
            result = {"$flags:{}".format(column_name): yson.YsonUint64(self.MISSING_FLAG)}
            return result
        if aggregate:
            flags = self.AGGREGATE_FLAG
        elif treat_empty_flags_as_null:
            flags = None
        else:
            flags = 0
        result = {"$value:{}".format(column_name): value}
        if flags is not None:
            flags = yson.YsonUint64(flags)
            result["$flags:{}".format(column_name)] = flags
        return result

    def _prepare_write_row(self, *args, **kwargs):
        for arg in args:
            kwargs.update(arg)
        kwargs["$change_type"] = self.WRITE_CHANGE_TYPE
        return kwargs

    def _prepare_delete_row(self, *args, **kwargs):
        for arg in args:
            kwargs.update(arg)
        kwargs["$change_type"] = self.DELETE_CHANGE_TYPE
        return kwargs

    def _run_operation(self, rows, input_table="//tmp/t_input", output_table="//tmp/t_output"):
        if not isinstance(rows, list):
            rows = [rows]
        create("table", input_table, force=True)
        write_table(input_table, rows)
        map(
            in_=input_table,
            out="<append=%true;schema_modification=unversioned_update>{}".format(output_table),
            command="cat",
            spec={"max_failed_job_count": 1})

    def test_schema_violation(self):
        sync_create_cells(1)

        schema = [
            {"name": "k1", "type": "int64", "sort_order": "ascending"},
            {"name": "k2", "type": "int64", "sort_order": "ascending", "required": True},
            {"name": "v1", "type": "int64", "aggregate": "sum"},
            {"name": "v2", "type": "string", "required": True},
        ]

        self._create_simple_dynamic_table("//tmp/t_output", schema=schema)
        sync_mount_table("//tmp/t_output")

        # First, test that at least something works and that subsequent errors are not bogus.
        self._run_operation(self._prepare_write_row(
            self._make_value("v1", 3),
            self._make_value("v2", "4"),
            k1=1,
            k2=2))
        assert select_rows("* from [//tmp/t_output]") == [{"k1": 1, "k2": 2, "v1": 3, "v2": "4"}]

        # Missing change_type.
        with raises_yt_error(SchemaViolation):
            self._run_operation({
                "k1": 1,
                "k2": 1,
                "$value:v2": "1"})

        # Invalid change_type.
        with raises_yt_error(SchemaViolation):
            self._run_operation({
                "k1": 1,
                "k2": 1,
                "$change_type": 2,
                "$value:v2": "1"})

        # Invalid flags.
        with raises_yt_error(SchemaViolation):
            self._run_operation({
                "k1": 1,
                "k2": 1,
                "$change_type": 0,
                "$value:v2": "1",
                "$flags:v2": 123})

        # Invalid key type.
        with raises_yt_error(SchemaViolation):
            self._run_operation(self._prepare_write_row(
                self._make_value("v2", "1"),
                k1=1,
                k2=0.5))
        with raises_yt_error(SchemaViolation):
            self._run_operation(self._prepare_delete_row(
                k1=1,
                k2=0.5))

        # Invalid value type.
        with raises_yt_error(SchemaViolation):
            self._run_operation(self._prepare_write_row(
                self._make_value("v2", 100500),
                k1=1,
                k2=1))

        # Delete with non-null value columns.
        with raises_yt_error(SchemaViolation):
            self._run_operation(self._prepare_delete_row(
                self._make_value("v2", 1),
                k1=1,
                k2=1))

        # Null required key column.
        with raises_yt_error(SchemaViolation):
            self._run_operation(self._prepare_write_row(
                self._make_value("v2", 1),
                k1=1))
            self._run_operation(self._prepare_delete_row(
                k1=1))

        # Null required value column.
        with raises_yt_error(SchemaViolation):
            self._run_operation(self._prepare_write_row(
                k1=1,
                k2=1))

        # Required column marked as missing.
        with raises_yt_error(SchemaViolation):
            self._run_operation(self._prepare_write_row(
                self._make_value("v2", None),
                k1=1,
                k2=1))

    @pytest.mark.parametrize("optimize_for", ["lookup", "scan"])
    def test_delete(self, optimize_for):
        sync_create_cells(1)
        self._create_simple_dynamic_table("//tmp/t_output", optimize_for=optimize_for)
        set("//tmp/t_output/@enable_compaction_and_partitioning", False)
        sync_mount_table("//tmp/t_output")

        insert_rows("//tmp/t_output", [{"key": 1, "value": "a"}])
        self._run_operation([
            self._prepare_delete_row(key=1),
            self._prepare_write_row(self._make_value("value", "b"), key=2)])

        for chunk_id in get("//tmp/t_output/@chunk_ids"):
            chunk_format = get("#{}/@table_chunk_format".format(chunk_id))
            if optimize_for == "lookup":
                assert chunk_format == "versioned_simple"
            else:
                assert chunk_format == "versioned_columnar"

        def _verify():
            assert_items_equal(select_rows("* from [//tmp/t_output]"), [{"key": 2, "value": "b"}])
            lookup_result = lookup_rows("//tmp/t_output", [{"key": 1}], versioned=True)
            assert len(lookup_result) == 1
            row = lookup_result[0]
            assert len(row.attributes["write_timestamps"]) == 1
            write_ts = row.attributes["write_timestamps"][0]
            assert len(row.attributes["delete_timestamps"]) == 1
            delete_ts = row.attributes["delete_timestamps"][0]

            assert delete_ts > write_ts
            return delete_ts

        operation_ts = _verify()
        set("//tmp/t_output/@enable_compaction_and_partitioning", True)
        sync_compact_table("//tmp/t_output")
        assert _verify() == operation_ts

    def test_aggregate(self):
        sync_create_cells(1)
        schema = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "int64", "aggregate": "sum"},
        ]
        self._create_simple_dynamic_table("//tmp/t_output", schema=schema)
        sync_mount_table("//tmp/t_output")

        def _verify(expected_value):
            expected = [{"key": 1, "value": expected_value}]
            assert lookup_rows("//tmp/t_output", [{"key": 1}]) == expected
            assert_items_equal(select_rows("* from [//tmp/t_output]"), expected)
            assert_items_equal(read_table("//tmp/t_output"), expected)

        self._run_operation(self._prepare_write_row(
            self._make_value("value", 1),
            key=1))
        _verify(1)

        self._run_operation(self._prepare_write_row(
            self._make_value("value", 1, self.AGGREGATE_FLAG),
            key=1))
        _verify(2)

        self._run_operation(self._prepare_write_row(
            self._make_value("value", 10),
            key=1))
        _verify(10)

        sync_compact_table("//tmp/t_output")
        _verify(10)

    def test_missing(self):
        sync_create_cells(1)
        schema = [
            {"name": "k1", "type": "int64", "sort_order": "ascending"},
            {"name": "v1", "type": "int64"},
            {"name": "v2", "type": "int64"},
        ]
        self._create_simple_dynamic_table("//tmp/t_output", schema=schema)
        sync_mount_table("//tmp/t_output")

        def _verify(expected_v1, expected_v2):
            expected = [{"k1": 1, "v1": expected_v1, "v2": expected_v2}]
            assert lookup_rows("//tmp/t_output", [{"k1": 1}]) == expected
            assert_items_equal(select_rows("* from [//tmp/t_output]"), expected)
            assert_items_equal(read_table("//tmp/t_output"), expected)

        self._run_operation(self._prepare_write_row(
            self._make_value("v1", 1),
            self._make_value("v2", 1),
            k1=1))
        _verify(1, 1)

        # v2 is explicitly missing.
        self._run_operation(self._prepare_write_row(
            self._make_value("v1", 2),
            self._make_value("v2", None),
            k1=1))
        _verify(2, 1)

        # v1 is not marked as missing thus is written as Null.
        self._run_operation(self._prepare_write_row(
            self._make_value("v2", 3),
            k1=1))
        _verify(None, 3)

    def test_null_flags(self):
        sync_create_cells(1)
        schema = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "int64"},
        ]
        self._create_simple_dynamic_table("//tmp/t_output", schema=schema)
        sync_mount_table("//tmp/t_output")

        def _verify(expected_value):
            expected = [{"key": 1, "value": expected_value}]
            assert lookup_rows("//tmp/t_output", [{"key": 1}]) == expected
            assert_items_equal(select_rows("* from [//tmp/t_output]"), expected)
            assert_items_equal(read_table("//tmp/t_output"), expected)

        self._run_operation(self._prepare_write_row(
            self._make_value("value", 1, treat_empty_flags_as_null=True),
            key=1))
        _verify(1)

        self._run_operation(self._prepare_write_row(
            self._make_value("value", 2, treat_empty_flags_as_null=False),
            key=1))
        _verify(2)

    def test_computed_columns(self):
        sync_create_cells(1)
        schema = [
            {"name": "hash", "type": "int64", "sort_order": "ascending", "expression": "key * 2"},
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "int64"},
        ]
        self._create_simple_dynamic_table("//tmp/t_output", schema=schema)
        sync_mount_table("//tmp/t_output")

        insert_rows("//tmp/t_output", [{"key": 1, "value": 1}])
        self._run_operation([
            self._prepare_delete_row(key=1),
            self._prepare_write_row(self._make_value("value", 2), key=2)])

        assert_items_equal(select_rows("* from [//tmp/t_output]"), [{"hash": 4, "key": 2, "value": 2}])

    def test_not_sorted(self):
        sync_create_cells(1)
        self._create_simple_dynamic_table("//tmp/t_output")
        sync_mount_table("//tmp/t_output")

        with raises_yt_error(UniqueKeyViolation):
            self._run_operation([
                self._prepare_write_row(key=1),
                self._prepare_delete_row(key=1)])

        with raises_yt_error(SortOrderViolation):
            self._run_operation([
                self._prepare_delete_row(key=2),
                self._prepare_write_row(key=1)])

    @pytest.mark.parametrize("mode", ["sorted", "ordered"])
    @pytest.mark.parametrize("use_schema", [True, False])
    def test_merge(self, mode, use_schema):
        sync_create_cells(1)
        self._create_simple_dynamic_table("//tmp/t_output")
        sync_mount_table("//tmp/t_output")

        rows = [
            self._prepare_write_row(self._make_value("value", "1"), key=1),
            self._prepare_write_row(self._make_value("value", "2"), key=2),
        ]
        flat_rows = [
            {"key": 1, "value": "1"},
            {"key": 2, "value": "2"},
        ]
        create("table", "//tmp/t_input")
        if use_schema:
            alter_table(
                "//tmp/t_input",
                schema=get("//tmp/t_output/@schema"),
                schema_modification="unversioned_update")
        write_table("//tmp/t_input", rows, sorted_by=["key"])

        merge(
            in_="//tmp/t_input",
            out="<append=%true;schema_modification=unversioned_update>//tmp/t_output",
            mode=mode)

        assert read_table("//tmp/t_output") == flat_rows
        assert_items_equal(select_rows("* from [//tmp/t_output]"), flat_rows)

    @pytest.mark.parametrize("use_schema", [True, False])
    def test_sort(self, use_schema):
        sync_create_cells(1)
        self._create_simple_dynamic_table("//tmp/t_output")
        sync_mount_table("//tmp/t_output")

        rows = [
            self._prepare_write_row(self._make_value("value", "2"), key=2),
            self._prepare_write_row(self._make_value("value", "1"), key=1),
        ]
        flat_rows = [
            {"key": 2, "value": "2"},
            {"key": 1, "value": "1"},
        ]
        create("table", "//tmp/t_input")
        if use_schema:
            alter_table(
                "//tmp/t_input",
                schema=get("//tmp/t_output/@schema"),
                schema_modification="unversioned_update_unsorted")
        write_table("//tmp/t_input", rows)

        sort(
            in_="//tmp/t_input",
            out="<append=%true;schema_modification=unversioned_update>//tmp/t_output",
            sort_by=["key"])

        assert read_table("//tmp/t_output") == sorted(flat_rows)
        assert_items_equal(select_rows("* from [//tmp/t_output]"), sorted(flat_rows))

    def test_sort_with_intermediate_table(self):
        sync_create_cells(1)
        self._create_simple_dynamic_table("//tmp/t_output")
        sync_mount_table("//tmp/t_output")

        rows = [
            self._prepare_write_row(self._make_value("value", "2"), key=2),
            self._prepare_write_row(self._make_value("value", "1"), key=1),
        ]
        flat_rows = [
            {"key": 2, "value": "2"},
            {"key": 1, "value": "1"},
        ]

        create("table", "//tmp/t_input")
        alter_table(
            "//tmp/t_input",
            schema=get("//tmp/t_output/@schema"),
            schema_modification="unversioned_update_unsorted")
        write_table("//tmp/t_input", rows)

        create("table", "//tmp/t_intermediate")
        alter_table(
            "//tmp/t_intermediate",
            schema=get("//tmp/t_output/@schema"),
            schema_modification="unversioned_update")

        sort(
            in_="//tmp/t_input",
            out="//tmp/t_intermediate",
            sort_by=["key"])
        merge(
            in_="//tmp/t_intermediate",
            out="<append=%true;schema_modification=unversioned_update>//tmp/t_output",
            mode="ordered")

        assert read_table("//tmp/t_output") == sorted(flat_rows)
        assert_items_equal(select_rows("* from [//tmp/t_output]"), sorted(flat_rows))

    def test_merge_from_original_schema_fails(self):
        sync_create_cells(1)
        schema = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "uint64"},
        ]

        self._create_simple_dynamic_table("//tmp/t_output", schema=schema)
        set("//tmp/t_output/@enable_compaction_and_partitioning", False)
        sync_mount_table("//tmp/t_output")
        create("table", "//tmp/t_input", attributes={"schema": get("//tmp/t_output/@schema")})

        rows = [{"key": 1, "value": yson.YsonUint64(1)}]
        write_table("//tmp/t_input", rows)

        with pytest.raises(YtError):
            merge(
                in_="//tmp/t_input",
                out="<append=%true;schema_modification=unversioned_update>//tmp/t_output",
                mode="ordered",
                spec={"max_failed_job_count": 1})

    def _assert_schema_equal(self, lhs, rhs, reordered=False):
        lhs = deepcopy(list(lhs))
        rhs = deepcopy(list(rhs))

        def _normalize_column(column):
            for k in ["type_v2", "type_v3"]:
                if k in column:
                    column.pop(k)
            column.setdefault("required", False)

        for column in lhs:
            _normalize_column(column)
        for column in rhs:
            _normalize_column(column)

        if reordered:
            assert_items_equal(lhs, rhs)
        else:
            assert lhs == rhs

    @pytest.mark.parametrize("modification", ["unversioned_update", "unversioned_update_unsorted"])
    def test_intermediate_table_schema_alter(self, modification):
        schema = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "v1", "type": "string"},
            {"name": "v2", "type": "double", "required": True},
        ]
        unversioned_update_schema = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "$change_type", "type": "uint64", "required": True},
            {"name": "$value:v1", "type": "string"},
            {"name": "$flags:v1", "type": "uint64"},
            {"name": "$value:v2", "type": "double"},
            {"name": "$flags:v2", "type": "uint64"},
        ]
        if modification == "unversioned_update_unsorted":
            unversioned_update_schema[0].pop("sort_order")

        create("table", "//tmp/t", attributes={"schema": schema})
        alter_table("//tmp/t", schema_modification=modification)
        self._assert_schema_equal(get("//tmp/t/@schema"), unversioned_update_schema)
        assert get("//tmp/t/@schema/@schema_modification") == modification

        # Cannot alter table with modified schema.
        with pytest.raises(YtError):
            alter_table("//tmp/t")

        create("table", "//tmp/t", force=True)
        alter_table("//tmp/t", schema=schema, schema_modification=modification)
        self._assert_schema_equal(get("//tmp/t/@schema"), unversioned_update_schema)
        assert get("//tmp/t/@schema/@schema_modification") == modification

        # Cannot apply modification to dynamic table.
        create("table", "//tmp/t", force=True, attributes={"schema": schema, "dynamic": True})
        with pytest.raises(YtError):
            alter_table("//tmp/t", schema_modification=modification)

        # Cannot create table with modified schema.
        with pytest.raises(YtError):
            create("table", "//tmp/t", force=True, attributes={
                "schema": make_schema(schema, schema_modification=modification)})

        # Cannot modify schema of nonempty table.
        create("table", "//tmp/t", force=True, schema=schema)
        write_table("//tmp/t", [{"key": 1, "v1": "abc", "v2": 0.5}])
        with pytest.raises(YtError):
            alter_table("//tmp/t", schema_modification=modification)

        # Cannot modify weak schema.
        create("table", "//tmp/t", force=True)
        sort(in_="//tmp/t",
             out="//tmp/t",
             sort_by="a")
        with pytest.raises(YtError):
            alter_table("//tmp/t", schema_modification=modification)

        # Cannot modify non-strict schema.
        create("table", "//tmp/t", force=True, attributes={
            "schema": make_schema(schema, strict=False)})
        with pytest.raises(YtError):
            alter_table("//tmp/t", schema_modification=modification)

        # Cannot modify schema of unsorted table.
        create("table", "//tmp/t", force=True, attributes={"schema": [{"name": "a", "type": "int64"}]})
        with pytest.raises(YtError):
            alter_table("//tmp/t", schema_modification=modification)

        # unique_keys is preserved.
        create("table", "//tmp/t", force=True, attributes={
            "schema": make_schema(schema, unique_keys=True)})
        alter_table("//tmp/t", schema_modification=modification)
        assert get("//tmp/t/@schema/@unique_keys") == (modification == "unversioned_update")

    @pytest.mark.parametrize("modification", ["unversioned_update", "unversioned_update_unsorted"])
    def test_unversioned_update_schema_inference(self, modification):
        schema = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "string"},
        ]
        unversioned_update_schema = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "$change_type", "type": "uint64", "required": True},
            {"name": "$value:value", "type": "string"},
            {"name": "$flags:value", "type": "uint64"},
        ]
        if modification == "unversioned_update_unsorted":
            unversioned_update_schema[0].pop("sort_order")
        sorted_schema = deepcopy(unversioned_update_schema)
        sorted_schema[0]["sort_order"] = "ascending"

        create("table", "//tmp/t", attributes={"schema": schema})
        alter_table("//tmp/t", schema_modification=modification)
        write_table("//tmp/t", [{"key": 1, "$change_type": 0}])

        create("table", "//tmp/t_sorted")
        sort(
            in_="//tmp/t",
            out="//tmp/t_sorted",
            sort_by=["key"])
        self._assert_schema_equal(get("//tmp/t_sorted/@schema"), sorted_schema, reordered=True)
        assert get("//tmp/t_sorted/@schema/@schema_modification") == modification

        create("table", "//tmp/t_merged")
        merge(
            in_="//tmp/t",
            out="//tmp/t_merged",
            mode="ordered")
        self._assert_schema_equal(get("//tmp/t_merged/@schema"), unversioned_update_schema, reordered=True)
        assert get("//tmp/t_merged/@schema/@schema_modification") == modification

##################################################################

class TestBulkInsertMulticell(TestBulkInsert):
    NUM_SECONDARY_MASTER_CELLS = 2

class TestUnversionedUpdateFormatRpcProxy(TestUnversionedUpdateFormat):
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True
