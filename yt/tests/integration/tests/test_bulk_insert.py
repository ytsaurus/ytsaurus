import pytest

from test_dynamic_tables import DynamicTablesBase

from yt_env_setup import YTEnvSetup, unix_only, wait, parametrize_external, Restarter,\
    NODES_SERVICE, MASTER_CELL_SERVICE
from yt_commands import *

from yt.environment.helpers import assert_items_equal

from yt.test_helpers import assert_items_equal

from flaky import flaky

from time import sleep

import __builtin__

##################################################################

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

    @authors("ifsmirnov")
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

        map(
            in_="//tmp/t_input",
            out="<append=true>//tmp/t_output",
            command="cat")

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

        wait(lambda: get("//tmp/t_output/@chunk_count") == 1)

    @authors("ifsmirnov")
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

        assert get("//tmp/t_output/@chunk_count") == 0

    @authors("ifsmirnov")
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

        assert get("//tmp/t_output/@chunk_count") == 0

    @authors("ifsmirnov")
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

    @authors("ifsmirnov")
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

    @authors("ifsmirnov")
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

    @authors("ifsmirnov")
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

        insert_rows("//tmp/t_output", [rows[2]])

        assert_items_equal(select_rows("* from [//tmp/t_output]"), rows)

        sync_compact_table("//tmp/t_output")
        assert read_table("//tmp/t_output") == rows

    @authors("ifsmirnov")
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

    @authors("ifsmirnov")
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
                dont_track=True)
            operations.append(op)

        for op in operations:
            op.wait_for_state("completed")

        assert read_table("//tmp/t_output") == [{"key": i, "value": str(i)} for i in range(len(operations))]
        assert_items_equal(select_rows("* from [//tmp/t_output]"), [{"key": i, "value": str(i)} for i in range(len(operations))])

    @authors("ifsmirnov")
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

    @authors("ifsmirnov")
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
        subtablet_chunk_list = get("#{}/@child_ids/0".format(tablet_chunk_list))
        assert get("#{}/@type".format(subtablet_chunk_list)) == "chunk_list"

        set("//tmp/t_output/@enable_compaction_and_partitioning", True)
        sync_compact_table("//tmp/t_output")

        wait(lambda: not exists("#{}".format(subtablet_chunk_list)))

    @authors("ifsmirnov")
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
            dont_track=True)

        op.wait_for_state("completing")
        tx = get(op.get_path() + "/@output_transaction_id")
        abort_transaction(tx)
        op.wait_for_state("failed")

        assert get("//tmp/t_output/@chunk_count") == 0
        assert_items_equal(select_rows("* from [//tmp/t_output]"), [rows[1]])
        insert_rows("//tmp/t_output", [rows[2]])
        assert_items_equal(select_rows("* from [//tmp/t_output]"), rows[1:3])

    @authors("ifsmirnov")
    def test_competing_tablet_transaction_lost(self):
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

        write_table("//tmp/t_input", [rows[0]])

        tablet_tx = start_transaction(type="tablet")
        insert_rows("//tmp/t_output", [rows[1]], tx=tablet_tx)

        op = map(
            in_="//tmp/t_input",
            out="<append=true>//tmp/t_output",
            command="sleep 5; cat")

        with pytest.raises(YtError):
            commit_transaction(tablet_tx)

        assert_items_equal(select_rows("* from [//tmp/t_output]"), rows[:1])

    # TODO(ifsmirnov): I promise to do it tomorrow (or at least in August).
    def _test_competing_tablet_transaction_won(self):
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

        write_table("//tmp/t_input", [rows[0]])

        tablet_tx = start_transaction(type="tablet")
        insert_rows("//tmp/t_output", [rows[1]], tx=tablet_tx)

        op = map(
            in_="//tmp/t_input",
            out="<append=true>//tmp/t_output",
            command="cat",
            dont_track=True)

        def _get_locks():
            return get("//sys/nodes/{}/orchid/tablet_cells/{}/tablets/{}/dynamic_table_locks".format(
                node, cell_id, tablet_id))

        wait(lambda: _get_locks())
        locks = _get_locks()
        assert len(locks) == 1
        bulk_insert_tx = locks.keys()[0]
        assert locks[bulk_insert_tx]["confirmed"] == True
        lock_timestamp  = locks[bulk_insert_tx]["timestamp"]

        commit_transaction(tablet_tx)

        op.wait_for_state("completed")
        assert_items_equal(select_rows("* from [//tmp/t_output]"), rows)

        rows = lookup_rows("//tmp/t_output", [{"key": 1}], versioned=True)
        assert len(rows) == 1
        row = rows[0]
        assert len(row.attributes["write_timestamps"]) == 1
        assert row.attributes["write_timestamps"][0] == lock_timestamp

##################################################################

class TestBulkInsertMulticell(TestBulkInsert):
    NUM_SECONDARY_MASTER_CELLS = 2
