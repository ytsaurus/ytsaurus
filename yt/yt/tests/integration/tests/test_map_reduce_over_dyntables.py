from yt_env_setup import (
    YTEnvSetup,
    wait,
    parametrize_external,
)
from yt_commands import (  # noqa
    authors, print_debug, wait, retry, wait_assert, wait_breakpoint, release_breakpoint, with_breakpoint,
    events_on_fs, reset_events_on_fs,
    create, ls, get, set, copy, move, remove, link, exists, concatenate,
    create_account, create_network_project, create_tmpdir, create_user, create_group, create_medium,
    create_pool, create_pool_tree, remove_pool_tree,
    create_data_center, create_rack, create_table,
    create_tablet_cell_bundle, remove_tablet_cell_bundle, create_tablet_cell, create_table_replica,
    make_ace, check_permission, add_member, remove_member, remove_group, remove_user,
    remove_network_project,
    make_batch_request, execute_batch, get_batch_error,
    start_transaction, abort_transaction, commit_transaction, lock,
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
    get_statistics, get_recursive_disk_space, get_chunk_owner_disk_space,
    make_random_string, raises_yt_error,
    build_snapshot, gc_collect, is_multicell,
    get_driver, execute_command,
    AsyncLastCommittedTimestamp, MinTimestamp)

from yt_type_helpers import make_schema

from yt.test_helpers import assert_items_equal
from yt.common import YtError
import yt.yson as yson

import pytest

import base64
import random
import time

##################################################################


class TestMapOnDynamicTables(YTEnvSetup):
    NUM_TEST_PARTITIONS = 3

    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1
    USE_DYNAMIC_TABLES = True

    def _create_simple_dynamic_table(self, path, sort_order="ascending", **attributes):
        if "schema" not in attributes:
            attributes.update(
                {
                    "schema": [
                        {"name": "key", "type": "int64", "sort_order": sort_order},
                        {"name": "value", "type": "string"},
                    ]
                }
            )
        create_dynamic_table(path, **attributes)

    @authors("savrus")
    @parametrize_external
    @pytest.mark.parametrize("optimize_for", ["lookup", "scan"])
    @pytest.mark.parametrize("sort_order", [None, "ascending"])
    @pytest.mark.parametrize("ordered", [False, True])
    def test_map_on_dynamic_table(self, external, ordered, sort_order, optimize_for):
        sync_create_cells(1)
        self._create_simple_dynamic_table(
            "//tmp/t",
            sort_order=sort_order,
            optimize_for=optimize_for,
            external=external,
        )
        set("//tmp/t/@min_compaction_store_count", 5)
        create("table", "//tmp/t_out")

        rows = [{"key": i, "value": str(i)} for i in range(10)]
        sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", rows)
        sync_unmount_table("//tmp/t")

        map(in_="//tmp/t", out="//tmp/t_out", ordered=ordered, command="cat")

        assert_items_equal(read_table("//tmp/t_out"), rows)

        rows1 = [{"key": i, "value": str(i + 1)} for i in range(3)]
        sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", rows1)
        sync_unmount_table("//tmp/t")

        rows2 = [{"key": i, "value": str(i + 2)} for i in range(2, 6)]
        sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", rows2)
        sync_unmount_table("//tmp/t")

        rows3 = [{"key": i, "value": str(i + 3)} for i in range(7, 8)]
        sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", rows3)
        sync_unmount_table("//tmp/t")

        assert len(get("//tmp/t/@chunk_ids")) == 4

        def update(new):
            def update_row(row):
                if sort_order == "ascending":
                    for r in rows:
                        if r["key"] == row["key"]:
                            r["value"] = row["value"]
                            return
                rows.append(row)

            for row in new:
                update_row(row)

        update(rows1)
        update(rows2)
        update(rows3)

        map(in_="//tmp/t", out="//tmp/t_out", ordered=ordered, command="cat")

        assert_items_equal(read_table("//tmp/t_out"), rows)

    @authors("savrus")
    @parametrize_external
    @pytest.mark.parametrize("optimize_for", ["lookup", "scan"])
    def test_sorted_dynamic_table_as_user_file(self, external, optimize_for):
        sync_create_cells(1)
        self._create_simple_dynamic_table("//tmp/t", optimize_for=optimize_for, external=external)
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")

        rows = [{"key": i, "value": str(i)} for i in range(5)]
        sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", rows)
        sync_unmount_table("//tmp/t")

        rows1 = [{"key": i, "value": str(i + 1)} for i in range(3, 8)]
        sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", rows1)
        sync_unmount_table("//tmp/t")

        write_table("//tmp/t_in", [{"a": "b"}])

        map(
            in_="//tmp/t_in",
            out="//tmp/t_out",
            file=["<format=<format=text>yson>//tmp/t"],
            command="cat t",
            spec={"mapper": {"format": yson.loads("<format=text>yson")}},
        )

        def update(new):
            def update_row(row):
                for r in rows:
                    if r["key"] == row["key"]:
                        r["value"] = row["value"]
                        return
                rows.append(row)

            for row in new:
                update_row(row)

        update(rows1)
        rows = sorted(rows, key=lambda r: r["key"])
        assert read_table("//tmp/t_out") == rows

    @authors("savrus")
    @parametrize_external
    def test_ordered_dynamic_table_as_user_file(self, external):
        sync_create_cells(1)
        self._create_simple_dynamic_table("//tmp/t", sort_order=None, external=external)
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")

        rows = [{"key": i, "value": str(i)} for i in range(5)]
        sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", rows)
        sync_unmount_table("//tmp/t")

        rows1 = [{"key": i, "value": str(i + 1)} for i in range(3, 8)]
        sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", rows1)
        sync_unmount_table("//tmp/t")

        write_table("//tmp/t_in", [{"a": "b"}])

        map(
            in_="//tmp/t_in",
            out="//tmp/t_out",
            file=["<format=<format=text>yson>//tmp/t"],
            command="cat t",
            spec={"mapper": {"format": yson.loads("<format=text>yson")}},
        )

        assert read_table("//tmp/t_out") == rows + rows1

    @authors("gritukan")
    def test_do_not_fetch_dynamic_stores_for_user_file(self):
        sync_create_cells(1)
        self._create_simple_dynamic_table("//tmp/t", dynamic_store_auto_flush_period=yson.YsonEntity())
        set("//tmp/t/@enable_dynamic_store_read", True)
        sync_mount_table("//tmp/t")
        flushed_rows = [{"key": i, "value": "foo"} for i in range(1, 10, 2)]
        insert_rows("//tmp/t", flushed_rows)
        sync_flush_table("//tmp/t")
        unflushed_rows = [{"key": i, "value": "foo"} for i in range(0, 10, 2)]
        insert_rows("//tmp/t", unflushed_rows)

        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")

        write_table("//tmp/t_in", [{"a": "b"}])

        with pytest.raises(YtError):
            map(
                in_="//tmp/t_in",
                out="//tmp/t_out",
                file=["<format=<format=text>yson>//tmp/t"],
                command="cat t",
                spec={"mapper": {"format": yson.loads("<format=text>yson")}},
            )

    @authors("savrus")
    @parametrize_external
    def test_dynamic_table_timestamp(self, external):
        sync_create_cells(1)
        self._create_simple_dynamic_table("//tmp/t", external=external, enable_dynamic_store_read=False)
        create("table", "//tmp/t_out")

        rows = [{"key": i, "value": str(i)} for i in range(2)]
        sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", rows)

        time.sleep(1)
        ts = generate_timestamp()

        sync_flush_table("//tmp/t")
        insert_rows("//tmp/t", [{"key": i, "value": str(i + 1)} for i in range(2)])
        sync_flush_table("//tmp/t")
        sync_compact_table("//tmp/t")

        map(in_="<timestamp=%s>//tmp/t" % ts, out="//tmp/t_out", command="cat")

        assert_items_equal(read_table("//tmp/t_out"), rows)

        with pytest.raises(YtError):
            map(
                in_="<timestamp=%s>//tmp/t" % MinTimestamp,
                out="//tmp/t_out",
                command="cat",
            )

        insert_rows("//tmp/t", rows)

        with pytest.raises(YtError):
            map(
                in_="<timestamp=%s>//tmp/t" % generate_timestamp(),
                out="//tmp/t_out",
                command="cat",
            )

    @authors("ifsmirnov")
    def test_retention_timestamp(self):
        sync_create_cells(1)
        self._create_simple_dynamic_table(
            "//tmp/t",
            schema=make_schema(
                [
                    {"name": "k", "type": "int64", "sort_order": "ascending"},
                    {"name": "u", "type": "string"},
                    {"name": "v", "type": "string"},
                ],
                unique_keys=True,
            ),
        )

        sync_mount_table("//tmp/t")

        insert_rows("//tmp/t", [{"k": 1, "u": "u1", "v": "v1"}])
        ts2 = generate_timestamp()
        insert_rows("//tmp/t", [{"k": 1, "u": "u2"}], update=True)
        insert_rows("//tmp/t", [{"k": 2, "v": "v3"}])
        ts3 = generate_timestamp()

        sync_unmount_table("//tmp/t")

        expected1 = [
            {"k": 1, "u": "u2", "v": "v1"},
            {"k": 2, "u": yson.YsonEntity(), "v": "v3"},
        ]
        expected2 = [
            {"k": 1, "u": "u2", "v": yson.YsonEntity()},
            {"k": 2, "u": yson.YsonEntity(), "v": "v3"},
        ]

        assert read_table("//tmp/t") == expected1
        assert read_table("<retention_timestamp={}>//tmp/t".format(ts2)) == expected2
        assert read_table("<retention_timestamp={}>//tmp/t".format(ts3)) == []

        create("table", "//tmp/t_out")
        map(
            in_="<retention_timestamp={}>//tmp/t".format(ts2),
            out="//tmp/t_out",
            command="cat",
        )

        assert read_table("//tmp/t_out") == expected2

    @authors("ifsmirnov")
    def test_retention_timestamp_bounds(self):
        sync_create_cells(1)
        self._create_simple_dynamic_table("//tmp/t")
        sync_mount_table("//tmp/t")
        sync_unmount_table("//tmp/t")
        ts1 = generate_timestamp()
        ts2 = generate_timestamp()
        with pytest.raises(YtError):
            read_table("<timestamp={};retention_timestamp={}>//tmp/t".format(ts1, ts2))
        with pytest.raises(YtError):
            read_table("<timestamp={};retention_timestamp={}>//tmp/t".format(ts1, ts1))

    @authors("ifsmirnov")
    def test_retention_timestamp_with_timestamp(self):
        sync_create_cells(1)
        self._create_simple_dynamic_table("//tmp/t")
        sync_mount_table("//tmp/t")

        insert_rows("//tmp/t", [{"key": 1, "value": "1"}])
        insert_rows("//tmp/t", [{"key": 2, "value": "2"}])
        delete_rows("//tmp/t", [{"key": 2}])
        ts1 = generate_timestamp()
        insert_rows("//tmp/t", [{"key": 2, "value": "2"}])
        ts2 = generate_timestamp()
        insert_rows("//tmp/t", [{"key": 3, "value": "3"}])

        sync_unmount_table("//tmp/t")

        actual = read_table("<timestamp={};retention_timestamp={}>//tmp/t".format(ts2, ts1))
        assert actual == [{"key": 2, "value": "2"}]

    @authors("savrus")
    @parametrize_external
    @pytest.mark.parametrize("optimize_for", ["lookup", "scan"])
    def test_dynamic_table_input_data_statistics(self, external, optimize_for):
        sync_create_cells(1)
        self._create_simple_dynamic_table("//tmp/t", optimize_for=optimize_for, external=external)
        create("table", "//tmp/t_out")

        rows = [{"key": i, "value": str(i)} for i in range(2)]
        sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", rows)
        sync_unmount_table("//tmp/t")

        op = map(in_="//tmp/t", out="//tmp/t_out", command="cat")

        statistics = get(op.get_path() + "/@progress/job_statistics")
        assert get_statistics(statistics, "data.input.chunk_count.$.completed.map.sum") == 1
        assert get_statistics(statistics, "data.input.row_count.$.completed.map.sum") == 2
        assert get_statistics(statistics, "data.input.uncompressed_data_size.$.completed.map.sum") > 0
        assert get_statistics(statistics, "data.input.compressed_data_size.$.completed.map.sum") > 0
        assert get_statistics(statistics, "data.input.data_weight.$.completed.map.sum") > 0

    @authors("savrus")
    @parametrize_external
    @pytest.mark.parametrize("optimize_for", ["lookup", "scan"])
    def test_dynamic_table_column_filter(self, optimize_for, external):
        sync_create_cells(1)
        create(
            "table",
            "//tmp/t",
            attributes={
                "external": external,
                "optimize_for": optimize_for,
                "schema": make_schema(
                    [
                        {"name": "k", "type": "int64", "sort_order": "ascending"},
                        {"name": "u", "type": "int64"},
                        {"name": "v", "type": "int64"},
                    ],
                    unique_keys=True,
                ),
            },
        )
        create("table", "//tmp/t_out")

        row = {"k": 0, "u": 1, "v": 2}
        write_table("//tmp/t", [row])
        alter_table("//tmp/t", dynamic=True)

        def get_data_size(statistics):
            return {
                "uncompressed_data_size": get_statistics(
                    statistics, "data.input.uncompressed_data_size.$.completed.map.sum"
                ),
                "compressed_data_size": get_statistics(
                    statistics, "data.input.compressed_data_size.$.completed.map.sum"
                ),
            }

        op = map(in_="//tmp/t", out="//tmp/t_out", command="cat")
        stat1 = get_data_size(get(op.get_path() + "/@progress/job_statistics"))
        assert read_table("//tmp/t_out") == [row]

        # FIXME(savrus) investigate test flapping
        print_debug(get("//tmp/t/@compression_statistics"))

        for columns in (["k"], ["u"], ["v"], ["k", "u"], ["k", "v"], ["u", "v"]):
            op = map(
                in_="<columns=[{0}]>//tmp/t".format(";".join(columns)),
                out="//tmp/t_out",
                command="cat",
            )
            stat2 = get_data_size(get(op.get_path() + "/@progress/job_statistics"))
            assert read_table("//tmp/t_out") == [{c: row[c] for c in columns}]

            if columns == ["u", "v"] or optimize_for == "lookup":
                assert stat1["uncompressed_data_size"] == stat2["uncompressed_data_size"]
                assert stat1["compressed_data_size"] == stat2["compressed_data_size"]
            else:
                assert stat1["uncompressed_data_size"] > stat2["uncompressed_data_size"]
                assert stat1["compressed_data_size"] > stat2["compressed_data_size"]

    @authors("ifsmirnov")
    def test_bizarre_column_filters(self):
        sync_create_cells(1)
        self._create_simple_dynamic_table("//tmp/t")
        sync_mount_table("//tmp/t")

        rows = (
            [{"key": yson.YsonEntity(), "value": "none"}]
            + [{"key": yson.YsonInt64(i), "value": str(i * i)} for i in range(2)]
            + [{"key": 100500, "value": yson.YsonEntity()}]
        )
        insert_rows("//tmp/t", rows)
        sync_unmount_table("//tmp/t")

        def _check(*columns):
            expected = [{column: row[column] for column in columns if column in row} for row in rows]
            actual = read_table("//tmp/t{" + ",".join(columns) + "}")
            assert expected == actual

        _check("key")
        _check("value")
        _check("key", "key")
        _check("value", "key")
        _check("value", "value", "key")
        _check("value", "key", "value", "key")
        _check("oops")
        _check("oops", "yup")
        _check("oops", "value", "yup")
        _check("oops", "value", "key")

    @authors("savrus")
    @pytest.mark.parametrize("optimize_for", ["lookup", "scan"])
    def test_rename_columns_dynamic_table_simple(self, optimize_for):
        sync_create_cells(1)
        self._create_simple_dynamic_table("//tmp/t", optimize_for=optimize_for)
        create("table", "//tmp/t_out")

        sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", [{"key": 1, "value": str(2)}])
        sync_unmount_table("//tmp/t")

        map(
            in_="<rename_columns={key=first;value=second}>//tmp/t",
            out="//tmp/t_out",
            command="cat",
        )

        assert read_table("//tmp/t_out") == [{"first": 1, "second": str(2)}]

    def _print_chunk_list_recursive(self, chunk_list):
        result = []

        def recursive(chunk_list, level):
            t = get("#{0}/@type".format(chunk_list))
            result.append([level, chunk_list, t, None, None])
            if t == "chunk":
                r = get("#{0}/@row_count".format(chunk_list))
                u = get("#{0}/@uncompressed_data_size".format(chunk_list))
                result[-1][3] = {"row_count": r, "data_size": u}
            if t == "chunk_list":
                s = get("#{0}/@statistics".format(chunk_list))
                # cs = get("#{0}/@cumulative_statistics".format(chunk_list))
                cs = None
                result[-1][3] = s
                result[-1][4] = cs
                for c in get("#{0}/@child_ids".format(chunk_list)):
                    recursive(c, level + 1)

        recursive(chunk_list, 0)
        for r in result:
            print "%s%s %s %s %s" % ("   " * r[0], r[1], r[2], r[3], r[4])


##################################################################


class TestMapOnDynamicTablesMulticell(TestMapOnDynamicTables):
    NUM_SECONDARY_MASTER_CELLS = 2


class TestMapOnDynamicTablesPortal(TestMapOnDynamicTablesMulticell):
    ENABLE_TMP_PORTAL = True


##################################################################


class MROverOrderedDynTablesHelper(YTEnvSetup):
    CONTROL_ATTRIBUTES_SPEC = {
        "control_attributes": {
            "enable_tablet_index": True,
            "enable_range_index": True,
            "enable_row_index": True,
            "enable_table_index": True,
        }
    }

    @staticmethod
    def _run_map_operation(input_ranges, input_table_name="//tmp/t"):
        map(
            in_=["{0}{1}".format(input_ranges, input_table_name)],
            out="//tmp/t_out",
            command="./script.py",
            file=["//tmp/script.py"],
            spec={
                "job_count": 1,
                "job_io": MROverOrderedDynTablesHelper.CONTROL_ATTRIBUTES_SPEC,
                "max_failed_job_count": 1,
                "mapper": {
                    "format": yson.loads("<format=text>yson"),
                },
            },
        )

    @staticmethod
    def _run_map_reduce_operation(input_ranges, input_table_name="//tmp/t"):
        map_reduce(
            sort_by="key",
            reduce_by="key",
            in_=["{0}{1}".format(input_ranges, input_table_name)],
            out="//tmp/t_out",
            mapper_command="./script.py",
            reducer_command="cat",
            mapper_file=["//tmp/script.py"],
            spec={
                "job_count": 1,
                "max_failed_job_count": 1,
                "mapper": {
                    "format": yson.loads("<format=text>yson"),
                },
                "reducer": {
                    "format": yson.loads("<format=text>yson"),
                },
                "map_job_io": MROverOrderedDynTablesHelper.CONTROL_ATTRIBUTES_SPEC,
            },
        )

    @staticmethod
    def _insert_chunk(first_value, tablet_index):
        rows = [{"$tablet_index": tablet_index, "key": i} for i in range(first_value, first_value + 3)]
        insert_rows("//tmp/t", rows)
        sync_flush_table("//tmp/t")

    @staticmethod
    def _validate_output(expected_content):
        output_table = read_table("//tmp/t_out")
        if not expected_content:
            assert not output_table
            return

        job_input = output_table[0]["out"]
        job_input = base64.standard_b64decode(job_input)
        actual_content = []
        current_attrs = {}
        for row in yson.loads(job_input, yson_type="list_fragment"):
            if type(row) == yson.yson_types.YsonEntity:
                for key, value in row.attributes.iteritems():
                    # row_index is set only once per sequence of contiguous chunks,
                    # but chunks are written asynchronously, so output row_index values may vary
                    if key == "row_index":
                        continue
                    current_attrs[key] = value
            else:
                new_row = dict(row)
                new_row.update(current_attrs.iteritems())
                actual_content.append(new_row)

        assert sorted(expected_content) == sorted(actual_content)

    @staticmethod
    def _prologue(shard_count, optimize_for):
        sync_create_cells(1)
        schema = [{"name": "key", "type": "int64"}]
        create_dynamic_table("//tmp/t", schema=schema, optimize_for=optimize_for, enable_dynamic_store_read=False)
        sync_reshard_table("//tmp/t", shard_count)
        sync_mount_table("//tmp/t")

        schema2 = make_schema([{"name": "key", "type": "int64"}], unique_keys=True, strict=True)
        create("table", "//tmp/t_out", schema=schema2)

        script = "\n".join(
            [
                "#!/usr/bin/python",
                "import sys",
                "import base64",
                "print '{out=\"' + base64.standard_b64encode(sys.stdin.read()) + '\"}'",
            ]
        )
        create("file", "//tmp/script.py", attributes={"executable": True})
        write_file("//tmp/script.py", script)


class TestInputOutputForOrderedWithTabletIndex(MROverOrderedDynTablesHelper):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1
    USE_DYNAMIC_TABLES = True

    @authors("akozhikhov")
    @pytest.mark.parametrize("optimize_for", ["lookup", "scan"])
    def test_ordered_tablet_index_correctness(self, optimize_for):
        self._prologue(shard_count=2, optimize_for=optimize_for)

        self._insert_chunk(first_value=0, tablet_index=0)
        self._insert_chunk(first_value=4, tablet_index=1)

        with pytest.raises(YtError):
            self._run_map_operation("<ranges=[{exact={row_index=0}}]>")

        self._run_map_operation(
            "<ranges=["
            "{exact={tablet_index=2}};"
            "{exact={tablet_index=1; row_index=3}};"
            "{lower_limit={tablet_index=0; row_index=1}; upper_limit={tablet_index=0; row_index=0}};"
            "{lower_limit={tablet_index=0; row_index=3}; upper_limit={tablet_index=0; row_index=0}};"
            "{lower_limit={tablet_index=2; row_index=0}; upper_limit={tablet_index=3; row_index=1}};"
            "{lower_limit={tablet_index=2;}; upper_limit={tablet_index=3;}};"
            "]>"
        )
        self._validate_output([])

        self._run_map_operation(
            "<ranges=["
            "{lower_limit={tablet_index=0; row_index=1}; upper_limit={tablet_index=0; row_index=4}};"
            "{lower_limit={tablet_index=0; row_index=5}; upper_limit={tablet_index=1; row_index=2}};"
            "{lower_limit={tablet_index=1; row_index=1}; upper_limit={tablet_index=2; row_index=1}};"
            "{lower_limit={tablet_index=0;}; upper_limit={tablet_index=1;}};"
            "]>"
        )
        self._validate_output(
            [
                {"key": 1, "tablet_index": 0, "range_index": 0},
                {"key": 2, "tablet_index": 0, "range_index": 0},
                {"key": 4, "tablet_index": 1, "range_index": 1},
                {"key": 5, "tablet_index": 1, "range_index": 1},
                {"key": 5, "tablet_index": 1, "range_index": 2},
                {"key": 6, "tablet_index": 1, "range_index": 2},
                {"key": 0, "tablet_index": 0, "range_index": 3},
                {"key": 1, "tablet_index": 0, "range_index": 3},
                {"key": 2, "tablet_index": 0, "range_index": 3},
            ]
        )

        # tabletIndex is supported only for ordered tables
        schema = make_schema(
            [
                {"name": "key", "type": "int64", "sort_order": "ascending"},
                {"name": "value", "type": "string"},
            ]
        )
        create_dynamic_table("//tmp/sorted_t", schema=schema)
        sync_mount_table("//tmp/sorted_t")
        insert_rows("//tmp/sorted_t", [{"key": 1, "value": str(2)}])
        sync_unmount_table("//tmp/sorted_t")
        with pytest.raises(YtError):
            self._run_map_operation("<ranges=[{exact={tablet_index=0}}]>", input_table_name="//tmp/sorted_t")

    @authors("akozhikhov")
    @pytest.mark.parametrize("optimize_for", ["lookup", "scan"])
    def test_ordered_tablet_index_general(self, optimize_for):
        self._prologue(shard_count=3, optimize_for=optimize_for)

        self._insert_chunk(first_value=0, tablet_index=0)
        self._insert_chunk(first_value=3, tablet_index=0)
        self._insert_chunk(first_value=6, tablet_index=0)
        self._insert_chunk(first_value=9, tablet_index=1)
        self._insert_chunk(first_value=12, tablet_index=2)

        self._run_map_operation(
            "<ranges=["
            "{exact={tablet_index=1}};"
            "{exact={tablet_index=0; row_index=1}};"
            "{exact={tablet_index=0; row_index=5}};"
            "{lower_limit={tablet_index=0; row_index=4}; upper_limit={tablet_index=1; row_index=2}};"
            "{lower_limit={tablet_index=0; row_index=8}; upper_limit={tablet_index=2; row_index=2}}]>"
        )

        expected_content = [
            {"key": 9, "tablet_index": 1, "range_index": 0},
            {"key": 10, "tablet_index": 1, "range_index": 0},
            {"key": 11, "tablet_index": 1, "range_index": 0},
            {"key": 1, "tablet_index": 0, "range_index": 1},
            {"key": 5, "tablet_index": 0, "range_index": 2},
            # check multiple chunks and multiple tablet ranges
            {"key": 4, "tablet_index": 0, "range_index": 3},
            {"key": 5, "tablet_index": 0, "range_index": 3},
            {"key": 6, "tablet_index": 0, "range_index": 3},
            {"key": 7, "tablet_index": 0, "range_index": 3},
            {"key": 8, "tablet_index": 0, "range_index": 3},
            {"key": 9, "tablet_index": 1, "range_index": 3},
            {"key": 10, "tablet_index": 1, "range_index": 3},
            {"key": 8, "tablet_index": 0, "range_index": 4},
            {"key": 9, "tablet_index": 1, "range_index": 4},
            {"key": 10, "tablet_index": 1, "range_index": 4},
            {"key": 11, "tablet_index": 1, "range_index": 4},
            {"key": 12, "tablet_index": 2, "range_index": 4},
            {"key": 13, "tablet_index": 2, "range_index": 4},
        ]
        self._validate_output(expected_content)

        # Check two trim scenarios:
        # 1) a chunks is partly trimmed (it will be shown because of some trim mechanics specifics).
        # 2) a chunk is completely trimmed (and hence it won't be shown).
        trim_rows("//tmp/t", 0, 2)
        trim_rows("//tmp/t", 1, 2)

        self._run_map_operation(
            "<ranges=["
            "{lower_limit={tablet_index=0; row_index=0}; upper_limit={tablet_index=0; row_index=4}};"
            "{exact={tablet_index=1; row_index=1}}]>"
        )

        expected_content = [
            {"key": 0, "tablet_index": 0, "range_index": 0},
            {"key": 1, "tablet_index": 0, "range_index": 0},
            {"key": 2, "tablet_index": 0, "range_index": 0},
            {"key": 3, "tablet_index": 0, "range_index": 0},
            {"key": 10, "tablet_index": 1, "range_index": 1},
        ]
        self._validate_output(expected_content)

        assert get("//tmp/t/@chunk_count") == 5
        trim_rows("//tmp/t", 0, 4)
        wait(lambda: get("//tmp/t/@chunk_count") == 4)

        self._run_map_operation(
            "<ranges=["
            "{exact={tablet_index=0; row_index=1}};"
            "{lower_limit={tablet_index=0; row_index=2}; upper_limit={tablet_index=0; row_index=5}};"
            "{exact={tablet_index=1; row_index=1}}]>"
        )

        expected_content = [
            {"key": 3, "tablet_index": 0, "range_index": 1},
            {"key": 4, "tablet_index": 0, "range_index": 1},
            {"key": 10, "tablet_index": 1, "range_index": 2},
        ]
        self._validate_output(expected_content)

    @authors("akozhikhov")
    @pytest.mark.parametrize("optimize_for", ["lookup", "scan"])
    def test_ordered_tablet_index_multiple_parents(self, optimize_for):
        self._prologue(shard_count=2, optimize_for=optimize_for)

        self._insert_chunk(0, tablet_index=0)
        self._insert_chunk(3, tablet_index=1)

        sync_unmount_table("//tmp/t")
        copy("//tmp/t", "//tmp/t_copied")

        # Second tablet is not physically copied, so it has two different tabletIndex values
        # which correspond to two parents of this tablet.
        sync_reshard_table("//tmp/t", 2, first_tablet_index=0, last_tablet_index=0)

        self._run_map_operation("<ranges=[{exact={tablet_index=1}};]>", input_table_name="//tmp/t_copied")

        expected_content = [
            {"key": 3, "range_index": 0, "tablet_index": 1},
            {"key": 4, "range_index": 0, "tablet_index": 1},
            {"key": 5, "range_index": 0, "tablet_index": 1},
        ]

        self._validate_output(expected_content)

    @authors("ifsmirnov")
    def test_ordered_tablet_index_stress(self):
        sync_create_cells(1)
        create_dynamic_table(
            "//tmp/t",
            schema=[{"name": "key", "type": "int64"}],
            enable_dynamic_store_read=True,
            dynamic_store_auto_flush_period=yson.YsonEntity())

        random.seed(152314)

        tablet_count = 10
        max_row_count_per_chunk = 100
        chunk_count_per_tablet = 10
        request_count = 200

        sync_reshard_table("//tmp/t", tablet_count)
        sync_mount_table("//tmp/t")

        data = [[] for i in range(tablet_count)]

        data_gen = (i for i in xrange(10**9))

        for wave in range(chunk_count_per_tablet):
            rows = []
            for tablet_index in range(tablet_count):
                row_count = random.randint(0, max_row_count_per_chunk)
                for i in range(row_count):
                    x = data_gen.next()
                    data[tablet_index].append(x)
                    rows.append({"$tablet_index": tablet_index, "key": x})
            insert_rows("//tmp/t", rows)
            if wave + 1 < chunk_count_per_tablet:
                sync_flush_table("//tmp/t")

        table_reader = {
            "dynamic_store_reader": {
                "max_rows_per_server_read": 10,
                "streaming_subrequest_failure_probability": 0.005,
                "window_size": 1,
            },
        }

        def _validate(start_tablet_index, start_row_index, end_tablet_index, end_row_index):
            expected = []
            if start_tablet_index == end_tablet_index:
                if start_tablet_index < tablet_count:
                    expected.extend(data[start_tablet_index][start_row_index:end_row_index])
            elif start_tablet_index < end_tablet_index:
                if start_tablet_index < tablet_count:
                    expected.extend(data[start_tablet_index][start_row_index:])
                for tablet_index in range(start_tablet_index + 1, min(tablet_count, end_tablet_index)):
                    expected.extend(data[tablet_index])
                if end_tablet_index < tablet_count:
                    expected.extend(data[end_tablet_index][:end_row_index])

            read_range = {
                "lower_limit": {"tablet_index": start_tablet_index, "row_index": start_row_index},
                "upper_limit": {"tablet_index": end_tablet_index, "row_index": end_row_index}}
            rows = read_table(
                "<ranges=[{}]>//tmp/t".format(yson.dumps(read_range)),
                verbose=False,
                table_reader=table_reader)
            actual = [row["key"] for row in rows]

            assert expected == actual

        for iter in range(request_count):
            if iter % 20 == 0:
                print_debug("Iteration {} of {}".format(iter, request_count))

            # Off-by-one error is intentional. start > end is also possible.
            start_tablet_index = random.randint(0, tablet_count + 1)
            end_tablet_index = random.randint(0, tablet_count + 1)

            if start_tablet_index < tablet_count:
                start_row_index = random.randint(0, len(data[start_tablet_index]) * 2)
            else:
                start_row_index = random.randint(0, 100)
            if end_tablet_index < tablet_count:
                end_row_index = random.randint(0, len(data[end_tablet_index]) * 2)
            else:
                end_row_index = random.randint(0, 100)

            _validate(start_tablet_index, start_row_index, end_tablet_index, end_row_index)

    @authors("ifsmirnov")
    @pytest.mark.parametrize("enable_dynamic_store_read", [True, False])
    def test_read_fully_trimmed_table(self, enable_dynamic_store_read):
        sync_create_cells(1)
        create_dynamic_table(
            "//tmp/t",
            schema=[{"name": "key", "type": "int64"}],
            enable_dynamic_store_read=enable_dynamic_store_read,
            dynamic_store_auto_flush_period=yson.YsonEntity())
        sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", [{"key": 1}])
        sync_flush_table("//tmp/t")
        insert_rows("//tmp/t", [{"key": 2}])
        trim_rows("//tmp/t", 0, 2)

        path_with_ranges = (
            "<ranges=["
            "{lower_limit={tablet_index=0; row_index=5};"
            "upper_limit={tablet_index=0; row_index=10}}]>//tmp/t"
        )

        # We don't care about result since read_table does not always respect
        # trimmed rows.
        read_table("//tmp/t")
        read_table(path_with_ranges)

        # Now trim those chunks for sure.
        sync_freeze_table("//tmp/t")
        sync_unfreeze_table("//tmp/t")
        wait(lambda: get("//tmp/t/@chunk_ids") == [])
        sync_freeze_table("//tmp/t")

        assert read_table("//tmp/t") == []
        assert read_table(path_with_ranges) == []


##################################################################


class TestInputOutputForOrderedWithTabletIndexMulticell(TestInputOutputForOrderedWithTabletIndex):
    NUM_SECONDARY_MASTER_CELLS = 2


class TestInputOutputForOrderedWithTabletIndexPortal(TestInputOutputForOrderedWithTabletIndexMulticell):
    ENABLE_TMP_PORTAL = True


##################################################################


class TestSchedulerMapReduceDynamic(MROverOrderedDynTablesHelper):
    NUM_MASTERS = 1
    NUM_NODES = 5
    NUM_SCHEDULERS = 1
    USE_DYNAMIC_TABLES = True

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "sort_operation_options": {"min_uncompressed_block_size": 1},
            "map_reduce_operation_options": {
                "min_uncompressed_block_size": 1,
            },
            "enable_partition_map_job_size_adjustment": True,
        }
    }

    def _create_simple_dynamic_table(self, path, sort_order="ascending", **attributes):
        if "schema" not in attributes:
            attributes.update(
                {
                    "schema": [
                        {"name": "key", "type": "int64", "sort_order": sort_order},
                        {"name": "value", "type": "string"},
                    ]
                }
            )
        create_dynamic_table(path, **attributes)

    @authors("savrus")
    @parametrize_external
    @pytest.mark.parametrize("optimize_for", ["lookup", "scan"])
    @pytest.mark.parametrize("sort_order", [None, "ascending"])
    def test_map_reduce_on_dynamic_table(self, external, sort_order, optimize_for):
        sync_create_cells(1)
        self._create_simple_dynamic_table(
            "//tmp/t",
            sort_order=sort_order,
            optimize_for=optimize_for,
            external=external,
        )

        create("table", "//tmp/t_out")

        rows = [{"key": i, "value": str(i)} for i in range(6)]
        sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", rows)
        sync_unmount_table("//tmp/t")

        map_reduce(
            in_="//tmp/t",
            out="//tmp/t_out",
            sort_by="key",
            mapper_command="cat",
            reducer_command="cat",
            spec={"max_failed_job_count": 1},
        )

        assert_items_equal(read_table("//tmp/t_out"), rows)

        rows1 = [{"key": i, "value": str(i + 1)} for i in range(3, 10)]
        sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", rows1)
        sync_unmount_table("//tmp/t")

        map_reduce(
            in_="//tmp/t",
            out="//tmp/t_out",
            sort_by="key",
            mapper_command="cat",
            reducer_command="cat",
        )

        def update(new):
            def update_row(row):
                if sort_order == "ascending":
                    for r in rows:
                        if r["key"] == row["key"]:
                            r["value"] = row["value"]
                            return
                rows.append(row)

            for row in new:
                update_row(row)

        update(rows1)

        assert_items_equal(read_table("//tmp/t_out"), rows)

    @authors("akozhikhov")
    @pytest.mark.parametrize("optimize_for", ["lookup", "scan"])
    def test_ordered_tables_tablet_index_in_map_reduce(self, optimize_for):
        self._prologue(2, optimize_for=optimize_for)

        self._insert_chunk(first_value=0, tablet_index=0)
        self._insert_chunk(first_value=3, tablet_index=0)
        self._insert_chunk(first_value=6, tablet_index=1)

        map_reduce(
            sort_by="key",
            reduce_by="key",
            in_=[
                "<ranges=[{lower_limit={tablet_index=0; row_index=2}; upper_limit={tablet_index=1; row_index=2}}]>"
                "//tmp/t"
            ],
            out="//tmp/t_out",
            mapper_command="cat",
            reducer_command="cat",
            spec={
                "job_count": 1,
                "max_failed_job_count": 1,
                "mapper": {
                    "format": yson.loads("<format=text>yson"),
                },
                "reducer": {
                    "format": yson.loads("<format=text>yson"),
                },
            },
        )

        assert read_table("//tmp/t_out") == [{"key": i} for i in range(2, 8)]

    @authors("akozhikhov")
    @pytest.mark.parametrize("optimize_for", ["lookup", "scan"])
    def test_ordered_tablet_index_in_map_reduce_general(self, optimize_for):
        self._prologue(2, optimize_for)

        self._insert_chunk(first_value=0, tablet_index=0)
        self._insert_chunk(first_value=3, tablet_index=0)
        self._insert_chunk(first_value=6, tablet_index=1)

        self._run_map_reduce_operation(
            "<ranges=[{lower_limit={tablet_index=0; row_index=2}; upper_limit={tablet_index=1; row_index=2}}]>"
        )

        expected_content = [
            {"key": 2, "tablet_index": 0, "range_index": 0},
            {"key": 3, "tablet_index": 0, "range_index": 0},
            {"key": 4, "tablet_index": 0, "range_index": 0},
            {"key": 5, "tablet_index": 0, "range_index": 0},
            {"key": 6, "tablet_index": 1, "range_index": 0},
            {"key": 7, "tablet_index": 1, "range_index": 0},
        ]
        self._validate_output(expected_content)


##################################################################


class TestSchedulerMapReduceDynamicMulticell(TestSchedulerMapReduceDynamic):
    NUM_SECONDARY_MASTER_CELLS = 2


class TestSchedulerMapReduceDynamicPortal(TestSchedulerMapReduceDynamicMulticell):
    ENABLE_TMP_PORTAL = True
