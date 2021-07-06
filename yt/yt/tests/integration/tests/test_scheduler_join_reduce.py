from yt_env_setup import YTEnvSetup

from yt_commands import (  # noqa
    authors, print_debug, wait, wait_assert, wait_breakpoint, release_breakpoint, with_breakpoint,
    events_on_fs, reset_events_on_fs,
    create, ls, get, set, copy, move, remove, link, exists,
    create_account, create_network_project, create_tmpdir, create_user, create_group,
    create_pool, create_pool_tree,
    create_data_center, create_rack,
    make_ace, check_permission, add_member,
    make_batch_request, execute_batch, get_batch_error,
    start_transaction, abort_transaction, lock,
    insert_rows, select_rows, delete_rows,
    read_file, write_file, read_table, write_table, write_local_file,
    map, reduce, map_reduce, join_reduce, merge, vanilla, sort, erase,
    run_test_vanilla, run_sleeping_vanilla,
    abort_job, list_jobs, get_job, abandon_job, interrupt_job,
    get_job_fail_context, get_job_input, get_job_stderr, get_job_spec,
    dump_job_context, poll_job_shell,
    abort_op, complete_op, suspend_op, resume_op,
    get_operation, list_operations, clean_operations,
    get_operation_cypress_path, scheduler_orchid_pool_path,
    scheduler_orchid_default_pool_tree_path, scheduler_orchid_operation_path,
    scheduler_orchid_default_pool_tree_config_path, scheduler_orchid_path,
    scheduler_orchid_node_path, scheduler_orchid_pool_tree_config_path,
    sync_create_cells, sync_mount_table, sync_unmount_table,
    get_first_chunk_id, get_singular_chunk_id, multicell_sleep,
    update_nodes_dynamic_config, update_controller_agent_config,
    update_op_parameters, enable_op_detailed_logs,
    set_node_banned, set_banned_flag, set_account_disk_space_limit,
    check_all_stderrs,
    create_test_tables, PrepareTables,
    get_statistics,
    make_random_string, raises_yt_error)

from yt_helpers import skip_if_no_descending

import yt_error_codes

import yt.yson as yson
from yt.common import YtError

import pytest


##################################################################


class TestSchedulerJoinReduceCommands(YTEnvSetup):
    NUM_TEST_PARTITIONS = 2

    NUM_MASTERS = 1
    NUM_NODES = 5
    NUM_SCHEDULERS = 1
    USE_DYNAMIC_TABLES = True

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "watchers_update_period": 100,
            "operations_update_period": 10,
            "running_jobs_update_period": 10,
        }
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "operations_update_period": 10,
            "join_reduce_operation_options": {
                "job_splitter": {
                    "min_job_time": 5000,
                    "min_total_data_size": 1024,
                    "update_period": 100,
                    "candidate_percentile": 0.8,
                    "max_jobs_per_split": 3,
                }
            },
        }
    }

    def skip_if_legacy_sorted_pool(self):
        if not isinstance(self, TestSchedulerJoinReduceCommandsNewSortedPool):
            pytest.skip("This test requires new sorted pool")

    @authors("klyachin")
    @pytest.mark.parametrize("sort_order", ["ascending", "descending"])
    def test_join_reduce_tricky_chunk_boundaries(self, sort_order):
        if sort_order == "descending":
            skip_if_no_descending(self.Env)
            self.skip_if_legacy_sorted_pool()

        def write(path, rows):
            if sort_order == "descending":
                rows = rows[::-1]
            write_table(
                path,
                rows,
                sorted_by=[
                    {"name": "key", "sort_order": sort_order},
                    {"name": "value", "sort_order": sort_order},
                ]
            )

        create("table", "//tmp/in1")
        write("//tmp/in1", [{"key": "0", "value": 1}, {"key": "2", "value": 2}])

        create("table", "//tmp/in2")
        write("//tmp/in2", [{"key": "2", "value": 6}, {"key": "5", "value": 8}])

        create("table", "//tmp/out")

        join_reduce(
            in_=["//tmp/in1{key}", "<foreign=true>//tmp/in2{key}"],
            out=["<sorted_by=[{{name=key;sort_order={}}}]>//tmp/out".format(sort_order)],
            command="cat",
            join_by=[{"name": "key", "sort_order": sort_order}],
            spec={
                "reducer": {"format": yson.loads("<line_prefix=tskv;enable_table_index=true>dsv")},
                "data_size_per_job": 1,
            },
        )

        rows = read_table("//tmp/out")
        assert len(rows) == 3
        if sort_order == "ascending":
            assert rows == [
                {"key": "0", "@table_index": "0"},
                {"key": "2", "@table_index": "0"},
                {"key": "2", "@table_index": "1"},
            ]
        else:
            assert rows == [
                {"key": "2", "@table_index": "0"},
                {"key": "2", "@table_index": "1"},
                {"key": "0", "@table_index": "0"},
            ]

        assert get("//tmp/out/@sorted")

    @authors("klyachin")
    @pytest.mark.parametrize("sort_order", ["ascending", "descending"])
    def test_join_reduce_cat_simple(self, sort_order):
        if sort_order == "descending":
            skip_if_no_descending(self.Env)
            self.skip_if_legacy_sorted_pool()

        def write(path, rows):
            if sort_order == "descending":
                rows = rows[::-1]
            write_table(path, rows, sorted_by=[{"name": "key", "sort_order": sort_order}])

        create("table", "//tmp/in1")
        write(
            "//tmp/in1",
            [
                {"key": 0, "value": 1},
                {"key": 1, "value": 2},
                {"key": 3, "value": 3},
                {"key": 7, "value": 4},
            ],
        )

        create("table", "//tmp/in2")
        write(
            "//tmp/in2",
            [
                {"key": -1, "value": 5},
                {"key": 1, "value": 6},
                {"key": 3, "value": 7},
                {"key": 5, "value": 8},
            ],
        )

        create("table", "//tmp/out")

        join_reduce(
            in_=["<foreign=true>//tmp/in1", "//tmp/in2"],
            out="<sorted_by=[{{name=key;sort_order={}}}]>//tmp/out".format(sort_order),
            join_by={"name": "key", "sort_order": sort_order},
            command="cat",
            spec={"reducer": {"format": "dsv"}},
        )

        if sort_order == "ascending":
            assert read_table("//tmp/out") == [
                {"key": "-1", "value": "5"},
                {"key": "1", "value": "2"},
                {"key": "1", "value": "6"},
                {"key": "3", "value": "3"},
                {"key": "3", "value": "7"},
                {"key": "5", "value": "8"},
            ]
        else:
            assert read_table("//tmp/out") == [
                {"key": "5", "value": "8"},
                {"key": "3", "value": "3"},
                {"key": "3", "value": "7"},
                {"key": "1", "value": "2"},
                {"key": "1", "value": "6"},
                {"key": "-1", "value": "5"},
            ]

        assert get("//tmp/out/@sorted")

    @authors("psushin")
    @pytest.mark.parametrize("sort_order", ["ascending", "descending"])
    def test_join_reduce_split_further(self, sort_order):
        if sort_order == "descending":
            skip_if_no_descending(self.Env)
            self.skip_if_legacy_sorted_pool()

        def write(path, rows):
            if sort_order == "descending":
                rows = rows[::-1]
            write_table(
                "<append=true>{}".format(path),
                rows,
                sorted_by=[{"name": "key", "sort_order": sort_order}])

        create("table", "//tmp/primary")
        write(
            "//tmp/primary",
            [{"key": 0, "value": 0}, {"key": 9, "value": 0}],
        )

        first_foreign_chunk = [{"key": 0, "value": 1}, {"key": 4, "value": 1}]
        second_foreign_chunk = [{"key": 5, "value": 1}, {"key": 9, "value": 1}]
        if sort_order == "descending":
            first_foreign_chunk, second_foreign_chunk = second_foreign_chunk, first_foreign_chunk

        create("table", "//tmp/foreign")
        write(
            "//tmp/foreign",
            first_foreign_chunk,
        )

        write(
            "//tmp/foreign",
            second_foreign_chunk,
        )

        create("table", "//tmp/out")

        join_reduce(
            in_=["<foreign=true>//tmp/foreign", "//tmp/primary"],
            out=["<sorted_by=[{{name=key;sort_order={}}}]>//tmp/out".format(sort_order)],
            join_by=[{"name": "key", "sort_order": sort_order}],
            command="cat",
            spec={"data_size_per_job": 1, "reducer": {"format": "dsv"}},
        )

        # Must be split into two jobs, despite that only one primary slice is available.
        assert get("//tmp/out/@chunk_count") == 2
        assert get("//tmp/out/@sorted")

    @authors("klyachin")
    def test_join_reduce_primary_attribute_compatibility(self):
        create("table", "//tmp/in1")
        write_table("//tmp/in1", [{"key": i, "value": i + 1} for i in range(8)], sorted_by="key")

        create("table", "//tmp/in2")
        write_table(
            "//tmp/in2",
            [{"key": 2 * i - 1, "value": i + 10} for i in range(4)],
            sorted_by="key",
        )

        create("table", "//tmp/out")

        join_reduce(
            in_=["//tmp/in1", "<primary=true>//tmp/in2"],
            out="<sorted_by=[key]>//tmp/out",
            join_by="key",
            command="cat",
            spec={"reducer": {"format": "dsv"}},
        )

        assert read_table("//tmp/out") == [
            {"key": "-1", "value": "10"},
            {"key": "1", "value": "2"},
            {"key": "1", "value": "11"},
            {"key": "3", "value": "4"},
            {"key": "3", "value": "12"},
            {"key": "5", "value": "6"},
            {"key": "5", "value": "13"},
        ]

        assert get("//tmp/out/@sorted")

    @authors("klyachin")
    def test_join_reduce_control_attributes_yson(self):
        create("table", "//tmp/in1")
        write_table(
            "//tmp/in1",
            [
                {"key": 2, "value": 7},
                {"key": 4, "value": 3},
            ],
            sorted_by="key",
        )

        create("table", "//tmp/in2")
        write_table(
            "//tmp/in2",
            [
                {"key": 0, "value": 4},
                {"key": 2, "value": 6},
                {"key": 4, "value": 8},
                {"key": 6, "value": 10},
            ],
            sorted_by="key",
        )

        create("table", "//tmp/out")

        op = join_reduce(
            in_=["//tmp/in1", "<foreign=true>//tmp/in2"],
            out="<sorted_by=[key]>//tmp/out",
            join_by="key",
            command="cat 1>&2",
            spec={
                "reducer": {"format": yson.loads("<format=text>yson")},
                "job_io": {
                    "control_attributes": {
                        "enable_table_index": "true",
                        "enable_row_index": "true",
                    }
                },
                "job_count": 1,
            },
        )

        job_ids = ls(op.get_path() + "/jobs")
        assert len(job_ids) == 1

        assert (
            op.read_stderr(job_ids[0])
            == """<"table_index"=0;>#;
<"row_index"=0;>#;
{"key"=2;"value"=7;};
<"table_index"=1;>#;
<"row_index"=1;>#;
{"key"=2;"value"=6;};
<"table_index"=0;>#;
<"row_index"=1;>#;
{"key"=4;"value"=3;};
<"table_index"=1;>#;
<"row_index"=2;>#;
{"key"=4;"value"=8;};
"""
        )

    @authors("klyachin")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    @pytest.mark.parametrize("sort_order", ["ascending", "descending"])
    def test_join_reduce_cat_two_output(self, optimize_for, sort_order):
        if sort_order == "descending":
            skip_if_no_descending(self.Env)
            self.skip_if_legacy_sorted_pool()

        schema = [
            {"name": "key", "type": "int64", "sort_order": sort_order},
            {"name": "value", "type": "int64", "sort_order": sort_order},
        ]

        def write(path, rows):
            if sort_order == "descending":
                rows = rows[::-1]
            write_table(path, rows)

        create(
            "table",
            "//tmp/in1",
            attributes={"schema": schema, "optimize_for": optimize_for},
        )
        write(
            "//tmp/in1",
            [
                {"key": 0, "value": 1},
                {"key": 2, "value": 2},
                {"key": 4, "value": 3},
                {"key": 8, "value": 4},
            ],
        )

        create(
            "table",
            "//tmp/in2",
            attributes={"schema": schema, "optimize_for": optimize_for},
        )
        write(
            "//tmp/in2",
            [
                {"key": 2, "value": 5},
                {"key": 3, "value": 6},
            ],
        )

        create(
            "table",
            "//tmp/in3",
            attributes={"schema": schema, "optimize_for": optimize_for},
        )
        write(
            "//tmp/in3",
            [
                {"key": 2, "value": 1},
            ],
        )

        create(
            "table",
            "//tmp/in4",
            attributes={"schema": schema, "optimize_for": optimize_for},
        )
        write(
            "//tmp/in4",
            [
                {"key": 3, "value": 7},
            ],
        )

        create("table", "//tmp/out1")
        create("table", "//tmp/out2")

        join_reduce(
            in_=[
                "//tmp/in1",
                "<foreign=true>//tmp/in2",
                "<foreign=true>//tmp/in3",
                "<foreign=true>//tmp/in4",
            ],
            out=[
                "<sorted_by=[{{name=key;sort_order={}}}]>//tmp/out1".format(sort_order),
                "<sorted_by=[{{name=key;sort_order={}}}]>//tmp/out2".format(sort_order)
            ],
            command="cat | tee /dev/fd/4 | grep @table_index=0",
            join_by=[{"name": "key", "sort_order": sort_order}],
            spec={"reducer": {"format": yson.loads("<enable_table_index=true>dsv")}},
        )

        if sort_order == "ascending":
            assert read_table("//tmp/out1") == [
                {"key": "0", "value": "1", "@table_index": "0"},
                {"key": "2", "value": "2", "@table_index": "0"},
                {"key": "4", "value": "3", "@table_index": "0"},
                {"key": "8", "value": "4", "@table_index": "0"},
            ]

            assert read_table("//tmp/out2") == [
                {"key": "0", "value": "1", "@table_index": "0"},
                {"key": "2", "value": "2", "@table_index": "0"},
                {"key": "2", "value": "5", "@table_index": "1"},
                {"key": "2", "value": "1", "@table_index": "2"},
                {"key": "4", "value": "3", "@table_index": "0"},
                {"key": "8", "value": "4", "@table_index": "0"},
            ]
        else:
            assert read_table("//tmp/out1") == [
                {"key": "8", "value": "4", "@table_index": "0"},
                {"key": "4", "value": "3", "@table_index": "0"},
                {"key": "2", "value": "2", "@table_index": "0"},
                {"key": "0", "value": "1", "@table_index": "0"},
            ]

            assert read_table("//tmp/out2") == [
                {"key": "8", "value": "4", "@table_index": "0"},
                {"key": "4", "value": "3", "@table_index": "0"},
                {"key": "2", "value": "2", "@table_index": "0"},
                {"key": "2", "value": "5", "@table_index": "1"},
                {"key": "2", "value": "1", "@table_index": "2"},
                {"key": "0", "value": "1", "@table_index": "0"},
            ]

        assert get("//tmp/out1/@sorted")
        assert get("//tmp/out2/@sorted")

    @authors("klyachin")
    def test_join_reduce_empty_in(self):
        create(
            "table",
            "//tmp/in1",
            attributes={"schema": [{"name": "key", "type": "any", "sort_order": "ascending"}]},
        )
        create(
            "table",
            "//tmp/in2",
            attributes={"schema": [{"name": "key", "type": "any", "sort_order": "ascending"}]},
        )
        create("table", "//tmp/out")

        join_reduce(
            in_=["//tmp/in1", "<foreign=true>//tmp/in2"],
            out="//tmp/out",
            join_by="key",
            command="cat",
        )

        assert read_table("//tmp/out") == []

    @authors("babenko", "klyachin")
    def test_join_reduce_duplicate_key_columns(self):
        create(
            "table",
            "//tmp/in1",
            attributes={
                "schema": [
                    {"name": "a", "type": "any", "sort_order": "ascending"},
                    {"name": "b", "type": "any", "sort_order": "ascending"},
                ]
            },
        )
        create(
            "table",
            "//tmp/in2",
            attributes={
                "schema": [
                    {"name": "a", "type": "any", "sort_order": "ascending"},
                    {"name": "b", "type": "any", "sort_order": "ascending"},
                ]
            },
        )
        create("table", "//tmp/out")

        # expected error: Duplicate key column name "a"
        with pytest.raises(YtError):
            join_reduce(in_="//tmp/in", out="//tmp/out", command="cat", join_by=["a", "b", "a"])

    @authors("klyachin")
    def test_join_reduce_unsorted_input(self):
        create("table", "//tmp/in1")
        write_table("//tmp/in1", {"foo": "bar"})
        create(
            "table",
            "//tmp/in2",
            attributes={"schema": [{"name": "foo", "type": "any", "sort_order": "ascending"}]},
        )
        create("table", "//tmp/out")

        # expected error: Input table //tmp/in1 is not sorted
        with pytest.raises(YtError):
            join_reduce(
                in_=["//tmp/in1", "<foreign=true>//tmp/in2"],
                out="//tmp/out",
                join_by="key",
                command="cat",
            )

    @authors("klyachin")
    def test_join_reduce_different_key_column(self):
        create("table", "//tmp/in1")
        write_table("//tmp/in1", {"foo": "bar"}, sorted_by=["foo"])
        create(
            "table",
            "//tmp/in2",
            attributes={"schema": [{"name": "baz", "type": "any", "sort_order": "ascending"}]},
        )
        create("table", "//tmp/out")

        # expected error: Key columns do not match
        with pytest.raises(YtError):
            join_reduce(
                in_=["//tmp/in1", "<foreign=true>//tmp/in2"],
                out="//tmp/out",
                join_by="key",
                command="cat",
            )

    @authors("gritukan")
    def test_join_reduce_different_sort_order(self):
        skip_if_no_descending(self.Env)
        self.skip_if_legacy_sorted_pool()

        create("table", "//tmp/in1")
        write_table("//tmp/in1", {"foo": "bar"}, sorted_by=["foo"])
        create("table", "//tmp/in2")
        write_table("//tmp/in2", {"foo": "bar"}, sorted_by=["foo"])

        with pytest.raises(YtError):
            join_reduce(
                in_=["//tmp/in1", "<foreign=true>//tmp/in2"],
                out="//tmp/out",
                join_by=[{"name": "key", "sort_order": "descending"}],
                command="cat",
            )

    @authors("klyachin")
    def test_join_reduce_non_prefix(self):
        create("table", "//tmp/in")
        create("table", "//tmp/out")
        write_table("//tmp/in", {"key": "1", "subkey": "2"}, sorted_by=["key", "subkey"])

        # expected error: Input table is sorted by columns that are not compatible with the requested columns"
        with pytest.raises(YtError):
            join_reduce(
                in_=["//tmp/in", "<foreign=true>//tmp/in"],
                out="//tmp/out",
                command="cat",
                join_by="subkey",
            )

    @authors("klyachin")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    @pytest.mark.parametrize("sort_order", ["ascending", "descending"])
    def test_join_reduce_short_limits(self, optimize_for, sort_order):
        if sort_order == "descending":
            skip_if_no_descending(self.Env)
            self.skip_if_legacy_sorted_pool()

        schema = [
            {"name": "key", "type": "string", "sort_order": sort_order},
            {"name": "subkey", "type": "string", "sort_order": sort_order},
        ]
        create(
            "table",
            "//tmp/in1",
            attributes={"schema": schema, "optimize_for": optimize_for},
        )
        create(
            "table",
            "//tmp/in2",
            attributes={"schema": schema, "optimize_for": optimize_for},
        )
        create("table", "//tmp/out")

        def write(path, rows):
            if sort_order == "descending":
                rows = rows[::-1]
            write_table(path, rows)

        write("//tmp/in1", [{"key": "1", "subkey": "2"}, {"key": "3"}, {"key": "5"}])
        write("//tmp/in2", [{"key": "1", "subkey": "3"}, {"key": "3", "subkey": "3"}, {"key": "4"}])

        if sort_order == "ascending":
            in_ = ['//tmp/in1["1":"4"]', "<foreign=true>//tmp/in2"]
        else:
            in_ = ['//tmp/in1["3":"0"]', "<foreign=true>//tmp/in2"]

        join_reduce(
            in_=in_,
            out="<sorted_by=[{{name=key;sort_order={0}}}; {{name=subkey;sort_order={0}}}]>//tmp/out".format(sort_order),
            command="cat",
            join_by=[
                {"name": "key", "sort_order": sort_order},
                {"name": "subkey", "sort_order": sort_order},
            ],
            spec={
                "reducer": {"format": yson.loads("<line_prefix=tskv>dsv")},
                "data_size_per_job": 1,
            },
        )

        if sort_order == "ascending":
            assert read_table("//tmp/out") == [
                {"key": "1", "subkey": "2"},
                {"key": "3", "subkey": yson.YsonEntity()},
            ]
        else:
            assert read_table("//tmp/out") == [
                {"key": "3", "subkey": yson.YsonEntity()},
                {"key": "1", "subkey": "2"},
            ]

    @authors("klyachin")
    def test_join_reduce_many_output_tables(self):
        output_tables = ["//tmp/t%d" % i for i in range(3)]

        create("table", "//tmp/t_in")
        for table_path in output_tables:
            create("table", table_path)

        write_table("//tmp/t_in", [{"k": 10}], sorted_by="k")

        reducer = """
cat  > /dev/null
echo {v = 0} >&1
echo {v = 1} >&4
echo {v = 2} >&7
"""
        create("file", "//tmp/reducer.sh")
        write_file("//tmp/reducer.sh", reducer)

        join_reduce(
            in_=["//tmp/t_in", "<foreign=true>//tmp/t_in"],
            out=output_tables,
            join_by="k",
            command="bash reducer.sh",
            file="//tmp/reducer.sh",
        )

        assert read_table(output_tables[0]) == [{"v": 0}]
        assert read_table(output_tables[1]) == [{"v": 1}]
        assert read_table(output_tables[2]) == [{"v": 2}]

    @authors("klyachin")
    @pytest.mark.parametrize("sort_order", ["ascending", "descending"])
    def test_join_reduce_job_count(self, sort_order):
        if sort_order == "descending":
            skip_if_no_descending(self.Env)
            self.skip_if_legacy_sorted_pool()

        create("table", "//tmp/in1", attributes={"compression_codec": "none"})
        create(
            "table",
            "//tmp/in2",
            attributes={"schema": [{"name": "key", "type": "string", "sort_order": sort_order}]},
        )
        create("table", "//tmp/out")

        count = 1000

        # Job count works only if we have enough splits in input chunks.
        # Its default rate 0.0001, so we should have enough rows in input table
        rows = [{"key": "%.010d" % num} for num in xrange(count)]
        if sort_order == "descending":
            rows = rows[::-1]
        write_table(
            "//tmp/in1",
            rows,
            sorted_by=[{"name": "key", "sort_order": sort_order}],
            table_writer={"block_size": 1024},
        )
        # write secondary table as one row per chunk
        rows = [{"key": "%.010d" % num} for num in xrange(0, count, 20)]
        if sort_order == "descending":
            rows = rows[::-1]
        write_table(
            "//tmp/in2",
            rows,
            sorted_by=[{"name": "key", "sort_order": sort_order}],
            max_row_buffer_size=1,
            table_writer={"desired_chunk_size": 1},
        )

        assert get("//tmp/in2/@chunk_count") == count // 20

        join_reduce(
            in_=["//tmp/in1", "<foreign=true>//tmp/in2"],
            out="//tmp/out",
            command='echo "key=`wc -l`"',
            join_by=[{"name": "key", "sort_order": sort_order}],
            spec={
                "reducer": {"format": yson.loads("<enable_table_index=true>dsv")},
                "data_size_per_job": 250,
            },
        )

        read_table("//tmp/out")
        get("//tmp/out/@row_count")
        # Check that operation has more than 1 job
        assert get("//tmp/out/@row_count") >= 2

    @authors("klyachin")
    def test_join_reduce_key_switch_yamr(self):
        create("table", "//tmp/in")
        create("table", "//tmp/out")

        write_table(
            "//tmp/in",
            [
                {"key": "a", "value": ""},
                {"key": "b", "value": ""},
                {"key": "b", "value": ""},
            ],
            sorted_by=["key"],
        )

        op = join_reduce(
            in_=["//tmp/in", "<foreign=true>//tmp/in"],
            out="//tmp/out",
            command="cat 1>&2",
            join_by=["key"],
            spec={
                "job_io": {"control_attributes": {"enable_key_switch": "true"}},
                "reducer": {
                    "format": yson.loads("<lenval=true>yamr"),
                    "enable_input_table_index": False,
                },
                "job_count": 1,
            },
        )

        jobs_path = op.get_path() + "/jobs"
        job_ids = ls(jobs_path)
        assert len(job_ids) == 1
        stderr_bytes = op.read_stderr(job_ids[0])

        assert (
            stderr_bytes.encode("hex") == "010000006100000000"
            "010000006100000000"
            "feffffff"
            "010000006200000000"
            "010000006200000000"
            "010000006200000000"
            "010000006200000000"
        )

    @authors("klyachin")
    @pytest.mark.parametrize("sort_order", ["ascending", "descending"])
    def test_join_reduce_with_small_block_size(self, sort_order):
        if sort_order == "descending":
            skip_if_no_descending(self.Env)
            self.skip_if_legacy_sorted_pool()

        create("table", "//tmp/in1", attributes={"compression_codec": "none"})
        create("table", "//tmp/in2")
        create("table", "//tmp/out")

        def write(path, rows1, rows2):
            if sort_order == "descending":
                rows1 = rows1[::-1]
                rows2 = rows2[::-1]
                rows1, rows2 = rows2, rows1
            for rows in [rows1, rows2]:
                write_table(
                    path,
                    rows,
                    sorted_by=[{"name": "key", "sort_order": sort_order}],
                    table_writer={"block_size": 1024}
                )

        count = 300

        write(
            "<append=true>//tmp/in1",
            [{"key": "%05d" % (10000 + num / 2), "val1": num} for num in xrange(count)],
            [{"key": "%05d" % (10000 + num / 2), "val1": num} for num in xrange(count, 2 * count)],
        )

        write(
            "<append=true>//tmp/in2",
            [{"key": "%05d" % (10000 + num / 2), "val2": num} for num in xrange(count)],
            [{"key": "%05d" % (10000 + num / 2), "val2": num} for num in xrange(count, 2 * count)],
        )

        if sort_order == "ascending":
            in_ = [
                '<ranges=[{lower_limit={row_index=100;key=["10010"]}'
                ';upper_limit={row_index=540;key=["10280"]}}];primary=true>//tmp/in1',
                "<foreign=true>//tmp/in2",
            ]
        else:
            in_ = [
                '<ranges=[{lower_limit={row_index=60;key=["10280"]};'
                'upper_limit={row_index=500;key=["10010"]}}];primary=true>//tmp/in1',
                "<foreign=true>//tmp/in2",
            ]

        join_reduce(
            in_=in_,
            out="//tmp/out",
            command="""awk '{print $0"\tji="ENVIRON["YT_JOB_INDEX"]"\tsi="ENVIRON["YT_START_ROW_INDEX"]}' """,
            join_by=[{"name": "key", "sort_order": sort_order}],
            spec={
                "reducer": {"format": yson.loads("<enable_table_index=true;table_index_column=ti>dsv")},
                "data_size_per_job": 500,
            },
        )

        assert get("//tmp/out/@row_count") > 800

    @authors("renadeen", "klyachin")
    @pytest.mark.parametrize("sort_order", ["ascending", "descending"])
    def test_join_reduce_skewed_key_distribution(self, sort_order):
        if sort_order == "descending":
            skip_if_no_descending(self.Env)
            self.skip_if_legacy_sorted_pool()

        create("table", "//tmp/in1")
        create("table", "//tmp/in2")
        create("table", "//tmp/out")

        if sort_order == "ascending":
            data1 = [{"key": "a"}] * 8000 + [{"key": "b"}] * 2000
        else:
            data1 = [{"key": "b"}] * 8000 + [{"key": "a"}] * 2000
        write_table(
            "//tmp/in1",
            data1,
            sorted_by=[{"name": "key", "sort_order": sort_order}],
            table_writer={"block_size": 1024})

        data2 = [{"key": "a"}, {"key": "b"}]
        if sort_order == "descending":
            data2 = data2[::-1]
        write_table(
            "//tmp/in2",
            data2,
            sorted_by=[{"name": "key", "sort_order": sort_order}])

        op = join_reduce(
            in_=["//tmp/in1", "<foreign=true>//tmp/in2"],
            out=["//tmp/out"],
            command="uniq",
            join_by=[{"name": "key", "sort_order": sort_order}],
            spec={
                "reducer": {"format": yson.loads("<enable_table_index=true>dsv")},
                "job_count": 2,
            },
        )

        assert get("//tmp/out/@chunk_count") == 2

        if sort_order == "ascending":
            assert sorted(read_table("//tmp/out")) == sorted(
                [
                    {"key": "a", "@table_index": "0"},
                    {"key": "a", "@table_index": "1"},
                    # ------partition boundary-------
                    {"key": "a", "@table_index": "0"},
                    {"key": "a", "@table_index": "1"},
                    {"key": "b", "@table_index": "0"},
                    {"key": "b", "@table_index": "1"},
                ]
            )
        else:
            assert sorted(read_table("//tmp/out")) == sorted(
                [
                    {"key": "b", "@table_index": "0"},
                    {"key": "b", "@table_index": "1"},
                    # ------partition boundary-------
                    {"key": "b", "@table_index": "0"},
                    {"key": "b", "@table_index": "1"},
                    {"key": "a", "@table_index": "0"},
                    {"key": "a", "@table_index": "1"},
                ]
            )

        histogram = get(op.get_path() + "/@progress/tasks/0/input_data_weight_histogram")
        assert sum(histogram["count"]) == 2

    # Check compatibility with deprecated <primary=true> attribute
    @authors("klyachin")
    def test_join_reduce_compatibility(self):
        create("table", "//tmp/in1")
        write_table(
            "//tmp/in1",
            [
                {"key": 1, "value": 1},
                {"key": 2, "value": 2},
            ],
            sorted_by="key",
        )

        create("table", "//tmp/in2")
        write_table(
            "//tmp/in2",
            [
                {"key": -1, "value": 5},
                {"key": 1, "value": 6},
            ],
            sorted_by="key",
        )

        create("table", "//tmp/out")

        join_reduce(
            in_=["//tmp/in1", "//tmp/in1", "<primary=true>//tmp/in2"],
            out="<sorted_by=[key]>//tmp/out",
            command="cat",
            join_by="key",
            spec={"reducer": {"format": "dsv"}},
        )

        assert read_table("//tmp/out") == [
            {"key": "-1", "value": "5"},
            {"key": "1", "value": "1"},
            {"key": "1", "value": "1"},
            {"key": "1", "value": "6"},
        ]

        assert get("//tmp/out/@sorted")

    @authors("max42", "klyachin")
    def test_join_reduce_row_count_limit(self):
        create("table", "//tmp/in1")

        write_table(
            "<append=true>//tmp/in1",
            [{"key": "%05d" % i, "value": "foo"} for i in range(5)],
            sorted_by=["key"],
            max_row_buffer_size=1,
            table_writer={"desired_chunk_size": 1},
        )

        create("table", "//tmp/in2")
        write_table(
            "<append=true>//tmp/in2",
            [{"key": "%05d" % i, "value": "bar"} for i in range(5)],
            sorted_by=["key"],
            max_row_buffer_size=1,
            table_writer={"desired_chunk_size": 1},
        )

        assert get("//tmp/in1/@chunk_count") == 5
        assert get("//tmp/in2/@chunk_count") == 5

        create("table", "//tmp/out")
        join_reduce(
            in_=["<foreign=true>//tmp/in2", "//tmp/in1"],
            out="<row_count_limit=5>//tmp/out",
            command="cat",
            join_by=["key"],
            spec={
                "reducer": {"format": "dsv"},
                "data_size_per_job": 1,
                "max_failed_job_count": 1,
            },
        )

        assert len(read_table("//tmp/out")) == 6

    @authors("klyachin")
    @pytest.mark.parametrize("sort_order", ["ascending", "descending"])
    def test_join_reduce_short_range(self, sort_order):
        if sort_order == "descending":
            skip_if_no_descending(self.Env)
            self.skip_if_legacy_sorted_pool()

        count = 300

        create("table", "//tmp/in1")
        rows = [{"key": "%05d" % num, "subkey": "", "value": num} for num in xrange(count)]
        if sort_order == "descending":
            rows = rows[::-1]
        write_table(
            "<append=true>//tmp/in1",
            rows,
            sorted_by=[
                {"name": "key", "sort_order": sort_order},
                {"name": "subkey", "sort_order": sort_order},
            ],
            table_writer={"block_size": 1024},
        )

        create("table", "//tmp/in2")
        write_table(
            "<append=true>//tmp/in2",
            rows,
            sorted_by=[
                {"name": "key", "sort_order": sort_order},
                {"name": "subkey", "sort_order": sort_order},
            ],
            table_writer={"block_size": 1024},
        )

        create("table", "//tmp/out")

        if sort_order == "ascending":
            in_ = ['//tmp/in1["00100":"00200"]', "<foreign=true>//tmp/in2"]
        else:
            in_ = ['//tmp/in1["00199":"00099"]', "<foreign=true>//tmp/in2"]

        join_reduce(
            in_=in_,
            out="//tmp/out",
            command="cat",
            join_by=[
                {"name": "key", "sort_order": sort_order},
                {"name": "subkey", "sort_order": sort_order},
            ],
            spec={
                "reducer": {"format": "dsv"},
                "data_size_per_job": 512,
                "max_failed_job_count": 1,
            },
        )

        assert get("//tmp/out/@row_count") == 200

    @authors("max42")
    def test_join_reduce_cartesian_product(self):
        create("table", "//tmp/in")
        for i in range(20):
            write_table(
                "<append=true>//tmp/in",
                [{"fake_key": ""} for num in xrange(i * 100, (i + 1) * 100)],
                sorted_by=["fake_key"],
                table_writer={"block_size": 1024},
            )

        create("table", "//tmp/out")
        join_reduce(
            in_=["//tmp/in", "<foreign=true>//tmp/in"],
            out="//tmp/out",
            command="echo a=$JOB_INDEX",
            join_by=["fake_key"],
            spec={
                "reducer": {"format": "dsv"},
                "max_failed_job_count": 1,
                "job_count": 10,
                "consider_only_primary_size": True,
            },
        )

        job_count = get("//tmp/out/@row_count")
        assert 9 <= job_count <= 11

    @authors("klyachin")
    def test_join_reduce_input_paths_attr(self):
        create("table", "//tmp/in1")
        for i in xrange(0, 5, 2):
            write_table(
                "<append=true>//tmp/in1",
                [{"key": "%05d" % (i + j), "value": "foo"} for j in xrange(2)],
                sorted_by=["key"],
            )

        create("table", "//tmp/in2")
        for i in xrange(3, 16, 2):
            write_table(
                "<append=true>//tmp/in2",
                [{"key": "%05d" % ((i + j) / 4), "value": "foo"} for j in xrange(2)],
                sorted_by=["key"],
            )

        create("table", "//tmp/out")
        op = join_reduce(
            track=False,
            in_=["<foreign=true>//tmp/in1", '//tmp/in2["00001":"00004"]'],
            out="//tmp/out",
            command="exit 1",
            join_by=["key"],
            spec={
                "reducer": {"format": "dsv"},
                "job_count": 1,
                "max_failed_job_count": 1,
            },
        )
        with pytest.raises(YtError):
            op.track()

    @authors("savrus")
    def test_join_reduce_on_dynamic_table(self):
        sync_create_cells(1)
        create(
            "table",
            "//tmp/t1",
            attributes={
                "schema": [
                    {"name": "key", "type": "string", "sort_order": "ascending"},
                    {"name": "value", "type": "string"},
                ],
                "dynamic": True,
            },
        )

        create("table", "//tmp/t2")
        create("table", "//tmp/t_out")

        rows = [{"key": str(i), "value": str(i)} for i in range(1)]
        sync_mount_table("//tmp/t1")
        insert_rows("//tmp/t1", rows)
        sync_unmount_table("//tmp/t1")

        joined_rows = [{"key": "0", "value": "joined"}]
        write_table("//tmp/t2", joined_rows, sorted_by=["key"])

        join_reduce(
            in_=["//tmp/t1", "<foreign=true>//tmp/t2"],
            out="//tmp/t_out",
            join_by="key",
            command="cat",
            spec={"reducer": {"format": "dsv"}},
        )

        assert read_table("//tmp/t_out") == rows + joined_rows

        rows = [{"key": str(i), "value": str(i + 1)} for i in range(1)]
        sync_mount_table("//tmp/t1")
        insert_rows("//tmp/t1", rows)
        sync_unmount_table("//tmp/t1")

        join_reduce(
            in_=["//tmp/t1", "<foreign=true>//tmp/t2"],
            out="//tmp/t_out",
            join_by="key",
            command="cat",
            spec={"reducer": {"format": "dsv"}},
        )

        assert read_table("//tmp/t_out") == rows + joined_rows

    @authors("max42")
    def test_join_reduce_with_dynamic_foreign(self):
        create(
            "table",
            "//tmp/t1",
            attributes={
                "schema": [
                    {"name": "key1", "type": "string", "sort_order": "ascending"},
                    {"name": "primary_value", "type": "int64"},
                ]
            },
        )

        sync_create_cells(1)
        create(
            "table",
            "//tmp/t2",
            attributes={
                "schema": [
                    {"name": "key1", "type": "string", "sort_order": "ascending"},
                    {"name": "key2", "type": "string", "sort_order": "ascending"},
                    {"name": "foreign_value", "type": "int64"},
                ],
                "dynamic": True,
            },
        )

        create("table", "//tmp/t_out")

        write_table("//tmp/t1", [{"key1": "7", "primary_value": 42}])

        rows = [{"key1": str(i), "key2": str(i * i), "foreign_value": i} for i in range(10)]
        sync_mount_table("//tmp/t2")
        insert_rows("//tmp/t2", rows)
        sync_unmount_table("//tmp/t2")

        join_reduce(
            in_=["//tmp/t1", "<foreign=%true>//tmp/t2"],
            out="//tmp/t_out",
            join_by="key1",
            command="cat",
            spec={"reducer": {"format": "dsv"}},
        )

        assert len(read_table("//tmp/t_out")) == 2

    @authors("klyachin", "gritukan")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    @pytest.mark.parametrize("sort_order", ["ascending", "descending"])
    def test_join_reduce_interrupt_job(self, optimize_for, sort_order):
        if sort_order == "descending":
            skip_if_no_descending(self.Env)
            self.skip_if_legacy_sorted_pool()

        create("table", "//tmp/input1", attributes={"optimize_for": optimize_for})
        rows = [
            {
                "key": "(%08d)" % (i * 2 + 1),
                "value": "(t_1)",
                "data": "a" * (2 * 1024 * 1024),
            }
            for i in range(3)
        ]
        if sort_order == "descending":
            rows = rows[::-1]

        write_table(
            "//tmp/input1",
            rows,
            sorted_by=[
                {"name": "key", "sort_order": sort_order},
                {"name": "value", "sort_order": sort_order},
            ],
        )

        create("table", "//tmp/input2", attributes={"optimize_for": optimize_for})
        rows = [{"key": "(%08d)" % (i / 2), "value": "(t_2)"} for i in range(14)]
        if sort_order == "descending":
            rows = rows[::-1]
        write_table(
            "//tmp/input2",
            rows,
            sorted_by=[{"name": "key", "sort_order": sort_order}],
        )

        create("table", "//tmp/output", attributes={"optimize_for": optimize_for})

        op = join_reduce(
            track=False,
            label="interrupt_job",
            in_=["<foreign=true>//tmp/input2", "//tmp/input1"],
            out="<sorted_by=[{{name=key;sort_order={}}}]>//tmp/output".format(sort_order),
            command=with_breakpoint("""read; echo "${REPLY/(???)/(job)}"; echo "$REPLY"; BREAKPOINT ; cat """),
            join_by=[{"name": "key", "sort_order": sort_order}],
            spec={
                "reducer": {"format": "dsv"},
                "max_failed_job_count": 1,
                "job_io": {
                    "buffer_row_count": 1,
                },
                "enable_job_splitting": False,
            },
        )

        jobs = wait_breakpoint()
        interrupt_job(jobs[0])
        release_breakpoint()
        op.track()

        result = read_table("//tmp/output", verbose=False)
        for row in result:
            print_debug("key:", row["key"], "value:", row["value"])
        assert len(result) == 11
        row_index = 0
        job_indexes = []
        row_table_count = {}
        for row in result:
            if row["value"] == "(job)":
                job_indexes.append(row_index)
            row_table_count[row["value"]] = row_table_count.get(row["value"], 0) + 1
            row_index += 1
        assert row_table_count["(job)"] == 2
        assert row_table_count["(t_1)"] == 3
        assert row_table_count["(t_2)"] == 6
        assert job_indexes[1] == 4
        assert (
            get(
                op.get_path()
                + "/@progress/job_statistics/data/input/row_count/$/completed/join_reduce/sum"
            )
            == len(result) - 2
        )

    @authors("psushin")
    @pytest.mark.parametrize("sort_order", ["ascending", "descending"])
    def test_join_reduce_job_splitter(self, sort_order):
        if sort_order == "descending":
            skip_if_no_descending(self.Env)
            self.skip_if_legacy_sorted_pool()

        create("table", "//tmp/in_1")
        for j in range(20):
            x = j if sort_order == "ascending" else 19 - j
            rows = [
                {
                    "key": "%08d" % (x * 4 + i),
                    "value": "(t_1)",
                    "data": "a" * (1024 * 1024),
                }
                for i in range(4)
            ]
            if sort_order == "descending":
                rows = rows[::-1]
            write_table(
                "<append=true>//tmp/in_1",
                rows,
                sorted_by=[{"name": "key", "sort_order": sort_order}],
                table_writer={
                    "block_size": 1024,
                },
            )

        create("table", "//tmp/in_2")
        for j in range(20):
            x = j if sort_order == "ascending" else 19 - j
            rows = [{"key": "(%08d)" % ((j * 10 + i) / 2), "value": "(t_2)"} for i in range(10)]
            if sort_order == "descending":
                rows = rows[::-1]
            write_table(
                "//tmp/in_2",
                rows,
                sorted_by=[{"name": "key", "sort_order": sort_order}],
                table_writer={
                    "block_size": 1024,
                },
            )

        input_ = ["<foreign=true>//tmp/in_2", "//tmp/in_1"]
        output = "//tmp/output"
        create("table", output)

        command = with_breakpoint(
            """
if [ "$YT_JOB_INDEX" == 0 ]; then
    BREAKPOINT
fi
while read ROW; do
    if [ "$YT_JOB_INDEX" == 0 ]; then
        sleep 3
    fi
    echo "$ROW"
done
"""
        )

        op = join_reduce(
            track=False,
            label="split_job",
            in_=input_,
            out=output,
            command=command,
            join_by=[{"name": "key", "sort_order": sort_order}],
            spec={
                "reducer": {
                    "format": "dsv",
                },
                "data_size_per_job": 17 * 1024 * 1024,
                "max_failed_job_count": 1,
                "job_io": {
                    "buffer_row_count": 1,
                },
            },
        )

        wait_breakpoint(job_count=1)
        wait(lambda: op.get_job_count("completed") >= 4)
        release_breakpoint()
        op.track()

        completed = get(op.get_path() + "/@progress/jobs/completed")
        interrupted = completed["interrupted"]
        assert completed["total"] >= 6
        assert interrupted["job_split"] >= 1

    @authors("renadeen")
    def test_join_reduce_two_primaries(self):
        create("table", "//tmp/in1")
        write_table("//tmp/in1", [{"key": 0}], sorted_by="key")

        create("table", "//tmp/in2")
        write_table("//tmp/in2", [{"key": 0}], sorted_by="key")

        create("table", "//tmp/in3")
        write_table("//tmp/in3", [{"key": 0, "value": 1}], sorted_by="key")

        create("table", "//tmp/out")

        join_reduce(
            in_=["//tmp/in1", "//tmp/in2", "<foreign=true>//tmp/in3"],
            out="//tmp/out",
            join_by="key",
            command="cat",
            spec={"reducer": {"format": "dsv"}},
        )

        expected = [{"key": "0"}, {"key": "0"}, {"key": "0", "value": "1"}]
        assert read_table("//tmp/out") == expected

    @authors("gritukan")
    def test_different_foreign_sort_columns(self):
        create("table", "//tmp/a", attributes={
            "schema": [
                {"name": "Host", "type": "string", "sort_order": "ascending"},
                {"name": "LastAccess", "type": "string", "sort_order": "ascending"},
            ]
        })
        create("table", "//tmp/b", attributes={
            "schema": [
                {"name": "Host", "type": "string", "sort_order": "ascending"},
                {"name": "Path", "type": "string", "sort_order": "ascending"},
                {"name": "LastAccess", "type": "string", "sort_order": "ascending"},
            ]
        })
        create("table", "//tmp/c")
        write_table("//tmp/a", [{"Host": "bar", "LastAccess": "a"}])
        write_table("//tmp/b", [{"Host": "bar", "Path": "foo", "LastAccess": "a"}])

        reduce(
            in_=["<foreign=true>//tmp/a", "//tmp/b"],
            out="//tmp/c",
            join_by=["Host"],
            reduce_by=["Host", "Path"],
            sort_by=["Host", "Path", "LastAccess"],
            command="cat",
            spec={"reducer": {"format": "dsv"}}
        )

        assert read_table("//tmp/c") == [
            {"Host": "bar", "LastAccess": "a"},
            {"Host": "bar", "Path": "foo", "LastAccess": "a"},
        ]

    @authors("gritukan")
    def test_foreign_table_read_range(self):
        create("table", "//tmp/in1")
        write_table(
            "//tmp/in1",
            [
                {"key": -1, "value": 1},
                {"key": 1, "value": 2},
                {"key": 3, "value": 3},
                {"key": 7, "value": 4},
            ],
            sorted_by=[{"name": "key", "sort_order": "ascending"}],
        )

        create("table", "//tmp/in2")
        write_table(
            "//tmp/in2",
            [
                {"key": -1, "value": 5},
                {"key": 1, "value": 6},
                {"key": 3, "value": 7},
                {"key": 5, "value": 8},
            ],
            sorted_by=[{"name": "key", "sort_order": "ascending"}],
        )

        create("table", "//tmp/out")

        primary_table = "<ranges=[{lower_limit={key=[0]};upper_limit={key=[2]}}]>//tmp/in1"
        foreign_table = "<foreign=true;ranges=[{lower_limit={key=[0]};upper_limit={key=[2]}}]>//tmp/in2"

        join_reduce(
            in_=[primary_table, foreign_table],
            out="<sorted_by=[{name=key;sort_order=ascending}]>//tmp/out",
            join_by={"name": "key", "sort_order": "ascending"},
            command="cat",
            spec={"reducer": {"format": "dsv"}},
        )

        assert read_table("//tmp/out") == [
            {"key": "1", "value": "2"},
            {"key": "1", "value": "6"},
        ]

        with raises_yt_error("does not support foreign tables with multiple ranges"):
            foreign_table = "<foreign=true;ranges=[{lower_limit={key=[0]};upper_limit={key=[2]}};"\
                            "{lower_limit={key=[3]};upper_limit={key=[4]}}]>//tmp/in2"
            join_reduce(
                in_=[primary_table, foreign_table],
                out="<sorted_by=[{name=key;sort_order=ascending}]>//tmp/out",
                join_by={"name": "key", "sort_order": "ascending"},
                command="cat",
                spec={"reducer": {"format": "dsv"}},
            )


class TestSchedulerJoinReduceCommandsMulticell(TestSchedulerJoinReduceCommands):
    NUM_SECONDARY_MASTER_CELLS = 2


##################################################################


class TestMaxTotalSliceCount(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "max_total_slice_count": 3,
        }
    }

    @authors("ignat")
    def test_hit_limit(self):
        create("table", "//tmp/t_primary")
        write_table("//tmp/t_primary", [{"key": 0}, {"key": 10}], sorted_by=["key"])

        create("table", "//tmp/t_foreign")
        write_table(
            "<append=true; sorted_by=[key]>//tmp/t_foreign",
            [{"key": 0}, {"key": 1}, {"key": 2}],
        )
        write_table(
            "<append=true; sorted_by=[key]>//tmp/t_foreign",
            [{"key": 3}, {"key": 4}, {"key": 5}],
        )
        write_table(
            "<append=true; sorted_by=[key]>//tmp/t_foreign",
            [{"key": 6}, {"key": 7}, {"key": 8}],
        )

        create("table", "//tmp/t_out")
        with raises_yt_error(yt_error_codes.DataSliceLimitExceeded):
            join_reduce(
                in_=["//tmp/t_primary", "<foreign=true>//tmp/t_foreign"],
                out="//tmp/t_out",
                join_by=["key"],
                command="cat > /dev/null",
            )


##################################################################


class TestSchedulerJoinReduceCommandsNewSortedPool(TestSchedulerJoinReduceCommands):
    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "watchers_update_period": 100,
            "operations_update_period": 10,
            "running_jobs_update_period": 10,
        }
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "operations_update_period": 10,
            "join_reduce_operation_options": {
                "job_splitter": {
                    "min_job_time": 3000,
                    "min_total_data_size": 1024,
                    "update_period": 100,
                    "candidate_percentile": 0.8,
                    "max_jobs_per_split": 3,
                },
            },
            "operation_options": {
                "spec_template": {
                    "use_new_sorted_pool": True,
                }
            }
        }
    }
