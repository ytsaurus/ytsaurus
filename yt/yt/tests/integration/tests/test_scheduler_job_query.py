from yt_env_setup import YTEnvSetup, find_ut_file

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
    read_file, write_file, read_table, write_table, write_local_file,
    map, reduce, map_reduce, join_reduce, merge, vanilla, sort, erase,
    run_test_vanilla, run_sleeping_vanilla,
    abort_job, list_jobs, get_job, abandon_job,
    get_job_fail_context, get_job_input, get_job_stderr, get_job_spec,
    dump_job_context, poll_job_shell,
    abort_op, complete_op, suspend_op, resume_op,
    get_operation, list_operations, clean_operations,
    get_operation_cypress_path, scheduler_orchid_pool_path,
    scheduler_orchid_default_pool_tree_path, scheduler_orchid_operation_path,
    scheduler_orchid_default_pool_tree_config_path, scheduler_orchid_path,
    scheduler_orchid_node_path, scheduler_orchid_pool_tree_config_path,
    sync_create_cells, sync_mount_table,
    get_first_chunk_id, get_singular_chunk_id, multicell_sleep,
    update_nodes_dynamic_config, update_controller_agent_config,
    update_op_parameters, enable_op_detailed_logs,
    set_node_banned, set_banned_flag,
    check_all_stderrs,
    create_test_tables, PrepareTables,
    get_statistics,
    make_random_string, raises_yt_error)

import yt_error_codes

from yt.common import YtError
from yt.test_helpers import assert_items_equal

import pytest
import os

##################################################################


class TestJobQuery(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 5
    NUM_SCHEDULERS = 1

    DELTA_CONTROLLER_AGENT_CONFIG = {"controller_agent": {"udf_registry_path": "//tmp/udfs"}}

    def _init_udf_registry(self):
        registry_path = "//tmp/udfs"
        create("map_node", registry_path)

        abs_path = os.path.join(registry_path, "abs_udf")
        create(
            "file",
            abs_path,
            attributes={
                "function_descriptor": {
                    "name": "abs_udf",
                    "argument_types": [{"tag": "concrete_type", "value": "int64"}],
                    "result_type": {"tag": "concrete_type", "value": "int64"},
                    "calling_convention": "simple",
                }
            },
        )

        abs_impl_path = find_ut_file("test_udfs.bc")
        write_local_file(abs_path, abs_impl_path)

    @authors("lukyan")
    def test_query_simple(self):
        create(
            "table",
            "//tmp/t1",
            attributes={"schema": [{"name": "a", "type": "string"}]},
        )
        create("table", "//tmp/t2")
        write_table("//tmp/t1", {"a": "b"})

        map(in_="//tmp/t1", out="//tmp/t2", command="cat", spec={"input_query": "a"})

        assert read_table("//tmp/t2") == [{"a": "b"}]

    @authors("gritukan")
    def test_query_invalid(self):
        create(
            "table",
            "//tmp/t1",
            attributes={"schema": [{"name": "a", "type": "string"}]},
        )
        create("table", "//tmp/t2")
        write_table("//tmp/t1", {"a": "b"})

        with raises_yt_error(yt_error_codes.OperationFailedToPrepare):
            map(
                in_="//tmp/t1",
                out="//tmp/t2",
                command="cat",
                spec={"input_query": "a,a"},
            )

        with raises_yt_error(yt_error_codes.OperationFailedToPrepare):
            map(in_="//tmp/t1", out="//tmp/t2", command="cat", spec={"input_query": "b"})

    @authors("lukyan")
    def test_query_two_input_tables(self):
        create(
            "table",
            "//tmp/t1",
            attributes={
                "schema": [
                    {"name": "a", "type": "string"},
                    {"name": "b", "type": "string"},
                ]
            },
        )
        create(
            "table",
            "//tmp/t2",
            attributes={
                "schema": [
                    {"name": "a", "type": "string"},
                    {"name": "c", "type": "string"},
                ]
            },
        )
        create("table", "//tmp/t_out")
        write_table("//tmp/t1", {"a": "1", "b": "1"})
        write_table("//tmp/t2", {"a": "2", "c": "2"})

        map(
            in_=["//tmp/t1", "//tmp/t2"],
            out="//tmp/t_out",
            command="cat",
            spec={"input_query": "*"},
        )

        expected = [{"a": "1", "b": "1", "c": None}, {"a": "2", "b": None, "c": "2"}]
        assert_items_equal(read_table("//tmp/t_out"), expected)

    @authors("savrus", "lukyan")
    def test_query_reader_projection(self):
        create(
            "table",
            "//tmp/t1",
            attributes={
                "schema": [
                    {"name": "a", "type": "string"},
                    {"name": "c", "type": "string"},
                ],
                "optimize_for": "scan",
            },
        )
        create("table", "//tmp/t2")
        write_table("//tmp/t1", {"a": "b", "c": "d"})

        op = map(in_="//tmp/t1", out="//tmp/t2", command="cat", spec={"input_query": "a"})

        assert read_table("//tmp/t2") == [{"a": "b"}]
        statistics = get(op.get_path() + "/@progress/job_statistics")
        attrs = get("//tmp/t1/@")
        assert (
            get_statistics(statistics, "data.input.uncompressed_data_size.$.completed.map.sum")
            < attrs["uncompressed_data_size"]
        )
        assert (
            get_statistics(statistics, "data.input.compressed_data_size.$.completed.map.sum")
            < attrs["compressed_data_size"]
        )
        assert get_statistics(statistics, "data.input.data_weight.$.completed.map.sum") < attrs["data_weight"]

    @authors("lukyan")
    @pytest.mark.parametrize("mode", ["ordered", "unordered"])
    def test_query_filtering(self, mode):
        create("table", "//tmp/t1", attributes={"schema": [{"name": "a", "type": "int64"}]})
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"a": i} for i in xrange(2)])

        map(
            in_="//tmp/t1",
            out="//tmp/t2",
            command="cat",
            mode=mode,
            spec={"input_query": "a where a > 0"},
        )

        assert read_table("//tmp/t2") == [{"a": 1}]

    @authors("lukyan")
    def test_query_asterisk(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        rows = [{"a": 1, "b": 2, "c": 3}, {"b": 5, "c": 6}, {"a": 7, "c": 8}]
        write_table("//tmp/t1", rows)

        schema = [
            {"name": "z", "type": "int64"},
            {"name": "a", "type": "int64"},
            {"name": "y", "type": "int64"},
            {"name": "b", "type": "int64"},
            {"name": "x", "type": "int64"},
            {"name": "c", "type": "int64"},
            {"name": "u", "type": "int64"},
        ]

        for row in rows:
            for column in schema:
                if column["name"] not in row.keys():
                    row[column["name"]] = None

        map(
            in_="//tmp/t1",
            out="//tmp/t2",
            command="cat",
            spec={"input_query": "* where a > 0 or b > 0", "input_schema": schema},
        )

        assert_items_equal(read_table("//tmp/t2"), rows)

    @authors("lukyan")
    def test_query_schema_in_spec(self):
        create(
            "table",
            "//tmp/t1",
            attributes={"schema": [{"name": "a", "type": "string"}]},
        )
        create(
            "table",
            "//tmp/t2",
            attributes={
                "schema": [
                    {"name": "a", "type": "string"},
                    {"name": "b", "type": "string"},
                ]
            },
        )
        create("table", "//tmp/t_out")
        write_table("//tmp/t1", {"a": "b"})
        write_table("//tmp/t2", {"a": "b"})

        map(
            in_="//tmp/t1",
            out="//tmp/t_out",
            command="cat",
            spec={
                "input_query": "*",
                "input_schema": [
                    {"name": "a", "type": "string"},
                    {"name": "b", "type": "string"},
                ],
            },
        )

        assert read_table("//tmp/t_out") == [{"a": "b", "b": None}]

        map(
            in_="//tmp/t2",
            out="//tmp/t_out",
            command="cat",
            spec={
                "input_query": "*",
                "input_schema": [{"name": "a", "type": "string"}],
            },
        )

        assert read_table("//tmp/t_out") == [{"a": "b"}]

    @authors("lukyan")
    def test_query_udf(self):
        self._init_udf_registry()

        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"a": i} for i in xrange(-1, 1)])

        map(
            in_="//tmp/t1",
            out="//tmp/t2",
            command="cat",
            spec={
                "input_query": "a where abs_udf(a) > 0",
                "input_schema": [{"name": "a", "type": "int64"}],
            },
        )

        assert read_table("//tmp/t2") == [{"a": -1}]

    @authors("lukyan")
    def test_query_wrong_schema(self):
        create(
            "table",
            "//tmp/t1",
            attributes={"schema": [{"name": "a", "type": "string"}]},
        )
        create("table", "//tmp/t2")
        write_table("//tmp/t1", {"a": "b"})

        with pytest.raises(YtError):
            map(
                in_="//tmp/t1",
                out="//tmp/t2",
                command="cat",
                spec={
                    "input_query": "a",
                    "input_schema": [{"name": "a", "type": "int64"}],
                },
            )

    @authors("lukyan")
    def test_query_range_inference(self):
        create(
            "table",
            "//tmp/t",
            attributes={"schema": [{"name": "a", "type": "int64", "sort_order": "ascending"}]},
        )
        create("table", "//tmp/t_out")
        for i in range(3):
            write_table("<append=%true>//tmp/t", [{"a": i * 10 + j} for j in xrange(3)])
        assert get("//tmp/t/@chunk_count") == 3

        def _test(selector, query, rows, chunk_count):
            op = map(
                in_="//tmp/t" + selector,
                out="//tmp/t_out",
                command="cat",
                spec={"input_query": query},
            )

            assert_items_equal(read_table("//tmp/t_out"), rows)
            statistics = get(op.get_path() + "/@progress/job_statistics")
            assert get_statistics(statistics, "data.input.chunk_count.$.completed.map.sum") == chunk_count

        _test("", "a where a between 5 and 15", [{"a": i} for i in xrange(10, 13)], 1)
        _test("[#0:]", "a where a between 5 and 15", [{"a": i} for i in xrange(10, 13)], 1)
        _test(
            "[11:12]",
            "a where a between 5 and 15",
            [{"a": i} for i in xrange(11, 12)],
            1,
        )
        _test(
            "[9:20]",
            "a where a between 5 and 15",
            [{"a": i} for i in xrange(10, 13)],
            1,
        )
        _test("[#2:#4]", "a where a <= 10", [{"a": 2}, {"a": 10}], 2)
        _test("[10]", "a where a > 0", [{"a": 10}], 1)

    @authors("savrus")
    def test_query_range_inference_with_computed_columns(self):
        create(
            "table",
            "//tmp/t",
            attributes={
                "schema": [
                    {
                        "name": "h",
                        "type": "int64",
                        "sort_order": "ascending",
                        "expression": "k + 100",
                    },
                    {"name": "k", "type": "int64", "sort_order": "ascending"},
                    {"name": "a", "type": "int64", "sort_order": "ascending"},
                ]
            },
        )
        create("table", "//tmp/t_out")
        for i in range(3):
            write_table("<append=%true>//tmp/t", [{"k": i, "a": i * 10 + j} for j in xrange(3)])
        assert get("//tmp/t/@chunk_count") == 3

        def _test(query, rows, chunk_count):
            op = map(
                in_="//tmp/t",
                out="//tmp/t_out",
                command="cat",
                spec={"input_query": query},
            )

            assert_items_equal(read_table("//tmp/t_out"), rows)
            statistics = get(op.get_path() + "/@progress/job_statistics")
            assert get_statistics(statistics, "data.input.chunk_count.$.completed.map.sum") == chunk_count

        _test("a where k = 1", [{"a": i} for i in xrange(10, 13)], 1)
        _test(
            "a where k = 1 and a between 5 and 15",
            [{"a": i} for i in xrange(10, 13)],
            1,
        )
        _test(
            "a where k in (1, 2) and a between 5 and 15",
            [{"a": i} for i in xrange(10, 13)],
            1,
        )
