from yt_env_setup import YTEnvSetup

from yt_commands import (  # noqa
    authors, print_debug, wait, wait_assert, wait_breakpoint, release_breakpoint, with_breakpoint,
    events_on_fs, reset_events_on_fs,
    create, ls, get, move, remove, link, exists,
    create_account, create_network_project, create_tmpdir, create_user, create_group,
    create_pool, create_pool_tree,
    create_data_center, create_rack,
    make_ace, check_permission, add_member,
    make_batch_request, execute_batch, get_batch_error,
    start_transaction, abort_transaction, lock,
    read_file, write_file, read_table, write_table,
    map, reduce, map_reduce, join_reduce, merge, vanilla, sort, erase,
    run_test_vanilla, run_sleeping_vanilla,
    abort_job, list_jobs, get_job, abandon_job,
    get_job_fail_context, get_job_input, get_job_stderr, get_job_spec,
    dump_job_context, poll_job_shell,
    abort_op, complete_op, suspend_op, resume_op,
    clean_operations,
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
    make_random_string, raises_yt_error,
    normalize_schema, make_schema)

##################################################################


def check_attributes(op, options):
    spec_path = op.get_path() + "/@spec"
    brief_spec_path = op.get_path() + "/@brief_spec"

    if "pool" in options:
        assert get(spec_path + "/pool") == get(brief_spec_path + "/pool")
    if "reducer" in options:
        assert get(spec_path + "/reducer/command") == get(brief_spec_path + "/reducer/command")
    if "mapper" in options:
        assert get(spec_path + "/mapper/command") == get(brief_spec_path + "/mapper/command")
    if "table_path" in options:
        assert get(spec_path + "/table_path") == get(brief_spec_path + "/table_path")

    if "input_table_path" in options:
        assert get(brief_spec_path + "/input_table_paths/@count") == len(list(get(spec_path + "/input_table_paths")))
        assert get(spec_path + "/input_table_paths/0") == get(brief_spec_path + "/input_table_paths/0")

    if "output_table_path" in options:
        assert get(brief_spec_path + "/output_table_paths/@count") == len(list(get(spec_path + "/output_table_paths")))
        assert get(spec_path + "/output_table_paths/0") == get(brief_spec_path + "/output_table_paths/0")

    if "output_table_path_1" in options:
        assert get(brief_spec_path + "/output_table_paths/@count") == 1
        assert get(spec_path + "/output_table_path") == get(brief_spec_path + "/output_table_paths/0")


class TestSchedulerBriefSpec(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 5
    NUM_SCHEDULERS = 1

    @authors("acid", "babenko", "ignat")
    def test_map(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        op = map(in_="//tmp/t1", out="//tmp/t2", command="cat")

        check_attributes(op, ["mapper", "input_table_path", "output_table_path"])

    @authors("babenko", "klyachin")
    def test_sort(self):
        create("table", "//tmp/t1")

        op = sort(in_="//tmp/t1", out="//tmp/t1", sort_by="key")

        check_attributes(op, ["input_table_path", "output_table_path_1"])

    @authors("ignat", "klyachin")
    def test_reduce(self):
        create("table", "//tmp/t1")
        write_table(
            "//tmp/t1",
            [
                {"key": 9, "value": 7},
            ],
            sorted_by=["key", "value"],
        )

        create("table", "//tmp/t2")
        op = reduce(in_="//tmp/t1", out="//tmp/t2", command="cat", reduce_by="key")

        check_attributes(op, ["reducer", "input_table_path", "output_table_path"])

    @authors("klyachin")
    def test_join_reduce(self):
        create("table", "//tmp/t1")
        write_table(
            "//tmp/t1",
            [
                {"key": 9, "value": 7},
            ],
            sorted_by=["key", "value"],
        )

        create("table", "//tmp/t2")
        write_table(
            "//tmp/t2",
            [
                {"key": 9, "value": 11},
            ],
            sorted_by=["key", "value"],
        )

        create("table", "//tmp/t3")
        op = join_reduce(
            in_=["//tmp/t1", "<foreign=true>//tmp/t2"],
            out="//tmp/t3",
            command="cat",
            join_by="key",
            spec={"reducer": {"format": "dsv"}},
        )

        check_attributes(op, ["reducer", "input_table_path", "output_table_path"])

    @authors("klyachin")
    def test_map_reduce(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        op = map_reduce(
            in_="//tmp/t1",
            out="//tmp/t2",
            sort_by="a",
            mapper_command="cat",
            reduce_combiner_command="cat",
            reducer_command="cat",
        )

        check_attributes(op, ["mapper", "reducer", "input_table_path", "output_table_path"])

    @authors("klyachin")
    def test_merge(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        create("table", "//tmp/t3")

        op = merge(mode="unordered", in_=["//tmp/t1", "//tmp/t2"], out="//tmp/t3")

        check_attributes(op, ["input_table_path", "output_table_path_1"])

    @authors("babenko")
    def test_erase(self):
        create("table", "//tmp/t1")

        op = erase("//tmp/t1")

        check_attributes(op, ["table_path"])
