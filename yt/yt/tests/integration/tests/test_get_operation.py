from yt_env_setup import YTEnvSetup

from yt_commands import (  # noqa
    authors, print_debug, wait, retry, wait_assert, wait_breakpoint, release_breakpoint, with_breakpoint,
    events_on_fs, reset_events_on_fs,
    create, ls, get, set, copy, move, remove, link, exists, concatenate,
    create_account, create_network_project, create_tmpdir, create_user, create_group,
    create_pool, create_pool_tree, remove_pool_tree,
    create_data_center, create_rack, create_table,
    create_tablet_cell_bundle, remove_tablet_cell_bundle, create_tablet_cell, create_table_replica,
    make_ace, check_permission, add_member, remove_member, remove_group, remove_user,
    remove_network_project,
    make_batch_request, execute_batch, get_batch_error,
    start_transaction, abort_transaction, commit_transaction, lock,
    insert_rows, select_rows, lookup_rows, delete_rows, trim_rows, alter_table,
    read_file, write_file, read_table, write_table, write_local_file, read_blob_table,
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
    set_node_banned, set_banned_flag, set_account_disk_space_limit, set_node_decommissioned,
    check_all_stderrs,
    create_test_tables, create_dynamic_table, PrepareTables,
    get_statistics, get_recursive_disk_space, get_chunk_owner_disk_space,
    make_random_string, raises_yt_error,
    build_snapshot, gc_collect, is_multicell,
    get_driver, Driver, execute_command,
    AsyncLastCommittedTimestamp)

import yt_error_codes

import yt.environment.init_operation_archive as init_operation_archive

from yt.common import uuid_to_parts, YtError

import pytest


def _get_orchid_operation_path(op_id):
    return "//sys/scheduler/orchid/scheduler/operations/{0}/progress".format(op_id)


def _get_operation_from_cypress(op_id):
    result = get(get_operation_cypress_path(op_id) + "/@")
    if "full_spec" in result:
        result["full_spec"].attributes.pop("opaque", None)
    result["type"] = result["operation_type"]
    result["id"] = op_id
    return result


def _get_operation_from_archive(op_id):
    id_hi, id_lo = uuid_to_parts(op_id)
    archive_table_path = "//sys/operations_archive/ordered_by_id"
    rows = lookup_rows(archive_table_path, [{"id_hi": id_hi, "id_lo": id_lo}])
    if rows:
        return rows[0]
    else:
        return {}


def get_running_job_count(op_id):
    wait(lambda: "brief_progress" in get_operation(op_id, attributes=["brief_progress"]))
    result = get_operation(op_id, attributes=["brief_progress"])
    return result["brief_progress"]["jobs"]["running"]


class TestGetOperation(YTEnvSetup):
    NUM_TEST_PARTITIONS = 4
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1
    USE_DYNAMIC_TABLES = True

    DELTA_DRIVER_CONFIG = {
        "default_get_operation_timeout": 10 * 1000,
    }

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "watchers_update_period": 100,
            "operations_update_period": 10,
            "operations_cleaner": {
                "enable": False,
                "analysis_period": 100,
                # Cleanup all operations
                "hard_retained_operation_count": 0,
                "clean_delay": 0,
            },
            "static_orchid_cache_update_period": 100,
            "alerts_update_period": 100,
        },
    }

    def setup(self):
        sync_create_cells(1)
        init_operation_archive.create_tables_latest_version(
            self.Env.create_native_client(), override_tablet_cell_bundle="default"
        )

    def teardown(self):
        remove("//sys/operations_archive", force=True)

    def clean_build_time(self, operation_result):
        del operation_result["brief_progress"]["build_time"]
        del operation_result["progress"]["build_time"]

    @authors("levysotsky", "babenko", "ignat")
    def test_get_operation(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"foo": "bar"}, {"foo": "baz"}, {"foo": "qux"}])

        op = map(
            track=False,
            label="get_job_stderr",
            in_="//tmp/t1",
            out="//tmp/t2",
            command=with_breakpoint("cat ; BREAKPOINT"),
            spec={"mapper": {"input_format": "json", "output_format": "json"}},
        )
        wait_breakpoint()

        wait(lambda: exists(op.get_path()))

        wait(lambda: get_running_job_count(op.id) == 1)

        res_get_operation = get_operation(op.id, include_scheduler=True)
        res_cypress = _get_operation_from_cypress(op.id)
        res_orchid_progress = get(_get_orchid_operation_path(op.id))

        def filter_attrs(attrs):
            PROPER_ATTRS = [
                "id",
                "authenticated_user",
                "brief_spec",
                "runtime_parameters",
                "finish_time",
                "type",
                # COMPAT(levysotsky): Old name for "type"
                "operation_type",
                "result",
                "start_time",
                "state",
                "suspended",
                "spec",
                "unrecognized_spec",
                "full_spec",
                "slot_index_per_pool_tree",
                "alerts",
            ]
            return {key: attrs[key] for key in PROPER_ATTRS if key in attrs}

        assert filter_attrs(res_get_operation) == filter_attrs(res_cypress)

        res_get_operation_progress = res_get_operation["progress"]

        assert res_get_operation["operation_type"] == res_get_operation["type"]

        for key in res_orchid_progress:
            if key != "build_time":
                assert key in res_get_operation_progress

        release_breakpoint()
        op.track()

        res_cypress_finished = _get_operation_from_cypress(op.id)

        clean_operations()

        res_get_operation_archive = get_operation(op.id)

        for key in res_get_operation_archive.keys():
            if key in res_cypress:
                assert res_get_operation_archive[key] == res_cypress_finished[key]

    # Check that operation that has not been saved by operation cleaner
    # is reported correctly (i.e. "No such operation").
    # Actually, cleaner is disabled in this test,
    # but we emulate its work by removing operation node from Cypress.
    @authors("levysotsky")
    def test_get_operation_dropped_by_cleaner(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"foo": "bar"}, {"foo": "baz"}, {"foo": "qux"}])
        op = map(
            track=False,
            in_="//tmp/t1",
            out="//tmp/t2",
            command=with_breakpoint("cat ; BREAKPOINT"),
        )
        wait_breakpoint()

        wait(lambda: _get_operation_from_archive(op.id))

        release_breakpoint()
        op.track()

        remove(op.get_path(), force=True)
        with raises_yt_error(yt_error_codes.NoSuchOperation):
            get_operation(op.id)

    @authors("ilpauzner")
    def test_progress_merge(self):
        update_controller_agent_config("enable_operation_progress_archivation", False)
        self.do_test_progress_merge()

    def do_test_progress_merge(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"foo": "bar"}, {"foo": "baz"}, {"foo": "qux"}])

        op = map(
            track=False,
            label="get_job_stderr",
            in_="//tmp/t1",
            out="//tmp/t2",
            command=with_breakpoint("cat ; BREAKPOINT"),
            spec={"mapper": {"input_format": "json", "output_format": "json"}},
        )
        wait_breakpoint()

        id_hi, id_lo = uuid_to_parts(op.id)
        archive_table_path = "//sys/operations_archive/ordered_by_id"
        brief_progress = {"ivan": "ivanov", "build_time": "2100-01-01T00:00:00.000000Z"}
        progress = {
            "semen": "semenych",
            "semenych": "gorbunkov",
            "build_time": "2100-01-01T00:00:00.000000Z",
        }

        insert_rows(
            archive_table_path,
            [
                {
                    "id_hi": id_hi,
                    "id_lo": id_lo,
                    "brief_progress": brief_progress,
                    "progress": progress,
                }
            ],
            update=True,
        )

        wait(lambda: _get_operation_from_archive(op.id))

        res_get_operation_new = get_operation(op.id)
        self.clean_build_time(res_get_operation_new)
        assert res_get_operation_new["brief_progress"] == {"ivan": "ivanov"}
        assert res_get_operation_new["progress"] == {
            "semen": "semenych",
            "semenych": "gorbunkov",
        }

        release_breakpoint()
        op.track()

        clean_operations()
        res_get_operation_new = get_operation(op.id)
        self.clean_build_time(res_get_operation_new)
        assert res_get_operation_new["brief_progress"] != {"ivan": "ivanov"}
        assert res_get_operation_new["progress"] != {
            "semen": "semenych",
            "semenych": "gorbunkov",
        }

    @authors("ignat")
    def test_attributes(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"foo": "bar"}, {"foo": "baz"}, {"foo": "qux"}])

        op = map(
            track=False,
            label="get_job_stderr",
            in_="//tmp/t1",
            out="//tmp/t2",
            command=with_breakpoint("cat ; BREAKPOINT"),
            spec={"mapper": {"input_format": "json", "output_format": "json"}},
        )
        wait_breakpoint()

        assert list(get_operation(op.id, attributes=["state"])) == ["state"]
        assert list(get_operation(op.id, attributes=["progress"])) == ["progress"]
        with pytest.raises(YtError):
            get_operation(op.id, attributes=["PYSCH"])

        for read_from in ("cache", "follower"):
            res_get_operation = get_operation(
                op.id,
                attributes=["progress", "state"],
                include_scheduler=True,
                read_from=read_from,
            )
            res_cypress = get(op.get_path() + "/@", attributes=["progress", "state"])

            assert sorted(list(res_get_operation)) == ["progress", "state"]
            assert "state" in sorted(list(res_cypress))
            assert res_get_operation["state"] == res_cypress["state"]
            assert ("alerts" in res_get_operation) == ("alerts" in res_cypress)
            if "alerts" in res_get_operation and "alerts" in res_cypress:
                assert res_get_operation["alerts"] == res_cypress["alerts"]

        with raises_yt_error(yt_error_codes.NoSuchAttribute):
            get_operation(op.id, attributes=["nonexistent-attribute-ZZZ"])

        release_breakpoint()
        op.track()

        clean_operations()

        assert list(get_operation(op.id, attributes=["progress"])) == ["progress"]

        requesting_attributes = [
            "progress",
            "runtime_parameters",
            "slot_index_per_pool_tree",
            "state",
        ]
        res_get_operation_archive = get_operation(op.id, attributes=requesting_attributes)
        assert sorted(list(res_get_operation_archive)) == requesting_attributes
        assert res_get_operation_archive["state"] == "completed"
        assert (
            res_get_operation_archive["runtime_parameters"]["scheduling_options_per_pool_tree"]["default"]["pool"]
            == "root"
        )
        assert res_get_operation_archive["slot_index_per_pool_tree"]["default"] == 0

        with raises_yt_error(yt_error_codes.NoSuchAttribute):
            get_operation(op.id, attributes=["nonexistent-attribute-ZZZ"])

    @authors("gritukan")
    def test_task_names_attribute_vanilla(self):
        op = vanilla(
            track=False,
            spec={
                "tasks": {
                    "master": {
                        "job_count": 1,
                        "command": with_breakpoint("BREAKPOINT"),
                    },
                    "slave": {
                        "job_count": 2,
                        "command": with_breakpoint("BREAKPOINT"),
                    },
                },
            },
        )

        wait(lambda: len(op.get_running_jobs()) == 3)

        def check_task_names():
            assert sorted(list(get_operation(op.id, attributes=["task_names"])["task_names"])) == ["master", "slave"]

        check_task_names()

        release_breakpoint()
        op.track()
        clean_operations()

        check_task_names()

    @authors("levysotsky")
    def test_get_operation_and_half_deleted_operation_node(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"foo": "bar"}, {"foo": "baz"}, {"foo": "qux"}])

        op = map(in_="//tmp/t1", out="//tmp/t2", command="cat")

        tx = start_transaction(timeout=300 * 1000)
        lock(
            op.get_path(),
            mode="shared",
            child_key="completion_transaction_id",
            transaction_id=tx,
        )

        cleaner_path = "//sys/scheduler/config/operations_cleaner"
        set(cleaner_path + "/enable", True, recursive=True)
        wait(lambda: not exists("//sys/operations/" + op.id))
        wait(lambda: exists(op.get_path()))
        wait(lambda: "state" in get_operation(op.id))

    @authors("kiselyovp", "ilpauzner")
    def test_not_existing_operation(self):
        with raises_yt_error(yt_error_codes.NoSuchOperation):
            get_operation("00000000-00000000-0000000-00000001")

    @authors("levysotsky")
    def test_archive_progress(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"foo": "bar"}, {"foo": "baz"}, {"foo": "qux"}])

        op = map(
            track=False,
            label="get_job_stderr",
            in_="//tmp/t1",
            out="//tmp/t2",
            command=with_breakpoint("cat ; BREAKPOINT"),
            spec={"mapper": {"input_format": "json", "output_format": "json"}},
        )
        wait_breakpoint()

        wait(lambda: _get_operation_from_archive(op.id))

        row = _get_operation_from_archive(op.id)
        assert "progress" in row
        assert "brief_progress" in row

        release_breakpoint()
        op.track()

    @authors("levysotsky")
    def test_archive_failure(self):
        if self.ENABLE_RPC_PROXY:
            pytest.skip("This test is independent from rpc proxy.")

        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"foo": "bar"}, {"foo": "baz"}, {"foo": "qux"}])

        # Unmount table to check that controller agent writes to Cypress during archive unavailability.
        sync_unmount_table("//sys/operations_archive/ordered_by_id")

        op = map(
            track=False,
            label="get_job_stderr",
            in_="//tmp/t1",
            out="//tmp/t2",
            command=with_breakpoint("cat ; BREAKPOINT"),
            spec={"mapper": {"input_format": "json", "output_format": "json"}},
        )

        wait_breakpoint()
        wait(lambda: _get_operation_from_cypress(op.id).get("brief_progress", {}).get("jobs", {}).get("running") == 1)

        res_api = get_operation(op.id)
        self.clean_build_time(res_api)
        res_cypress = _get_operation_from_cypress(op.id)
        self.clean_build_time(res_cypress)

        assert res_api["brief_progress"] == res_cypress["brief_progress"]
        assert res_api["progress"] == res_cypress["progress"]

        sync_mount_table("//sys/operations_archive/ordered_by_id")

        wait(lambda: _get_operation_from_archive(op.id))
        assert _get_operation_from_archive(op.id).get("brief_progress", {}).get("jobs", {}).get("running") == 1
        release_breakpoint()
        op.track()

        res_api = get_operation(op.id)
        res_cypress = _get_operation_from_cypress(op.id)

        assert res_api["brief_progress"]["jobs"]["running"] == 0
        # Brief progress in Cypress is obsolete as archive is up now.
        assert res_cypress["brief_progress"]["jobs"]["running"] > 0

        assert res_api["progress"]["jobs"]["running"] == 0
        # Progress in Cypress is obsolete as archive is up now.
        assert res_cypress["progress"]["jobs"]["running"] > 0

        sync_unmount_table("//sys/operations_archive/ordered_by_id")
        with raises_yt_error(yt_error_codes.OperationProgressOutdated):
            get_operation(op.id, maximum_cypress_progress_age=0)
        sync_mount_table("//sys/operations_archive/ordered_by_id")

        clean_operations()

        res_api = get_operation(op.id)
        res_archive = _get_operation_from_archive(op.id)

        assert res_api["brief_progress"] == res_archive["brief_progress"]
        assert res_api["progress"] == res_archive["progress"]

        # Unmount table again and check that error is _not_ "No such operation".
        sync_unmount_table("//sys/operations_archive/ordered_by_id")
        with raises_yt_error(yt_error_codes.TabletNotMounted):
            get_operation(op.id)

    @authors("levysotsky")
    def test_get_operation_no_archive(self):
        remove("//sys/operations_archive", force=True)
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"foo": "bar"}, {"foo": "baz"}, {"foo": "qux"}])

        op = map(
            track=False,
            in_="//tmp/t1",
            out="//tmp/t2",
            command=with_breakpoint("cat ; BREAKPOINT"),
        )

        wait_breakpoint()
        wait(lambda: _get_operation_from_cypress(op.id).get("brief_progress", {}).get("jobs", {}).get("running") == 1)

        res_api = get_operation(op.id)
        self.clean_build_time(res_api)
        res_cypress = _get_operation_from_cypress(op.id)
        self.clean_build_time(res_cypress)

        assert res_api["brief_progress"] == res_cypress["brief_progress"]
        assert res_api["progress"] == res_cypress["progress"]


##################################################################


class TestGetOperationRpcProxy(TestGetOperation):
    USE_DYNAMIC_TABLES = True
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True
    ENABLE_HTTP_PROXY = True


##################################################################


class TestOperationAliases(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_SCHEDULERS = 1
    NUM_NODES = 3

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "operations_cleaner": {
                "enable": False,
                # Analyze all operations each 100ms
                "analysis_period": 100,
                # Wait each batch to remove not more than 100ms
                "remove_batch_timeout": 100,
                # Wait each batch to archive not more than 100ms
                "archive_batch_timeout": 100,
                # Retry sleeps
                "min_archivation_retry_sleep_delay": 100,
                "max_archivation_retry_sleep_delay": 110,
                # Leave no more than 5 completed operations
                "soft_retained_operation_count": 0,
                # Operations older than 50ms can be considered for removal
                "clean_delay": 50,
            },
            "static_orchid_cache_update_period": 100,
            "alerts_update_period": 100,
        }
    }

    def setup(self):
        # Init operations archive.
        sync_create_cells(1)

    @authors("max42")
    def test_aliases(self):
        with pytest.raises(YtError):
            # Alias should start with *.
            vanilla(
                spec={
                    "tasks": {"main": {"command": "sleep 1000", "job_count": 1}},
                    "alias": "my_op",
                }
            )

        op = vanilla(
            spec={
                "tasks": {"main": {"command": "sleep 1000", "job_count": 1}},
                "alias": "*my_op",
            },
            track=False,
        )

        assert ls("//sys/scheduler/orchid/scheduler/operations") == [op.id, "*my_op"]
        wait(lambda: op.get_state() == "running")

        def get_op(path):
            data = get(path)
            del data["progress"]
            return data

        assert get_op("//sys/scheduler/orchid/scheduler/operations/" + op.id) == get_op(
            "//sys/scheduler/orchid/scheduler/operations/\\*my_op"
        )

        assert list_operations()["operations"][0]["brief_spec"]["alias"] == "*my_op"

        # It is not allowed to use alias of already running operation.
        with pytest.raises(YtError):
            vanilla(
                spec={
                    "tasks": {"main": {"command": "sleep 1000", "job_count": 1}},
                    "alias": "*my_op",
                }
            )

        suspend_op("*my_op")
        assert get(op.get_path() + "/@suspended")
        resume_op("*my_op")
        assert not get(op.get_path() + "/@suspended")
        update_op_parameters("*my_op", parameters={"acl": [make_ace("allow", "u", ["manage", "read"])]})
        assert len(get(op.get_path() + "/@alerts")) == 1
        wait(lambda: get(op.get_path() + "/@state") == "running")
        abort_op("*my_op")
        assert get(op.get_path() + "/@state") == "aborted"

        with pytest.raises(YtError):
            complete_op("*my_another_op")

        with pytest.raises(YtError):
            complete_op("my_op")

        # Now using alias *my_op is ok.
        op = vanilla(
            spec={
                "tasks": {"main": {"command": "sleep 1000", "job_count": 1}},
                "alias": "*my_op",
            },
            track=False,
        )

        wait(lambda: get(op.get_path() + "/@state") == "running")

        complete_op("*my_op")
        assert get(op.get_path() + "/@state") == "completed"

    @authors("max42")
    def test_get_operation_latest_archive_version(self):
        init_operation_archive.create_tables_latest_version(
            self.Env.create_native_client(), override_tablet_cell_bundle="default"
        )

        # When no operation is assigned to an alias, get_operation should return an error.
        with pytest.raises(YtError):
            get_operation("*my_op", include_runtime=True)

        op = vanilla(
            spec={
                "tasks": {"main": {"command": "sleep 1000", "job_count": 1}},
                "alias": "*my_op",
            },
            track=False,
        )
        wait(lambda: get(op.get_path() + "/@state") == "running")

        with pytest.raises(YtError):
            # It is impossible to resolve aliases without including runtime.
            get_operation("*my_op", include_runtime=False)

        info = get_operation("*my_op", include_runtime=True)
        assert info["id"] == op.id
        assert info["type"] == "vanilla"

        op.complete()

        # Operation should still be exposed via Orchid, and get_operation will extract information from there.
        assert get(r"//sys/scheduler/orchid/scheduler/operations/\*my_op") == {"operation_id": op.id}
        info = get_operation("*my_op", include_runtime=True)
        assert info["id"] == op.id
        assert info["type"] == "vanilla"

        assert exists(op.get_path())

        clean_operations()

        # Alias should become removed from the Orchid (but it may happen with some visible delay, so we wait for it).
        wait(lambda: not exists("//sys/scheduler/orchid/scheduler/operations/\\*my_op"))
        # But get_operation should still work as expected.
        info = get_operation("*my_op", include_runtime=True)
        assert info["id"] == op.id
        assert info["type"] == "vanilla"


class TestOperationAliasesRpcProxy(TestOperationAliases):
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True
    ENABLE_HTTP_PROXY = True
