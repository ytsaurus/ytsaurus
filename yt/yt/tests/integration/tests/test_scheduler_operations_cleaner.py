from yt_env_setup import YTEnvSetup, Restarter, SCHEDULERS_SERVICE

from yt_commands import (  # noqa
    authors, print_debug, wait, wait_assert, wait_breakpoint, release_breakpoint, with_breakpoint,
    events_on_fs, reset_events_on_fs,
    create, ls, get, set, copy, move, remove, link, exists,
    create_account, create_network_project, create_tmpdir, create_user, create_group,
    create_pool, create_pool_tree,
    create_data_center, create_rack,
    make_ace, check_permission, add_member,
    make_batch_request, execute_batch, get_batch_error,
    start_transaction, abort_transaction, commit_transaction, lock,
    insert_rows, select_rows, lookup_rows, delete_rows, trim_rows, alter_table,
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
    sync_freeze_table, sync_unfreeze_table,
    get_first_chunk_id, get_singular_chunk_id, get_chunk_replication_factor, multicell_sleep,
    update_nodes_dynamic_config, update_controller_agent_config,
    update_op_parameters, enable_op_detailed_logs,
    set_node_banned, set_banned_flag, set_account_disk_space_limit,
    check_all_stderrs,
    create_test_tables, create_dynamic_table, PrepareTables,
    get_statistics,
    make_random_string, raises_yt_error,
    get_driver)

import yt.environment.init_operation_archive as init_operation_archive

from yt.common import uuid_to_parts, YT_DATETIME_FORMAT_STRING, YtError, YtResponseError

import pytest

from datetime import datetime, timedelta
import __builtin__

##################################################################

CLEANER_ORCHID = "//sys/scheduler/orchid/scheduler/operations_cleaner"


def _try_track(op, expect_fail=False):
    if expect_fail:
        with pytest.raises(YtError):
            op.track()
    else:
        try:
            op.track()
        except YtResponseError as e:
            if not e.is_resolve_error():
                raise


def _run_maps_parallel(count, command, expect_fail=False, max_failed_job_count=1):
    create("table", "//tmp/input", ignore_existing=True)
    write_table("//tmp/input", [{"foo": "bar"}, {"foo": "baz"}, {"foo": "qux"}])
    ops = []
    for i in xrange(count):
        create("table", "//tmp/output_{}".format(i), ignore_existing=True)
        op = map(
            in_="//tmp/input",
            out="//tmp/output_{}".format(i),
            track=False,
            spec={"max_failed_job_count": max_failed_job_count},
            command=command,
        )
        ops.append(op)
    for op in ops:
        _try_track(op, expect_fail=expect_fail)
    return [op.id for op in ops]


class TestSchedulerOperationsCleaner(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "operations_cleaner": {
                "enable": True,
                # Analyze all operations each 100ms
                "analysis_period": 100,
                # Wait each batch to remove not more than 100ms
                "remove_batch_timeout": 100,
                # Subbatch to test.
                "remove_subbatch_size": 2,
                # Wait each batch to archive not more than 100ms
                "archive_batch_timeout": 100,
                # Retry sleeps
                "min_archivation_retry_sleep_delay": 100,
                "max_archivation_retry_sleep_delay": 110,
                # Leave no more than 5 completed operations
                "soft_retained_operation_count": 3,
                # Operations older than 50ms can be considered for removal
                "clean_delay": 50,
                # If more than this count of operations are enqueued and archivation
                # can't succeed then operations will be just removed.
                "max_operation_count_enqueued_for_archival": 5,
                "max_operation_count_per_user": 3,
                "fetch_batch_size": 1,
                "max_removal_sleep_delay": 100,
            },
            "static_orchid_cache_update_period": 100,
            "alerts_update_period": 100,
        }
    }

    def setup(self):
        sync_create_cells(1)

    def teardown(self):
        remove("//sys/operations_archive", force=True)

        # Drain archive queue
        with Restarter(self.Env, SCHEDULERS_SERVICE):
            remove("//sys/operations/*")

    def _lookup_ordered_by_id_row(self, op_id):
        id_hi, id_lo = uuid_to_parts(op_id)
        rows = lookup_rows("//sys/operations_archive/ordered_by_id", [{"id_hi": id_hi, "id_lo": id_lo}])
        assert len(rows) == 1
        return rows[0]

    def _operation_exist(self, op_id):
        return exists(get_operation_cypress_path(op_id))

    def _get_removed_operations(self, ops):
        removed = []
        for op in ops:
            if not self._operation_exist(op):
                removed.append(op)
        return removed

    @authors("asaitgalin")
    def test_basic_sanity(self):
        init_operation_archive.create_tables_latest_version(
            self.Env.create_native_client(), override_tablet_cell_bundle="default"
        )

        ops = _run_maps_parallel(7, "cat")

        wait(lambda: len(self._get_removed_operations(ops)) == 4)
        archived_operations = self._get_removed_operations(ops)
        for op in archived_operations:
            row = self._lookup_ordered_by_id_row(op)
            assert row["state"] == "completed"
            assert op in row["filter_factors"]
            assert "//tmp/input" in row["filter_factors"]
            assert "//tmp/output" in row["filter_factors"]
            assert row["progress"]["jobs"]["failed"] == 0
            assert row["authenticated_user"] == "root"
            assert "finish_time" in row
            assert "start_time" in row
            assert "alerts" in row
            assert row["runtime_parameters"]["scheduling_options_per_pool_tree"]["default"]["pool"] == "root"

    @authors("asaitgalin")
    def test_operations_archive_is_not_initialized(self):
        ops = _run_maps_parallel(7, "cat")

        wait(lambda: get(CLEANER_ORCHID + "/archive_pending") == 4)

        _run_maps_parallel(3, "cat")

        # Earliest operations should be removed
        wait(lambda: len(self._get_removed_operations(ops)) == 7)
        assert __builtin__.set(self._get_removed_operations(ops)) == __builtin__.set(ops[:7])

        def scheduler_alert_set():
            for alert in get("//sys/scheduler/@alerts"):
                if "archivation" in alert["message"]:
                    return True
            return False

        wait(scheduler_alert_set)

    def _test_start_stop_impl(self, command, lookup_timeout=None, max_failed_job_count=1):
        init_operation_archive.create_tables_latest_version(
            self.Env.create_native_client(), override_tablet_cell_bundle="default"
        )

        config = {"enable": False}
        if lookup_timeout is not None:
            config["finished_operations_archive_lookup_timeout"] = int(lookup_timeout.total_seconds() * 1000)
        set("//sys/scheduler/config", {"operations_cleaner": config})
        wait(lambda: not get(CLEANER_ORCHID + "/enable"))
        wait(lambda: not get(CLEANER_ORCHID + "/enable_archivation"))

        ops = _run_maps_parallel(7, command, max_failed_job_count=max_failed_job_count)

        assert get(CLEANER_ORCHID + "/archive_pending") == 0
        set("//sys/scheduler/config/operations_cleaner/enable", True)

        wait(lambda: len(self._get_removed_operations(ops)) == 4)
        return ops

    @authors("asaitgalin")
    def test_start_stop(self):
        self._test_start_stop_impl("cat")

    @authors("asaitgalin")
    def test_revive(self):
        init_operation_archive.create_tables_latest_version(
            self.Env.create_native_client(), override_tablet_cell_bundle="default"
        )

        _run_maps_parallel(7, "cat")

        wait(lambda: get(CLEANER_ORCHID + "/submitted") == 3)

        with Restarter(self.Env, SCHEDULERS_SERVICE):
            pass

        wait(lambda: get(CLEANER_ORCHID + "/submitted") == 3)

    @authors("asaitgalin")
    def test_max_operation_count_per_user(self):
        init_operation_archive.create_tables_latest_version(
            self.Env.create_native_client(), override_tablet_cell_bundle="default"
        )
        ops = _run_maps_parallel(5, "false", expect_fail=True)
        wait(lambda: len(self._get_removed_operations(ops)) == 2)

    @staticmethod
    def list_op_format(t):
        return t.strftime(YT_DATETIME_FORMAT_STRING)

    @authors("levysotsky")
    def test_archive_lookup(self):
        before_start_time = datetime.utcnow()
        ops = self._test_start_stop_impl(
            'if [ "$YT_JOB_INDEX" -eq 0 ]; then exit 1; fi; cat',
            max_failed_job_count=2,
        )

        # We expect to get all the operations by "with_failed_jobs" filter
        # as brief_progress is reported only to archive and must be fetched
        # by cleaner on startup.
        res = list_operations(
            include_archive=True,
            from_time=self.list_op_format(before_start_time),
            to_time=self.list_op_format(datetime.utcnow()),
            with_failed_jobs=True,
        )
        assert res["failed_jobs_count"] == len(ops)
        assert list(reversed(ops)) == [op["id"] for op in res["operations"]]

    @authors("levysotsky")
    def test_archive_lookup_failure(self):
        before_start_time = datetime.utcnow()
        self._test_start_stop_impl(
            'if [ "$YT_JOB_INDEX" -eq 0 ]; then exit 1; fi; cat',
            lookup_timeout=timedelta(milliseconds=1),
            max_failed_job_count=2,
        )

        # We expect to get only not removed operations by "with_failed_jobs" filter
        # as brief_progress is reported only to archive and will not be
        # fetched due to low timeout.
        res = list_operations(
            include_archive=True,
            from_time=self.list_op_format(before_start_time),
            to_time=self.list_op_format(datetime.utcnow()),
            with_failed_jobs=True,
        )
        assert res["failed_jobs_count"] == 3
        assert len(res["operations"]) == 3
