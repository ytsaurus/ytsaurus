from yt_env_setup import YTEnvSetup, Restarter, SCHEDULERS_SERVICE

from yt_commands import (
    authors, wait, create, create_table, get, set, remove, exists,
    lookup_rows, write_table,
    map, list_operations, get_operation_cypress_path, sync_create_cells, clean_operations)

import yt.environment.init_operation_archive as init_operation_archive

from yt.common import uuid_to_parts, YT_DATETIME_FORMAT_STRING, YtError, YtResponseError

import pytest

from datetime import datetime, timedelta
import builtins

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
    for i in range(count):
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
                "max_operation_age": 5000,
            },
            "static_orchid_cache_update_period": 100,
            "alerts_update_period": 100,
        }
    }

    def setup_method(self, method):
        super(TestSchedulerOperationsCleaner, self).setup_method(method)
        sync_create_cells(1)

    def teardown_method(self, method):
        remove("//sys/operations_archive", force=True)
        super(TestSchedulerOperationsCleaner, self).teardown_method(method)

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
        assert builtins.set(self._get_removed_operations(ops)) == builtins.set(ops[:7])

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
        wait(lambda: not get(CLEANER_ORCHID + "/enable_operation_archivation"))

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

    @authors("omgronny")
    def test_get_original_path(self):
        init_operation_archive.create_tables_latest_version(
            self.Env.create_native_client(), override_tablet_cell_bundle="default"
        )

        input_table_name = "//tmp/input"
        input_table_id = create_table(input_table_name)
        write_table(input_table_name, [{"foo": "bar"}, {"foo": "baz"}, {"foo": "qux"}])
        output_table_name = "//tmp/output"
        output_table_id = create_table(output_table_name)
        op = map(
            in_="<original_path=\"{}\">#{}".format(input_table_name, input_table_id),
            out="<original_path=\"{}\">#{}".format(output_table_name, output_table_id),
            command="cat",
        )

        clean_operations()

        row = self._lookup_ordered_by_id_row(op.id)
        assert row["state"] == "completed"
        assert op.id in row["filter_factors"]
        assert input_table_name in row["filter_factors"]
        assert output_table_name in row["filter_factors"]
