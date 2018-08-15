from yt_env_setup import YTEnvSetup
from yt_commands import *

import yt.environment.init_operation_archive as init_operation_archive
from yt.test_helpers import wait

from yt.operations_archive.clear_operations import id_to_parts_new

import __builtin__

##################################################################

CLEANER_ORCHID = "//sys/scheduler/orchid/scheduler/operations_cleaner"

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
                "max_removal_sleep_delay": 100
            },
            "static_orchid_cache_update_period": 100,
            "alerts_update_period": 100
        }
    }

    def setup(self):
        sync_create_cells(1)

    def teardown(self):
        remove("//sys/operations_archive", force=True)
        remove("//sys/scheduler/config", force=True)

        # Drain archive queue
        self.Env.kill_schedulers()
        remove("//sys/operations/*")
        self.Env.start_schedulers()

    def _lookup_ordered_by_id_row(self, op_id):
        id_hi, id_lo = id_to_parts_new(op_id)
        rows = lookup_rows("//sys/operations_archive/ordered_by_id", [{"id_hi": id_hi, "id_lo": id_lo}])
        assert len(rows) == 1
        return rows[0]

    def _operation_exist(self, op_id):
        return exists("//sys/operations/" + op_id) and exists(get_operation_cypress_path(op_id))

    def _get_removed_operations(self, ops):
        removed = []
        for op in ops:
            if not self._operation_exist(op):
                removed.append(op)
        return removed

    def test_basic_sanity(self):
        init_operation_archive.create_tables_latest_version(self.Env.create_native_client())

        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"foo": "bar"}, {"foo": "baz"}, {"foo": "qux"}])

        ops = []
        op_count = 7
        for _ in xrange(op_count):
            op = map(
                in_="//tmp/t1",
                out="//tmp/t2",
                command="cat")
            ops.append(op.id)

        wait(lambda: len(self._get_removed_operations(ops)) == 4)
        archived_operations = self._get_removed_operations(ops)
        for op in archived_operations:
            row = self._lookup_ordered_by_id_row(op)
            assert row["state"] == "completed"
            assert op in row["filter_factors"]
            assert "//tmp/t1" in row["filter_factors"]
            assert "//tmp/t2" in row["filter_factors"]
            assert row["progress"]["jobs"]["failed"] == 0
            assert row["authenticated_user"] == "root"
            assert "finish_time" in row
            assert "start_time" in row
            assert "alerts" in row
            assert row["runtime_parameters"]["scheduling_options_per_pool_tree"]["default"]["pool"] == "root"

    def test_operations_archive_is_not_initialized(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"foo": "bar"}, {"foo": "baz"}, {"foo": "qux"}])

        ops = []
        op_count = 7
        for _ in xrange(op_count):
            op = map(
                in_="//tmp/t1",
                out="//tmp/t2",
                command="cat")
            ops.append(op.id)

        wait(lambda: get(CLEANER_ORCHID + "/archive_pending") == 4)

        for _ in xrange(3):
            map(
                in_="//tmp/t1",
                out="//tmp/t2",
                command="cat")

        # Earliest operations should be removed
        wait(lambda: len(self._get_removed_operations(ops)) == 7)
        assert __builtin__.set(self._get_removed_operations(ops)) == __builtin__.set(ops[:7])

        def scheduler_alert_set():
            for alert in get("//sys/scheduler/@alerts"):
                if "archivation" in alert["message"]:
                    return True
            return False

        wait(scheduler_alert_set)

    def test_start_stop(self):
        init_operation_archive.create_tables_latest_version(self.Env.create_native_client())

        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"foo": "bar"}, {"foo": "baz"}, {"foo": "qux"}])

        set("//sys/scheduler/config", {"operations_cleaner": {"enable": False}})
        wait(lambda: not get(CLEANER_ORCHID + "/enable"))
        wait(lambda: not get(CLEANER_ORCHID + "/enable_archivation"))

        ops = []
        op_count = 7
        for _ in xrange(op_count):
            op = map(
                in_="//tmp/t1",
                out="//tmp/t2",
                command="cat")
            ops.append(op.id)

        assert get(CLEANER_ORCHID + "/archive_pending") == 0
        set("//sys/scheduler/config/operations_cleaner/enable", True)

        wait(lambda: len(self._get_removed_operations(ops)) == 4)

    def test_revive(self):
        init_operation_archive.create_tables_latest_version(self.Env.create_native_client())

        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"foo": "bar"}, {"foo": "baz"}, {"foo": "qux"}])

        ops = []
        op_count = 7
        for _ in xrange(op_count):
            op = map(
                in_="//tmp/t1",
                out="//tmp/t2",
                command="cat")
            ops.append(op.id)

        wait(lambda: get(CLEANER_ORCHID + "/submitted_count") == 3)

        self.Env.kill_schedulers()
        self.Env.start_schedulers()

        wait(lambda: get(CLEANER_ORCHID + "/submitted_count") == 3)

    def test_max_operation_count_per_user(self):
        init_operation_archive.create_tables_latest_version(self.Env.create_native_client())

        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"foo": "bar"}, {"foo": "baz"}, {"foo": "qux"}])

        ops = []
        op_count = 5
        for _ in xrange(op_count):
            try:
                op = map(
                    in_="//tmp/t1",
                    out="//tmp/t2",
                    command="false",
                    spec={"max_failed_job_count": 1},
                    dont_track=True)
                ops.append(op.id)
                op.track()
                assert False, "Operation expected to fail"
            except YtError:
                pass

        wait(lambda: len(self._get_removed_operations(ops)) == 2)
