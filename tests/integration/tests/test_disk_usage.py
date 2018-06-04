from yt_env_setup import require_ytserver_root_privileges, wait
from yt_commands import *

from quota_mixin import QuotaMixin


@require_ytserver_root_privileges
class TestDiskUsage(QuotaMixin):
    NUM_SCHEDULERS = 1
    NUM_MASTERS = 1
    NUM_NODES = 1
    DELTA_NODE_CONFIG = {
        "exec_agent": {
            "slot_manager": {
                "locations": [
                    {
                        "disk_quota": 1024 * 1024,
                        "disk_usage_watermark": 0
                    }
                ],
                "disk_info_update_period": 100,
            },
            "job_controller": {
                "waiting_jobs_timeout": 1000,
                "resource_limits": {
                    "user_slots": 3,
                    "cpu": 3.0
                }
            },
            "min_required_disk_space": 0,
        }
    }

    DELTA_MASTER_CONFIG = {
        "cypress_manager": {
            "default_table_replication_factor": 1
        }
    }

    def _init_tables(self):
        tables = ["//tmp/t1", "//tmp/t2", "//tmp/t3"]
        for table in tables:
            create("table", table)
        write_table(tables[0], [{"foo": "bar"} for _ in xrange(10)])
        return tables

    def run_test(self, tables, fatty_options):
        options = {
            "in_": tables[0],
            "out": tables[1],
            "dont_track": True,
        }

        options.update(fatty_options)

        first = map(**options)

        events_on_fs().wait_event("file_written")

        check_op = {
            "in_": tables[0],
            "out": tables[2],
            "command": "true",
            "spec": {"mapper": {"disk_space_limit": 1024 * 1024 / 2}, "max_failed_job_count": 1}
        }

        op = map(dont_track=True, **check_op)
        wait(lambda: exists(op.get_path() + "/controller_orchid/progress/jobs"))
        get_aborted = lambda suffix = "": get(op.get_path() + "/controller_orchid/progress/jobs/aborted" + suffix, verbose=False)
        wait(lambda: get_aborted("/total"))
        assert get_aborted("/scheduled/other") > 0
        op.abort()

        events_on_fs().notify_event("finish_job")
        first.track()

        map(**check_op)

    def test_lack_space_node(self):
        tables = self._init_tables()
        options = {
            "command": " ; ".join([
                "dd if=/dev/zero of=zeros.txt count=1500",
                events_on_fs().notify_event_cmd("file_written"),
                events_on_fs().wait_event_cmd("finish_job"),
            ])
        }

        self.run_test(tables, options)

    def test_lack_space_node_with_quota(self):
        tables = self._init_tables()
        options = {
            "command": " ; ".join([
                "true",
                events_on_fs().notify_event_cmd("file_written"),
                events_on_fs().wait_event_cmd("finish_job"),
            ]),
            "spec": {"mapper": {"disk_space_limit": 1024 * 1024 * 2 / 3}, "max_failed_job_count": 1}
        }

        self.run_test(tables, options)
