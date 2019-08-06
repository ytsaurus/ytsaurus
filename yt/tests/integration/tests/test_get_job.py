from yt_env_setup import YTEnvSetup
from yt_commands import *

import yt.environment.init_operation_archive as init_operation_archive

from yt.common import date_string_to_datetime

import __builtin__
import datetime

class TestGetJob(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1
    USE_DYNAMIC_TABLES = True

    DELTA_NODE_CONFIG = {
        "exec_agent": {
            "statistics_reporter": {
                "enabled": True,
                "reporting_period": 10,
                "min_repeat_delay": 10,
                "max_repeat_delay": 10,
            }
        },
    }

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "enable_job_reporter": True,
            "enable_job_spec_reporter": True,
            "enable_job_stderr_reporter": True,
        }
    }

    def setup(self):
        sync_create_cells(1)
        init_operation_archive.create_tables_latest_version(self.Env.create_native_client(), override_tablet_cell_bundle="default")
        self._tmpdir = create_tmpdir("jobids")

    def teardown(self):
        remove("//sys/operations_archive")

    @authors("levysotsky")
    def test_get_job(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"foo": "bar"}, {"foo": "baz"}, {"foo": "qux"}])

        before_start_time = datetime.datetime.utcnow()
        op = map(
            dont_track=True,
            label="get_job",
            in_="//tmp/t1",
            out="//tmp/t2",
            command=with_breakpoint("echo SOME-STDERR > 2 ; cat ; BREAKPOINT"),
        )

        job_id, = wait_breakpoint()

        job_info = retry(lambda: get_job(op.id, job_id))

        assert job_info["job_id"] == job_id
        assert job_info["operation_id"] == op.id
        assert job_info["type"] == "map"
        assert job_info["state"] == "running"
        start_time = date_string_to_datetime(job_info["start_time"])
        assert before_start_time < start_time < datetime.datetime.utcnow()

        attributes = ["job_id", "state", "start_time"]
        job_info = retry(lambda: get_job(op.id, job_id, attributes=attributes))
        assert __builtin__.set(job_info.keys()) == __builtin__.set(attributes)

        release_breakpoint()
        op.track()
        def has_spec():
            job_info = retry(lambda: get_job(op.id, job_id))
            assert "has_spec" in job_info and job_info["has_spec"]
        wait_assert(has_spec)

##################################################################

class TestGetJobRpcProxy(TestGetJob):
    USE_DYNAMIC_TABLES = True
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True
    ENABLE_PROXY = True

