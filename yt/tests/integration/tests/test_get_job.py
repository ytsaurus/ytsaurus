from yt_env_setup import YTEnvSetup
from yt_commands import *

import yt.environment.init_operation_archive as init_operation_archive

from yt.common import date_string_to_datetime, uuid_to_parts

from yt.wrapper.common import uuid_hash_pair

import __builtin__
import datetime

OPERATION_JOB_ARCHIVE_TABLE = "//sys/operations_archive/jobs"

class TestGetJob(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1
    USE_DYNAMIC_TABLES = True

    DELTA_NODE_CONFIG = {
        "exec_agent": {
            "job_reporter": {
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

    def _check_get_job(self, op_id, job_id, before_start_time, state, check_has_spec):
        job_info = retry(lambda: get_job(op_id, job_id))

        assert job_info["job_id"] == job_id
        assert job_info["operation_id"] == op_id
        assert job_info["type"] == "map"
        assert job_info["state"] == state
        start_time = date_string_to_datetime(job_info["start_time"])
        assert before_start_time < start_time < datetime.datetime.utcnow()

        attributes = ["job_id", "state", "start_time"]
        job_info = retry(lambda: get_job(op_id, job_id, attributes=attributes))
        assert __builtin__.set(attributes).issubset(__builtin__.set(job_info.keys()))
        attribute_difference = __builtin__.set(job_info.keys()) - __builtin__.set(attributes)
        assert attribute_difference.issubset(__builtin__.set(["archive_state", "controller_agent_state"]))

        if not check_has_spec:
            return

        def has_spec():
            job_info = retry(lambda: get_job(op_id, job_id))
            assert "has_spec" in job_info and job_info["has_spec"]
        wait_assert(has_spec)

    def _delete_job_from_archive(self, op_id, job_id):
        op_id_hi, op_id_lo = uuid_to_parts(op_id)
        job_id_hi, job_id_lo = uuid_to_parts(job_id)
        delete_rows(
            OPERATION_JOB_ARCHIVE_TABLE,
            [{
                "operation_id_hi": op_id_hi,
                "operation_id_lo": op_id_lo,
                "job_id_hi": job_id_hi,
                "job_id_lo": job_id_lo,
            }],
            atomicity="none",
        )

    @authors("levysotsky")
    def test_get_job(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"foo": "bar"}, {"foo": "baz"}, {"foo": "qux"}])
        before_start_time = datetime.datetime.utcnow()
        op = map(
            track=False,
            label="get_job",
            in_="//tmp/t1",
            out="//tmp/t2",
            command=with_breakpoint("""
                echo SOME-STDERR >&2 ;
                cat ;
                if [[ "$YT_JOB_INDEX" == "0" ]]; then
                    BREAKPOINT
                    exit 1
                fi
            """),
        )
        job_id, = wait_breakpoint()

        self._check_get_job(op.id, job_id, before_start_time, state="running", check_has_spec=False)

        release_breakpoint()
        op.track()

        self._check_get_job(op.id, job_id, before_start_time, state="failed", check_has_spec=True)

        self._delete_job_from_archive(op.id, job_id)

        # Controller agent must be able to respond as it stores
        # zombie operation orchids.
        self._check_get_job(op.id, job_id, before_start_time, state="failed", check_has_spec=False)

    @authors("levysotsky")
    def test_get_stubborn_job(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"foo": "bar"}, {"foo": "baz"}, {"foo": "qux"}])
        before_start_time = datetime.datetime.utcnow()
        op = map(
            track=False,
            in_="//tmp/t1",
            out="//tmp/t2",
            command=with_breakpoint("echo SOME-STDERR >&2; cat; BREAKPOINT"),
        )
        job_id, = wait_breakpoint()

        operation_hash = uuid_hash_pair(op.id)
        job_hash = uuid_hash_pair(job_id)
        archive_path = init_operation_archive.DEFAULT_ARCHIVE_PATH + "/jobs"
        def get_job_from_archive(job_id):
            rows = lookup_rows(archive_path, [{
                "operation_id_hi": operation_hash.hi,
                "operation_id_lo": operation_hash.lo,
                "job_id_hi": job_hash.hi,
                "job_id_lo": job_hash.lo,
            }])
            return rows[0] if rows else None
        wait(lambda: get_job_from_archive(job_id) is not None)
        job_from_archive = get_job_from_archive(job_id)

        abort_job(job_id)
        release_breakpoint()
        op.track()

        # We emulate the situation when aborted (in CA's opinion) job
        # still reports "running" to archive.
        del job_from_archive["operation_id_hash"]
        insert_rows(
            init_operation_archive.DEFAULT_ARCHIVE_PATH + "/jobs",
            [job_from_archive],
            update=True,
            atomicity="none",
        )

        self._check_get_job(op.id, job_id, before_start_time, state="running", check_has_spec=False)
        job_info = retry(lambda: get_job(op.id, job_id))
        assert job_info["archive_state"] == job_info["state"] == "running"

        self._delete_job_from_archive(op.id, job_id)
        self._check_get_job(op.id, job_id, before_start_time, state="aborted", check_has_spec=False)
        job_info = retry(lambda: get_job(op.id, job_id))
        assert job_info["controller_agent_state"] == job_info["state"] == "aborted"

    @authors("levysotsky")
    def test_not_found(self):
        with raises_yt_error(NoSuchJob):
            get_job("1-2-3-4", "5-6-7-8")

##################################################################

class TestGetJobRpcProxy(TestGetJob):
    USE_DYNAMIC_TABLES = True
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True
    ENABLE_HTTP_PROXY = True

