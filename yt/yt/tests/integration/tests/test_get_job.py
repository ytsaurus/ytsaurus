from yt_env_setup import YTEnvSetup
from yt_commands import *

import yt.environment.init_operation_archive as init_operation_archive

from yt.common import date_string_to_datetime, uuid_to_parts, parts_to_uuid

import __builtin__
import copy
import datetime

JOB_ARCHIVE_TABLE = "//sys/operations_archive/jobs"
OPERATION_IDS_TABLE = "//sys/operations_archive/operation_ids"


def _delete_job_from_archive(op_id, job_id):
    op_id_hi, op_id_lo = uuid_to_parts(op_id)
    job_id_hi, job_id_lo = uuid_to_parts(job_id)
    delete_rows(
        JOB_ARCHIVE_TABLE,
        [
            {
                "operation_id_hi": op_id_hi,
                "operation_id_lo": op_id_lo,
                "job_id_hi": job_id_hi,
                "job_id_lo": job_id_lo,
            }
        ],
        atomicity="none",
    )


def _update_job_in_archive(op_id, job_id, attributes):
    op_id_hi, op_id_lo = uuid_to_parts(op_id)
    job_id_hi, job_id_lo = uuid_to_parts(job_id)
    attributes.update(
        {
            "operation_id_hi": op_id_hi,
            "operation_id_lo": op_id_lo,
            "job_id_hi": job_id_hi,
            "job_id_lo": job_id_lo,
        }
    )
    insert_rows(JOB_ARCHIVE_TABLE, [attributes], update=True, atomicity="none")


def _get_job_from_archive(op_id, job_id):
    op_id_hi, op_id_lo = uuid_to_parts(op_id)
    job_id_hi, job_id_lo = uuid_to_parts(job_id)
    rows = lookup_rows(
        JOB_ARCHIVE_TABLE,
        [
            {
                "operation_id_hi": op_id_hi,
                "operation_id_lo": op_id_lo,
                "job_id_hi": job_id_hi,
                "job_id_lo": job_id_lo,
            }
        ],
    )
    return rows[0] if rows else None


class _TestGetJobBase(YTEnvSetup):
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
        init_operation_archive.create_tables_latest_version(
            self.Env.create_native_client(),
            override_tablet_cell_bundle="default",
        )

    def _check_get_job(
        self,
        op_id,
        job_id,
        before_start_time,
        state=None,
        has_spec=True,
        is_stale=False,
        archive_state=None,
        controller_agent_state=None,
        pool=None,
        pool_tree=None,
    ):
        """None arguments mean do not check corresponding field in job"""

        job_info = retry(lambda: get_job(op_id, job_id))

        assert job_info["job_id"] == job_id
        assert job_info["operation_id"] == op_id
        assert job_info["type"] == "map"
        if state is not None:
            assert job_info["state"] == state
        if archive_state is not None:
            assert job_info["archive_state"] == archive_state
        if controller_agent_state is not None:
            assert job_info["controller_agent_state"] == controller_agent_state
        if pool is not None:
            assert job_info["pool"] == pool
        if pool_tree is not None:
            assert job_info["pool_tree"] == pool_tree
        start_time = date_string_to_datetime(job_info["start_time"])
        assert before_start_time < start_time < datetime.datetime.utcnow()
        assert job_info.get("is_stale") == is_stale

        attributes = ["job_id", "state", "start_time"]
        job_info = retry(lambda: get_job(op_id, job_id, attributes=attributes))
        assert __builtin__.set(attributes).issubset(__builtin__.set(job_info.keys()))
        attribute_difference = __builtin__.set(job_info.keys()) - __builtin__.set(attributes)
        assert attribute_difference.issubset(__builtin__.set(["archive_state", "controller_agent_state", "is_stale"]))
        assert job_info.get("is_stale") == is_stale

        def check_has_spec():
            job_info = retry(lambda: get_job(op_id, job_id))
            assert job_info.get("has_spec") == has_spec

        if has_spec is not None:
            wait_assert(check_has_spec)


class _TestGetJobCommon(_TestGetJobBase):
    @authors("levysotsky")
    def test_get_job(self):
        create_pool("my_pool")
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"foo": "bar"}, {"foo": "baz"}, {"foo": "qux"}])
        before_start_time = datetime.datetime.utcnow()
        op = map(
            track=False,
            label="get_job",
            in_="//tmp/t1",
            out="//tmp/t2",
            spec={
                "scheduling_options_per_pool_tree": {
                    "default": {"pool": "my_pool"},
                },
                "mapper": {
                    "monitoring": {
                        "enable": True,
                        "sensor_names": ["cpu/user"],
                    },
                },
            },
            command=with_breakpoint(
                """
                echo SOME-STDERR >&2 ;
                cat ;
                if [[ "$YT_JOB_INDEX" == "0" ]]; then
                    BREAKPOINT
                    exit 1
                fi
            """
            ),
        )
        (job_id,) = wait_breakpoint()

        self._check_get_job(op.id, job_id, before_start_time, state="running", has_spec=None,
                            pool="my_pool", pool_tree="default")

        def correct_stderr_size():
            job_info = retry(lambda: get_job(op.id, job_id))
            return job_info.get("stderr_size", 0) == len("SOME-STDERR\n")

        wait(correct_stderr_size)

        release_breakpoint()
        op.track()

        self._check_get_job(op.id, job_id, before_start_time, state="failed", has_spec=True,
                            pool="my_pool", pool_tree="default")

        job_info = retry(lambda: get_job(op.id, job_id))
        assert job_info["fail_context_size"] > 0
        events = job_info["events"]
        assert len(events) > 0
        assert all(field in events[0] for field in ["phase", "state", "time"])

        _delete_job_from_archive(op.id, job_id)

        # Controller agent must be able to respond as it stores
        # zombie operation orchids.
        self._check_get_job(op.id, job_id, before_start_time, state="failed", has_spec=None)

    @authors("levysotsky")
    def test_operation_ids_table(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"foo": "bar"}, {"foo": "baz"}, {"foo": "qux"}])
        op = map(
            track=False,
            in_="//tmp/t1",
            out="//tmp/t2",
            command=with_breakpoint("cat; BREAKPOINT"),
        )
        job_id, = wait_breakpoint()
        release_breakpoint()
        op.track()

        def get_operation_id_from_archive(job_id):
            job_id_hi, job_id_lo = uuid_to_parts(job_id)
            rows = lookup_rows(OPERATION_IDS_TABLE, [{
                "job_id_hi": job_id_hi,
                "job_id_lo": job_id_lo,
            }])
            if not rows:
                return None
            return parts_to_uuid(rows[0]["operation_id_hi"], rows[0]["operation_id_lo"])

        wait(lambda: get_operation_id_from_archive(job_id) is not None)
        assert get_operation_id_from_archive(job_id) == op.id


class TestGetJob(_TestGetJobCommon):
    @authors("gritukan")
    def test_get_job_task_name_attribute_vanilla(self):
        op = vanilla(
            track=False,
            spec={
                "tasks": {
                    "master": {
                        "job_count": 1,
                        "command": with_breakpoint("BREAKPOINT", breakpoint_name="master"),
                    },
                    "slave": {
                        "job_count": 2,
                        "command": with_breakpoint("BREAKPOINT", breakpoint_name="slave"),
                    },
                },
            },
        )

        master_job_ids = wait_breakpoint(breakpoint_name="master", job_count=1)
        slave_job_ids = wait_breakpoint(breakpoint_name="slave", job_count=2)

        def check_task_names():
            for job_id in master_job_ids:
                job_info = retry(lambda: get_job(op.id, job_id))
                assert job_info["task_name"] == "master"
            for job_id in slave_job_ids:
                job_info = retry(lambda: get_job(op.id, job_id))
                assert job_info["task_name"] == "slave"

        check_task_names()

        release_breakpoint(breakpoint_name="master")
        release_breakpoint(breakpoint_name="slave")
        op.track()

        check_task_names()

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
        (job_id,) = wait_breakpoint()

        wait(lambda: _get_job_from_archive(op.id, job_id) is not None)
        job_from_archive = _get_job_from_archive(op.id, job_id)

        abort_job(job_id)
        release_breakpoint()
        op.track()

        # We emulate the situation when aborted (in CA's opinion) job
        # still reports "running" to archive.
        del job_from_archive["job_id_partition_hash"]
        del job_from_archive["operation_id_hash"]
        _update_job_in_archive(op.id, job_id, job_from_archive)

        self._check_get_job(op.id, job_id, before_start_time, archive_state="running", has_spec=None)

        _delete_job_from_archive(op.id, job_id)

        self._check_get_job(
            op.id,
            job_id,
            before_start_time,
            state="aborted",
            controller_agent_state="aborted",
            has_spec=None,
        )
        job_info = retry(lambda: get_job(op.id, job_id))
        assert "archive_state" not in job_info

    @authors("levysotsky")
    def test_not_found(self):
        with raises_yt_error(NoSuchJob):
            get_job("1-2-3-4", "5-6-7-8")


class TestGetJobStatisticsLz4(_TestGetJobCommon):
    DELTA_NODE_CONFIG = copy.deepcopy(_TestGetJobBase.DELTA_NODE_CONFIG)
    DELTA_NODE_CONFIG["exec_agent"]["job_reporter"]["report_statistics_lz4"] = True


class TestGetJobIsStale(_TestGetJobBase):
    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "zombie_operation_orchids": {
                "clean_period": 5 * 1000,
            },
        }
    }

    @authors("levysotsky")
    def test_get_job_is_stale(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"foo": "bar"}, {"foo": "baz"}, {"foo": "qux"}])
        op = map(
            track=False,
            in_="//tmp/t1",
            out="//tmp/t2",
            command=with_breakpoint("echo SOME-STDERR >&2; cat; BREAKPOINT"),
        )
        (job_id,) = wait_breakpoint()

        abort_job(job_id)
        release_breakpoint()
        op.track()

        # We emulate the situation when aborted (in CA's opinion) job
        # still reports "running" to archive.
        _update_job_in_archive(op.id, job_id, {"state": "running", "transient_state": "running"})

        def is_job_removed_from_controller_agent():
            job_info = retry(lambda: get_job(op.id, job_id))
            return job_info.get("controller_agent_state") is None

        wait(is_job_removed_from_controller_agent)

        job_info = retry(lambda: get_job(op.id, job_id))
        assert job_info.get("controller_agent_state") is None
        assert job_info.get("archive_state") == "running"
        assert job_info.get("is_stale")


class TestGetJobMonitoring(_TestGetJobBase):
    USE_PORTO = True

    @authors("levysotsky")
    def test_get_job_monitoring(self):
        op = run_test_vanilla(
            with_breakpoint("BREAKPOINT"),
            job_count=1,
            task_patch={
                "monitoring": {
                    "enable": True,
                    "sensor_names": ["cpu/user"],
                },
            },
        )
        job_id, = wait_breakpoint()

        wait(lambda: "monitoring_descriptor" in get_job(op.id, job_id))
        job = get_job(op.id, job_id)
        assert "monitoring_descriptor" in job
        descriptor = job["monitoring_descriptor"]

        def get_sensors():
            path = "//sys/cluster_nodes/{}/orchid/profiling/user_job/cpu/user"
            return retry(lambda: get(path.format(job["address"])))

        wait(lambda: len(get_sensors()) > 0)
        assert any(sample["tags"].get("job_descriptor") == descriptor for sample in get_sensors())


##################################################################


class TestGetJobRpcProxy(TestGetJob):
    USE_DYNAMIC_TABLES = True
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True
    ENABLE_HTTP_PROXY = True


class TestGetJobStatisticsLz4RpcProxy(TestGetJobStatisticsLz4):
    USE_DYNAMIC_TABLES = True
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True
    ENABLE_HTTP_PROXY = True
