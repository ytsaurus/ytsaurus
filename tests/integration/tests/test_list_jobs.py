from yt_env_setup import wait, YTEnvSetup
from yt_commands import *
import yt.environment.init_operation_archive as init_operation_archive
from yt.wrapper.operation_commands import add_failed_operation_stderrs_to_error_message
from yt.wrapper.common import uuid_hash_pair
from yt.common import date_string_to_datetime
from yt.test_helpers import WaitFailed

from test_rpc_proxy import create_input_table

from collections import defaultdict
from datetime import datetime
import sys
import time
import traceback
import __builtin__

import pytest

def get_stderr_from_table(operation_id, job_id):
    operation_hash = uuid_hash_pair(operation_id)
    job_hash = uuid_hash_pair(job_id)
    rows = list(select_rows("stderr from [//sys/operations_archive/stderrs] where operation_id_lo={0}u and operation_id_hi={1}u and job_id_lo={2}u and job_id_hi={3}u"\
        .format(operation_hash.lo, operation_hash.hi, job_hash.lo, job_hash.hi)))
    assert len(rows) == 1
    return remove_asan_warning(rows[0]["stderr"])


def get_profile_from_table(operation_id, job_id):
    operation_hash = uuid_hash_pair(operation_id)
    job_hash = uuid_hash_pair(job_id)
    rows = list(select_rows("profile_type, profile_blob from [//sys/operations_archive/job_profiles] where operation_id_lo={0}u and operation_id_hi={1}u and job_id_lo={2}u and job_id_hi={3}u"\
        .format(operation_hash.lo, operation_hash.hi, job_hash.lo, job_hash.hi)))
    assert len(rows) == 1
    return rows[0]["profile_type"], rows[0]["profile_blob"]


def checked_list_jobs(*args, **kwargs):
    res = list_jobs(*args, **kwargs)
    if res["errors"]:
        raise YtError(message="list_jobs failed", inner_erros=res["errors"])
    return res


class TestListJobs(YTEnvSetup):
    SINGLE_SETUP_TEARDOWN = True

    DELTA_NODE_CONFIG = {
        "exec_agent": {
            "statistics_reporter": {
                "enabled": True,
                "reporting_period": 10,
                "min_repeat_delay": 10,
                "max_repeat_delay": 10,
            },
        },
        "scheduler_connector": {
            "heartbeat_period": 100,  # msec
        },
        "job_proxy_heartbeat_period": 100,  # msec
    }

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "enable_job_reporter": True,
            "enable_job_spec_reporter": True,
            "enable_job_stderr_reporter": True,
            "enable_job_fail_context_reporter": True,
            "static_orchid_cache_update_period": 100,
            "watchers_update_period": 100,
            "operations_update_period": 10,
            "operations_cleaner": {
                "enable": False,
                "analysis_period": 100,
                # Cleanup all operations
                "hard_retained_operation_count": 0,
                "clean_delay": 0,
            },
        },
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "controller_static_orchid_update_period": 100,
        },
    }

    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1
    USE_DYNAMIC_TABLES = True

    @classmethod
    def setup_class(cls):
        super(TestListJobs, cls).setup_class()
        sync_create_cells(1)
        init_operation_archive.create_tables_latest_version(cls.Env.create_native_client(), override_tablet_cell_bundle="default")
        cls._tmpdir = create_tmpdir("list_jobs")

    def _create_tables(self):
        input_table = "//tmp/input_" + make_random_string()
        output_table = "//tmp/output_" + make_random_string()
        create_input_table(
            input_table,
            [{"foo": "bar"}, {"foo": "baz"}, {"foo": "qux"}],
            [{"name": "foo", "type": "string"}],
            driver_backend=self.DRIVER_BACKEND,
        )
        create("table", output_table)
        return input_table, output_table

    @staticmethod
    def _validate_address_filter(op, data_source):
        address_to_job_ids = defaultdict(list)
        jobs = checked_list_jobs(op.id, data_source=data_source)["jobs"]
        for job in jobs:
            address = job["address"]
            address_to_job_ids[address].append(job["id"])
        for address, job_ids in address_to_job_ids.iteritems():
            actual_jobs_for_address = checked_list_jobs(op.id, data_source=data_source, address=address)["jobs"]
            assert __builtin__.set(job["id"] for job in actual_jobs_for_address) == __builtin__.set(job_ids)

    @staticmethod
    def _get_answers_for_filters_during_map(job_ids):
        return {
            ("job_state", "failed"): job_ids["failed_map"],
            ("job_state", "aborted"): job_ids["aborted_map"],
            ("job_state", "running"): job_ids["completed_map"],
            ("job_state", "completed"): [],

            ("job_type", "partition_map"): job_ids["map"],
            ("job_type", "partition_reduce"): [],

            ("with_stderr", True): job_ids["failed_map"] + job_ids["completed_map"],
            ("with_stderr", False): job_ids["aborted_map"],

            ("with_fail_context", True): job_ids["failed_map"],
            ("with_fail_context", False): job_ids["aborted_map"] + job_ids["completed_map"],
        }

    @staticmethod
    def _get_answers_for_filters_during_reduce(job_ids):
        return {
            ("job_state", "failed"): job_ids["failed_map"],
            ("job_state", "aborted"): job_ids["aborted_map"],
            ("job_state", "completed"): job_ids["completed_map"],
            ("job_state", "running"): job_ids["reduce"],

            ("job_type", "partition_map"): job_ids["map"],
            ("job_type", "partition_reduce"): job_ids["reduce"],

            ("with_stderr", True): job_ids["failed_map"] + job_ids["completed_map"] + job_ids["reduce"],
            ("with_stderr", False): job_ids["aborted_map"],

            ("with_fail_context", True): job_ids["failed_map"],
            ("with_fail_context", False): job_ids["aborted_map"] + job_ids["completed_map"] + job_ids["reduce"],
        }

    @staticmethod
    def _get_answers_for_filters_after_finish(job_ids):
        return {
            ("job_state", "failed"): job_ids["failed_map"],
            ("job_state", "aborted"): job_ids["aborted_map"],
            ("job_state", "completed"): job_ids["completed_map"] + job_ids["reduce"],
            ("job_state", "running"): [],

            ("job_type", "partition_map"): job_ids["map"],
            ("job_type", "partition_reduce"): job_ids["reduce"],

            ("with_stderr", True): job_ids["failed_map"] + job_ids["completed_map"] + job_ids["reduce"],
            ("with_stderr", False): job_ids["aborted_map"],

            ("with_fail_context", True): job_ids["failed_map"],
            ("with_fail_context", False): job_ids["aborted_map"] + job_ids["completed_map"] + job_ids["reduce"],
        }

    @staticmethod
    def _validate_filters(op, answers, data_source):
        for (name, value), correct_job_ids in answers.iteritems():
            jobs = checked_list_jobs(op.id, data_source=data_source, **{name: value})["jobs"]
            assert \
                __builtin__.set(job["id"] for job in jobs) == __builtin__.set(correct_job_ids), \
                "Assertion for filter {}={} failed".format(name, repr(value))
        TestListJobs._validate_address_filter(op, data_source=data_source)

    @staticmethod
    def _get_answers_for_sorting_during_map(job_ids):
        return {
            "type": [job_ids["map"]],
            "state": [job_ids["aborted_map"], job_ids["failed_map"], job_ids["completed_map"]],
            "start_time": [job_ids["map"]],
            "finish_time": [job_ids["completed_map"], job_ids["failed_map"], job_ids["aborted_map"]],
            "duration": [job_ids["failed_map"] + job_ids["aborted_map"], job_ids["completed_map"]],
            "id": [[job_id] for job_id in sorted(job_ids["map"])],
        }

    @staticmethod
    def _get_answers_for_sorting_during_reduce(job_ids):
        return {
            "type": [job_ids["map"], job_ids["reduce"]],
            "state": [job_ids["aborted_map"], job_ids["completed_map"], job_ids["failed_map"], job_ids["reduce"]],
            "start_time": [job_ids["map"], job_ids["reduce"]],
            "finish_time": [job_ids["reduce"], job_ids["failed_map"], job_ids["aborted_map"], job_ids["completed_map"]],
            "duration": [job_ids["map"] + job_ids["reduce"]],
            "id": [[job_id] for job_id in sorted(job_ids["map"] + job_ids["reduce"])],
        }

    @staticmethod
    def _get_answers_for_sorting_after_finish(job_ids):
        return {
            "type": [job_ids["map"], job_ids["reduce"]],
            "state": [job_ids["aborted_map"], job_ids["completed_map"] + job_ids["reduce"], job_ids["failed_map"]],
            "start_time": [job_ids["map"], job_ids["reduce"]],
            "finish_time": [job_ids["failed_map"], job_ids["aborted_map"], job_ids["completed_map"], job_ids["reduce"]],
            "duration": [job_ids["map"] + job_ids["reduce"]],
            "id": [[job_id] for job_id in sorted(job_ids["map"] + job_ids["reduce"])],
        }

    @staticmethod
    def _get_sorting_key(field_name):
        if field_name in ["type", "state", "id"]:
            return lambda job: job[field_name]
        elif field_name in ["start_time", "finish_time"]:
            def key(job):
                time_str = job.get(field_name)
                if time_str is not None:
                    return date_string_to_datetime(time_str)
                else:
                    return datetime.min
            return key
        elif field_name == "finish_time":
            return lambda job: date_string_to_datetime(job[field_name])
        elif field_name == "duration":
            def key(job):
                finish_time_str = job.get("finish_time")
                finish_time = date_string_to_datetime(finish_time_str) if finish_time_str is not None else datetime.now()
                return finish_time - date_string_to_datetime(job["start_time"])
            return key
        else:
            raise Exception("Unknown sorting key {}".format(field_name))

    @staticmethod
    def _validate_sorting(op, correct_answers, data_source):
        for field_name, correct_job_ids in correct_answers.iteritems():
            for sort_order in ["ascending", "descending"]:
                jobs = checked_list_jobs(
                    op.id,
                    data_source=data_source,
                    sort_field=field_name,
                    sort_order=sort_order,
                )["jobs"]

                reverse = (sort_order == "descending")
                assert \
                    sorted(jobs, key=TestListJobs._get_sorting_key(field_name), reverse=reverse) == jobs, \
                    "Assertion for sort_field={} and sort_order={} failed".format(field_name, sort_order)

                if reverse:
                    correct_job_ids = reversed(correct_job_ids)

                group_start_idx = 0
                for correct_group in correct_job_ids:
                    group = jobs[group_start_idx : group_start_idx + len(correct_group)]
                    assert \
                        __builtin__.set(job["id"] for job in group) == __builtin__.set(correct_group), \
                        "Assertion for sort_field={} and sort_order={} failed".format(field_name, sort_order)
                    group_start_idx += len(correct_group)

    @staticmethod
    def _check_during_map(op, job_ids, data_source):
        res = checked_list_jobs(op.id, data_source=data_source)
        assert __builtin__.set(job["id"] for job in res["jobs"]) == __builtin__.set(job_ids["map"])
        assert res["type_counts"] == {"partition_map": 5}
        assert res["state_counts"] == {"running": 3, "failed": 1, "aborted": 1}

        assert res["controller_agent_job_count"] == 3
        assert res["scheduler_job_count"] == 3
        if data_source == "runtime":
            assert res["cypress_job_count"] == 2
            assert res["archive_job_count"] == yson.YsonEntity()
        else:
            assert data_source == "archive"
            assert res["cypress_job_count"] == yson.YsonEntity()
            assert res["archive_job_count"] == 5

        answers_for_filters = TestListJobs._get_answers_for_filters_during_map(job_ids)
        TestListJobs._validate_filters(op, answers_for_filters, data_source=data_source)

        answers_for_sorting = TestListJobs._get_answers_for_sorting_during_map(job_ids)
        TestListJobs._validate_sorting(op, answers_for_sorting, data_source=data_source)

    @staticmethod
    def _check_during_reduce(op, job_ids, data_source):
        res = checked_list_jobs(op.id, data_source=data_source)
        assert __builtin__.set(job["id"] for job in res["jobs"]) == __builtin__.set(job_ids["reduce"] + job_ids["map"])
        assert res["type_counts"] == {"partition_reduce": 1, "partition_map": 5}
        assert res["state_counts"] == {"running": 1, "completed": 3, "failed": 1, "aborted": 1}

        assert res["controller_agent_job_count"] == 1
        assert res["scheduler_job_count"] == 1
        if data_source == "runtime":
            assert res["cypress_job_count"] == 5
            assert res["archive_job_count"] == yson.YsonEntity()
        else:
            assert data_source == "archive"
            assert res["cypress_job_count"] == yson.YsonEntity()
            assert res["archive_job_count"] == 6

        answers_for_filters = TestListJobs._get_answers_for_filters_during_reduce(job_ids)
        TestListJobs._validate_filters(op, answers_for_filters, data_source=data_source)

        answers_for_sorting = TestListJobs._get_answers_for_sorting_during_reduce(job_ids)
        TestListJobs._validate_sorting(op, answers_for_sorting, data_source=data_source)

    @staticmethod
    def _check_after_finish(op, job_ids, data_source):
        res = checked_list_jobs(op.id, data_source=data_source)
        assert __builtin__.set(job["id"] for job in res["jobs"]) == __builtin__.set(job_ids["reduce"] + job_ids["map"])
        assert res["type_counts"] == {"partition_reduce": 1, "partition_map": 5}
        assert res["state_counts"] == {"completed": 4, "failed": 1, "aborted": 1}

        assert res["controller_agent_job_count"] == 0
        assert res["scheduler_job_count"] == 0
        if data_source == "runtime":
            assert res["cypress_job_count"] == 6
            assert res["archive_job_count"] == yson.YsonEntity()
        else:
            assert data_source == "archive"
            assert res["cypress_job_count"] == yson.YsonEntity()
            assert res["archive_job_count"] == 6

        answers_for_filters = TestListJobs._get_answers_for_filters_after_finish(job_ids)
        TestListJobs._validate_filters(op, answers_for_filters, data_source=data_source)

        answers_for_sorting = TestListJobs._get_answers_for_sorting_after_finish(job_ids)
        TestListJobs._validate_sorting(op, answers_for_sorting, data_source=data_source)

        res = checked_list_jobs(op.id, data_source=data_source, with_stderr=True)
        job_id = res["jobs"][0]["id"]
        assert get_stderr_from_table(op.id, job_id) == "STDERR-OUTPUT\n"
        assert get_profile_from_table(op.id, job_id) == ("test", "foobar")

    def _run_op_and_wait_mapper_breakpoint(self):
        input_table, output_table = self._create_tables()
        failed_job_id_fname = os.path.join(self._tmpdir, "failed_job_id")
        # Write stderrs in jobs to ensure they will be saved.
        mapper_command = with_breakpoint(
            """echo STDERR-OUTPUT >&2 ; cat; printf 'test\\nfoobar' >&8; """
            """test $YT_JOB_INDEX -eq "1" && echo $YT_JOB_ID > {failed_job_id_fname} && exit 1; """
            """BREAKPOINT"""
                .format(failed_job_id_fname=failed_job_id_fname),
            breakpoint_name="mapper",
        )
        reducer_command = with_breakpoint(
            """echo STDERR-OUTPUT >&2 ; cat; printf 'test\\nfoobar' >&8; BREAKPOINT""",
            breakpoint_name="reducer",
        )
        op = map_reduce(
            dont_track=True,
            label="list_jobs",
            in_=input_table,
            out=output_table,
            mapper_command=mapper_command,
            reducer_command=reducer_command,
            sort_by="foo",
            reduce_by="foo",
            spec={
                "enable_profiling": True,
                "mapper": {
                    "input_format": "json",
                    "output_format": "json",
                },
                "map_job_count" : 3,
            },
        )

        job_ids = {}
        job_ids["completed_map"] = wait_breakpoint(breakpoint_name="mapper", job_count=3)

        wait(lambda: op.get_job_count("failed") == 1)
        wait(lambda: os.path.exists(failed_job_id_fname))
        with open(failed_job_id_fname) as f:
            job_ids["failed_map"] = [f.read().strip()]

        wait(op.get_running_jobs)
        job_ids["aborted_map"] = [job_ids["completed_map"].pop()]
        abort_job(job_ids["aborted_map"][0])

        job_ids["completed_map"] = wait_breakpoint(breakpoint_name="mapper", job_count=4)
        aborted_job_index = job_ids["completed_map"].index(job_ids["aborted_map"][0])
        del job_ids["completed_map"][aborted_job_index]

        def check_running_jobs():
            jobs = op.get_running_jobs()
            return job_ids["aborted_map"][0] not in jobs and len(jobs) >= 3
        wait(check_running_jobs)

        job_ids["map"] = job_ids["completed_map"] + job_ids["failed_map"] + job_ids["aborted_map"]
        return op, job_ids

    @add_failed_operation_stderrs_to_error_message
    @pytest.mark.parametrize("data_source", ["runtime", "archive"])
    def test_list_jobs(self, data_source):
        op, job_ids = self._run_op_and_wait_mapper_breakpoint()

        wait_assert(self._check_during_map, op, job_ids, data_source=data_source)

        release_breakpoint(breakpoint_name="mapper")
        job_ids["reduce"] = wait_breakpoint(breakpoint_name="reducer", job_count=1)

        wait_assert(self._check_during_reduce, op, job_ids, data_source=data_source)

        release_breakpoint(breakpoint_name="reducer")
        op.track()

        wait_assert(self._check_after_finish, op, job_ids, data_source=data_source)

        if data_source == "runtime":
            return

        clean_operations()

        wait_assert(self._check_after_finish, op, job_ids, data_source=data_source)

    @pytest.mark.parametrize("data_source", ["runtime", "archive"])
    def test_running_jobs_stderr_size(self, data_source):
        input_table, output_table = self._create_tables()
        op = map(
            dont_track=True,
            in_=input_table,
            out=output_table,
            command=with_breakpoint("echo MAPPER-STDERR-OUTPUT >&2 ; cat ; BREAKPOINT"),
        )

        expected_stderr_size = len("MAPPER-STDERR-OUTPUT\n")

        jobs = wait_breakpoint()
        def get_stderr_size():
            return get(op.get_path() + "/controller_orchid/running_jobs/{0}/stderr_size".format(jobs[0]))
        wait(lambda: get_stderr_size() == expected_stderr_size)

        res = checked_list_jobs(op.id, data_source=data_source)
        assert __builtin__.set(job["id"] for job in res["jobs"]) == __builtin__.set(jobs)
        for job in res["jobs"]:
            assert job["stderr_size"] == expected_stderr_size

        res = checked_list_jobs(op.id, with_stderr=True, data_source=data_source)
        for job in res["jobs"]:
            assert job["stderr_size"] == expected_stderr_size
        assert __builtin__.set(job["id"] for job in res["jobs"]) == __builtin__.set(jobs)

        res = checked_list_jobs(op.id, with_stderr=False, data_source=data_source)
        assert res["jobs"] == []

        release_breakpoint()
        op.track()

    @pytest.mark.parametrize("data_source", ["runtime", "archive"])
    def test_aborted_jobs(self, data_source):
        input_table, output_table = self._create_tables()
        before_start = datetime.utcnow()
        op = map(
            dont_track=True,
            in_=input_table,
            out=output_table,
            command=with_breakpoint("echo MAPPER-STDERR-OUTPUT >&2 ; cat ; BREAKPOINT"),
            spec={"job_count": 3},
        )

        jobs = wait_breakpoint()

        assert jobs
        abort_job(jobs[0])

        release_breakpoint()
        op.track()

        res = checked_list_jobs(op.id, data_source=data_source)["jobs"]
        assert any(job["state"] == "aborted" for job in res)
        assert all((date_string_to_datetime(job["start_time"]) > before_start) for job in res)
        assert all((date_string_to_datetime(job["finish_time"]) >= date_string_to_datetime(job["start_time"])) for job in res)

    def test_running_aborted_jobs(self):
        input_table, output_table = self._create_tables()
        op = map(
            dont_track=True,
            in_=input_table,
            out=output_table,
            command='if [ "$YT_JOB_INDEX" = "0" ]; then sleep 1000; fi;',
        )

        wait(lambda: op.get_running_jobs())
        wait(lambda: len(checked_list_jobs(op.id, data_source="archive")["jobs"]) == 1)

        unmount_table("//sys/operations_archive/jobs")
        wait(lambda: get("//sys/operations_archive/jobs/@tablet_state") == "unmounted")

        self.Env.kill_nodes()
        self.Env.start_nodes()

        clear_metadata_caches()

        wait_for_cells()

        mount_table("//sys/operations_archive/jobs")
        wait(lambda: get("//sys/operations_archive/jobs/@tablet_state") == "mounted")

        op.track()

        def check():
            jobs = checked_list_jobs(op.id, running_jobs_lookbehind_period=1000, data_source="archive")["jobs"]
            return len(jobs) == 1
        wait(check)

    def test_stderrs_and_hash_buckets_storage(self):
        input_table, output_table = self._create_tables()
        op = map(
            dont_track=True,
            in_=input_table,
            out=output_table,
            command="echo foo >&2; false",
            spec={
                "max_failed_job_count": 1,
                "testing": {"cypress_storage_mode": "hash_buckets"},
            },
        )

        wait(lambda: get(op.get_path() + "/@state") == "failed")
        jobs = checked_list_jobs(op.id, data_source="auto")["jobs"]
        assert len(jobs) == 1
        assert jobs[0]["stderr_size"] > 0

    def test_list_jobs_of_vanilla_operation(self):
        spec = {
            "tasks": {
                "task_a": {
                    "job_count": 1,
                    "command": "false",
                },
            },
            "max_failed_job_count": 1,
        }

        op = vanilla(spec=spec, dont_track=True)
        with pytest.raises(YtError):
            op.track()

        clean_operations()
        jobs = checked_list_jobs(op.id, data_source="archive")["jobs"]
        assert len(jobs) == 1

    def test_errors(self):
        input_table, output_table = self._create_tables()
        op = map(
            dont_track=True,
            in_=input_table,
            out=output_table,
            command=with_breakpoint("BREAKPOINT"),
        )
        wait_breakpoint()

        unmount_table("//sys/operations_archive/jobs")
        wait(lambda: get("//sys/operations_archive/jobs/@tablet_state") == "unmounted")
        try:
            wait(lambda: len(list_jobs(op.id, data_source="archive")["errors"]) == 1)
        finally:
            mount_table("//sys/operations_archive/jobs")
            wait(lambda: get("//sys/operations_archive/jobs/@tablet_state") == "mounted")
            release_breakpoint()
            op.track()

##################################################################

class TestListJobsRpcProxy(TestListJobs):
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True
    ENABLE_PROXY = True
