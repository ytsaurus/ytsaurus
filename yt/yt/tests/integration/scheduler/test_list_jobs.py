from yt_env_setup import YTEnvSetup, Restarter, NODES_SERVICE, CONTROLLER_AGENTS_SERVICE

from yt_commands import (
    authors, wait, retry, wait_no_assert, wait_breakpoint, release_breakpoint, with_breakpoint, create,
    get, create_tmpdir,
    create_pool, insert_rows, select_rows, lookup_rows, write_table, map, map_reduce, vanilla, run_test_vanilla,
    abort_job, list_jobs, clean_operations, mount_table, unmount_table, wait_for_cells, sync_create_cells,
    update_controller_agent_config,
    make_random_string, raises_yt_error, clear_metadata_caches, ls)

from yt_scheduler_helpers import scheduler_new_orchid_pool_tree_path

import yt_error_codes

import yt.yson as yson
import yt.environment.init_operation_archive as init_operation_archive
from yt.wrapper.operation_commands import add_failed_operation_stderrs_to_error_message
from yt.wrapper.common import uuid_hash_pair, YtError
from yt.common import date_string_to_datetime

import os
import time
import builtins
from copy import deepcopy
from flaky import flaky
from collections import defaultdict
from datetime import datetime

import pytest


def get_stderr_from_table(operation_id, job_id):
    operation_hash = uuid_hash_pair(operation_id)
    job_hash = uuid_hash_pair(job_id)
    rows = list(
        select_rows(
            "stderr from [//sys/operations_archive/stderrs] "
            "where operation_id_lo={0}u and operation_id_hi={1}u and "
            "job_id_lo={2}u and job_id_hi={3}u".format(operation_hash.lo, operation_hash.hi, job_hash.lo, job_hash.hi)
        )
    )
    assert len(rows) == 1
    return rows[0]["stderr"].encode("ascii", errors="ignore")


def get_job_from_table(operation_id, job_id):
    path = init_operation_archive.DEFAULT_ARCHIVE_PATH + "/jobs"
    operation_hash = uuid_hash_pair(operation_id)
    job_hash = uuid_hash_pair(job_id)
    rows = lookup_rows(
        path,
        [
            {
                "operation_id_hi": operation_hash.hi,
                "operation_id_lo": operation_hash.lo,
                "job_id_hi": job_hash.hi,
                "job_id_lo": job_hash.lo,
            }
        ],
    )
    return rows[0] if rows else None


def set_job_in_table(operation_id, job_id, fields):
    path = init_operation_archive.DEFAULT_ARCHIVE_PATH + "/jobs"
    operation_hash = uuid_hash_pair(operation_id)
    job_hash = uuid_hash_pair(job_id)
    fields.update(
        {
            "operation_id_hi": operation_hash.hi,
            "operation_id_lo": operation_hash.lo,
            "job_id_hi": job_hash.hi,
            "job_id_lo": job_hash.lo,
        }
    )
    insert_rows(path, [fields], update=True, atomicity="none")


def checked_list_jobs(*args, **kwargs):
    res = list_jobs(*args, **kwargs)
    if res["errors"]:
        raise YtError(message="list_jobs failed", inner_errors=res["errors"])
    return res


class TestListJobsBase(YTEnvSetup):
    DELTA_NODE_CONFIG = {
        "exec_node": {
            "job_reporter": {
                "enabled": True,
                "reporting_period": 10,
                "min_repeat_delay": 10,
                "max_repeat_delay": 10,
            },
            "controller_agent_connector": {"heartbeat_period": 100},  # 100 msec
            "scheduler_connector": {"heartbeat_period": 100},  # 100 msec
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
            "operation_time_limit_check_period": 100,
            "snapshot_period": 500,
            "operations_update_period": 100,
            "job_reporter": {
                "enabled": True,
                "reporting_period": 10,
                "min_repeat_delay": 10,
                "max_repeat_delay": 10,
            },
        },
    }

    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1
    USE_DYNAMIC_TABLES = True

    def setup_method(self, method):
        super(TestListJobsBase, self).setup_method(method)
        sync_create_cells(1)
        init_operation_archive.create_tables_latest_version(
            self.Env.create_native_client(), override_tablet_cell_bundle="default"
        )
        self._tmpdir = create_tmpdir("list_jobs")
        self.failed_job_id_fname = os.path.join(self._tmpdir, "failed_job_id")

    def restart_services_and_wait_jobs_table(self, services):
        def get_user_slots_limit():
            return get(scheduler_new_orchid_pool_tree_path("default") + "/resource_limits/user_slots")

        initial_user_slots = get_user_slots_limit()

        unmount_table("//sys/operations_archive/jobs")
        wait(lambda: get("//sys/operations_archive/jobs/@tablet_state") == "unmounted")
        with Restarter(self.Env, services):
            pass
        clear_metadata_caches()
        wait_for_cells()
        mount_table("//sys/operations_archive/jobs")
        wait(lambda: get("//sys/operations_archive/jobs/@tablet_state") == "mounted")

        wait(lambda: get_user_slots_limit() == initial_user_slots)

    def _create_tables(self):
        input_table = "//tmp/input_" + make_random_string()
        output_table = "//tmp/output_" + make_random_string()
        create("table", input_table)
        create("table", output_table)
        write_table(input_table, [{"foo": "bar"}, {"foo": "baz"}, {"foo": "qux"}])
        return input_table, output_table

    @staticmethod
    def _validate_address_filter(op):
        nodes = ls("//sys/cluster_nodes")
        random_address = nodes[0]
        prefixes = []
        for node in nodes:
            prefixes.append(node)
            prefixes.append(node[:-1])
        for small_prefix in ["", random_address[:1], random_address[:2]]:
            prefixes.append(small_prefix)
        for wrong_prefix in [random_address[2:5], random_address[3:7], random_address[1:4]]:
            prefixes.append(wrong_prefix)

        jobs = checked_list_jobs(op.id)["jobs"]
        prefix_to_expected_job_list = defaultdict(list)
        for prefix in prefixes:
            prefix_to_expected_job_list[prefix] = [job["id"] for job in jobs if job["address"].startswith(prefix)]

        for prefix, expected_jobs in prefix_to_expected_job_list.items():
            actual_jobs_for_address = checked_list_jobs(op.id, address=prefix)["jobs"]
            assert builtins.set(job["id"] for job in actual_jobs_for_address) == builtins.set(expected_jobs)

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
    def _validate_filters(op, answers):
        for (name, value), correct_job_ids in answers.items():
            jobs = checked_list_jobs(op.id, **{name: value})["jobs"]
            assert builtins.set(job["id"] for job in jobs) == builtins.set(
                correct_job_ids
            ), "Assertion for filter {}={} failed".format(name, repr(value))
        TestListJobs._validate_address_filter(op)

    @staticmethod
    def _get_answers_for_sorting_during_map(job_ids):
        return {
            "type": [job_ids["map"]],
            "state": [
                job_ids["aborted_map"],
                job_ids["failed_map"],
                job_ids["completed_map"],
            ],
            "start_time": [job_ids["map"]],
            "finish_time": [
                job_ids["completed_map"],
                job_ids["failed_map"],
                job_ids["aborted_map"],
            ],
            "duration": [
                job_ids["failed_map"] + job_ids["aborted_map"],
                job_ids["completed_map"],
            ],
            "id": [[job_id] for job_id in sorted(job_ids["map"])],
        }

    @staticmethod
    def _get_answers_for_sorting_during_reduce(job_ids):
        return {
            "type": [job_ids["map"], job_ids["reduce"]],
            "state": [
                job_ids["aborted_map"],
                job_ids["completed_map"],
                job_ids["failed_map"],
                job_ids["reduce"],
            ],
            "start_time": [job_ids["map"], job_ids["reduce"]],
            "finish_time": [
                job_ids["reduce"],
                job_ids["failed_map"],
                job_ids["aborted_map"],
                job_ids["completed_map"],
            ],
            "duration": [job_ids["map"] + job_ids["reduce"]],
            "id": [[job_id] for job_id in sorted(job_ids["map"] + job_ids["reduce"])],
        }

    @staticmethod
    def _get_answers_for_sorting_after_finish(job_ids):
        return {
            "type": [job_ids["map"], job_ids["reduce"]],
            "state": [
                job_ids["aborted_map"],
                job_ids["completed_map"] + job_ids["reduce"],
                job_ids["failed_map"],
            ],
            "start_time": [job_ids["map"], job_ids["reduce"]],
            "finish_time": [
                job_ids["failed_map"],
                job_ids["aborted_map"],
                job_ids["completed_map"],
                job_ids["reduce"],
            ],
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
                finish_time = (
                    date_string_to_datetime(finish_time_str) if finish_time_str is not None else datetime.now()
                )
                return finish_time - date_string_to_datetime(job["start_time"])

            return key
        else:
            raise Exception("Unknown sorting key {}".format(field_name))

    @staticmethod
    def _validate_sorting(op, correct_answers):
        for field_name, correct_job_ids in correct_answers.items():
            for sort_order in ["ascending", "descending"]:
                jobs = checked_list_jobs(
                    op.id,
                    sort_field=field_name,
                    sort_order=sort_order,
                )["jobs"]

                reverse = sort_order == "descending"
                assert (
                    sorted(
                        jobs,
                        key=TestListJobs._get_sorting_key(field_name),
                        reverse=reverse,
                    )
                    == jobs
                ), "Assertion for sort_field={} and sort_order={} failed".format(field_name, sort_order)

                if reverse:
                    correct_job_ids = reversed(correct_job_ids)

                group_start_idx = 0
                for correct_group in correct_job_ids:
                    group = jobs[group_start_idx:group_start_idx + len(correct_group)]
                    assert builtins.set(job["id"] for job in group) == builtins.set(
                        correct_group
                    ), "Assertion for sort_field={} and sort_order={} failed".format(field_name, sort_order)
                    group_start_idx += len(correct_group)

    @staticmethod
    def _check_during_map(op, job_ids):
        res = checked_list_jobs(op.id)
        assert builtins.set(job["id"] for job in res["jobs"]) == builtins.set(job_ids["map"])
        assert res["type_counts"] == {"partition_map": 5}
        assert res["state_counts"] == {"running": 3, "failed": 1, "aborted": 1}

        assert res["controller_agent_job_count"] == 5
        assert res["scheduler_job_count"] == 5
        assert res["cypress_job_count"] == yson.YsonEntity()
        assert res["archive_job_count"] == 5

        answers_for_filters = TestListJobs._get_answers_for_filters_during_map(job_ids)
        TestListJobs._validate_filters(op, answers_for_filters)

        answers_for_sorting = TestListJobs._get_answers_for_sorting_during_map(job_ids)
        TestListJobs._validate_sorting(op, answers_for_sorting)

    @staticmethod
    def _check_during_reduce(op, job_ids):
        res = checked_list_jobs(op.id)
        assert builtins.set(job["id"] for job in res["jobs"]) == builtins.set(job_ids["reduce"] + job_ids["map"])
        assert res["type_counts"] == {"partition_reduce": 1, "partition_map": 5}
        assert res["state_counts"] == {
            "running": 1,
            "completed": 3,
            "failed": 1,
            "aborted": 1,
        }

        assert res["controller_agent_job_count"] == 6
        assert res["scheduler_job_count"] == 6
        assert res["cypress_job_count"] == yson.YsonEntity()
        assert res["archive_job_count"] == 6

        answers_for_filters = TestListJobs._get_answers_for_filters_during_reduce(job_ids)
        TestListJobs._validate_filters(op, answers_for_filters)

        answers_for_sorting = TestListJobs._get_answers_for_sorting_during_reduce(job_ids)
        TestListJobs._validate_sorting(op, answers_for_sorting)

    @staticmethod
    def _check_after_finish(op, job_ids, operation_cleaned):
        res = checked_list_jobs(op.id)
        assert builtins.set(job["id"] for job in res["jobs"]) == builtins.set(job_ids["reduce"] + job_ids["map"])
        assert res["type_counts"] == {"partition_reduce": 1, "partition_map": 5}
        assert res["state_counts"] == {"completed": 4, "failed": 1, "aborted": 1}

        if operation_cleaned:
            assert res["controller_agent_job_count"] == res["scheduler_job_count"] == 0
        else:
            assert res["controller_agent_job_count"] == res["scheduler_job_count"] == 6
        assert res["cypress_job_count"] == yson.YsonEntity()
        assert res["archive_job_count"] == 6

        answers_for_filters = TestListJobs._get_answers_for_filters_after_finish(job_ids)
        TestListJobs._validate_filters(
            op,
            answers_for_filters,
        )

        answers_for_sorting = TestListJobs._get_answers_for_sorting_after_finish(job_ids)
        TestListJobs._validate_sorting(
            op,
            answers_for_sorting,
        )

        res = checked_list_jobs(op.id, with_stderr=True)
        job_id = res["jobs"][0]["id"]
        assert get_stderr_from_table(op.id, job_id) == b"STDERR-OUTPUT\n"

    def _run_op_and_wait_mapper_breakpoint(self, spec_patch={}):
        input_table, output_table = self._create_tables()
        # Write stderrs in jobs to ensure they will be saved.
        mapper_command = with_breakpoint(
            """echo STDERR-OUTPUT >&2 ; cat; """
            """test $YT_JOB_INDEX -eq "1" && echo $YT_JOB_ID > {failed_job_id_fname} && exit 1; """
            """BREAKPOINT""".format(failed_job_id_fname=self.failed_job_id_fname),
            breakpoint_name="mapper",
        )
        reducer_command = with_breakpoint(
            """echo STDERR-OUTPUT >&2 ; cat; BREAKPOINT""",
            breakpoint_name="reducer",
        )
        spec = {
            "mapper": {
                "input_format": "json",
                "output_format": "json",
            },
            "map_job_count": 3,
            "partition_count": 1,
        }
        spec.update(spec_patch)
        op = map_reduce(
            track=False,
            label="list_jobs",
            in_=input_table,
            out=output_table,
            mapper_command=mapper_command,
            reducer_command=reducer_command,
            sort_by="foo",
            reduce_by="foo",
            spec=spec,
            fail_fast=False,
        )

        job_ids = {}
        job_ids["completed_map"] = wait_breakpoint(breakpoint_name="mapper", job_count=3)
        return op, job_ids

    def _run_op_and_fill_job_ids(self):
        op, job_ids = self._run_op_and_wait_mapper_breakpoint()

        wait(lambda: op.get_job_count("failed") == 1)
        wait(lambda: os.path.exists(self.failed_job_id_fname))
        with open(self.failed_job_id_fname) as f:
            job_ids["failed_map"] = [f.read().strip()]

        wait(op.get_running_jobs)
        job_ids["aborted_map"] = [job_ids["completed_map"].pop()]
        abort_job(job_ids["aborted_map"][0])

        job_ids["completed_map"] = wait_breakpoint(breakpoint_name="mapper", job_count=4)
        aborted_job_index = job_ids["completed_map"].index(job_ids["aborted_map"][0])
        del job_ids["completed_map"][aborted_job_index]

        @wait_no_assert
        def check_running_jobs():
            jobs = op.get_running_jobs()
            assert job_ids["aborted_map"][0] not in jobs
            assert len(jobs) >= 3

        job_ids["map"] = job_ids["completed_map"] + job_ids["failed_map"] + job_ids["aborted_map"]
        return op, job_ids

    @authors("levysotsky")
    @add_failed_operation_stderrs_to_error_message
    def test_list_jobs_attributes(self):
        create_pool("my_pool")
        before_start_time = datetime.utcnow()
        op, job_ids = self._run_op_and_wait_mapper_breakpoint(
            spec_patch={
                "scheduling_options_per_pool_tree": {
                    "default": {"pool": "my_pool"},
                },
            },
        )

        wait(op.get_running_jobs)
        aborted_map_job_id = job_ids["completed_map"].pop()
        abort_job(aborted_map_job_id)

        release_breakpoint(breakpoint_name="mapper")
        release_breakpoint(breakpoint_name="reducer")
        op.track()

        completed_map_job_id = job_ids["completed_map"][0]

        @wait_no_assert
        def has_job_state_converged():
            set_job_in_table(op.id, completed_map_job_id, {"transient_state": "running"})
            time.sleep(1)
            assert get_job_from_table(op.id, completed_map_job_id)["transient_state"] == "running"
            assert get_job_from_table(op.id, aborted_map_job_id)["transient_state"] == "aborted"

        def check_times(job):
            start_time = date_string_to_datetime(completed_map_job["start_time"])
            assert completed_map_job.get("finish_time") is not None
            finish_time = date_string_to_datetime(completed_map_job.get("finish_time"))
            assert before_start_time < start_time < finish_time < datetime.now()

        res = checked_list_jobs(op.id)

        completed_map_job_list = [job for job in res["jobs"] if job["id"] == completed_map_job_id]
        assert len(completed_map_job_list) == 1
        completed_map_job = completed_map_job_list[0]

        check_times(completed_map_job)
        assert completed_map_job["controller_state"] == "completed"
        assert completed_map_job["archive_state"] == "running"
        assert completed_map_job["type"] == "partition_map"
        assert "slot_index" in completed_map_job["exec_attributes"]
        assert len(completed_map_job["exec_attributes"]["sandbox_path"]) > 0
        assert completed_map_job["pool"] == "my_pool"
        assert completed_map_job["pool_tree"] == "default"
        assert completed_map_job["job_cookie"] >= 0

        stderr_size = len(b"STDERR-OUTPUT\n")
        assert completed_map_job["stderr_size"] == stderr_size

        aborted_map_job_list = [job for job in res["jobs"] if job["id"] == aborted_map_job_id]
        assert len(aborted_map_job_list) == 1
        aborted_map_job = aborted_map_job_list[0]

        check_times(aborted_map_job)
        assert aborted_map_job["type"] == "partition_map"
        assert aborted_map_job.get("abort_reason") == "user_request"

    @authors("levysotsky")
    @pytest.mark.timeout(150)
    @add_failed_operation_stderrs_to_error_message
    def test_list_jobs(self):
        op, job_ids = self._run_op_and_fill_job_ids()

        wait_no_assert(lambda: self._check_during_map(op, job_ids))

        release_breakpoint(breakpoint_name="mapper")
        job_ids["reduce"] = wait_breakpoint(breakpoint_name="reducer", job_count=1)

        wait_no_assert(lambda: self._check_during_reduce(op, job_ids))

        release_breakpoint(breakpoint_name="reducer")
        op.track()

        wait_no_assert(lambda: self._check_after_finish(op, job_ids, operation_cleaned=False))

        clean_operations()

        wait_no_assert(lambda: self._check_after_finish(op, job_ids, operation_cleaned=True))


class TestListJobsStatisticsLz4(TestListJobsBase):
    DELTA_NODE_CONFIG = deepcopy(TestListJobsBase.DELTA_NODE_CONFIG)
    DELTA_NODE_CONFIG["exec_node"]["job_reporter"]["report_statistics_lz4"] = True


class TestListJobs(TestListJobsBase):
    @authors("ermolovd", "levysotsky")
    def test_running_jobs_stderr_size(self):
        input_table, output_table = self._create_tables()
        op = map(
            track=False,
            in_=input_table,
            out=output_table,
            command=with_breakpoint("echo MAPPER-STDERR-OUTPUT >&2 ; cat ; BREAKPOINT"),
        )

        expected_stderr_size = len(b"MAPPER-STDERR-OUTPUT\n")

        jobs = wait_breakpoint()

        def get_stderr_size():
            return get(op.get_path() + "/controller_orchid/running_jobs/{0}/stderr_size".format(jobs[0]))

        wait(lambda: get_stderr_size() == expected_stderr_size)

        res = checked_list_jobs(
            op.id,
        )
        assert builtins.set(job["id"] for job in res["jobs"]) == builtins.set(jobs)
        for job in res["jobs"]:
            assert job["stderr_size"] == expected_stderr_size

        res = checked_list_jobs(
            op.id,
            with_stderr=True,
        )
        for job in res["jobs"]:
            assert job["stderr_size"] == expected_stderr_size
        assert builtins.set(job["id"] for job in res["jobs"]) == builtins.set(jobs)

        res = checked_list_jobs(
            op.id,
            with_stderr=False,
        )
        assert res["jobs"] == []

        release_breakpoint()
        op.track()

        @wait_no_assert
        def any_has_spec():
            res = checked_list_jobs(
                op.id,
            )
            assert any("has_spec" in job and job["has_spec"] for job in res["jobs"])

    @authors("ignat", "levysotsky")
    def test_aborted_jobs(self):
        input_table, output_table = self._create_tables()
        before_start = datetime.utcnow()
        op = map(
            track=False,
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

        res = checked_list_jobs(
            op.id,
        )["jobs"]
        assert any(job["state"] == "aborted" for job in res)
        assert all((date_string_to_datetime(job["start_time"]) > before_start) for job in res)
        assert all(
            (date_string_to_datetime(job["finish_time"]) >= date_string_to_datetime(job["start_time"])) for job in res
        )

    @authors("ignat")
    def test_running_aborted_jobs(self):
        update_controller_agent_config("enable_snapshot_loading", False)

        input_table, output_table = self._create_tables()
        op = map(
            track=False,
            in_=input_table,
            out=output_table,
            command='if [ "$YT_JOB_INDEX" = "0" ]; then sleep 1000; fi;',
        )

        wait(lambda: op.get_running_jobs())
        wait(lambda: len(checked_list_jobs(op.id)["jobs"]) == 1)

        op.suspend()

        self.restart_services_and_wait_jobs_table([CONTROLLER_AGENTS_SERVICE, NODES_SERVICE])

        op.resume()

        @wait_no_assert
        def check():
            actual_jobs = checked_list_jobs(op.id, running_jobs_lookbehind_period=1000)["jobs"]
            all_jobs = checked_list_jobs(op.id, running_jobs_lookbehind_period=60 * 1000)["jobs"]
            assert len(actual_jobs) == 1
            assert len(all_jobs) == 2

    @authors("levysotsky")
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

        op = vanilla(spec=spec, track=False)
        with pytest.raises(YtError):
            op.track()

        clean_operations()
        jobs = checked_list_jobs(op.id)["jobs"]
        assert len(jobs) == 1

    @authors("levysotsky")
    def test_errors(self):
        input_table, output_table = self._create_tables()
        op = map(
            track=False,
            in_=input_table,
            out=output_table,
            command=with_breakpoint("BREAKPOINT"),
        )
        wait_breakpoint()

        unmount_table("//sys/operations_archive/jobs")
        wait(lambda: get("//sys/operations_archive/jobs/@tablet_state") == "unmounted")
        try:
            wait(lambda: len(list_jobs(op.id)["errors"]) >= 1)
        finally:
            mount_table("//sys/operations_archive/jobs")
            wait(lambda: get("//sys/operations_archive/jobs/@tablet_state") == "mounted")
            release_breakpoint()
            op.track()

    @authors("levysotsky")
    @add_failed_operation_stderrs_to_error_message
    def test_stale_jobs(self):
        input_table, output_table = self._create_tables()
        op = map(
            track=False,
            in_=input_table,
            out=output_table,
            command=with_breakpoint("cat ; BREAKPOINT"),
            spec={
                "mapper": {
                    "input_format": "json",
                    "output_format": "json",
                },
                "map_job_count": 1,
            },
        )

        (job_id,) = wait_breakpoint()

        wait(lambda: get_job_from_table(op.id, job_id)["controller_state"] == "running")

        release_breakpoint()
        op.track()

        wait(lambda: get_job_from_table(op.id, job_id)["controller_state"] == "completed")

        @wait_no_assert
        def has_job_state_converged():
            set_job_in_table(op.id, job_id, {"transient_state": "running", "controller_state": "running"})
            time.sleep(1)
            assert get_job_from_table(op.id, job_id)["transient_state"] == "running"

        op.wait_for_state("completed")

        res = checked_list_jobs(op.id)
        res_jobs = [job for job in res["jobs"] if job["id"] == job_id]
        assert len(res_jobs) == 1
        res_job = res_jobs[0]

        assert res_job.get("controller_state") == "running"
        assert res_job["archive_state"] == "running"
        assert res_job.get("is_stale")

    @authors("levysotsky")
    @flaky(max_runs=3)
    def test_revival(self):
        op = run_test_vanilla(
            with_breakpoint("BREAKPOINT"),
            job_count=1,
            spec={"testing": {"delay_inside_revive": 5000}},
        )
        (job_id,) = wait_breakpoint()
        op.wait_for_fresh_snapshot()

        with Restarter(self.Env, CONTROLLER_AGENTS_SERVICE):
            pass

        with raises_yt_error(yt_error_codes.UncertainOperationControllerState):
            checked_list_jobs(op.id)

        res = retry(lambda: checked_list_jobs(op.id))
        res_jobs = [job for job in res["jobs"] if job["id"] == job_id]
        assert len(res_jobs) == 1
        res_job = res_jobs[0]

        assert res_job.get("controller_state") == "running"
        assert res_job.get("archive_state") == "running"
        assert not res_job.get("is_stale")

    @authors("omgronny")
    def test_consider_controller_state_in_archive_select(self):
        op = run_test_vanilla(
            with_breakpoint("BREAKPOINT"),
            job_count=1,
        )
        (job_id,) = wait_breakpoint()

        wait(lambda: get_job_from_table(op.id, job_id)["controller_state"] == "running")

        self.restart_services_and_wait_jobs_table([NODES_SERVICE])

        release_breakpoint()
        op.track()

        wait(lambda: get_job_from_table(op.id, job_id)["controller_state"] == "aborted")

        res = checked_list_jobs(op.id)
        res_job = [job for job in res["jobs"] if job["id"] == job_id][0]

        assert res_job.get("controller_state") == "aborted"
        assert res_job["archive_state"] == "running"

        res = checked_list_jobs(op.id, state="aborted")
        res_jobs = [job for job in res["jobs"] if job["id"] == job_id]
        assert len(res_jobs) == 1
        res_job = res_jobs[0]

        assert res_job.get("controller_state") == "aborted"
        assert res_job.get("archive_state") == "running"

        len(checked_list_jobs(op.id, state="running")["jobs"]) == 0

    @authors("gritukan")
    def test_task_name(self):
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
            jobs = checked_list_jobs(
                op.id,
                task_name="master",
            )["jobs"]
            assert sorted([job["id"] for job in jobs]) == sorted(master_job_ids)

            jobs = checked_list_jobs(
                op.id,
                task_name="slave",
            )["jobs"]
            assert sorted([job["id"] for job in jobs]) == sorted(slave_job_ids)

            jobs = checked_list_jobs(
                op.id,
                task_name="non_existent_task",
            )["jobs"]
            assert jobs == []

        check_task_names()

        release_breakpoint(breakpoint_name="master")
        release_breakpoint(breakpoint_name="slave")
        op.track()
        clean_operations()

        check_task_names()


##################################################################


class TestListJobsRpcProxy(TestListJobs):
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True
    ENABLE_HTTP_PROXY = True


class TestListJobsStatisticsLz4RpcProxy(TestListJobsStatisticsLz4):
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True
    ENABLE_HTTP_PROXY = True
