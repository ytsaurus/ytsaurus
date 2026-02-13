from yt_env_setup import YTEnvSetup, Restarter, NODES_SERVICE, CONTROLLER_AGENTS_SERVICE, SCHEDULERS_SERVICE

from yt_commands import (
    authors, wait, retry, wait_no_assert, wait_breakpoint,
    release_breakpoint, with_breakpoint, create, get, set, create_tmpdir,
    create_pool, insert_rows, select_rows, lookup_rows, write_table,
    map, map_reduce, vanilla, run_test_vanilla, run_sleeping_vanilla, interrupt_job,
    abort_job, list_jobs, clean_operations, mount_table, unmount_table, wait_for_cells, sync_create_cells,
    update_controller_agent_config, print_debug, exists, get_allocation_id_from_job_id,
    make_random_string, raises_yt_error, clear_metadata_caches, ls, get_job, list_operation_events)

from yt_scheduler_helpers import scheduler_new_orchid_pool_tree_path

from yt_operations_archive_helpers import (
    get_allocation_id_from_archive, get_job_from_archive, update_job_in_archive)

import yt_error_codes

import yt.yson as yson
import yt.environment.init_operations_archive as init_operations_archive
from yt.wrapper.operation_commands import add_failed_operation_stderrs_to_error_message
from yt.wrapper.common import uuid_hash_pair, YtError
from yt.common import date_string_to_datetime, date_string_to_timestamp_mcs, datetime_to_string, utcnow, update

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
    path = init_operations_archive.DEFAULT_ARCHIVE_PATH + "/jobs"
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
    path = init_operations_archive.DEFAULT_ARCHIVE_PATH + "/jobs"
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
    ENABLE_MULTIDAEMON = False  # There are component restarts.
    DELTA_NODE_CONFIG = {
        "exec_node": {
            "job_proxy": {
                "job_proxy_heartbeat_period": 100,  # msec
            },
        },
    }

    DELTA_DYNAMIC_NODE_CONFIG = {
        "%true": {
            "exec_node": {
                "job_reporter": {
                    "reporting_period": 10,
                    "min_repeat_delay": 10,
                    "max_repeat_delay": 10,
                },
            },
        }
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

    DELTA_DRIVER_CONFIG = {
        "cluster_connection_dynamic_config_policy": "from_cluster_directory",
    }

    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1
    USE_DYNAMIC_TABLES = True

    def setup_method(self, method):
        super(TestListJobsBase, self).setup_method(method)
        sync_create_cells(1)
        init_operations_archive.create_tables_latest_version(
            self.Env.create_native_client(), override_tablet_cell_bundle="default"
        )
        self._tmpdir = create_tmpdir("list_jobs")
        self.failed_job_id_fname = os.path.join(self._tmpdir, "failed_job_id")

    def _get_operation_incarnation(self, op):
        wait(lambda: exists(op.get_orchid_path() + "/controller/operation_incarnation"))
        incarnation_id = get(op.get_orchid_path() + "/controller/operation_incarnation")
        print_debug(f"Current incarnation of operation {op.id} is {incarnation_id}")

        return incarnation_id


class TestListJobsCommon(TestListJobsBase):
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
        type_counts_full = {"partition_reduce": 1, "partition_map": 5}
        state_counts_full = {"completed": 4, "failed": 1, "aborted": 1}
        res = checked_list_jobs(op.id)
        assert builtins.set(job["id"] for job in res["jobs"]) == builtins.set(job_ids["reduce"] + job_ids["map"])
        assert res["type_counts"] == type_counts_full
        assert res["state_counts"] == state_counts_full

        if operation_cleaned:
            assert res["controller_agent_job_count"] == res["scheduler_job_count"] == 0
        else:
            assert res["controller_agent_job_count"] == res["scheduler_job_count"] == 6
        assert res["cypress_job_count"] == yson.YsonEntity()
        assert res["archive_job_count"] == 6

        @wait_no_assert
        def check_statistics_with_filters():
            res = checked_list_jobs(op.id, type="partition_reduce")
            assert res["type_counts"] == type_counts_full
            assert res["state_counts"] == {"completed": 1}

            res = checked_list_jobs(op.id, state="completed")
            assert res["type_counts"] == {"partition_reduce": 1, "partition_map": 3}
            assert res["state_counts"] == state_counts_full

            res = checked_list_jobs(op.id, state="completed", type="partition_reduce")
            assert res["type_counts"] == {"partition_reduce": 1, "partition_map": 3}
            assert res["state_counts"] == {"completed": 1}

            res = checked_list_jobs(op.id, with_stderr=True)
            assert res["type_counts"] == {"partition_reduce": 1, "partition_map": 4}
            assert res["state_counts"] == {"completed": 4, "failed": 1}

            res = checked_list_jobs(op.id, with_stderr=True, state="completed")
            assert res["type_counts"] == {"partition_reduce": 1, "partition_map": 3}
            assert res["state_counts"] == {"completed": 4, "failed": 1}

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

    @authors("omgronny", "bystrovserg")
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
        assert "events" not in completed_map_job
        assert "statistics" not in completed_map_job

        stderr_size = len(b"STDERR-OUTPUT\n")
        assert completed_map_job["stderr_size"] == stderr_size

        jobs_with_extra_attributes = [job for job in checked_list_jobs(op.id, attributes=["events", "statistics"])["jobs"] if job["id"] == completed_map_job_id]
        assert len(jobs_with_extra_attributes) == 1
        job_with_event = jobs_with_extra_attributes[0]
        assert "events" in job_with_event
        assert "statistics" in job_with_event
        assert len(job_with_event["events"]) > 0
        assert all(field in job_with_event["events"][0] for field in ["phase", "state", "time"])

        aborted_map_job_list = [job for job in res["jobs"] if job["id"] == aborted_map_job_id]
        assert len(aborted_map_job_list) == 1
        aborted_map_job = aborted_map_job_list[0]

        check_times(aborted_map_job)
        assert aborted_map_job["type"] == "partition_map"
        assert aborted_map_job.get("abort_reason") == "user_request"
        assert "events" not in aborted_map_job
        assert "statistics" not in completed_map_job

    @authors("omgronny")
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


class TestListJobsStatisticsLz4(TestListJobsCommon):
    ENABLE_MULTIDAEMON = False  # There are component restarts.
    DELTA_DYNAMIC_NODE_CONFIG = deepcopy(TestListJobsBase.DELTA_DYNAMIC_NODE_CONFIG)
    DELTA_DYNAMIC_NODE_CONFIG["%true"]["exec_node"]["job_reporter"]["report_statistics_lz4"] = True


class TestListJobs(TestListJobsCommon):
    ENABLE_MULTIDAEMON = False  # There are component restarts.
    NUM_TEST_PARTITIONS = 2

    @authors("ermolovd", "omgronny")
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

    @authors("ignat", "omgronny")
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

    @authors("omgronny")
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

    @authors("omgronny")
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

    @authors("omgronny")
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

    @authors("omgronny")
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

    @authors("faucct")
    def test_distributed_operation(self):
        op = vanilla(
            spec={
                "tasks": {
                    "master": {
                        "job_count": 1,
                        "command": ":",
                        "collective_options": {"size": 2},
                    },
                },
            },
        )
        wait(lambda: len(op.list_jobs()) == 2)
        main, replica = sorted(
            list_jobs(op.id, attributes=["collective_member_rank", "collective_id"])["jobs"],
            key=lambda job: job["collective_member_rank"],
        )
        assert main["collective_id"] == main["id"]
        assert main["collective_member_rank"] == 0
        assert replica["collective_id"] == main["id"]
        assert replica["collective_member_rank"] == 1

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

    @authors("omgronny")
    def test_list_jobs_with_monitoring_descriptor(self):
        update_controller_agent_config(
            "user_job_monitoring/extended_max_monitored_user_jobs_per_operation", 2)

        op = run_sleeping_vanilla(
            job_count=3,
            task_patch={
                "monitoring": {
                    "enable": True,
                },
            },
        )

        @wait_no_assert
        def monitoring_descriptors_registered():
            jobs = list_jobs(op.id)["jobs"]
            assert len([job for job in jobs if "monitoring_descriptor" in job]) == 2

        monitored_jobs = list_jobs(op.id, with_monitoring_descriptor=True)["jobs"]
        assert len(monitored_jobs) == 2
        for job in monitored_jobs:
            assert "monitoring_descriptor" in job

        non_monitored_jobs = list_jobs(op.id, with_monitoring_descriptor=False)["jobs"]
        assert len(non_monitored_jobs) == 1
        for job in non_monitored_jobs:
            assert "monitoring_descriptor" not in job

    @authors("omgronny", "bystrovserg")
    def test_list_jobs_sort_by_task_name(self):
        op = vanilla(
            track=False,
            spec={
                "tasks": {
                    "a": {
                        "job_count": 1,
                        "command": with_breakpoint("BREAKPOINT", breakpoint_name="a"),
                    },
                    "b": {
                        "job_count": 1,
                        "command": with_breakpoint("BREAKPOINT", breakpoint_name="b"),
                    },
                    "c": {
                        "job_count": 1,
                        "command": with_breakpoint("BREAKPOINT", breakpoint_name="c"),
                    },
                },
            },
        )

        wait_breakpoint(breakpoint_name="a", job_count=1)
        wait_breakpoint(breakpoint_name="b", job_count=1)
        wait_breakpoint(breakpoint_name="c", job_count=1)

        def check_sorted_list_jobs_with_attr(attr):
            jobs = list_jobs(op.id, sort_field="task_name", attributes=attr)["jobs"]
            assert get_job_from_archive(op.id, jobs[0]["id"])["task_name"] == "a"
            assert get_job_from_archive(op.id, jobs[1]["id"])["task_name"] == "b"
            assert get_job_from_archive(op.id, jobs[2]["id"])["task_name"] == "c"

        def check_sorted_list_jobs():
            jobs = list_jobs(op.id, sort_field="task_name")["jobs"]
            assert jobs[0]["task_name"] == "a"
            assert jobs[1]["task_name"] == "b"
            assert jobs[2]["task_name"] == "c"

        check_sorted_list_jobs()
        check_sorted_list_jobs_with_attr([])
        check_sorted_list_jobs_with_attr(["task_name"])

        release_breakpoint(breakpoint_name="a")
        release_breakpoint(breakpoint_name="b")
        release_breakpoint(breakpoint_name="c")

        op.track()

        check_sorted_list_jobs()
        check_sorted_list_jobs_with_attr([])
        check_sorted_list_jobs_with_attr(["task_name"])

    @authors("bystrovserg")
    def test_list_jobs_continuation_token(self):
        op = run_sleeping_vanilla(job_count=3)

        @wait_no_assert
        def wait():
            jobs = list_jobs(op.id)["jobs"]
            assert len(jobs) == 3

        jobs = list_jobs(op.id, state="running", type="vanilla", sort_field="start_time", limit=1)
        assert len(jobs["jobs"]) == 1

        jobs = list_jobs(op.id, continuation_token=jobs["continuation_token"], state="completed")["jobs"]
        assert len(jobs) == 2
        assert "continuation_token" not in jobs

        print_debug("Check continuation_token with attributes")
        jobs_with_attr = list_jobs(op.id, attributes=["start_time"], limit=1, sort_field="start_time")
        assert len(jobs_with_attr["jobs"]) == 1
        assert jobs_with_attr["jobs"][0].keys() == {"id", "start_time"}

        jobs_with_attr = list_jobs(op.id, continuation_token=jobs_with_attr["continuation_token"])
        assert len(jobs_with_attr["jobs"]) == 2
        assert jobs_with_attr["jobs"][0].keys() == {"id", "start_time"}

    @authors("bystrovserg")
    def test_operation_incarnation(self):
        op = run_test_vanilla(
            with_breakpoint("BREAKPOINT"),
            job_count=2,
            task_patch={"gang_options": {}},
        )

        job_ids = wait_breakpoint(job_count=2)

        assert len(job_ids) == 2

        incarnation = self._get_operation_incarnation(op)

        wait(lambda: len(list_jobs(op.id, verbose=False)["jobs"]) == 2)

        jobs = list_jobs(op.id)["jobs"]

        wait(lambda: len(list_operation_events(op.id, event_type="incarnation_started")) == 1)
        event_before = list_operation_events(op.id, event_type="incarnation_started")[0]

        assert event_before["event_type"] == "incarnation_started"
        assert event_before.get("incarnation_switch_reason") is None

        assert frozenset(job["id"] for job in jobs) == frozenset(job_ids)
        assert all([job["operation_incarnation"] == incarnation for job in jobs])

        first_job_id = job_ids[0]

        print_debug("aborting job ", first_job_id)

        abort_job(first_job_id)

        job_orchid_addresses = {job_id: op.get_job_node_orchid_path(job_id) + f"/exec_node/job_controller/active_jobs/{job_id}" for job_id in job_ids}

        release_breakpoint(job_id=job_ids[0])
        wait(lambda: not exists(job_orchid_addresses[job_ids[0]]))

        release_breakpoint(job_id=job_ids[1])
        wait(lambda: not exists(job_orchid_addresses[job_ids[1]]))

        new_job_ids = wait_breakpoint(job_count=2)

        assert len(builtins.set(job_ids) & builtins.set(new_job_ids)) == 0

        new_incarnation = self._get_operation_incarnation(op)

        assert new_incarnation != incarnation

        release_breakpoint()

        op.track()

        wait(lambda: len(list_jobs(op.id, verbose=False)["jobs"]) == 4)

        jobs = list_jobs(op.id, operation_incarnation=incarnation)["jobs"]
        assert frozenset(job["id"] for job in jobs) == frozenset(job_ids)
        assert all([job["operation_incarnation"] == incarnation for job in jobs])

        jobs = list_jobs(op.id, operation_incarnation=new_incarnation)["jobs"]
        assert frozenset(job["id"] for job in jobs) == frozenset(new_job_ids)
        assert all([job["operation_incarnation"] == new_incarnation for job in jobs])

        @wait_no_assert
        def check_list_operation_events():
            wait(lambda: len(list_operation_events(op.id, event_type="incarnation_started")) == 2)
            events_after = list_operation_events(op.id, event_type="incarnation_started")
            assert all(event["event_type"] == "incarnation_started" for event in events_after)
            event_operation_started, event_job_aborted = events_after[0], events_after[1]
            assert event_operation_started.get("incarnation_switch_reason") is None
            assert event_job_aborted["incarnation_switch_reason"] == "job_aborted"
            assert event_operation_started["timestamp"] < event_job_aborted["timestamp"]
            assert event_operation_started["incarnation"] == incarnation
            assert event_job_aborted["incarnation"] == new_incarnation

            event_info_job_aborted = event_job_aborted["incarnation_switch_info"]
            event_info_operation_started = event_operation_started["incarnation_switch_info"]
            assert len(event_info_operation_started) == 0
            assert event_info_job_aborted["trigger_job_id"] == first_job_id
            assert event_info_job_aborted["abort_reason"] == "user_request"
            assert event_info_job_aborted["trigger_job_error"] is not None
            assert event_info_job_aborted["trigger_job_error"]["message"] is not None

            event_after = list_operation_events(op.id, event_type="incarnation_started", limit=1)
            assert len(event_after) == 1
            assert event_after[0]["incarnation"] == incarnation

    @authors("bystrovserg")
    def test_incarnation_events_on_success_revive(self):
        op = run_test_vanilla(
            with_breakpoint("BREAKPOINT"),
            job_count=1,
            task_patch={"gang_options": {}},
        )

        wait_breakpoint(job_count=1)

        op.wait_for_fresh_snapshot()

        wait(lambda: len(list_operation_events(op.id, event_type="incarnation_started")) == 1)

        with Restarter(self.Env, SCHEDULERS_SERVICE):
            pass

        wait_breakpoint(job_count=1)
        release_breakpoint()

        op.track()

        assert len(list_operation_events(op.id, event_type="incarnation_started")) == 1

    @authors("bystrovserg")
    def test_incarnation_events_on_job_lack_after_revival(self):
        total_cpu_limit = get("//sys/scheduler/orchid/scheduler/cluster/resource_limits/cpu")
        create_pool("test_pool", attributes={"strong_guarantee_resources": {"cpu": total_cpu_limit}})

        sleeping_op = run_sleeping_vanilla(spec={"pool": "test_pool"}, job_count=3)

        # Will not start jobs while sleeping_op is running.
        op = run_test_vanilla(
            with_breakpoint("BREAKPOINT"),
            job_count=3,
            task_patch={"gang_options": {}},
            spec={"pool": "fake_pool"},
        )

        first_incarnation_id = self._get_operation_incarnation(op)

        op.wait_for_fresh_snapshot()

        wait(lambda: len(list_operation_events(op.id)) == 1)

        with Restarter(self.Env, CONTROLLER_AGENTS_SERVICE):
            sleeping_op.abort()

        second_incarnation_id = self._get_operation_incarnation(op)

        print_debug(f"First incarnation id: {first_incarnation_id}, second incarnation id: {second_incarnation_id}")

        # Incarnation switch because there was no jobs for op
        assert first_incarnation_id != second_incarnation_id

        wait_breakpoint(job_count=3)
        release_breakpoint()

        op.track()

        wait(lambda: len(list_operation_events(op.id)) == 2)
        event = list_operation_events(op.id)[1]
        event["incarnation_switch_reason"] == "job_lack_after_revival"

    @authors("bystrovserg")
    def test_list_operation_events_empty_trigger_job_error(self):
        exit_code = 17

        command = f"""(trap "exit {exit_code}" SIGINT; BREAKPOINT)"""

        op = run_test_vanilla(
            command=with_breakpoint(command),
            job_count=3,
            task_patch={
                "interruption_signal": "SIGINT",
                "restart_exit_code": exit_code,
                "gang_options": {},
            },
        )
        first_job_ids = wait_breakpoint(job_count=3)
        job_id_to_interrupt = first_job_ids[0]

        interrupt_job(job_id_to_interrupt)

        wait(lambda: get_job(op.id, job_id_to_interrupt).get("interruption_info") is not None)

        for job_id in first_job_ids:
            release_breakpoint(job_id=job_id)

        wait_breakpoint(job_count=3)
        release_breakpoint()

        op.track()

        wait(lambda: len(list_operation_events(op.id, event_type="incarnation_started")) == 2)
        job_interrupted_incarnation = list_operation_events(op.id, event_type="incarnation_started")[1]
        assert job_interrupted_incarnation["incarnation_switch_reason"] == "job_interrupted"
        assert job_interrupted_incarnation["incarnation_switch_info"]["trigger_job_id"] == job_id_to_interrupt
        assert job_interrupted_incarnation["incarnation_switch_info"].get("trigger_job_error") is None

    @authors("bystrovserg")
    def test_list_operation_events_interruption_failed(self):
        command = """(trap "sleep 1000" SIGINT; BREAKPOINT)"""

        op = vanilla(
            track=False,
            spec={
                "tasks": {
                    "tasks_a": {
                        "job_count": 1,
                        "command": with_breakpoint(command),
                        "interruption_signal": "SIGINT",
                        "signal_root_process_only": True,
                        "restart_exit_code": 5,
                        "gang_options": {},
                    }
                },
                "max_failed_job_count": 1,
            },
        )

        (job_id,) = wait_breakpoint()

        interrupt_job(job_id, interrupt_timeout=2000)

        wait(lambda: len(list_operation_events(op.id)) == 2)
        event = list_operation_events(op.id)[1]
        assert event["incarnation_switch_reason"] == "job_aborted"
        assert event["incarnation_switch_info"]["trigger_job_id"] == job_id
        assert event["incarnation_switch_info"]["abort_reason"] == "interruption_timeout"

        assert event["incarnation_switch_info"].get("interruption_reason") is not None
        assert event["incarnation_switch_info"]["interruption_reason"] == "user_request"

        release_breakpoint()
        op.track()

    @authors("bystrovserg")
    def test_from_time_and_to_time_filters(self):
        def check_filter(expected_job_count, from_time=None, to_time=None):
            filters = {}
            if from_time is not None:
                filters["from_time"] = from_time
            if to_time is not None:
                filters["to_time"] = to_time
            wait(lambda: len(list_jobs(op.id, **filters)["jobs"]) == expected_job_count)

        start_time = datetime_to_string(utcnow())
        op = vanilla(
            track=False,
            spec={
                "tasks": {
                    "a": {
                        "job_count": 1,
                        "command": with_breakpoint("BREAKPOINT", breakpoint_name="a"),
                    },
                    "b": {
                        "job_count": 1,
                        "command": with_breakpoint("BREAKPOINT", breakpoint_name="b"),
                    },
                },
            },
        )

        wait_breakpoint(breakpoint_name="a", job_count=1)
        wait_breakpoint(breakpoint_name="b", job_count=1)

        # Both to_time and from_time filters use start_time for comparison.
        middle_time_before_breakpoint = datetime_to_string(utcnow())
        check_filter(2, from_time=start_time, to_time=middle_time_before_breakpoint)

        release_breakpoint(breakpoint_name="a")
        wait(lambda: len(list_jobs(op.id, state="completed")["jobs"]) == 1)

        check_filter(2, from_time=start_time, to_time=middle_time_before_breakpoint)

        middle_time_after_breakpoint = datetime_to_string(utcnow())
        check_filter(2, from_time=start_time, to_time=middle_time_after_breakpoint)

        release_breakpoint(breakpoint_name="b")

        op.track()
        end_time = datetime_to_string(utcnow())

        check_filter(2, from_time=start_time, to_time=end_time)
        check_filter(2, from_time=start_time, to_time=middle_time_after_breakpoint)

        check_filter(2, from_time=start_time)
        check_filter(0, from_time=end_time)

        check_filter(0, to_time=start_time)
        check_filter(2, to_time=middle_time_after_breakpoint)
        check_filter(2, to_time=end_time)

    @authors("bystrovserg")
    def test_with_interruption_info(self):
        create_pool("research")
        create_pool("prod", attributes={"strong_guarantee_resources": {"cpu": 3}})

        op1 = run_test_vanilla(with_breakpoint("BREAKPOINT"), spec={"pool": "research"})
        wait_breakpoint()

        op2 = run_sleeping_vanilla(spec={"pool": "prod"}, job_count=3)

        wait(lambda: op1.get_job_count(state="aborted", verbose=True) == 1)

        wait(lambda: len(list_jobs(op2.id, with_interruption_info=False)["jobs"]) == 3)
        wait(lambda: len(list_jobs(op1.id, with_interruption_info=True)["jobs"]) == 1)

    @authors("aleksandr.gaev")
    def test_job_addresses(self):
        op = run_test_vanilla(
            with_breakpoint("BREAKPOINT"),
        )
        (job_id,) = wait_breakpoint()

        def check_addresses(list_jobs_response):
            return len(list_jobs_response) == 1 and "address" in list_jobs_response[0] and "addresses" in list_jobs_response[0]

        wait(lambda: check_addresses(list_jobs(op.id, verbose=False)["jobs"]))

        release_breakpoint()

        op.track()

        jobs = list_jobs(op.id, verbose=False)["jobs"]
        assert check_addresses(jobs)
        assert len(jobs[0].get("address")) > 0
        assert len(jobs[0].get("addresses")) > 0
        assert jobs[0].get("addresses")["default"] == jobs[0].get("address")

    @authors("bystrovserg")
    def test_attributes_simple(self):
        op = run_test_vanilla(
            with_breakpoint("BREAKPOINT"),
            job_count=1,
        )
        (job_id,) = wait_breakpoint()

        def test_attributes(attr, expected_attr, is_optional=False):
            wait(lambda: len(list_jobs(op.id, attributes=attr)["jobs"]) == 1)
            jobs = list_jobs(op.id, attributes=attr)["jobs"]
            assert len(jobs) == 1
            if not is_optional:
                assert len(jobs[0]) == len(expected_attr)
            for attribute in jobs[0].keys():
                assert attribute in expected_attr
            if isinstance(expected_attr, dict):
                assert all(jobs[0].get(attribute) == value for attribute, value in expected_attr.items())

        # Test default attributes.
        test_attributes([], ["id"])

        test_attributes(["job_id", "type", "start_time"], ["id", "type", "start_time"])
        test_attributes(["state"], ["id", "state", "archive_state", "controller_state"], True)
        test_attributes(["operation_id"], {"id" : job_id, "operation_id" : op.id})

        release_breakpoint()
        op.track()

        test_attributes([], ["id"])
        test_attributes(["job_id", "type", "start_time"], ["id", "type", "start_time"])
        test_attributes(["state"], ["id", "state", "archive_state", "controller_state"])
        test_attributes(["state", "controller_state"], ["id", "state", "archive_state", "controller_state"])
        test_attributes(["operation_id"], {"id" : job_id, "operation_id" : op.id})
        test_attributes(["statistics", "brief_statistics"], ["id", "statistics", "brief_statistics"])

    @authors("bystrovserg")
    @flaky(max_runs=3)
    def test_controller_start_finish_time(self):
        op = run_test_vanilla(
            with_breakpoint("BREAKPOINT"),
        )
        (job_id,) = wait_breakpoint()

        def check_times(op_finished):
            jobs = list_jobs(op.id)["jobs"]
            assert len(jobs) == 1
            job_from_api = jobs[0]
            job_from_archive = get_job_from_archive(op.id, job_id)

            start_time_by_list_jobs = date_string_to_timestamp_mcs(job_from_api["start_time"])
            assert job_from_archive.get("controller_start_time") is not None
            assert start_time_by_list_jobs == job_from_archive["controller_start_time"]
            assert job_from_archive["start_time"] != job_from_archive["controller_start_time"]

            if op_finished:
                finish_time_by_list_jobs = date_string_to_timestamp_mcs(job_from_api["finish_time"])
                assert job_from_archive.get("controller_finish_time") is not None
                assert finish_time_by_list_jobs == job_from_archive["controller_finish_time"]
                assert job_from_archive["finish_time"] != job_from_archive["controller_finish_time"]

        wait_no_assert(lambda: check_times(op_finished=False))

        release_breakpoint()
        op.track()

        wait_no_assert(lambda: check_times(op_finished=True))

    # Before setting start_time and finish_time by the controller agent,
    # "list_jobs" returned the controller agent's times for running jobs
    # and the node's times for completed jobs. Test checks that these times stay the same now.
    @authors("bystrovserg")
    def test_same_time_for_running_and_completed_jobs(self):
        op = run_test_vanilla(
            with_breakpoint("BREAKPOINT"),
        )
        wait_breakpoint()

        wait(lambda: len(list_jobs(op.id)["jobs"]) == 1)
        running_jobs = list_jobs(op.id)["jobs"]

        release_breakpoint()
        op.track()

        wait(lambda: len(list_jobs(op.id)["jobs"]) == 1)
        completed_jobs = list_jobs(op.id)["jobs"]

        assert frozenset(job["start_time"] for job in running_jobs) == frozenset(job["start_time"] for job in completed_jobs)

    @authors("bystrovserg")
    def test_state_count_in_statistics(self):
        op = run_test_vanilla(
            with_breakpoint("BREAKPOINT"),
        )
        (job_id,) = wait_breakpoint()
        with Restarter(self.Env, NODES_SERVICE):
            pass

        release_breakpoint()
        op.track()

        wait_for_cells()

        @wait_no_assert
        def check_state_counts():
            assert len(list_jobs(op.id)["state_counts"]) == 2
            state_counts = list_jobs(op.id)["state_counts"]
            assert frozenset(state_counts.keys()) == frozenset(["aborted", "completed"])

    @authors("bystrovserg")
    def test_brief_statistics_without_full_statistics(self):
        if not self.ENABLE_RPC_PROXY:
            set("//sys/clusters/primary/request_full_statistics_for_brief_statistics_in_list_jobs", False)
        else:
            rpc_config = {
                "cluster_connection" : {
                    "request_full_statistics_for_brief_statistics_in_list_jobs": False
                }
            }
            set("//sys/rpc_proxies/@config", rpc_config)

        input_table, output_table = self._create_tables()
        op = map(
            track=False,
            in_=input_table,
            out=output_table,
            command="cat",
            spec={"job_count": 1},
        )
        op.track()

        wait(lambda: len(list_jobs(op.id)["jobs"]) == 1)
        job_id = list_jobs(op.id)["jobs"][0]["id"]

        print_debug("Clear job's statistics in archive")
        update_job_in_archive(op.id, job_id, {"statistics" : "", "statistics_lz4" : ""})

        @wait_no_assert
        def check_brief_statistics():
            jobs = list_jobs(op.id, attributes=["brief_statistics"])["jobs"]
            assert len(jobs) == 1
            job = jobs[0]
            assert job.get("brief_statistics") != {}

    @authors("bystrovserg")
    def test_stale_for_completed_jobs(self):
        op = run_test_vanilla(
            with_breakpoint("BREAKPOINT"),
            job_count=2,
        )
        jobs = wait_breakpoint()
        release_breakpoint(job_id=jobs[0])

        wait(lambda: get_job(op.id, jobs[0])["state"] == "completed")
        orchid_path = op.get_orchid_path()
        wait(lambda: len(get(orchid_path + "/retained_finished_jobs", default=1)) == 0)

        wait(lambda: len(list_jobs(op.id)["jobs"]) == 2)
        wait(lambda: all(not job["is_stale"] for job in list_jobs(op.id)["jobs"]))

        release_breakpoint()
        op.track()

    # TODO(bystrovserg): Do smth with copypaste of similar tests for get_job and list_jobs.
    @authors("bystrovserg")
    def test_gang_rank(self):
        op = run_test_vanilla(
            with_breakpoint("BREAKPOINT"),
            job_count=3,
            task_patch={
                "gang_options": {},
            },
        )

        wait_breakpoint(job_count=3)

        wait(lambda: len(op.get_running_jobs()) == 3)
        running_jobs = op.get_running_jobs()

        job_ranks = {job_id: info["gang_rank"] for job_id, info in running_jobs.items()}
        print_debug("Acquired job ranks from controller: {}".format(job_ranks))

        def check_job_ranks():
            jobs = list_jobs(op.id, attributes=["gang_rank"])["jobs"]
            wait(lambda: len(list_jobs(op.id, attributes=["gang_rank"])["jobs"]) == 3)
            wait(lambda: all(job.get("gang_rank") is not None for job in list_jobs(op.id, attributes=["gang_rank"])["jobs"]))
            jobs = list_jobs(op.id, attributes=["gang_rank"])["jobs"]

            job_ranks_api = {job["id"]: job["gang_rank"] for job in jobs}
            assert job_ranks_api == job_ranks

        check_job_ranks()

        release_breakpoint()
        op.track()

        check_job_ranks()

    @authors("bystrovserg")
    def test_monitoring_filter(self):
        update_controller_agent_config(
            "user_job_monitoring/extended_max_monitored_user_jobs_per_operation", 2)

        op = run_test_vanilla(
            with_breakpoint("BREAKPOINT"),
            job_count=2,
            task_patch={
                "monitoring": {
                    "enable": True,
                    "sensor_names": ["cpu/user"],
                },
            },
        )

        jobs_before = wait_breakpoint(job_count=2)
        wait(lambda: len(list_jobs(op.id)["jobs"]) == 2)

        jobs_with_monitoring_descriptor = list_jobs(op.id, with_monitoring_decriptor=True)["jobs"]
        assert len(jobs_with_monitoring_descriptor) == 2
        job_id1 = jobs_with_monitoring_descriptor[0]["id"]
        descriptor1 = jobs_with_monitoring_descriptor[0]["monitoring_descriptor"]
        job_id2 = jobs_with_monitoring_descriptor[1]["id"]
        descriptor2 = jobs_with_monitoring_descriptor[1]["monitoring_descriptor"]

        assert descriptor1 != descriptor2
        abort_job(job_id1)

        wait(lambda: len(op.list_jobs()) == 3)

        assert get_job(op.id, job_id2)["monitoring_descriptor"] == descriptor2

        new_job_ids = builtins.set(op.list_jobs()).difference(jobs_before)
        assert len(new_job_ids) == 1
        job_id3 = new_job_ids.pop()

        wait(lambda: "monitoring_descriptor" in get_job(op.id, job_id3))
        assert get_job(op.id, job_id3)["monitoring_descriptor"] == descriptor1

        assert len(op.list_jobs()) == 3
        assert frozenset([job["id"] for job in list_jobs(op.id, monitoring_descriptor=descriptor1)["jobs"]]) == frozenset([job_id1, job_id3])
        assert list_jobs(op.id, monitoring_descriptor=descriptor2)["jobs"][0]["id"] == job_id2

        # Wrong descriptor
        assert len(list_jobs(op.id, monitoring_descriptor="deadbeef")["jobs"]) == 0

        release_breakpoint()
        op.track()

        assert frozenset([job["id"] for job in list_jobs(op.id, monitoring_descriptor=descriptor1)["jobs"]]) == frozenset([job_id1, job_id3])
        assert list_jobs(op.id, monitoring_descriptor=descriptor2)["jobs"][0]["id"] == job_id2


class TestListJobsAllocation(TestListJobsBase):
    NUM_NODES = 1
    ENABLE_MULTIDAEMON = True

    DELTA_NODE_CONFIG = update(TestListJobsBase.DELTA_NODE_CONFIG, {
        "job_resource_manager": {
            "resource_limits": {
                "cpu": 1,
                "user_slots": 1,
            },
        },
    })

    DELTA_DYNAMIC_NODE_CONFIG = update(TestListJobsBase.DELTA_DYNAMIC_NODE_CONFIG, {
        "%true": {
            "exec_node": {
                "job_controller": {
                    "allocation": {
                        "enable_multiple_jobs": True,
                    },
                },
            },
        },
    })

    @authors("bystrovserg")
    def test_allocation_id(self):
        op = run_test_vanilla(
            with_breakpoint("BREAKPOINT"),
        )

        def check_allocation_id(job_id, include_archive=False):
            wait(lambda: len(list_jobs(op.id)["jobs"]) == 1 and "allocation_id" in list_jobs(op.id)["jobs"][0])
            job_from_list = list_jobs(op.id)["jobs"][0]
            if include_archive:
                job_allocation_id_from_archive = get_allocation_id_from_archive(op.id, job_id)
                assert job_from_list["allocation_id"] == job_allocation_id_from_archive
            assert job_from_list["allocation_id"] == get_allocation_id_from_job_id(job_id)

        job_ids = wait_breakpoint()
        assert len(job_ids) == 1
        check_allocation_id(job_ids[0], include_archive=False)

        release_breakpoint()

        op.track()
        check_allocation_id(job_ids[0], include_archive=True)

    @authors("bystrovserg")
    def test_same_allocation(self):
        create("table", "//tmp/t_in", attributes={"replication_factor": 1})
        create("table", "//tmp/t_out", attributes={"replication_factor": 1})

        write_table("//tmp/t_in", [{"foo": "bar"}] * 2)

        op = map(
            wait_for_jobs=True,
            track=False,
            command=with_breakpoint("BREAKPOINT ; cat"),
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={"data_size_per_job": 1, "enable_multiple_jobs_in_allocation": True},
        )

        job_ids = wait_breakpoint()
        assert len(job_ids) == 1

        job_id1 = job_ids[0]

        wait(lambda: len(checked_list_jobs(op.id)["jobs"]) == 1)
        assert "allocation_id" in checked_list_jobs(op.id)["jobs"][0]
        allocation_id_before = checked_list_jobs(op.id)["jobs"][0]["allocation_id"]

        release_breakpoint(job_id=job_id1)

        job_ids = wait_breakpoint()
        assert len(job_ids) == 1

        job_id2 = job_ids[0]

        assert job_id1 != job_id2

        wait(lambda: len(checked_list_jobs(op.id)["jobs"]) == 2)
        jobs_after = checked_list_jobs(op.id)["jobs"]
        assert "allocation_id" in jobs_after[0] and "allocation_id" in jobs_after[1]
        assert allocation_id_before == jobs_after[0]["allocation_id"] == jobs_after[1]["allocation_id"]

        release_breakpoint()

        op.track()

        wait(lambda: len(checked_list_jobs(op.id)["jobs"]) == 2)
        jobs_end = checked_list_jobs(op.id)["jobs"]
        assert "allocation_id" in jobs_end[0] and "allocation_id" in jobs_end[1]
        assert allocation_id_before == jobs_end[0]["allocation_id"] == jobs_end[1]["allocation_id"]


##################################################################


class TestListJobsRpcProxy(TestListJobs):
    ENABLE_MULTIDAEMON = False  # There are component restarts.
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True
    ENABLE_HTTP_PROXY = True


class TestListJobsStatisticsLz4RpcProxy(TestListJobsStatisticsLz4):
    ENABLE_MULTIDAEMON = False  # There are component restarts.
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True
    ENABLE_HTTP_PROXY = True


class TestListJobsAllocationdIdRpcProxy(TestListJobsAllocation):
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True
    ENABLE_HTTP_PROXY = True
