from yt_env_setup import YTEnvSetup
from yt_commands import *
from yt.test_helpers import wait

import yt.environment.init_operation_archive as init_operation_archive

from flaky import flaky

def get_sorted_jobs(op):
    jobs = []
    for id, job in op.get_running_jobs().iteritems():
        job["id"] = id
        jobs.append(job)

    return sorted(jobs, key=lambda job: job["start_time"])


class TestSpeculativeJobEngine(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    @authors("renadeen")
    def test_both_jobs_ends_simultaneously(self):
        op = self.run_vanilla_with_one_regular_and_one_speculative_job()

        assert get(op.get_path() + "/@brief_progress/jobs")["running"] == 2
        assert get(op.get_path() + "/@brief_progress/jobs")["aborted"] == 0
        assert get(op.get_path() + "/@brief_progress/jobs")["completed"] == 0
        assert get(op.get_path() + "/@brief_progress/jobs")["total"] == 2

        release_breakpoint()
        op.track()

        assert get(op.get_path() + "/@brief_progress/jobs")["running"] == 0
        assert get(op.get_path() + "/@brief_progress/jobs")["aborted"] == 1
        assert get(op.get_path() + "/@brief_progress/jobs")["completed"] == 1
        assert get(op.get_path() + "/@brief_progress/jobs")["total"] == 1

    @authors("renadeen")
    def test_original_faster_than_speculative(self):
        op = self.run_vanilla_with_one_regular_and_one_speculative_job()
        original, speculative = get_sorted_jobs(op)

        release_breakpoint(job_id=original["id"])
        op.track()

        job_counters = get(op.get_path() + "/@progress/jobs")
        assert job_counters["aborted"]["scheduled"]["speculative_run_lost"] == 1
        assert job_counters["aborted"]["scheduled"]["speculative_run_won"] == 0

    @authors("renadeen")
    def test_speculative_faster_than_original(self):
        op = self.run_vanilla_with_one_regular_and_one_speculative_job()
        original, speculative = get_sorted_jobs(op)

        release_breakpoint(job_id=speculative["id"])
        op.track()

        job_counters = get(op.get_path() + "/@progress/jobs")
        assert job_counters["aborted"]["scheduled"]["speculative_run_lost"] == 0
        assert job_counters["aborted"]["scheduled"]["speculative_run_won"] == 1

    @authors("renadeen")
    def test_speculative_job_fail_fails_whole_operation(self):
        op = self.run_vanilla_with_one_regular_and_one_speculative_job(
            command="BREAKPOINT;exit 1",
            spec={"max_failed_job_count": 1}
        )
        original, speculative = get_sorted_jobs(op)

        release_breakpoint(job_id=speculative["id"])
        op.track(raise_on_failed=False)
        assert op.get_state() == "failed"

        job_counters = get(op.get_path() + "/@progress/jobs")
        assert job_counters["pending"] == 0
        assert job_counters["failed"] == 1
        assert job_counters["total"] == 1

    @authors("renadeen")
    def test_speculative_job_fail_but_regular_job_continues(self):
        op = self.run_vanilla_with_one_regular_and_one_speculative_job(
            command="BREAKPOINT;exit 1",
            spec={"max_failed_job_count": 2}
        )
        original, speculative = get_sorted_jobs(op)

        release_breakpoint(job_id=speculative["id"])
        wait(lambda: get(op.get_path() + "/@progress/jobs")["running"] == 1)

        job_counters = get(op.get_path() + "/@progress/jobs")
        assert job_counters["pending"] == 0
        assert job_counters["failed"] == 1
        assert job_counters["total"] == 1
        op.abort()

    @authors("renadeen")
    def test_speculative_job_aborts_but_regular_job_succeeds(self):
        op = self.run_vanilla_with_one_regular_and_one_speculative_job()
        original, speculative = get_sorted_jobs(op)

        abort_job(speculative["id"])
        wait(lambda: get(op.get_path() + "/@brief_progress/jobs")["running"] == 1)

        job_counters = get(op.get_path() + "/@brief_progress/jobs")
        assert job_counters["pending"] == 0
        assert job_counters["aborted"] == 1
        assert job_counters["total"] == 1

        release_breakpoint()
        op.track()

        job_counters = get(op.get_path() + "/@brief_progress/jobs")
        assert job_counters["running"] == 0
        assert job_counters["aborted"] == 1
        assert job_counters["completed"] == 1
        assert job_counters["total"] == 1

    @authors("renadeen")
    def test_regular_job_aborts_but_speculative_job_succeeds(self):
        op = self.run_vanilla_with_one_regular_and_one_speculative_job()
        original, speculative = get_sorted_jobs(op)

        abort_job(original["id"])
        wait(lambda: get(op.get_path() + "/@brief_progress/jobs")["running"] == 1)

        job_counters = get(op.get_path() + "/@brief_progress/jobs")
        assert job_counters["pending"] == 0
        assert job_counters["aborted"] == 1
        assert job_counters["total"] == 1

        release_breakpoint()
        op.track()

        job_counters = get(op.get_path() + "/@brief_progress/jobs")
        assert job_counters["running"] == 0
        assert job_counters["aborted"] == 1
        assert job_counters["completed"] == 1
        assert job_counters["total"] == 1

    @authors("renadeen")
    def test_original_succeeds_but_speculative_fails_instead_of_abort(self):
        op = self.run_vanilla_with_one_regular_and_one_speculative_job(command='BREAKPOINT; if [ "$YT_JOB_INDEX" = "1" ]; then exit 1; fi;')
        original, speculative = get_sorted_jobs(op)

        release_breakpoint(job_id=original["id"])
        time.sleep(0.01)
        release_breakpoint(job_id=speculative["id"])
        op.track()

        assert op.get_state() == "completed"

    @authors("renadeen")
    def test_map_with_speculative_job(self):
        create_test_tables()
        op = map(
            command=with_breakpoint("BREAKPOINT; cat"),
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={"testing": {"testing_speculative_launch_mode": "always"}},
            track=False)
        wait_breakpoint(job_count=2)
        original, speculative = get_sorted_jobs(op)

        release_breakpoint(job_id=speculative["id"])
        op.track()

        assert get(op.get_path() + "/@brief_progress/jobs")["aborted"] == 1
        assert get(op.get_path() + "/@brief_progress/jobs")["completed"] == 1
        assert get(op.get_path() + "/@brief_progress/jobs")["total"] == 1
        assert read_table("//tmp/t_out") == [{"x": '0'}]

    def run_vanilla_with_one_regular_and_one_speculative_job(self, spec=None, command="BREAKPOINT", mode="always"):
        spec = spec if spec else {}
        spec["testing"] = {"testing_speculative_launch_mode": mode}
        op = run_test_vanilla(with_breakpoint(command), spec=spec, job_count=1)
        wait_breakpoint(job_count=2)
        wait(lambda: get(op.get_path() + "/@progress/jobs")["running"] == 2)
        return op

class TestSpeculativeJobSplitter(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 6
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "watchers_update_period": 100,
            "operations_update_period": 10,
            "running_jobs_update_period": 10,
        }
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "operations_update_period": 10,
            "map_operation_options": {
                "job_splitter": {
                    "min_job_time": 500,
                    "min_total_data_size": 1000 ** 3,  # makes jobs unsplittable
                    "update_period": 100,
                    "candidate_percentile": 0.8,
                    "max_jobs_per_split": 3,
                    "max_input_table_count": 5,
                    "exec_to_prepare_time_ratio": 1,
                    "split_timeout_before_speculate": 100,
                },
                "spec_template": {
                    "max_failed_job_count": 1
                }
            }
        }
    }

    DELTA_NODE_CONFIG = {
        "exec_agent": {
            "scheduler_connector": {
                "heartbeat_period": 100
            }
        }
    }

    ROW_COUNT_TO_FILL_PIPE = 1000000

    @authors("renadeen")
    def test_speculative_on_residual_job(self):
        op = self.run_op_with_residual_speculative_job()
        regular, speculative = get_sorted_jobs(op)

        release_breakpoint(job_id=speculative["id"])
        op.track()

    @authors("renadeen")
    def test_speculative_with_automerge(self):
        op = self.run_op_with_residual_speculative_job(spec={"auto_merge": {"mode": "relaxed"}})
        regular, speculative = get_sorted_jobs(op)

        release_breakpoint(job_id=speculative["id"])
        op.track()

    @authors("renadeen")
    @flaky(max_runs=3)
    def test_aborted_speculative_job_is_restarted(self):
        op = self.run_op_with_residual_speculative_job()
        regular, speculative = get_sorted_jobs(op)
        abort_job(speculative["id"])

        wait(lambda: get(op.get_path() + "/@brief_progress/jobs")["aborted"] == 1)
        wait(lambda: get(op.get_path() + "/@brief_progress/jobs")["running"] == 2)

        release_breakpoint()
        op.track()

    # TODO(renadeen): improve test
    @authors("renadeen")
    def test_three_speculative_jobs_for_three_regular(self):
        create_test_tables(row_count=2*self.ROW_COUNT_TO_FILL_PIPE)

        op = map(
            track=False,
            in_="//tmp/t_in",
            out="//tmp/t_out",
            command=with_breakpoint("BREAKPOINT; cat"),
            spec={
                "job_io": {"buffer_row_count": 1},
                "data_weight_per_job": 2*10**7
            }
        )
        wait_breakpoint(job_count=6)
        wait(lambda: get(op.get_path() + "/@brief_progress/jobs")["running"] == 6)
        assert get(op.get_path() + "/@brief_progress/jobs")["pending"] == 0

        release_breakpoint()
        op.track()

    @authors("renadeen")
    def test_max_speculative_job_count(self):
        create_test_tables(row_count=2*self.ROW_COUNT_TO_FILL_PIPE)

        op = map(
            track=False,
            in_="//tmp/t_in",
            out="//tmp/t_out",
            command=with_breakpoint("BREAKPOINT; cat"),
            spec={
                "job_io": {"buffer_row_count": 1},
                "data_weight_per_job": 2*10**7,
                "max_speculative_job_count_per_task": 1
            }
        )
        wait_breakpoint(job_count=4)
        wait(lambda: get(op.get_path() + "/@brief_progress/jobs")["running"] == 4)
        assert get(op.get_path() + "/@brief_progress/jobs")["pending"] == 0

        release_breakpoint()
        op.track()

    def run_op_with_residual_speculative_job(self, command="BREAKPOINT; cat", spec=None):
        spec = {} if spec is None else spec
        spec["job_io"] = {"buffer_row_count": 1}
        create_test_tables(row_count=self.ROW_COUNT_TO_FILL_PIPE)

        # Job is unslplittable since min_total_data_size is very large
        op = map(
            track=False,
            in_="//tmp/t_in",
            out="//tmp/t_out",
            command=with_breakpoint(command),
            spec=spec
        )
        wait_breakpoint(job_count=2)
        wait(lambda: get(op.get_path() + "/@brief_progress/jobs")["running"] == 2)

        return op


class TestListSpeculativeJobs(YTEnvSetup):
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
            },
            "job_controller": {
                "resource_limits": {
                    "user_slots": 4,
                    "cpu": 4.0
                }
            }
        },
        "scheduler_connector": {
            "heartbeat_period": 100,
        },
        "job_proxy_heartbeat_period": 100,
    }

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "watchers_update_period": 100,
            "operations_update_period": 10,
            "running_jobs_update_period": 10,
            "enable_job_reporter": True,
            "enable_job_spec_reporter": True,
            "static_orchid_cache_update_period": 100,
            "operations_cleaner": {
                "enable": False,
                "analysis_period": 100,
                # Cleanup all operations
                "hard_retained_operation_count": 0,
                "clean_delay": 0,
            },
        }
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "operations_update_period": 10,
            "controller_static_orchid_update_period": 100,
            "job_reporter": {
                "enabled": True,
                "reporting_period": 10,
                "min_repeat_delay": 10,
                "max_repeat_delay": 10,
            },
        },
    }

    def setup_method(self, method):
        super(TestListSpeculativeJobs, self).setup_method(method)
        sync_create_cells(1)
        init_operation_archive.create_tables_latest_version(
            self.Env.create_native_client(),
            override_tablet_cell_bundle="default",
        )

    @authors("renadeen")
    def test_list_speculative_jobs(self):
        def assert_list_jobs(op_id, data_source):
            all_jobs = list_jobs(op_id, data_source=data_source)["jobs"]
            assert len(all_jobs) == 4
            grouped_jobs = {}
            for job in all_jobs:
                job_competition_id = job["job_competition_id"]
                if job_competition_id not in grouped_jobs:
                    grouped_jobs[job_competition_id] = []
                grouped_jobs[job_competition_id].append(job["id"])

            assert len(grouped_jobs) == 2

            for job_competition_id in grouped_jobs:
                listed_jobs = list_jobs(op_id, data_source=data_source, job_competition_id=job_competition_id)["jobs"]
                listed_jobs = [job["id"] for job in listed_jobs]

                assert len(listed_jobs) == 2
                assert sorted(listed_jobs) == sorted(grouped_jobs[job_competition_id])

        spec = {
            "job_speculation_timeout": 100,
            "max_speculative_job_count_per_task": 2,
        }
        op = run_test_vanilla(with_breakpoint("BREAKPOINT"), spec=spec, job_count=2)
        wait_breakpoint(job_count=4)
        assert_list_jobs(op.id, "archive")
        assert_list_jobs(op.id, "runtime")

        release_breakpoint()
        op.track()
        assert_list_jobs(op.id, "archive")

    @authors("renadeen")
    def test_list_speculative_jobs_with_get_job(self):
        def assert_get_and_list_jobs(op_id, job_id):
            job = get_job(op_id, job_id)
            jobs = list_jobs(op_id, job_competition_id=job["job_competition_id"])["jobs"]
            assert len(jobs) == 2
            assert jobs[0]["job_competition_id"] == job["job_competition_id"]
            assert jobs[1]["job_competition_id"] == job["job_competition_id"]
            assert jobs[0]["id"] == job_id or jobs[1]["id"] == job_id

        spec = {
            "job_speculation_timeout": 100,
            "max_speculative_job_count_per_task": 2,
        }
        op = run_test_vanilla(with_breakpoint("BREAKPOINT"), spec=spec, job_count=2)
        some_job_id = wait_breakpoint(job_count=4)[0]
        assert_get_and_list_jobs(op.id, some_job_id)

        release_breakpoint()
        op.track()
        assert_get_and_list_jobs(op.id, some_job_id)

    @authors("renadeen")
    def test_with_competitors_flag_in_list_jobs(self):
        def assert_list_jobs(op_id, data_source):
            def group_jobs_by_competition():
                all_jobs = list_jobs(op_id, data_source=data_source)["jobs"]
                assert len(all_jobs) == 3
                assert len([j for j in all_jobs if j["has_competitors"]]) == 2
                result = {}
                for job in all_jobs:
                    competition_id = job["job_competition_id"]
                    if competition_id not in result:
                        result[competition_id] = []
                    result[competition_id].append(job["id"])
                return result

            grouped = group_jobs_by_competition()
            with_competitors = list_jobs(op_id, data_source=data_source, with_competitors=True)["jobs"]
            assert len(with_competitors) == 2
            first, second = with_competitors
            assert first["job_competition_id"] == second["job_competition_id"]
            assert sorted([first["id"], second["id"]]) == sorted(grouped[first["job_competition_id"]])

        spec = {
            "testing": {"testing_speculative_launch_mode": "once"},
            "max_speculative_job_count_per_task": 10,
        }

        op = run_test_vanilla(with_breakpoint("BREAKPOINT"), spec=spec, job_count=2)
        wait_breakpoint(job_count=3)
        assert_list_jobs(op.id, "runtime")

        release_breakpoint()
        op.track()
        assert_list_jobs(op.id, "archive")

    @authors("renadeen")
    def test_has_competitors_flag_when_speculative_lost(self):
        spec = {
            "testing": {"testing_speculative_launch_mode": "once"},
            "max_speculative_job_count_per_task": 10,
        }

        op = run_test_vanilla(with_breakpoint("BREAKPOINT"), spec=spec, job_count=2)
        wait_breakpoint(job_count=3)

        jobs = get_sorted_jobs(op)
        release_breakpoint(job_id=jobs[0]["id"])
        release_breakpoint(job_id=jobs[1]["id"])
        op.track()

        jobs = list_jobs(op.id, with_competitors=True)["jobs"]
        assert len(jobs) == 2
        assert jobs[0]["has_competitors"]
        assert jobs[1]["has_competitors"]


class TestSpeculativeJobsOther(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 1
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "watchers_update_period": 100,
            "operations_update_period": 10,
            "running_jobs_update_period": 10,
        }
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "operations_update_period": 10,
        }
    }

    DELTA_NODE_CONFIG = {
        "exec_agent": {
            "scheduler_connector": {
                "heartbeat_period": 100
            },
            "job_controller": {
                "resource_limits": {
                    "cpu": 3,
                    "user_slots": 3
                }
            }
        }
    }

    @authors("gritukan")
    def test_speculation_job_timeout(self):
        spec = {
            "enable_job_splitting": False,
            "job_speculation_timeout": 100
        }
        op = run_test_vanilla(with_breakpoint("BREAKPOINT"), spec=spec, job_count=1)

        wait_breakpoint(job_count=2)
        wait(lambda: get(op.get_path() + "/@brief_progress/jobs")["running"] == 2)
        assert get(op.get_path() + "/@brief_progress/jobs")["pending"] == 0

        release_breakpoint()
        op.track()

        job_counters = get(op.get_path() + "/@progress/jobs")
        assert job_counters["aborted"]["scheduled"]["speculative_run_lost"] > 0 or \
            job_counters["aborted"]["scheduled"]["speculative_run_won"] > 0

    @authors("renadeen")
    def test_speculative_for_speculative(self):
        spec = {
            "enable_job_splitting": False,
            "job_speculation_timeout": 100
        }
        op = run_test_vanilla(with_breakpoint("BREAKPOINT"), spec=spec, job_count=1)

        wait_breakpoint(job_count=2)
        original, speculative = get_sorted_jobs(op)

        abort_job(original["id"])
        wait(lambda: get(op.get_path() + "/@brief_progress/jobs")["aborted"] == 1)
        wait(lambda: get(op.get_path() + "/@brief_progress/jobs")["running"] == 2)

        release_breakpoint()
        op.track()

        job_counters = get(op.get_path() + "/@brief_progress/jobs")
        assert job_counters["running"] == 0
        assert job_counters["aborted"] == 2
        assert job_counters["completed"] == 1
        assert job_counters["total"] == 1
