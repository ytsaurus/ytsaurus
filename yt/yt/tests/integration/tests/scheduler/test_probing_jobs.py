import datetime
from yt_env_setup import (
    YTEnvSetup,
    Restarter,
    CONTROLLER_AGENTS_SERVICE,
    SCHEDULERS_SERVICE,
)

from yt_helpers import create_custom_pool_tree_with_one_node

from yt_commands import (
    abort_job, authors, wait, wait_breakpoint, release_breakpoint, with_breakpoint, get,
    run_test_vanilla)


def get_sorted_jobs(op):
    jobs = []
    for id, job in op.get_running_jobs().items():
        job["id"] = id
        jobs.append(job)

    return sorted(jobs, key=lambda job: job["start_time"])


class TestProbingJobs(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "snapshot_period": 500,
            "operations_update_period": 100,
        }
    }

    DELTA_NODE_CONFIG = {
        "exec_agent": {
            "job_controller": {
                "total_confirmation_period": 5000,
            }
        }
    }

    @authors("renadeen")
    def test_simple_probing(self):
        create_custom_pool_tree_with_one_node("cloud_tree")

        op = run_test_vanilla(with_breakpoint("BREAKPOINT"), spec={
            "probing_ratio": 1,
            "probing_pool_tree": "cloud_tree",
        }, job_count=1)
        wait_breakpoint(job_count=2, timeout=datetime.timedelta(seconds=15))
        wait(lambda: get(op.get_path() + "/@brief_progress/jobs")["running"] == 2)

        counters = self.wait_for_running_and_get_counters(op, 2)
        assert counters["running"] == 2
        assert counters["aborted"] == 0
        assert counters["completed"] == 0
        assert counters["total"] == 2

        release_breakpoint()
        op.track()

        counters = get(op.get_path() + "/@brief_progress/jobs")
        assert counters["running"] == 0
        assert counters["aborted"] == 1
        assert counters["completed"] == 1
        assert counters["total"] == 1

    @authors("renadeen")
    def test_revive_probing_job(self):
        create_custom_pool_tree_with_one_node("cloud_tree")

        op, primary_job, probing_job = self.run_vanilla_with_probing_job()

        counters = self.wait_for_running_and_get_counters(op, 2)
        assert counters["running"] == 2
        assert counters["aborted"] == 0
        assert counters["completed"] == 0
        assert counters["total"] == 2

        with Restarter(self.Env, SCHEDULERS_SERVICE):
            pass

        counters = self.wait_for_running_and_get_counters(op, 2)
        assert counters["running"] == 2
        assert counters["aborted"] == 0
        assert counters["completed"] == 0
        assert counters["total"] == 2

        with Restarter(self.Env, CONTROLLER_AGENTS_SERVICE):
            pass

        release_breakpoint()
        op.track()

        counters = self.wait_for_running_and_get_counters(op, 0)
        assert counters["aborted"] == 1
        assert counters["completed"] == 1
        assert counters["total"] == 1

    @authors("renadeen")
    def test_main_complete_then_probing_complete(self):
        create_custom_pool_tree_with_one_node("cloud_tree")

        op, primary_job, probing_job = self.run_vanilla_with_probing_job()

        release_breakpoint(job_id=primary_job)

        wait(lambda: get(op.get_path() + "/@brief_progress/jobs")["completed"] == 1)
        assert get(op.get_path() + "/@brief_progress/jobs")["running"] == 1
        assert self.get_job_attribute(op, probing_job, "state") == "running"

        release_breakpoint(job_id=probing_job)

        op.track()

        counters = get(op.get_path() + "/@brief_progress/jobs")
        assert counters["running"] == 0
        assert counters["aborted"] == 1
        assert counters["completed"] == 1
        assert counters["total"] == 1

    @authors("renadeen")
    def test_main_complete_then_probing_abort(self):
        create_custom_pool_tree_with_one_node("cloud_tree")

        op, primary_job, probing_job = self.run_vanilla_with_probing_job()

        release_breakpoint(job_id=primary_job)

        wait(lambda: get(op.get_path() + "/@brief_progress/jobs")["completed"] == 1)
        counters = get(op.get_path() + "/@brief_progress/jobs")
        assert counters["running"] == 1
        assert counters["aborted"] == 0
        assert self.get_job_attribute(op, probing_job, "state") == "running"

        abort_job(probing_job)

        op.track()

        counters = get(op.get_path() + "/@brief_progress/jobs")
        assert counters["running"] == 0
        assert counters["aborted"] == 1
        assert counters["completed"] == 1
        assert counters["total"] == 1

    @authors("renadeen")
    def test_main_aborted_then_probing_complete(self):
        create_custom_pool_tree_with_one_node("cloud_tree")

        op, primary_job, probing_job = self.run_vanilla_with_probing_job()

        abort_job(job_id=primary_job)
        # Abort of main job leads to abort of probing, then main restarts and probing is spawned once again.

        wait(lambda: get(op.get_path() + "/@brief_progress/jobs")["aborted"] == 2)
        wait(lambda: get(op.get_path() + "/@brief_progress/jobs")["running"] == 2)
        assert get(op.get_path() + "/@brief_progress/jobs")["completed"] == 0

        release_breakpoint()

        op.track()

        counters = get(op.get_path() + "/@brief_progress/jobs")
        assert counters["running"] == 0
        assert counters["aborted"] == 3
        assert counters["completed"] == 1
        assert counters["total"] == 1

    @authors("renadeen")
    def test_probing_completed_then_main_complete(self):
        create_custom_pool_tree_with_one_node("cloud_tree")

        op, primary_job, probing_job = self.run_vanilla_with_probing_job()

        release_breakpoint(job_id=probing_job)

        wait(lambda: get(op.get_path() + "/@brief_progress/jobs")["completed"] == 1)
        assert get(op.get_path() + "/@brief_progress/jobs")["running"] == 1
        assert self.get_job_attribute(op, primary_job, "state") == "running"

        release_breakpoint(job_id=primary_job)

        op.track()

        counters = get(op.get_path() + "/@brief_progress/jobs")
        assert counters["running"] == 0
        assert counters["aborted"] == 1
        assert counters["completed"] == 1
        assert counters["total"] == 1

    @authors("renadeen")
    def test_probing_complete_then_main_abort(self):
        create_custom_pool_tree_with_one_node("cloud_tree")

        op, primary_job, probing_job = self.run_vanilla_with_probing_job()

        release_breakpoint(job_id=probing_job)

        wait(lambda: get(op.get_path() + "/@brief_progress/jobs")["completed"] == 1)
        counters = get(op.get_path() + "/@brief_progress/jobs")
        assert counters["running"] == 1
        assert counters["aborted"] == 0
        assert self.get_job_attribute(op, primary_job, "state") == "running"

        abort_job(primary_job)

        op.track()

        counters = get(op.get_path() + "/@brief_progress/jobs")
        assert counters["running"] == 0
        assert counters["aborted"] == 1
        assert counters["completed"] == 1
        assert counters["total"] == 1

    @authors("renadeen")
    def test_probing_aborted_then_main_complete(self):
        create_custom_pool_tree_with_one_node("cloud_tree")

        op, primary_job, probing_job = self.run_vanilla_with_probing_job()

        abort_job(job_id=probing_job)
        # Abort of probing job doesn't lead to abort of main.

        wait(lambda: get(op.get_path() + "/@brief_progress/jobs")["aborted"] == 1)
        counters = get(op.get_path() + "/@brief_progress/jobs")
        assert counters["running"] == 1
        assert counters["completed"] == 0
        assert self.get_job_attribute(op, primary_job, "state") == "running"

        release_breakpoint()

        op.track()

        counters = get(op.get_path() + "/@brief_progress/jobs")
        assert counters["running"] == 0
        assert counters["aborted"] == 1
        assert counters["completed"] == 1
        assert counters["total"] == 1

    @authors("renadeen")
    def test_probing_aborted_then_main_aborted(self):
        create_custom_pool_tree_with_one_node("cloud_tree")

        op, primary_job, probing_job = self.run_vanilla_with_probing_job()

        abort_job(job_id=probing_job)
        # Abort of probing job doesn't lead to abort of main.

        wait(lambda: get(op.get_path() + "/@brief_progress/jobs")["aborted"] == 1)
        counters = get(op.get_path() + "/@brief_progress/jobs")
        assert counters["running"] == 1
        assert counters["completed"] == 0
        assert self.get_job_attribute(op, primary_job, "state") == "running"

        abort_job(job_id=primary_job)
        # Abort of primary job leads to restart of primary and probing job.

        wait(lambda: get(op.get_path() + "/@brief_progress/jobs")["aborted"] == 2)
        wait(lambda: get(op.get_path() + "/@brief_progress/jobs")["running"] == 2)
        assert get(op.get_path() + "/@brief_progress/jobs")["completed"] == 0

        release_breakpoint()
        op.track()

        counters = get(op.get_path() + "/@brief_progress/jobs")
        assert counters["running"] == 0
        assert counters["aborted"] == 3
        assert counters["completed"] == 1
        assert counters["total"] == 1

    def run_vanilla_with_probing_job(self):
        op = run_test_vanilla(with_breakpoint("BREAKPOINT"), spec={
            "probing_ratio": 1,
            "probing_pool_tree": "cloud_tree",
        }, job_count=1)

        job_ids = wait_breakpoint(job_count=2, timeout=datetime.timedelta(seconds=15))
        primary_job, probing_job = self.get_primary_and_probing_job(op, job_ids)
        return op, primary_job, probing_job

    def get_primary_and_probing_job(self, op, job_ids):
        first_is_probing = self.get_job_attribute(op, job_ids[0], "probing")
        second_is_probing = self.get_job_attribute(op, job_ids[1], "probing")
        if first_is_probing:
            if second_is_probing:
                raise Exception("Both jobs cannot be probing")
            return job_ids[1], job_ids[0]

        if not second_is_probing:
            raise Exception("One of the jobs must be probing")

        return job_ids[0], job_ids[1]

    def get_job_attribute(self, op, job_id, attribute):
        return get(op.get_path() + "/controller_orchid/running_jobs/{}/{}".format(job_id, attribute))

    def wait_for_running_and_get_counters(self, op, expected_running):
        wait(lambda: get(op.get_path() + "/@brief_progress/jobs")["running"] == expected_running)
        return get(op.get_path() + "/@brief_progress/jobs")
