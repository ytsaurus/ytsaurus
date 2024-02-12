import datetime
import time

from yt_env_setup import (
    YTEnvSetup,
    Restarter,
    CONTROLLER_AGENTS_SERVICE,
    SCHEDULERS_SERVICE,
)

from yt_helpers import create_custom_pool_tree_with_one_node

from yt_commands import (
    abort_job, authors, create_test_tables, wait, wait_breakpoint, release_breakpoint, with_breakpoint, get, ls,
    run_test_vanilla, map, map_reduce, set_node_banned)

from yt.common import YtError


def get_sorted_jobs(op):
    jobs = []
    for id, job in op.get_running_jobs().items():
        job["id"] = id
        jobs.append(job)

    return sorted(jobs, key=lambda job: job["start_time"])


class TestCrashOnSchedulingJobWithStaleNeededResources(YTEnvSetup):
    # Scenario:
    # 1. an operation with one job has started, demand = needed_resources = {default_tree: 1 job, cloud_tree: 0 jobs}
    # 2. the scheduler schedules one job, demand = usage = {default_tree: 1 job, cloud_tree: 0 jobs}
    # 3. the agent launches the job and adds probing to cloud_tree, so on agent needed_resources = {default_tree: 0 job, cloud_tree: 1 jobs}
    # 4. heartbeat period between scheduler and agent is big so needed resources are preserved for a while
    # 5. after fair share update demand is recalculated: demand = usage + needed_resources = {default_tree: 2 jobs, cloud_tree: 0 jobs}
    # 6. the scheduler tries to schedule second job
    # 7. the controller checks that needed_resources are not trivial and proceeds
    # 8. the controller doesn't find any cookies in chunk pool and it decides that it is speculative job
    # 9. there is no speculative candidates to launch -> CRASH

    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "scheduler_heartbeat_period": 3000,
        }
    }

    @authors("renadeen")
    def test_crash_on_scheduling_job_with_stale_needed_resources(self):
        create_custom_pool_tree_with_one_node("cloud_tree")

        op = run_test_vanilla(with_breakpoint("BREAKPOINT"), spec={
            "probing_ratio": 1,
            "probing_pool_tree": "cloud_tree",
        }, job_count=1)

        wait_breakpoint(job_count=1, timeout=datetime.timedelta(seconds=15))
        release_breakpoint()
        op.track()


class TestCrashOnLostProbingJobResult(YTEnvSetup):
    # YT-17172
    # Scenario:
    # 1. map reduce operation is started
    # 2. one original and one probing map jobs are scheduled
    # 3. the probing job completes, the original hangs, first reduce job is launched
    # 4. ban node with intermediate chunk
    # 5. first reduce job completes
    # 6. second reduce job fails to start due to unavailable chunk, map job is restarted
    # 7. probing job manager sees original map job running and another non-probing job with the same cookie being scheduled
    # 8. CRASH

    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "map_reduce_operation_options": {
                "min_uncompressed_block_size": 1,
            },
        }
    }

    @authors("renadeen")
    def test_lost_probing_jobs(self):
        create_custom_pool_tree_with_one_node("cloud_tree")

        create_test_tables(row_count=10)

        op = map_reduce(
            in_="//tmp/t_in",
            out="//tmp/t_out",
            reduce_by="x",
            sort_by="x",
            mapper_command='cat; if [ "$YT_JOB_INDEX" == "0" ]; then sleep 1000; fi',
            reducer_command=with_breakpoint("cat;BREAKPOINT"),
            spec={
                "probing_ratio": 1,
                "probing_pool_tree": "cloud_tree",
                "intermediate_data_replication_factor": 1,
                "sort_job_io": {"table_reader": {"retry_count": 1, "pass_count": 1}},
            },
            track=False,
        )

        wait_breakpoint()

        self.ban_nodes_with_intermediate_chunks()

        job_id = self.get_reducer_job(op)
        abort_job(job_id)

        release_breakpoint()
        op.track()

    def get_reducer_job(self, op):
        jobs = op.get_running_jobs()

        for job_id, job in jobs.items():
            if job["job_type"] == "partition_reduce" and not job["probing"]:
                return job_id

        raise Exception("Reducer not found in {}".format(list(jobs)))

    def ban_nodes_with_intermediate_chunks(self):
        chunks = ls("//sys/chunks", attributes=["staging_transaction_id"])
        intermediate_chunk_ids = []
        for c in chunks:
            if "staging_transaction_id" in c.attributes:
                tx_id = c.attributes["staging_transaction_id"]
                try:
                    if 'Scheduler "output" transaction' in get("#{}/@title".format(tx_id)):
                        intermediate_chunk_ids.append(str(c))
                except YtError:
                    # Transaction may vanish
                    pass

        assert len(intermediate_chunk_ids) == 1
        intermediate_chunk_id = intermediate_chunk_ids[0]

        replicas = get("#{}/@stored_replicas".format(intermediate_chunk_id))
        assert len(replicas) == 1
        node_id = replicas[0]

        set_node_banned(node_id, True)

        wait(lambda: get("//sys/scheduler/orchid/scheduler/nodes/{}/master_state".format(node_id)) == "offline")

        return [node_id]


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
    def test_simple_probing_map(self):
        create_custom_pool_tree_with_one_node("cloud_tree")

        create_test_tables()
        op = map(
            command=with_breakpoint("BREAKPOINT;cat"),
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={
                "probing_ratio": 1,
                "probing_pool_tree": "cloud_tree",
            },
            track=False,
        )
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

    @authors("renadeen")
    def test_request_speculative_and_probing_but_agent_launches_only_one_of_them(self):
        create_custom_pool_tree_with_one_node("cloud_tree")

        op = run_test_vanilla(with_breakpoint("BREAKPOINT"), spec={
            "probing_ratio": 1,
            "probing_pool_tree": "cloud_tree",
            "testing": {"testing_speculative_launch_mode": "always"}
        }, job_count=1)
        wait_breakpoint(job_count=2, timeout=datetime.timedelta(seconds=15))
        wait(lambda: get(op.get_path() + "/@brief_progress/jobs")["running"] == 2)

        time.sleep(2)
        assert get(op.get_path() + "/@brief_progress/jobs")["running"] == 2
        # We don't care whether probing or speculative were launched.

        release_breakpoint()
        op.track()

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
