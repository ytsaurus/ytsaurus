from yt_env_setup import (
    YTEnvSetup,
    Restarter,
    CONTROLLER_AGENTS_SERVICE,
)

from yt_commands import (
    authors, run_test_vanilla,
    with_breakpoint, wait_breakpoint, release_breakpoint,
    get, exists, wait, print_debug,
    update_controller_agent_config, ls,
)

from yt_helpers import profiler_factory, JobCountProfiler

from yt.common import YtResponseError

from datetime import datetime, timezone

import pytest


##################################################################


class TestNodeHeartbeatsToCA(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 1
    NUM_SCHEDULERS = 1

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "testing_options": {
                "delay_in_handshake": 2000,
            },
        },
    }

    @authors("pogorelov")
    def test_delay_in_handshake(self):
        op = run_test_vanilla(with_breakpoint("BREAKPOINT"), job_count=1)

        wait_breakpoint()

        with Restarter(self.Env, CONTROLLER_AGENTS_SERVICE):
            pass

        release_breakpoint()

        op.track()


class TestJobStored(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 2
    NUM_SCHEDULERS = 1

    @authors("pogorelov")
    def test_job_resend_backoff_start_time(self):
        op = run_test_vanilla(with_breakpoint("BREAKPOINT"), job_count=2)

        job_id1, job_id2 = wait_breakpoint(job_count=2)

        job_orchid_path = op.get_job_node_orchid_path(job_id1) + f"/exec_node/job_controller/active_jobs/{job_id1}"

        assert not exists(f"{job_orchid_path}/job_resend_backoff_start_time")

        release_breakpoint(job_id=job_id1)

        wait(lambda: exists(f"{job_orchid_path}/job_resend_backoff_start_time"))

        backoff_time = get(f"{job_orchid_path}/job_resend_backoff_start_time")
        assert backoff_time is not None

        now = datetime.now(timezone.utc)
        backoff_datetime = datetime.fromisoformat(backoff_time.replace('Z', '+00:00'))
        assert now >= backoff_datetime

        release_breakpoint()
        op.track()

    @authors("pogorelov")
    def test_job_resend_backoff_start_time_after_ca_restart(self):
        op = run_test_vanilla(with_breakpoint("BREAKPOINT"), job_count=2)

        job_id1, job_id2 = wait_breakpoint(job_count=2)

        job_orchid_path = op.get_job_node_orchid_path(job_id1) + f"/exec_node/job_controller/active_jobs/{job_id1}"

        assert not exists(f"{job_orchid_path}/job_resend_backoff_start_time")

        release_breakpoint(job_id=job_id1)

        wait(lambda: exists(f"{job_orchid_path}/job_resend_backoff_start_time"))

        initial_time = get(f"{job_orchid_path}/job_resend_backoff_start_time")
        assert initial_time is not None

        with Restarter(self.Env, CONTROLLER_AGENTS_SERVICE):
            pass

        # Wait for the time to be updated after CA restart
        wait(lambda: get(f"{job_orchid_path}/job_resend_backoff_start_time") > initial_time)

        release_breakpoint()
        op.track()


class TestOrphanedJob(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 2
    NUM_SCHEDULERS = 1

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "snapshot_period": 3000,
        },
    }

    DELTA_DYNAMIC_NODE_CONFIG = {
        "%true": {
            "exec_node": {
                "scheduler_connector": {
                    "request_new_agent_delay": 4000,
                },
            },
        },
    }

    @authors("pogorelov")
    @pytest.mark.parametrize("finished", [True, False])
    def test_orphaned_job(self, finished):
        op = run_test_vanilla(with_breakpoint("BREAKPOINT"), job_count=1)

        job_id, = wait_breakpoint(job_count=1)

        op.wait_for_fresh_snapshot()

        print_debug(f"Job is: {job_id}")

        orchid_path = op.get_job_node_orchid_path(job_id) + f"/exec_node/job_controller/active_jobs/{job_id}"

        wait(lambda: exists(orchid_path))

        with Restarter(self.Env, CONTROLLER_AGENTS_SERVICE):
            update_controller_agent_config("job_tracker/node_disconnection_timeout", 1, wait_for_orchid=False)
            update_controller_agent_config("job_tracker/revival_node_disconnection_timeout", 1, wait_for_orchid=False)

        agent_addresses = ls("//sys/controller_agents/instances")
        assert len(agent_addresses) == 1
        profiler = profiler_factory().at_controller_agent(agent_addresses[0])
        node_disconnection_counter = profiler.counter("controller_agent/job_tracker/node_unregistration_count")
        aborted_job_counter = JobCountProfiler(
            "aborted",
            tags={"tree": "default", "job_type": "vanilla", "abort_reason": "node_offline"},
        )

        wait(lambda: node_disconnection_counter.get() >= 2)
        # NB(pogorelov): In the beginning aborted_job_counter may return 0, and sum() will raise an exception.
        wait(lambda: aborted_job_counter.get() >= 1, ignore_exceptions=True)

        if finished:
            print_debug(f"Waiting for job {job_id} to finish")
            release_breakpoint(job_id=job_id)

            def check():
                try:
                    return get(f"{orchid_path}/job_state") == "completed"
                except YtResponseError:
                    # return not exists(orchid_path)
                    # NB(pogorelov): We increase request_new_agent_delay, so such case should be rare.
                    # We could uncomment line above but test will be less precise.
                    assert False

            wait(check)

        wait(lambda: not exists(orchid_path))

        update_controller_agent_config("job_tracker/node_disconnection_timeout", 1000)
        update_controller_agent_config("job_tracker/revival_node_disconnection_timeout", 1000)

        release_breakpoint()

        op.track()

##################################################################
