from yt_env_setup import (
    YTEnvSetup,
    Restarter,
    CONTROLLER_AGENTS_SERVICE,
)

from yt_commands import (
    authors, run_test_vanilla,
    with_breakpoint, wait_breakpoint, release_breakpoint,
    get, exists, wait,
)

from datetime import datetime, timezone

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


##################################################################
