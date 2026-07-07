from yt_env_setup import (
    YTEnvSetup,
    Restarter,
    CONTROLLER_AGENTS_SERVICE,
)

from yt_commands import (
    authors, wait_breakpoint, with_breakpoint,
    run_test_vanilla, update_controller_agent_config, raises_yt_error)


class TestJobProbingDuringRevival(YTEnvSetup):
    ENABLE_MULTIDAEMON = False  # There are component restarts.
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    @authors("pogorelov")
    def test_interruption_during_revival(self):
        op = run_test_vanilla(with_breakpoint("BREAKPOINT"))

        (job_id, ) = wait_breakpoint()

        update_controller_agent_config("testing_options/delay_in_handshake", 5000)

        with Restarter(self.Env, CONTROLLER_AGENTS_SERVICE):
            pass

        with raises_yt_error("Job cannot be interrupted as it is in aborted state|Allocation .* not found|no child with key .*"):
            op.interrupt_job(job_id)

        op.abort()
