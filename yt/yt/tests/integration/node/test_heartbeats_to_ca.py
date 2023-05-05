from yt_env_setup import (
    YTEnvSetup,
    Restarter,
    CONTROLLER_AGENTS_SERVICE,
)

from yt_commands import (
    authors, run_test_vanilla,
    with_breakpoint, wait_breakpoint, release_breakpoint
)

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

##################################################################
