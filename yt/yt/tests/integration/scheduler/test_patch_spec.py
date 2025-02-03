from yt_env_setup import YTEnvSetup

from yt_commands import (
    authors,
    raises_yt_error,
    run_sleeping_vanilla,
    patch_op_spec
)

##################################################################


class TestPatchSpec(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "fair_share_update_period": 100,
            "operations_update_period": 10,
        }
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {"controller_agent": {"snapshot_period": 1000}}

    @authors("coteeq")
    def test_basic(self):
        op = run_sleeping_vanilla()

        with raises_yt_error():
            patch_op_spec(
                op.id,
                patch={
                    "changes": [
                        {
                            "path": "/max_failed_job_count",
                            "value": 10,
                        }
                    ]
                }
            )
