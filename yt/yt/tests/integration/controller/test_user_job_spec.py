from yt_env_setup import YTEnvSetup

from yt_commands import (
    authors, wait, get, exists,
    run_sleeping_vanilla, update_controller_agent_config,
)

from yt_scheduler_helpers import scheduler_orchid_operation_path


class TestOperationOptions(YTEnvSetup):
    ENABLE_MULTIDAEMON = True
    NUM_MASTERS = 1
    NUM_NODES = 1
    NUM_SCHEDULERS = 1

    @authors("ignat")
    def test_min_cpu_limit(self):
        update_controller_agent_config("vanilla_operation_options/min_cpu_limit", 0.5)

        op = run_sleeping_vanilla(
            job_count=1,
            task_patch={"cpu_limit": 0.3},
        )

        wait(lambda: exists(scheduler_orchid_operation_path(op.id)))
        wait(lambda: get(scheduler_orchid_operation_path(op.id) + "/resource_usage/cpu") == 0.5)

        wait(lambda: "specified_cpu_limit_is_too_small" in op.get_alerts())
