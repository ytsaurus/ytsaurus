from yt_odin_checks.lib.check_runner import main
from yt_odin_checks.lib.scheduler_alerts import run_check_impl


def run_check(yt_client, logger, options, states):
    return run_check_impl(
        yt_client,
        logger,
        options,
        states,
        include_alert_types=("update_fair_share",),
        skip_pool_trees=(
            "physical",
            "gpu_geforce_1080ti",
            "gpu_tesla_k40",
            "gpu_tesla_m40",
            "gpu_tesla_p40",
            "gpu_tesla_v100",
            "gpu_tesla_v100_nvlink",
            "gpu_tesla_a100_80g_cloud",
            "gpu_a800_80g_cloud",
            "gpu_a800_cloud",
            "gpu_h100_80g_noib",
            "gpu_h100_80g_cloud",
            "gpu_tesla_a100",
            "gpu_tesla_a100_80g",
            "gpu_a800_80g",
            "gpu_h100_80g",
        ),
    )


if __name__ == "__main__":
    main(run_check)
