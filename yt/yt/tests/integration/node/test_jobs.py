from yt_env_setup import (YTEnvSetup)

from yt_commands import (
    authors, ls, wait, run_test_vanilla, with_breakpoint,
    wait_breakpoint, release_breakpoint,
    disable_scheduler_jobs_on_node, enable_scheduler_jobs_on_node)

from yt_helpers import JobCountProfiler

##################################################################


class TestJobs(YTEnvSetup):
    NUM_NODES = 1
    NUM_SCHEDULERS = 1

    @authors("pogorelov")
    def test_uninterruptible_jobs(self):
        aborted_job_profiler = JobCountProfiler(
            "aborted", tags={"tree": "default", "job_type": "vanilla", "abort_reason": "interruption_unsupported"})

        op = run_test_vanilla(with_breakpoint("BREAKPOINT"), job_count=1)

        wait_breakpoint()

        nodes = ls("//sys/cluster_nodes")
        assert len(nodes) == 1
        disable_scheduler_jobs_on_node(nodes[0], "test")

        wait(lambda: aborted_job_profiler.get_job_count_delta() == 1)

        release_breakpoint()

        enable_scheduler_jobs_on_node(nodes[0])

        op.track()
