from yt_env_setup import YTEnvSetup, wait

from yt_commands import (
    authors, run_test_vanilla, with_breakpoint, wait_breakpoint, get, ls, release_breakpoint)


class TestJobControllerOrchid(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_SCHEDULERS = 1
    NUM_NODES = 1

    @authors("pogorelov")
    def test_jobs_waiting_for_cleanup(self):
        op = run_test_vanilla(
            with_breakpoint("BREAKPOINT"),
            job_count=1,
            spec={"job_testing_options": {"delay_in_cleanup": 100000}},
        )

        job_id = wait_breakpoint()[0]

        node = ls("//sys/cluster_nodes")[0]

        release_breakpoint()

        wait(lambda: job_id in get("//sys/cluster_nodes/{}/orchid/job_controller/jobs_waiting_for_cleanup".format(node)))

        assert job_id not in get("//sys/cluster_nodes/{}/orchid/job_controller/active_jobs".format(node))

        op.track()
