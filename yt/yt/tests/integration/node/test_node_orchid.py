from yt_env_setup import YTEnvSetup, wait

from yt_commands import (
    authors, run_test_vanilla, with_breakpoint, wait_breakpoint, get, ls, release_breakpoint, exists)


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

        wait(lambda: job_id in get("//sys/cluster_nodes/{}/orchid/exec_node/job_controller/jobs_waiting_for_cleanup".format(node)))

        assert job_id not in get("//sys/cluster_nodes/{}/orchid/exec_node/job_controller/active_jobs".format(node))

        assert not exists("//sys/cluster_nodes/{0}/orchid/data_node/job_controller/active_jobs/{1}".format(node, job_id))

        op.track()


class TestJobResourceManagerOrchid(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_SCHEDULERS = 1
    NUM_NODES = 1

    # TODO(arkady-e1ppa): Make jobs request additional resources so that additional_resource_usage comparison is non-trivial
    @authors("arkady-e1ppa")
    def test_resource_manager_consistency(self):
        def jrm_resource_usage(node, job_id):
            resource_holder = get("//sys/cluster_nodes/{0}/orchid/exec_node/job_resource_manager/resource_holders/{1}".format(node, job_id))
            return (
                resource_holder["base_resource_usage"],
                resource_holder["additional_resource_usage"],
            )

        def jc_resource_usage(node, job_id):
            job_info = get("//sys/cluster_nodes/{0}/orchid/exec_node/job_controller/active_jobs/{1}".format(node, job_id))
            return (
                job_info["base_resource_usage"],
                job_info["additional_resource_usage"],
            )

        op = run_test_vanilla(
            with_breakpoint("BREAKPOINT"),
            job_count=2,
        )

        job_ids = wait_breakpoint()

        node = ls("//sys/cluster_nodes")[0]

        assert all([jrm_resource_usage(node, job_id) == jc_resource_usage(node, job_id) for job_id in job_ids])

        release_breakpoint()

        op.track()
