from yt_env_setup import YTEnvSetup, wait

from yt_commands import (
    authors, run_test_vanilla, with_breakpoint,
    wait_breakpoint, get, ls, release_breakpoint, exists,
    get_allocation_id_from_job_id, create, write_table, map,
    update_nodes_dynamic_config)


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

    @authors("arkady-e1ppa")
    def test_job_proxy_orchid_1(self):
        input_table = "//tmp/in"
        output_table = "//tmp/out"

        create(
            "table",
            input_table,
            attributes={
                "replication_factor": 1,  # NB: We have only 1 node
            })
        create(
            "table",
            output_table,
            attributes={
                "replication_factor": 1,
            })

        write_table(
            input_table,
            [{"foo": 1} , {"bar": 2}],
        )

        buffer_row_count = 42
        op = map(
            track=False,
            in_=input_table,
            out=output_table,
            command=with_breakpoint("""BREAKPOINT; cat"""),
            spec={
                "job_count": 1,
                "job_io": {
                    "buffer_row_count": buffer_row_count,
                },
                "max_failed_job_count": 1,
            },
        )

        (job_id, ) = wait_breakpoint()

        (node_id, ) = ls("//sys/cluster_nodes")

        orchid_prefix = f"//sys/cluster_nodes/{node_id}/orchid/exec_node/job_controller/active_jobs"
        orchid_suffix = "job_proxy/job_io/buffer_row_count"
        wait(lambda: job_id in get(orchid_prefix))

        wait(lambda: get(f"{orchid_prefix}/{job_id}/{orchid_suffix}") == buffer_row_count, ignore_exceptions=True)

        release_breakpoint()
        op.track()

    @authors("arkady-e1ppa")
    def test_job_proxy_orchid_2(self):
        input_table = "//tmp/in"
        output_table = "//tmp/out"

        create(
            "table",
            input_table,
            attributes={
                "replication_factor": 1,  # NB: We have only 1 node
            })
        create(
            "table",
            output_table,
            attributes={
                "replication_factor": 1,
            })

        row_count = 500
        write_table(
            input_table,
            [{"value": i} for i in range(1, row_count)],
        )

        update_nodes_dynamic_config(value=row_count * 100, path="exec_node/job_controller/job_proxy/pipe_reader_timeout_threshold")

        op = map(
            track=False,
            in_=input_table,
            out=output_table,
            command=with_breakpoint("""read row; echo $row; BREAKPOINT; cat"""),
            spec={
                "job_count": 1,
                "job_io": {
                    "buffer_row_count": 1,
                    "use_adaptive_buffer_row_count": True,
                },
                "max_failed_job_count": 1,
            },
        )

        (job_id, ) = wait_breakpoint()

        (node_id, ) = ls("//sys/cluster_nodes")

        orchid_prefix = f"//sys/cluster_nodes/{node_id}/orchid/exec_node/job_controller/active_jobs"
        orchid_suffix = "job_proxy/job_io/buffer_row_count"
        wait(lambda: job_id in get(orchid_prefix))

        wait(lambda: get(f"{orchid_prefix}/{job_id}/{orchid_suffix}") > 1, ignore_exceptions=True)

        release_breakpoint()
        op.track()


class TestJobResourceManagerOrchid(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_SCHEDULERS = 1
    NUM_NODES = 1

    # TODO(arkady-e1ppa): Make jobs request additional resources so that additional_resource_usage comparison is non-trivial
    @authors("arkady-e1ppa")
    def test_resource_manager_consistency(self):
        def jrm_resource_usage(node, allocation_id):
            resource_holder = get("//sys/cluster_nodes/{0}/orchid/exec_node/job_resource_manager/resource_holders/{1}".format(node, allocation_id))
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

        assert all([jrm_resource_usage(node, get_allocation_id_from_job_id(job_id)) == jc_resource_usage(node, job_id) for job_id in job_ids])

        release_breakpoint()

        op.track()
