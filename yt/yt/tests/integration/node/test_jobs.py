from yt_env_setup import (YTEnvSetup)

from yt_commands import (
    authors, ls, get, set, create, write_table, wait, run_test_vanilla,
    with_breakpoint, wait_breakpoint, release_breakpoint, vanilla, map,
    disable_scheduler_jobs_on_node, enable_scheduler_jobs_on_node,
    interrupt_job, update_nodes_dynamic_config, abort_job, run_sleeping_vanilla,
    set_node_banned, extract_statistic_v2, get_allocation_id_from_job_id)

from yt_helpers import JobCountProfiler, profiler_factory


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

    @authors("pogorelov")
    def test_interruption_timeout(self):
        aborted_job_profiler = JobCountProfiler(
            "aborted", tags={"tree": "default", "job_type": "vanilla", "abort_reason": "interruption_timeout"})

        command = """(trap "sleep 1000" SIGINT; BREAKPOINT)"""

        op = vanilla(
            track=False,
            spec={
                "tasks": {
                    "tasks_a": {
                        "job_count": 1,
                        "command": with_breakpoint(command),
                        "interruption_signal": "SIGINT",
                        "signal_root_process_only": True,
                        "restart_exit_code": 5,
                    }
                },
                "max_failed_job_count": 1,
            },
        )

        (job_id,) = wait_breakpoint()

        interrupt_job(job_id, interrupt_timeout=2000)

        wait(lambda: aborted_job_profiler.get_job_count_delta() == 1)

        release_breakpoint()

        op.track()

    @authors("arkady-e1ppa")
    def test_job_proxy_exit_profiling(self):
        update_nodes_dynamic_config(value=True, path="/exec_node/job_controller/profile_job_proxy_process_exit")
        nodes = ls("//sys/cluster_nodes")
        profilers = [profiler_factory().at_node(node) for node in nodes]
        exit_ok = [profiler.counter("job_controller/job_proxy_process_exit/zero_exit_code") for profiler in profilers]

        op = run_test_vanilla("echo 1> null", job_count=3)

        op.wait_for_state("completed")

        wait(lambda: any([counter.get_delta() > 0 for counter in exit_ok]))

        op.track()


@authors("khlebnikov")
class TestJobsCri(TestJobs):
    JOB_ENVIRONMENT_TYPE = "cri"


class TestJobsDisabled(YTEnvSetup):
    NUM_NODES = 1
    NUM_SCHEDULERS = 1

    DELTA_NODE_CONFIG = {
        "job_resource_manager": {
            "resource_limits": {
                "cpu": 100,
                "user_slots": 100,
            },
        }
    }

    def _get_op_job(self, op):
        wait(lambda: op.list_jobs())

        job_ids = op.list_jobs()
        assert len(job_ids) == 1
        return job_ids[0]

    @authors("pogorelov")
    def test_job_abort_on_fatal_alert(self):
        node_address = ls("//sys/cluster_nodes")[0]
        assert not get("//sys/cluster_nodes/{}/@alerts".format(node_address))

        aborted_job_profiler = JobCountProfiler(
            "aborted", tags={"tree": "default", "job_type": "vanilla", "abort_reason": "node_with_disabled_jobs"})

        update_nodes_dynamic_config({
            "exec_node": {
                "job_controller": {
                    "job_common": {
                        "waiting_for_job_cleanup_timeout": 500,
                    }
                }

            },
        })

        op1 = run_sleeping_vanilla(
            spec={"job_testing_options": {"delay_in_cleanup": 1000}, "sanity_check_delay": 60 * 1000},
        )

        job_id_1 = self._get_op_job(op1)

        op2 = run_sleeping_vanilla()

        abort_job(job_id_1)

        wait(lambda: aborted_job_profiler.get_job_count_delta() >= 1)

        op1.abort()
        op2.abort()


class TestNodeBanned(YTEnvSetup):
    NUM_NODES = 1
    NUM_SCHEDULERS = 1

    DELTA_NODE_CONFIG = {
        "job_resource_manager": {
            "resource_limits": {
                "cpu": 1,
                "user_slots": 1,
            },
        }
    }

    @authors("pogorelov")
    def test_job_abort_on_node_banned(self):
        nodes = ls("//sys/cluster_nodes")
        assert len(nodes) == 1
        node = nodes[0]

        path = "job_controller/job_final_state"
        counter = profiler_factory().at_node(node, fixed_tags={"state": "aborted"}).counter(path)

        op = run_test_vanilla(with_breakpoint("BREAKPOINT"))

        wait_breakpoint()

        set_node_banned(node, True)

        wait(lambda: counter.get_delta(verbose=True) != 0)

        op.abort()


class TestJobProxyCallFailed(YTEnvSetup):
    NUM_NODES = 1
    NUM_SCHEDULERS = 1

    DELTA_DYNAMIC_NODE_CONFIG = {
        "%true": {
            "exec_node": {
                "job_controller": {
                    "job_proxy": {
                        "testing_config": {
                            "fail_on_job_proxy_spawned_call": True,
                        },
                    },
                },
            },
        }
    }

    @authors("pogorelov")
    def test_on_job_proxy_spawned_call_fail(self):
        aborted_job_profiler = JobCountProfiler(
            "aborted", tags={"tree": "default", "job_type": "vanilla", "abort_reason": "other"})

        op = run_test_vanilla("sleep 1", job_count=1)

        wait(lambda: aborted_job_profiler.get_job_count_delta() >= 1)

        update_nodes_dynamic_config({
            "exec_node": {
                "job_controller": {
                    "job_proxy": {
                        "testing_config": {
                            "fail_on_job_proxy_spawned_call": False,
                        },
                    },
                },
            },
        })

        op.track()


class TestJobStatistics(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    @authors("ngc224")
    def test_io_statistics(self):
        create("table", "//tmp/t_input")
        write_table("//tmp/t_input", {"foo": "bar"})

        create("table", "//tmp/t_output")

        op = map(
            command="cat",
            in_="//tmp/t_input",
            out="//tmp/t_output",
            track=True,
        )

        statistics = op.get_statistics()
        chunk_reader_statistics = statistics["chunk_reader_statistics"]

        assert extract_statistic_v2(chunk_reader_statistics, "data_bytes_read_from_disk") > 0
        assert extract_statistic_v2(chunk_reader_statistics, "data_io_requests") > 0
        assert extract_statistic_v2(chunk_reader_statistics, "meta_bytes_read_from_disk", summary_type="count") > 0
        assert extract_statistic_v2(chunk_reader_statistics, "meta_io_requests", summary_type="count") > 0

    @authors("artemagafonov")
    def test_vanilla_statistics(self):
        op = run_test_vanilla("true", track=True)

        statistics = op.get_statistics()
        pipes_statistic = statistics["user_job"]["pipes"]

        assert "data" not in statistics or ("data" in statistics and "input" not in statistics["data"])
        assert "input" not in pipes_statistic
        assert "output" in pipes_statistic


class TestAllocationWithTwoJobs(YTEnvSetup):
    NUM_NODES = 1
    NUM_SCHEDULERS = 1

    DELTA_NODE_CONFIG = {
        "job_resource_manager": {
            "resource_limits": {
                "cpu": 1,
                "user_slots": 1,
            },
        }
    }

    DELTA_DYNAMIC_NODE_CONFIG = {
        "%true": {
            "exec_node": {
                "job_controller": {
                    "allocation": {
                        "enable_multiple_jobs": True,
                    },
                },
            },
        }
    }

    @authors("pogorelov")
    def test_simple(self):
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")

        set("//tmp/t_in" + "/@replication_factor", 1)
        set("//tmp/t_out" + "/@replication_factor", 1)

        write_table("//tmp/t_in", [{"foo": "bar"}] * 2)

        op = map(
            wait_for_jobs=True,
            track=False,
            command=with_breakpoint("BREAKPOINT ; cat"),
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={"data_size_per_job": 1},
        )

        job_ids = wait_breakpoint()
        assert len(job_ids) == 1

        job_id1 = job_ids[0]

        allocation_id1 = get_allocation_id_from_job_id(job_id1)

        release_breakpoint(job_id=job_id1)

        job_ids = wait_breakpoint()
        assert len(job_ids) == 1

        job_id2 = job_ids[0]

        assert job_id1 != job_id2

        allocation_id2 = get_allocation_id_from_job_id(job_id2)

        assert allocation_id1 == allocation_id2

        release_breakpoint()

        op.track()
