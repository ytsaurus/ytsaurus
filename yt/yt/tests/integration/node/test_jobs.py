from yt_env_setup import (YTEnvSetup)

from yt_commands import (
    authors, ls, get, wait, run_test_vanilla, with_breakpoint,
    wait_breakpoint, release_breakpoint, vanilla,
    disable_scheduler_jobs_on_node, enable_scheduler_jobs_on_node,
    interrupt_job, update_nodes_dynamic_config, abort_job, run_sleeping_vanilla)

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
