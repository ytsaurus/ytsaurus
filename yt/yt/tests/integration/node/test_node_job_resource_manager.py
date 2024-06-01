import time
from yt_env_setup import YTEnvSetup

from yt_commands import (
    authors, with_breakpoint, wait_breakpoint, release_breakpoint, run_test_vanilla,
    run_sleeping_vanilla, update_nodes_dynamic_config, ls, get, wait, abort_job)

from yt_helpers import JobCountProfiler, profiler_factory

from flaky import flaky


class TestUserMemoryUsageTracker(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_SCHEDULERS = 1
    NUM_NODES = 1

    @authors("arkady-e1ppa")
    def test_completed_job_resource_usage(self):
        node = ls("//sys/cluster_nodes")[0]
        profiler = profiler_factory().at_node(node)
        user_jobs_used = profiler \
            .counter(
                name="cluster_node/memory_usage/used",
                tags={'category': 'user_jobs'})

        op = run_test_vanilla(with_breakpoint("BREAKPOINT"))
        wait_breakpoint()

        wait(lambda: user_jobs_used.get_delta() > 0)

        release_breakpoint()
        op.track()

        wait(lambda: user_jobs_used.get_delta() == 0)

    @authors("arkady-e1ppa")
    def test_aborted_running_job_resource_usage(self):
        node = ls("//sys/cluster_nodes")[0]
        profiler = profiler_factory().at_node(node)
        user_jobs_used = profiler \
            .counter(
                name="cluster_node/memory_usage/used",
                tags={'category': 'user_jobs'})

        op = run_test_vanilla(with_breakpoint("BREAKPOINT"))
        (job_id, ) = wait_breakpoint()

        wait(lambda: user_jobs_used.get_delta() > 0)

        abort_job(job_id)
        release_breakpoint()
        op.track()

        wait(lambda: user_jobs_used.get_delta() == 0)

    @authors("arkady-e1ppa")
    @flaky(max_runs=5)
    def test_aborted_created_job_resource_usage(self):
        node = ls("//sys/cluster_nodes")[0]
        profiler = profiler_factory().at_node(node)
        user_jobs_used = profiler \
            .counter(
                name="cluster_node/memory_usage/used",
                tags={'category': 'user_jobs'})

        update_nodes_dynamic_config({
            "exec_node": {
                "job_controller": {
                    "test_resource_acquisition_delay": 10000,
                },
                "controller_agent_connector": {
                    "heartbeat_executor": {
                        "period": 2 * 10000,
                    }
                },
            },
        })

        def wait_for_job_to_finish(op):
            if op.get_job_count("aborted") > 0:
                return True

            return op.get_job_count("completed") > 0

        op = run_test_vanilla("echo 1 > null")
        wait(lambda: get("//sys/cluster_nodes/{}/orchid/exec_node/job_controller/active_job_count".format(node)) > 0)
        op.abort()
        wait(lambda: wait_for_job_to_finish(op), timeout=90)

        op.track(raise_on_aborted=False)

        wait(lambda: user_jobs_used.get_delta() == 0)


class TestMemoryPressureDetector(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_SCHEDULERS = 1
    NUM_NODES = 1

    FREE_MEMORY_WATERMARK = 1024**3

    DELTA_NODE_CONFIG = {
        "exec_node": {
            "slot_manager": {
                "job_environment": {
                    "type": "testing",
                    "testing_job_environment_scenario": "increasing_major_page_fault_count",
                },
            },
        }
    }

    DELTA_DYNAMIC_NODE_CONFIG = {
        "%true": {
            "job_resource_manager": {
                "free_memory_watermark": FREE_MEMORY_WATERMARK,
            }
        }
    }

    @authors("renadeen")
    def test_memory_pressure_detector_at_node(self):
        node = ls("//sys/cluster_nodes")[0]
        node_memory = "//sys/cluster_nodes/{}/orchid/exec_node/job_resource_manager/resource_limits/user_memory".format(node)

        memory_before_pressure = get(node_memory)

        update_nodes_dynamic_config({
            "job_resource_manager": {
                "memory_pressure_detector": {
                    "enabled": True,
                    "check_period": 1000,
                    "major_page_fault_count_threshold": 100,
                    "memory_watermark_multiplier_increase_step": 0.5,
                    "max_memory_watermark_multiplier": 5,
                }
            },
        })

        wait(lambda: get(node_memory) < memory_before_pressure - 3*self.FREE_MEMORY_WATERMARK)


class TestMemoryPressureAtJobProxy(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_SCHEDULERS = 1
    NUM_NODES = 3

    @authors("renadeen")
    def test_memory_pressure_detector_at_job_proxy(self):
        update_nodes_dynamic_config({
            "exec_node": {
                "job_controller": {
                    "job_proxy": {
                        "job_environment": {
                            "job_thrashing_detector": {
                                "enabled": True,
                                "check_period": 1000,
                                "major_page_fault_count_threshold": 100,
                                "limit_overflow_count_threshold_to_abort_job": 3,
                            },
                            "type": "testing",
                            "testing_job_environment_scenario": "increasing_major_page_fault_count",
                        }
                    }
                }
            },
        })

        time.sleep(1)
        aborted_job_profiler = JobCountProfiler("aborted", tags={"tree": "default", "job_type": "vanilla", "abort_reason": "job_memory_thrashing"})

        op = run_sleeping_vanilla()

        wait(lambda: op.get_job_count(state="aborted") > 0, timeout=10)
        wait(lambda: aborted_job_profiler.get_job_count_delta() == 1)
