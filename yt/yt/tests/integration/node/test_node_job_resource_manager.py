import time
from yt_env_setup import YTEnvSetup

from yt_commands import (
    authors, run_sleeping_vanilla, update_nodes_dynamic_config, ls, get, wait)

from yt_helpers import JobCountProfiler


class TestMemoryPressureDetector(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_SCHEDULERS = 1
    NUM_NODES = 1

    FREE_MEMORY_WATERMARK = 1024**3

    DELTA_NODE_CONFIG = {
        "exec_agent": {
            "job_controller": {
                "free_memory_watermark": FREE_MEMORY_WATERMARK,
            },
            "slot_manager": {
                "job_environment": {
                    "type": "testing",
                    "testing_job_environment_scenario": "increasing_major_page_fault_count",
                },
            },
        }
    }

    @authors("renadeen")
    def test_memory_pressure_detector_at_node(self):
        node = ls("//sys/cluster_nodes")[0]
        node_memory = "//sys/cluster_nodes/{}/orchid/job_controller/resource_limits/user_memory".format(node)

        memory_before_pressure = get(node_memory)

        update_nodes_dynamic_config({
            "exec_agent": {
                "job_controller": {
                    "memory_pressure_detector": {
                        "enabled": True,
                        "check_period": 1000,
                        "major_page_fault_count_threshold": 100,
                        "memory_watermark_multiplier_increase_step": 0.5,
                        "max_memory_watermark_multiplier": 5,
                    }
                },
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
            "exec_agent": {
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
