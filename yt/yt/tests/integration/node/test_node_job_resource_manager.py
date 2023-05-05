from yt_env_setup import YTEnvSetup

from yt_commands import (
    authors, update_nodes_dynamic_config, ls, get, wait)


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
    def test_memory_pressure_detector(self):
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
