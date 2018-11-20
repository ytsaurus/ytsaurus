from yt_env_setup import YTEnvSetup, wait, require_ytserver_root_privileges
from yt_commands import *


SPEC_WITH_CPU_MONITOR = {
    "job_cpu_monitor": {
        "check_period": 10,
        "smoothing_factor": 0.2,
        "vote_window_size": 10,
        "vote_decision_threshold": 5,
        "min_cpu_limit": 0.1,
        "enable_cpu_reclaim": True
    }
}


@require_ytserver_root_privileges
class TestDynamicCpuReclaim(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_SCHEDULERS = 1
    NUM_NODES = 1

    DELTA_NODE_CONFIG = {
        "exec_agent": {
            "slot_manager": {
                "job_environment": {
                    "type": "cgroups",
                    "supported_cgroups": ["cpu", "cpuacct", "blkio"]
                }
            },
            "job_controller": {
                "resource_limits": {
                    "cpu": 1.5,
                    "user_slots": 2
                },
                "cpu_overdraft_timeout": 1000,
                "resource_adjustment_period": 1000
            }
        }
    }

    def test_dynamic_cpu_statistics(self):
        run_test_vanilla(with_breakpoint("BREAKPOINT; while true; do : ; done"), SPEC_WITH_CPU_MONITOR)
        job_id = wait_breakpoint()[0]
        stats_path = self.wait_and_get_stats_path(job_id)

        wait(lambda: get(stats_path + "/smoothed_cpu_usage_x100")["max"] <= 15)
        wait(lambda: get(stats_path + "/preemptable_cpu_x100")["max"] >= 85)

        release_breakpoint()

        wait(lambda: get(stats_path + "/smoothed_cpu_usage_x100")["max"] >= 85)
        wait(lambda: get(stats_path + "/preemptable_cpu_x100")["max"] == 0)

    def test_new_jobs_are_scheduled_on_reclaimed_cpu(self):
        # node has 1.5 cpu, min spare cpu to schedule new jobs is 1

        run_test_vanilla(with_breakpoint("BREAKPOINT", "Op1"), spec=SPEC_WITH_CPU_MONITOR)
        job_id1 = wait_breakpoint("Op1")[0]

        stats_path = self.wait_and_get_stats_path(job_id1)
        wait(lambda: get(stats_path + "/preemptable_cpu_x100")["max"] > 50)

        run_test_vanilla(with_breakpoint("BREAKPOINT", "Op2"))
        wait_breakpoint("Op2")

    def test_node_aborts_job_on_lack_of_cpu(self):
        op1 = run_test_vanilla(with_breakpoint("BREAKPOINT; while true; do : ; done", "Op1"), spec=SPEC_WITH_CPU_MONITOR)
        wait_breakpoint("Op1")

        op2 = run_test_vanilla("while true; do : ; done")
        wait(lambda: len(op2.get_running_jobs()) == 1)

        release_breakpoint("Op1")
        wait(lambda: len(op2.get_running_jobs()) == 0)
        wait(lambda: get(op2.get_path() + "/@progress/jobs/aborted/scheduled/resource_overdraft") == 1)
        assert len(op1.get_running_jobs()) == 1

    def wait_and_get_stats_path(self, job_id):
        node = ls("//sys/nodes")[0]
        result = "//sys/nodes/{0}/orchid/job_controller/active_jobs/scheduler/{1}/statistics/job_proxy".format(node, job_id)
        wait(lambda: exists(result))
        return result


@require_ytserver_root_privileges
class TestSchedulerAbortsJobOnLackOfCpu(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_SCHEDULERS = 1
    NUM_NODES = 1

    DELTA_NODE_CONFIG = {
        "exec_agent": {
            "slot_manager": {
                "job_environment": {
                    "type": "cgroups",
                    "supported_cgroups": ["cpu", "cpuacct", "blkio"]
                }
            },
            "job_controller": {
                "resource_limits": {
                    "cpu": 2.5,
                    "user_slots": 3
                }
            }
        }
    }

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "watchers_update_period": 100
        }
    }

    def test_scheduler_aborts_job_on_lack_of_cpu(self):
        set("//sys/pool_trees/default/@max_unpreemptable_running_job_count", 0)
        set("//sys/pool_trees/default/@preemptive_scheduling_backoff", 0)
        set("//sys/pool_trees/default/@aggressive_preemption_satisfaction_threshold", 0.1)
        set("//sys/pool_trees/default/@preemption_satisfaction_threshold", 0.1)
        time.sleep(0.2)

        op1 = run_test_vanilla(with_breakpoint("BREAKPOINT; while true; do : ; done", "Op1"), spec=SPEC_WITH_CPU_MONITOR)
        wait_breakpoint("Op1")

        op2 = run_test_vanilla("while true; do : ; done", spec={"weight": 0.001}, job_count=2)
        wait(lambda: len(op2.get_running_jobs()) == 2)

        release_breakpoint("Op1")

        wait(lambda: len(op2.get_running_jobs()) == 1)
        wait(lambda: get(op2.get_path() + "/@progress/jobs/aborted/scheduled/preemption") == 1)
        assert len(op1.get_running_jobs()) == 1


@require_ytserver_root_privileges
class TestNodeAbortsJobOnLackOfMemory(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_SCHEDULERS = 1
    NUM_NODES = 1

    DELTA_NODE_CONFIG = {
        "exec_agent": {
            "job_controller": {
                "resource_limits": {
                    "memory": 512 * 1024 * 1024
                },
                "resource_adjustment_period": 1000
            }
        }
    }

    def test_node_aborts_job_on_lack_of_memory(self):
        memory_consume_command = 'python -c "import time\ncount = 100*1000*1000\nx = list(range(count))\ntime.sleep(1000)"'
        op1 = run_test_vanilla(with_breakpoint("BREAKPOINT; " + memory_consume_command, "Op1"), spec={
            "vanilla": {
                "memory_limit": 400 * 1024 * 1024,
                "memory_reserve_factor": 0.5
            }
        })
        wait_breakpoint("Op1")

        op2 = run_test_vanilla(memory_consume_command, spec={
            "vanilla": {
                "memory_limit": 400 * 1024 * 1024,
                "memory_reserve_factor": 1
            }
        })
        wait(lambda: len(op2.get_running_jobs()) == 1)

        release_breakpoint("Op1")
        wait(lambda: get(op1.get_path() + "/@progress/jobs/aborted/scheduled/resource_overdraft") == 1)
