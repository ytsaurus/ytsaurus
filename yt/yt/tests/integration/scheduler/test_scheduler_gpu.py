from yt_env_setup import (
    YTEnvSetup,
)

from yt_commands import (
    authors, wait, ls, get, set, create_data_center, create_rack, run_sleeping_vanilla, update_pool_tree_config,
    create_pool_tree, exists,
)

from yt_scheduler_helpers import (
    scheduler_orchid_path, scheduler_orchid_node_path,
    scheduler_new_orchid_pool_tree_path,
)


##################################################################


class TestDryRunGpuSchedulingPolicy(YTEnvSetup):
    ENABLE_MULTIDAEMON = False  # There are component restarts.
    NUM_MASTERS = 1
    NUM_NODES = 2
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "watchers_update_period": 100,
            "fair_share_update_period": 100,
            "fair_share_profiling_period": 100,
        }
    }

    DELTA_DYNAMIC_NODE_CONFIG = {
        "%true": {
            "exec_node": {
                "controller_agent_connector": {
                    "heartbeat_executor": {
                        "period": 500,
                    },
                },
            },
        },
    }

    DELTA_NODE_CONFIG = {
        "exec_node": {
            "gpu_manager": {
                "testing": {
                    "test_resource": True,
                    "test_gpu_count": 8
                },
            },
            "job_proxy": {
                "job_proxy_heartbeat_period": 100,
            },
        },
        "job_resource_manager": {
            "resource_limits": {
                "cpu": 10,
                "user_slots": 10,
            },
        }
    }

    DATA_CENTER = "SAS"
    RACK = "SAS1"

    def setup_method(self, method):
        super(TestDryRunGpuSchedulingPolicy, self).setup_method(method)

        update_pool_tree_config("default", {"nodes_filter": "!gpu"})
        create_pool_tree("gpu", config={
            "nodes_filter": "gpu",
            "main_resource": "gpu",
            "gpu_scheduling_policy": {
                "mode": "dry_run",
                "plan_update_period": 100,
                "module_type": "data_center",
                "modules": [TestDryRunGpuSchedulingPolicy.DATA_CENTER],
                "full_host_aggressive_preemption_timeout": 1000,
            },
            "preemptive_scheduling_backoff": 0,
            "fair_share_starvation_timeout": 100,
            "fair_share_starvation_tolerance": 0.95,
            "preemption_satisfaction_threshold": 0.99,
            "non_preemptible_resource_usage_threshold": {"user_slots": 0},
        })

        set("//sys/pool_trees/@default_tree", "gpu")
        wait(lambda: get(scheduler_orchid_path() + "/scheduler/default_pool_tree", default=None) == "gpu")

        create_data_center(TestDryRunGpuSchedulingPolicy.DATA_CENTER)
        create_rack(TestDryRunGpuSchedulingPolicy.RACK)
        set("//sys/racks/{}/@data_center".format(TestDryRunGpuSchedulingPolicy.RACK), TestDryRunGpuSchedulingPolicy.DATA_CENTER)
        for node in ls("//sys/cluster_nodes"):
            set("//sys/cluster_nodes/{}/@rack".format(node), TestDryRunGpuSchedulingPolicy.RACK)
            set("//sys/cluster_nodes/{}/@user_tags".format(node), ["gpu"])
        for node in ls("//sys/cluster_nodes"):
            wait(lambda: get(scheduler_orchid_node_path(node) + "/data_center") == TestDryRunGpuSchedulingPolicy.DATA_CENTER)

        wait(lambda: get(scheduler_new_orchid_pool_tree_path("gpu") + "/node_count") == TestDryRunGpuSchedulingPolicy.NUM_NODES)

    @authors("eshcherbin")
    def test_simple(self):
        op = run_sleeping_vanilla(
            task_patch={"gpu_limit": 1, "enable_gpu_layers": False},
        )

        wait(lambda: len(op.get_running_jobs()) == 1)

        wait(lambda: exists(scheduler_new_orchid_pool_tree_path("gpu") + "/gpu_assignment_plan"))
        wait(lambda: len(get(scheduler_new_orchid_pool_tree_path("gpu") + "/gpu_assignment_plan")) == 1)

        node = list(get(scheduler_new_orchid_pool_tree_path("gpu") + "/gpu_assignment_plan"))[0]
        wait(lambda: node in ls("//sys/cluster_nodes"))
        wait(lambda: len(get(scheduler_new_orchid_pool_tree_path("gpu") + f"/gpu_assignment_plan/{node}")) == 1)

        assignment = get(scheduler_new_orchid_pool_tree_path("gpu") + f"/gpu_assignment_plan/{node}")[0]
        assert assignment["operation_id"] == op.id
        assert assignment["allocation_group_name"] == "task"
        assert assignment["resource_usage"]["gpu"] == 1


##################################################################
