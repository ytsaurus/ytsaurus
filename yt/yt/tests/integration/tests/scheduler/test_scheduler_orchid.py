from yt_env_setup import YTEnvSetup

from yt_commands import (
    authors, run_sleeping_vanilla, wait, get, set, exists, ls,
    create_pool, create_pool_tree)

from yt_scheduler_helpers import (
    scheduler_orchid_operations_by_pool_path, scheduler_orchid_operation_path,
    scheduler_orchid_operation_by_pool_path, scheduler_new_orchid_pool_tree_path, scheduler_orchid_pool_tree_path)

import builtins

##################################################################


class TestSchedulerOperationsByPoolOrchid(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "fair_share_update_period": 100,
        },
    }

    DELTA_NODE_CONFIG = {
        "exec_agent": {
            "scheduler_connector": {"heartbeat_period": 100},  # 100 msec
            "controller_agent_connector": {"heartbeat_period": 100},  # 100 msec
        },
    }

    @authors("pogorelov")
    def test_pool_tree_orchid(self):
        create_pool_tree("pool_tree", config={"nodes_filter": "tag"})
        wait(
            lambda: exists(scheduler_new_orchid_pool_tree_path("pool_tree")))

    @authors("pogorelov")
    def test_pool(self):
        create_pool("parent_pool")
        create_pool("child_pool", parent_name="parent_pool")
        wait(lambda: exists(scheduler_orchid_operations_by_pool_path("parent_pool", "default")))
        wait(lambda: exists(scheduler_orchid_operations_by_pool_path("child_pool", "default")))

    @authors("pogorelov")
    def test_operations(self):
        create_pool("pool")
        op1 = run_sleeping_vanilla(spec={"pool": "pool"}, job_count=1)
        op2 = run_sleeping_vanilla(spec={"pool": "pool"}, job_count=1)

        pool_path = scheduler_orchid_operations_by_pool_path("pool")
        wait(lambda: len(ls(pool_path)) == 2)

        assert(builtins.set([op1.id, op2.id]) == builtins.set(ls(pool_path)))

        wait(lambda: exists(scheduler_orchid_operation_path(op1.id)))

        wait(lambda: get(scheduler_orchid_operation_by_pool_path(op1.id, "pool")) == get(scheduler_orchid_operation_path(op1.id)))

        wait(lambda: exists(scheduler_orchid_operation_path(op2.id)))

        wait(lambda: get(scheduler_orchid_operation_by_pool_path(op2.id, "pool")) == get(scheduler_orchid_operation_path(op2.id)))

    @authors("pogorelov")
    def test_pools(self):
        create_pool("pool")
        create_pool("child", parent_name="pool", attributes={"weight": 3.0})

        wait(lambda: get(
            scheduler_new_orchid_pool_tree_path("default") + "/pools") == get(
                scheduler_orchid_pool_tree_path("default") + "/pools"))

        wait(lambda: get(
            scheduler_new_orchid_pool_tree_path("default") + "/pool_count") == get(
                scheduler_orchid_pool_tree_path("default") + "/pool_count"))

    @authors("pogorelov")
    def test_resource_distribution_info(self):
        set("//sys/pool_trees/default/@config/main_resource", "cpu")
        create_pool("pool", attributes={"strong_guarantee_resources": {"cpu": 72}})

        wait(lambda: get(
            scheduler_new_orchid_pool_tree_path("default") + "/resource_distribution_info") == get(
                scheduler_orchid_pool_tree_path("default") + "/resource_distribution_info"))
