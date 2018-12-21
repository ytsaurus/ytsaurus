from yt_env_setup import YTEnvSetup, wait
from yt.test_helpers import are_almost_equal
from yt_commands import *

import pytest


class TestRuntimeParameters(YTEnvSetup):

    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "fair_share_update_period": 100,
            "operations_update_period": 10,
        }
    }

    def test_update_owners(self):
        create_user("u")
        create_test_tables()
        op = map(
            command="sleep 100",
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={"weight": 5},
            dont_track=True)
        wait(lambda: op.get_state() == "running", iter=10)

        assert check_permission("u", "write", op.get_path())["action"] == "deny"
        update_op_parameters(op.id, parameters={"owners": ["u"]})
        assert check_permission("u", "write", op.get_path())["action"] == "allow"

        update_op_parameters(op.id, parameters={"owners": ["missing_user"]})
        wait(lambda: op.get_alerts())
        assert op.get_alerts().keys() == ["invalid_acl"]

        self.Env.kill_schedulers()
        self.Env.start_schedulers()
        time.sleep(0.1)

        assert op.get_alerts().keys() == ["invalid_acl"]

        update_op_parameters(op.id, parameters={"owners": []})
        wait(lambda: not op.get_alerts())

    def test_update_runtime_parameters(self):
        create_test_tables()

        op = map(
            command="sleep 100",
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={"weight": 5},
            dont_track=True)
        wait(lambda: op.get_state() == "running", iter=10)

        progress_path = "//sys/scheduler/orchid/scheduler/operations/{0}/progress/scheduling_info_per_pool_tree/default".format(op.id)
        assert get(progress_path + "/weight") == 5.0

        update_op_parameters(op.id, parameters={
            "scheduling_options_per_pool_tree": {
                "default": {
                    "weight": 3.0,
                    "resource_limits": {
                        "user_slots": 0
                    }
                }
            }
        })

        default_tree_parameters_path = op.get_path() + "/@runtime_parameters/scheduling_options_per_pool_tree/default"

        assert are_almost_equal(get(default_tree_parameters_path + "/weight"), 3.0)
        assert get(default_tree_parameters_path + "/resource_limits/user_slots") == 0

        assert are_almost_equal(get(progress_path + "/weight"), 3.0)
        # wait() is essential since resource limits are copied from runtime parameters only during fair-share update.
        wait(lambda: get(progress_path + "/resource_limits")["user_slots"] == 0, iter=5)

        self.Env.kill_schedulers()
        self.Env.start_schedulers()

        wait(lambda: op.get_state() == "running", iter=10)

        assert are_almost_equal(get(progress_path + "/weight"), 3.0)
        # wait() is essential since resource limits are copied from runtime parameters only during fair-share update.
        wait(lambda: get(progress_path + "/resource_limits")["user_slots"] == 0, iter=5)

    @pytest.mark.xfail(run=False, reason="YT-9226")
    def test_update_pool_default_pooltree(self):
        create("map_node", "//sys/pools/initial_pool")
        create("map_node", "//sys/pools/changed_pool")

        create_test_tables()

        op = map(
            command="sleep 100",
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={"pool": "initial_pool"},
            dont_track=True)

        wait(lambda: op.get_state() == "running", iter=10)

        update_op_parameters(op.id, parameters={"pool": "changed_pool"})

        path = "//sys/scheduler/orchid/scheduler/operations/{0}/progress/scheduling_info_per_pool_tree/default/pool".format(op.id)
        assert get(path) == "changed_pool"

    @pytest.mark.xfail(run=False, reason="YT-9226")
    def test_running_operation_counts_on_update_pool(self):
        create("map_node", "//sys/pools/initial_pool")
        create("map_node", "//sys/pools/changed_pool")

        create_test_tables()

        op = map(
            command="sleep 100",
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={"pool": "initial_pool"},
            dont_track=True)

        wait(lambda: op.get_state() == "running", iter=10)

        pools_path = "//sys/scheduler/orchid/scheduler/pools/"
        wait(lambda: get(pools_path + "initial_pool/running_operation_count") == 1)
        wait(lambda: get(pools_path + "changed_pool/running_operation_count") == 0)

        update_op_parameters(op.id, parameters={"pool": "changed_pool"})

        wait(lambda: get(pools_path + "initial_pool/running_operation_count") == 0)
        wait(lambda: get(pools_path + "changed_pool/running_operation_count") == 1)

    @pytest.mark.xfail(run=False, reason="YT-9226")
    def test_update_pool_of_multitree_operation(self):
        self.create_custom_pool_tree_with_one_node(pool_tree="custom")
        create("map_node", "//sys/pools/default_pool")
        create("map_node", "//sys/pool_trees/custom/custom_pool1")
        create("map_node", "//sys/pool_trees/custom/custom_pool2")
        create_test_tables()

        op = map(
            command="sleep 100",
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={
                "pool_trees": ["default", "custom"],
                "scheduling_options_per_pool_tree": {
                    "default": {"pool": "default_pool"},
                    "custom": {"pool": "custom_pool1"}
                }
            },
            dont_track=True)

        wait(lambda: op.get_state() == "running", iter=10)

        update_op_parameters(op.id, parameters={"scheduling_options_per_pool_tree": {"custom": {"pool": "custom_pool2"}}})

        path = "//sys/scheduler/orchid/scheduler/operations/{0}/progress/scheduling_info_per_pool_tree/custom/pool".format(op.id)
        assert get(path) == "custom_pool2"

    def create_custom_pool_tree_with_one_node(self, pool_tree):
        tag = pool_tree
        node = ls("//sys/nodes")[0]
        set("//sys/nodes/" + node + "/@user_tags/end", tag)
        create("map_node", "//sys/pool_trees/" + pool_tree, attributes={"nodes_filter": tag})
        set("//sys/pool_trees/default/@nodes_filter", "!" + tag)
        return node


class TestJobsAreScheduledAfterPoolChange(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 1
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "fair_share_update_period": 100,
            "operations_update_period": 10,
            "pool_change_is_allowed": True
        }
    }

    DELTA_NODE_CONFIG = {
        "exec_agent": {
            "job_controller": {
                "resource_limits": {
                    "user_slots": 10,
                    "cpu": 10,
                    "memory": 10 * 1024 ** 3,
                }
            }
        }
    }

    def test_jobs_are_scheduled_after_pool_change(self):
        create("map_node", "//sys/pools/initial_pool")
        create("map_node", "//sys/pools/changed_pool")
        op = run_test_vanilla(":", job_count=100000, spec={"pool": "initial_pool"})
        wait(lambda: op.get_job_count("running") > 5, iter=10)

        update_op_parameters(op.id, parameters={"pool": "changed_pool"})
        time.sleep(0.1)

        scheduled = op.get_job_count("running") + op.get_job_count("completed")
        wait(lambda: op.get_job_count("running") + op.get_job_count("completed") > scheduled + 10)
