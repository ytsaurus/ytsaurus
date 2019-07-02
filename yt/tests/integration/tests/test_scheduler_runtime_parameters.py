from yt_env_setup import YTEnvSetup, wait, Restarter, SCHEDULERS_SERVICE
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
            "pool_change_is_allowed": True,
            "watchers_update_period": 100,  # Update pools configuration period
        }
    }

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

        with Restarter(self.Env, SCHEDULERS_SERVICE):
            pass

        wait(lambda: op.get_state() == "running", iter=10)

        assert are_almost_equal(get(progress_path + "/weight"), 3.0)
        # wait() is essential since resource limits are copied from runtime parameters only during fair-share update.
        wait(lambda: get(progress_path + "/resource_limits")["user_slots"] == 0, iter=5)

    def test_change_pool_of_default_pooltree(self):
        create("map_node", "//sys/pools/initial_pool")
        create("map_node", "//sys/pools/changed_pool")

        op = run_sleeping_vanilla(spec={"pool": "initial_pool"})

        wait(lambda: op.get_state() == "running", iter=10)

        update_op_parameters(op.id, parameters={"pool": "changed_pool"})

        path = "//sys/scheduler/orchid/scheduler/operations/{0}/progress/scheduling_info_per_pool_tree/default/pool".format(op.id)
        assert get(path) == "changed_pool"

    def test_running_operation_counts_on_change_pool(self):
        create("map_node", "//sys/pools/initial_pool")
        create("map_node", "//sys/pools/changed_pool")

        op = run_sleeping_vanilla(spec={"pool": "initial_pool"})

        wait(lambda: op.get_state() == "running", iter=10)

        pools_path = "//sys/scheduler/orchid/scheduler/pools/"
        wait(lambda: get(pools_path + "initial_pool/running_operation_count") == 1)
        wait(lambda: get(pools_path + "changed_pool/running_operation_count") == 0)

        update_op_parameters(op.id, parameters={"pool": "changed_pool"})

        wait(lambda: get(pools_path + "initial_pool/running_operation_count") == 0)
        wait(lambda: get(pools_path + "changed_pool/running_operation_count") == 1)

    def test_change_pool_of_multitree_operation(self):
        self.create_custom_pool_tree_with_one_node(pool_tree="custom")
        create("map_node", "//sys/pools/default_pool")
        create("map_node", "//sys/pool_trees/custom/custom_pool1")
        create("map_node", "//sys/pool_trees/custom/custom_pool2")
        time.sleep(0.1)

        op = run_sleeping_vanilla(
            spec={
                "pool_trees": ["default", "custom"],
                "scheduling_options_per_pool_tree": {
                    "default": {"pool": "default_pool"},
                    "custom": {"pool": "custom_pool1"}
                }
            })

        wait(lambda: op.get_state() == "running", iter=10)

        update_op_parameters(op.id, parameters={"scheduling_options_per_pool_tree": {"custom": {"pool": "custom_pool2"}}})

        path = "//sys/scheduler/orchid/scheduler/operations/{0}/progress/scheduling_info_per_pool_tree/custom/pool".format(op.id)
        assert get(path) == "custom_pool2"

    def test_operation_count_validation_on_change_pool(self):
        set("//sys/pools/initial_pool", {})
        set("//sys/pools/full_pool", {})
        set("//sys/pools/full_pool/@max_running_operation_count", 0)

        op = run_sleeping_vanilla(spec={"pool": "initial_pool"})

        wait(lambda: op.get_state() == "running")

        with pytest.raises(YtError):
            update_op_parameters(op.id, parameters={"pool": "full_pool"})

        path = "//sys/scheduler/orchid/scheduler/operations/{0}/progress/scheduling_info_per_pool_tree/default/pool".format(op.id)
        assert get(path) == "initial_pool"

    def test_no_pool_validation_on_change_weight(self):
        set("//sys/pools/test_pool", {})
        op = run_sleeping_vanilla(spec={"pool": "test_pool"})
        wait(lambda: op.get_state() == "running")

        set("//sys/pools/test_pool/@max_operation_count", 0)
        set("//sys/pools/test_pool/@max_running_operation_count", 0)

        orchid_pools = "//sys/scheduler/orchid/scheduler/pools"
        wait(lambda: get(orchid_pools + "/test_pool/max_running_operation_count") == 0)

        # assert this doesn't fail
        update_op_parameters(op.id, parameters={"weight": 2})

    def create_custom_pool_tree_with_one_node(self, pool_tree):
        tag = pool_tree
        node = ls("//sys/cluster_nodes")[0]
        set("//sys/cluster_nodes/" + node + "/@user_tags/end", tag)
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


class TestOperationDetailedLogs(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 10
    NUM_SCHEDULERS = 1

    def get_scheduled_job_log_entries(self):
        scheduler_debug_logs_filename = self.Env.configs["scheduler"][0]["logging"]["writers"]["debug"]["file_name"]
        return [line for line in open(scheduler_debug_logs_filename, "r") if "Scheduled a job" in line]

    def test_enable_detailed_logs(self):
        create("map_node", "//sys/pool_trees/default/fake_pool")
        set("//sys/pool_trees/default/fake_pool/@resource_limits", {"user_slots": 3})

        op = run_sleeping_vanilla(job_count=10, spec={"pool": "fake_pool"})
        wait(lambda: len(op.get_running_jobs()) == 3)

        # Check that there are no detailed logs by default.

        assert len(self.get_scheduled_job_log_entries()) == 0

        # Enable detailed logging and check that expected the expected log entries are produced.

        update_op_parameters(op.id, parameters={
            "scheduling_options_per_pool_tree": {
                "default": {
                    "enable_detailed_logs": True,
                }
            }
        })
        time.sleep(1)

        assert len(op.get_running_jobs()) == 3
        set("//sys/pool_trees/default/fake_pool/@resource_limits/user_slots", 5)
        wait(lambda: len(op.get_running_jobs()) == 5)
        time.sleep(0.5)  # Give it time to flush the log to disk.

        log_entries = self.get_scheduled_job_log_entries()
        assert len(log_entries) == 2
        for log_entry in log_entries:
            assert "OperationId: {}".format(op.id) in log_entry
            assert "TreeId: default" in log_entry

        # Disable detailed logging and check that no new log entries are produced.

        update_op_parameters(op.id, parameters={
            "scheduling_options_per_pool_tree": {
                "default": {
                    "enable_detailed_logs": False,
                }
            }
        })
        time.sleep(1)

        assert len(op.get_running_jobs()) == 5
        set("//sys/pool_trees/default/fake_pool/@resource_limits/user_slots", 7)
        wait(lambda: len(op.get_running_jobs()) == 7)
        time.sleep(0.5)  # Give it time to flush the log to disk.

        assert len(log_entries) == 2

        op.abort()

    def test_enable_detailed_logs_requires_administer_permission(self):
        create_user("u1")
        op = run_sleeping_vanilla(job_count=10, authenticated_user="u1")

        def update_enable_detailed_logs():
            update_op_parameters(
                op.id,
                parameters={
                    "scheduling_options_per_pool_tree": {
                        "default": {
                            "enable_detailed_logs": False,
                        }
                    },
                },
                authenticated_user="u1",
            )

        with pytest.raises(YtError) as excinfo:
            update_enable_detailed_logs()
        if not excinfo.value.contains_code(AuthorizationErrorCode):
            raise excinfo.value

        add_member("u1", "superusers")
        update_enable_detailed_logs()
