from yt_env_setup import YTEnvSetup, wait
from yt.environment.helpers import assert_almost_equal
from yt_commands import *

class TestRuntimeParameters(YTEnvSetup):

    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "fair_share_update_period": 100
        }
    }

    def test_runtime_parameters(self):
        create_user("u")
        create_test_tables()

        op = map(
            command="sleep 100",
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={"weight": 5},
            dont_track=True)

        time.sleep(1.0)

        assert check_permission("u", "write", "//sys/operations/" + op.id)["action"] == "deny"
        assert get("//sys/scheduler/orchid/scheduler/operations/{0}/progress/weight".format(op.id)) == 5.0

        set("//sys/operations/{0}/@owners/end".format(op.id), "u")
        set("//sys/operations/{0}/@weight".format(op.id), 3)
        set("//sys/operations/{0}/@resource_limits".format(op.id), {"user_slots": 0})

        time.sleep(1.0)

        assert check_permission("u", "write", "//sys/operations/" + op.id)["action"] == "allow"
        assert get("//sys/scheduler/orchid/scheduler/operations/{0}/progress/weight".format(op.id)) == 3.0
        assert get("//sys/scheduler/orchid/scheduler/operations/{0}/progress/resource_limits".format(op.id))["user_slots"] == 0

        set("//sys/operations/{0}/@owners/end".format(op.id), "missing_user")

        get_alerts = lambda: get("//sys/operations/{0}/@alerts".format(op.id))
        wait(get_alerts)
        alerts = get_alerts()
        assert alerts.keys() == ["invalid_acl"]

        get("//sys/operations/{0}/@owners".format(op.id))

        self.Env.kill_schedulers()
        time.sleep(1)
        self.Env.start_schedulers()

        time.sleep(1)

        get("//sys/operations/{0}/@owners".format(op.id))

        alerts = get_alerts()
        assert alerts.keys() == ["invalid_acl"]

        remove("//sys/operations/{0}/@owners/-1".format(op.id))
        time.sleep(1.0)

        wait(lambda: not get_alerts())

    def test_update_runtime_parameters(self):
        create_user("u")
        create_test_tables()

        op = map(
            command="sleep 100",
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={"weight": 5},
            dont_track=True)

        wait(lambda: op.get_state() == "running", iter=10)

        assert check_permission("u", "write", "//sys/operations/" + op.id)["action"] == "deny"
        assert get("//sys/scheduler/orchid/scheduler/operations/{0}/progress/weight".format(op.id)) == 5.0

        update_op_parameters(op.id, parameters={"owners": ["u"]})
        assert check_permission("u", "write", "//sys/operations/" + op.id)["action"] == "allow"

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

        assert assert_almost_equal(
            get("//sys/operations/" + op.id + "/@runtime_parameters/scheduling_options_per_pool_tree/default/weight"),
            3.0)
        assert get("//sys/operations/" + op.id + "/@runtime_parameters/scheduling_options_per_pool_tree/default/resource_limits/user_slots") == 0

        assert assert_almost_equal(
            get("//sys/scheduler/orchid/scheduler/operations/{0}/progress/weight".format(op.id)),
            3.0)

        # wait() here is essential since resource limits are recomputed during fair-share update.
        wait(lambda: get("//sys/scheduler/orchid/scheduler/operations/{0}/progress/resource_limits".format(op.id))["user_slots"] == 0,
             iter=5)

        self.Env.kill_schedulers()
        self.Env.start_schedulers()

        wait(lambda: op.get_state() == "running", iter=10)

        assert assert_almost_equal(
            get("//sys/scheduler/orchid/scheduler/operations/{0}/progress/weight".format(op.id)),
            3.0)
        assert get("//sys/scheduler/orchid/scheduler/operations/{0}/progress/resource_limits".format(op.id))["user_slots"] == 0

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
