import builtins
import datetime
import time

from yt_env_setup import (
    YTEnvSetup,
    Restarter,
    NODES_SERVICE,
    SCHEDULERS_SERVICE,
)

from yt_commands import (
    authors, create, wait, write_table, ls, get, create_data_center, create_rack, run_sleeping_vanilla, update_pool_tree_config,
    update_pool_tree_config_option, create_pool_tree, exists, map, update_scheduler_config, create_pool, set_node_banned, set
)

from yt_scheduler_helpers import (
    scheduler_orchid_path, scheduler_orchid_node_path, scheduler_new_orchid_pool_tree_path, scheduler_orchid_pool_path,
    scheduler_orchid_operation_path,
)

from yt_helpers import (
    write_log_barrier, read_structured_log, profiler_factory
)


##################################################################

def get_operation_from_orchid(operation, tree="gpu"):
    return get(scheduler_new_orchid_pool_tree_path(tree) + f"/gpu_assignment_plan/operations/{operation.id}")


def get_node_from_orchid(node, tree="gpu"):
    return get(scheduler_new_orchid_pool_tree_path(tree) + f"/gpu_assignment_plan/nodes/{node}")


def get_operation_assignments_from_orchid(operation, tree="gpu"):
    return get_operation_from_orchid(operation, tree=tree)["assignments"]


def check_assignment(assignment, operation_id, group_name, gpu_usage, preemptible):
    assert assignment["operation_id"] == operation_id
    assert assignment["allocation_group_name"] == group_name
    assert assignment["resource_usage"]["gpu"] == gpu_usage
    assert assignment["preemptible"] == preemptible


def check_operation(operation, is_gang, group_name, allocation_count, min_needed_gpu_per_allocation, assigned_gpu_usage, assignment_count, enabled=None, scheduling_module=None):
    assert operation["gang"] == is_gang
    assert group_name in operation["initial_grouped_needed_resources"]
    assert operation["initial_grouped_needed_resources"][group_name]["allocation_count"] == allocation_count
    assert operation["initial_grouped_needed_resources"][group_name]["min_needed_resources"]["gpu"] == min_needed_gpu_per_allocation
    assert operation["assigned_resource_usage"]["gpu"] == assigned_gpu_usage
    assert len(operation["assignments"]) == assignment_count
    if enabled is not None:
        assert operation["enabled"] == enabled
    if scheduling_module is not None:
        assert operation["scheduling_module"] == scheduling_module


def wait_for_operations_in_orchid(operation_count, tree="gpu"):
    wait(lambda: len(get(scheduler_new_orchid_pool_tree_path(tree) + "/gpu_assignment_plan/operations")) == operation_count)


def wait_for_assignments_in_orchid(operation, assignment_count, tree="gpu", exactly=False):
    if exactly:
        wait(lambda: len(get(scheduler_new_orchid_pool_tree_path(tree) + f"/gpu_assignment_plan/operations/{operation.id}/assignments")) == assignment_count)
    else:
        wait(lambda: len(get(scheduler_new_orchid_pool_tree_path(tree) + f"/gpu_assignment_plan/operations/{operation.id}/assignments")) >= assignment_count)


##################################################################

class DryRunGpuSchedulingPolicyTestBaseConfig(YTEnvSetup):
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
        super(DryRunGpuSchedulingPolicyTestBaseConfig, self).setup_method(method)

        update_pool_tree_config("default", {"node_tag_filter": "!gpu"})
        create_pool_tree("gpu", config={
            "node_tag_filter": "gpu",
            "main_resource": "gpu",
            "gpu_scheduling_policy": {
                "mode": "dry_run",
                "plan_update_period": 100,
                "module_type": "data_center",
                "modules": [self.DATA_CENTER],
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

        create_data_center(self.DATA_CENTER)
        create_rack(self.RACK)
        set("//sys/racks/{}/@data_center".format(self.RACK), self.DATA_CENTER)
        for node in ls("//sys/cluster_nodes"):
            set("//sys/cluster_nodes/{}/@rack".format(node), self.RACK)
            set("//sys/cluster_nodes/{}/@user_tags".format(node), ["gpu"])
        for node in ls("//sys/cluster_nodes"):
            wait(lambda: get(scheduler_orchid_node_path(node) + "/data_center") == self.DATA_CENTER)

        wait(lambda: get(scheduler_new_orchid_pool_tree_path("gpu") + "/node_count") == self.NUM_NODES)

##################################################################


class TestDryRunGpuSchedulingPolicy(DryRunGpuSchedulingPolicyTestBaseConfig):
    @authors("eshcherbin")
    def test_simple(self):
        op = run_sleeping_vanilla(
            task_patch={"gpu_limit": 1, "enable_gpu_layers": False},
        )

        wait(lambda: len(op.get_running_jobs()) == 1)

        wait(lambda: exists(scheduler_new_orchid_pool_tree_path("gpu") + "/gpu_assignment_plan"))

        wait_for_operations_in_orchid(operation_count=1)
        wait_for_assignments_in_orchid(op, assignment_count=1)

        operation = get_operation_from_orchid(op)
        check_operation(
            operation=operation,
            is_gang=False,
            group_name="task",
            allocation_count=1,
            min_needed_gpu_per_allocation=1,
            assigned_gpu_usage=1,
            assignment_count=1,
            enabled=True,
        )
        assignment = operation["assignments"][0]
        check_assignment(
            assignment=assignment,
            operation_id=op.id,
            group_name="task",
            gpu_usage=1,
            preemptible=False)

        node_address = assignment["node_address"]
        assert node_address in ls("//sys/cluster_nodes")
        node = get(scheduler_new_orchid_pool_tree_path("gpu") + f"/gpu_assignment_plan/nodes/{node_address}")
        assert node["assigned_resource_usage"]["gpu"] == 1
        assert node["resourse_limits"]["gpu"] == 8
        assert node["scheduling_module"] == TestDryRunGpuSchedulingPolicy.DATA_CENTER

        assert len(get(scheduler_new_orchid_pool_tree_path("gpu") + f"/gpu_assignment_plan/nodes/{node_address}/assignments")) == 1
        assert get(scheduler_new_orchid_pool_tree_path("gpu") + f"/gpu_assignment_plan/nodes/{node_address}/assignments")[0] == assignment

    @authors("yaishenka")
    def test_simple_full_host(self):
        op = run_sleeping_vanilla(
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
        )

        wait(lambda: len(op.get_running_jobs()) == 1)

        wait_for_operations_in_orchid(operation_count=1)
        wait_for_assignments_in_orchid(op, assignment_count=1)

        operation = get_operation_from_orchid(op)
        check_operation(
            operation=operation,
            is_gang=False,
            group_name="task",
            allocation_count=1,
            min_needed_gpu_per_allocation=8,
            assigned_gpu_usage=8,
            assignment_count=1,
            enabled=True,
        )

        assignment = operation["assignments"][0]
        check_assignment(
            assignment=assignment,
            operation_id=op.id,
            group_name="task",
            gpu_usage=8,
            preemptible=False)

        node_address = assignment["node_address"]
        node = get(scheduler_new_orchid_pool_tree_path("gpu") + f"/gpu_assignment_plan/nodes/{node_address}")
        assert len(node["assignments"]) == 1
        assert node["assignments"][0] == assignment

    @authors("yaishenka")
    def test_simple_two_jobs(self):
        op = run_sleeping_vanilla(
            task_patch={"gpu_limit": 1, "enable_gpu_layers": False},
            job_count=2,
        )

        wait(lambda: len(op.get_running_jobs()) == 2)

        wait_for_operations_in_orchid(operation_count=1)
        wait_for_assignments_in_orchid(op, assignment_count=2)

        operation = get_operation_from_orchid(op)
        check_operation(
            operation=operation,
            is_gang=False,
            group_name="task",
            allocation_count=2,
            min_needed_gpu_per_allocation=1,
            assigned_gpu_usage=2,
            assignment_count=2,
            enabled=True,
        )

        for assignment in operation["assignments"]:
            check_assignment(
                assignment=assignment,
                operation_id=op.id,
                group_name="task",
                gpu_usage=1,
                preemptible=False)

        node_address = assignment["node_address"]
        node = get(scheduler_new_orchid_pool_tree_path("gpu") + f"/gpu_assignment_plan/nodes/{node_address}")
        assert len(node["assignments"]) == 2
        for assignment in node["assignments"]:
            check_assignment(
                assignment=assignment,
                operation_id=op.id,
                group_name="task",
                gpu_usage=1,
                preemptible=False)

    @authors("yaishenka")
    def test_simple_two_jobs_full_host(self):
        op = run_sleeping_vanilla(
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
            job_count=2,
        )

        wait(lambda: len(op.get_running_jobs()) == 2)

        wait_for_operations_in_orchid(operation_count=1)
        wait_for_assignments_in_orchid(op, assignment_count=2)

        operation = get_operation_from_orchid(op)
        check_operation(
            operation=operation,
            is_gang=False,
            group_name="task",
            allocation_count=2,
            min_needed_gpu_per_allocation=8,
            assigned_gpu_usage=16,
            assignment_count=2,
            enabled=True,
        )

        for assignment in operation["assignments"]:
            check_assignment(
                assignment=assignment,
                operation_id=op.id,
                group_name="task",
                gpu_usage=8,
                preemptible=False)

            node_address = assignment["node_address"]
            node = get(scheduler_new_orchid_pool_tree_path("gpu") + f"/gpu_assignment_plan/nodes/{node_address}")
            assert len(node["assignments"]) == 1
            for node_assignment in node["assignments"]:
                assert node_assignment == assignment

    @authors("yaishenka")
    def test_simple_two_ops(self):
        op1 = run_sleeping_vanilla(
            task_patch={"gpu_limit": 1, "enable_gpu_layers": False},
            job_count=1,
        )
        op2 = run_sleeping_vanilla(
            task_patch={"gpu_limit": 1, "enable_gpu_layers": False},
            job_count=1,
        )

        wait(lambda: len(op1.get_running_jobs()) == 1)
        wait(lambda: len(op2.get_running_jobs()) == 1)

        wait_for_operations_in_orchid(operation_count=2)
        wait_for_assignments_in_orchid(op1, assignment_count=1)
        wait_for_assignments_in_orchid(op2, assignment_count=1)

        for op in [op1, op2]:
            operation = get_operation_from_orchid(op)
            check_operation(
                operation=operation,
                is_gang=False,
                group_name="task",
                allocation_count=1,
                min_needed_gpu_per_allocation=1,
                assigned_gpu_usage=1,
                assignment_count=1,
                enabled=True,
            )

            for assignment in operation["assignments"]:
                check_assignment(
                    assignment=assignment,
                    operation_id=op.id,
                    group_name="task",
                    gpu_usage=1,
                    preemptible=False)

    # Just test that in theory it also works with CPU trees.
    @authors("eshcherbin")
    def test_simple_cpu_tree(self):
        update_pool_tree_config("default", {"node_tag_filter": "!gpu & !cpu"})
        create_pool_tree("cpu", config={
            "node_tag_filter": "cpu",
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

        for node in ls("//sys/cluster_nodes"):
            set("//sys/cluster_nodes/{}/@user_tags".format(node), ["cpu"])
        wait(lambda: get(scheduler_new_orchid_pool_tree_path("cpu") + "/node_count") == TestDryRunGpuSchedulingPolicy.NUM_NODES)

        op = run_sleeping_vanilla(spec={"pool_trees": ["cpu"]})

        wait(lambda: len(op.get_running_jobs()) == 1)

        wait(lambda: exists(scheduler_new_orchid_pool_tree_path("cpu") + "/gpu_assignment_plan"))

        wait_for_operations_in_orchid(operation_count=1, tree="cpu")
        wait_for_assignments_in_orchid(op, tree="cpu", assignment_count=1)

        operation = get_operation_from_orchid(op, tree="cpu")
        check_operation(
            operation=operation,
            is_gang=False,
            group_name="task",
            allocation_count=1,
            min_needed_gpu_per_allocation=0,
            assigned_gpu_usage=0,
            assignment_count=1,
            enabled=True,
        )
        assignment = operation["assignments"][0]
        check_assignment(
            assignment=assignment,
            operation_id=op.id,
            group_name="task",
            gpu_usage=0,
            preemptible=False)

        node_address = assignment["node_address"]
        assert node_address in ls("//sys/cluster_nodes")
        node = get(scheduler_new_orchid_pool_tree_path("cpu") + f"/gpu_assignment_plan/nodes/{node_address}")
        assert node["assigned_resource_usage"]["cpu"] == 1.0
        assert node["resourse_limits"]["cpu"] == 10.0
        assert node["scheduling_module"] == TestDryRunGpuSchedulingPolicy.DATA_CENTER

        assert len(get(scheduler_new_orchid_pool_tree_path("cpu") + f"/gpu_assignment_plan/nodes/{node_address}/assignments")) == 1
        assert get(scheduler_new_orchid_pool_tree_path("cpu") + f"/gpu_assignment_plan/nodes/{node_address}/assignments")[0] == assignment

    @authors("yaishenka")
    def test_two_ops_full_host(self):
        op1 = run_sleeping_vanilla(
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
            job_count=1,
        )
        op2 = run_sleeping_vanilla(
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
            job_count=1,
        )

        wait(lambda: len(op1.get_running_jobs()) == 1)
        wait(lambda: len(op2.get_running_jobs()) == 1)

        wait_for_operations_in_orchid(operation_count=2)
        wait_for_assignments_in_orchid(op1, assignment_count=1)
        wait_for_assignments_in_orchid(op2, assignment_count=1)

        for op in [op1, op2]:
            operation = get_operation_from_orchid(op)
            check_operation(
                operation=operation,
                is_gang=False,
                group_name="task",
                allocation_count=1,
                min_needed_gpu_per_allocation=8,
                assigned_gpu_usage=8,
                assignment_count=1,
                enabled=True,
            )

            for assignment in operation["assignments"]:
                check_assignment(
                    assignment=assignment,
                    operation_id=op.id,
                    group_name="task",
                    gpu_usage=8,
                    preemptible=False)

    @authors("yaishenka")
    def test_vanilla_more_gpu_goes_first(self):
        op1 = run_sleeping_vanilla(
            task_patch={"gpu_limit": 1, "enable_gpu_layers": False},
            job_count=1,
            track=False,
            spec={"testing": {"delay_inside_materialize": 100}},
        )
        op2 = run_sleeping_vanilla(
            task_patch={"gpu_limit": 4, "enable_gpu_layers": False},
            job_count=1,
            track=False
        )

        wait(lambda: len(op1.get_running_jobs()) == 1)
        wait(lambda: len(op2.get_running_jobs()) == 1)

        wait_for_operations_in_orchid(operation_count=2)
        wait_for_assignments_in_orchid(op1, assignment_count=1)
        wait_for_assignments_in_orchid(op2, assignment_count=1)

        assignment1 = get_operation_assignments_from_orchid(op1)[0]
        assignment2 = get_operation_assignments_from_orchid(op2)[0]

        op1_creation_time = datetime.datetime.fromisoformat(assignment1["creation_time"])
        op2_creation_time = datetime.datetime.fromisoformat(assignment2["creation_time"])

        assert op2_creation_time <= op1_creation_time

    @authors("yaishenka")
    def test_vanilla_gang(self):
        update_pool_tree_config_option("gpu", "enable_step_function_for_gang_operations", False)
        op = run_sleeping_vanilla(
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
            spec={"is_gang": True},
            job_count=2,
        )

        wait(lambda: len(op.get_running_jobs()) == 2)

        wait_for_operations_in_orchid(operation_count=1)
        wait_for_assignments_in_orchid(op, assignment_count=2)

        wait(
            lambda: get(scheduler_orchid_pool_path("root", tree="gpu") + "/resource_demand/gpu") == 16.0
        )

        operation = get_operation_from_orchid(op)
        check_operation(
            operation=operation,
            is_gang=True,
            group_name="task",
            allocation_count=2,
            min_needed_gpu_per_allocation=8,
            assigned_gpu_usage=16,
            assignment_count=2,
            enabled=True,
        )

        for assignment in operation["assignments"]:
            check_assignment(
                assignment=assignment,
                operation_id=op.id,
                group_name="task",
                gpu_usage=8,
                preemptible=False)

            node_address = assignment["node_address"]
            node = get(scheduler_new_orchid_pool_tree_path("gpu") + f"/gpu_assignment_plan/nodes/{node_address}")
            assert len(node["assignments"]) == 1
            for node_assignment in node["assignments"]:
                assert node_assignment == assignment

    @authors("yaishenka")
    def test_operation_cant_schedule(self):
        run_sleeping_vanilla(
            task_patch={"gpu_limit": 9, "enable_gpu_layers": False},
            job_count=1,
        )

        wait(lambda: exists(scheduler_new_orchid_pool_tree_path("gpu") + "/gpu_assignment_plan"))

        time.sleep(10)
        assert len(get(scheduler_new_orchid_pool_tree_path("gpu") + "/gpu_assignment_plan/operations")) == 0

    @authors("yaishenka")
    def test_simple_map(self):
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")
        write_table("//tmp/t_in", {"foo": "bar"})

        op = map(
            track=False,
            in_="//tmp/t_in",
            out="//tmp/t_out",
            command="sleep 1000",
            spec={
                "mapper": {
                    "job_count": 1,
                    "gpu_limit": 1,
                    "enable_gpu_layers": False,
                },
            },
        )

        wait(lambda: len(op.get_running_jobs()) == 1)
        wait_for_operations_in_orchid(operation_count=1)
        wait_for_assignments_in_orchid(op, assignment_count=1)

        operation = get_operation_from_orchid(op)
        check_operation(
            operation=operation,
            is_gang=False,
            group_name="map",
            allocation_count=1,
            min_needed_gpu_per_allocation=1,
            assigned_gpu_usage=1,
            assignment_count=1,
            enabled=True,
        )

        for assignment in operation["assignments"]:
            check_assignment(
                assignment=assignment,
                operation_id=op.id,
                group_name="map",
                gpu_usage=1,
                preemptible=False)

        node_address = assignment["node_address"]
        node = get(scheduler_new_orchid_pool_tree_path("gpu") + f"/gpu_assignment_plan/nodes/{node_address}")
        assert len(node["assignments"]) == 1
        for assignment in node["assignments"]:
            check_assignment(
                assignment=assignment,
                operation_id=op.id,
                group_name="map",
                gpu_usage=1,
                preemptible=False)

    @authors("yaishenka")
    def test_empty_operation(self):
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")

        op = map(
            track=False,
            in_="//tmp/t_in",
            out="//tmp/t_out",
            command="sleep 1000",
        )

        wait_for_operations_in_orchid(operation_count=1)

        operation = get_operation_from_orchid(op)
        assert not operation["initial_grouped_needed_resources"]

    @authors("yaishenka")
    def test_simple_fullhost_map(self):
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")
        write_table("//tmp/t_in", {"foo": "bar"})

        op = map(
            track=False,
            in_="//tmp/t_in",
            out="//tmp/t_out",
            command="sleep 1000",
            spec={
                "mapper": {
                    "job_count": 1,
                    "gpu_limit": 8,
                    "enable_gpu_layers": False,
                },
            },
        )

        wait(lambda: len(op.get_running_jobs()) == 1)
        wait_for_operations_in_orchid(operation_count=1)
        wait_for_assignments_in_orchid(op, assignment_count=1)

        operation = get_operation_from_orchid(op)
        check_operation(
            operation=operation,
            is_gang=False,
            group_name="map",
            allocation_count=1,
            min_needed_gpu_per_allocation=8,
            assigned_gpu_usage=8,
            assignment_count=1,
            enabled=True,
        )

        for assignment in operation["assignments"]:
            check_assignment(
                assignment=assignment,
                operation_id=op.id,
                group_name="map",
                gpu_usage=8,
                preemptible=False)

    @authors("yaishenka")
    def test_fullhost_map_two_jobs(self):
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")

        for _ in range(2):
            write_table("<append=true>//tmp/t_in", {"foo": "bar"})

        op = map(
            track=False,
            in_="//tmp/t_in",
            out="//tmp/t_out",
            command="sleep 1000",
            spec={
                "job_count": 2,
                "mapper": {
                    "gpu_limit": 8,
                    "enable_gpu_layers": False,
                },
            }
        )

        wait(lambda: len(op.get_running_jobs()) == 2)
        wait_for_operations_in_orchid(operation_count=1)
        wait_for_assignments_in_orchid(op, assignment_count=2)

        operation = get_operation_from_orchid(op)
        check_operation(
            operation=operation,
            is_gang=False,
            group_name="map",
            allocation_count=2,
            min_needed_gpu_per_allocation=8,
            assigned_gpu_usage=16,
            assignment_count=2,
            enabled=True,
        )

        for assignment in operation["assignments"]:
            check_assignment(
                assignment=assignment,
                operation_id=op.id,
                group_name="map",
                gpu_usage=8,
                preemptible=False)

    @authors("yaishenka")
    def test_vanilla_goes_first(self):
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")
        write_table("//tmp/t_in", {"foo": "bar"})

        op1 = map(
            track=False,
            in_="//tmp/t_in",
            out="//tmp/t_out",
            command="sleep 1000",
            spec={
                "mapper": {
                    "job_count": 1,
                    "gpu_limit": 1,
                    "enable_gpu_layers": False,
                },
                "testing": {
                    "delay_inside_materialize": 100,
                },
            },
        )
        op2 = run_sleeping_vanilla(
            task_patch={"gpu_limit": 4, "enable_gpu_layers": False},
            job_count=1,
        )

        wait(lambda: len(op1.get_running_jobs()) == 1)
        wait(lambda: len(op2.get_running_jobs()) == 1)

        wait_for_operations_in_orchid(operation_count=2)
        wait_for_assignments_in_orchid(op1, assignment_count=1)
        wait_for_assignments_in_orchid(op2, assignment_count=1)

        assignment1 = get_operation_assignments_from_orchid(op1)[0]
        assignment2 = get_operation_assignments_from_orchid(op2)[0]

        op1_creation_time = datetime.datetime.fromisoformat(assignment1["creation_time"])
        op2_creation_time = datetime.datetime.fromisoformat(assignment2["creation_time"])

        assert op2_creation_time <= op1_creation_time

    @authors("eshcherbin")
    def test_orchid_with_offline_node(self):
        update_scheduler_config("node_registration_timeout", 1000)

        wait(lambda: len(get(scheduler_new_orchid_pool_tree_path("gpu") + "/gpu_assignment_plan/nodes")) ==
             TestDryRunGpuSchedulingPolicy.NUM_NODES)
        with Restarter(self.Env, NODES_SERVICE):
            wait(lambda: len(get(scheduler_new_orchid_pool_tree_path("gpu") + "/gpu_assignment_plan/nodes")) == 0)

            update_scheduler_config("testing_options/node_heartbeat_processing_delay", {
                "duration": 3000,
                "type": "async",
            })

        wait(lambda: len(get(scheduler_new_orchid_pool_tree_path("gpu") + "/gpu_assignment_plan/nodes")) ==
             TestDryRunGpuSchedulingPolicy.NUM_NODES)

    @authors("severovv")
    def test_tag_filters(self):
        nodes = list(ls("//sys/cluster_nodes"))

        first_node_only = run_sleeping_vanilla(
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
            job_count=1,
            spec={"scheduling_tag_filter": nodes[0]},
        )
        wait(lambda: len(first_node_only.get_running_jobs()) == 1)

        waits_for_first = run_sleeping_vanilla(
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
            job_count=1,
            spec={"scheduling_tag_filter": nodes[0]},
        )

        wait_for_operations_in_orchid(operation_count=2)
        wait_for_assignments_in_orchid(first_node_only, assignment_count=1)

        node1 = get_node_from_orchid(nodes[0])
        assert len(node1["assignments"]) == 1
        assert node1["assignments"][0]["operation_id"] == first_node_only.id

        time.sleep(5)
        assert len(get_operation_assignments_from_orchid(waits_for_first)) == 0

        any_node = run_sleeping_vanilla(
            task_patch={"gpu_limit": 7, "enable_gpu_layers": False},
            job_count=1,
        )
        wait(lambda: len(any_node.get_running_jobs()) == 1)

        wait_for_operations_in_orchid(operation_count=3)
        wait_for_assignments_in_orchid(any_node, assignment_count=1)

        node2 = get_node_from_orchid(nodes[1])
        assert len(node2["assignments"]) == 1
        assert node2["assignments"][0]["operation_id"] == any_node.id

    @authors("yaishenka")
    def test_plan_assignment_above_fair_share(self):
        create_pool(
            "haha_pool",
            pool_tree="gpu",
            attributes={"mode": "fifo"},
            wait_for_orchid=False,
        )
        run_sleeping_vanilla(
            task_patch={"gpu_limit": 5, "enable_gpu_layers": False},
            job_count=3,
            spec={
                "pool": "haha_pool",
            },
        )

        op = run_sleeping_vanilla(
            task_patch={"gpu_limit": 3, "enable_gpu_layers": False},
            job_count=1,
            spec={
                "testing": {"delay_inside_materialize": 100},
                "pool": "haha_pool",
            },
        )

        wait(lambda: len(op.get_running_jobs()) == 1)
        wait_for_operations_in_orchid(operation_count=2)
        wait_for_assignments_in_orchid(op, assignment_count=1)

        assignment = get_operation_assignments_from_orchid(op)[0]
        check_assignment(
            assignment=assignment,
            operation_id=op.id,
            group_name="task",
            gpu_usage=3,
            preemptible=True)

    @authors("yaishenka")
    def test_starving(self):
        nodes = list(ls("//sys/cluster_nodes"))

        op = run_sleeping_vanilla(
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
            job_count=2,
            spec={"scheduling_tag_filter": nodes[0]},
        )
        wait(lambda: len(op.get_running_jobs()) == 1)
        wait(lambda: get(scheduler_orchid_operation_path(op.id, tree="gpu") + "/starvation_status") == "starving")

        wait(lambda: get_operation_from_orchid(op)["starving"])

    @authors("yaishenka")
    def test_profiling(self):
        wait(lambda: exists(scheduler_new_orchid_pool_tree_path("gpu") + "/gpu_assignment_plan"))

        profiler = profiler_factory().at_scheduler(fixed_tags={"tree": "gpu"})
        prefix = "scheduler/gpu_policy"

        assignments_counter = profiler.gauge(prefix + "/assignments_count")
        planned_assignments_counter = profiler.counter(prefix + "/planned_assignments_count")
        preempted_assignments_counter = profiler.counter(prefix + "/planned_assignments_count")
        enabled_operations_counter = profiler.gauge(prefix + "/enabled_operations_count")
        full_host_module_bound_operations_counter = profiler.gauge(prefix + "/full_host_module_bound_operations_count")
        assigned_gpu_counter = profiler.gauge(prefix + "/assigned_gpu_count")

        module_bound_counter = profiler.gauge(prefix + "/module/full_host_module_bound_operations_count", fixed_tags={"module": "SAS"})
        module_total_counter = profiler.gauge(prefix + "/module/total_nodes_count", fixed_tags={"module": "SAS"})
        module_unreserved_counter = profiler.gauge(prefix + "/module/unreserved_nodes_count", fixed_tags={"module": "SAS"})

        wait(lambda: assignments_counter.get() == 0)
        wait(lambda: planned_assignments_counter.get() == 0)
        wait(lambda: preempted_assignments_counter.get() == 0)
        wait(lambda: enabled_operations_counter.get() == 0)
        wait(lambda: full_host_module_bound_operations_counter.get() == 0)
        wait(lambda: assigned_gpu_counter.get() == 0)
        wait(lambda: module_bound_counter.get() == 0)
        wait(lambda: module_total_counter.get() == 2)
        wait(lambda: module_unreserved_counter.get() == 2)

        op = run_sleeping_vanilla(
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
        )

        wait(lambda: assignments_counter.get() == 1)
        wait(lambda: planned_assignments_counter.get() == 1)
        wait(lambda: enabled_operations_counter.get() == 1)
        wait(lambda: full_host_module_bound_operations_counter.get() == 1)
        wait(lambda: assigned_gpu_counter.get() == 8)
        wait(lambda: module_bound_counter.get() == 1)
        wait(lambda: module_unreserved_counter.get() == 1)

        op.abort()

        wait(lambda: assignments_counter.get() == 0)
        wait(lambda: preempted_assignments_counter.get() == 1)
        wait(lambda: enabled_operations_counter.get() == 0)
        wait(lambda: full_host_module_bound_operations_counter.get() == 0)
        wait(lambda: assigned_gpu_counter.get() == 0)
        wait(lambda: module_bound_counter.get() == 0)
        wait(lambda: module_unreserved_counter.get() == 2)

    @authors("yaishenka")
    def test_gpu_structured_event_log(self):
        scheduler_log_file = self.path_to_run + "/logs/scheduler-0.json.log"
        scheduler_address = ls("//sys/scheduler/instances")[0]
        from_barrier = write_log_barrier(scheduler_address)

        op = run_sleeping_vanilla(
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
        )

        wait_for_operations_in_orchid(operation_count=1)
        wait_for_assignments_in_orchid(op, 1, exactly=True)

        time.sleep(5)

        to_barrier = write_log_barrier(scheduler_address)
        structured_log = read_structured_log(scheduler_log_file, from_barrier=from_barrier, to_barrier=to_barrier,
                                             row_filter=lambda e: "event_type" in e)

        assignment_log_found = False
        modules_log_found = False
        operation_log_found = False
        for event in structured_log:
            if event["category"] != "SchedulerGpuStructuredLog":
                continue
            if event["event_type"] == "operations_info":
                if op.id not in event["operations"]:
                    continue
                operation_log = event["operations"][op.id]
                if not operation_log["enabled"]:
                    continue
                if not operation_log["assignments"]:
                    continue
                operation_log_found = True
                check_operation(
                    operation=operation_log,
                    is_gang=False,
                    group_name="task",
                    allocation_count=1,
                    min_needed_gpu_per_allocation=8,
                    assigned_gpu_usage=8,
                    assignment_count=1,
                    enabled=True,
                )
                assignment = operation_log["assignments"][0]
                check_assignment(
                    assignment=assignment,
                    operation_id=op.id,
                    group_name="task",
                    gpu_usage=8,
                    preemptible=False
                )
            if event["event_type"] == "modules_info":
                if "SAS" not in event["modules"]:
                    continue
                if not event["modules"]["SAS"]["node_count"] == 2:
                    continue
                modules_log_found = True
            if event["event_type"] == "assignment_added":
                assert event["operation_id"] == op.id
                assignment_log_found = True

        assert assignment_log_found
        assert modules_log_found
        assert operation_log_found

##################################################################


class TestGpuSchedulerPersistentState(DryRunGpuSchedulingPolicyTestBaseConfig):
    def setup_method(self, method):
        super(TestGpuSchedulerPersistentState, self).setup_method(method)

        update_pool_tree_config_option("gpu", "gpu_scheduling_policy", {
            "mode": "dry_run",
            "plan_update_period": 100,
            "module_type": "data_center",
            "modules": [TestDryRunGpuSchedulingPolicy.DATA_CENTER],
            "full_host_aggressive_preemption_timeout": 1000,
            "initialization_timeout": 1000,
        })

    def _get_persistent_state_path(self, tree="gpu", entity="node"):
        return f"//sys/scheduler/strategy_state/tree_states/{tree}/gpu_scheduling_policy_state/{entity}_states"

    def _get_node_address_to_node_state_map(self, tree="gpu"):
        result = {}
        node_states_path = self._get_persistent_state_path(tree=tree)
        for node_id in get(node_states_path):
            node = get(self._get_persistent_state_path() + f"/{node_id}")
            result[node["address"]] = node

        return result

    def _compare_assignment_with_orchid(self, assignment, assignment_from_orchid):
        check_assignment(
            assignment=assignment,
            operation_id=assignment_from_orchid["operation_id"],
            group_name=assignment_from_orchid["allocation_group_name"],
            gpu_usage=assignment_from_orchid["resource_usage"]["gpu"],
            preemptible=assignment_from_orchid["preemptible"],
        )

    @authors("yaishenka")
    def test_simple_restart(self):
        op = run_sleeping_vanilla(
            task_patch={"gpu_limit": 1, "enable_gpu_layers": False},
        )

        wait(lambda: len(op.get_running_jobs()) == 1)

        wait_for_operations_in_orchid(operation_count=1)
        wait_for_assignments_in_orchid(op, assignment_count=1)

        operation_from_orchid = get_operation_from_orchid(op)
        assignment_from_orchid = operation_from_orchid["assignments"][0]

        node_address_to_node_state_map = self._get_node_address_to_node_state_map()

        for _, node in node_address_to_node_state_map.items():
            assert node["scheduling_module"] == "SAS"

        node = node_address_to_node_state_map[assignment_from_orchid["node_address"]]
        assert len(node["assignment_states"]) == 1
        assignment_from_node = node["assignment_states"][0]
        self._compare_assignment_with_orchid(assignment_from_node, assignment_from_orchid)

        operation_states_path = self._get_persistent_state_path(entity="operation")
        operation_ids = {op.id, }
        for operation_id in get(operation_states_path):
            operation = get(self._get_persistent_state_path(entity="operation") + f"/{operation_id}")
            assert "scheduling_module" not in operation
            assert operation_id in operation_ids
            operation_ids.remove(operation_id)

        assert len(operation_ids) == 0

        with Restarter(self.Env, SCHEDULERS_SERVICE):
            pass

        wait_for_operations_in_orchid(operation_count=1)
        wait_for_assignments_in_orchid(op, assignment_count=1)

        revived_operation_from_orchid = get_operation_from_orchid(op)
        revived_assignment = revived_operation_from_orchid["assignments"][0]
        self._compare_assignment_with_orchid(revived_assignment, assignment_from_orchid)

    @authors("yaishenka")
    def test_full_host_module_bound_restart(self):
        update_pool_tree_config_option("gpu", "enable_step_function_for_gang_operations", False)
        op = run_sleeping_vanilla(
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
            spec={"is_gang": True},
            job_count=2,
        )

        wait(lambda: len(op.get_running_jobs()) == 2)

        wait_for_operations_in_orchid(operation_count=1)
        wait_for_assignments_in_orchid(op, assignment_count=2)

        operation_from_orchid = get_operation_from_orchid(op)
        assignments_from_orchid = operation_from_orchid["assignments"]

        node_address_to_node_state_map = self._get_node_address_to_node_state_map()
        operation_states_map = {}
        operation_states_path = self._get_persistent_state_path(entity="operation")
        for operation_id in get(operation_states_path):
            operation_states_map[operation_id] = get(self._get_persistent_state_path(entity="operation") + f"/{operation_id}")

        for _, node in node_address_to_node_state_map.items():
            assert node["scheduling_module"] == "SAS"

        for assignment in assignments_from_orchid:
            node = node_address_to_node_state_map[assignment["node_address"]]
            assert len(node["assignment_states"]) == 1
            assignment_from_node = node["assignment_states"][0]
            self._compare_assignment_with_orchid(assignment_from_node, assignment)

        operation_states_path = self._get_persistent_state_path(entity="operation")
        operation_ids = {op.id, }
        for operation_id, operation in operation_states_map.items():
            assert operation["scheduling_module"] == "SAS"
            assert operation_id in operation_ids
            operation_ids.remove(operation_id)

        assert len(operation_ids) == 0

        with Restarter(self.Env, SCHEDULERS_SERVICE):
            pass

        wait_for_operations_in_orchid(operation_count=1)
        wait_for_assignments_in_orchid(op, assignment_count=2)

        revived_operation_from_orchid = get_operation_from_orchid(op)
        assignments = {assignment["node_address"]: assignment for assignment in revived_operation_from_orchid["assignments"]}
        for assignment in assignments_from_orchid:
            self._compare_assignment_with_orchid(assignments[assignment["node_address"]], assignment)

    @authors("yaishenka")
    def test_map_vanilla_restart(self):
        op_vanilla = run_sleeping_vanilla(
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
            job_count=1,
        )
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")
        write_table("<append=true>//tmp/t_in", {"foo": "bar"})

        op_map = map(
            track=False,
            in_="//tmp/t_in",
            out="//tmp/t_out",
            command="sleep 1000",
            spec={
                "job_count": 1,
                "mapper": {
                    "gpu_limit": 4,
                    "enable_gpu_layers": False,
                },
            }
        )

        wait(lambda: len(op_vanilla.get_running_jobs()) == 1)
        wait(lambda: len(op_map.get_running_jobs()) == 1)

        wait_for_operations_in_orchid(operation_count=2)
        wait_for_assignments_in_orchid(op_vanilla, assignment_count=1)
        wait_for_assignments_in_orchid(op_map, assignment_count=1)

        vanilla_operation_from_orchid = get_operation_from_orchid(op_vanilla)
        map_operation_from_orchid = get_operation_from_orchid(op_map)

        node_address_to_node_state_map = self._get_node_address_to_node_state_map()
        operation_states_map = {}
        operation_states_path = self._get_persistent_state_path(entity="operation")
        for operation_id in get(operation_states_path):
            operation_states_map[operation_id] = get(self._get_persistent_state_path(entity="operation") + f"/{operation_id}")

        assignments_in_orchid = vanilla_operation_from_orchid["assignments"] + map_operation_from_orchid["assignments"]
        for assignment in assignments_in_orchid:
            node = node_address_to_node_state_map[assignment["node_address"]]
            assert len(node["assignment_states"]) == 1
            assignment_from_node = node["assignment_states"][0]
            self._compare_assignment_with_orchid(assignment_from_node, assignment)

        with Restarter(self.Env, SCHEDULERS_SERVICE):
            pass

        wait_for_operations_in_orchid(operation_count=2)
        wait_for_assignments_in_orchid(op_vanilla, assignment_count=1)
        wait_for_assignments_in_orchid(op_map, assignment_count=1)

        revived_vanilla = get_operation_from_orchid(op_vanilla)
        revived_assignment = revived_vanilla["assignments"][0]
        self._compare_assignment_with_orchid(revived_assignment, vanilla_operation_from_orchid["assignments"][0])

        revived_map = get_operation_from_orchid(op_map)
        revived_assignment = revived_map["assignments"][0]
        self._compare_assignment_with_orchid(revived_assignment, map_operation_from_orchid["assignments"][0])

    @authors("yaishenka")
    def test_restart_without_gpu_policy_state(self):
        op = run_sleeping_vanilla(
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
            job_count=1,
        )

        wait(lambda: len(op.get_running_jobs()) == 1)

        current_state_config = get("//sys/scheduler/strategy_state/tree_states/gpu")
        current_state_config.pop("gpu_scheduling_policy_state")

        with Restarter(self.Env, SCHEDULERS_SERVICE):
            set("//sys/scheduler/strategy_state/tree_states/gpu", current_state_config)

        wait_for_operations_in_orchid(operation_count=1)
        wait_for_assignments_in_orchid(op, assignment_count=1)

##################################################################


class TestPlanAssignmentAboveFairShare(DryRunGpuSchedulingPolicyTestBaseConfig):
    NUM_NODES = 3

    @authors("yaishenka")
    def test_plan_assignment_above_fair_share_with_tag(self):
        nodes = list(ls("//sys/cluster_nodes"))
        set("//sys/cluster_nodes/{}/@user_tags".format(nodes[0]), ["gpu", "custom_tag"])
        create_pool(
            "haha_pool",
            pool_tree="gpu",
            attributes={"mode": "fifo"},
            wait_for_orchid=False,
        )
        op1 = run_sleeping_vanilla(
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
            job_count=3,
            spec={
                "pool": "haha_pool",
                "scheduling_tag_filter": "custom_tag",
            },
        )

        wait(lambda: len(op1.get_running_jobs()) == 1)
        wait_for_operations_in_orchid(operation_count=1)

        op2 = run_sleeping_vanilla(
            task_patch={"gpu_limit": 3, "enable_gpu_layers": False},
            job_count=1,
            spec={
                "pool": "haha_pool",
            },
        )

        wait(lambda: len(op2.get_running_jobs()) == 1)
        wait_for_assignments_in_orchid(op2, assignment_count=1)

        assignment = get_operation_assignments_from_orchid(op2)[0]
        check_assignment(
            assignment=assignment,
            operation_id=op2.id,
            group_name="task",
            gpu_usage=3,
            preemptible=True)

        wait(lambda: get(scheduler_orchid_operation_path(op1.id, tree="gpu") + "/starvation_status") == "starving")

        set("//sys/cluster_nodes/{}/@user_tags".format(nodes[1]), ["gpu", "custom_tag"])

        wait_for_assignments_in_orchid(op2, assignment_count=0)
        wait_for_assignments_in_orchid(op1, assignment_count=2)


##################################################################

class TestDryRunGpuSchedulingPolicyMultiModule(YTEnvSetup):
    ENABLE_MULTIDAEMON = False  # There are component restarts.
    NUM_MASTERS = 1
    NUM_NODES = 4
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

    DATA_CENTERS = ["SAS", "VLA"]
    RACKS = ["SAS1", "VLA1"]

    def _setup_data_centers(self, node_count_per_data_center, ibc_to_dc=None):
        dc_to_rack = dict(zip(TestDryRunGpuSchedulingPolicyMultiModule.DATA_CENTERS, TestDryRunGpuSchedulingPolicyMultiModule.RACKS))
        data_center_index_per_node = sum([[i] * count for i, count in enumerate(node_count_per_data_center)], [])

        def get_node_rack(i, node):
            if ibc_to_dc is None:
                return TestDryRunGpuSchedulingPolicyMultiModule.RACKS[data_center_index_per_node[i]]
            ibc = get("//sys/cluster_nodes/{}/@annotations/infiniband_cluster_tag".format(node))
            return dc_to_rack[ibc_to_dc[ibc]]

        nodes = list(ls("//sys/cluster_nodes"))
        for i, node in enumerate(nodes):
            set("//sys/cluster_nodes/{}/@rack".format(node), get_node_rack(i, node))

        rack_to_dc = dict(zip(TestDryRunGpuSchedulingPolicyMultiModule.RACKS, TestDryRunGpuSchedulingPolicyMultiModule.DATA_CENTERS))
        for i, node in enumerate(nodes):
            wait(lambda: get(scheduler_orchid_node_path(node) + "/data_center") == rack_to_dc[get_node_rack(i, node)])

    def setup_method(self, method):
        super(TestDryRunGpuSchedulingPolicyMultiModule, self).setup_method(method)

        update_pool_tree_config("default", {"node_tag_filter": "!gpu"})
        create_pool_tree("gpu", config={
            "node_tag_filter": "gpu",
            "main_resource": "gpu",
            "gpu_scheduling_policy": {
                "mode": "dry_run",
                "plan_update_period": 100,
                "module_type": "data_center",
                "modules": TestDryRunGpuSchedulingPolicyMultiModule.DATA_CENTERS,
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

        dc_to_rack = dict(zip(TestDryRunGpuSchedulingPolicyMultiModule.DATA_CENTERS, TestDryRunGpuSchedulingPolicyMultiModule.RACKS))
        for dc, r in dc_to_rack.items():
            create_data_center(dc)
            create_rack(r)
            set("//sys/racks/{}/@data_center".format(r), dc)

        for node in ls("//sys/cluster_nodes"):
            set("//sys/cluster_nodes/{}/@user_tags".format(node), ["gpu"])

        module_count = TestDryRunGpuSchedulingPolicyMultiModule.NUM_NODES // 2
        self._setup_data_centers([module_count, module_count])

        wait(lambda: get(scheduler_new_orchid_pool_tree_path("gpu") + "/node_count") == TestDryRunGpuSchedulingPolicyMultiModule.NUM_NODES)

    @authors("yaishenka")
    def test_simple_full_host_vanilla(self):
        op = run_sleeping_vanilla(
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
        )

        wait(lambda: len(op.get_running_jobs()) == 1)

        wait_for_operations_in_orchid(operation_count=1)
        wait_for_assignments_in_orchid(op, assignment_count=1)

        operation = get_operation_from_orchid(op)
        check_operation(
            operation=operation,
            is_gang=False,
            group_name="task",
            allocation_count=1,
            min_needed_gpu_per_allocation=8,
            assigned_gpu_usage=8,
            assignment_count=1,
            enabled=True,
        )

        assignment = operation["assignments"][0]
        check_assignment(assignment, op.id, "task", 8, preemptible=False)

        node_address = assignment["node_address"]
        node = get(scheduler_new_orchid_pool_tree_path("gpu") + f"/gpu_assignment_plan/nodes/{node_address}")
        assert len(node["assignments"]) == 1
        assert node["assignments"][0] == assignment

    @authors("yaishenka")
    def test_specified_modules(self):
        op = run_sleeping_vanilla(
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
            spec={"scheduling_modules": ["VLA"]},
        )

        wait(lambda: len(op.get_running_jobs()) == 1)

        wait_for_operations_in_orchid(operation_count=1)
        wait_for_assignments_in_orchid(op, assignment_count=1)

        operation = get_operation_from_orchid(op)
        check_operation(
            operation=operation,
            is_gang=False,
            group_name="task",
            allocation_count=1,
            min_needed_gpu_per_allocation=8,
            assigned_gpu_usage=8,
            assignment_count=1,
            enabled=True,
            scheduling_module="VLA"
        )

        assignment = operation["assignments"][0]
        check_assignment(assignment, op.id, "task", 8, preemptible=False)

        node_address = assignment["node_address"]
        node = get(scheduler_new_orchid_pool_tree_path("gpu") + f"/gpu_assignment_plan/nodes/{node_address}")
        assert len(node["assignments"]) == 1
        assert node["assignments"][0] == assignment
        assert node["scheduling_module"] == "VLA"

    @authors("yaishenka")
    def test_gang_schedule_to_one_module(self):
        update_pool_tree_config_option("gpu", "enable_step_function_for_gang_operations", False)
        op = run_sleeping_vanilla(
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
            spec={"is_gang": True},
            job_count=2,
        )

        wait(lambda: len(op.get_running_jobs()) == 2)

        wait_for_operations_in_orchid(operation_count=1)
        wait_for_assignments_in_orchid(op, assignment_count=2)

        operation = get_operation_from_orchid(op)
        check_operation(
            operation=operation,
            is_gang=True,
            group_name="task",
            allocation_count=2,
            min_needed_gpu_per_allocation=8,
            assigned_gpu_usage=16,
            assignment_count=2,
            enabled=True,
        )

        modules = builtins.set()
        for assignment in operation["assignments"]:
            node_address = assignment["node_address"]
            node = get(scheduler_new_orchid_pool_tree_path("gpu") + f"/gpu_assignment_plan/nodes/{node_address}")
            modules.add(node["scheduling_module"])

        assert len(modules) == 1

        assert operation["scheduling_module"] == list(modules)[0]

    @authors("yaishenka")
    def test_cant_schedule_to_specified_module(self):
        update_pool_tree_config_option("gpu", "enable_step_function_for_gang_operations", False)
        op1 = run_sleeping_vanilla(
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
            spec={
                "is_gang": True,
                "scheduling_modules": ["VLA"],
            },
            job_count=2,
        )
        op2 = run_sleeping_vanilla(
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
            spec={
                "is_gang": True,
                "scheduling_modules": ["VLA"],
            },
            job_count=2,
        )

        wait(lambda: len(op1.get_running_jobs()) == 2)

        wait_for_operations_in_orchid(operation_count=2)

        operation1 = get_operation_from_orchid(op1)
        operation2 = get_operation_from_orchid(op2)

        assert len(operation1["assignments"]) + len(operation2["assignments"]) == 2

    @authors("yaishenka")
    def test_fill_up_one_module_first(self):
        operations = [
            run_sleeping_vanilla(
                task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
                job_count=1,
            ),
            run_sleeping_vanilla(
                task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
                job_count=1,
            ),
        ]
        for op in operations:
            wait(lambda: len(op.get_running_jobs()) == 1)

        wait_for_operations_in_orchid(operation_count=len(operations))

        modules = builtins.set()
        for op in operations:
            operation = get_operation_from_orchid(op)
            for assignment in operation["assignments"]:
                node_address = assignment["node_address"]
                node = get(scheduler_new_orchid_pool_tree_path("gpu") + f"/gpu_assignment_plan/nodes/{node_address}")
                modules.add(node["scheduling_module"])

        assert len(modules) == 1

    @authors("yaishenka")
    def test_ban_node_haha(self):
        update_scheduler_config("node_registration_timeout", 1000)
        update_scheduler_config("node_heartbeat_timeout", 1000)
        update_scheduler_config("node_reconnection_timeout", 1000)

        op = run_sleeping_vanilla(
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
        )

        wait(lambda: len(op.get_running_jobs()) == 1)

        wait_for_operations_in_orchid(operation_count=1)
        wait_for_assignments_in_orchid(op, assignment_count=1)

        def check_for_new_assignment(op, assignment_node_address):
            operation = get_operation_from_orchid(op)
            assignments = operation["assignments"]

            if len(assignments) == 0:
                return False

            assignment = assignments[0]
            return assignment_node_address != assignment["node_address"]

        operation = get_operation_from_orchid(op)
        assignment = operation["assignments"][0]
        node_address = assignment["node_address"]
        assignment_node_address = assignment["node_address"]
        module = operation["scheduling_module"]

        set_node_banned(node_address, True)

        wait(lambda: check_for_new_assignment(op, assignment_node_address))
        operation = get_operation_from_orchid(op)
        assert operation["scheduling_module"] == module
