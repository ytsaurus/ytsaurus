import builtins
import time

import yt.yson as yson

from yt_env_setup import (
    YTEnvSetup,
    Restarter,
    CONTROLLER_AGENTS_SERVICE,
    SCHEDULERS_SERVICE,
)

from yt_commands import (
    authors, create, wait, write_table, ls, get, create_data_center, create_rack, run_sleeping_vanilla, update_pool_tree_config,
    update_pool_tree_config_option, create_pool_tree, exists, map, update_scheduler_config, create_pool, set_node_banned, set,
    run_test_vanilla, with_breakpoint, release_breakpoint, get_allocation_id_from_job_id, vanilla, update_op_parameters, update_controller_agent_config,
)

from yt_scheduler_helpers import (
    scheduler_orchid_path, scheduler_orchid_node_path, scheduler_new_orchid_pool_tree_path, scheduler_orchid_pool_path,
    scheduler_orchid_operation_path
)

from yt.test_helpers import are_almost_equal

from yt_gpu_scheduler_helpers import (
    get_operation_from_gpu_policy_orchid, get_node_from_gpu_policy_orchid, get_operation_gpu_assignments_from_gpu_policy_orchid,
    wait_for_operations_in_gpu_policy_orchid, wait_for_assignments_in_gpu_policy_orchid, check_assignment_from_gpu_policy_orchid, check_operation_from_gpu_policy_orchid,
    check_gpu_allocations_from_gpu_policy_orchid, wait_for_gpu_allocations_empty_in_gpu_policy_orchid,
)


##################################################################

def wait_operation_unregistered(operation_id, tree="gpu"):
    wait(lambda: not exists(scheduler_orchid_operation_path(operation_id, tree=tree)))


def compare_assignment_without_preemptible_start_time(assignment1, assignment2):
    lhs = dict(assignment1)
    lhs.pop("preemptible_progress_start_time")

    rhs = dict(assignment2)
    rhs.pop("preemptible_progress_start_time")

    assert lhs == rhs


##################################################################

# TODO(YT-27869): Add Preemption tests.
class AllocatingGpuSchedulingPolicyBaseConfig(YTEnvSetup):
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
        super(AllocatingGpuSchedulingPolicyBaseConfig, self).setup_method(method)

        update_pool_tree_config("default", {"node_tag_filter": "!gpu"})
        create_pool_tree("gpu", config={
            "node_tag_filter": "gpu",
            "main_resource": "gpu",
            "gpu_scheduling_policy": {
                "mode": "allocating",
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
            "policy_kind": "gpu",
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

class TestAllocationGpuSchedulingPolicy(AllocatingGpuSchedulingPolicyBaseConfig):
    @authors("yaishenka")
    def test_simple(self):
        op = run_test_vanilla(
            with_breakpoint("BREAKPOINT"),
            task_patch={"gpu_limit": 1, "enable_gpu_layers": False},
        )

        wait(lambda: len(op.get_running_jobs()) == 1)
        wait(lambda: get(scheduler_orchid_operation_path(op.id, tree="gpu") + "/resource_usage/gpu", default=None) == 1)

        wait(lambda: get(scheduler_orchid_operation_path(op.id, tree="gpu") + "/grouped_needed_resources", default=None) == {})

        wait_for_operations_in_gpu_policy_orchid(operation_count=1)
        wait_for_assignments_in_gpu_policy_orchid(op, assignment_count=1, exactly=True)

        operation = get_operation_from_gpu_policy_orchid(op)
        check_operation_from_gpu_policy_orchid(
            operation=operation,
            is_gang=False,
            group_name="task",
            allocation_count=1,
            min_needed_gpu_per_allocation=1,
            assigned_gpu_usage=1,
            assignment_count=1,
            enabled=True,
        )

        job_id = list(op.get_running_jobs())[0]
        allocation_id = get_allocation_id_from_job_id(job_id)
        assignment = operation["assignments"][0]
        check_assignment_from_gpu_policy_orchid(
            assignment=assignment,
            operation_id=op.id,
            group_name="task",
            gpu_usage=1,
            preemptible=False,
            allocation_id=allocation_id)
        check_gpu_allocations_from_gpu_policy_orchid(operation["allocations"], [allocation_id], 1)

        node_address = assignment["node_address"]
        assert node_address in ls("//sys/cluster_nodes")
        node = get(scheduler_new_orchid_pool_tree_path("gpu") + f"/gpu_assignment_plan/nodes/{node_address}")
        assert node["assigned_resource_usage"]["gpu"] == 1
        assert node["resource_limits"]["gpu"] == 8
        assert node["scheduling_module"] == AllocatingGpuSchedulingPolicyBaseConfig.DATA_CENTER

        assert len(get(scheduler_new_orchid_pool_tree_path("gpu") + f"/gpu_assignment_plan/nodes/{node_address}/assignments")) == 1
        node_assignment = get(scheduler_new_orchid_pool_tree_path("gpu") + f"/gpu_assignment_plan/nodes/{node_address}/assignments")[0]

        compare_assignment_without_preemptible_start_time(node_assignment, assignment)

        release_breakpoint()

        wait(lambda: get(scheduler_orchid_operation_path(op.id, tree="gpu") + "/resource_usage/gpu", default=None) == 0)
        wait(lambda: len(get(scheduler_new_orchid_pool_tree_path("gpu") + f"/gpu_assignment_plan/nodes/{node_address}/assignments")) == 0)
        wait_for_gpu_allocations_empty_in_gpu_policy_orchid(op)

    @authors("yaishenka")
    def test_simple_full_host(self):
        op = run_test_vanilla(
            with_breakpoint("BREAKPOINT"),
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
        )

        wait(lambda: len(op.get_running_jobs()) == 1)

        wait(lambda: get(scheduler_orchid_operation_path(op.id, tree="gpu") + "/resource_usage/gpu", default=None) == 8)

        wait(lambda: get(scheduler_orchid_operation_path(op.id, tree="gpu") + "/grouped_needed_resources", default=None) == {})

        wait_for_operations_in_gpu_policy_orchid(operation_count=1)
        wait_for_assignments_in_gpu_policy_orchid(op, assignment_count=1, exactly=True)

        operation = get_operation_from_gpu_policy_orchid(op)
        check_operation_from_gpu_policy_orchid(
            operation=operation,
            is_gang=False,
            group_name="task",
            allocation_count=1,
            min_needed_gpu_per_allocation=8,
            assigned_gpu_usage=8,
            assignment_count=1,
            enabled=True,
        )

        job_id = list(op.get_running_jobs())[0]
        allocation_id = get_allocation_id_from_job_id(job_id)
        assignment = operation["assignments"][0]
        check_assignment_from_gpu_policy_orchid(
            assignment=assignment,
            operation_id=op.id,
            group_name="task",
            gpu_usage=8,
            preemptible=False,
            allocation_id=allocation_id)
        check_gpu_allocations_from_gpu_policy_orchid(operation["allocations"], [allocation_id], 8)

        node_address = assignment["node_address"]
        node = get(scheduler_new_orchid_pool_tree_path("gpu") + f"/gpu_assignment_plan/nodes/{node_address}")
        assert len(node["assignments"]) == 1
        compare_assignment_without_preemptible_start_time(node["assignments"][0], assignment)

        release_breakpoint()

        wait(lambda: get(scheduler_orchid_operation_path(op.id, tree="gpu") + "/resource_usage/gpu", default=None) == 0)
        wait(lambda: len(get(scheduler_new_orchid_pool_tree_path("gpu") + f"/gpu_assignment_plan/nodes/{node_address}/assignments")) == 0)
        wait_for_gpu_allocations_empty_in_gpu_policy_orchid(op)

    @authors("yaishenka")
    def test_simple_two_jobs(self):
        op = run_test_vanilla(
            with_breakpoint("BREAKPOINT"),
            task_patch={"gpu_limit": 1, "enable_gpu_layers": False},
            job_count=2,
        )

        wait(lambda: len(op.get_running_jobs()) == 2)

        wait(lambda: get(scheduler_orchid_operation_path(op.id, tree="gpu") + "/resource_usage/gpu", default=None) == 2)

        wait(lambda: get(scheduler_orchid_operation_path(op.id, tree="gpu") + "/grouped_needed_resources", default=None) == {})

        wait_for_operations_in_gpu_policy_orchid(operation_count=1)
        wait_for_assignments_in_gpu_policy_orchid(op, assignment_count=2, exactly=True)

        operation = get_operation_from_gpu_policy_orchid(op)
        check_operation_from_gpu_policy_orchid(
            operation=operation,
            is_gang=False,
            group_name="task",
            allocation_count=2,
            min_needed_gpu_per_allocation=1,
            assigned_gpu_usage=2,
            assignment_count=2,
            enabled=True,
        )

        allocation_ids = [get_allocation_id_from_job_id(job_id) for job_id in list(op.get_running_jobs())]
        for assignment in operation["assignments"]:
            check_assignment_from_gpu_policy_orchid(
                assignment=assignment,
                operation_id=op.id,
                group_name="task",
                gpu_usage=1,
                preemptible=False)
            assert assignment["allocation_id"] in allocation_ids
        check_gpu_allocations_from_gpu_policy_orchid(operation["allocations"], allocation_ids, 1)

        node_address = assignment["node_address"]
        node = get(scheduler_new_orchid_pool_tree_path("gpu") + f"/gpu_assignment_plan/nodes/{node_address}")
        assert len(node["assignments"]) == 2
        for assignment in node["assignments"]:
            check_assignment_from_gpu_policy_orchid(
                assignment=assignment,
                operation_id=op.id,
                group_name="task",
                gpu_usage=1,
                preemptible=False)
            assert assignment["allocation_id"] in allocation_ids

        release_breakpoint()

        wait(lambda: get(scheduler_orchid_operation_path(op.id, tree="gpu") + "/resource_usage/gpu", default=None) == 0)
        wait(lambda: len(get(scheduler_new_orchid_pool_tree_path("gpu") + f"/gpu_assignment_plan/nodes/{node_address}/assignments")) == 0)
        wait_for_gpu_allocations_empty_in_gpu_policy_orchid(op)

    @authors("yaishenka")
    def test_simple_two_jobs_full_host(self):
        op = run_test_vanilla(
            with_breakpoint("BREAKPOINT"),
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
            job_count=2,
        )

        wait(lambda: len(op.get_running_jobs()) == 2)

        wait(lambda: get(scheduler_orchid_operation_path(op.id, tree="gpu") + "/resource_usage/gpu", default=None) == 16)

        wait(lambda: get(scheduler_orchid_operation_path(op.id, tree="gpu") + "/grouped_needed_resources", default=None) == {})

        wait_for_operations_in_gpu_policy_orchid(operation_count=1)
        wait_for_assignments_in_gpu_policy_orchid(op, assignment_count=2, exactly=True)

        operation = get_operation_from_gpu_policy_orchid(op)
        check_operation_from_gpu_policy_orchid(
            operation=operation,
            is_gang=False,
            group_name="task",
            allocation_count=2,
            min_needed_gpu_per_allocation=8,
            assigned_gpu_usage=16,
            assignment_count=2,
            enabled=True,
        )

        allocation_ids = [get_allocation_id_from_job_id(job_id) for job_id in list(op.get_running_jobs())]
        for assignment in operation["assignments"]:
            check_assignment_from_gpu_policy_orchid(
                assignment=assignment,
                operation_id=op.id,
                group_name="task",
                gpu_usage=8,
                preemptible=False)
            assert assignment["allocation_id"] in allocation_ids

            node_address = assignment["node_address"]
            node = get(scheduler_new_orchid_pool_tree_path("gpu") + f"/gpu_assignment_plan/nodes/{node_address}")
            assert len(node["assignments"]) == 1
            for node_assignment in node["assignments"]:
                compare_assignment_without_preemptible_start_time(node_assignment, assignment)
        check_gpu_allocations_from_gpu_policy_orchid(operation["allocations"], allocation_ids, 8)

        release_breakpoint()

        wait(lambda: get(scheduler_orchid_operation_path(op.id, tree="gpu") + "/resource_usage/gpu", default=None) == 0)
        wait_for_gpu_allocations_empty_in_gpu_policy_orchid(op)

    @authors("yaishenka")
    def test_simple_two_ops(self):
        op1 = run_test_vanilla(
            with_breakpoint("BREAKPOINT"),
            task_patch={"gpu_limit": 1, "enable_gpu_layers": False},
            job_count=1,
        )
        op2 = run_test_vanilla(
            with_breakpoint("BREAKPOINT"),
            task_patch={"gpu_limit": 1, "enable_gpu_layers": False},
            job_count=1,
        )

        wait(lambda: len(op1.get_running_jobs()) == 1)
        wait(lambda: len(op2.get_running_jobs()) == 1)

        wait(lambda: get(scheduler_orchid_operation_path(op1.id, tree="gpu") + "/resource_usage/gpu", default=None) == 1)
        wait(lambda: get(scheduler_orchid_operation_path(op2.id, tree="gpu") + "/resource_usage/gpu", default=None) == 1)

        wait(lambda: get(scheduler_orchid_operation_path(op1.id, tree="gpu") + "/grouped_needed_resources", default=None) == {})
        wait(lambda: get(scheduler_orchid_operation_path(op2.id, tree="gpu") + "/grouped_needed_resources", default=None) == {})

        wait_for_operations_in_gpu_policy_orchid(operation_count=2)
        wait_for_assignments_in_gpu_policy_orchid(op1, assignment_count=1, exactly=True)
        wait_for_assignments_in_gpu_policy_orchid(op2, assignment_count=1, exactly=True)

        for op in [op1, op2]:
            operation = get_operation_from_gpu_policy_orchid(op)
            check_operation_from_gpu_policy_orchid(
                operation=operation,
                is_gang=False,
                group_name="task",
                allocation_count=1,
                min_needed_gpu_per_allocation=1,
                assigned_gpu_usage=1,
                assignment_count=1,
                enabled=True,
            )

            job_id = list(op.get_running_jobs())[0]
            allocation_id = get_allocation_id_from_job_id(job_id)

            assert len(operation["assignments"]) == 1
            assignment = operation["assignments"][0]
            check_assignment_from_gpu_policy_orchid(
                assignment=assignment,
                operation_id=op.id,
                group_name="task",
                gpu_usage=1,
                preemptible=False,
                allocation_id=allocation_id)
            check_gpu_allocations_from_gpu_policy_orchid(operation["allocations"], [allocation_id], 1)

        release_breakpoint()

        wait_operation_unregistered(op1.id)
        wait_operation_unregistered(op2.id)

    @authors("yaishenka")
    def test_two_ops_full_host(self):
        op1 = run_test_vanilla(
            with_breakpoint("BREAKPOINT"),
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
            job_count=1,
        )
        op2 = run_test_vanilla(
            with_breakpoint("BREAKPOINT"),
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
            job_count=1,
        )

        wait(lambda: len(op1.get_running_jobs()) == 1)
        wait(lambda: len(op2.get_running_jobs()) == 1)

        wait(lambda: get(scheduler_orchid_operation_path(op1.id, tree="gpu") + "/resource_usage/gpu", default=None) == 8)
        wait(lambda: get(scheduler_orchid_operation_path(op2.id, tree="gpu") + "/resource_usage/gpu", default=None) == 8)

        wait(lambda: get(scheduler_orchid_operation_path(op1.id, tree="gpu") + "/grouped_needed_resources", default=None) == {})
        wait(lambda: get(scheduler_orchid_operation_path(op2.id, tree="gpu") + "/grouped_needed_resources", default=None) == {})

        wait_for_operations_in_gpu_policy_orchid(operation_count=2)
        wait_for_assignments_in_gpu_policy_orchid(op1, assignment_count=1, exactly=True)
        wait_for_assignments_in_gpu_policy_orchid(op2, assignment_count=1, exactly=True)

        for op in [op1, op2]:
            operation = get_operation_from_gpu_policy_orchid(op)
            check_operation_from_gpu_policy_orchid(
                operation=operation,
                is_gang=False,
                group_name="task",
                allocation_count=1,
                min_needed_gpu_per_allocation=8,
                assigned_gpu_usage=8,
                assignment_count=1,
                enabled=True,
            )

            job_id = list(op.get_running_jobs())[0]
            allocation_id = get_allocation_id_from_job_id(job_id)

            assert len(operation["assignments"]) == 1
            assignment = operation["assignments"][0]
            check_assignment_from_gpu_policy_orchid(
                assignment=assignment,
                operation_id=op.id,
                group_name="task",
                gpu_usage=8,
                preemptible=False,
                allocation_id=allocation_id)
            check_gpu_allocations_from_gpu_policy_orchid(operation["allocations"], [allocation_id], 8)

        release_breakpoint()

        wait_operation_unregistered(op1.id)
        wait_operation_unregistered(op2.id)

    @authors("yaishenka")
    def test_vanilla_gang(self):
        update_pool_tree_config_option("gpu", "enable_step_function_for_gang_operations", False)
        op = run_test_vanilla(
            with_breakpoint("BREAKPOINT"),
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
            spec={"is_gang": True},
            job_count=2,
        )

        wait(lambda: len(op.get_running_jobs()) == 2)
        wait(lambda: get(scheduler_orchid_operation_path(op.id, tree="gpu") + "/resource_usage/gpu", default=None) == 16)

        wait(lambda: get(scheduler_orchid_pool_path("root", tree="gpu") + "/resource_demand/gpu") == 16)

        wait(lambda: get(scheduler_orchid_operation_path(op.id, tree="gpu") + "/grouped_needed_resources", default=None) == {})

        wait_for_operations_in_gpu_policy_orchid(operation_count=1)
        wait_for_assignments_in_gpu_policy_orchid(op, assignment_count=2, exactly=True)

        operation = get_operation_from_gpu_policy_orchid(op)
        check_operation_from_gpu_policy_orchid(
            operation=operation,
            is_gang=True,
            group_name="task",
            allocation_count=2,
            min_needed_gpu_per_allocation=8,
            assigned_gpu_usage=16,
            assignment_count=2,
            enabled=True,
        )

        allocation_ids = [get_allocation_id_from_job_id(job_id) for job_id in list(op.get_running_jobs())]

        for assignment in operation["assignments"]:
            check_assignment_from_gpu_policy_orchid(
                assignment=assignment,
                operation_id=op.id,
                group_name="task",
                gpu_usage=8,
                preemptible=False)
            assert assignment["allocation_id"] in allocation_ids

            node_address = assignment["node_address"]
            node = get(scheduler_new_orchid_pool_tree_path("gpu") + f"/gpu_assignment_plan/nodes/{node_address}")
            assert len(node["assignments"]) == 1
            for node_assignment in node["assignments"]:
                compare_assignment_without_preemptible_start_time(node_assignment, assignment)
        check_gpu_allocations_from_gpu_policy_orchid(operation["allocations"], allocation_ids, 8)

        release_breakpoint()

        wait(lambda: get(scheduler_orchid_operation_path(op.id, tree="gpu") + "/resource_usage/gpu", default=None) == 0)
        wait_for_gpu_allocations_empty_in_gpu_policy_orchid(op)

    @authors("yaishenka")
    def test_operation_cant_schedule(self):
        op = run_sleeping_vanilla(
            task_patch={"gpu_limit": 9, "enable_gpu_layers": False},
            job_count=1,
        )

        wait(lambda: exists(scheduler_new_orchid_pool_tree_path("gpu") + "/gpu_assignment_plan"))

        time.sleep(2)

        assert len(get(scheduler_new_orchid_pool_tree_path("gpu") + f"/gpu_assignment_plan/operations/{op.id}/assignments")) == 0

    @authors("yaishenka")
    def test_simple_map(self):
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")
        write_table("//tmp/t_in", {"foo": "bar"})

        op = map(
            track=False,
            in_="//tmp/t_in",
            out="//tmp/t_out",
            command=with_breakpoint("BREAKPOINT"),
            spec={
                "mapper": {
                    "job_count": 1,
                    "gpu_limit": 1,
                    "enable_gpu_layers": False,
                },
            },
        )

        wait(lambda: len(op.get_running_jobs()) == 1)
        wait(lambda: get(scheduler_orchid_operation_path(op.id, tree="gpu") + "/resource_usage/gpu", default=None) == 1)

        wait(lambda: get(scheduler_orchid_operation_path(op.id, tree="gpu") + "/grouped_needed_resources", default=None) == {})

        wait_for_operations_in_gpu_policy_orchid(operation_count=1)
        wait_for_assignments_in_gpu_policy_orchid(op, assignment_count=1, exactly=True)

        operation = get_operation_from_gpu_policy_orchid(op)
        check_operation_from_gpu_policy_orchid(
            operation=operation,
            is_gang=False,
            group_name="map",
            allocation_count=1,
            min_needed_gpu_per_allocation=1,
            assigned_gpu_usage=1,
            assignment_count=1,
            enabled=True,
        )

        job_id = list(op.get_running_jobs())[0]
        allocation_id = get_allocation_id_from_job_id(job_id)

        assert len(operation["assignments"]) == 1
        assignment = operation["assignments"][0]
        check_assignment_from_gpu_policy_orchid(
            assignment=assignment,
            operation_id=op.id,
            group_name="map",
            gpu_usage=1,
            preemptible=False,
            allocation_id=allocation_id)
        check_gpu_allocations_from_gpu_policy_orchid(operation["allocations"], [allocation_id], 1)

        node_address = assignment["node_address"]
        node = get(scheduler_new_orchid_pool_tree_path("gpu") + f"/gpu_assignment_plan/nodes/{node_address}")
        assert len(node["assignments"]) == 1
        for assignment in node["assignments"]:
            check_assignment_from_gpu_policy_orchid(
                assignment=assignment,
                operation_id=op.id,
                group_name="map",
                gpu_usage=1,
                preemptible=False)

        release_breakpoint()

        wait(lambda: get(scheduler_orchid_operation_path(op.id, tree="gpu") + "/resource_usage/gpu", default=None) == 0)
        wait_for_gpu_allocations_empty_in_gpu_policy_orchid(op)

    @authors("yaishenka")
    def test_simple_fullhost_map(self):
        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out")
        write_table("//tmp/t_in", {"foo": "bar"})

        op = map(
            track=False,
            in_="//tmp/t_in",
            out="//tmp/t_out",
            command=with_breakpoint("BREAKPOINT"),
            spec={
                "mapper": {
                    "job_count": 1,
                    "gpu_limit": 8,
                    "enable_gpu_layers": False,
                },
            },
        )

        wait(lambda: len(op.get_running_jobs()) == 1)
        wait(lambda: get(scheduler_orchid_operation_path(op.id, tree="gpu") + "/resource_usage/gpu", default=None) == 8)

        wait(lambda: get(scheduler_orchid_operation_path(op.id, tree="gpu") + "/grouped_needed_resources", default=None) == {})

        wait_for_operations_in_gpu_policy_orchid(operation_count=1)
        wait_for_assignments_in_gpu_policy_orchid(op, assignment_count=1, exactly=True)

        operation = get_operation_from_gpu_policy_orchid(op)
        check_operation_from_gpu_policy_orchid(
            operation=operation,
            is_gang=False,
            group_name="map",
            allocation_count=1,
            min_needed_gpu_per_allocation=8,
            assigned_gpu_usage=8,
            assignment_count=1,
            enabled=True,
        )

        job_id = list(op.get_running_jobs())[0]
        allocation_id = get_allocation_id_from_job_id(job_id)

        assert len(operation["assignments"]) == 1
        assignment = operation["assignments"][0]
        check_assignment_from_gpu_policy_orchid(
            assignment=assignment,
            operation_id=op.id,
            group_name="map",
            gpu_usage=8,
            preemptible=False,
            allocation_id=allocation_id)
        check_gpu_allocations_from_gpu_policy_orchid(operation["allocations"], [allocation_id], 8)

        release_breakpoint()

        wait(lambda: get(scheduler_orchid_operation_path(op.id, tree="gpu") + "/resource_usage/gpu", default=None) == 0)
        wait_for_gpu_allocations_empty_in_gpu_policy_orchid(op)

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
            command=with_breakpoint("BREAKPOINT"),
            spec={
                "job_count": 2,
                "mapper": {
                    "gpu_limit": 8,
                    "enable_gpu_layers": False,
                },
            }
        )

        wait(lambda: len(op.get_running_jobs()) == 2)
        wait(lambda: get(scheduler_orchid_operation_path(op.id, tree="gpu") + "/resource_usage/gpu", default=None) == 16)

        wait(lambda: get(scheduler_orchid_operation_path(op.id, tree="gpu") + "/grouped_needed_resources", default=None) == {})

        wait_for_operations_in_gpu_policy_orchid(operation_count=1)
        wait_for_assignments_in_gpu_policy_orchid(op, assignment_count=2, exactly=True)

        operation = get_operation_from_gpu_policy_orchid(op)
        check_operation_from_gpu_policy_orchid(
            operation=operation,
            is_gang=False,
            group_name="map",
            allocation_count=2,
            min_needed_gpu_per_allocation=8,
            assigned_gpu_usage=16,
            assignment_count=2,
            enabled=True,
        )

        allocation_ids = [get_allocation_id_from_job_id(job_id) for job_id in list(op.get_running_jobs())]

        for assignment in operation["assignments"]:
            check_assignment_from_gpu_policy_orchid(
                assignment=assignment,
                operation_id=op.id,
                group_name="map",
                gpu_usage=8,
                preemptible=False)
            assert assignment["allocation_id"] in allocation_ids
        check_gpu_allocations_from_gpu_policy_orchid(operation["allocations"], allocation_ids, 8)

        release_breakpoint()

        wait(lambda: get(scheduler_orchid_operation_path(op.id, tree="gpu") + "/resource_usage/gpu", default=None) == 0)
        wait_for_gpu_allocations_empty_in_gpu_policy_orchid(op)

    @authors("yaishenka")
    def test_tag_filters(self):
        update_pool_tree_config_option("gpu", "enable_step_function_for_gang_operations", False)

        nodes = list(ls("//sys/cluster_nodes"))

        first_node_only = run_sleeping_vanilla(
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
            job_count=1,
            spec={"scheduling_tag_filter": nodes[0]},
        )
        wait(lambda: len(first_node_only.get_running_jobs()) == 1)
        wait(lambda: get(scheduler_orchid_operation_path(first_node_only.id, tree="gpu") + "/resource_usage/gpu", default=None) == 8)

        wait(lambda: get(scheduler_orchid_operation_path(first_node_only.id, tree="gpu") + "/grouped_needed_resources", default=None) == {})

        waits_for_first = run_sleeping_vanilla(
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
            job_count=1,
            spec={"scheduling_tag_filter": nodes[0]},
        )

        wait_for_operations_in_gpu_policy_orchid(operation_count=2)
        wait_for_assignments_in_gpu_policy_orchid(first_node_only, assignment_count=1, exactly=True)

        node1 = get_node_from_gpu_policy_orchid(nodes[0])
        assert len(node1["assignments"]) == 1
        assert node1["assignments"][0]["operation_id"] == first_node_only.id

        time.sleep(2)
        assert len(get_operation_gpu_assignments_from_gpu_policy_orchid(waits_for_first)) == 0

        any_node = run_sleeping_vanilla(
            task_patch={"gpu_limit": 7, "enable_gpu_layers": False},
            job_count=1,
        )
        wait(lambda: len(any_node.get_running_jobs()) == 1)
        wait(lambda: get(scheduler_orchid_operation_path(any_node.id, tree="gpu") + "/resource_usage/gpu", default=None) == 7)

        wait(lambda: get(scheduler_orchid_operation_path(any_node.id, tree="gpu") + "/grouped_needed_resources", default=None) == {})

        wait_for_operations_in_gpu_policy_orchid(operation_count=3)
        wait_for_assignments_in_gpu_policy_orchid(any_node, assignment_count=1, exactly=True)

        node2 = get_node_from_gpu_policy_orchid(nodes[1])
        assert len(node2["assignments"]) == 1
        assert node2["assignments"][0]["operation_id"] == any_node.id

    @authors("yaishenka")
    def test_schedule_above_fair_share(self):
        create_pool(
            "haha_pool",
            pool_tree="gpu",
            attributes={"mode": "fifo"},
            wait_for_orchid=False,
        )
        op1 = run_sleeping_vanilla(
            task_patch={"gpu_limit": 5, "enable_gpu_layers": False},
            job_count=3,
            spec={
                "pool": "haha_pool",
            },
        )

        wait(lambda: len(op1.get_running_jobs()) == 2)
        wait(lambda: get(scheduler_orchid_operation_path(op1.id, tree="gpu") + "/resource_usage/gpu", default=None) == 10)

        op2 = run_sleeping_vanilla(
            task_patch={"gpu_limit": 3, "enable_gpu_layers": False},
            job_count=1,
            spec={
                "testing": {"delay_inside_materialize": 100},
                "pool": "haha_pool",
            },
        )

        wait(lambda: len(op2.get_running_jobs()) == 1)
        wait(lambda: get(scheduler_orchid_operation_path(op2.id, tree="gpu") + "/resource_usage/gpu", default=None) == 3)

        wait(lambda: get(scheduler_orchid_operation_path(op2.id, tree="gpu") + "/grouped_needed_resources", default=None) == {})

        wait_for_operations_in_gpu_policy_orchid(operation_count=2)
        wait_for_assignments_in_gpu_policy_orchid(op2, assignment_count=1, exactly=True)

        job_id = list(op2.get_running_jobs())[0]
        allocation_id = get_allocation_id_from_job_id(job_id)
        assignment = get_operation_gpu_assignments_from_gpu_policy_orchid(op2)[0]
        check_assignment_from_gpu_policy_orchid(
            assignment=assignment,
            operation_id=op2.id,
            group_name="task",
            gpu_usage=3,
            preemptible=True,
            allocation_id=allocation_id)

    @authors("yaishenka")
    def test_starving(self):
        update_pool_tree_config_option("gpu", "enable_step_function_for_gang_operations", False)

        nodes = list(ls("//sys/cluster_nodes"))

        op = run_sleeping_vanilla(
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
            job_count=2,
            spec={"scheduling_tag_filter": nodes[0]},
        )
        wait(lambda: len(op.get_running_jobs()) == 1)
        wait(lambda: get(scheduler_orchid_operation_path(op.id, tree="gpu") + "/resource_usage/gpu", default=None) == 8)
        wait(lambda: are_almost_equal(get(scheduler_orchid_operation_path(op.id, tree="gpu") + "/fair_resources/gpu"), 16))
        wait(lambda: get(scheduler_orchid_operation_path(op.id, tree="gpu") + "/starvation_status") == "starving")

        wait(lambda: get_operation_from_gpu_policy_orchid(op)["starving"])

    # Just test that in theory it also works with CPU trees.
    @authors("yaishenka")
    def test_simple_cpu_tree(self):
        update_pool_tree_config("default", {"node_tag_filter": "!gpu & !cpu"})
        create_pool_tree("cpu", config={
            "node_tag_filter": "cpu",
            "gpu_scheduling_policy": {
                "mode": "allocating",
                "plan_update_period": 100,
                "module_type": "data_center",
                "modules": [TestAllocationGpuSchedulingPolicy.DATA_CENTER],
                "full_host_aggressive_preemption_timeout": 1000,
            },
            "preemptive_scheduling_backoff": 0,
            "fair_share_starvation_timeout": 100,
            "fair_share_starvation_tolerance": 0.95,
            "preemption_satisfaction_threshold": 0.99,
            "non_preemptible_resource_usage_threshold": {"user_slots": 0},
            "policy_kind": "gpu",
        })

        for node in ls("//sys/cluster_nodes"):
            set("//sys/cluster_nodes/{}/@user_tags".format(node), ["cpu"])
        wait(lambda: get(scheduler_new_orchid_pool_tree_path("cpu") + "/node_count") == TestAllocationGpuSchedulingPolicy.NUM_NODES)

        op = run_test_vanilla(
            with_breakpoint("BREAKPOINT"),
            spec={"pool_trees": ["cpu"]}
        )

        wait(lambda: len(op.get_running_jobs()) == 1)
        wait(lambda: get(scheduler_orchid_operation_path(op.id, tree="cpu") + "/resource_usage/cpu", default=None) == 1.0)

        wait(lambda: get(scheduler_orchid_operation_path(op.id, tree="cpu") + "/grouped_needed_resources", default=None) == {})

        wait(lambda: exists(scheduler_new_orchid_pool_tree_path("cpu") + "/gpu_assignment_plan"))

        wait_for_operations_in_gpu_policy_orchid(operation_count=1, tree="cpu")
        wait_for_assignments_in_gpu_policy_orchid(op, tree="cpu", assignment_count=1, exactly=True)

        operation = get_operation_from_gpu_policy_orchid(op, tree="cpu")
        check_operation_from_gpu_policy_orchid(
            operation=operation,
            is_gang=False,
            group_name="task",
            allocation_count=1,
            min_needed_gpu_per_allocation=0,
            assigned_gpu_usage=0,
            assignment_count=1,
            enabled=True,
        )
        job_id = list(op.get_running_jobs())[0]
        allocation_id = get_allocation_id_from_job_id(job_id)
        assignment = operation["assignments"][0]
        check_assignment_from_gpu_policy_orchid(
            assignment=assignment,
            operation_id=op.id,
            group_name="task",
            gpu_usage=0,
            preemptible=False,
            allocation_id=allocation_id)

        node_address = assignment["node_address"]
        assert node_address in ls("//sys/cluster_nodes")
        node = get(scheduler_new_orchid_pool_tree_path("cpu") + f"/gpu_assignment_plan/nodes/{node_address}")
        assert node["assigned_resource_usage"]["cpu"] == 1.0
        assert node["resource_limits"]["cpu"] == 10.0
        assert node["scheduling_module"] == TestAllocationGpuSchedulingPolicy.DATA_CENTER

        assert len(get(scheduler_new_orchid_pool_tree_path("cpu") + f"/gpu_assignment_plan/nodes/{node_address}/assignments")) == 1
        node_assignment = get(scheduler_new_orchid_pool_tree_path("cpu") + f"/gpu_assignment_plan/nodes/{node_address}/assignments")[0]

        compare_assignment_without_preemptible_start_time(node_assignment, assignment)

        release_breakpoint()

        wait(lambda: get(scheduler_orchid_operation_path(op.id, tree="cpu") + "/resource_usage/cpu", default=None) == 0.0)

    @authors("yaishenka")
    def test_preempt_in_right_order(self):
        create_pool(
            "guaranteed_haha",
            pool_tree="gpu",
            attributes={
                "strong_guarantee_resources": {
                    "gpu": 12
                }
            },
        )
        create_pool(
            "no_guaranteed_haha",
            pool_tree="gpu",
            wait_for_orchid=False,
        )

        def get_preemptible_progress_start_time(op):
            assignments = get_operation_gpu_assignments_from_gpu_policy_orchid(op)
            if len(assignments) != 1:
                return None

            assignment = assignments[0]
            preemptible_progress_start_time = assignment["preemptible_progress_start_time"]

            if preemptible_progress_start_time == yson.YsonEntity():
                return None

            return preemptible_progress_start_time

        def check_for_preemptible_progress_start_time(op):
            if get_preemptible_progress_start_time(op) is None:
                return False
            return True

        op1 = run_sleeping_vanilla(
            task_patch={"gpu_limit": 4, "enable_gpu_layers": False},
            job_count=1,
            spec={
                "pool": "no_guaranteed_haha",
            },
        )

        wait(lambda: len(op1.get_running_jobs()) == 1)
        wait(lambda: get(scheduler_orchid_operation_path(op1.id, tree="gpu") + "/resource_usage/gpu", default=None) == 4)
        wait(lambda: get(scheduler_orchid_operation_path(op1.id, tree="gpu") + "/grouped_needed_resources", default=None) == {})
        wait(lambda: check_for_preemptible_progress_start_time(op1))

        # We need to sleep at least 1 second to generate diff between PreemptibleProgressStartTime
        time.sleep(1)

        op2 = run_sleeping_vanilla(
            task_patch={"gpu_limit": 4, "enable_gpu_layers": False},
            job_count=1,
            spec={
                "pool": "no_guaranteed_haha",
            },
        )

        wait(lambda: len(op2.get_running_jobs()) == 1)
        wait(lambda: get(scheduler_orchid_operation_path(op2.id, tree="gpu") + "/resource_usage/gpu", default=None) == 4)
        wait(lambda: get(scheduler_orchid_operation_path(op2.id, tree="gpu") + "/grouped_needed_resources", default=None) == {})
        wait(lambda: check_for_preemptible_progress_start_time(op2))

        op3 = run_sleeping_vanilla(
            task_patch={"gpu_limit": 4, "enable_gpu_layers": False},
            job_count=3,
            spec={
                "pool": "guaranteed_haha",
            },
        )

        wait(lambda: len(op3.get_running_jobs()) == 3)
        wait(lambda: get(scheduler_orchid_operation_path(op3.id, tree="gpu") + "/resource_usage/gpu", default=None) == 12)
        wait(lambda: get(scheduler_orchid_operation_path(op3.id, tree="gpu") + "/grouped_needed_resources", default=None) == {})

        wait(lambda: len(op2.get_running_jobs()) == 0)

    # See YT-27935: the GPU planner decides per-assignment which allocation group to schedule,
    # and tells the CA via the schedule request. The CA must only schedule jobs from the requested
    # group, even if a different group would also fit into the node's resource envelope.
    @authors("eshcherbin")
    def test_heterogeneous_operation(self):
        op = vanilla(
            track=False,
            spec={
                "tasks": {
                    "task_small": {
                        "job_count": 2,
                        "command": with_breakpoint("BREAKPOINT"),
                        "gpu_limit": 1,
                        "enable_gpu_layers": False,
                    },
                    "task_big": {
                        "job_count": 1,
                        "command": with_breakpoint("BREAKPOINT"),
                        "gpu_limit": 4,
                        "enable_gpu_layers": False,
                    },
                },
            },
        )

        wait(lambda: len(op.get_running_jobs()) == 3)
        wait(lambda: get(scheduler_orchid_operation_path(op.id, tree="gpu") + "/resource_usage/gpu", default=None) == 6)
        wait(lambda: get(scheduler_orchid_operation_path(op.id, tree="gpu") + "/grouped_needed_resources", default=None) == {})

        wait_for_operations_in_gpu_policy_orchid(operation_count=1)
        wait_for_assignments_in_gpu_policy_orchid(op, assignment_count=3, exactly=True)

        operation = get_operation_from_gpu_policy_orchid(op)
        assignments_by_group = {}
        for assignment in operation["assignments"]:
            assignments_by_group.setdefault(assignment["allocation_group_name"], []).append(assignment)

        assert len(assignments_by_group["task_small"]) == 2
        assert len(assignments_by_group["task_big"]) == 1
        for assignment in assignments_by_group["task_small"]:
            assert assignment["resource_usage"]["gpu"] == 1
        for assignment in assignments_by_group["task_big"]:
            assert assignment["resource_usage"]["gpu"] == 4

        # Every assignment must be realized (have an allocation_id) and the job the CA actually
        # started under that allocation must be from the task the planner asked for. If the CA
        # had picked a different task, the job's task_name (from the CA running_jobs orchid)
        # would not match the assignment's allocation_group_name.
        allocation_id_to_group_name = {}
        for assignment in operation["assignments"]:
            assert assignment["allocation_id"] != yson.YsonEntity()
            allocation_id_to_group_name[assignment["allocation_id"]] = assignment["allocation_group_name"]

        running_by_task = {"task_small": 0, "task_big": 0}
        for job_id, job_info in op.get_running_jobs().items():
            allocation_id = get_allocation_id_from_job_id(job_id)
            assert allocation_id in allocation_id_to_group_name
            assert job_info["task_name"] == allocation_id_to_group_name[allocation_id]
            running_by_task[job_info["task_name"]] += 1

        assert running_by_task["task_small"] == 2
        assert running_by_task["task_big"] == 1

        release_breakpoint()

        wait(lambda: get(scheduler_orchid_operation_path(op.id, tree="gpu") + "/resource_usage/gpu", default=None) == 0)

    @authors("yaishenka")
    def test_abort_after_controller_agent_failure(self):
        # Scenario: operation has running GPU allocations, controller agent fails
        # (DisableOperation(false) -> assignments preserved with Reviving=true),
        # then the operation is aborted before revival (UnregisterOperation called without
        # a prior EnableOperation). Without the fix, YT_VERIFY(Assignments().empty())
        # in UnregisterOperation crashes the scheduler.

        def get_agent_states():
            return [agent_info["state"]
                    for agent_info in get("//sys/scheduler/orchid/scheduler/controller_agents").values()]

        op = run_sleeping_vanilla(
            task_patch={"gpu_limit": 1, "enable_gpu_layers": False},
        )

        wait(lambda: len(op.get_running_jobs()) == 1)
        wait(lambda: get(scheduler_orchid_operation_path(op.id, tree="gpu") + "/resource_usage/gpu", default=None) == 1)

        wait_for_operations_in_gpu_policy_orchid(operation_count=1)
        wait_for_assignments_in_gpu_policy_orchid(op, assignment_count=1, exactly=True)

        # Ensure assignment is non-preliminary — it must have a live GPU allocation object.
        # Only non-preliminary assignments get Reviving=true after DisableOperation(false).
        wait(lambda: len(get_operation_from_gpu_policy_orchid(op)["allocations"]) == 1)

        with Restarter(self.Env, CONTROLLER_AGENTS_SERVICE):
            # Wait until the scheduler has processed the disconnect: DisableOperation(false) has
            # run, preserving non-preliminary assignments with Reviving=true.
            wait(lambda: all(state == "unregistered" for state in get_agent_states()))
            # Abort without waiting for revival. UnregisterOperation is triggered with
            # non-empty Reviving assignments still in DisabledOperations_.
            op.abort()

        wait(lambda: op.get_state() == "aborted")

        # Verify the scheduler survived — would time out if YT_VERIFY crashed the process.
        wait_operation_unregistered(op.id)


##################################################################

class TestAllocatingGpuSchedulingPolicyMultiModule(YTEnvSetup):
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
        dc_to_rack = dict(zip(TestAllocatingGpuSchedulingPolicyMultiModule.DATA_CENTERS, TestAllocatingGpuSchedulingPolicyMultiModule.RACKS))
        data_center_index_per_node = sum([[i] * count for i, count in enumerate(node_count_per_data_center)], [])

        def get_node_rack(i, node):
            if ibc_to_dc is None:
                return TestAllocatingGpuSchedulingPolicyMultiModule.RACKS[data_center_index_per_node[i]]
            ibc = get("//sys/cluster_nodes/{}/@annotations/infiniband_cluster_tag".format(node))
            return dc_to_rack[ibc_to_dc[ibc]]

        nodes = list(ls("//sys/cluster_nodes"))
        for i, node in enumerate(nodes):
            set("//sys/cluster_nodes/{}/@rack".format(node), get_node_rack(i, node))

        rack_to_dc = dict(zip(TestAllocatingGpuSchedulingPolicyMultiModule.RACKS, TestAllocatingGpuSchedulingPolicyMultiModule.DATA_CENTERS))
        for i, node in enumerate(nodes):
            wait(lambda: get(scheduler_orchid_node_path(node) + "/data_center") == rack_to_dc[get_node_rack(i, node)])

    def setup_method(self, method):
        super(TestAllocatingGpuSchedulingPolicyMultiModule, self).setup_method(method)

        update_pool_tree_config("default", {"node_tag_filter": "!gpu"})
        create_pool_tree("gpu", config={
            "node_tag_filter": "gpu",
            "main_resource": "gpu",
            "gpu_scheduling_policy": {
                "mode": "allocating",
                "plan_update_period": 100,
                "module_type": "data_center",
                "modules": TestAllocatingGpuSchedulingPolicyMultiModule.DATA_CENTERS,
                "full_host_aggressive_preemption_timeout": 1000,
            },
            "preemptive_scheduling_backoff": 0,
            "fair_share_starvation_timeout": 100,
            "fair_share_starvation_tolerance": 0.95,
            "preemption_satisfaction_threshold": 0.99,
            "non_preemptible_resource_usage_threshold": {"user_slots": 0},
            "policy_kind": "gpu",
        })

        set("//sys/pool_trees/@default_tree", "gpu")
        wait(lambda: get(scheduler_orchid_path() + "/scheduler/default_pool_tree", default=None) == "gpu")

        dc_to_rack = dict(zip(TestAllocatingGpuSchedulingPolicyMultiModule.DATA_CENTERS, TestAllocatingGpuSchedulingPolicyMultiModule.RACKS))
        for dc, r in dc_to_rack.items():
            create_data_center(dc)
            create_rack(r)
            set("//sys/racks/{}/@data_center".format(r), dc)

        for node in ls("//sys/cluster_nodes"):
            set("//sys/cluster_nodes/{}/@user_tags".format(node), ["gpu"])

        module_count = TestAllocatingGpuSchedulingPolicyMultiModule.NUM_NODES // 2
        self._setup_data_centers([module_count, module_count])

        wait(lambda: get(scheduler_new_orchid_pool_tree_path("gpu") + "/node_count") == TestAllocatingGpuSchedulingPolicyMultiModule.NUM_NODES)

    @authors("yaishenka")
    def test_simple_full_host_vanilla(self):
        op = run_sleeping_vanilla(
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
        )

        wait(lambda: len(op.get_running_jobs()) == 1)
        wait(lambda: get(scheduler_orchid_operation_path(op.id, tree="gpu") + "/resource_usage/gpu", default=None) == 8)

        wait(lambda: get(scheduler_orchid_operation_path(op.id, tree="gpu") + "/grouped_needed_resources", default=None) == {})

        wait_for_operations_in_gpu_policy_orchid(operation_count=1)
        wait_for_assignments_in_gpu_policy_orchid(op, assignment_count=1, exactly=True)

        operation = get_operation_from_gpu_policy_orchid(op)
        check_operation_from_gpu_policy_orchid(
            operation=operation,
            is_gang=False,
            group_name="task",
            allocation_count=1,
            min_needed_gpu_per_allocation=8,
            assigned_gpu_usage=8,
            assignment_count=1,
            enabled=True,
        )

        job_id = list(op.get_running_jobs())[0]
        allocation_id = get_allocation_id_from_job_id(job_id)
        assignment = operation["assignments"][0]
        check_assignment_from_gpu_policy_orchid(
            assignment,
            op.id,
            "task",
            8,
            preemptible=False,
            allocation_id=allocation_id,
        )
        check_gpu_allocations_from_gpu_policy_orchid(operation["allocations"], [allocation_id], 8)

        node_address = assignment["node_address"]
        node = get(scheduler_new_orchid_pool_tree_path("gpu") + f"/gpu_assignment_plan/nodes/{node_address}")
        assert len(node["assignments"]) == 1
        compare_assignment_without_preemptible_start_time(node["assignments"][0], assignment)

    @authors("yaishenka")
    def test_specified_modules(self):
        op = run_sleeping_vanilla(
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
            spec={"scheduling_modules": ["VLA"]},
        )

        wait(lambda: len(op.get_running_jobs()) == 1)
        wait(lambda: get(scheduler_orchid_operation_path(op.id, tree="gpu") + "/resource_usage/gpu", default=None) == 8)

        wait(lambda: get(scheduler_orchid_operation_path(op.id, tree="gpu") + "/grouped_needed_resources", default=None) == {})

        wait_for_operations_in_gpu_policy_orchid(operation_count=1)
        wait_for_assignments_in_gpu_policy_orchid(op, assignment_count=1, exactly=True)

        operation = get_operation_from_gpu_policy_orchid(op)
        check_operation_from_gpu_policy_orchid(
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

        job_id = list(op.get_running_jobs())[0]
        allocation_id = get_allocation_id_from_job_id(job_id)
        assignment = operation["assignments"][0]
        check_assignment_from_gpu_policy_orchid(
            assignment,
            op.id,
            "task",
            8,
            preemptible=False,
            allocation_id=allocation_id,
        )
        check_gpu_allocations_from_gpu_policy_orchid(operation["allocations"], [allocation_id], 8)

        node_address = assignment["node_address"]
        node = get(scheduler_new_orchid_pool_tree_path("gpu") + f"/gpu_assignment_plan/nodes/{node_address}")
        assert len(node["assignments"]) == 1
        compare_assignment_without_preemptible_start_time(node["assignments"][0], assignment)
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
        wait(lambda: get(scheduler_orchid_operation_path(op.id, tree="gpu") + "/resource_usage/gpu", default=None) == 16)

        wait(lambda: get(scheduler_orchid_operation_path(op.id, tree="gpu") + "/grouped_needed_resources", default=None) == {})

        wait_for_operations_in_gpu_policy_orchid(operation_count=1)
        wait_for_assignments_in_gpu_policy_orchid(op, assignment_count=2, exactly=True)

        operation = get_operation_from_gpu_policy_orchid(op)
        check_operation_from_gpu_policy_orchid(
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

        wait(lambda: len(op1.get_running_jobs()) == 2)

        wait(lambda: get(scheduler_orchid_operation_path(op1.id, tree="gpu") + "/grouped_needed_resources", default=None) == {})

        op2 = run_sleeping_vanilla(
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
            spec={
                "is_gang": True,
                "scheduling_modules": ["VLA"],
            },
            job_count=2,
        )

        wait_for_operations_in_gpu_policy_orchid(operation_count=2)

        operation1 = get_operation_from_gpu_policy_orchid(op1)
        operation2 = get_operation_from_gpu_policy_orchid(op2)

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
            wait(lambda: get(scheduler_orchid_operation_path(op.id, tree="gpu") + "/grouped_needed_resources", default=None) == {})

        wait_for_operations_in_gpu_policy_orchid(operation_count=len(operations))

        modules = builtins.set()
        for op in operations:
            operation = get_operation_from_gpu_policy_orchid(op)
            for assignment in operation["assignments"]:
                node_address = assignment["node_address"]
                node = get(scheduler_new_orchid_pool_tree_path("gpu") + f"/gpu_assignment_plan/nodes/{node_address}")
                modules.add(node["scheduling_module"])

        assert len(modules) == 1

    @authors("yaishenka")
    def test_ban_node(self):
        update_scheduler_config("node_registration_timeout", 1000)
        update_scheduler_config("node_heartbeat_timeout", 1000)
        update_scheduler_config("node_reconnection_timeout", 1000)

        op = run_sleeping_vanilla(
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
        )

        wait(lambda: len(op.get_running_jobs()) == 1)

        wait(lambda: get(scheduler_orchid_operation_path(op.id, tree="gpu") + "/grouped_needed_resources", default=None) == {})

        wait_for_operations_in_gpu_policy_orchid(operation_count=1)
        wait_for_assignments_in_gpu_policy_orchid(op, assignment_count=1, exactly=True)

        def check_for_new_assignment(op, assignment_node_address):
            operation = get_operation_from_gpu_policy_orchid(op)
            assignments = operation["assignments"]

            if len(assignments) == 0:
                return False

            assignment = assignments[0]
            return assignment_node_address != assignment["node_address"]

        operation = get_operation_from_gpu_policy_orchid(op)
        assignment = operation["assignments"][0]
        node_address = assignment["node_address"]
        assignment_node_address = assignment["node_address"]
        module = operation["scheduling_module"]

        set_node_banned(node_address, True)

        wait(lambda: len(op.get_running_jobs()) == 1)
        wait(lambda: get(scheduler_orchid_operation_path(op.id, tree="gpu") + "/grouped_needed_resources", default=None) == {})

        wait(lambda: check_for_new_assignment(op, assignment_node_address))
        operation = get_operation_from_gpu_policy_orchid(op)
        assert operation["scheduling_module"] == module

    @authors("yaishenka")
    def test_modules_reset(self):
        update_scheduler_config("node_registration_timeout", 1000)
        update_scheduler_config("node_heartbeat_timeout", 1000)
        update_scheduler_config("node_reconnection_timeout", 1000)
        update_pool_tree_config_option("gpu", "enable_step_function_for_gang_operations", False)

        op = run_sleeping_vanilla(
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
            job_count=2,
            spec={"is_gang": True},
        )
        wait(lambda: len(op.get_running_jobs()) == 2)
        wait(lambda: get(scheduler_orchid_operation_path(op.id, tree="gpu") + "/grouped_needed_resources", default=None) == {})
        wait_for_assignments_in_gpu_policy_orchid(op, 2)

        occupied_node = get_operation_gpu_assignments_from_gpu_policy_orchid(op)[0]["node_address"]
        set_node_banned(occupied_node, True)
        wait_for_assignments_in_gpu_policy_orchid(op, 1, exactly=True)
        wait(lambda: len(op.get_running_jobs()) == 1)

        update_pool_tree_config_option("gpu", "gpu_scheduling_policy/module_reconsideration_timeout", 5000)
        wait(lambda: get(scheduler_orchid_operation_path(op.id, tree="gpu") + "/grouped_needed_resources", default=None) == {})
        wait_for_assignments_in_gpu_policy_orchid(op, 2)


##################################################################

class TestSchedulingLimits(AllocatingGpuSchedulingPolicyBaseConfig):
    @authors("severovv")
    def test_pool_limits_change(self):
        create_pool("limited", pool_tree="gpu", attributes={"resource_limits": {"gpu": 10}}, wait_for_orchid=True)

        op = run_sleeping_vanilla(
            task_patch={"gpu_limit": 4, "enable_gpu_layers": False},
            job_count=4,
            pool="limited",
        )
        wait(lambda: len(op.get_running_jobs()) == 2)

        set("//sys/pool_trees/gpu/limited/@resource_limits", {"gpu": 5})
        wait(lambda: len(op.get_running_jobs()) == 1)

        set("//sys/pool_trees/gpu/limited/@resource_limits", {"gpu": 13})
        wait(lambda: len(op.get_running_jobs()) == 3)

    @authors("severovv")
    def test_operation_limits_change(self):

        def set_operation_limit(op, limit):
            update_op_parameters(
                op.id,
                parameters={"scheduling_options_per_pool_tree": {"gpu": {"resource_limits": {"gpu": limit}}}},
            )

        op = run_sleeping_vanilla(
            task_patch={"gpu_limit": 4, "enable_gpu_layers": False},
            job_count=4,
        )
        set_operation_limit(op, 10)
        wait(lambda: len(op.get_running_jobs()) == 2)

        set_operation_limit(op, 5)
        wait(lambda: len(op.get_running_jobs()) == 1)

        set_operation_limit(op, 13)
        wait(lambda: len(op.get_running_jobs()) == 3)

    @authors("severovv")
    def test_over_fs_planning_limits(self):
        create_pool("limited", pool_tree="gpu", attributes={"resource_limits": {"gpu": 12}}, wait_for_orchid=True)

        _ = run_sleeping_vanilla(
            task_patch={"gpu_limit": 4, "enable_gpu_layers": False},
            job_count=4,
            pool="limited",
            spec={"scheduling_tag_filter": "unschedulable"}
        )

        op = run_sleeping_vanilla(
            task_patch={"gpu_limit": 4, "enable_gpu_layers": False},
            job_count=4,
            pool="limited",
        )

        # one job over fair share but still within the limits
        wait(lambda: len(op.get_running_jobs()) == 3)

    @authors("severovv")
    def test_preemptive_planning_limits(self):
        create_pool("limited", pool_tree="gpu", attributes={"strong_guarantee_resources": {"gpu": 8}, "resource_limits": {"gpu": 8}})
        create_pool("guaranteed", pool_tree="gpu", parent_name="limited", attributes={"strong_guarantee_resources": {"gpu": 8}})
        create_pool("unguaranteed", pool_tree="gpu", parent_name="limited", attributes={"strong_guarantee_resources": {"gpu": 0}}, wait_for_orchid=True)

        bg_runner = run_sleeping_vanilla(
            task_patch={"gpu_limit": 4, "enable_gpu_layers": False},
            job_count=1,
            pool="guaranteed",
        )
        preemptive_runner = run_sleeping_vanilla(
            task_patch={"gpu_limit": 4, "enable_gpu_layers": False},
            job_count=1,
            pool="unguaranteed",
        )

        wait(lambda: len(bg_runner.get_running_jobs()) == 1)
        wait(lambda: len(preemptive_runner.get_running_jobs()) == 1)
        bg_runner_allocation_id = get_allocation_id_from_job_id(list(bg_runner.get_running_jobs())[0])

        will_replace = run_sleeping_vanilla(
            task_patch={"gpu_limit": 4, "enable_gpu_layers": False},
            job_count=1,
            pool="guaranteed",
        )

        wait(lambda: len(bg_runner.get_running_jobs()) == 1)
        wait(lambda: len(will_replace.get_running_jobs()) == 1)
        wait(lambda: len(preemptive_runner.get_running_jobs()) == 0)

        # check that bg runner allocation was never preempted during the test
        assert bg_runner_allocation_id == get_allocation_id_from_job_id(list(bg_runner.get_running_jobs())[0])
        wait(lambda: get(preemptive_runner.get_path() + "/@brief_progress/jobs")["aborted"] >= 1)

    @authors("severovv")
    def test_controller_returns_more_than_pool_limit(self):
        create_pool("strict_pool", pool_tree="gpu", attributes={"resource_limits": {"cpu": 3.0}}, wait_for_orchid=True)

        # Two full-host jobs are assigned to different nodes
        # Both of them are scheduled, first gets 3.0 cpu limit, second 2.0
        # After returning from CA one of them will be aborted because of limit violation
        op = run_sleeping_vanilla(
            task_patch={"cpu_limit": 1.0, "gpu_limit": 8, "enable_gpu_layers": False},
            pool="strict_pool",
            job_count=2,
            spec={
                "testing": {
                    "schedule_allocation_cpu_multiplier": 2.0,
                    "inside_schedule_job_delay": {
                        "duration": 1000,
                    },
                },
            },
        )

        wait_for_assignments_in_gpu_policy_orchid(op, assignment_count=2, exactly=True)
        wait(lambda: len(op.get_running_jobs()) == 1)
        wait(lambda: get(op.get_path() + "/controller_orchid/progress/jobs/aborted/non_scheduled/scheduling_resource_overcommit", 0) == 1)


##################################################################

class TestAllocationGpuSchedulingPolicyRevival(YTEnvSetup):
    ENABLE_MULTIDAEMON = False  # There are component restarts.
    NUM_MASTERS = 1
    NUM_NODES = 3
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

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "snapshot_period": 500,
        }
    }

    DATA_CENTER = "SAS"
    RACK = "SAS1"

    def setup_method(self, method):
        super(TestAllocationGpuSchedulingPolicyRevival, self).setup_method(method)

        update_pool_tree_config("default", {"node_tag_filter": "!gpu"})
        create_pool_tree("gpu", config={
            "node_tag_filter": "gpu",
            "main_resource": "gpu",
            "gpu_scheduling_policy": {
                "mode": "allocating",
                "plan_update_period": 100,
                "module_type": "data_center",
                "modules": [self.DATA_CENTER],
                "full_host_aggressive_preemption_timeout": 1000,
                "initialization_timeout": 1000
            },
            "preemptive_scheduling_backoff": 0,
            "fair_share_starvation_timeout": 100,
            "fair_share_starvation_tolerance": 0.95,
            "preemption_satisfaction_threshold": 0.99,
            "non_preemptible_resource_usage_threshold": {"user_slots": 0},
            "policy_kind": "gpu",
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

    @authors("yaishenka")
    def test_revival_rescues_reviving_assignment(self):
        # Scenario: operation with a running GPU allocation, controller agent restarts.
        # DisableOperation(false) preserves the non-preliminary assignment with
        # Reviving=true and clears AllocationIdToAllocationState_. When CA reconnects,
        # the operation is revived from snapshot, RegisterAllocationsFromRevivedOperation runs
        # Case A: the surviving assignment is rescued (Reviving cleared, fresh
        # TAllocationState reattached, original AllocationGroupName preserved).

        def get_agent_states():
            return [agent_info["state"]
                    for agent_info in get("//sys/scheduler/orchid/scheduler/controller_agents").values()]

        op = run_sleeping_vanilla(
            task_patch={"gpu_limit": 1, "enable_gpu_layers": False},
        )

        wait_for_assignments_in_gpu_policy_orchid(op, assignment_count=1, exactly=True)
        wait(lambda: len(get_operation_from_gpu_policy_orchid(op)["allocations"]) == 1)

        operation = get_operation_from_gpu_policy_orchid(op)
        original_assignment = operation["assignments"][0]
        original_allocation_id = list(operation["allocations"].keys())[0]
        original_node_address = original_assignment["node_address"]
        original_group_name = original_assignment["allocation_group_name"]

        op.wait_for_fresh_snapshot()

        with Restarter(self.Env, CONTROLLER_AGENTS_SERVICE):
            wait(lambda: all(state == "unregistered" for state in get_agent_states()))
            # Inside: assignment still present with Reviving=true, allocations cleared.
            wait(lambda: not get_operation_from_gpu_policy_orchid(op)["enabled"])
            wait(lambda: len(get_operation_from_gpu_policy_orchid(op)["allocations"]) == 0)
            wait(lambda: len(get_operation_from_gpu_policy_orchid(op)["assignments"]) == 1)
            assert get_operation_gpu_assignments_from_gpu_policy_orchid(op)[0]["reviving"]

        # CA reconnects, scheduler revives the operation from snapshot.
        # RegisterAllocationsFromRevivedOperation runs Case A: the surviving assignment is rescued.
        wait(lambda: get_operation_from_gpu_policy_orchid(op)["enabled"])
        wait(lambda: len(get_operation_from_gpu_policy_orchid(op)["allocations"]) == 1)

        operation = get_operation_from_gpu_policy_orchid(op)
        assert len(operation["assignments"]) == 1
        rescued = operation["assignments"][0]
        # Case A invariant: original group name preserved (not "revived").
        assert rescued["allocation_group_name"] == original_group_name
        assert rescued["node_address"] == original_node_address
        assert rescued["allocation_id"] == original_allocation_id

        # The TAllocationState has been reattached to the same allocation id.
        assert original_allocation_id in operation["allocations"]
        assert operation["allocations"][original_allocation_id]["resource_usage"]["gpu"] == 1

        op.abort()
        wait_operation_unregistered(op.id)

    @authors("yaishenka")
    def test_revival_after_scheduler_restart(self):
        # Scenario: operation with a running GPU allocation, scheduler restarts. Operation is
        # revived from snapshot. The GPU policy state is fresh (no prior assignments), so
        # RegisterAllocationsFromRevivedOperation runs Case B: synthesizes a fresh
        # non-preliminary assignment via the standard preliminary->realized flow, with
        # allocation_group_name set to "revived" (placeholder, since the controller->scheduler
        # revival protocol doesn't currently propagate the original task name).

        op = run_sleeping_vanilla(
            task_patch={"gpu_limit": 1, "enable_gpu_layers": False},
        )

        wait_for_assignments_in_gpu_policy_orchid(op, assignment_count=1, exactly=True)
        wait(lambda: len(get_operation_from_gpu_policy_orchid(op)["allocations"]) == 1)

        operation = get_operation_from_gpu_policy_orchid(op)
        original_assignment = operation["assignments"][0]
        original_allocation_id = list(operation["allocations"].keys())[0]
        original_node_address = original_assignment["node_address"]
        assert not original_assignment["reviving"]

        op.wait_for_fresh_snapshot()

        with Restarter(self.Env, SCHEDULERS_SERVICE):
            pass

        # Scheduler comes back, revives operation from snapshot.
        # RegisterAllocationsFromRevivedOperation runs Case B (fresh GPU policy state).
        wait(lambda: get_operation_from_gpu_policy_orchid(op)["enabled"])
        wait(lambda: len(get_operation_from_gpu_policy_orchid(op)["allocations"]) == 1)
        wait(lambda: len(get_operation_gpu_assignments_from_gpu_policy_orchid(op)) == 1)

        operation = get_operation_from_gpu_policy_orchid(op)
        revived = get_operation_gpu_assignments_from_gpu_policy_orchid(op)[0]
        # Case B invariant: synthesized assignment uses placeholder group name and is not flagged.
        assert revived["allocation_group_name"] == "revived"
        assert revived["node_address"] == original_node_address
        assert revived["allocation_id"] == original_allocation_id
        assert not revived["reviving"]

        # The TAllocationState has been recreated and bound to the same allocation id.
        assert original_allocation_id in operation["allocations"]
        assert operation["allocations"][original_allocation_id]["resource_usage"]["gpu"] == 1

        op.abort()
        wait_operation_unregistered(op.id)

    @authors("yaishenka")
    def test_remove_orphan_adoption_on_allocation_finished(self):
        # Scenario: scheduler restarts with revival deferred via
        # `delay_inside_register_allocations_from_revived_operation`. While the delay is
        # pending we ban the target node. With `node_registration_timeout` /
        # `node_heartbeat_timeout` lowered, the registration lease expires and the scheduler
        # unregisters the node from the GPU policy's Nodes_. When revival fires, Case C
        # kicks in: AddOrphanAllocation parks the TAllocationState (empty weak_ptr) in
        # operation->AllocationIdToAllocationState_ and PendingRevivedAllocations_ records it.
        #
        # When the node is unbanned, the previously-running allocation is reported finished
        # (it was aborted when the node went down). DoProcessFinishedAllocation runs and
        # (a) RemoveAllocation erases the orphan TAllocationState,
        # (b) IncreaseHierarchicalResourceUsage(-usage) balances the bump from ReviveAllocation,
        # (c) the pending-map scrub block erases the PendingRevivedAllocations_ entry.
        # The operation is back to a clean state, the controller schedules a brand-new
        # allocation through the regular scheduling flow (not Case A/B), and the test
        # verifies the new assignment uses a non-"revived" group name and a different
        # allocation_id - confirming no orphan leaked into the new cycle.

        update_scheduler_config("node_registration_timeout", 1000)
        update_scheduler_config("node_heartbeat_timeout", 1000)
        update_scheduler_config("node_reconnection_timeout", 1000)

        nodes = list(ls("//sys/cluster_nodes"))

        op = run_sleeping_vanilla(
            task_patch={"gpu_limit": 1, "enable_gpu_layers": False},
            spec={
                "testing": {
                    "delay_inside_register_allocations_from_revived_operation": 5000
                },
                "scheduling_tag_filter": nodes[0],
            },
        )

        wait_for_assignments_in_gpu_policy_orchid(op, assignment_count=1, exactly=True)
        wait(lambda: len(get_operation_from_gpu_policy_orchid(op)["allocations"]) == 1)

        operation = get_operation_from_gpu_policy_orchid(op)
        target_node = operation["assignments"][0]["node_address"]
        original_allocation_id = list(operation["allocations"].keys())[0]

        op.wait_for_fresh_snapshot()

        update_pool_tree_config_option("gpu", "testing_options", {
            "delay_inside_process_allocation_updates": {
                "duration": 10000,
                "type": "async",
            },
        })

        with Restarter(self.Env, SCHEDULERS_SERVICE):
            pass

        set_node_banned(target_node, True, wait_for_master=True, wait_for_scheduler=True)

        wait(lambda: original_allocation_id in get_operation_from_gpu_policy_orchid(op).get("allocations", {}))
        wait_for_assignments_in_gpu_policy_orchid(op, assignment_count=0, exactly=True)

        update_pool_tree_config_option("gpu", "testing_options", {
            "delay_inside_process_allocation_updates": {
                "duration": 0,
                "type": "sync",
            },
        })

        update_scheduler_config("node_registration_timeout", 30000)
        update_scheduler_config("node_heartbeat_timeout", 30000)
        update_scheduler_config("node_reconnection_timeout", 30000)

        wait(lambda: original_allocation_id not in get_operation_from_gpu_policy_orchid(op).get("allocations", {}))

        set_node_banned(target_node, False, wait_for_master=True, wait_for_scheduler=True)

        wait(lambda: len(op.get_running_jobs()) == 1)

        wait(lambda: get(scheduler_orchid_operation_path(op.id, tree="gpu") + "/grouped_needed_resources", default=None) == {})

        # Node re-register. Allocation finished (because it was aborted when node goes offline). Planning new one.
        wait_for_assignments_in_gpu_policy_orchid(op, assignment_count=1, exactly=True)

        operation = get_operation_from_gpu_policy_orchid(op)
        new_assignment = get_operation_gpu_assignments_from_gpu_policy_orchid(op)[0]
        assert new_assignment["allocation_group_name"] != "revived"
        assert new_assignment["allocation_id"] != original_allocation_id
        assert not new_assignment["reviving"]

        assert original_allocation_id not in operation["allocations"]

        op.abort()
        wait_operation_unregistered(op.id)

    @authors("yaishenka")
    def test_partial_revival_strips_unrescued_assignments(self):
        # Scenario: an operation has 2 running GPU allocations on different nodes. One of
        # them is preempted via a node ban so only the surviving allocation lands in the
        # snapshot. The ban is then lifted and the preempted job re-allocates with a new
        # id - but no new snapshot is taken before the CA restart. On CA reconnect,
        # revival rescues only the snapshotted allocation (Case A); the other assignment,
        # marked Reviving=true by the CA disconnect, is not in the revival list
        # and is stripped by EnableOperation (Part 1 Step 5).

        update_scheduler_config("node_registration_timeout", 1000)
        update_scheduler_config("node_heartbeat_timeout", 1000)
        update_scheduler_config("node_reconnection_timeout", 1000)

        def get_agent_states():
            return [agent_info["state"]
                    for agent_info in get("//sys/scheduler/orchid/scheduler/controller_agents").values()]

        nodes = list(ls("//sys/cluster_nodes"))

        # gpu_limit=6 forces each job to use >half of a node's 8 GPUs, so the two jobs
        # have to land on different nodes. (gpu_limit=8 would trip the full-host
        # module-bound path; gpu_limit<=4 would let both jobs share one node.)
        op = run_sleeping_vanilla(
            job_count=2,
            task_patch={"gpu_limit": 6, "enable_gpu_layers": False},
            spec={"scheduling_tag_filter": f"{nodes[0]}|{nodes[1]}"},
        )

        wait_for_assignments_in_gpu_policy_orchid(op, assignment_count=2, exactly=True)
        wait(lambda: len(get_operation_from_gpu_policy_orchid(op)["allocations"]) == 2)

        operation = get_operation_from_gpu_policy_orchid(op)
        nodes_used = sorted({a["node_address"] for a in operation["assignments"]})
        assert len(nodes_used) == 2
        survivor_node, doomed_node = nodes_used

        # Ban the doomed node; its allocation gets preempted, leaving 1 running allocation.
        set_node_banned(doomed_node, True, wait_for_master=True, wait_for_scheduler=True)
        wait_for_assignments_in_gpu_policy_orchid(op, assignment_count=1, exactly=True)
        wait(lambda: len(get_operation_from_gpu_policy_orchid(op)["allocations"]) == 1)

        operation = get_operation_from_gpu_policy_orchid(op)
        surviving_assignment = operation["assignments"][0]
        snapshotted_allocation_id = surviving_assignment["allocation_id"]
        original_group_name = surviving_assignment["allocation_group_name"]
        assert surviving_assignment["node_address"] == survivor_node

        op.wait_for_fresh_snapshot()

        # Bump snapshot_period high so no further snapshot lands before the CA restart.
        update_controller_agent_config("snapshot_period", 600000)

        # Unban; the preempted job re-allocates with a new id (not in the snapshot).
        set_node_banned(doomed_node, False, wait_for_master=True, wait_for_scheduler=True)
        update_scheduler_config("node_registration_timeout", 10000)
        update_scheduler_config("node_heartbeat_timeout", 10000)
        update_scheduler_config("node_reconnection_timeout", 10000)

        wait_for_assignments_in_gpu_policy_orchid(op, assignment_count=2, exactly=True)
        wait(lambda: len(get_operation_from_gpu_policy_orchid(op)["allocations"]) == 2)

        operation = get_operation_from_gpu_policy_orchid(op)
        all_allocation_ids = builtins.set(operation["allocations"].keys())
        assert snapshotted_allocation_id in all_allocation_ids
        late_allocation_id = (all_allocation_ids - {snapshotted_allocation_id}).pop()

        with Restarter(self.Env, CONTROLLER_AGENTS_SERVICE):
            wait(lambda: all(state == "unregistered" for state in get_agent_states()))
            # Both assignments survive with Reviving=true; allocations cleared.
            wait(lambda: len(get_operation_from_gpu_policy_orchid(op)["allocations"]) == 0)
            assignments = get_operation_gpu_assignments_from_gpu_policy_orchid(op)
            assert len(assignments) == 2
            assert all(a["reviving"] for a in assignments)

        # CA reconnects. Revival rescues snapshotted_allocation_id only (Case A).
        # EnableOperation strips the late_allocation_id assignment.
        wait(lambda: get_operation_from_gpu_policy_orchid(op)["enabled"])
        wait(lambda: snapshotted_allocation_id in get_operation_from_gpu_policy_orchid(op).get("allocations", {}))
        wait(lambda: late_allocation_id not in get_operation_from_gpu_policy_orchid(op).get("allocations", {}))

        operation = get_operation_from_gpu_policy_orchid(op)
        rescued = next(
            a for a in operation["assignments"]
            if a["allocation_id"] == snapshotted_allocation_id
        )
        assert rescued["allocation_group_name"] == original_group_name
        assert not rescued["reviving"]
        # Don't assert total assignment count; CA may schedule a fresh replacement for the
        # stripped allocation through the regular flow, which would re-add an assignment
        # with the original task group name (not "revived") and a brand-new allocation id
        # (different from both snapshotted_allocation_id and late_allocation_id).

        op.abort()
        wait_operation_unregistered(op.id)

    @authors("yaishenka")
    def test_no_revival_strips_reviving_assignments(self):
        # Scenario: an operation has a running GPU allocation. We disable snapshot loading
        # on the controller agent, then restart it. On reconnect the CA falls into the
        # Materialize branch (scheduler.cpp:1307 - !RevivedFromSnapshot, since snapshot
        # loading is disabled), so RegisterAllocationsFromRevivedOperation is NOT called.
        # The Reviving=true assignment left on the GPU policy must be stripped
        # by EnableOperation (Part 1 Step 5) so the operation can start fresh without
        # retaining a stale assignment.

        def get_agent_states():
            return [agent_info["state"]
                    for agent_info in get("//sys/scheduler/orchid/scheduler/controller_agents").values()]

        op = run_sleeping_vanilla(
            task_patch={"gpu_limit": 1, "enable_gpu_layers": False},
        )

        wait_for_assignments_in_gpu_policy_orchid(op, assignment_count=1, exactly=True)
        wait(lambda: len(get_operation_from_gpu_policy_orchid(op)["allocations"]) == 1)

        operation = get_operation_from_gpu_policy_orchid(op)
        pre_restart_allocation_id = list(operation["allocations"].keys())[0]

        # Disable snapshot loading; a snapshot may exist but the CA won't load it on restart.
        update_controller_agent_config("enable_snapshot_loading", False)

        with Restarter(self.Env, CONTROLLER_AGENTS_SERVICE):
            wait(lambda: all(state == "unregistered" for state in get_agent_states()))

        # CA reconnects without loading any snapshot. RegisterAllocationsFromRevivedOperation
        # is not called. EnableOperation strips the Reviving assignment.
        wait(lambda: pre_restart_allocation_id not in get_operation_from_gpu_policy_orchid(op).get("allocations", {}))

        # Eventually the CA schedules a fresh allocation through the regular flow.
        wait_for_assignments_in_gpu_policy_orchid(op, assignment_count=1, exactly=True)
        wait(lambda: len(get_operation_from_gpu_policy_orchid(op)["allocations"]) == 1)

        operation = get_operation_from_gpu_policy_orchid(op)
        new_assignment = operation["assignments"][0]
        assert new_assignment["allocation_group_name"] != "revived"
        assert new_assignment["allocation_id"] != pre_restart_allocation_id
        assert not new_assignment["reviving"]

        op.abort()
        wait_operation_unregistered(op.id)

    @authors("yaishenka")
    def test_revival_after_policy_switch_to_classic(self):
        # Reversed scenario from test_revival_after_policy_switch_to_allocating: start
        # under the GPU allocating policy, switch the tree to policy_kind="classic" and
        # restart the scheduler. The classic (fair-share) policy takes over; the operation
        # continues running with its original allocation. This validates that switching
        # OUT of the GPU policy doesn't leave stale state behind that would prevent the
        # classic policy from picking up running operations on revival.

        op = run_sleeping_vanilla(
            task_patch={"gpu_limit": 1, "enable_gpu_layers": False},
        )

        wait_for_assignments_in_gpu_policy_orchid(op, assignment_count=1, exactly=True)
        wait(lambda: len(get_operation_from_gpu_policy_orchid(op)["allocations"]) == 1)

        job_id = list(op.get_running_jobs())[0]
        pre_switch_allocation_id = get_allocation_id_from_job_id(job_id)
        pre_switch_node_address = get(op.get_path() + f"/controller_orchid/running_jobs/{job_id}/address")

        op.wait_for_fresh_snapshot()

        # Switch the pool tree's policy to classic. The gpu_scheduling_policy block stays
        # in config but is ignored by the classic policy. Takes effect on scheduler restart.
        update_pool_tree_config_option("gpu", "policy_kind", "classic")

        with Restarter(self.Env, SCHEDULERS_SERVICE):
            pass

        # Scheduler is back with the classic policy. The GPU policy orchid no longer
        # exists for this tree, and the operation continues running with its original
        # allocation.
        assert not exists(scheduler_new_orchid_pool_tree_path("gpu") + "/gpu_assignment_plan")

        wait(lambda: len(op.get_running_jobs()) == 1)
        new_job_id = list(op.get_running_jobs())[0]
        assert get_allocation_id_from_job_id(new_job_id) == pre_switch_allocation_id
        assert get(op.get_path() + f"/controller_orchid/running_jobs/{new_job_id}/address") == pre_switch_node_address

        op.abort()
        wait(lambda: not exists(scheduler_orchid_operation_path(op.id, tree="gpu")))

    @authors("yaishenka")
    def test_adopt_orphaned_allocation_after_node_registration(self):
        # Scenario: scheduler restarts while node registration is artificially delayed
        # (via handle_nodes_attributes_delay + node_heartbeat_processing_delay). Revival
        # fires before the node re-registers in the GPU policy, so ReviveAllocation falls
        # into Case C: AddOrphanAllocation parks the TAllocationState (null Assignment_)
        # in operation->AllocationIdToAllocationState_ and PendingRevivedAllocations_ records
        # the (nodeId, allocationId) pair. Once the delays are cleared the node re-registers,
        # RevivePendingAllocations creates a "revived" assignment and links the orphan
        # TAllocationState to it via SetAssignment. The test verifies that the allocation
        # appears in the orchid with creation_time earlier than its assignment's
        # creation_time — confirming it went through the orphan path (the allocation
        # state was created before the revived assignment) — and that the job is
        # running normally afterwards.
        update_scheduler_config("node_registration_timeout", 100000)
        update_scheduler_config("node_heartbeat_timeout", 100000)
        update_scheduler_config("node_reconnection_timeout", 100000)

        nodes = list(ls("//sys/cluster_nodes"))

        op = run_sleeping_vanilla(
            task_patch={"gpu_limit": 1, "enable_gpu_layers": False},
            spec={
                "scheduling_tag_filter": nodes[0]
            },
        )

        wait_for_assignments_in_gpu_policy_orchid(op, assignment_count=1, exactly=True)
        wait(lambda: len(get_operation_from_gpu_policy_orchid(op)["allocations"]) == 1)

        operation = get_operation_from_gpu_policy_orchid(op)
        original_allocation_id = list(operation["allocations"].keys())[0]

        op.wait_for_fresh_snapshot()

        update_scheduler_config("nodes_attributes_update_period", 3000)
        update_scheduler_config("testing_options/handle_nodes_attributes_delay", {
            "duration": 3000,
            "type": "async",
        })
        update_scheduler_config("testing_options/node_heartbeat_processing_delay", {
            "duration": 3000,
            "type": "async",
        })

        with Restarter(self.Env, SCHEDULERS_SERVICE):
            pass

        update_scheduler_config("nodes_attributes_update_period", 100)
        update_scheduler_config("testing_options/handle_nodes_attributes_delay", {
            "duration": 0,
            "type": "sync",
        })
        update_scheduler_config("testing_options/node_heartbeat_processing_delay", {
            "duration": 0,
            "type": "sync",
        })

        wait(lambda: len(op.get_running_jobs()) == 1)
        wait_for_assignments_in_gpu_policy_orchid(op, assignment_count=1, exactly=True)
        wait(lambda: original_allocation_id in get_operation_from_gpu_policy_orchid(op).get("allocations", {}))
        operation = get_operation_from_gpu_policy_orchid(op)
        allocation = operation["allocations"][original_allocation_id]
        assignment = operation["assignments"][0]
        # Orphan allocation is parked first; the revived assignment that adopts it
        # is created later — so the allocation predates its assignment.
        assert allocation["creation_time"] < assignment["creation_time"]

##################################################################


class TestAllocationGpuSchedulingPolicyRevivalOnPolicySwitch(YTEnvSetup):
    ENABLE_MULTIDAEMON = False
    NUM_MASTERS = 1
    NUM_NODES = 2
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = AllocatingGpuSchedulingPolicyBaseConfig.DELTA_SCHEDULER_CONFIG
    DELTA_DYNAMIC_NODE_CONFIG = AllocatingGpuSchedulingPolicyBaseConfig.DELTA_DYNAMIC_NODE_CONFIG
    DELTA_NODE_CONFIG = AllocatingGpuSchedulingPolicyBaseConfig.DELTA_NODE_CONFIG
    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "snapshot_period": 500,
            "snapshot_writer": {
                "upload_replication_factor": 1,
                "min_upload_replication_factor": 1,
            },
        }
    }

    DATA_CENTER = AllocatingGpuSchedulingPolicyBaseConfig.DATA_CENTER
    RACK = AllocatingGpuSchedulingPolicyBaseConfig.RACK

    def setup_method(self, method):
        super().setup_method(method)

        update_pool_tree_config("default", {"node_tag_filter": "!gpu"})
        # Boot the tree with the classic policy. NB: gpu_scheduling_policy is intentionally
        # absent here; the test will add it before restarting the scheduler.
        create_pool_tree("gpu", config={
            "node_tag_filter": "gpu",
            "main_resource": "gpu",
            "preemptive_scheduling_backoff": 0,
            "fair_share_starvation_timeout": 100,
            "fair_share_starvation_tolerance": 0.95,
            "preemption_satisfaction_threshold": 0.99,
            "non_preemptible_resource_usage_threshold": {"user_slots": 0},
            "policy_kind": "classic",
        })

        set("//sys/pool_trees/@default_tree", "gpu")
        wait(lambda: get(scheduler_orchid_path() + "/scheduler/default_pool_tree", default=None) == "gpu")

        create_data_center(self.DATA_CENTER)
        create_rack(self.RACK)
        set(f"//sys/racks/{self.RACK}/@data_center", self.DATA_CENTER)
        for node in ls("//sys/cluster_nodes"):
            set(f"//sys/cluster_nodes/{node}/@rack", self.RACK)
            set(f"//sys/cluster_nodes/{node}/@user_tags", ["gpu"])
        for node in ls("//sys/cluster_nodes"):
            wait(lambda: get(scheduler_orchid_node_path(node) + "/data_center") == self.DATA_CENTER)

    @authors("yaishenka")
    def test_revival_after_policy_switch_to_allocating(self):
        # Scenario: a pool tree is initially configured with policy_kind="classic" (the
        # fair-share scheduling policy). An operation runs with a GPU allocation. We then
        # switch the tree's policy_kind to "gpu" and add the gpu_scheduling_policy block;
        # this only takes effect on scheduler restart because SchedulingPolicy_ is built
        # once in TPoolTree's constructor (pool_tree.cpp:305). After the scheduler restart,
        # the GPU policy revives the operation from snapshot via Case B in
        # RegisterAllocationsFromRevivedOperation: no prior GPU-policy-side assignment
        # exists, so a fresh non-preliminary assignment is synthesized via the standard
        # preliminary->realized flow with allocation_group_name == "revived" and the
        # original allocation_id reattached.

        op = run_sleeping_vanilla(
            task_patch={"gpu_limit": 1, "enable_gpu_layers": False},
        )

        # Wait for the allocation to actually run under the classic policy.
        wait(lambda: len(op.get_running_jobs()) == 1)
        job_id = list(op.get_running_jobs())[0]
        pre_switch_allocation_id = get_allocation_id_from_job_id(job_id)
        pre_switch_node_address = get(op.get_path() + f"/controller_orchid/running_jobs/{job_id}/address")

        op.wait_for_fresh_snapshot()

        # Switch the pool tree to the GPU allocating policy. This config update has no
        # effect until the scheduler restarts and rebuilds TPoolTree.
        update_pool_tree_config_option("gpu", "gpu_scheduling_policy", {
            "mode": "allocating",
            "plan_update_period": 100,
            "module_type": "data_center",
            "modules": [self.DATA_CENTER],
            "full_host_aggressive_preemption_timeout": 1000,
        })
        update_pool_tree_config_option("gpu", "policy_kind", "gpu")

        with Restarter(self.Env, SCHEDULERS_SERVICE):
            pass

        # Scheduler is back with the GPU policy. RegisterAllocationsFromRevivedOperation
        # runs Case B for the snapshotted allocation - no prior assignment, fresh GPU
        # policy state, so it synthesizes a "revived" assignment.
        wait(lambda: get_operation_from_gpu_policy_orchid(op)["enabled"])
        wait(lambda: len(get_operation_from_gpu_policy_orchid(op)["allocations"]) == 1)
        wait_for_assignments_in_gpu_policy_orchid(op, assignment_count=1, exactly=True)

        operation = get_operation_from_gpu_policy_orchid(op)
        revived = get_operation_gpu_assignments_from_gpu_policy_orchid(op)[0]
        assert revived["allocation_group_name"] == "revived"
        assert revived["allocation_id"] == pre_switch_allocation_id
        assert revived["node_address"] == pre_switch_node_address
        assert not revived["reviving"]

        assert pre_switch_allocation_id in operation["allocations"]
        assert operation["allocations"][pre_switch_allocation_id]["resource_usage"]["gpu"] == 1

        op.abort()
        wait_operation_unregistered(op.id)

##################################################################
