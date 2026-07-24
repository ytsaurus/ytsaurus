import builtins
import time

from copy import deepcopy

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
    run_test_vanilla, with_breakpoint, release_breakpoint, get_allocation_id_from_job_id, vanilla, update_op_parameters,
    print_debug, update_controller_agent_config, update_nodes_dynamic_config, get_applied_node_dynamic_config,
)

from yt_scheduler_helpers import (
    scheduler_orchid_path, scheduler_orchid_node_path, scheduler_new_orchid_pool_tree_path, scheduler_orchid_pool_path,
    scheduler_orchid_operation_path, scheduler_orchid_pool_tree_config_path
)

from yt_helpers import read_structured_log, write_log_barrier, profiler_factory

from yt.test_helpers import are_almost_equal

from yt_gpu_scheduler_helpers import (
    get_operation_from_gpu_policy_orchid, get_node_from_gpu_policy_orchid, get_operation_gpu_assignments_from_gpu_policy_orchid,
    wait_for_operations_in_gpu_policy_orchid, wait_for_assignments_in_gpu_policy_orchid, check_assignment_from_gpu_policy_orchid, check_operation_from_gpu_policy_orchid,
    check_gpu_allocations_from_gpu_policy_orchid, wait_for_gpu_allocations_empty_in_gpu_policy_orchid,
    wait_for_allocation_preempted, wait_for_gpu_event, read_gpu_events, is_default_guid,
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

        wait(lambda: get(scheduler_orchid_operation_path(op.id, tree="gpu") + "/resource_usage/gpu", default=None) in (None, 0))
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

        wait(lambda: get(scheduler_orchid_operation_path(op.id, tree="gpu") + "/resource_usage/gpu", default=None) in (None, 0))
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

        wait(lambda: get(scheduler_orchid_operation_path(op.id, tree="gpu") + "/resource_usage/gpu", default=None) in (None, 0))
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

        wait(lambda: get(scheduler_orchid_operation_path(op.id, tree="gpu") + "/resource_usage/gpu", default=None) in (None, 0))
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

        wait(lambda: get(scheduler_orchid_operation_path(op.id, tree="gpu") + "/resource_usage/gpu", default=None) in (None, 0))
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

        wait(lambda: get(scheduler_orchid_operation_path(op.id, tree="gpu") + "/resource_usage/gpu", default=None) in (None, 0))
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

        wait(lambda: get(scheduler_orchid_operation_path(op.id, tree="gpu") + "/resource_usage/gpu", default=None) in (None, 0))
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

        wait(lambda: get(scheduler_orchid_operation_path(op.id, tree="gpu") + "/resource_usage/gpu", default=None) in (None, 0))
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

        wait(lambda: get(scheduler_orchid_operation_path(op.id, tree="cpu") + "/resource_usage/cpu", default=None) in (None, 0.0))

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
            assert not is_default_guid(assignment["allocation_id"])
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

        wait(lambda: get(scheduler_orchid_operation_path(op.id, tree="gpu") + "/resource_usage/gpu", default=None) in (None, 0))

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

    @authors("yaishenka")
    def test_stuck_gpu_operation_is_aborted(self):
        update_scheduler_config("operation_stuck_check/period", 500)
        update_scheduler_config("operation_stuck_check/safe_timeout", 3000)

        # A scheduling tag filter that no node matches: the op is starving and feasible
        # but can never place an allocation, so it stays continuously starving with zero
        # realized allocations until the stuck check aborts it.
        op = run_test_vanilla(
            command="sleep 1000",
            spec={
                "pool_trees": ["gpu"],
                "scheduling_options_per_pool_tree": {
                    "gpu": {"scheduling_tag_filter": "nonexistent_tag"},
                },
            },
            task_patch={"gpu_limit": 8},
            track=False,
        )

        wait(lambda: op.get_state() == "failed", timeout=30)

        error_text = str(op.get_error()).lower()
        assert (
            "stuck" in error_text
            or "no successful scheduled allocations" in error_text
        )

    @authors("yaishenka")
    def test_healthy_gpu_operation_is_not_aborted_as_stuck(self):
        update_scheduler_config("operation_stuck_check/period", 500)
        update_scheduler_config("operation_stuck_check/safe_timeout", 2000)

        op = run_test_vanilla(
            command="sleep 30",
            spec={"pool_trees": ["gpu"]},
            task_patch={"gpu_limit": 8},
            track=False,
        )

        wait(lambda: op.get_state() == "running", timeout=20)

        # Wait > 2x safe_timeout to give the periodic stuck-check at least
        # two opportunities to fire. Healthy op must stay running.
        time.sleep(5)

        assert op.get_state() == "running", \
            "Healthy GPU op was wrongly aborted as stuck (state={})".format(op.get_state())

    @authors("yaishenka")
    def test_gpu_build_operation_progress_emits_expected_fields(self):
        op = run_test_vanilla(
            command="sleep 30",
            spec={"pool_trees": ["gpu"]},
            task_patch={"gpu_limit": 8},
            track=False,
        )
        wait(lambda: op.get_state() == "running", timeout=20)

        op_orchid_path = scheduler_orchid_operation_path(op.id, tree="gpu")
        wait(lambda: exists(op_orchid_path))
        wait(lambda: get(op_orchid_path + "/realized_assignment_count", default=0) >= 1)

        progress = get(op_orchid_path)
        for field in [
            "scheduling_module",
            "realized_assignment_count",
            "preliminary_assignment_count",
            "preemptible",
            "starving",
            "enabled",
        ]:
            assert field in progress, \
                "Missing field {} in GPU operation Orchid: {}".format(field, list(progress.keys()))

        assert progress["enabled"]
        assert progress["realized_assignment_count"] >= 1


##################################################################

class TestAllocatingGpuPolicyNetworkPriority(AllocatingGpuSchedulingPolicyBaseConfig):
    NUM_NODES = 5

    USE_PORTO = True

    DELTA_DYNAMIC_NODE_CONFIG = {
        "%true": {
            "exec_node": {
                "controller_agent_connector": {
                    "heartbeat_executor": {
                        "period": 500,
                    },
                },
                "gpu_manager": {
                    "enable_network_service_level": True,
                },
            },
        },
    }

    DELTA_NODE_CONFIG = {
        "exec_node": {
            "gpu_manager": {
                "testing": {
                    "test_resource": True,
                    "test_gpu_count": 8,
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
        },
        "slot_manager": {
            "job_environment": {
                "type": "porto",
            },
        },
    }

    MODULE_SHARE_TO_NETWORK_PRIORITY = [
        {"module_share": 0.1, "network_priority": 1},
        {"module_share": 0.4, "network_priority": 2},
        {"module_share": 0.8, "network_priority": 3},
    ]

    def setup_method(self, method):
        super(TestAllocatingGpuPolicyNetworkPriority, self).setup_method(method)

        update_pool_tree_config_option(
            "gpu",
            "gpu_scheduling_policy/module_share_to_network_priority",
            self.MODULE_SHARE_TO_NETWORK_PRIORITY)

    @authors("yaishenka")
    def test_network_priority(self):
        # NB(yaishenka): TGpuManager::ApplyNetworkPriority skips emitting the test event
        # when the new priority equals the current one (initially DefaultNetworkPriority = 0).
        # All four cases run in sequence so each transition is observable.

        update_pool_tree_config_option("gpu", "enable_step_function_for_gang_operations", False)

        # 5 jobs x 8 GPU => full module; share = 5/5 => priority 3.
        # NB(yaishenka): is_gang is required so the operation is full-host module-bound
        # (see TOperation::IsFullHostModuleBound).
        from_barriers = self.write_log_barriers_on_all_nodes()
        full_module_op = run_sleeping_vanilla(
            job_count=5,
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
            spec={"is_gang": True},
        )
        wait(lambda: len(full_module_op.get_running_jobs()) == 5)

        self.assert_network_priority_of_all_jobs(full_module_op, from_barriers, 3)
        full_module_op.abort()

        # 1 job x 8 GPU; share = 1/5 = 0.2 => priority 1.
        # Single-allocation vanilla is full-host module-bound without is_gang.
        from_barriers = self.write_log_barriers_on_all_nodes()
        one_node_op = run_sleeping_vanilla(
            job_count=1,
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
        )
        wait(lambda: len(one_node_op.get_running_jobs()) == 1)

        self.assert_network_priority_of_all_jobs(one_node_op, from_barriers, 1)
        one_node_op.abort()

        # 3 jobs x 8 GPU; share = 3/5 = 0.6 => priority 2.
        from_barriers = self.write_log_barriers_on_all_nodes()
        half_module_op = run_sleeping_vanilla(
            job_count=3,
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
            spec={"is_gang": True},
        )
        wait(lambda: len(half_module_op.get_running_jobs()) == 3)

        self.assert_network_priority_of_all_jobs(half_module_op, from_barriers, 2)
        half_module_op.abort()

        # 10 jobs x 4 GPU saturates all 5 nodes but op is not full-host (gpu_limit < 8),
        # so it is never module-bound. NetworkPriority stays nullopt => exec node falls
        # back to DefaultNetworkPriority = 0.
        from_barriers = self.write_log_barriers_on_all_nodes()
        non_full_host_op = run_sleeping_vanilla(
            job_count=10,
            task_patch={"gpu_limit": 4, "enable_gpu_layers": False},
        )
        wait(lambda: len(non_full_host_op.get_running_jobs()) == 10)

        self.assert_network_priority_of_all_jobs(non_full_host_op, from_barriers, 0)
        non_full_host_op.abort()

    def write_log_barriers_on_all_nodes(self):
        return [write_log_barrier(self.get_node_address(i)) for i in range(self.NUM_NODES)]

    def assert_network_priority_of_all_jobs(self, op, from_barriers, expected_network_priority):
        address_to_node_index = self.get_address_to_node_index()
        jobs = op.get_running_jobs().values()
        node_indices = builtins.set([address_to_node_index[job["address"]] for job in jobs])
        wait(lambda: self.check_network_priority_on_nodes(node_indices, from_barriers, expected_network_priority))

    def check_network_priority_on_nodes(self, node_indices, from_barriers, expected_network_priority):
        for node_index in node_indices:
            log = read_structured_log(
                self.get_structured_log_path(node_index),
                from_barrier=from_barriers[node_index],
                row_filter=lambda row: "event_type" in row and row["event_type"] == "apply_network_priority_in_test",
            )
            if len(log) == 0:
                print_debug("log of node {} is empty".format(node_index))
                return False

            assert len(log) == 1, "log has more than one row: {}".format(log)
            assert log[0]["network_priority"] == expected_network_priority, \
                "wrong network priority in log: {}, node_index: {}".format(log, node_index)
        return True

    def get_address_to_node_index(self):
        return {self.get_node_address(i): i for i in range(self.NUM_NODES)}

    def get_structured_log_path(self, node_id):
        return "{}/logs/node-{}.json.log".format(self.path_to_run, node_id)

    def get_node_address(self, node_id):
        return "localhost:" + str(self.Env.configs["node"][node_id]["rpc_port"])


##################################################################

class TestAllocatingGpuSchedulingPolicyPreemption(AllocatingGpuSchedulingPolicyBaseConfig):
    def _scheduler_log_file(self):
        return self.path_to_run + "/logs/scheduler-0.json.log"

    def _scheduler_address(self):
        return ls("//sys/scheduler/instances")[0]

    @authors("yaishenka")
    def test_preempt_assignment_exceeding_resource_limits(self):
        scheduler_log_file = self._scheduler_log_file()
        from_barrier = write_log_barrier(self._scheduler_address())

        op = run_sleeping_vanilla(
            task_patch={"gpu_limit": 4, "enable_gpu_layers": False},
            job_count=4,
        )

        wait(lambda: len(op.get_running_jobs()) == 4)
        wait(lambda: get(scheduler_orchid_operation_path(op.id, tree="gpu") + "/resource_usage/gpu", default=None) == 16)
        wait_for_assignments_in_gpu_policy_orchid(op, assignment_count=4, exactly=True)

        update_op_parameters(
            op.id,
            parameters={"scheduling_options_per_pool_tree": {"gpu": {"resource_limits": {"gpu": 8}}}},
        )

        events = wait_for_allocation_preempted(
            scheduler_log_file,
            from_barrier,
            op,
            reason="resource_limits_violated",
            count=2,
        )
        assert len(events) == 2
        for event in events:
            assert event["preempted_usage"]["gpu"] == 4
            assert event["preemption_info"]["reason"] == "resource_limits_violated"
            assert op.id in event["preemption_info"]["description"]

        wait(lambda: get(scheduler_orchid_operation_path(op.id, tree="gpu") + "/resource_usage/gpu", default=None) == 8)
        wait_for_assignments_in_gpu_policy_orchid(op, assignment_count=2, exactly=True)

        update_op_parameters(
            op.id,
            parameters={"scheduling_options_per_pool_tree": {"gpu": {"resource_limits": {"gpu": 0}}}},
        )

        wait_for_allocation_preempted(
            scheduler_log_file,
            from_barrier,
            op,
            reason="resource_limits_violated",
            count=4,
        )

        wait(lambda: get(scheduler_orchid_operation_path(op.id, tree="gpu") + "/resource_usage/gpu", default=None) == 0)
        wait_for_gpu_allocations_empty_in_gpu_policy_orchid(op)

    @authors("yaishenka")
    def test_starving_regular_op_preempts_low_priority(self):
        create_pool(
            "guaranteed_pool",
            pool_tree="gpu",
            attributes={"strong_guarantee_resources": {"gpu": 8}},
        )
        create_pool(
            "no_guaranteed_pool",
            pool_tree="gpu",
            wait_for_orchid=False,
        )

        scheduler_log_file = self._scheduler_log_file()

        filler_op = run_sleeping_vanilla(
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
            job_count=2,
            spec={"pool": "no_guaranteed_pool"},
        )

        wait(lambda: len(filler_op.get_running_jobs()) == 2)
        wait(lambda: get(scheduler_orchid_operation_path(filler_op.id, tree="gpu") + "/resource_usage/gpu", default=None) == 16)
        wait_for_assignments_in_gpu_policy_orchid(filler_op, assignment_count=2, exactly=True)

        from_barrier = write_log_barrier(self._scheduler_address())

        starving_op = run_sleeping_vanilla(
            task_patch={"gpu_limit": 4, "enable_gpu_layers": False},
            job_count=1,
            spec={"pool": "guaranteed_pool"},
        )

        wait(lambda: len(starving_op.get_running_jobs()) == 1)

        scheduling_event = wait_for_gpu_event(
            scheduler_log_file,
            from_barrier,
            event_type="allocation_scheduled",
            op=starving_op,
        )
        target_node = scheduling_event["node_address"]

        events = wait_for_allocation_preempted(
            scheduler_log_file,
            from_barrier,
            filler_op,
            reason="preemption",
        )
        # The starving op needs only 4 GPU. Preempting one filler frees a full
        # 8-GPU node — the planner should pick exactly one assignment.
        assert len(events) == 1
        event = events[0]
        assert event["preempted_usage"]["gpu"] == 8
        assert event["node_address"] == target_node
        assert event["preemption_info"]["reason"] == "preemption"
        assert event["preemption_info"]["preempted_for_operation_id"] == starving_op.id

    @authors("yaishenka")
    def test_disable_operation_removes_preliminary_assignments(self):
        scheduler_log_file = self._scheduler_log_file()

        # Effectively stop node -> scheduler heartbeats so the scheduler never
        # asks a node to start an allocation for the placed assignment. The GPU
        # planner still places a preliminary assignment (its plan update is
        # scheduler-side and independent of node heartbeats), but it is never
        # realized into an allocation. This avoids holding an in-flight
        # ScheduleAllocation in the CA, which made op.abort() flap.
        update_nodes_dynamic_config({
            "exec_node": {
                "scheduler_connector": {
                    "heartbeat_executor": {
                        "period": 1000000,  # 1000 sec
                    },
                },
            },
        })

        from_barrier = write_log_barrier(self._scheduler_address())

        op = run_sleeping_vanilla(
            task_patch={"gpu_limit": 4, "enable_gpu_layers": False},
        )

        wait_for_assignments_in_gpu_policy_orchid(op, assignment_count=1, exactly=True)

        assignment = get_operation_gpu_assignments_from_gpu_policy_orchid(op)[0]
        assert is_default_guid(assignment.get("allocation_id")), \
            "assignment should still be preliminary at this point"

        op.abort()

        # DisableOperation runs RemoveAssignment (silent) on the preliminary
        # assignment; UnregisterOperation then has nothing left to preempt.
        wait_operation_unregistered(op.id)

        to_barrier = write_log_barrier(self._scheduler_address())
        events = read_gpu_events(scheduler_log_file, from_barrier, to_barrier=to_barrier, op=op)
        event_types = {event["event_type"] for event in events}

        # Sanity: the planner did place the preliminary assignment.
        assert "assignment_added" in event_types
        # The contract: RemoveAssignment is silent and the assignment never
        # realized, so none of these events should appear for this op.
        for forbidden in ("assignment_preempted", "allocation_scheduled", "allocation_preempted"):
            assert forbidden not in event_types, \
                f"unexpected {forbidden} event for op disabled with only preliminary assignment"

    @authors("yaishenka")
    def test_full_host_aggressive_preemption_evicts_non_fhmb(self):
        # Goal: isolate trigger #3 (full-host aggressive preemption) from
        # trigger #2 (regular preemptive planning) and from the FHMB
        # self-preemptible failure mode.
        #
        # Constraints from the GPU policy:
        # - For regular ops: assignment->Preemptible is true iff cumulative
        #   usage_share exceeds fair_share
        #   (assignment_plan_update_context_detail.cpp:425-427).
        # - For FHMB ops: operation->Preemptible is true iff fair_share is
        #   effectively zero (line 404). A preemptible FHMB op crashes
        #   assignment_plan_update.cpp:284 (YT_VERIFY(!operation->IsPreemptible())).
        #
        # Both invariants must hold simultaneously, so total guarantees must
        # cover the cluster (16 GPU) with the FHMB pool getting a non-zero
        # slice. Two regular pools (guarantee=4 each) host an op using 8 GPU
        # apiece: of the two 4-GPU assignments, the first stays non-preemptible
        # (cumulative=4 == fair_share=4), the second is preemptible
        # (cumulative=8 > fair_share). With one preemptible 4-GPU assignment
        # per node, trigger #2's discount can free at most 4 GPU per node —
        # below the FHMB's 8-GPU demand. After the aggressive timeout, trigger
        # #3 evicts the remaining 4 GPU of non-preemptible regular assignments
        # to free a full host.
        create_pool(
            "regular_pool_a",
            pool_tree="gpu",
            attributes={"strong_guarantee_resources": {"gpu": 4}},
        )
        create_pool(
            "regular_pool_b",
            pool_tree="gpu",
            attributes={"strong_guarantee_resources": {"gpu": 4}},
        )
        create_pool(
            "fhmb_pool",
            pool_tree="gpu",
            attributes={"strong_guarantee_resources": {"gpu": 8}},
            wait_for_orchid=False,
        )

        scheduler_log_file = self._scheduler_log_file()

        regular_op_a = run_sleeping_vanilla(
            task_patch={"gpu_limit": 4, "enable_gpu_layers": False},
            job_count=2,
            spec={"pool": "regular_pool_a"},
        )
        regular_op_b = run_sleeping_vanilla(
            task_patch={"gpu_limit": 4, "enable_gpu_layers": False},
            job_count=2,
            spec={"pool": "regular_pool_b"},
        )

        wait(lambda: len(regular_op_a.get_running_jobs()) == 2)
        wait(lambda: len(regular_op_b.get_running_jobs()) == 2)
        wait_for_assignments_in_gpu_policy_orchid(regular_op_a, assignment_count=2, exactly=True)
        wait_for_assignments_in_gpu_policy_orchid(regular_op_b, assignment_count=2, exactly=True)

        from_barrier = write_log_barrier(self._scheduler_address())

        fhmb_op = run_sleeping_vanilla(
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
            spec={"pool": "fhmb_pool"},
        )

        # Bin-packing may place op_a / op_b either both on separate nodes or
        # interleaved across the two — the test doesn't care which.
        # Wait until enough aggressive-preemption events have freed a full host.
        def aggressive_events():
            return [
                event
                for event in read_gpu_events(scheduler_log_file, from_barrier, event_type="allocation_preempted")
                if event.get("reason") == "full_host_aggressive_preemption"
            ]

        wait(lambda: sum(event["preempted_usage"]["gpu"] for event in aggressive_events()) >= 8)

        events = aggressive_events()
        target_node = events[0]["node_address"]
        total_freed = 0
        for event in events:
            assert event["node_address"] == target_node, \
                "aggressive preemption should evict from a single node"
            assert event["preemption_info"]["reason"] == "full_host_aggressive_preemption"
            assert event["operation_id"] in (regular_op_a.id, regular_op_b.id)
            total_freed += event["preempted_usage"]["gpu"]
        assert total_freed == 8

        # Sanity: FHMB op gets scheduled on the freed node.
        fhmb_scheduled = wait_for_gpu_event(
            scheduler_log_file,
            from_barrier,
            event_type="allocation_scheduled",
            op=fhmb_op,
        )
        assert fhmb_scheduled["node_address"] == target_node
        assert fhmb_scheduled["resource_usage"]["gpu"] == 8


##################################################################

class TestAllocatingGpuPolicyDisableDuringScheduleAllocation(AllocatingGpuSchedulingPolicyBaseConfig):
    # A single 8-GPU node so that an operation with two 4-GPU jobs gets both of its
    # preliminary assignments placed on the same node, hence handled within one
    # ScheduleAllocations(node) loop.
    NUM_NODES = 1

    @authors("yaishenka")
    def test_disable_operation_while_scheduling_first_assignment(self):
        # Regression for the YT_VERIFY coredump in ScheduleAllocations.
        #
        # ScheduleAllocations iterates a *copy* of node->Assignments(). For the first
        # preliminary assignment it calls DoScheduleAllocation, which parks on
        # WaitFor(scheduleAllocationFuture) waiting for the controller. While the fiber is
        # parked (the control thread is free), op.abort() runs DisableOperation /
        # UnregisterOperation, which removes ALL of the operation's assignments from both the
        # node and the operation. When the parked heartbeat resumes and the loop reaches the
        # second assignment, the operation is no longer enabled, so the disabled branch ran
        # RemoveAssignment(assignment, /*strict*/ true) -> YT_VERIFY(node/operation contains
        # assignment) fails (the assignment was already removed) -> scheduler crash.
        #
        # schedule_allocation_delay_scheduler is a *scheduler-side* delay applied inside the GPU
        # policy's DoScheduleAllocation (on the control thread), independent of the controller
        # agent. With type "async" it yields the heartbeat fiber without any controller call in
        # flight, so:
        #   * the park cannot be cut short by op.abort() (which only completes the controller
        #     schedule-allocation future), and
        #   * op.abort()'s scheduler-side DisableOperation is not gated on a slow controller call,
        #     so it runs promptly while the heartbeat is parked mid-loop and removes both
        #     assignments.
        # (A controller-agent-side schedule_allocation_delay does NOT work here: op.abort() returns
        # the scheduler-side wait immediately but defers DisableOperation until the in-flight CA
        # call finishes, so the loop drains both assignments before the operation is disabled.)
        schedule_allocation_delay = 8000

        op = run_sleeping_vanilla(
            job_count=2,
            task_patch={"gpu_limit": 4, "enable_gpu_layers": False},
            spec={
                "testing": {
                    "schedule_allocation_delay_scheduler": {
                        "duration": schedule_allocation_delay,
                        "type": "async",
                    },
                },
            },
        )

        # Both 4-GPU jobs fit on the single 8-GPU node: the planner places two preliminary
        # assignments there, both processed in one ScheduleAllocations loop. The delay keeps
        # them from being realized, so they stay preliminary.
        wait_for_assignments_in_gpu_policy_orchid(op, assignment_count=2, exactly=True)

        assignments = get_operation_gpu_assignments_from_gpu_policy_orchid(op)
        assert len(assignments) == 2
        for assignment in assignments:
            assert is_default_guid(assignment.get("allocation_id")), \
                "assignments must still be preliminary (schedule allocation parked)"

        # Give a scheduling heartbeat time to enter DoScheduleAllocation for the first assignment
        # and park inside the scheduler-side delay. The park lasts ~schedule_allocation_delay, so
        # the abort below reliably lands while the fiber is parked.
        time.sleep(2)

        # Non-blocking: returns once the abort is accepted. The scheduler-side DisableOperation /
        # UnregisterOperation then run on the control thread during the park, removing both
        # assignments while the heartbeat is still parked.
        op.abort()

        # Let the parked heartbeat resume (after the scheduler-side delay) so the loop reaches the
        # already-removed second assignment and executes the disabled branch. Without the fix the
        # YT_VERIFY in RemoveAssignment(strict=true) fires and crashes the scheduler, which marks
        # the test red on its own. With the fix the scheduler survives and the operation cleans up.
        time.sleep(schedule_allocation_delay / 1000.0 + 2)

        wait_operation_unregistered(op.id)


##################################################################

class AllocatingGpuSchedulingPolicyMultiModuleBaseConfig(YTEnvSetup):
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
        dc_to_rack = dict(zip(self.DATA_CENTERS, self.RACKS))
        data_center_index_per_node = sum([[i] * count for i, count in enumerate(node_count_per_data_center)], [])

        def get_node_rack(i, node):
            if ibc_to_dc is None:
                return self.RACKS[data_center_index_per_node[i]]
            ibc = get("//sys/cluster_nodes/{}/@annotations/infiniband_cluster_tag".format(node))
            return dc_to_rack[ibc_to_dc[ibc]]

        nodes = list(ls("//sys/cluster_nodes"))
        for i, node in enumerate(nodes):
            set("//sys/cluster_nodes/{}/@rack".format(node), get_node_rack(i, node))

        rack_to_dc = dict(zip(self.RACKS, self.DATA_CENTERS))
        for i, node in enumerate(nodes):
            wait(lambda: get(scheduler_orchid_node_path(node) + "/data_center") == rack_to_dc[get_node_rack(i, node)])

    def setup_method(self, method):
        super(AllocatingGpuSchedulingPolicyMultiModuleBaseConfig, self).setup_method(method)

        update_pool_tree_config("default", {"node_tag_filter": "!gpu"})
        create_pool_tree("gpu", config={
            "node_tag_filter": "gpu",
            "main_resource": "gpu",
            "gpu_scheduling_policy": {
                "mode": "allocating",
                "plan_update_period": 100,
                "module_type": "data_center",
                "modules": self.DATA_CENTERS,
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

        dc_to_rack = dict(zip(self.DATA_CENTERS, self.RACKS))
        for dc, r in dc_to_rack.items():
            create_data_center(dc)
            create_rack(r)
            set("//sys/racks/{}/@data_center".format(r), dc)

        for node in ls("//sys/cluster_nodes"):
            set("//sys/cluster_nodes/{}/@user_tags".format(node), ["gpu"])

        module_count = self.NUM_NODES // len(self.DATA_CENTERS)
        self._setup_data_centers([module_count] * len(self.DATA_CENTERS))

        wait(lambda: get(scheduler_new_orchid_pool_tree_path("gpu") + "/node_count") == self.NUM_NODES)


##################################################################

class TestAllocatingGpuSchedulingPolicyMultiModule(AllocatingGpuSchedulingPolicyMultiModuleBaseConfig):
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

    @authors("severovv")
    def test_priority_module_binding(self):
        update_pool_tree_config_option("gpu", "gpu_scheduling_policy/priority_module_binding_timeout", 1000)

        create_pool("pool1", pool_tree="gpu", attributes={}, wait_for_orchid=False)
        create_pool(
            "pool2",
            pool_tree="gpu",
            attributes={
                "strong_guarantee_resources": {"gpu": 16},
                "enable_priority_scheduling_segment_module_assignment": True,
            })

        annoying_ops = []
        for dc in self.DATA_CENTERS:
            annoying_ops.append(run_sleeping_vanilla(
                job_count=1,
                pool="pool1",
                task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
                spec={"scheduling_modules": [dc]},
            ))

        for op in annoying_ops:
            wait(lambda: len(op.get_running_jobs()) == 1)

        good_op = run_sleeping_vanilla(
            job_count=2,
            pool="pool2",
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
            spec={"is_gang": True},
        )
        wait(lambda: len(good_op.get_running_jobs()) == 2)

        def get_priority_module_binding(op) -> bool | None:
            value = get(
                scheduler_new_orchid_pool_tree_path("gpu") + f"/gpu_assignment_plan/operations/{op.id}/priority_module_binding_enabled",
                default=None,
            )
            return None if value is None else bool(value)

        wait(lambda: get_priority_module_binding(good_op) is True)
        for op in annoying_ops:
            assert get_priority_module_binding(op) is False

    @authors("severovv")
    def test_priority_module_binding_inheritance(self):
        # parent enables priority module binding; one child inherits it, the other overrides it to false.
        create_pool(
            "parent",
            pool_tree="gpu",
            attributes={"enable_priority_scheduling_segment_module_assignment": True})
        create_pool("child_inherit", pool_tree="gpu", parent_name="parent", attributes={})
        create_pool(
            "child_unset",
            pool_tree="gpu",
            parent_name="parent",
            attributes={"enable_priority_scheduling_segment_module_assignment": False})
        create_pool("default_pool", pool_tree="gpu", attributes={})

        ops = {
            pool: run_sleeping_vanilla(
                job_count=1,
                pool=pool,
                task_patch={"gpu_limit": 1, "enable_gpu_layers": False},
            )
            for pool in ("child_inherit", "child_unset", "default_pool")
        }

        for op in ops.values():
            wait(lambda: len(op.get_running_jobs()) == 1)

        def get_priority_module_binding(op) -> bool | None:
            value = get(
                scheduler_new_orchid_pool_tree_path("gpu") + f"/gpu_assignment_plan/operations/{op.id}/priority_module_binding_enabled",
                default=None,
            )
            return None if value is None else bool(value)

        wait(lambda: get_priority_module_binding(ops["child_inherit"]) is True)
        assert get_priority_module_binding(ops["child_unset"]) is False
        assert get_priority_module_binding(ops["default_pool"]) is False

    @authors("bystrovserg")
    def test_operation_module_survives_policy_switch_to_classic(self):
        policy_kind_path = scheduler_orchid_pool_tree_config_path("gpu") + "/policy_kind"
        wait(lambda: get(policy_kind_path, default=None) == "gpu")

        op = run_sleeping_vanilla(
            job_count=1,
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
        )
        wait(lambda: get_operation_from_gpu_policy_orchid(op).get("scheduling_module") in self.DATA_CENTERS)
        module = get_operation_from_gpu_policy_orchid(op)["scheduling_module"]

        # Fill the rest of the op's module, leaving the other module completely empty.
        nodes_per_module = self.NUM_NODES // len(self.DATA_CENTERS)
        blocker = run_sleeping_vanilla(
            job_count=nodes_per_module - 1,
            spec={"scheduling_modules": [module]},
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
        )
        wait(lambda: len(blocker.get_running_jobs()) == nodes_per_module - 1)

        op_gpu_module_path = (
            "//sys/scheduler/strategy_state/tree_states/gpu/scheduling_policy_state"
            "/operation_states/{}/scheduling_module".format(op.id)
        )
        wait(lambda: exists(op_gpu_module_path) and get(op_gpu_module_path) == module)

        # Enable classic scheduling segments and switch the tree to the classic policy.
        update_pool_tree_config_option("gpu", "scheduling_segments", {
            "mode": "large_gpu",
            "initialization_timeout": 60000,
            "manage_period": 100,
            "unsatisfied_segments_rebalancing_timeout": 1000,
            "data_centers": self.DATA_CENTERS,
            "module_type": "data_center",
        })
        update_pool_tree_config_option("gpu", "policy_kind", "classic")

        with Restarter(self.Env, SCHEDULERS_SERVICE):
            pass

        # The tree is now running the classic policy.
        wait(lambda: get(policy_kind_path, default=None) == "classic")

        # The op must stay on its persisted (full) module instead of moving to the empty one.
        op_classic_module_path = scheduler_orchid_operation_path(op.id, tree="gpu") + "/scheduling_segment_module"
        wait(lambda: get(op_classic_module_path, default=None) == module)

        op.abort()
        blocker.abort()

##################################################################


class TestAllocatingGpuSchedulingPolicyMetrics(AllocatingGpuSchedulingPolicyMultiModuleBaseConfig):
    NUM_NODES = 1
    DATA_CENTERS = ["SAS"]
    RACKS = ["SAS1"]

    @authors("severovv")
    def test_scheduling_metrics(self):
        profiler = profiler_factory().at_scheduler(fixed_tags={"tree": "gpu", "scheduling_stage": "gpu"})
        prefix = "scheduler/"

        controller_schedule_count = profiler.counter(prefix + "controller_schedule_job_count")
        attempt_count = profiler.counter(prefix + "schedule_job_attempt_count")
        failure_count = profiler.counter(prefix + "schedule_job_failure_count")
        no_pending_jobs_fail_count = profiler.counter(prefix + "controller_schedule_job_fail", tags={"reason": "no_pending_jobs"})

        scheduled_allocation_count = profiler.counter(prefix + "scheduled_allocation_count")
        preempted_allocation_count = profiler.counter(prefix + "preempted_allocation_count")

        controller_schedule_time = profiler.summary(prefix + "controller_schedule_job_time")
        total_controller_schedule_time = profiler.summary(prefix + "controller_schedule_job_time/total")
        exec_controller_schedule_time = profiler.summary(prefix + "controller_schedule_job_time/exec")

        cumulative_total_controller_time = profiler.counter(prefix + "cumulative_controller_schedule_job_time/total")
        cumulative_exec_controller_time = profiler.counter(prefix + "cumulative_controller_schedule_job_time/exec")

        module_profiler = profiler_factory().at_scheduler(fixed_tags={"tree": "gpu", "module": "SAS"})
        module_prefix = prefix + "gpu_policy/module/"
        module_total_nodes = module_profiler.gauge(module_prefix + "total_nodes_count")
        module_unreserved_nodes = module_profiler.gauge(module_prefix + "unreserved_nodes_count")
        module_full_host_bound_operations = module_profiler.gauge(module_prefix + "full_host_module_bound_operations_count")

        _ = run_sleeping_vanilla(
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
            job_count=1,
        )

        wait(lambda: controller_schedule_count.get_delta() >= 1)
        wait(lambda: scheduled_allocation_count.get_delta() >= 1)

        wait(lambda: attempt_count.get() >= 1)
        wait(lambda: controller_schedule_count.get() >= 1)
        wait(lambda: failure_count.get(default=None) is not None)
        wait(lambda: no_pending_jobs_fail_count.get(default=None) is not None)
        wait(lambda: preempted_allocation_count.get(default=None) is not None)

        wait(lambda: controller_schedule_time.get_max(default=None) is not None)
        wait(lambda: total_controller_schedule_time.get_max(default=None) is not None)
        wait(lambda: exec_controller_schedule_time.get_max(default=None) is not None)

        wait(lambda: cumulative_total_controller_time.get() > 0)
        wait(lambda: cumulative_exec_controller_time.get() > 0)

        # The single full-host operation reserves one of the module's nodes.
        wait(lambda: module_total_nodes.get() == self.NUM_NODES)
        wait(lambda: module_unreserved_nodes.get() == 0)
        wait(lambda: module_full_host_bound_operations.get() == 1)

##################################################################


class TestAllocatingGpuSchedulingPolicyMultiModulePreemption(AllocatingGpuSchedulingPolicyMultiModuleBaseConfig):
    def _scheduler_log_file(self):
        return self.path_to_run + "/logs/scheduler-0.json.log"

    def _scheduler_address(self):
        return ls("//sys/scheduler/instances")[0]

    @authors("yaishenka")
    def test_full_host_op_evicted_from_module_after_timeout(self):
        # Speed up the reconsideration timeout; push the aggressive timeout
        # far out so trigger #3 doesn't fire first.
        update_pool_tree_config_option(
            "gpu",
            "gpu_scheduling_policy/module_reconsideration_timeout",
            2000,
        )
        update_pool_tree_config_option(
            "gpu",
            "gpu_scheduling_policy/full_host_aggressive_preemption_timeout",
            60000,
        )

        # Pin a small filler op to one VLA node so that the FHMB op (pinned
        # to VLA via scheduling_modules) cannot place a full host there.
        # With only 1 free VLA node and demand=2 hosts, the FHMB op waits
        # past module_reconsideration_timeout and gets evicted.
        vla_nodes = [
            n for n in ls("//sys/cluster_nodes")
            if get(f"//sys/cluster_nodes/{n}/@data_center") == "VLA"
        ]
        assert len(vla_nodes) == 2
        target_vla_node = vla_nodes[0]

        create_pool(
            "filler_pool",
            pool_tree="gpu",
            attributes={"strong_guarantee_resources": {"gpu": 4}},
        )
        create_pool(
            "fhmb_pool",
            pool_tree="gpu",
            attributes={"strong_guarantee_resources": {"gpu": 16}},
            wait_for_orchid=False,
        )

        filler_op = run_sleeping_vanilla(
            task_patch={"gpu_limit": 4, "enable_gpu_layers": False},
            spec={
                "pool": "filler_pool",
                "scheduling_tag_filter": target_vla_node,
            },
        )

        wait(lambda: len(filler_op.get_running_jobs()) == 1)
        wait_for_assignments_in_gpu_policy_orchid(filler_op, assignment_count=1, exactly=True)

        scheduler_log_file = self._scheduler_log_file()
        from_barrier = write_log_barrier(self._scheduler_address())

        fhmb_op = run_sleeping_vanilla(
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
            job_count=2,
            spec={
                "pool": "fhmb_pool",
                "scheduling_modules": ["VLA"],
                "is_gang": True,
            },
        )

        # After module_reconsideration_timeout, the FHMB op's single placed
        # full-host allocation is preempted via EvictOperationFromSchedulingModule.
        events = wait_for_allocation_preempted(
            scheduler_log_file,
            from_barrier,
            fhmb_op,
            reason="eviction_from_scheduling_module",
        )
        assert len(events) >= 1
        event = events[0]
        assert event["preempted_usage"]["gpu"] == 8
        assert event["preemption_info"]["reason"] == "eviction_from_scheduling_module"

    @authors("yaishenka")
    def test_full_host_op_rebound_preempts_old_module_assignments(self):
        # Trigger #6 fires inside BindFullHostOperationToModule when an
        # operation has assignments in module X (operationUsedModule = X)
        # but the planner picks a different module Y as the bind target.
        # That requires the op to be in a transient state where
        # SchedulingModule is unset *and* Assignments_ is non-empty. The
        # only path to that state is via InitializeModuleStates
        # (assignment_plan_update.cpp:186): if the op is preemptible at
        # plan-tick time, the planner resets its SchedulingModule but
        # leaves its assignments alone.
        #
        # For an FHMB gang operation, SetPreemptible is driven by
        # Dominates(epsilon, fair_share) (context_detail.cpp:404). With
        # enable_step_function_for_gang_operations on, the fair-share
        # update zeroes the gang's fair_share whenever it can't be funded
        # in full — so we can flip op_gang preemptible by introducing a
        # pool that swallows the rest of the cluster.
        #
        # While op_gang is preemptible, UpdateOperationResources zeroes
        # its ready-to-assign count, so the planner does NOT add it to
        # operationsToPlan (avoiding the YT_VERIFY(!preemptible) at
        # assignment_plan_update.cpp:284). That gives us a safe window
        # to also ban a node in op_gang's original module: the ban runs
        # UpdateNodeDescriptor → PreemptAllNodeAssignments synchronously
        # and removes one of the two existing assignments without
        # disturbing the (still-preemptible) op_gang.
        #
        # Sequence:
        #   1. Place a 2-job gang FHMB op_gang.
        #   2. Start op_filler in pool_guaranteed (strong_guarantee = full
        #      cluster). op_filler's scheduling_tag_filter matches no node,
        #      so it claims the pool's fair share without ever placing or
        #      preempting anything.
        #   3. Wait for op_gang.scheduling_module to be reset (orchid
        #      signal that the gang step-function zeroed fair_share and
        #      InitializeModuleStates cleared the bind).
        #   4. Ban one node in op_gang's original module. The synchronous
        #      preemption shrinks op_gang.Assignments_ from 2 to 1; ready
        #      stays 0 because op_gang is still preemptible.
        #   5. Abort op_filler. op_gang.fair_share recovers → next plan
        #      tick marks op_gang non-preemptible and recomputes its
        #      ready-to-assign count (now 1).
        #   6. ProcessFullHostMBOps adds op_gang via shouldPlanModule
        #      (!preemptible, !SchedulingModule, !IsZeroAssignedUsage).
        #      BindFullHostOperationToModule sees the original module's
        #      GetNodeCount = 1 (the banned node is gone from AvailableNodes_)
        #      < demand = 2 → infeasible. The other module is feasible →
        #      operationUsedModule != bestModule → trigger #6 fires and
        #      preempts the surviving original-module assignment.
        update_scheduler_config("node_registration_timeout", 1000)
        update_scheduler_config("node_heartbeat_timeout", 1000)
        update_scheduler_config("node_reconnection_timeout", 1000)
        update_pool_tree_config_option("gpu", "enable_step_function_for_gang_operations", True)

        create_pool("pool_gang", pool_tree="gpu")
        create_pool(
            "pool_guaranteed",
            pool_tree="gpu",
            attributes={"strong_guarantee_resources": {"gpu": 32}},
        )

        op_gang = run_sleeping_vanilla(
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
            job_count=2,
            spec={"pool": "pool_gang", "is_gang": True},
        )

        wait(lambda: len(op_gang.get_running_jobs()) == 2)
        wait(lambda: get(scheduler_orchid_operation_path(op_gang.id, tree="gpu") + "/grouped_needed_resources", default=None) == {})
        wait_for_assignments_in_gpu_policy_orchid(op_gang, assignment_count=2, exactly=True)

        initial_assignments = get_operation_gpu_assignments_from_gpu_policy_orchid(op_gang)
        initial_nodes = [a["node_address"] for a in initial_assignments]
        initial_module = get(f"//sys/cluster_nodes/{initial_nodes[0]}/@data_center")

        op_filler = run_sleeping_vanilla(
            task_patch={"gpu_limit": 6, "enable_gpu_layers": False},
            job_count=5,
            spec={
                "pool": "pool_guaranteed",
                "scheduling_tag_filter": "nonexistent_tag_for_op_b",
            },
        )

        def op_gang_null_module():
            op_view = get_operation_from_gpu_policy_orchid(op_gang)
            return op_view.get("scheduling_module", yson.YsonEntity()) == yson.YsonEntity()
        wait(op_gang_null_module)

        set_node_banned(initial_nodes[0], True, wait_for_master=True, wait_for_scheduler=True)
        wait(lambda: not exists(scheduler_new_orchid_pool_tree_path("gpu") + f"/gpu_assignment_plan/nodes/{initial_nodes[0]}"))

        scheduler_log_file = self._scheduler_log_file()
        from_barrier = write_log_barrier(self._scheduler_address())

        op_filler.abort()

        events = wait_for_allocation_preempted(
            scheduler_log_file,
            from_barrier,
            op_gang,
            reason="operation_bound_to_other_module",
        )
        assert len(events) >= 1
        event = events[0]
        assert event["preempted_usage"]["gpu"] == 8
        assert event["preemption_info"]["reason"] == "operation_bound_to_other_module"
        assert get(f"//sys/cluster_nodes/{event['node_address']}/@data_center") == initial_module


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
        # we need to separate plan updates and node heartbeats, because they are taking one lock
        # if plan update starts between node heartbeats they get separated and test fails
        update_pool_tree_config_option("gpu", "gpu_scheduling_policy/plan_update_period", 1000000)
        # wait for last plan update
        time.sleep(0.2)

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
        wait_for_operations_in_gpu_policy_orchid(operation_count=1)

        # Kickstart a single planning iteration with a period large enough (2000ms)
        # that no subsequent plan update can fire during the test window:
        # SA delay (1000ms) + heartbeat period (500ms) ≈ 1500ms < 2000ms.
        # A heartbeat arriving before this kickstart is harmless — node->Assignments()
        # is still empty and ScheduleAllocations is a no-op.
        update_pool_tree_config_option("gpu", "gpu_scheduling_policy/plan_update_period", 2000)

        wait_for_assignments_in_gpu_policy_orchid(op, assignment_count=2, exactly=True)
        wait(lambda: len(op.get_running_jobs()) == 1)
        wait(lambda: get(op.get_path() + "/controller_orchid/progress/jobs/aborted/non_scheduled/scheduling_resource_overcommit", 0) == 1)

        update_pool_tree_config_option("gpu", "gpu_scheduling_policy/plan_update_period", 100)


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
        # Case A invariant: original group name preserved.
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
        # non-preliminary assignment via the standard preliminary->realized flow. The
        # controller->scheduler revival protocol propagates the original allocation group
        # name, so the synthesized assignment keeps the original task name.

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
        # Case B invariant: synthesized assignment keeps the original group name and is not flagged.
        assert revived["allocation_group_name"] == original_group_name
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
        # verifies the new assignment uses the task group name and a different
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

        update_pool_tree_config_option("gpu", "gpu_scheduling_policy/testing_options", {
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

        update_pool_tree_config_option("gpu", "gpu_scheduling_policy/testing_options", {
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
        assert new_assignment["allocation_group_name"] == "task"
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
        # with the original task group name and a brand-new allocation id
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
        assert new_assignment["allocation_group_name"] == "task"
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
        # RevivePendingAllocations creates an assignment carrying the original allocation
        # group name and links the orphan TAllocationState to it via SetAssignment. The
        # test verifies that the allocation
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
        # preliminary->realized flow with the original allocation group name (propagated
        # by the revival protocol) and the original allocation_id reattached.

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
        # policy state, so it synthesizes an assignment carrying the original task group name.
        wait(lambda: get_operation_from_gpu_policy_orchid(op)["enabled"])
        wait(lambda: len(get_operation_from_gpu_policy_orchid(op)["allocations"]) == 1)
        wait_for_assignments_in_gpu_policy_orchid(op, assignment_count=1, exactly=True)

        operation = get_operation_from_gpu_policy_orchid(op)
        revived = get_operation_gpu_assignments_from_gpu_policy_orchid(op)[0]
        # run_sleeping_vanilla creates a single task named "task".
        assert revived["allocation_group_name"] == "task"
        assert revived["allocation_id"] == pre_switch_allocation_id
        assert revived["node_address"] == pre_switch_node_address
        assert not revived["reviving"]

        assert pre_switch_allocation_id in operation["allocations"]
        assert operation["allocations"][pre_switch_allocation_id]["resource_usage"]["gpu"] == 1

        op.abort()
        wait_operation_unregistered(op.id)

    @authors("bystrovserg")
    def test_operation_module_survives_policy_switch_to_allocating(self):
        module = self.DATA_CENTER
        policy_kind_path = scheduler_orchid_pool_tree_config_path("gpu") + "/policy_kind"

        # The tree starts under the classic policy (set in setup_method).
        wait(lambda: get(policy_kind_path, default=None) == "classic")

        # Enable classic large-GPU scheduling segments so full-host ops become module-bound.
        update_pool_tree_config_option("gpu", "scheduling_segments", {
            "mode": "large_gpu",
            "initialization_timeout": 10000,
            "manage_period": 100,
            "unsatisfied_segments_rebalancing_timeout": 1000,
            "data_centers": [module],
            "module_type": "data_center",
        })
        create_pool("large_gpu", pool_tree="gpu", attributes={"allow_normal_preemption": False})

        # Fill the module with a running full-host op so the target op can only reserve it.
        blocker = run_sleeping_vanilla(
            job_count=self.NUM_NODES,
            spec={"pool": "large_gpu", "scheduling_segment_modules": [module]},
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
        )
        wait(lambda: len(blocker.get_running_jobs()) == self.NUM_NODES)

        op = run_sleeping_vanilla(
            job_count=1,
            spec={"pool": "large_gpu"},
            task_patch={"gpu_limit": 8, "enable_gpu_layers": False},
        )
        op_module_path = (
            "//sys/scheduler/strategy_state/tree_states/gpu/scheduling_policy_state"
            "/scheduling_segments_state/operation_states/{}/module".format(op.id)
        )
        wait(lambda: exists(op_module_path) and get(op_module_path) == module)
        assert op.get_running_jobs() == {}

        # Switch to the GPU allocating policy and restart. The long init timeout freezes planning.
        update_pool_tree_config_option("gpu", "gpu_scheduling_policy", {
            "mode": "allocating",
            "plan_update_period": 100,
            "module_type": "data_center",
            "modules": [module],
            "full_host_aggressive_preemption_timeout": 1000,
            "initialization_timeout": 60000,
        })
        update_pool_tree_config_option("gpu", "policy_kind", "gpu")

        with Restarter(self.Env, SCHEDULERS_SERVICE):
            pass

        # The tree is now running the GPU policy.
        wait(lambda: get(policy_kind_path, default=None) == "gpu")

        # During the initialization window the reserving op must already be on its persisted module
        wait_for_operations_in_gpu_policy_orchid(operation_count=2)
        wait(lambda: get_operation_from_gpu_policy_orchid(op).get("scheduling_module") == module)
        wait(lambda: get_operation_from_gpu_policy_orchid(blocker).get("scheduling_module") == module)

        op.abort()
        blocker.abort()

##################################################################


class TestProcessAllocationUpdateAfterFinishRace(YTEnvSetup):
    """Reproduces a race between a preemptible-progress-reset allocation update and a Finished update.

    A reset update for a classic-tree allocation is swapped out of the node shard's submit map by a batch
    that also carries gpu-tree updates. The gpu allocating policy parks the batch fiber (WaitFor hop to the
    strategy control invoker, stretched by a testing delay). While the batch is parked, the allocation
    finishes: UnregisterAllocation creates a fresh Finished entry, which a later batch processes first,
    removing the allocation from the operation shared state. Pre-fix the parked batch then wakes and
    processes the stale reset for the already-removed allocation, hitting the YT_VERIFY in
    TOperationSharedState::GetAllocationProperties. Post-fix the classic reset is applied synchronously
    before the batch parks, so it can never overtake the finish.
    """

    ENABLE_MULTIDAEMON = False  # Scheduler crash must not take down other components.
    NUM_MASTERS = 1
    NUM_NODES = 4
    NUM_SCHEDULERS = 1

    DATA_CENTER = "SAS"
    RACK = "SAS1"

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            # Single shard: gpu and classic allocation updates must co-batch.
            "node_shard_count": 1,
            "watchers_update_period": 100,
            "fair_share_update_period": 100,
            "fair_share_profiling_period": 100,
            # Frequent resource usage updates: every gpu heartbeat batch carries gpu updates and parks.
            "running_allocations_update_period": 100,
            "testing_options": {
                # Keep the operation enabled in strategy after completion aborts its allocations.
                "finish_operation_transition_delay": {"duration": 3000, "type": "async"},
            },
        }
    }

    # NB: The test environment already sets running_job_time_statistics_updates_send_period to 10ms
    # (an alias of running_allocation_time_statistics_updates_send_period), so preemptible progress
    # resets reach the scheduler quickly without an override here.

    DELTA_NODE_CONFIG = {
        "exec_node": {
            "gpu_manager": {
                "testing": {
                    "test_resource": True,
                    "test_gpu_count": 8,
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
        },
    }

    DELTA_DYNAMIC_NODE_CONFIG = {
        "%true": {
            "exec_node": {
                "job_controller": {
                    "allocation": {
                        "enable_multiple_jobs": True,
                    },
                },
                "scheduler_connector": {
                    # NB: the node's own heartbeats sweep the submit map, competing with parking gpu
                    # batches for the sitting reset updates. setup_method re-splits this into
                    # 200ms (gpu nodes) / 400ms (classic node) via tag-filtered config entries.
                    "heartbeat_executor": {
                        "period": 200,
                    },
                },
                "controller_agent_connector": {
                    # Load-bearing: resets are generated from running-job summaries on this channel.
                    "heartbeat_executor": {
                        "period": 100,
                    },
                },
            },
        }
    }

    def setup_method(self, method):
        super(TestProcessAllocationUpdateAfterFinishRace, self).setup_method(method)

        # Disable the periodic submit executor: heartbeat-driven submits only, so reset updates
        # sit in the member map until a (possibly parking) heartbeat batch sweeps them.
        update_scheduler_config("node_shard_submit_allocations_to_strategy_period", 600000000)

        update_pool_tree_config("default", {"node_tag_filter": "!gpu & !main"})

        create_pool_tree("gpu", config={
            "node_tag_filter": "gpu",
            "main_resource": "gpu",
            "gpu_scheduling_policy": {
                "mode": "allocating",
                "plan_update_period": 100,
                "module_type": "data_center",
                "modules": [self.DATA_CENTER],
                # Stretch the park of every batch processing gpu-tree updates so it covers the captured
                # reset's allocation finish (~1.0-2.7s after capture).
                "testing_options": {
                    "delay_inside_process_allocation_updates": {"duration": 1500, "type": "async"},
                },
            },
            "policy_kind": "gpu",
        })
        create_pool_tree("main", config={
            "node_tag_filter": "main",
        })

        set("//sys/pool_trees/@default_tree", "main")
        wait(lambda: get(scheduler_orchid_path() + "/scheduler/default_pool_tree", default=None) == "main")

        create_data_center(self.DATA_CENTER)
        create_rack(self.RACK)
        set("//sys/racks/{}/@data_center".format(self.RACK), self.DATA_CENTER)

        nodes = ls("//sys/cluster_nodes")
        for node in nodes:
            set("//sys/cluster_nodes/{}/@rack".format(node), self.RACK)
        for node in nodes[:3]:
            set("//sys/cluster_nodes/{}/@user_tags".format(node), ["gpu"])
        set("//sys/cluster_nodes/{}/@user_tags".format(nodes[3]), ["main"])

        # The classic node sweeps the submit map at 400ms while gpu nodes stay at 200ms: fewer classic
        # sweeps mean more sitting reset updates are captured by parking gpu batches. Node dynamic config
        # requires every node to match exactly one tag-filter entry and entries do not merge, so the
        # "%true" defaults (enable_multiple_jobs, the connector periods, framework defaults) are cloned
        # into both groups.
        node_config = get("//sys/cluster_nodes/@config")
        gpu_node_config = deepcopy(node_config["%true"])
        classic_node_config = deepcopy(node_config["%true"])
        classic_node_config["exec_node"]["scheduler_connector"]["heartbeat_executor"]["period"] = 400
        set("//sys/cluster_nodes/@config", {"gpu": gpu_node_config, "!gpu": classic_node_config})

        def get_scheduler_heartbeat_period(node):
            applied = get_applied_node_dynamic_config(node)
            return applied.get("exec_node", {}).get("scheduler_connector", {}).get("heartbeat_executor", {}).get("period")

        wait(lambda: get_scheduler_heartbeat_period(nodes[3]) == 400)
        for node in nodes[:3]:
            wait(lambda: get_scheduler_heartbeat_period(node) == 200)

        for node in nodes[:3]:
            wait(lambda: get(scheduler_orchid_node_path(node) + "/data_center", default=None) == self.DATA_CENTER)

        wait(lambda: get(scheduler_new_orchid_pool_tree_path("gpu") + "/node_count") == 3)
        wait(lambda: get(scheduler_new_orchid_pool_tree_path("main") + "/node_count") == 1)

    @authors("yaishenka")
    def test_preemptible_progress_reset_races_allocation_finish(self):
        # Long-running gpu operation: 6 allocations of 4 gpus are forced to spread 2 per node,
        # producing resource usage updates in every gpu node heartbeat batch.
        gpu_op = run_sleeping_vanilla(
            job_count=6,
            spec={"pool_trees": ["gpu"]},
            task_patch={"gpu_limit": 4, "enable_gpu_layers": False},
        )
        wait(lambda: len(gpu_op.get_running_jobs()) == 6)

        # Classic operations: 5 concurrent multi-job allocations, 2 jobs each — half of all resets are
        # last-job resets, each racing the allocation's Finished update right after the second job ends.
        for _ in range(8):
            op = run_test_vanilla(
                command="sleep 0.5",
                job_count=10,
                spec={"enable_multiple_jobs_in_allocation": True},
                task_patch={"cpu_limit": 2.0},
            )
            op.track()

        gpu_op.abort()

##################################################################
