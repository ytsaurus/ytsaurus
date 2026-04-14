import builtins
import time

import yt.yson as yson

from yt_env_setup import (
    YTEnvSetup,
)

from yt_commands import (
    authors, create, wait, write_table, ls, get, create_data_center, create_rack, run_sleeping_vanilla, update_pool_tree_config,
    update_pool_tree_config_option, create_pool_tree, exists, map, update_scheduler_config, create_pool, set_node_banned, set,
    run_test_vanilla, with_breakpoint, release_breakpoint, get_allocation_id_from_job_id,
)

from yt_scheduler_helpers import (
    scheduler_orchid_path, scheduler_orchid_node_path, scheduler_new_orchid_pool_tree_path, scheduler_orchid_pool_path,
    scheduler_orchid_operation_path
)

from yt.test_helpers import are_almost_equal

from yt_gpu_scheduler_helpers import (
    get_operation_from_gpu_policy_orchid, get_node_from_gpu_policy_orchid, get_operation_gpu_assignments_from_gpu_policy_orchid,
    wait_for_operations_in_gpu_policy_orchid, wait_for_assignments_in_gpu_policy_orchid, check_assignment_from_gpu_policy_orchid, check_operation_from_gpu_policy_orchid,
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

        node_address = assignment["node_address"]
        node = get(scheduler_new_orchid_pool_tree_path("gpu") + f"/gpu_assignment_plan/nodes/{node_address}")
        assert len(node["assignments"]) == 1
        compare_assignment_without_preemptible_start_time(node["assignments"][0], assignment)

        release_breakpoint()

        wait(lambda: get(scheduler_orchid_operation_path(op.id, tree="gpu") + "/resource_usage/gpu", default=None) == 0)
        wait(lambda: len(get(scheduler_new_orchid_pool_tree_path("gpu") + f"/gpu_assignment_plan/nodes/{node_address}/assignments")) == 0)

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

        release_breakpoint()

        wait(lambda: get(scheduler_orchid_operation_path(op.id, tree="gpu") + "/resource_usage/gpu", default=None) == 0)

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

        release_breakpoint()

        wait(lambda: get(scheduler_orchid_operation_path(op.id, tree="gpu") + "/resource_usage/gpu", default=None) == 0)

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

        release_breakpoint()

        wait(lambda: get(scheduler_orchid_operation_path(op.id, tree="gpu") + "/resource_usage/gpu", default=None) == 0)

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

        release_breakpoint()

        wait(lambda: get(scheduler_orchid_operation_path(op.id, tree="gpu") + "/resource_usage/gpu", default=None) == 0)

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
