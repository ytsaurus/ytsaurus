from yt_scheduler_helpers import (
    scheduler_new_orchid_pool_tree_path,
)

from yt_commands import (
    get, wait
)


def get_operation_from_gpu_policy_orchid(operation, tree="gpu"):
    return get(scheduler_new_orchid_pool_tree_path(tree) + f"/gpu_assignment_plan/operations/{operation.id}")


def get_node_from_gpu_policy_orchid(node, tree="gpu"):
    return get(scheduler_new_orchid_pool_tree_path(tree) + f"/gpu_assignment_plan/nodes/{node}")


def get_operation_gpu_assignments_from_gpu_policy_orchid(operation, tree="gpu"):
    return get_operation_from_gpu_policy_orchid(operation, tree=tree)["assignments"]


def check_assignment_from_gpu_policy_orchid(assignment, operation_id, group_name, gpu_usage, preemptible, allocation_id=None):
    assert assignment["operation_id"] == operation_id
    assert assignment["allocation_group_name"] == group_name
    assert assignment["resource_usage"]["gpu"] == gpu_usage
    assert assignment["preemptible"] == preemptible
    if (allocation_id):
        assert assignment["allocation_id"] == allocation_id


def wait_for_operations_in_gpu_policy_orchid(operation_count, tree="gpu"):
    wait(lambda: len(get(scheduler_new_orchid_pool_tree_path(tree) + "/gpu_assignment_plan/operations")) == operation_count)


def wait_for_assignments_in_gpu_policy_orchid(operation, assignment_count, tree="gpu", exactly=False):
    if exactly:
        wait(lambda: len(get(scheduler_new_orchid_pool_tree_path(tree) + f"/gpu_assignment_plan/operations/{operation.id}/assignments")) == assignment_count)
    else:
        wait(lambda: len(get(scheduler_new_orchid_pool_tree_path(tree) + f"/gpu_assignment_plan/operations/{operation.id}/assignments")) >= assignment_count)


def check_operation_from_gpu_policy_orchid(operation, is_gang, group_name, allocation_count, min_needed_gpu_per_allocation, assigned_gpu_usage, assignment_count, enabled=None, scheduling_module=None):
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
