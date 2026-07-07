from yt_scheduler_helpers import (
    scheduler_new_orchid_pool_tree_path,
)

from yt_commands import (
    get, wait, exists
)

from yt_helpers import read_structured_log

import builtins


GPU_STRUCTURED_LOG_CATEGORY = "SchedulerGpuStructuredLog"

NULL_GUID = "0-0-0-0"


def is_default_guid(guid):
    """Whether a guid read from the orchid/structured log is the default (null) guid.

    A strong-typedef guid with a null state (e.g. an unrealized assignment's
    allocation_id) serializes as the all-zero "0-0-0-0" string rather than an entity,
    so an absent value (None) and "0-0-0-0" both mean "no id".
    """
    return guid is None or guid == NULL_GUID


def gpu_scheduler_orchid_operation_path(operation_id, tree="gpu"):
    return scheduler_new_orchid_pool_tree_path(tree) + f"/gpu_assignment_plan/operations/{operation_id}"


def get_operation_from_gpu_policy_orchid(operation, tree="gpu"):
    return get(gpu_scheduler_orchid_operation_path(operation.id, tree=tree))


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
    wait(lambda: exists(gpu_scheduler_orchid_operation_path(operation.id, tree=tree) + "/assignments"))
    if exactly:
        wait(lambda: len(get(gpu_scheduler_orchid_operation_path(operation.id, tree=tree) + "/assignments")) == assignment_count)
    else:
        wait(lambda: len(get(gpu_scheduler_orchid_operation_path(operation.id, tree=tree) + "/assignments")) >= assignment_count)


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


def get_operation_gpu_allocations_from_gpu_policy_orchid(operation, tree="gpu"):
    return get_operation_from_gpu_policy_orchid(operation, tree=tree)["allocations"]


def check_gpu_allocations_from_gpu_policy_orchid(allocations, expected_allocation_ids, expected_gpu_usage):
    assert builtins.set(allocations.keys()) == builtins.set(expected_allocation_ids)
    for allocation_id in expected_allocation_ids:
        assert allocations[allocation_id]["allocation_id"] == allocation_id
        assert allocations[allocation_id]["resource_usage"]["gpu"] == expected_gpu_usage


def wait_for_gpu_allocations_empty_in_gpu_policy_orchid(operation, tree="gpu"):
    path = gpu_scheduler_orchid_operation_path(operation.id, tree=tree) + "/allocations"
    wait(lambda: get(path, default=None) in (None, {}))


def read_gpu_events(scheduler_log_file, from_barrier, to_barrier=None, event_type=None, op=None, allocation_id=None, predicate=None):
    """Read GPU structured-log events between two barriers and filter them.

    Args:
        scheduler_log_file: path to scheduler-0.json.log (caller resolves it).
        from_barrier: start barrier id obtained from write_log_barrier.
        to_barrier: optional end barrier id. If None, reads to EOF.
        event_type: optional event_type filter (e.g. "allocation_preempted").
        op: optional operation (object with .id) or operation id string;
            matches event["operation_id"].
        allocation_id: optional allocation_id to match.
        predicate: optional callable invoked on the event dict; included only
            if it returns truthy.

    Returns:
        List of matching event dicts, in log order.
    """
    op_id = op.id if op is not None and hasattr(op, "id") else op

    def row_filter(event):
        if event.get("category") != GPU_STRUCTURED_LOG_CATEGORY:
            return False
        if event_type is not None and event.get("event_type") != event_type:
            return False
        if op_id is not None and event.get("operation_id") != op_id:
            return False
        if allocation_id is not None and event.get("allocation_id") != allocation_id:
            return False
        if predicate is not None and not predicate(event):
            return False
        return True

    return read_structured_log(
        scheduler_log_file,
        from_barrier=from_barrier,
        to_barrier=to_barrier,
        row_filter=row_filter,
    )


def wait_for_gpu_event(scheduler_log_file, from_barrier, event_type, **kwargs):
    """Wait until a matching GPU structured event appears. Returns the first match."""
    holder = {}

    def check():
        events = read_gpu_events(scheduler_log_file, from_barrier, event_type=event_type, **kwargs)
        if events:
            holder["event"] = events[0]
            return True
        return False

    wait(check)
    return holder["event"]


def wait_for_allocation_preempted(scheduler_log_file, from_barrier, op, reason, count=1):
    """Wait until at least |count| AllocationPreempted events appear for |op| with |reason|.

    Returns the list of matching events.
    """
    holder = {}

    def predicate(event):
        return event.get("reason") == reason

    def check():
        events = read_gpu_events(
            scheduler_log_file,
            from_barrier,
            event_type="allocation_preempted",
            op=op,
            predicate=predicate,
        )
        if len(events) >= count:
            holder["events"] = events
            return True
        return False

    wait(check)
    return holder["events"]


def wait_for_assignment_preempted_preliminary(scheduler_log_file, from_barrier, op, reason):
    """Wait for an AssignmentPreempted event for |op| with |reason| where the
    underlying assignment was preliminary (no allocation_id on the assignment).

    Returns the matching event.
    """
    holder = {}

    def predicate(event):
        if event.get("reason") != reason:
            return False
        assignment = event.get("assignment", {}) or {}
        if assignment.get("operation_id") != (op.id if hasattr(op, "id") else op):
            return False
        return is_default_guid(assignment.get("allocation_id"))

    def check():
        events = read_gpu_events(
            scheduler_log_file,
            from_barrier,
            event_type="assignment_preempted",
            predicate=predicate,
        )
        if events:
            holder["event"] = events[0]
            return True
        return False

    wait(check)
    return holder["event"]
