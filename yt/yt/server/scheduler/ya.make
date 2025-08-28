LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    allocation.cpp
    allocation_tracker_service.cpp
    bootstrap.cpp
    controller_agent.cpp
    controller_agent_tracker.cpp
    controller_agent_tracker_service.cpp
    exec_node.cpp
    helpers.cpp
    master_connector.cpp
    node_manager.cpp
    node_shard.cpp
    operation.cpp
    operation_alert_event.cpp
    operation_controller.cpp
    operation_controller_impl.cpp
    operations_cleaner.cpp
    program.cpp
    scheduler.cpp
    scheduler_service.cpp

    strategy/field_filter.cpp
    strategy/helpers.cpp
    strategy/job_resources_helpers.cpp
    strategy/operation.cpp
    strategy/operation_controller.cpp
    strategy/operation_state.cpp
    strategy/persistent_state.cpp
    strategy/pool_tree.cpp
    strategy/pool_tree_element.cpp
    strategy/pool_tree_profile_manager.cpp
    strategy/pool_tree_snapshot.cpp
    strategy/pools_config_parser.cpp
    strategy/resource_tree.cpp
    strategy/resource_tree_element.cpp
    strategy/scheduling_heartbeat_context.cpp
    strategy/scheduling_heartbeat_context_detail.cpp
    strategy/strategy.cpp

    strategy/policy/gpu_allocation_assignment_plan_update.cpp
    strategy/policy/gpu_allocation_scheduler_structs.cpp
    strategy/policy/operation_shared_state.cpp
    strategy/policy/packing.cpp
    strategy/policy/packing_detail.cpp
    strategy/policy/persistent_state.cpp
    strategy/policy/pool_tree_snapshot_state.cpp
    strategy/policy/scheduling_policy.cpp
    strategy/policy/scheduling_segment_manager.cpp
    strategy/policy/structs.cpp
)

PEERDIR(
    library/cpp/getopt
    library/cpp/yt/phdr_cache

    yt/yt/server/lib
    yt/yt/server/lib/scheduler

    # TODO(max42): eliminate.
    yt/yt/server/lib/transaction_server
    yt/yt/server/lib/controller_agent

    yt/yt/library/monitoring
    yt/yt/library/numeric
    yt/yt/library/server_program
)

END()

RECURSE_FOR_TESTS(
    unittests
)
