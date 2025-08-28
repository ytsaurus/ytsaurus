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
    strategy_operation_controller.cpp
    fields_filter.cpp
    helpers.cpp
    job_resources_helpers.cpp
    master_connector.cpp
    node_manager.cpp
    node_shard.cpp
    operation.cpp
    operation_alert_event.cpp
    operation_controller.cpp
    operation_controller_impl.cpp
    operations_cleaner.cpp
    persistent_state.cpp
    pool_tree.cpp
    pool_tree_element.cpp
    pool_tree_profile_manager.cpp
    pool_tree_snapshot.cpp
    pools_config_parser.cpp
    program.cpp
    resource_tree.cpp
    resource_tree_element.cpp
    scheduler.cpp
    scheduler_service.cpp
    scheduling_heartbeat_context.cpp
    scheduling_heartbeat_context_detail.cpp
    serialize.cpp
    strategy.cpp

    policy/gpu_allocation_assignment_plan_update.cpp
    policy/gpu_allocation_scheduler_structs.cpp
    policy/operation_shared_state.cpp
    policy/packing.cpp
    policy/packing_detail.cpp
    policy/persistent_state.cpp
    policy/pool_tree_snapshot_state.cpp
    policy/scheduling_policy.cpp
    policy/scheduling_segment_manager.cpp
    policy/structs.cpp
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
