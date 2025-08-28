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
    gpu_allocation_assignment_plan_update.cpp
    gpu_allocation_scheduler_structs.cpp
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
    packing.cpp
    packing_detail.cpp
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
    scheduling_policy.cpp
    scheduling_policy_operation_shared_state.cpp
    scheduling_policy_persistent_state.cpp
    scheduling_policy_pool_tree_snapshot_state.cpp
    scheduling_policy_structs.cpp
    scheduling_segment_manager.cpp
    serialize.cpp
    strategy.cpp
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
