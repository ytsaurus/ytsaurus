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
    fair_share_strategy.cpp
    fair_share_strategy_operation_controller.cpp
    fair_share_tree.cpp
    fair_share_tree_allocation_scheduler.cpp
    fair_share_tree_allocation_scheduler_operation_shared_state.cpp
    fair_share_tree_allocation_scheduler_structs.cpp
    fair_share_tree_element.cpp
    fair_share_tree_profiling.cpp
    fair_share_tree_scheduling_snapshot.cpp
    fair_share_tree_snapshot.cpp
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
    packing.cpp
    packing_detail.cpp
    persistent_fair_share_tree_allocation_scheduler_state.cpp
    persistent_scheduler_state.cpp
    pools_config_parser.cpp
    program.cpp
    resource_tree.cpp
    resource_tree_element.cpp
    scheduler.cpp
    scheduler_service.cpp
    scheduling_context.cpp
    scheduling_context_detail.cpp
    scheduling_segment_manager.cpp
    serialize.cpp
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
