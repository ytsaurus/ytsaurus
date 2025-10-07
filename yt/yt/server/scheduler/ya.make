LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    allocation_tracker_service.cpp
    bootstrap.cpp
    controller_agent.cpp
    controller_agent_tracker.cpp
    controller_agent_tracker_service.cpp
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
)

PEERDIR(
    yt/yt/server/scheduler/common
    yt/yt/server/scheduler/strategy

    library/cpp/getopt
    library/cpp/yt/phdr_cache

    yt/yt/server/lib
    yt/yt/server/lib/scheduler

    # TODO(max42): eliminate.
    yt/yt/server/lib/transaction_server
    yt/yt/server/lib/controller_agent

    yt/yt/library/monitoring
    yt/yt/library/numeric
    yt/yt/library/orchid
    yt/yt/library/server_program
)

END()

RECURSE(
    common
    strategy
)

RECURSE_FOR_TESTS(
    unittests
)
