LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    config.cpp
    controller_service.cpp
    controller.cpp
    flow_executor.cpp
    job_balancer.cpp
    job_balancer_common.cpp
    job_balancer_greedy.cpp
    job_balancer_resource_queue.cpp
    job_balancer_result.cpp
    job_manager.cpp
    lease_manager.cpp
    persisted_state_manager.cpp
    state_access.cpp
    state_manager.cpp
    throttler_host.cpp
    worker_tracker_service.cpp
    worker_tracker.cpp
    worker.cpp
    yt_connector.cpp
)

PEERDIR(
    library/cpp/containers/concurrent_hash_set
    library/cpp/getopt
    library/cpp/yson/node
    library/cpp/yt/memory
    yt/yt/core
    yt/yt/flow/library/cpp/pipeline_helpers/flow_execute
    yt/yt/flow/library/cpp/common
    yt/yt/flow/library/cpp/controller/describe
    yt/yt/flow/library/cpp/distributed_throttler
    yt/yt/flow/library/cpp/misc
    yt/yt/flow/library/cpp/tables
    yt/yt/flow/library/cpp/client
    yt/yt/library/cypress_election
    yt/yt/library/lock_election
    yt/yt/library/orchid
    yt/yt/library/profiling/solomon
    yt/yt/library/query/engine_api
)

END()

RECURSE_FOR_TESTS(unittests)
