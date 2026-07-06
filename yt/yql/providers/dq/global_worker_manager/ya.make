LIBRARY()

PEERDIR(
    contrib/ydb/library/yql/providers/dq/actors
    contrib/ydb/library/yql/providers/dq/api/grpc
    contrib/ydb/library/yql/providers/dq/api/protos
    contrib/ydb/library/yql/providers/dq/counters
    contrib/ydb/library/yql/providers/dq/runtime
    contrib/ydb/library/yql/providers/dq/task_runner
    yql/essentials/providers/common/config
    yql/essentials/providers/common/gateway
    yql/essentials/providers/common/metrics
    yql/essentials/utils/failure_injector
    yt/yql/providers/dq/actors
    yt/yql/providers/dq/actors/yt
    yt/yql/providers/dq/config
    yt/yql/providers/dq/scheduler
    yt/yql/providers/dq/service
)

YQL_LAST_ABI_VERSION()

SET(
    SOURCE
    benchmark.cpp
    global_worker_manager.cpp
    service_node_pinger.cpp
    workers_storage.cpp
    worker_filter.cpp
)

IF (NOT OS_WINDOWS)
    SET(
        SOURCE
        ${SOURCE}
        service_node_resolver.cpp
        coordination_helper.cpp
    )
ELSE()
    SET(
        SOURCE
        ${SOURCE}
        coordination_helper_win.cpp
    )
ENDIF()

SRCS(
    ${SOURCE}
)

END()

IF (NOT OPENSOURCE OR OPENSOURCE_PROJECT == "ydb")
    RECURSE_FOR_TESTS(
        ut
    )
ENDIF()
