LIBRARY()

SRCS(
    dq_warmup_common.cpp
    global_worker_manager.cpp
    service_node_pinger.cpp
    worker_filter.cpp
    workers_storage.cpp
)

PEERDIR(
    contrib/ydb/library/yql/providers/dq/actors
    contrib/ydb/library/yql/providers/dq/api/grpc
    contrib/ydb/library/yql/providers/dq/api/protos
    contrib/ydb/library/yql/providers/dq/common
    contrib/ydb/library/yql/providers/dq/counters
    contrib/ydb/library/yql/providers/dq/planner
    contrib/ydb/library/yql/providers/dq/provider
    contrib/ydb/library/yql/providers/dq/runtime
    contrib/ydb/library/yql/providers/dq/task_runner
    library/cpp/svnversion
    library/cpp/threading/future
    yql/essentials/providers/common/config
    yql/essentials/providers/common/gateway
    yql/essentials/providers/common/metrics
    yql/essentials/utils/failure_injector
    yt/yql/providers/dq/actors
    yt/yql/providers/dq/actors/yt
    yt/yql/providers/dq/common
    yt/yql/providers/dq/config
    yt/yql/providers/dq/gateway
    yt/yql/providers/dq/scheduler
    yt/yql/providers/dq/service
)

IF (NOT OS_WINDOWS)
    SRCS(
        coordination_helper.cpp
        dq_gateway_with_uploader.cpp
        service_node_resolver.cpp
    )
ELSE()
    SRCS(
        coordination_helper_win.cpp
        dq_gateway_with_uploader_win.cpp
    )
ENDIF()

YQL_LAST_ABI_VERSION()

END()

IF (NOT OPENSOURCE OR OPENSOURCE_PROJECT == "ydb")
    RECURSE_FOR_TESTS(
        ut
    )
    RECURSE(
        benchmark
    )
ENDIF()
