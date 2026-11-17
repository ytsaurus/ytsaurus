YQL_LIBRARY()

PEERDIR(
    contrib/ydb/library/yql/providers/dq/api/protos
    contrib/ydb/library/yql/providers/dq/worker_manager/interface
    yql/essentials/providers/common/metrics
    yql/essentials/utils/log
    yt/yql/providers/dq/common/attrs
)

SRCS(
    allocate_workers_response.cpp
    worker_info.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)
