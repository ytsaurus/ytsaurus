LIBRARY()

SRCS(
    nodes_health_check.cpp
    rate_limiter_resources.cpp
    response_tasks.cpp
    task_get.cpp
    task_ping.cpp
    task_result_write.cpp
    utils.cpp
)

PEERDIR(
    contrib/libs/fmt
    library/cpp/lwtrace/mon
    library/cpp/monlib/service/pages
    contrib/ydb/core/base
    contrib/ydb/core/fq/libs/common
    contrib/ydb/core/fq/libs/compute/common
    contrib/ydb/core/fq/libs/config
    contrib/ydb/core/fq/libs/control_plane_storage/proto
    contrib/ydb/core/fq/libs/metrics
    contrib/ydb/core/fq/libs/quota_manager
    contrib/ydb/core/fq/libs/quota_manager/events
    contrib/ydb/core/fq/libs/rate_limiter/events
    contrib/ydb/core/fq/libs/ydb
    contrib/ydb/core/kqp/opt
    contrib/ydb/core/kqp/proxy_service/script_executions_utils
    contrib/ydb/core/metering
    contrib/ydb/core/mon
    contrib/ydb/library/actors/core
    contrib/ydb/library/protobuf_printer
    contrib/ydb/library/security
    contrib/ydb/public/lib/fq
    contrib/ydb/public/sdk/cpp/src/client/scheme
    contrib/ydb/public/sdk/cpp/src/client/value
    yql/essentials/public/issue
    yql/essentials/utils
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
