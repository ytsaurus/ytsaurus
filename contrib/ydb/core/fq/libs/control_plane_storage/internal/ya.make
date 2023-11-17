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
    library/cpp/actors/core
    library/cpp/lwtrace/mon
    library/cpp/monlib/service/pages
    contrib/ydb/core/base
    contrib/ydb/core/metering
    contrib/ydb/core/fq/libs/common
    contrib/ydb/core/fq/libs/config
    contrib/ydb/core/fq/libs/control_plane_storage/proto
    contrib/ydb/core/fq/libs/exceptions
    contrib/ydb/core/fq/libs/quota_manager
    contrib/ydb/core/fq/libs/quota_manager/events
    contrib/ydb/core/fq/libs/rate_limiter/events
    contrib/ydb/core/fq/libs/ydb
    contrib/ydb/core/mon
    contrib/ydb/library/protobuf_printer
    contrib/ydb/library/security
    contrib/ydb/library/yql/public/issue
    contrib/ydb/public/lib/fq
    contrib/ydb/public/sdk/cpp/client/ydb_scheme
    contrib/ydb/public/sdk/cpp/client/ydb_value
)

YQL_LAST_ABI_VERSION()

END()
