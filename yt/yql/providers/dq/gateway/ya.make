LIBRARY()

SRCS(
    yql_dq_gateway.cpp
    yql_dq_gateway_factory.h
)

PEERDIR(
    contrib/ydb/library/yql/providers/dq/actors
    contrib/ydb/library/yql/providers/dq/api/grpc
    contrib/ydb/library/yql/providers/dq/api/protos
    contrib/ydb/library/yql/providers/dq/common
    contrib/ydb/library/yql/providers/dq/provider
    contrib/ydb/public/lib/yson_value
    contrib/ydb/public/sdk/cpp/src/library/grpc/client
    library/cpp/threading/future
    library/cpp/threading/task_scheduler
    library/cpp/yson
    library/cpp/yson/node
    yql/essentials/providers/common/provider
    yql/essentials/public/issue
    yql/essentials/utils/backtrace
    yql/essentials/utils/failure_injector
    yql/essentials/utils/log
    yt/yql/providers/dq/config
)

YQL_LAST_ABI_VERSION()

END()
