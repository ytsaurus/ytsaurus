LIBRARY()

SRCS(
    base_service.h
    base.h
)

PEERDIR(
    contrib/ydb/library/grpc/server
    library/cpp/string_utils/quote
    contrib/ydb/core/base
    contrib/ydb/core/grpc_services/counters
    contrib/ydb/core/grpc_streaming
    contrib/ydb/core/jaeger_tracing
    contrib/ydb/public/api/protos
    contrib/ydb/public/sdk/cpp/src/client/resources
    yql/essentials/public/issue
)

YQL_LAST_ABI_VERSION()

END()
