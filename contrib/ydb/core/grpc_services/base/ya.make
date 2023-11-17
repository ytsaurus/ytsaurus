LIBRARY()

SRCS(
    base_service.h
    base.h
)

PEERDIR(
    library/cpp/grpc/server
    library/cpp/string_utils/quote
    contrib/ydb/core/base
    contrib/ydb/core/grpc_services/counters
    contrib/ydb/core/grpc_streaming
    contrib/ydb/public/api/protos
    contrib/ydb/public/sdk/cpp/client/resources
    contrib/ydb/library/yql/public/issue
)

YQL_LAST_ABI_VERSION()

END()
