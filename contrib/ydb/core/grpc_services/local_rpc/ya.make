LIBRARY()

SRCS(
    local_rpc.h
)

PEERDIR(
    contrib/ydb/core/base
    contrib/ydb/core/grpc_services/base
    contrib/ydb/core/grpc_streaming
    contrib/ydb/library/actors/wilson
    contrib/ydb/library/wilson_ids
    contrib/ydb/library/yverify_stream
    contrib/ydb/public/sdk/cpp/src/client/types
    contrib/ydb/public/sdk/cpp/src/client/types/status
    contrib/ydb/public/sdk/cpp/src/library/issue
)

YQL_LAST_ABI_VERSION()

END()
