LIBRARY()

SRCS(
    bulk_upsert_retry_state.cpp
    retry.cpp
)

PEERDIR(
    contrib/ydb/public/api/protos
    contrib/ydb/public/sdk/cpp/src/client/impl/internal/grpc_connections
    contrib/ydb/public/sdk/cpp/src/client/impl/observability
    contrib/ydb/public/sdk/cpp/src/client/value
)

END()
