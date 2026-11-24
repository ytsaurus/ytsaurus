LIBRARY()

INCLUDE(${ARCADIA_ROOT}/contrib/libs/ydb-cpp-sdk/sdk_common.inc)

SRCS(
    retry.cpp
    bulk_upsert_retry_state.cpp
)

PEERDIR(
    contrib/libs/ydb-cpp-sdk/src/client/impl/observability
    contrib/libs/ydb-cpp-sdk/src/client/impl/internal/grpc_connections
)

END()
