LIBRARY()

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/public/sdk/cpp/sdk_common.inc)

SRCS(
    retry.cpp
)

PEERDIR(
    contrib/ydb/public/sdk/cpp/src/client/impl/ydb_internal/grpc_connections
)

END()

