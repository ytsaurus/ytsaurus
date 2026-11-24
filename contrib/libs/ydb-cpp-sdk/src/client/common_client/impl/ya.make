LIBRARY()

INCLUDE(${ARCADIA_ROOT}/contrib/libs/ydb-cpp-sdk/sdk_common.inc)

SRCS(
    client.cpp
)

PEERDIR(
    contrib/libs/ydb-cpp-sdk/src/client/impl/internal/grpc_connections
    contrib/libs/ydb-cpp-sdk/src/library/time
)

END()
