LIBRARY()

INCLUDE(${ARCADIA_ROOT}/contrib/libs/ydb-cpp-sdk/sdk_common.inc)

SRCS(
    login.cpp
)

PEERDIR(
    contrib/libs/jwt-cpp
    contrib/ydb/public/api/grpc
    contrib/libs/ydb-cpp-sdk/src/client/types/status
    contrib/libs/ydb-cpp-sdk/src/client/impl/internal/grpc_connections
    contrib/libs/ydb-cpp-sdk/src/client/impl/internal/common
    contrib/libs/ydb-cpp-sdk/src/library/issue
)

END()
