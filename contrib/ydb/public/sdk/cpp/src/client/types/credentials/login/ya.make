LIBRARY()
INCLUDE(${ARCADIA_ROOT}/contrib/ydb/public/sdk/cpp/sdk_common_arcadia.inc)

SRCS(
    login.cpp
)

PEERDIR(
    contrib/libs/jwt-cpp
    contrib/ydb/public/api/grpc
    contrib/ydb/public/sdk/cpp/src/client/types/status
    contrib/ydb/public/sdk/cpp/src/client/impl/internal/grpc_connections
    contrib/ydb/public/sdk/cpp/src/client/impl/internal/common
    contrib/ydb/public/sdk/cpp/src/library/issue
)

END()
