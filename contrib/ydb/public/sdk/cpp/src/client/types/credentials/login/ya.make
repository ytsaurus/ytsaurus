LIBRARY()

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
