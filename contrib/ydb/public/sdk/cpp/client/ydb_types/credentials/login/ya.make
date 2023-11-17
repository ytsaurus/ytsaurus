LIBRARY()

SRCS(
    login.cpp
)

PEERDIR(
    contrib/ydb/library/login
    contrib/ydb/public/api/grpc
    contrib/ydb/public/sdk/cpp/client/ydb_types/status
    contrib/ydb/public/sdk/cpp/client/impl/ydb_internal/grpc_connections
    contrib/ydb/library/yql/public/issue
)

END()
