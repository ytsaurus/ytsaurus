LIBRARY()

SRCS(
    driver.cpp
)

PEERDIR(
    contrib/ydb/public/sdk/cpp/src/client/impl/ydb_internal/common
    contrib/ydb/public/sdk/cpp/src/client/impl/ydb_internal/grpc_connections
    contrib/ydb/public/sdk/cpp/src/client/resources
    contrib/ydb/public/sdk/cpp/src/client/common_client
    contrib/ydb/public/sdk/cpp/src/client/types/status
)

END()
