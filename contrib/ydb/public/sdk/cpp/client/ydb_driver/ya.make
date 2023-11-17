LIBRARY()

SRCS(
    driver.cpp
)

PEERDIR(
    contrib/ydb/public/sdk/cpp/client/impl/ydb_internal/common
    contrib/ydb/public/sdk/cpp/client/impl/ydb_internal/grpc_connections
    contrib/ydb/public/sdk/cpp/client/resources
    contrib/ydb/public/sdk/cpp/client/ydb_common_client
    contrib/ydb/public/sdk/cpp/client/ydb_types/status
)

END()

RECURSE_FOR_TESTS(
    ut
)
