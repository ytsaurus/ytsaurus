LIBRARY()

SRCS(
    actions.cpp
    grpc_connections.cpp
)

PEERDIR(
    contrib/ydb/public/api/grpc
    contrib/ydb/public/api/protos
    contrib/ydb/public/sdk/cpp/src/client/impl/ydb_internal/db_driver_state
    contrib/ydb/public/sdk/cpp/src/client/impl/ydb_internal/plain_status
    contrib/ydb/public/sdk/cpp/src/client/impl/ydb_internal/thread_pool
    contrib/ydb/public/sdk/cpp/src/client/impl/ydb_stats
    contrib/ydb/public/sdk/cpp/src/client/resources
    contrib/ydb/public/sdk/cpp/src/client/types/exceptions
)

END()
