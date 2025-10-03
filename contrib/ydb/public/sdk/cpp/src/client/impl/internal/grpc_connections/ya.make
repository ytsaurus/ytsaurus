LIBRARY()

SRCS(
    actions.cpp
    grpc_connections.cpp
)

PEERDIR(
    contrib/ydb/public/api/grpc
    contrib/ydb/public/api/protos
    contrib/ydb/public/sdk/cpp/src/client/impl/internal/db_driver_state
    contrib/ydb/public/sdk/cpp/src/client/impl/internal/plain_status
    contrib/ydb/public/sdk/cpp/src/client/impl/stats
    contrib/ydb/public/sdk/cpp/src/client/resources
    contrib/ydb/public/sdk/cpp/src/client/types/exceptions
    contrib/ydb/public/sdk/cpp/src/client/types/executor
)

END()
