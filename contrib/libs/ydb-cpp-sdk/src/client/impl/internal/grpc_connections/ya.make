LIBRARY()

INCLUDE(${ARCADIA_ROOT}/contrib/libs/ydb-cpp-sdk/sdk_common.inc)

SRCS(
    actions.cpp
    grpc_connections.cpp
)

PEERDIR(
    contrib/ydb/public/api/grpc
    contrib/ydb/public/api/protos
    contrib/libs/ydb-cpp-sdk/src/client/impl/internal/db_driver_state
    contrib/libs/ydb-cpp-sdk/src/client/impl/internal/plain_status
    contrib/libs/ydb-cpp-sdk/src/client/impl/internal/rpc_request_settings
    contrib/libs/ydb-cpp-sdk/src/client/impl/internal/sdk_runtime
    contrib/libs/ydb-cpp-sdk/src/client/impl/stats
    contrib/libs/ydb-cpp-sdk/src/client/resources
    contrib/libs/ydb-cpp-sdk/src/client/types/exceptions
    contrib/libs/ydb-cpp-sdk/src/client/types/executor
)

END()
