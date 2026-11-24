LIBRARY()

INCLUDE(${ARCADIA_ROOT}/contrib/libs/ydb-cpp-sdk/sdk_common.inc)

SRCS(
    driver.cpp
)

PEERDIR(
    contrib/libs/ydb-cpp-sdk/src/client/impl/internal/common
    contrib/libs/ydb-cpp-sdk/src/client/impl/internal/grpc_connections
    contrib/libs/ydb-cpp-sdk/src/client/resources
    contrib/libs/ydb-cpp-sdk/src/client/common_client
    contrib/libs/ydb-cpp-sdk/src/client/types/status
)

END()
