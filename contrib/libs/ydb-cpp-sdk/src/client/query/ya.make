LIBRARY()

INCLUDE(${ARCADIA_ROOT}/contrib/libs/ydb-cpp-sdk/sdk_common.inc)

SRCS(
    client.cpp
    query.cpp
    stats.cpp
    tx.cpp
)

PEERDIR(
    contrib/libs/ydb-cpp-sdk/src/client/metrics
    contrib/libs/ydb-cpp-sdk/src/client/trace
    contrib/libs/ydb-cpp-sdk/src/client/impl/observability
    contrib/libs/ydb-cpp-sdk/src/client/impl/internal/make_request
    contrib/libs/ydb-cpp-sdk/src/client/impl/internal/retry
    contrib/libs/ydb-cpp-sdk/src/client/impl/session
    contrib/libs/ydb-cpp-sdk/src/client/common_client
    contrib/libs/ydb-cpp-sdk/src/client/driver
    contrib/libs/ydb-cpp-sdk/src/client/query/impl
    contrib/libs/ydb-cpp-sdk/src/client/result
    contrib/libs/ydb-cpp-sdk/src/client/types
    contrib/libs/ydb-cpp-sdk/src/client/types/operation
)

END()
