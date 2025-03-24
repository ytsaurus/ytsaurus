LIBRARY()

SRCS(
    client.cpp
    query.cpp
    stats.cpp
    tx.cpp
)

PEERDIR(
    contrib/ydb/public/sdk/cpp/src/client/impl/ydb_internal/make_request
    contrib/ydb/public/sdk/cpp/src/client/impl/ydb_internal/kqp_session_common
    contrib/ydb/public/sdk/cpp/src/client/impl/ydb_internal/session_pool
    contrib/ydb/public/sdk/cpp/src/client/impl/ydb_internal/retry
    contrib/ydb/public/sdk/cpp/src/client/common_client
    contrib/ydb/public/sdk/cpp/src/client/driver
    contrib/ydb/public/sdk/cpp/src/client/query/impl
    contrib/ydb/public/sdk/cpp/src/client/result
    contrib/ydb/public/sdk/cpp/src/client/types/operation
)

END()
