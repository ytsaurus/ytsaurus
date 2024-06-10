LIBRARY()

SRCS(
    client.cpp
    client.h
    query.cpp
    query.h
    stats.cpp
    stats.h
    tx.cpp
    tx.h
)

PEERDIR(
    contrib/ydb/public/sdk/cpp/client/impl/ydb_internal/make_request
    contrib/ydb/public/sdk/cpp/client/impl/ydb_internal/kqp_session_common
    contrib/ydb/public/sdk/cpp/client/impl/ydb_internal/session_pool
    contrib/ydb/public/sdk/cpp/client/impl/ydb_internal/retry
    contrib/ydb/public/sdk/cpp/client/ydb_common_client
    contrib/ydb/public/sdk/cpp/client/ydb_driver
    contrib/ydb/public/sdk/cpp/client/ydb_query/impl
    contrib/ydb/public/sdk/cpp/client/ydb_result
    contrib/ydb/public/sdk/cpp/client/ydb_types/operation
)

END()
