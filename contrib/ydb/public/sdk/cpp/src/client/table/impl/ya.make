LIBRARY()

SRCS(
    client_session.cpp
    data_query.cpp
    readers.cpp
    request_migrator.cpp
    table_client.cpp
    transaction.cpp
)

PEERDIR(
    library/cpp/threading/future
    contrib/ydb/public/api/protos
    contrib/ydb/public/sdk/cpp/src/client/impl/ydb_endpoints
    contrib/ydb/public/sdk/cpp/src/client/impl/ydb_internal/session_pool
    contrib/ydb/public/sdk/cpp/src/client/table/query_stats
    contrib/ydb/public/sdk/cpp/src/library/string_utils/helpers
)

END()
