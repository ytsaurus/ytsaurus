LIBRARY()

SRCS(
    client_session.cpp
    data_query.cpp
    readers.cpp
    request_migrator.cpp
    table_client.cpp
)

PEERDIR(
    library/cpp/threading/future
    contrib/ydb/public/api/protos
    contrib/ydb/public/lib/operation_id/protos
    contrib/ydb/public/sdk/cpp/client/impl/ydb_endpoints
    contrib/ydb/public/sdk/cpp/client/impl/ydb_internal/session_pool
    contrib/ydb/public/sdk/cpp/client/ydb_table/query_stats
    contrib/ydb/library/yql/public/issue/protos
)

END()
