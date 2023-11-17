LIBRARY()

SRCS(
    ydb_clickhouse_internal.cpp
    ydb_logstore.cpp
)

PEERDIR(
    contrib/ydb/core/scheme
    contrib/ydb/public/api/grpc/draft
    contrib/ydb/public/sdk/cpp/client/ydb_proto
    contrib/ydb/public/sdk/cpp/client/ydb_table
)

END()
