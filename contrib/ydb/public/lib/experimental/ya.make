LIBRARY()

ADDINCL(
    contrib/ydb/public/sdk/cpp
)

SRCS(
    ydb_clickhouse_internal.cpp
    ydb_logstore.cpp
    ydb_object_storage.cpp
)

PEERDIR(
    contrib/ydb/core/scheme
    contrib/ydb/public/api/grpc/draft
    contrib/ydb/public/sdk/cpp/src/client/proto
    contrib/ydb/public/sdk/cpp/src/client/table
)

END()
