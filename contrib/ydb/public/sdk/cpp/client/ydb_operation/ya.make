LIBRARY()

SRCS(
    operation.cpp
)

PEERDIR(
    contrib/ydb/public/api/grpc
    contrib/ydb/public/lib/operation_id
    contrib/ydb/public/sdk/cpp/client/ydb_query
    contrib/ydb/public/sdk/cpp/client/ydb_common_client/impl
    contrib/ydb/public/sdk/cpp/client/ydb_driver
    contrib/ydb/public/sdk/cpp/client/ydb_export
    contrib/ydb/public/sdk/cpp/client/ydb_import
    contrib/ydb/public/sdk/cpp/client/ydb_types/operation
)

END()
