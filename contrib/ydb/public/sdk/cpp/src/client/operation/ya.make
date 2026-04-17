LIBRARY()

SRCS(
    operation.cpp
)

PEERDIR(
    contrib/ydb/public/api/grpc
    contrib/ydb/public/sdk/cpp/src/library/operation_id
    contrib/ydb/public/sdk/cpp/src/client/query
    contrib/ydb/public/sdk/cpp/src/client/common_client/impl
    contrib/ydb/public/sdk/cpp/src/client/driver
    contrib/ydb/public/sdk/cpp/src/client/export
    contrib/ydb/public/sdk/cpp/src/client/import
    contrib/ydb/public/sdk/cpp/src/client/ss_tasks
    contrib/ydb/public/sdk/cpp/src/client/types/operation
)

END()
