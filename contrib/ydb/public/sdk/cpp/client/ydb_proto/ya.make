LIBRARY()

SRCS(
    accessor.cpp
)

PEERDIR(
    contrib/ydb/public/api/grpc
    contrib/ydb/public/api/grpc/draft
    contrib/ydb/public/api/protos
    contrib/ydb/public/lib/operation_id/protos
    contrib/ydb/public/sdk/cpp/client/ydb_params
    contrib/ydb/public/sdk/cpp/client/ydb_value
    contrib/ydb/library/yql/public/issue/protos
)

END()
