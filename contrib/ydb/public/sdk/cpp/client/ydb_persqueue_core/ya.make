LIBRARY()

GENERATE_ENUM_SERIALIZATION(contrib/ydb/public/sdk/cpp/client/ydb_persqueue_core/persqueue.h)

SRCS(
    persqueue.h
)

PEERDIR(
    library/cpp/retry
    contrib/ydb/public/sdk/cpp/client/ydb_persqueue_core/impl
    contrib/ydb/public/sdk/cpp/client/ydb_proto
    contrib/ydb/public/sdk/cpp/client/ydb_driver
    contrib/ydb/public/api/grpc
    contrib/ydb/public/api/grpc/draft
    contrib/ydb/public/api/protos
)

END()

RECURSE_FOR_TESTS(
    ut
)
