LIBRARY()

GENERATE_ENUM_SERIALIZATION(contrib/ydb/public/sdk/cpp/client/ydb_topic/topic.h)

SRCS(
    topic.h
    proto_accessor.cpp
)

PEERDIR(
    contrib/ydb/public/sdk/cpp/client/ydb_topic/codecs

    library/cpp/retry
    contrib/ydb/public/sdk/cpp/client/ydb_topic/impl
    contrib/ydb/public/sdk/cpp/client/ydb_proto
    contrib/ydb/public/sdk/cpp/client/ydb_driver
    contrib/ydb/public/api/grpc
    contrib/ydb/public/api/grpc/draft
    contrib/ydb/public/api/protos
    contrib/ydb/public/sdk/cpp/client/ydb_table
)

END()

RECURSE_FOR_TESTS(
    ut
)
