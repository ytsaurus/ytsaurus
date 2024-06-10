LIBRARY()

SRCS(
    out.cpp
    proto_accessor.cpp
    topic.h
)

PEERDIR(
    contrib/ydb/public/sdk/cpp/client/ydb_topic/common
    contrib/ydb/public/sdk/cpp/client/ydb_topic/impl
    contrib/ydb/public/sdk/cpp/client/ydb_topic/include

    contrib/ydb/public/sdk/cpp/client/ydb_proto
    contrib/ydb/public/sdk/cpp/client/ydb_driver
    contrib/ydb/public/sdk/cpp/client/ydb_table
    contrib/ydb/public/api/grpc
    contrib/ydb/public/api/grpc/draft
    contrib/ydb/public/api/protos

    library/cpp/retry
)

END()

RECURSE_FOR_TESTS(
    ut
)
