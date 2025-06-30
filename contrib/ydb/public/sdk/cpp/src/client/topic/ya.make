LIBRARY()

SRCS(
    out.cpp
)

PEERDIR(
    contrib/ydb/public/sdk/cpp/src/client/topic/codecs
    contrib/ydb/public/sdk/cpp/src/client/topic/common
    contrib/ydb/public/sdk/cpp/src/client/topic/impl
    contrib/ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic

    contrib/ydb/public/sdk/cpp/src/client/proto
    contrib/ydb/public/sdk/cpp/src/client/driver
    contrib/ydb/public/sdk/cpp/src/client/table

    contrib/ydb/public/api/grpc
    contrib/ydb/public/api/grpc/draft
    contrib/ydb/public/api/protos
)

END()

RECURSE_FOR_TESTS(
    ut
)
