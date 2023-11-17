LIBRARY()

SRCS(
    coordination.cpp
    proto_accessor.cpp
)

GENERATE_ENUM_SERIALIZATION(coordination.h)

PEERDIR(
    contrib/ydb/public/api/grpc
    contrib/ydb/public/sdk/cpp/client/impl/ydb_internal/make_request
    contrib/ydb/public/sdk/cpp/client/ydb_common_client
    contrib/ydb/public/sdk/cpp/client/ydb_common_client/impl
    contrib/ydb/public/sdk/cpp/client/ydb_driver
    contrib/ydb/public/sdk/cpp/client/ydb_proto
    contrib/ydb/public/sdk/cpp/client/ydb_types
    contrib/ydb/public/sdk/cpp/client/ydb_types/status
)

END()

RECURSE_FOR_TESTS(
    ut
)
