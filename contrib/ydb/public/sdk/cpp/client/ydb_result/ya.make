LIBRARY()

SRCS(
    proto_accessor.cpp
    result.cpp
)

PEERDIR(
    contrib/ydb/public/api/protos
    contrib/ydb/public/sdk/cpp/client/ydb_types/fatal_error_handlers
    contrib/ydb/public/sdk/cpp/client/ydb_value
    contrib/ydb/public/sdk/cpp/client/ydb_proto
)

END()

RECURSE_FOR_TESTS(
    ut
)
