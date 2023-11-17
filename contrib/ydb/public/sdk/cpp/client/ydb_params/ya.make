LIBRARY()

SRCS(
    params.cpp
    impl.cpp
)

PEERDIR(
    contrib/ydb/public/api/protos
    contrib/ydb/public/sdk/cpp/client/ydb_types/fatal_error_handlers
    contrib/ydb/public/sdk/cpp/client/ydb_value
)

END()

RECURSE_FOR_TESTS(
    ut
)
