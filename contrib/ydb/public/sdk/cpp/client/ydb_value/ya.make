LIBRARY()

SRCS(
    value.cpp
)

GENERATE_ENUM_SERIALIZATION(value.h)

PEERDIR(
    library/cpp/containers/stack_vector
    contrib/ydb/public/api/protos
    contrib/ydb/public/sdk/cpp/client/impl/ydb_internal/value_helpers
    contrib/ydb/public/sdk/cpp/client/ydb_types/fatal_error_handlers
    contrib/ydb/library/yql/public/decimal
    contrib/ydb/library/uuid
)

END()

RECURSE_FOR_TESTS(
    ut
)
