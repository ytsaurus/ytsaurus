LIBRARY()

SRCS(
    out.cpp
    value.cpp
)

GENERATE_ENUM_SERIALIZATION(contrib/ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/value/value.h)

PEERDIR(
    library/cpp/containers/stack_vector
    contrib/ydb/public/api/protos
    contrib/ydb/public/sdk/cpp/src/client/impl/ydb_internal/value_helpers
    contrib/ydb/public/sdk/cpp/src/client/types/fatal_error_handlers
    contrib/ydb/public/sdk/cpp/src/library/decimal
    contrib/ydb/public/sdk/cpp/src/library/uuid
)

END()
