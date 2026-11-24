LIBRARY()

INCLUDE(${ARCADIA_ROOT}/contrib/libs/ydb-cpp-sdk/sdk_common.inc)

SRCS(
    out.cpp
    value.cpp
)

GENERATE_ENUM_SERIALIZATION(contrib/libs/ydb-cpp-sdk/include/ydb-cpp-sdk/client/value/value.h)

PEERDIR(
    library/cpp/containers/stack_vector
    contrib/ydb/public/api/protos
    contrib/libs/ydb-cpp-sdk/src/client/impl/internal/value_helpers
    contrib/libs/ydb-cpp-sdk/src/client/types/fatal_error_handlers
    contrib/libs/ydb-cpp-sdk/src/library/decimal
    contrib/libs/ydb-cpp-sdk/src/library/uuid
)

END()
