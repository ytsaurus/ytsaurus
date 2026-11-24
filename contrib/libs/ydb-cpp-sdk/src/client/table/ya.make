LIBRARY()

INCLUDE(${ARCADIA_ROOT}/contrib/libs/ydb-cpp-sdk/sdk_common.inc)

SRCS(
    out.cpp
    proto_accessor.cpp
    table.cpp
)

GENERATE_ENUM_SERIALIZATION(contrib/libs/ydb-cpp-sdk/include/ydb-cpp-sdk/client/table/table_enum.h)

PEERDIR(
    contrib/ydb/public/api/protos
    contrib/libs/ydb-cpp-sdk/src/client/impl/internal/make_request
    contrib/libs/ydb-cpp-sdk/src/client/impl/internal/retry
    contrib/libs/ydb-cpp-sdk/src/client/impl/session
    contrib/libs/ydb-cpp-sdk/src/client/driver
    contrib/libs/ydb-cpp-sdk/src/client/params
    contrib/libs/ydb-cpp-sdk/src/client/proto
    contrib/libs/ydb-cpp-sdk/src/client/result
    contrib/libs/ydb-cpp-sdk/src/client/scheme
    contrib/libs/ydb-cpp-sdk/src/client/table/impl
    contrib/libs/ydb-cpp-sdk/src/client/table/query_stats
    contrib/libs/ydb-cpp-sdk/src/client/types/operation
    contrib/libs/ydb-cpp-sdk/src/client/value
)

END()
