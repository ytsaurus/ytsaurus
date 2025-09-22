LIBRARY()

SRCS(
    out.cpp
    proto_accessor.cpp
    table.cpp
)

GENERATE_ENUM_SERIALIZATION(contrib/ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table_enum.h)

PEERDIR(
    contrib/ydb/public/api/protos
    contrib/ydb/public/sdk/cpp/src/client/impl/internal/make_request
    contrib/ydb/public/sdk/cpp/src/client/impl/internal/retry
    contrib/ydb/public/sdk/cpp/src/client/impl/session
    contrib/ydb/public/sdk/cpp/src/client/driver
    contrib/ydb/public/sdk/cpp/src/client/params
    contrib/ydb/public/sdk/cpp/src/client/proto
    contrib/ydb/public/sdk/cpp/src/client/result
    contrib/ydb/public/sdk/cpp/src/client/scheme
    contrib/ydb/public/sdk/cpp/src/client/table/impl
    contrib/ydb/public/sdk/cpp/src/client/table/query_stats
    contrib/ydb/public/sdk/cpp/src/client/types/operation
    contrib/ydb/public/sdk/cpp/src/client/value
)

END()
