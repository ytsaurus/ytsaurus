LIBRARY()

SRCS(
    table.cpp
    proto_accessor.cpp
)

GENERATE_ENUM_SERIALIZATION(table_enum.h)

PEERDIR(
    contrib/ydb/public/api/protos
    contrib/ydb/public/sdk/cpp/client/impl/ydb_internal/make_request
    contrib/ydb/public/sdk/cpp/client/impl/ydb_internal/kqp_session_common
    contrib/ydb/public/sdk/cpp/client/impl/ydb_internal/retry
    contrib/ydb/public/sdk/cpp/client/ydb_driver
    contrib/ydb/public/sdk/cpp/client/ydb_params
    contrib/ydb/public/sdk/cpp/client/ydb_proto
    contrib/ydb/public/sdk/cpp/client/ydb_result
    contrib/ydb/public/sdk/cpp/client/ydb_scheme
    contrib/ydb/public/sdk/cpp/client/ydb_table/impl
    contrib/ydb/public/sdk/cpp/client/ydb_table/query_stats
    contrib/ydb/public/sdk/cpp/client/ydb_types/operation
    contrib/ydb/public/sdk/cpp/client/ydb_value
)

END()
