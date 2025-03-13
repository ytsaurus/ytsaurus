LIBRARY()

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/public/sdk/cpp/sdk_common.inc)

SRCS(
    ydb_dynamic_config.cpp
    ydb_replication.cpp
    ydb_scripting.cpp
    ydb_view.cpp
)

GENERATE_ENUM_SERIALIZATION(contrib/ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/draft/ydb_replication.h)

PEERDIR(
    contrib/ydb/public/sdk/cpp/src/library/issue
    contrib/ydb/public/api/grpc/draft
    contrib/ydb/public/sdk/cpp/src/client/table
    contrib/ydb/public/sdk/cpp/src/client/types/operation
    contrib/ydb/public/sdk/cpp/src/client/value
)

END()
