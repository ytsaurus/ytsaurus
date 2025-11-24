LIBRARY()

SRCS(
    local_session.cpp
    local_table_client.cpp
    query_actor.cpp
    schema.cpp
    sdk_session.cpp
    sdk_table_client.cpp
    util.cpp
    ydb_local_connection.cpp
    ydb_sdk_connection.cpp
    ydb.cpp
)

PEERDIR(
    contrib/ydb/library/actors/core
    library/cpp/retry
    contrib/ydb/core/base
    contrib/ydb/core/fq/libs/config
    contrib/ydb/core/fq/libs/events
    contrib/ydb/library/security
    contrib/ydb/public/sdk/cpp/adapters/issue
    contrib/ydb/public/sdk/cpp/src/client/coordination
    contrib/ydb/public/sdk/cpp/src/client/rate_limiter
    contrib/ydb/public/sdk/cpp/src/client/scheme
    contrib/ydb/public/sdk/cpp/src/client/table
    contrib/ydb/library/query_actor
    contrib/ydb/core/base/generated
    contrib/ydb/library/aclib/protos
)

GENERATE_ENUM_SERIALIZATION(ydb.h)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
