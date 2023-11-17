LIBRARY()

SRCS(
    schema.cpp
    util.cpp
    ydb.cpp
)

PEERDIR(
    library/cpp/actors/core
    library/cpp/retry
    contrib/ydb/core/base
    contrib/ydb/core/fq/libs/config
    contrib/ydb/core/fq/libs/events
    contrib/ydb/library/security
    contrib/ydb/public/sdk/cpp/client/ydb_coordination
    contrib/ydb/public/sdk/cpp/client/ydb_rate_limiter
    contrib/ydb/public/sdk/cpp/client/ydb_scheme
    contrib/ydb/public/sdk/cpp/client/ydb_table
)

GENERATE_ENUM_SERIALIZATION(ydb.h)

YQL_LAST_ABI_VERSION()

END()
