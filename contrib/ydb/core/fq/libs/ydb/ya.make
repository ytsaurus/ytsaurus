LIBRARY()

SRCS(
    schema.cpp
    util.cpp
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
)

GENERATE_ENUM_SERIALIZATION(ydb.h)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
