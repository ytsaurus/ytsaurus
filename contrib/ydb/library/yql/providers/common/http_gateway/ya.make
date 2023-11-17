LIBRARY()

SRCS(
    yql_http_gateway.cpp
    yql_http_default_retry_policy.cpp
)

PEERDIR(
    contrib/libs/curl
    library/cpp/actors/prof
    library/cpp/monlib/dynamic_counters
    library/cpp/retry
    contrib/ydb/library/yql/providers/common/proto
    contrib/ydb/library/yql/public/issue
    contrib/ydb/library/yql/utils/log
    contrib/ydb/library/yql/utils
)

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    mock
)

RECURSE_FOR_TESTS(
    ut
)
