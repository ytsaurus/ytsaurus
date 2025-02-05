UNITTEST_FOR(contrib/ydb/library/yql/providers/dq/provider)

PEERDIR(
    library/cpp/testing/unittest
    contrib/ydb/library/yql/providers/dq/provider
    yql/essentials/sql/pg_dummy
    yql/essentials/public/udf/service/stub
)

SRCS(
    yql_dq_provider_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
