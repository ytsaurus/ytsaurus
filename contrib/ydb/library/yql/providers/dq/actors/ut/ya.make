UNITTEST_FOR(contrib/ydb/library/yql/providers/dq/actors)

PEERDIR(
    library/cpp/testing/unittest
    library/cpp/time_provider
    contrib/ydb/library/actors/testlib
    contrib/ydb/library/yql/dq/actors
    contrib/ydb/library/yql/dq/actors/compute
    contrib/ydb/library/yql/providers/dq/actors
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg_dummy
)

SRCS(
    grouped_issues_ut.cpp
    actors_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
